"""Base runner: agent + resolved ``RunConfig`` + lazy workspace inputs."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Self

import httpx
from agents import Agent, ModelSettings, OpenAIProvider, RunConfig
from agents.sandbox import SandboxAgent
from openai import AsyncOpenAI

from agentdeck.adapters.caps.sandbox import UnixSandbox, open_sandbox
from agentdeck.agents.base import inner_agent_of, is_sandbox_tool
from agentdeck.runtime.observability import init_observability, sandbox_trace_env
from agentdeck.runtime.settings import Settings, get_settings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Mapping, Sequence
    from pathlib import Path


def default_use_responses() -> bool:
    """Default to the SDK's Responses transport, overridable via env.

    Set ``OPENAI_USE_RESPONSES=false`` when targeting a Chat-Completions-
    only model server. Default deployments pick up real per-message
    response ids and avoid the ``FAKE_RESPONSES_ID`` collision entirely.
    """
    raw = os.environ.get("OPENAI_USE_RESPONSES")
    if raw is None:
        return True
    return raw.strip().lower() not in {"0", "false", "no", "off"}


@dataclass(slots=True)
class BaseRunner(ABC):
    """Holds an agent, a resolved :class:`RunConfig`, and workspace inputs."""

    agent: Agent
    run_config: RunConfig
    environment: Mapping[str, str] = field(default_factory=dict)
    input_files: Sequence[str | os.PathLike[str]] = ()
    max_turns: int = field(default_factory=lambda: get_settings().runner.max_turns)

    @classmethod
    def from_agent(
        cls,
        agent: Agent,
        *,
        model: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        workflow_name: str | None = None,
        temperature: float | None = None,
        max_turns: int | None = None,
        max_tokens: int | None = None,
        environment: Mapping[str, str] | None = None,
        input_files: Sequence[str | os.PathLike[str]] | None = None,
        **runner_options: Any,
    ) -> Self:
        """Build a runner with run config resolved from settings + per-call kwargs."""
        settings: Settings = get_settings()
        openai = settings.openai.with_overrides(api_key=api_key, base_url=base_url, model=model)
        runner = settings.runner.with_overrides(
            workflow_name=workflow_name,
            temperature=temperature,
            max_turns=max_turns,
            max_tokens=max_tokens,
        )
        # Start Langfuse + OpenInference once (no-op when Langfuse is disabled) and let
        # its result drive the SDK's native tracing switch. When active, OpenInference
        # exports the Agents-SDK trace pipeline to Langfuse; when not, tracing is off (no
        # OpenAI-backend export). Session identity comes from the run's own trace: the
        # telemetry sink for a run played through the Runtime, ``trace_run`` for a direct call.
        run_config = RunConfig(
            workflow_name=runner.workflow_name,
            model=openai.model,
            nest_handoff_history=True,
            tracing_disabled=not init_observability(),
            # Honour the SDK default (Responses); flip via OPENAI_USE_RESPONSES=false
            # when targeting a Chat-Completions-only model server.
            model_provider=OpenAIProvider(
                # A custom CA bundle needs its own httpx client (verify=<path>), so
                # base_url/api_key must ride on that client rather than the provider.
                openai_client=AsyncOpenAI(
                    base_url=openai.base_url or None,
                    api_key=openai.api_key,
                    http_client=httpx.AsyncClient(verify=openai.ca_bundle),
                )
                if openai.ca_bundle
                else None,
                base_url=None if openai.ca_bundle else (openai.base_url or None),
                api_key=None if openai.ca_bundle else (openai.api_key or None),
                use_responses=default_use_responses(),
            ),
            # ``include_usage`` asks the Chat-Completions API to emit the streaming usage
            # chunk (prompt/completion tokens) — without it, streamed turns land in Langfuse
            # with no token counts. No-op on the Responses API (usage is always included).
            model_settings=ModelSettings(
                temperature=runner.temperature, max_tokens=runner.max_tokens, include_usage=True
            ),
        )
        return cls(
            agent=agent,
            run_config=run_config,
            environment=settings.sandbox_env(environment),
            input_files=tuple(input_files or ()),
            max_turns=runner.max_turns,
            **runner_options,
        )

    @asynccontextmanager
    async def attach_sandbox(
        self,
        *,
        manifest_root: Path | None = None,
    ) -> AsyncGenerator[UnixSandbox | None, None]:
        """Open a sandbox and bind it to ``run_config.sandbox`` if needed.

        Opens when the top-level agent OR any direct handoff is a
        :class:`SandboxAgent` — the SDK requires ``run_config.sandbox``
        before any handoff target executes, not just for the first agent
        on the turn. No-op for graphs of plain :class:`Agent` only.
        Joins an outer sandbox bound to the current async context.
        """
        if not needs_sandbox(self.agent):
            yield None
            return
        async with open_sandbox(
            environment=dict(self.environment),
            input_files=tuple(self.input_files),
            manifest_root=os.fspath(manifest_root) if manifest_root else None,
            trace_env=sandbox_trace_env,
        ) as ws:
            self.run_config.sandbox = ws.sandbox_run_config
            yield ws

    @abstractmethod
    async def run(self) -> Any:
        """Drive the configured agent."""

    def run_streamed(self, message: Any = None, *, session: Any = None) -> AsyncIterator[Any]:
        """Streamed counterpart to :meth:`run`, yielding incremental output.

        Not abstract: streaming depends on the underlying engine, so a runner opts in by
        overriding this as an async generator (see :class:`HeadlessRunner`).
        """
        raise NotImplementedError(f"{type(self).__name__} does not support streaming")


def needs_sandbox(agent: Agent, _seen: set[int] | None = None) -> bool:
    """``True`` if ``agent`` or any reachable worker requires a sandbox.

    Walks both direct handoffs (Agent instances; Handoff wrappers don't expose
    a stable agent reference, so they're treated as non-sandboxed) and tools
    that wrap an inner Agent via :meth:`BaseAgent.as_tool`. The lookup is
    keyed off two sidecar registries in :mod:`agentdeck.agents.base` (no
    private attributes stamped on SDK types).

    Cycle guard via ``_seen`` covers handoff / tool loops.
    """
    seen = _seen if _seen is not None else set()
    if id(agent) in seen:
        return False
    seen.add(id(agent))
    if isinstance(agent, SandboxAgent):
        return True
    for entry in getattr(agent, "handoffs", ()) or ():
        if isinstance(entry, Agent) and needs_sandbox(entry, seen):
            return True
    for tool in getattr(agent, "tools", ()) or ():
        if is_sandbox_tool(tool):
            return True
        inner = inner_agent_of(tool)
        if isinstance(inner, Agent) and needs_sandbox(inner, seen):
            return True
    return False


__all__ = ["BaseRunner", "default_use_responses", "needs_sandbox"]
