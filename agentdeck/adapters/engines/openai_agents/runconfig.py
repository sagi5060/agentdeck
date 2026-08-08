"""How one run is configured: plain resolved values in, an SDK ``RunConfig`` out.

The values arrive from the composition root (``agentdeck/composition.py``'s
``resolve_run_settings``) rather than being read here, for the reason the store and the
control port are already resolved there: an adapter that reaches for ``get_settings()``
cannot be handed a different endpoint by a caller, and a second front door would have to
mutate process state to get one.

A bare :class:`RunSettings` therefore configures nothing at all — no model override, no
provider, the SDK's own defaults — which is what a code-first caller wiring
``OpenAIAgentsEngine()`` by hand gets. Naming a model is what turns on the provider, since
an endpoint the run was pointed at is the only thing that makes overriding an agent's own
model correct.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import httpx
from agents import ModelSettings, MultiProvider, OpenAIProvider, RunConfig
from openai import AsyncOpenAI


@dataclass(frozen=True, slots=True)
class RunSettings:
    """Everything a run's ``RunConfig`` is resolved from, as values an adapter can hold.

    Defaults are the SDK's, not the project's: ``RunSettings()`` must leave a run exactly as
    ``RunConfig()`` would, so the contract suite and a code-first caller keep configuring
    their agents themselves.
    """

    model: str | None = None
    api_key: str = ""
    base_url: str = ""
    ca_bundle: str = ""
    use_responses: bool = True
    workflow_name: str = "Agent workflow"
    nest_handoff_history: bool = False
    temperature: float | None = None
    max_tokens: int | None = None
    max_turns: int = 10


def build_run_config(settings: RunSettings, *, sandbox: Any = None) -> RunConfig:
    """One run's ``RunConfig``.

    Built per run, never once and mutated: ``sandbox`` is this run's workspace handle, and a
    shared config carrying somebody else's would hand two concurrent turns the same session.
    """
    return RunConfig(
        workflow_name=settings.workflow_name,
        model=settings.model,
        nest_handoff_history=settings.nest_handoff_history,
        tracing_disabled=not tracing_enabled(),
        model_provider=_provider(settings) or MultiProvider(),
        # ``include_usage`` asks the Chat-Completions API to emit the streaming usage chunk
        # (prompt/completion tokens) — without it, streamed turns carry no token counts at
        # all, so ``usage.reported`` and ``run.completed`` would both report zero. No-op on
        # the Responses API, where usage is always included.
        model_settings=ModelSettings(
            temperature=settings.temperature, max_tokens=settings.max_tokens, include_usage=True
        ),
        sandbox=sandbox,
    )


def tracing_enabled() -> bool:
    """Opt-in switch for the SDK's default trace exporter (issue #61).

    Off by default: a keyless/fake-model run (tests, CI, the M0 demo) has no OpenAI
    account to export traces to, and the SDK's exporter otherwise attempts a real HTTPS
    call on every run, logging a non-fatal ``Tracing client error 401``. Set
    ``AGENTDECK_OPENAI_AGENTS_TRACING_ENABLED=true`` to restore it for a deployment that
    wants the SDK's own trace export.

    Not the Langfuse switch it used to be: traces are built from the event stream by
    ``adapters/telemetry/langfuse``, so the SDK's own exporter is a separate question now
    and answered separately.
    """
    raw = os.environ.get("AGENTDECK_OPENAI_AGENTS_TRACING_ENABLED")
    return raw is not None and raw.strip().lower() in {"1", "true", "yes", "on"}


def _provider(settings: RunSettings) -> OpenAIProvider | None:
    """The provider for the endpoint these settings name, or ``None`` for "no endpoint".

    A custom CA bundle needs its own httpx client (``verify=<path>``), so ``base_url`` and
    ``api_key`` ride on that client rather than on the provider — the provider ignores both
    once it is handed a client of its own.
    """
    if not settings.model:
        return None
    if settings.ca_bundle:
        return OpenAIProvider(
            openai_client=AsyncOpenAI(
                base_url=settings.base_url or None,
                api_key=settings.api_key,
                http_client=httpx.AsyncClient(verify=settings.ca_bundle),
            ),
            use_responses=settings.use_responses,
        )
    return OpenAIProvider(
        base_url=settings.base_url or None,
        api_key=settings.api_key or None,
        use_responses=settings.use_responses,
    )


__all__ = ["RunSettings", "build_run_config", "tracing_enabled"]
