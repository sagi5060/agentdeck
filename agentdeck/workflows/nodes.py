"""Workflow node callables: skill, agent, sandbox-agent, file-loader."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from langgraph.config import get_config, get_stream_writer

from agentdeck.adapters.engines.langgraph.engine import STREAM_CONFIGURABLE_KEY
from agentdeck.agents.base import BaseAgent, BaseSandboxAgent
from agentdeck.agents.runners.headless import HeadlessRunner, StreamDone
from agentdeck.core.ports.sandbox import require_sandbox
from agentdeck.runtime.settings import get_settings
from agentdeck.skills import SkillBundle, SkillExecutionError, SkillExecutor, SkillOutputSchema, SkillResult

logger = logging.getLogger(__name__)

# Set by the langgraph engine on every run it drives, and by DevWorkflowRunner.run_stream();
# unset under run()/ainvoke(), which stay on Runner.run. Imported from the engine that owns it
# rather than redeclared, so the two cannot name the channel differently.

ArgvBuilder = Callable[[Any], Sequence[str | Path]]
StateUpdate = Callable[[Any, SkillResult], Awaitable[dict[str, Any]] | dict[str, Any]]
PathBuilder = Callable[[Any], str | Path | None]
TextParser = Callable[[str], Any]


class SkillNode:
    """Run a :class:`SkillBundle` as a graph node.

    ``into`` parses stdout into the bundle's ``output_schema`` and stores
    it under that state field. ``env`` contributes extra sandbox env
    (dict or ``(state) -> dict`` callable) and flows through to
    :meth:`Settings.sandbox_env`'s ``extra`` kwarg. ``update`` is the
    schema-less escape hatch: ``(state, raw_result) -> dict`` merged into
    state.
    """

    __slots__ = ("bundle", "argv", "into", "env", "update", "timeout", "raise_on_error", "_schema")

    def __init__(
        self,
        bundle: SkillBundle,
        *,
        argv: ArgvBuilder | Sequence[str | Path] = (),
        into: str | None = None,
        env: Callable[[Any], Mapping[str, str]] | Mapping[str, str] | None = None,
        update: StateUpdate | None = None,
        timeout: float | None = None,
        raise_on_error: bool = True,
    ) -> None:
        bundle = _require_skill_bundle(bundle)
        if into is not None and bundle.output_schema is None:
            raise ValueError(
                f"SkillNode(into={into!r}) requires bundle {bundle.name!r} "
                f"to declare 'metadata.output_schema' in SKILL.md.",
            )
        self.bundle = bundle
        # Normalize to builders once, so __call__ needs no runtime narrowing.
        # ``cast`` because ``callable()`` can't prove a Sequence isn't also callable.
        self.argv: ArgvBuilder = cast("ArgvBuilder", argv) if callable(argv) else _const(tuple(argv))
        self.into = into
        self.env: Callable[[Any], Mapping[str, str]] | None = (
            cast("Callable[[Any], Mapping[str, str]]", env)
            if callable(env)
            else _const(dict(env))
            if env is not None
            else None
        )
        self.update = update
        self.timeout = timeout
        self.raise_on_error = raise_on_error
        self._schema: type[SkillOutputSchema] | None = bundle.output_schema if into is not None else None

    async def __call__(self, state: Any) -> dict[str, Any]:
        argv = self.argv(state)
        extras = self.env(state) if self.env is not None else None
        logger.debug("skill %s: start argv=%s", self.bundle.name, argv)
        # Resolve settings per-call so test monkeypatches take effect.
        executor = SkillExecutor(bundle=self.bundle, env=get_settings().sandbox_env())
        result = await executor.run(
            *(str(a) for a in argv),
            timeout=self.timeout,
            env_extras=dict(extras) if extras else None,
        )
        logger.debug("skill %s: exit=%s ok=%s", self.bundle.name, result.exit_code, result.ok)
        if self.raise_on_error and not result.ok:
            raise SkillExecutionError(self.bundle.name, result.exit_code, result.stderr)

        delta: dict[str, Any] = {}
        if self._schema is not None and self.into is not None:
            delta[self.into] = self._schema.from_result(result)
        if self.update is not None:
            update = self.update(state, result)
            resolved = cast("dict[str, Any]", await update if inspect.isawaitable(update) else update)
            if resolved:
                delta.update(resolved)
        return delta


class LoadFileNode:
    """Read a file emitted by an upstream node into ``into``.

    ``path(state)`` returns the file path or ``None`` (no-op so the node can
    sit downstream of optional stages). Absolute paths read the host fs
    directly; relative paths go through the active sandbox.
    """

    __slots__ = ("path", "into", "parse")

    def __init__(
        self,
        *,
        path: PathBuilder,
        into: str,
        parse: TextParser | None = None,
    ) -> None:
        # NOTE: ``path`` must read from state at call time; static strings here
        # are almost always a mistake (a missing ``lambda s: s.field``).
        if not callable(path):
            raise TypeError(
                f"LoadFileNode(path=...) expects a callable (state -> path); got {path!r}.",
            )
        self.path = path
        self.into = into
        self.parse = parse

    async def __call__(self, state: Any) -> dict[str, Any]:
        target = self.path(state)
        if not target:
            return {}
        target_path = Path(str(target))
        if target_path.is_absolute():
            text = target_path.read_text(encoding="utf-8")
        else:
            text = await require_sandbox().read_text(str(target_path))
        return {self.into: self.parse(text) if self.parse else text}


class AgentNode:
    """Run a :class:`BaseAgent` subclass as a graph node.

    Reads the prompt from ``state[input_key]`` and writes the agent's
    ``final_output`` to ``state[output_key]``. ``input_files_key`` mounts
    host paths from state under the sandbox ``input_files/`` prefix.
    Forwards the nested agent's text deltas into the graph's custom stream
    (``get_stream_writer()``) so ``run_workflow_stream`` surfaces them too.
    """

    __slots__ = ("agent_cls", "input_key", "output_key", "input_files_key", "_built")

    _expected_base: type = BaseAgent

    def __init__(
        self,
        agent: type[BaseAgent],
        *,
        input_key: str = "input",
        output_key: str = "output",
        input_files_key: str | None = None,
    ) -> None:
        expected = type(self)._expected_base
        # Public API: defend against passing instances or unrelated classes.
        if not issubclass(agent, expected):
            raise TypeError(
                f"{type(self).__name__}(agent=...) expects a {expected.__name__} subclass; got {agent!r}.",
            )
        self.agent_cls = agent
        self.input_key = input_key
        self.output_key = output_key
        self.input_files_key = input_files_key
        # SDK Agent built by ``BaseAgent.build()`` is stateless across runs;
        # reuse the first build to skip capability-wiring cost.
        self._built: Any | None = None

    async def __call__(self, state: Any) -> dict[str, Any]:
        if self._built is None:
            self._built = self.agent_cls.build()
        logger.debug("agent node %s: start", self.agent_cls.__name__)
        files = _read(state, self.input_files_key)
        kwargs: dict[str, Any] = {"input_files": list(files)} if files else {}
        runner = HeadlessRunner.from_agent(self._built, **kwargs)
        prompt = _read(state, self.input_key)
        if not _is_streaming():
            result = await runner.run(prompt)
            return {self.output_key: result.final_output}
        writer = get_stream_writer()
        done: Any = None
        async for chunk in runner.run_streamed(prompt):
            if isinstance(chunk, StreamDone):
                done = chunk
            else:
                writer(chunk)
        return {self.output_key: done.final_output}


class SandboxAgentNode(AgentNode):
    """:class:`AgentNode` constrained to :class:`BaseSandboxAgent` subclasses."""

    _expected_base = BaseSandboxAgent


def _const[T](value: T) -> Callable[[Any], T]:
    """Wrap a static value as a ``(state) -> value`` builder."""
    return lambda _state: value


def _is_streaming() -> bool:
    """``True`` only inside ``DevWorkflowRunner.run_stream``'s ``astream`` call."""
    return bool(get_config().get("configurable", {}).get(STREAM_CONFIGURABLE_KEY))


def _read(state: Any, key: str | None) -> Any:
    if key is None:
        return None
    if isinstance(state, Mapping):
        return state.get(key)
    return getattr(state, key, None)


def _require_skill_bundle(bundle: object) -> SkillBundle:
    if not isinstance(bundle, SkillBundle):
        raise TypeError(f"SkillNode(bundle=...) expects a SkillBundle; got {bundle!r}.")
    return bundle


__all__ = ["AgentNode", "LoadFileNode", "SandboxAgentNode", "SkillExecutionError", "SkillNode"]
