"""Sandbox-aware ``BaseWorkflow`` over LangGraph state graphs."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, ClassVar

from langgraph.graph import END, START
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Command
from pydantic import BaseModel

from agentdeck.adapters.engines.langgraph.checkpointer import resolve_checkpointer
from agentdeck.errors import ConfigError
from agentdeck.runtime.settings import get_settings
from agentdeck.workflows.interrupts import INTERRUPT_KEY, InterruptResult, as_interrupt, interrupt_result
from agentdeck.workflows.state import dump_state

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Sequence

    from agents.tool import FunctionTool
    from langgraph.graph import StateGraph
    from langgraph.graph.state import CompiledStateGraph

    from agentdeck.workflows.runners.dev import DevWorkflowRunner

_CAMEL_RE = re.compile(r"(?<!^)(?=[A-Z])")


class BaseWorkflow:
    """Declarative LangGraph workflow with a shared sandbox.

    Override :attr:`state` (a Pydantic model) and :meth:`build_graph`
    (returns an unbuilt ``StateGraph``). :meth:`run` opens one
    sandbox for the whole graph; all nodes — and any agents
    they invoke — share that session via ``require_sandbox()``.
    """

    name: ClassVar[str] = ""
    description: ClassVar[str] = ""
    state: ClassVar[type]
    # Opt-in durability: compiles with a checkpointer (``AGENTDECK_CHECKPOINT_*``,
    # see ``adapters.engines.langgraph.checkpointer``) so a run can resume by ``thread_id``
    # after the process dies. ``False`` (default) compiles exactly as before — no behavior change.
    durable: ClassVar[bool] = False

    # ``cls.__dict__.get`` (vs. ``getattr``) prevents subclasses from
    # inheriting the parent's compiled graph.
    _compiled: ClassVar[CompiledStateGraph[Any] | None] = None

    @classmethod
    def build_graph(cls) -> StateGraph[Any]:
        raise NotImplementedError(
            f"{cls.__name__}.build_graph() must return a langgraph StateGraph.",
        )

    @classmethod
    def build(cls) -> CompiledStateGraph[Any]:
        """Return the compiled graph, building (and caching) on first call."""
        if cls.__dict__.get("_compiled") is None:
            if cls.durable:
                checkpoint = get_settings().checkpoint
                checkpointer = resolve_checkpointer(checkpoint.backend, checkpoint.url)
                cls._compiled = cls.build_graph().compile(checkpointer=checkpointer)
            else:
                cls._compiled = cls.build_graph().compile()
        compiled: CompiledStateGraph[Any, None, Any, Any] | None = cls._compiled
        assert compiled is not None  # set above; narrows ClassVar Optional
        return compiled

    @classmethod
    def runner(cls, **runner_options: Any) -> DevWorkflowRunner:
        from agentdeck.workflows.runners.dev import DevWorkflowRunner

        return DevWorkflowRunner.from_workflow(cls, **runner_options)

    @classmethod
    async def run(cls, state: Any = None, *, thread_id: str | None = None, **runner_options: Any) -> Any:
        """Run the graph once. ``thread_id`` scopes checkpointed state (required if ``durable``).

        Returns the final state, or an :class:`~agentdeck.workflows.interrupts.InterruptResult`
        if a node called ``langgraph.types.interrupt()`` — hand that payload to a human and
        resume the run with :meth:`resume`. The interrupted node re-runs from its start on
        resume, so it must be pure: put side effects in earlier nodes.
        """
        runner_options = cls._thread_scoped_options(thread_id, runner_options)
        result = await cls.runner(**runner_options).run(state)
        interrupted = cls._interrupt_or_none(result, thread_id)
        return result if interrupted is None else interrupted

    @classmethod
    async def run_stream(
        cls, state: Any = None, *, thread_id: str | None = None, **runner_options: Any
    ) -> AsyncIterator[dict[str, Any] | InterruptResult]:
        """Streaming counterpart to :meth:`run`: yields ``node_update``/``custom`` events per
        :meth:`DevWorkflowRunner.run_stream <agentdeck.workflows.runners.dev.DevWorkflowRunner.run_stream>`,
        then one terminal ``done`` event — or an :class:`~agentdeck.workflows.interrupts.InterruptResult`
        event in its place when the run paused for a human. Same ``thread_id`` semantics as ``run``.
        """
        runner_options = cls._thread_scoped_options(thread_id, runner_options)
        async for event in cls.runner(**runner_options).run_stream(state):
            # LangGraph reports the pause as an update from a pseudo-node; the terminal event carries it.
            if event["type"] == "node_update" and event["node"] == INTERRUPT_KEY:
                continue
            interrupted = cls._interrupt_or_none(event["state"], thread_id) if event["type"] == "done" else None
            yield event if interrupted is None else interrupted

    @classmethod
    def _interrupt_or_none(cls, result: Any, thread_id: str | None) -> InterruptResult | None:
        """The pause ``result`` stopped on, or ``None``; a non-durable pause can't be resumed."""
        interrupted = as_interrupt(result, thread_id or "")
        if interrupted is not None and not cls.durable:
            raise ConfigError(
                f"{cls.__name__} called interrupt() but is durable=False: with no checkpointer the paused run "
                "cannot be resumed. Set `durable = True` on the workflow.",
            )
        return interrupted

    @classmethod
    def _thread_scoped_options(cls, thread_id: str | None, runner_options: dict[str, Any]) -> dict[str, Any]:
        if cls.durable and thread_id is None:
            raise ValueError(
                f"{cls.__name__} is durable=True; a thread_id is required to load/persist checkpointed state.",
            )
        if thread_id is None:
            return runner_options
        config = dict(runner_options.get("config") or {})
        config["configurable"] = {**config.get("configurable", {}), "thread_id": thread_id}
        return {**runner_options, "config": config}

    @classmethod
    async def resume(cls, thread_id: str, value: Any, **runner_options: Any) -> Any:
        """Resume the run paused on ``thread_id``: ``interrupt()`` returns ``value``.

        Returns the final state, or the next
        :class:`~agentdeck.workflows.interrupts.InterruptResult` if the run pauses again.
        """
        if not cls.durable:
            raise ConfigError(f"{cls.__name__} is durable=False: there is no checkpointed run to resume.")
        return await cls.run(Command(resume=value), thread_id=thread_id, **runner_options)

    @classmethod
    async def pending(cls) -> list[InterruptResult]:
        """Every thread of this workflow currently paused on an interrupt — the approval inbox."""
        if not cls.durable:
            return []
        graph = cls.build()
        saver = graph.checkpointer
        if saver is None or isinstance(saver, bool):
            return []
        # Drain the listing first: the sqlite saver locks for the whole alist generator.
        thread_ids = {
            tid
            async for checkpoint in saver.alist(None)
            if isinstance(tid := checkpoint.config.get("configurable", {}).get("thread_id"), str)
        }
        pending: list[InterruptResult] = []
        for thread_id in sorted(thread_ids):
            # Foreign threads (shared saver) replay with no pending tasks, so no interrupts.
            snapshot = await graph.aget_state({"configurable": {"thread_id": thread_id}})
            pending.extend(interrupt_result(i.value, thread_id) for i in snapshot.interrupts)
        return pending

    @classmethod
    def as_tool(
        cls,
        *,
        name: str | None = None,
        description: str | None = None,
        output_keys: Sequence[str] | None = None,
        defaults: dict[str, Any] | None = None,
        strict_json_schema: bool = False,
    ) -> FunctionTool:
        """Expose this workflow as a :class:`agents.tool.FunctionTool`.

        ``output_keys`` filters the final state to a subset of channels.
        ``defaults`` pins specific workflow-state fields to fixed values
        regardless of the LLM's argument payload — useful for binding
        knobs the agent should not toggle (e.g. ``interactive=True``).
        Pinned fields are stripped from the JSON schema the LLM sees so
        the model does not even try to set them.

        Strict JSON schema is off by default — small chat-completions
        models stall on multi-required-field schemas.
        """
        from agents.tool import FunctionTool

        if not issubclass(cls.state, BaseModel):
            raise TypeError(
                f"{cls.__name__}.state must be a Pydantic model to be exposed as a tool; got {cls.state!r}.",
            )
        keys = tuple(output_keys) if output_keys is not None else None
        pinned = dict(defaults) if defaults else {}
        cls.build()  # surface graph build errors at tool-construction time

        async def on_invoke(_ctx: Any, raw_args: str) -> str:
            args = json.loads(raw_args) if raw_args else {}
            if pinned:
                args = {**args, **pinned}  # pinned values always win
            # Through cls.run so the workflow inherits an active sandbox
            # from the calling agent.
            result = await cls.run(args)
            if keys is not None:
                result: dict[str, Any] = {k: result.get(k) for k in keys}
            return dump_state(result)

        schema = cls.state.model_json_schema()
        if pinned:
            schema = _strip_schema_fields(schema, pinned.keys())

        return FunctionTool(
            name=name or _CAMEL_RE.sub("_", cls.__name__).lower(),
            description=description or cls._tool_description(),
            params_json_schema=schema,
            on_invoke_tool=on_invoke,
            strict_json_schema=strict_json_schema,
        )

    @classmethod
    def node_names(cls) -> list[str]:
        """Node names on the graph excluding START/END.

        Reads the unbuilt :class:`StateGraph` so info-style introspection
        works even if compilation would fail.
        """
        return [n for n in cls.build_graph().nodes if n not in {START, END}]

    @classmethod
    def _tool_description(cls) -> str:
        if cls.description:
            return cls.description.strip()
        if cls.__doc__:
            return cls.__doc__.strip().splitlines()[0]
        return f"Run the {cls.__name__} workflow."


def _strip_schema_fields(schema: dict[str, Any], fields: Iterable[str]) -> dict[str, Any]:
    """Return ``schema`` with ``fields`` removed from ``properties`` and ``required``."""
    drop = set(fields)
    out = dict(schema)
    if isinstance(props := out.get("properties"), dict):
        out["properties"] = {k: v for k, v in props.items() if k not in drop}
    if isinstance(required := out.get("required"), list):
        out["required"] = [r for r in required if r not in drop]
    return out


__all__ = ["BaseWorkflow"]
