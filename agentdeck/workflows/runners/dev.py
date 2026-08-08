"""Single-shot graph runner used by :meth:`BaseWorkflow.run`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from agentdeck.adapters.caps.sandbox import open_sandbox
from agentdeck.runtime.observability import sandbox_trace_env, trace_run
from agentdeck.workflows.nodes import STREAM_CONFIGURABLE_KEY
from agentdeck.workflows.runners.base import BaseWorkflowRunner
from agentdeck.workflows.state import coerce_input

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from langchain_core.runnables import RunnableConfig


@dataclass(slots=True)
class DevWorkflowRunner(BaseWorkflowRunner):
    """Compile-and-invoke driver — every node sees one shared workspace."""

    async def run(self, state: Any = None) -> Any:
        # One root observation makes the whole graph run a single trace, every node's
        # agent/skill spans nested under it, carrying its input/output. A run played through
        # the Runtime is traced from the event stream instead; this is the direct-call path.
        initial = coerce_input(state, self.workflow.state)
        with trace_run(name=self.workflow.name or self.workflow.__name__, input=initial) as tr:
            async with open_sandbox(
                environment=self.environment,
                input_files=self.input_files,
                trace_env=sandbox_trace_env,
            ):
                result = await self.graph.ainvoke(initial, config=self.config)
                tr.set_output(result)
                return result

    async def run_stream(self, state: Any = None) -> AsyncIterator[dict[str, Any]]:
        """One ``astream`` over ``["updates", "custom"]``: a ``node_update`` event per
        completed node, a ``custom`` event per :func:`~langgraph.config.get_stream_writer`
        call (e.g. a nested :class:`~agentdeck.workflows.nodes.AgentNode`'s text deltas),
        then one terminal ``done`` event carrying the final state.
        """
        initial = coerce_input(state, self.workflow.state)
        final_state: Any = initial
        with trace_run(name=self.workflow.name or self.workflow.__name__, input=initial) as tr:
            async with open_sandbox(
                environment=self.environment,
                input_files=self.input_files,
                trace_env=sandbox_trace_env,
            ):
                # Tells a nested AgentNode (via get_config()) that it's safe to use run_streamed.
                stream_config: RunnableConfig = {
                    **self.config,
                    "configurable": {**self.config.get("configurable", {}), STREAM_CONFIGURABLE_KEY: True},
                }
                # astream's stub only declares the single-mode shape; multi-mode yields (mode, chunk) tuples.
                stream = cast(
                    "AsyncIterator[tuple[str, Any]]",
                    self.graph.astream(initial, config=stream_config, stream_mode=["updates", "custom", "values"]),
                )
                async for mode, chunk in stream:
                    if mode == "updates":
                        for node, delta in cast("dict[str, Any]", chunk).items():
                            yield {"type": "node_update", "node": node, "delta": delta}
                    elif mode == "custom":
                        yield {"type": "custom", "data": chunk}
                    else:  # "values" — tracked for the final state, not surfaced as its own event
                        final_state = chunk
                tr.set_output(final_state)
                yield {"type": "done", "state": final_state}


__all__ = ["DevWorkflowRunner"]
