"""Which checkpointer a graph is compiled around, and what each answer costs a second run.

``durable`` has three states and they are not two. Declared ``False`` must mean *no
checkpointer* — an in-memory one keyed by thread makes a second run on that thread resume
the first run's state, which is the opposite of what a workflow declaring itself
non-durable asked for. v1 compiled such a graph with no saver at all
(``BaseWorkflow.build``); this pins that the adapter still does.

Absent is the third state and is deliberately not ``False``: a spec built in code never
said, so it keeps the engine's own default — which is what lets a hand-wired
``LangGraphEngine()`` interrupt at all, and what the contract suite's interrupt case runs on.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypedDict

from langgraph.graph import END, START, StateGraph

from agentdeck.adapters.engines.langgraph import LangGraphEngine
from agentdeck.core.content import DataBlock
from agentdeck.core.context import RunContext
from agentdeck.core.events import RunCompleted
from agentdeck.core.invocable import InvocableKind, InvocableSpec
from agentdeck.runtime.discovery import DURABLE_KEY

if TYPE_CHECKING:
    from agentdeck.core.content import Input


class _State(TypedDict, total=False):
    input: str
    seen: list[str]


def _accumulate(state: _State) -> _State:
    # Reads what is already there, so a resumed thread is visible in the output rather than
    # having to be inferred from the checkpointer's internals.
    return {"seen": [*state.get("seen", []), state.get("input", "")]}


def _spec(metadata: dict[str, Any]) -> InvocableSpec:
    graph: StateGraph[Any] = StateGraph(_State)
    graph.add_node("acc", _accumulate)
    graph.add_edge(START, "acc")
    graph.add_edge("acc", END)
    return InvocableSpec(
        name="Accumulator",
        kind=InvocableKind.WORKFLOW,
        engine=LangGraphEngine.engine,
        native=graph,
        metadata=metadata,
    )


def _ctx(run_id: str) -> RunContext:
    # One session_id across both runs: that is the thread a second turn would resume.
    return RunContext(tenant="acme", principal="user:1", run_id=run_id, trace_id=run_id, session_id="thread-1")


async def _seen(engine: LangGraphEngine, spec: InvocableSpec, text: str, run_id: str) -> list[str]:
    input: Input = [DataBlock(data={"input": text})]
    payloads = [payload async for payload in engine.start(spec, input, [], _ctx(run_id))]
    terminal = payloads[-1]
    assert isinstance(terminal, RunCompleted), terminal
    block = terminal.output[0]
    assert isinstance(block, DataBlock)
    return block.data["seen"]


async def test_a_non_durable_workflow_starts_fresh_on_a_thread_it_already_ran() -> None:
    """``durable=False`` twice on one thread is two independent runs, not a resume.

    The engine instance is shared on purpose: one process serving two requests is exactly
    where an in-memory saver keyed by thread would carry state across.
    """
    engine = LangGraphEngine()
    spec = _spec({DURABLE_KEY: False})

    first = await _seen(engine, spec, "a", "r-1")
    second = await _seen(engine, spec, "b", "r-2")

    assert first == ["a"]
    assert second == ["b"], f"the second run resumed the first's state: {second}"


async def test_a_spec_that_never_declared_durable_keeps_the_engines_own_checkpointer() -> None:
    """Absent metadata is not ``False``: a code-built spec keeps the engine default.

    Without it a hand-wired ``LangGraphEngine()`` could not interrupt, which the contract
    suite's interrupt case depends on — so this pins the distinction the fix rests on.
    """
    engine = LangGraphEngine()
    spec = _spec({})

    first = await _seen(engine, spec, "a", "r-1")
    second = await _seen(engine, spec, "b", "r-2")

    assert first == ["a"]
    assert second == ["a", "b"], f"the engine default was not in place: {second}"
