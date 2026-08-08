"""Issue #47 end to end: a real tool and a real node reporting, through a real surface.

The three claims worth proving outside the Runtime's own unit tests, because each one is a
different reach problem: an openai-agents **function tool** finds the reporter on the SDK's
context object, a langgraph **node** finds it in langgraph's own ``configurable``, and an SSE
client sees both kinds arrive in order without the surface knowing they exist. The reference
CLI renderer reading them closes the loop, including its default case — the promise every
consumer makes about a kind it has never heard of.

Scripted fakes only: the SDK boundary is the one thing stubbed, so nothing here calls a model.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypedDict

import httpx
import pytest
from agents import Agent, RunContextWrapper, function_tool
from agents.models.interface import Model
from langgraph.graph.state import END, StateGraph
from openai.types.responses import (
    Response,
    ResponseCompletedEvent,
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
    ResponseUsage,
)
from openai.types.responses.response_usage import InputTokensDetails, OutputTokensDetails

from agentdeck.adapters.engines.langgraph import REPORTER_KEY, LangGraphEngine
from agentdeck.adapters.engines.openai_agents import ExecutionStore, OpenAIAgentsEngine
from agentdeck.adapters.engines.stub import StubEngine, stub_spec
from agentdeck.adapters.stores.memory import MemoryEventStore
from agentdeck.core.content import DataBlock, coerce_input
from agentdeck.core.context import RunContext
from agentdeck.core.events import (
    Event,
    ProgressReported,
    RunCompleted,
    StatusReported,
    Usage,
    check_contiguous,
    check_terminal,
)
from agentdeck.core.invocable import InvocableKind, InvocableSpec
from agentdeck.core.status import RunStatus, status_of
from agentdeck.runtime.service import Runtime
from agentdeck.surfaces.cli.chat import render

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Sequence

    from langchain_core.runnables import RunnableConfig

    from agentdeck.core.content import Input
    from agentdeck.core.events import KnownPayload

pytest.importorskip("fastapi")

TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
CTX = RunContext(tenant="acme", principal="user:1", run_id="r-1", trace_id="tr-1", session_id="s-1")


def _reports(events: Sequence[Event]) -> list[tuple[str, Any]]:
    """The reported events, as (kind, what a UI would show) — the shape assertions read on."""
    out: list[tuple[str, Any]] = []
    for event in events:
        if isinstance(event.payload, StatusReported):
            out.append(("status.reported", event.payload.message))
        elif isinstance(event.payload, ProgressReported):
            out.append(("progress.reported", (event.payload.step, event.payload.current, event.payload.total)))
    return out


# --- an openai-agents function tool ----------------------------------------------------

_USAGE = ResponseUsage(
    input_tokens=3,
    input_tokens_details=InputTokensDetails(cached_tokens=0),
    output_tokens=2,
    output_tokens_details=OutputTokensDetails(reasoning_tokens=0),
    total_tokens=5,
)


class _CallsTheToolOnce(Model):
    """Calls ``search_github`` on the first turn, answers once its result is in the tail."""

    async def stream_response(self, _instructions: Any = None, input: Any = None, *_a: Any, **_k: Any) -> AsyncIterator:
        last = input[-1] if input else None
        called = isinstance(last, dict) and last.get("type") == "function_call_output"
        output: list[Any] = (
            [
                ResponseOutputMessage(
                    id="msg_1",
                    content=[ResponseOutputText(annotations=[], text="two issues, both open", type="output_text")],
                    role="assistant",
                    status="completed",
                    type="message",
                )
            ]
            if called
            else [
                ResponseFunctionToolCall(
                    id="fc_1", call_id="call_1", name="search_github", arguments="{}", type="function_call"
                )
            ]
        )
        yield ResponseCompletedEvent(
            response=Response(
                id="resp_1",
                created_at=0.0,
                model="fake-reporting",
                object="response",
                output=output,
                parallel_tool_calls=False,
                tool_choice="auto",
                tools=[],
                usage=_USAGE,
            ),
            sequence_number=0,
            type="response.completed",
        )

    async def get_response(self, *_a: Any, **_k: Any) -> Any:
        raise NotImplementedError("this test only streams")


@function_tool
async def search_github(wrapper: RunContextWrapper[RunContext]) -> str:
    """Search GitHub, reporting what it is doing while it does."""
    await wrapper.context.reporter.status("Searching GitHub")
    await wrapper.context.reporter.progress("Reviewing issues", current=2, total=4)
    return "two open issues"


async def test_a_function_tool_reports_through_the_sdk_context() -> None:
    """A tool six frames inside the SDK, with no Runtime in sight: the run context arrives as
    the SDK's own context object, and the reporter on it is the whole reach."""
    agent = Agent(name="Searcher", instructions="use the tool", tools=[search_github], model=_CallsTheToolOnce())
    spec = InvocableSpec(name="Searcher", kind=InvocableKind.AGENT, engine=OpenAIAgentsEngine.engine, native=agent)
    store = MemoryEventStore()
    runtime = Runtime([OpenAIAgentsEngine(ExecutionStore())], store, {spec.name: spec})

    events = [event async for event in runtime.run("Searcher", coerce_input("what is open?"), CTX)]

    assert _reports(events) == [
        ("status.reported", "Searching GitHub"),
        ("progress.reported", ("Reviewing issues", 2, 4)),
    ]
    # The ceiling, asserted rather than only documented (``core/reporting.py``): the reports are
    # drained at the engine's *next* payload, and this SDK emits both of a tool call's item
    # events only once the tool has returned — so a report made inside the call surfaces just
    # ahead of ``tool.call.started``, not during the call. Ordered and inside the run either
    # way; if this list ever changes, the drain's granularity changed with it.
    assert [event.kind for event in events] == [
        "run.started",
        # One per finished model call, which is what makes two turns visible as two: the
        # terminal event's usage is the turn's cumulative total and cannot tell them apart.
        "usage.reported",
        "status.reported",
        "progress.reported",
        "tool.call.started",
        "tool.call.completed",
        "usage.reported",
        "message.completed",
        "run.completed",
    ]
    assert check_contiguous(events) == [] and check_terminal(events) is None
    assert await store.read(CTX.log_key, CTX) == events
    assert status_of(events) is RunStatus.COMPLETED


# --- a langgraph node ------------------------------------------------------------------


class _State(TypedDict, total=False):
    input: str
    reviewed: int


async def _review(state: _State, config: RunnableConfig) -> dict[str, Any]:
    """A node reporting mid-graph, reaching the reporter through langgraph's own config."""
    reporter = config["configurable"][REPORTER_KEY]
    await reporter.status("Reviewing issues")
    await reporter.progress("Reviewing issues", current=2, total=4)
    return {"reviewed": 2}


def _graph() -> StateGraph:
    graph = StateGraph(_State)
    graph.add_node("review", _review)
    graph.set_entry_point("review")
    graph.add_edge("review", END)
    return graph


async def test_a_workflow_node_reports_through_the_graph_config() -> None:
    spec = InvocableSpec(name="Reviewer", kind=InvocableKind.WORKFLOW, engine=LangGraphEngine.engine, native=_graph())
    store = MemoryEventStore()
    runtime = Runtime([LangGraphEngine()], store, {spec.name: spec})

    events = [event async for event in runtime.run("Reviewer", coerce_input("review 4412"), CTX)]

    assert _reports(events) == [
        ("status.reported", "Reviewing issues"),
        ("progress.reported", ("Reviewing issues", 2, 4)),
    ]
    # The node's own update and the graph's final state still arrive, unchanged by the reports.
    assert [event.kind for event in events if event.kind not in {"status.reported", "progress.reported"}] == [
        "run.started",
        "node.updated",
        "run.completed",
    ]
    final = events[-1].payload
    assert isinstance(final, RunCompleted)
    assert isinstance(final.output[0], DataBlock) and final.output[0].data["reviewed"] == 2
    assert check_contiguous(events) == [] and check_terminal(events) is None


# --- the SSE surface and the reference renderer ----------------------------------------


class _ReportingStub(StubEngine):
    """A scripted run that reports between its payloads — the surface must not care which
    engine did it, so the cheapest one is the honest choice here."""

    async def start(
        self, spec: InvocableSpec, input: Input, history: Sequence[Event], ctx: RunContext
    ) -> AsyncGenerator[KnownPayload, None]:
        await ctx.reporter.status("Searching GitHub")
        await ctx.reporter.progress("Reviewing issues", current=2, total=4)
        async for payload in super().start(spec, input, history, ctx):
            yield payload


def _runtime() -> Runtime:
    done = RunCompleted(output=coerce_input("two issues, both open"), usage=Usage(input_tokens=1, output_tokens=1))
    spec = stub_spec("Searcher", done)
    return Runtime([_ReportingStub()], MemoryEventStore(), {spec.name: spec})


async def test_an_sse_client_receives_the_reports_in_order() -> None:
    """The surface streams the canonical event and knows nothing about these kinds; a client
    reading frames in order is what "streamed clients receive ordered events" means."""
    from agentdeck.surfaces.serve.app import build_app

    app = build_app(_runtime())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v2/invocables/Searcher/chat", json={"session_id": "s-1", "message": "hi"})

    frames = [
        Event.model_validate(json.loads(line.removeprefix("data: ")))
        for line in response.text.splitlines()
        if line.startswith("data: ")
    ]
    assert [event.kind for event in frames] == [
        "run.started",
        "status.reported",
        "progress.reported",
        "run.completed",
    ]
    assert _reports(frames) == [
        ("status.reported", "Searching GitHub"),
        ("progress.reported", ("Reviewing issues", 2, 4)),
    ]


async def test_the_reference_renderer_prints_a_status_and_a_counted_stage(capsys) -> None:
    async def lines() -> AsyncIterator[str]:
        yield f"data: {_event(StatusReported(message='Searching GitHub')).model_dump_json()}"
        yield f"data: {_event(ProgressReported(step='Reviewing issues', current=2, total=4)).model_dump_json()}"
        yield f"data: {_event(ProgressReported(step='Wrapping up')).model_dump_json()}"
        yield f"data: {_event(ProgressReported(step='Halfway', current=2)).model_dump_json()}"
        # A kind this renderer has never heard of must not stop it — the default case is the
        # forward-compatibility promise, and a new kind is exactly when it gets tested.
        yield 'data: {"v": 1, "kind": "future.thing", "seq": 4, "run_id": "r-1", "session_id": null, "tenant": "acme", "origin": "Searcher", "ts": "2026-01-01T12:00:00Z", "payload": {"whatever": 1}}'

    await render(lines())

    assert capsys.readouterr().out.splitlines() == [
        "[status] Searching GitHub",
        "[progress] Reviewing issues (2/4)",
        "[progress] Wrapping up",
        "[progress] Halfway (2/?)",
    ]


def _event(payload: KnownPayload) -> Event:
    return Event(
        kind=payload.kind,
        seq=0,
        run_id="r-1",
        session_id="s-1",
        tenant="acme",
        origin="Searcher",
        ts=TS,
        payload=payload,
    )
