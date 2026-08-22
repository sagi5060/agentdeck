"""An imperative ``@workflow``: ordinary Python that suspends where it stands.

The property every case here is really about is the one a graph cannot have: the body keeps its
locals across a suspension. A workflow that asks a person a question and is answered an hour
later continues on the *next line*, having run everything before it exactly once  -  which is what
makes it Python rather than a state machine wearing Python's syntax.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agentdeck import Deck, ToolCtx, WorkflowCtx, tool, workflow
from agentdeck.core.control import CONTROL_POLL_INTERVAL
from agentdeck.core.status import RunStatus
from agentdeck.errors import ConfigError


@pytest.fixture(autouse=True)
def _no_project(tmp_path, monkeypatch):
    """A cwd with no ``.agentdeck``: every catalog here is code-first."""
    monkeypatch.chdir(tmp_path)


@workflow
async def echo(ctx: WorkflowCtx, message: str) -> str:
    """Hand back what it was given."""
    return f"echo:{message}"


@workflow
async def joined(ctx: WorkflowCtx, left: str, right: str) -> str:
    return f"{left}+{right}"


@workflow
async def whole(ctx: WorkflowCtx, payload: dict[str, Any]) -> list[str]:
    return sorted(payload)


@workflow
async def boom(ctx: WorkflowCtx) -> None:
    raise ZeroDivisionError("the body raised")


# --- what it returns, and how its input reaches it ---------------------------------------


async def test_a_native_workflow_runs_and_returns_its_value() -> None:
    async with Deck(workflows=[echo]) as deck:
        assert await deck.run("echo", "hi") == "echo:hi"


async def test_one_parameter_takes_the_input_whole() -> None:
    """Binding is by signature, so a single parameter is a single value  -  a mapping included.
    A body that declares one argument must not have a dict silently spread across it."""
    async with Deck(workflows=[whole]) as deck:
        assert await deck.run("whole", {"b": 2, "a": 1}) == ["a", "b"]


async def test_several_parameters_bind_by_name() -> None:
    async with Deck(workflows=[joined]) as deck:
        assert await deck.run("joined", {"left": "a", "right": "b"}) == "a+b"


async def test_a_mismatched_input_says_what_the_body_wanted() -> None:
    async with Deck(workflows=[joined]) as deck:
        with pytest.raises(ConfigError, match="left, right"):
            await deck.run("joined", "just a string")


async def test_an_input_key_may_not_name_the_context_parameter() -> None:
    """The context is AgentDeck's to fill; a mapping key of the same name is refused rather than
    silently overwriting the injected ``WorkflowCtx``."""
    async with Deck(workflows=[joined]) as deck:
        with pytest.raises(ConfigError, match="ctx"):
            await deck.run("joined", {"left": "a", "right": "b", "ctx": "not a context"})


async def test_an_unknown_input_key_is_refused_by_name() -> None:
    async with Deck(workflows=[joined]) as deck:
        with pytest.raises(ConfigError, match="left, right"):
            await deck.run("joined", {"left": "a", "middle": "b"})


async def test_a_body_that_raises_fails_the_run_and_the_caller_sees_why() -> None:
    """The body's own exception travels, exactly as an engine's does: the run is ``FAILED`` in
    the log, and the caller who was awaiting it gets the traceback rather than a ``None``."""
    async with Deck(workflows=[boom]) as deck:
        run = await deck.runs.start("boom", None)
        with pytest.raises(ZeroDivisionError, match="the body raised"):
            await run
        assert await run.status() is RunStatus.FAILED
        # The record carries the type only: a message can hold whatever the body was working on.
        failure = [event async for event in run.events()][-1]
        assert failure.kind == "run.failed"
        assert "ZeroDivisionError" in failure.payload.message


# --- suspension keeps the body alive ------------------------------------------------------


async def test_an_answer_continues_the_next_line_rather_than_replaying() -> None:
    """The whole point. A graph re-enters its node from the top on resume; this body ran its
    first half once, and the answer lands where it stopped."""
    ran: list[str] = []

    @workflow
    async def survey(ctx: WorkflowCtx, topic: str) -> str:
        ran.append("before")
        answer = await ctx.ask(f"how about {topic}?")
        ran.append("after")
        return f"{topic}:{answer}"

    async with Deck(workflows=[survey]) as deck:
        run = await deck.runs.start("survey", "kites", session_id="s-1")
        await _settles(run, RunStatus.WAITING_ANSWER)

        assert ran == ["before"]
        pending = await run.pending()
        assert pending is not None
        assert pending["payload"]["question"] == "how about kites?"

        await run.answer("good")

        assert ran == ["before", "after"]
        assert await run == "kites:good"


async def test_a_pause_parks_the_body_and_a_resume_carries_on() -> None:
    """A pause is not a replay: the loop that had done two turns does three more, not five."""
    turns: list[int] = []
    holding = asyncio.Event()
    paused = asyncio.Event()

    @workflow
    async def counting(ctx: WorkflowCtx, total: int) -> list[int]:
        for turn in range(total):
            if turn == 2:
                holding.set()
                await paused.wait()
            await ctx.safepoint()
            turns.append(turn)
        return list(turns)

    async with Deck(workflows=[counting]) as deck:
        run = await deck.runs.start("counting", 5, session_id="s-2")
        await holding.wait()
        await run.pause("operator stepped away")
        # The gate reuses its last answer for one interval, so a body that just polled would run
        # past a pause recorded inside it. Cooperative control, up to one interval late.
        await asyncio.sleep(CONTROL_POLL_INTERVAL)
        paused.set()
        await _settles(run, RunStatus.PAUSED)

        assert turns == [0, 1]
        assert run.can.resume and not run.can.pause

        await run.resume()
        assert await run == [0, 1, 2, 3, 4]


async def test_a_consumer_that_disconnects_between_a_pauses_two_plain_payloads_still_gets_reaped() -> None:
    """A pause is three payloads (``control.requested``, ``control.observed``, ``run.paused``),
    and only the last is a suspending one. A consumer that closes the stream right after the
    first must not orphan the body: it has to stay reachable for aclose() regardless."""
    from agentdeck.adapters.control.memory import MemoryControlPort
    from agentdeck.adapters.executors.native.executor import NativeExecutor
    from agentdeck.core.context import RunContext
    from agentdeck.core.control import Gate, Signal
    from agentdeck.core.invocable import InvocableKind, InvocableSpec

    run_id = "r-disconnect"
    port = MemoryControlPort()
    await port.signal(run_id, Signal.PAUSE, "operator stepped away")

    @workflow
    async def parking(ctx: WorkflowCtx) -> str:
        await ctx.safepoint()
        return "never"  # pragma: no cover  -  the pause never lets this line run

    ctx = RunContext(run_id=run_id, gate=Gate(control=port, id=run_id, poll_interval=0))
    spec = InvocableSpec(name="parking", kind=InvocableKind.WORKFLOW, executor="native", native=parking)
    executor = NativeExecutor()

    stream = executor.execute(spec, [], [], ctx)
    first = await anext(stream)
    assert first.kind == "control.requested"
    await stream.aclose()

    assert run_id in executor._parked
    await asyncio.wait_for(executor.aclose(), timeout=1)
    assert executor._parked == {}


async def test_an_answer_outside_the_options_is_refused_and_the_run_stays_answerable() -> None:
    """A question with options is the only kind AgentDeck can judge, and judging it before the
    claim is what keeps a mistyped reply from ending the run: the answerer is told, the run is
    still waiting, and the next answer lands."""
    decided: list[Any] = []

    @workflow
    async def gated(ctx: WorkflowCtx) -> str:
        decided.append(await ctx.ask("ship it?", options=[True, False]))
        return "done"

    async with Deck(workflows=[gated]) as deck:
        run = await deck.runs.start("gated", None)
        await _settles(run, RunStatus.WAITING_ANSWER)

        # The options travel on the interrupt, so a surface listing pending runs can render them.
        pending = await run.pending()
        assert pending is not None
        assert pending["payload"]["options"] == [True, False]

        with pytest.raises(ValueError, match="waiting for one of"):
            await run.answer("maybe")

        assert await run.status() is RunStatus.WAITING_ANSWER
        assert decided == []
        # Recorded, so an audit sees the attempt. Not a lifecycle event: the run did not move.
        assert [event.kind async for event in run.events()][-1] == "answer.refused"

        await run.answer(False)
        assert await run == "done"
        assert decided == [False]


async def test_a_question_with_no_options_takes_whatever_it_is_given() -> None:
    """The other half of the rule: nothing here can judge a free-form answer better than the body
    can, so nothing tries."""
    seen: list[Any] = []

    @workflow
    async def freeform(ctx: WorkflowCtx) -> str:
        seen.append(await ctx.ask("what is the plan?"))
        return "noted"

    async with Deck(workflows=[freeform]) as deck:
        run = await deck.runs.start("freeform", None)
        await _settles(run, RunStatus.WAITING_ANSWER)
        await run.answer({"plan": "go left", "confidence": 0.4})

        assert await run == "noted"
        assert seen == [{"plan": "go left", "confidence": 0.4}]


async def test_a_cancel_ends_the_run_rather_than_parking_it() -> None:
    """The other half of the parking rule: there is nothing to come back to, so the body unwinds
    and the run is over."""
    reached: list[str] = []
    holding = asyncio.Event()
    cancelled = asyncio.Event()

    @workflow
    async def looping(ctx: WorkflowCtx) -> str:
        holding.set()
        await cancelled.wait()
        await ctx.safepoint()
        reached.append("past the safepoint")
        return "finished"

    async with Deck(workflows=[looping]) as deck:
        run = await deck.runs.start("looping", None)
        await holding.wait()
        await run.cancel("operator said stop")
        cancelled.set()
        await _settles(run, RunStatus.CANCELLED)

        assert reached == []
        assert [event.kind async for event in run.events()][-1] == "run.cancelled"


# --- the contract the decorators check ----------------------------------------------------


def test_a_tool_may_not_ask_for_the_workflow_context() -> None:
    """The rule the two decorators exist for: a tool that can suspend a person for an answer is
    no longer a leaf capability."""
    with pytest.raises(ConfigError, match="asks for WorkflowCtx"):

        @tool
        async def sneaky(ctx: WorkflowCtx, query: str) -> str:  # pragma: no cover  -  never built
            return query


def test_a_workflow_may_not_ask_for_the_tool_context() -> None:
    with pytest.raises(ConfigError, match="asks for ToolCtx"):

        @workflow
        async def narrow(ctx: ToolCtx, topic: str) -> str:  # pragma: no cover  -  never built
            return topic


def test_a_native_definition_has_to_be_async() -> None:
    """A blocking body would stall the loop every other run on this deck shares."""
    with pytest.raises(ConfigError, match="not async"):

        @workflow
        def blocking(ctx: WorkflowCtx) -> str:  # pragma: no cover  -  never built
            return "no"


async def test_an_agent_can_use_a_native_tool() -> None:
    """A ``@tool`` is still a tool: the agent compiles the function underneath, so declaring one
    costs nothing on the path that existed before it."""
    from agentdeck import Agent

    @tool
    async def lookup(ctx: ToolCtx, sku: str) -> str:
        """Look a SKU up."""
        return f"{sku} is in stock"

    deck = Deck(agents=[Agent(name="Shop", instructions="Answer with the tool.", tools=[lookup])])
    try:
        deck.build()
        assert [compiled.name for compiled in deck._invocables["Shop"].native.tools] == ["lookup"]
    finally:
        Deck._release()


def test_a_definition_takes_its_name_and_description_from_the_function() -> None:
    assert echo.name == "echo"
    assert echo.description == "Hand back what it was given."


async def test_a_parked_body_this_process_lost_says_so() -> None:
    """The declared ceiling: a parked body is a coroutine in memory, so a restart loses it.
    Answering such a run must say that rather than silently replaying the workflow."""
    ran: list[str] = []

    @workflow
    async def asking(ctx: WorkflowCtx) -> str:
        ran.append("ran")
        return str(await ctx.ask("still there?"))

    async with Deck(workflows=[asking]) as deck:
        run = await deck.runs.start("asking", None)
        await _settles(run, RunStatus.WAITING_ANSWER)
        # What a restart leaves behind: the log still says WAITING_ANSWER, the body is gone.
        executor = next(e for e in deck._executor_instances or () if e.name == "native")
        executor._parked.clear()

        with pytest.raises(ConfigError, match="no longer holds"):
            await run.answer("yes")
        assert ran == ["ran"]


async def test_a_second_concurrent_ask_is_refused_rather_than_orphaning_the_first() -> None:
    """agentdeck #414: one run parks on one payload at a time, so a second ``ask`` raced against
    the first used to overwrite its future and leave that branch waiting forever. ``ctx.parallel``
    refuses this at the call; a raw ``asyncio.gather`` reaches ``suspend`` itself, so the channel
    refuses it there. The refusal travels with the body, which is why it surfaces at the answer."""

    @workflow
    async def two_questions(ctx: WorkflowCtx) -> list[str]:
        async def branch(question: str) -> str:
            return str(await ctx.ask(question))

        return list(await asyncio.gather(branch("a?"), branch("b?")))

    async with Deck(workflows=[two_questions]) as deck:
        run = await deck.runs.start("two_questions", None)
        await _settles(run, RunStatus.WAITING_ANSWER)

        with pytest.raises(ConfigError, match="one payload at a time") as exc_info:
            await asyncio.wait_for(run.answer("yes"), timeout=5)
        message = str(exc_info.value)
        assert "'a?'" in message
        assert "'b?'" in message
        assert await run.status() is RunStatus.FAILED


async def _settles(run: Any, status: RunStatus) -> None:
    """Wait for the run to reach ``status``. The body runs in its own task, so a test that
    asserted immediately would be racing it."""
    for _ in range(200):
        if await run.status() is status:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"run {run.id} never reached {status}, last seen {await run.status()}")
