"""The openai-agents engine: ``EnginePort`` over ``agents.Runner``.

``spec.native`` is the pre-built ``agents.Agent`` (handoffs and tools included) — this
adapter only runs it and translates its stream, per ``core/ports/engine.py``. Execution
state (the SDK session) is engine-private (ADR-D5): the session, not the log, is what
feeds the model. The log passed in as ``history`` is read for exactly one purpose — the
turn-start reconciliation in ``reconcile.py``, which repairs a session left behind by a
crash between the log write and the session write.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, aclosing, asynccontextmanager, nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, cast

from agents import Agent, Runner
from pydantic import BaseModel

from agentdeck.adapters.engines.openai_agents.reconcile import reconcile
from agentdeck.adapters.engines.openai_agents.runconfig import RunSettings, build_run_config
from agentdeck.adapters.engines.openai_agents.sessions import ExecutionStore
from agentdeck.adapters.engines.openai_agents.translate import translate
from agentdeck.core.content import DataBlock, TextBlock, coerce_input
from agentdeck.core.control import ControlSignalled
from agentdeck.core.events import Custom, RunCompleted, Usage, UsageReported
from agentdeck.core.ports import EnginePort
from agentdeck.errors import ConfigError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Sequence

    from agents.memory.session import Session
    from agents.result import RunResultStreaming
    from agents.usage import Usage as SDKUsage

    from agentdeck.core.content import Input
    from agentdeck.core.context import RunContext
    from agentdeck.core.events import Event, KnownPayload
    from agentdeck.core.invocable import InvocableSpec

STRUCTURED_OUTPUT = "openai_agents.structured_output"
"""Where an ``output_type`` agent's validated result travels *in addition to*
``RunCompleted.output``.

Redundant by construction — the same object rides the terminal event as a ``DataBlock`` — and
kept anyway, because ``surfaces/serve/compat.py`` reads this event to build v1's ``done``
frame. Retiring it moves that wire, which is a change of its own rather than a side effect
of this one (D10: an engine namespaces a ``custom`` event, it never mints a kind)."""

SandboxScope = Callable[[Agent[Any]], AbstractAsyncContextManager[Any]]
"""How this engine opens whatever sandbox an agent needs: given the agent, a scope yielding
the SDK ``sandbox`` handle for its run (or ``None``).

Injected rather than built here because a sandbox is a capability, not an engine concern —
it becomes a port of its own in the next slice. Unset means no agent in this project needs
one, which is every code-first caller until it says otherwise."""


@dataclass(slots=True)
class Launch:
    """One run's SDK handle, plus whether this engine reached its terminal payload.

    ``finished`` exists because nothing on the SDK result can answer that question at the
    moment it is asked: a run abandoned mid-stream and a run that ended normally both arrive
    at ``_launch``'s exit already cancelled, so ``is_complete`` is true either way, and
    ``final_output`` is only *usually* absent from the abandoned one (the SDK's run loop is
    detached, so it may well have finished while nobody was reading). The engine's own control
    flow is the authority, and this is how it says so.

    It is the *engine's* view, not the log's, and cannot be made the log's: it is set before the
    terminal payload is yielded, because the Runtime breaks on that payload and never returns
    here. So a store that rejects the terminal append leaves this ``True`` while the log ends in
    ``run.failed`` — an observability span reporting success for a run the log calls failed. The
    log is the record; a reader reconciling the two believes the log.
    """

    result: RunResultStreaming
    finished: bool = False


class OpenAIAgentsEngine(EnginePort):
    """Plays ``spec.native`` (an ``agents.Agent``) through ``Runner.run_streamed``.

    Everything a run is configured with arrives here already resolved — ``sessions`` is the
    conversation memory (Redis-backed or local), ``settings`` the endpoint and limits, and
    ``sandbox`` the scope an agent that needs one runs inside. All three default to the
    SDK's own behavior, so ``OpenAIAgentsEngine()`` still runs an agent that configured
    itself.
    """

    engine: ClassVar[str] = "openai-agents"

    def __init__(
        self,
        sessions: ExecutionStore | None = None,
        *,
        settings: RunSettings | None = None,
        sandbox: SandboxScope | None = None,
    ) -> None:
        self._sessions = sessions or ExecutionStore()
        self._settings = settings or RunSettings()
        self._sandbox = sandbox

    async def start(
        self,
        spec: InvocableSpec,
        input: Input,
        history: Sequence[Event],
        ctx: RunContext,
    ) -> AsyncGenerator[KnownPayload, None]:
        agent = _agent_of(spec)
        session = self._session(ctx)
        if session is not None:
            diverged = await reconcile(session, history)
            if diverged is not None:
                # Two stores disagreeing is worth a place in the record, not just a log line;
                # the run itself still has the session it needs and plays on.
                yield diverged
        async with self._launch(agent, _to_sdk_input(input), ctx, session) as launch:
            result = launch.result
            tool_names: dict[str, str] = {}
            # The SDK's run loop is a detached task; an abandoned generator must cancel it
            # explicitly (mirrors agents/runners/headless.py's run_streamed, same reason).
            stream = cast("AsyncGenerator[Any, None]", result.stream_events())
            try:
                async with aclosing(stream) as events:
                    async for event in events:
                        payload = self._translate(event, tool_names)
                        if payload is not None:
                            yield payload
                        try:
                            await ctx.gate.checkpoint()
                        except ControlSignalled as signalled:
                            # A complete chunk was just yielded (or none was, at the very
                            # first safe point) — never a partial one — so this is the next
                            # safe point the contract promises, not "right now, mid-token".
                            # The SDK run is dropped either way: a paused turn has no
                            # checkpoint to sit in, so resuming replays it from the log.
                            result.cancel()
                            for payload in signalled.payloads:
                                yield payload
                            return
            except BaseException:
                result.cancel()
                raise
            result.cancel()
            terminal = self._terminal(result)
            # Set before the yields, not after them: the Runtime breaks on the terminal event,
            # so the line after this loop never runs.
            launch.finished = True
            for payload in terminal:
                yield payload

    def _session(self, ctx: RunContext) -> Session | None:
        """The execution state this run reads and writes — the adapter's own store by default."""
        return self._sessions.session_for(ctx)

    @asynccontextmanager
    async def _launch(
        self, agent: Agent[Any], message: str, ctx: RunContext, session: Session | None
    ) -> AsyncIterator[Launch]:
        """Start the run and hold whatever scope it needs open until the stream is drained.

        Lifecycle rule: **code after the ``yield`` may never run.** A successful run ends
        with the Runtime breaking on the terminal event, which closes this generator — the
        ``yield`` raises ``GeneratorExit`` and the lines below it are skipped. Anything that
        must happen once per finished run therefore belongs in the ``GeneratorExit`` path,
        keyed on ``Launch.finished``, never only after the ``yield``.
        """
        scope = self._sandbox(agent) if self._sandbox is not None else nullcontext(None)
        async with scope as sandbox:
            yield Launch(
                Runner.run_streamed(
                    agent,
                    message,
                    # The run context travels as the SDK's own context object, which is the one thing
                    # the SDK hands a function tool: a tool declaring ``RunContextWrapper[RunContext]``
                    # reaches ``wrapper.context.reporter`` (and the gate) without importing a Runtime.
                    # Nothing in the SDK reads it — it is opaque to the run loop by design.
                    context=ctx,
                    session=session,
                    run_config=build_run_config(self._settings, sandbox=sandbox),
                    max_turns=self._settings.max_turns,
                )
            )

    def _translate(self, event: Any, tool_names: dict[str, str]) -> KnownPayload | None:
        payload = translate(event, tool_names)
        return payload if payload is not None else _usage_reported(event)

    def _terminal(self, result: RunResultStreaming) -> Sequence[KnownPayload]:
        completed = _run_completed(result)
        structured = [block.data for block in completed.output if isinstance(block, DataBlock)]
        if not structured:
            return (completed,)
        return (Custom(name=STRUCTURED_OUTPUT, data={"output": structured[0]}), completed)

    async def resume(
        self,
        spec: InvocableSpec,
        thread_id: str,
        value: Any,
        ctx: RunContext,
    ) -> AsyncGenerator[KnownPayload, None]:
        # M0 scope is UC1's plain chat, which never suspends — there is no interrupted run
        # for this engine to continue. Raising (not a silent no-op) matches the Runtime's
        # own rule that this method is only ever called on a WAITING_HUMAN run.
        raise ConfigError(f"openai-agents engine (M0) has no interrupts to resume: {spec.name!r} never suspends")
        yield  # pragma: no cover — makes this an async generator; never reached


def _usage_reported(event: Any) -> KnownPayload | None:
    """One finished model call → one ``usage.reported``.

    The terminal event's ``usage`` is the SDK's cumulative total for the turn, so without
    this a consumer cannot tell one model call from four — which is exactly what v1's
    ``usage.requests`` counted.
    """
    if event.type != "raw_response_event" or getattr(event.data, "type", None) != "response.completed":
        return None
    response = event.data.response
    usage = getattr(response, "usage", None)
    if usage is None:
        return None
    return UsageReported(
        model=str(getattr(response, "model", "") or ""),
        usage=Usage(input_tokens=usage.input_tokens, output_tokens=usage.output_tokens),
    )


def _agent_of(spec: InvocableSpec) -> Agent[Any]:
    if not isinstance(spec.native, Agent):
        raise ConfigError(f"{spec.name!r} has no openai-agents Agent: expected native=Agent, got {type(spec.native)}")
    return spec.native


def _to_sdk_input(input: Input) -> str:
    # M0 scope is plain-text chat; images/resources are a follow-up, not a silent
    # drop — better to raise now than answer a question the model never saw.
    texts = [block.text for block in input if isinstance(block, TextBlock)]
    if len(texts) != len(input):
        raise ConfigError("openai-agents engine (M0) only supports text input blocks")
    return "\n".join(texts)


def _run_completed(result: RunResultStreaming) -> RunCompleted:
    output = result.final_output
    if isinstance(output, str):
        return RunCompleted(output=coerce_input(output), usage=_usage_of(result))
    return RunCompleted(output=[DataBlock(data=_structured(output))], usage=_usage_of(result))


def _structured(output: Any) -> Any:
    """An ``output_type`` agent's validated result as JSON data.

    It travels as a ``DataBlock``, which is why this no longer raises: refusing a non-``str``
    final output turned a documented feature into a failed run. The ceiling, and it applies to
    every branch below: a leaf JSON cannot carry becomes its ``str()`` — a non-finite float
    included, since ``null`` would claim it was absent — rather than failing the run at its
    last event.
    """
    if isinstance(output, BaseModel):
        try:
            output = output.model_dump(mode="json")
        except ValueError:
            # PydanticSerializationError, which is a ValueError: one leaf pydantic cannot
            # render as JSON. The python dump keeps the rest and the net below takes that
            # leaf, so only its fidelity is lost — not the whole run's terminal event.
            output = output.model_dump()
    elif dataclasses.is_dataclass(output) and not isinstance(output, type):
        output = dataclasses.asdict(output)
    return json.loads(json.dumps(output, default=str), parse_constant=str)


def _usage_of(result: RunResultStreaming) -> Usage:
    usage: SDKUsage | None = getattr(result.context_wrapper, "usage", None)
    if usage is None:
        return Usage(input_tokens=0, output_tokens=0)
    return Usage(input_tokens=usage.input_tokens, output_tokens=usage.output_tokens)


__all__ = ["STRUCTURED_OUTPUT", "Launch", "OpenAIAgentsEngine", "SandboxScope"]
