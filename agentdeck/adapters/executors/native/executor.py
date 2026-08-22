"""The executor for AgentDeck's own definitions: a coroutine, and a channel out of it.

Everything else in ``adapters/executors/`` bridges somebody else's runtime. This one has none to
bridge  -  a native ``@workflow`` is a Python function  -  so it owns the two things a function
cannot do for itself: turn what the body says into payloads on the run, and stop the body in
place until somebody answers it.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from agentdeck.core.content import DataBlock, TextBlock, answer_of
from agentdeck.core.context import WorkflowCtx
from agentdeck.core.control import ControlSignalled
from agentdeck.core.events import RunCompleted, Usage
from agentdeck.core.invocable import NativeInvocable
from agentdeck.core.ports import Executor
from agentdeck.core.status import SUSPENDED_KINDS, Play, continuation_of
from agentdeck.errors import ConfigError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from agentdeck.core.content import Input
    from agentdeck.core.context import Agents, Invoker, RunContext
    from agentdeck.core.events import Event, KnownPayload
    from agentdeck.core.invocable import InvocableSpec
    from agentdeck.core.status import Continuation

# The body's own ``finally`` puts ``None`` on the channel: it is over, whichever way it ended.
# A payload is never ``None``, so the two cannot be confused.


class _Channel:
    """The one way a running body reaches its run: payloads out, one answer back.

    A queue rather than a callback, because the body and the generator draining it are two tasks:
    the body may produce several payloads between two reads, and must never block on whether
    anybody is reading.
    """

    def __init__(self) -> None:
        self._out: asyncio.Queue[KnownPayload | None] = asyncio.Queue()
        self._answer: asyncio.Future[Any] | None = None
        # Held alongside the future only so the refusal below can name what already owns the slot.
        self._suspended_on: KnownPayload | None = None

    async def emit(self, payload: KnownPayload) -> None:
        await self._out.put(payload)

    async def suspend(self, payload: KnownPayload) -> Any:
        """Hand out the payload that suspends the run, then wait here for what answers it."""
        if (parked := self._suspended_on) is not None:
            # Assigning over a live slot is what #414 was: the branch already waiting kept waiting
            # on a future nothing could complete. This leaves that future alone, so the run stays
            # answerable and the refusal travels out with the body instead.
            raise ConfigError(
                f"this run is already suspended on {_named(parked)}, so "
                f"{_named(payload)} cannot suspend it too: one run parks on one payload at a time, "
                f"and a second would replace the first and never be answered (agentdeck #414). "
                f"Suspend in sequence, or give each one a child run of its own."
            )
        self._suspended_on = payload
        self._answer = asyncio.get_running_loop().create_future()
        await self._out.put(payload)
        return await self._answer

    async def next(self) -> KnownPayload | None:
        """The next payload, or ``None`` once the body has ended."""
        return await self._out.get()

    async def close(self) -> None:
        await self._out.put(None)

    def wake(self, value: Any) -> None:
        """Give the parked body its answer. ``None`` is what a lifted pause carries."""
        answer, self._answer, self._suspended_on = self._answer, None, None
        if answer is None or answer.done():
            raise ConfigError("this run is not parked on anything: nothing is waiting to be answered.")
        answer.set_result(value)


def _named(payload: KnownPayload) -> str:
    """How the refusal above names a suspending payload: its question, or else its kind.

    ``ctx.safepoint()`` parks on the same slot as ``ctx.ask()``, so this cannot assume a question
    is there to quote.
    """
    question = getattr(payload, "payload", {}).get("question")
    return repr(question) if question else f"a {payload.kind} payload"


@dataclass(slots=True)
class _Body:
    """One run's live coroutine and the channel it talks through."""

    task: asyncio.Task[None]
    channel: _Channel = field(default_factory=_Channel)


class NativeExecutor(Executor):
    """Plays a ``@workflow`` (or a ``@tool``) as the coroutine it is.

    **Suspension parks, it does not unwind.** An agent turn is re-entered from
    a checkpoint, so raising through it costs nothing. An imperative body has no checkpoint: its
    locals *are* the workflow, so a pause keeps the coroutine alive and waiting instead
    (``docs/design/execution-api.md``). A cancel still raises, because the run is over and there
    is nothing left to preserve.

    The ceiling that comes with it: a parked body lives in this process and this executor
    instance. Surviving a restart is the durable replay model, which is deferred  -  so a resume
    that finds no parked body says exactly that rather than silently replaying the workflow.
    """

    name: ClassVar[str] = "native"
    suspendable: ClassVar[bool] = True

    def __init__(self, invoker: Invoker | None = None, agents: Agents | None = None) -> None:
        # Keyed by run id, because that is what a resume names and what a parked body belongs to.
        self._parked: dict[str, _Body] = {}
        # The two things this executor cannot do for a workflow: start another run, and add to
        # what can be started. Both belong to whoever holds the catalog, so both are handed in
        # rather than reached for.
        self._invoker = invoker
        self._agents = agents

    async def execute(
        self,
        spec: InvocableSpec,
        input: Input,
        history: Sequence[Event],
        ctx: RunContext,
    ) -> AsyncGenerator[KnownPayload, None]:
        continuation = continuation_of(history, ctx.run_id)
        body = self._begin(spec, input, ctx) if continuation.play is Play.FRESH else self._wake(ctx, continuation)
        # Registered from here, not from the first suspending yield: a pause is three payloads,
        # and a consumer that walks away between the first and the last must still leave this
        # body reachable for aclose() to cancel  -  it is already alive and holding its locals.
        self._parked[ctx.run_id] = body
        while True:
            payload = await body.channel.next()
            if payload is None:
                self._parked.pop(ctx.run_id, None)
                # The body's own exception, if it had one: the Runtime records ``run.failed`` and
                # hands it to the caller, exactly as it does for every other executor. Catching
                # it here would leave a workflow that raised looking like one that returned None.
                await body.task
                return
            if payload.kind in SUSPENDED_KINDS:
                # Never awaited here: awaiting it is what a suspension is not.
                yield payload
                return
            yield payload

    async def aclose(self) -> None:
        """Cancel every body still parked. A workflow waiting for an answer nobody will now give
        is over when the deck that started it is: leaving the coroutine suspended would outlive
        the loop it was created on."""
        parked, self._parked = list(self._parked.values()), {}
        for body in parked:
            body.task.cancel()
        for body in parked:
            with suppress(asyncio.CancelledError):
                await body.task

    def _begin(self, spec: InvocableSpec, input: Input, ctx: RunContext) -> _Body:
        definition = _definition_of(spec)
        channel = _Channel()
        body = _Body(task=asyncio.create_task(self._play(definition, input, ctx, channel)), channel=channel)
        return body

    def _wake(self, ctx: RunContext, continuation: Continuation) -> _Body:
        body = self._parked.pop(ctx.run_id, None)
        if body is None:
            raise ConfigError(
                f"run {ctx.run_id!r} was suspended by a native workflow whose body this process no "
                f"longer holds  -  it was parked in memory, and a restart (or a second worker) loses "
                f"it. Durable replay of an imperative workflow is not built yet: keep the process "
                f"that started the run alive until it is answered, or model the step as a durable "
                f"workflow  -  see docs/design/execution-api.md."
            )
        body.channel.wake(continuation.answer)
        return body

    async def _play(self, definition: NativeInvocable, input: Input, ctx: RunContext, channel: _Channel) -> None:
        """Run the body to its end, and put whatever that end was on the channel.

        Two exits are payloads: a return is ``run.completed``, and a signal honored at a safepoint
        is its own three. Anything else raised is left to travel  -  the task keeps it, and
        :meth:`execute` re-raises it into the run so the Runtime records and reports it the way
        it does for every other executor.
        """
        try:
            result = await definition.call(**_arguments(definition, input, ctx, channel, self._invoker, self._agents))
            await channel.emit(RunCompleted(output=_as_output(result), usage=Usage(input_tokens=0, output_tokens=0)))
        except ControlSignalled as signalled:
            for payload in signalled.payloads:
                await channel.emit(payload)
        finally:
            await channel.close()


def _definition_of(spec: InvocableSpec) -> NativeInvocable:
    if not isinstance(spec.native, NativeInvocable):
        raise ConfigError(
            f"{spec.name!r} is registered on the native executor but its native payload is a "
            f"{type(spec.native).__name__}; only a @tool or @workflow definition can be played here."
        )
    return spec.native


def _arguments(
    definition: NativeInvocable,
    input: Input,
    ctx: RunContext,
    channel: _Channel,
    invoker: Invoker | None,
    agents: Agents | None,
) -> dict[str, Any]:
    """Bind the run's input to the body's own parameters, the way calling it would.

    One value arrives from outside (a run is started with an input, not an argument list), so a
    mapping binds by name and anything else binds to the single parameter the body declares. A
    body whose one parameter is itself a mapping therefore takes it whole: binding is by
    signature, and one parameter is one value (``docs/design/execution-api.md``).
    """
    arguments: dict[str, Any] = {}
    if definition.context_parameter is not None:
        arguments[definition.context_parameter] = _context_for(definition, ctx, channel, invoker, agents)
    visible = definition.parameters
    if not visible:
        return arguments
    value = answer_of(input)
    if len(visible) == 1:
        return arguments | {visible[0]: value}
    if isinstance(value, dict):
        if definition.context_parameter is not None and definition.context_parameter in value:
            raise ConfigError(
                f"{definition.kind.value} {definition.name!r} declares {definition.context_parameter!r} "
                f"as its context parameter, which AgentDeck injects; the input mapping cannot also name "
                f"it. Rename the {definition.context_parameter!r} key in the input, or the parameter."
            )
        if set(value) != set(visible):
            raise ConfigError(
                f"{definition.kind.value} {definition.name!r} takes {len(visible)} arguments "
                f"({', '.join(visible)}), so its input mapping must name exactly those; got "
                f"({', '.join(sorted(value))})."
            )
        return arguments | {name: value[name] for name in visible}
    raise ConfigError(
        f"{definition.kind.value} {definition.name!r} takes {len(visible)} arguments "
        f"({', '.join(visible)}), so its input has to be a mapping naming them; got "
        f"{type(value).__name__}."
    )


def _context_for(
    definition: NativeInvocable,
    ctx: RunContext,
    channel: _Channel,
    invoker: Invoker | None,
    agents: Agents | None,
) -> Any:
    """The context the body declared, holding the channel it can stop on.

    Which class it is was settled by the decorator; both get the channel, because a body this
    executor is playing can always be parked  -  a tool's ``safepoint`` waits here exactly as a
    workflow's does, and only a tool played inside somebody else's turn has to unwind instead.
    Only a workflow gets the invoker and the agent mint: a tool that could start another run,
    or add one to the catalog, is no longer a leaf.
    """
    context_class = definition.context_class
    if context_class is None:
        return None
    if issubclass(context_class, WorkflowCtx):
        return context_class(ctx, channel, invoker, agents)
    return context_class(ctx, channel)


def _as_output(result: Any) -> Input:
    """A body's return value as the content ``run.completed`` carries.

    A value, so a data block  -  a string included, because what a workflow returns is its result
    and not a message to a person. Content a body built itself is the one exception, and passes
    through as the blocks it already is.
    """
    if isinstance(result, list) and result:
        blocks: Input = [block for block in result if isinstance(block, TextBlock | DataBlock)]
        if len(blocks) == len(result):
            return blocks
    return [DataBlock(data=result)]


__all__ = ["NativeExecutor"]
