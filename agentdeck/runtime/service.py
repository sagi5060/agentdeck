"""The Runtime: the one place a run is orchestrated.

Per event, in this order: append to the log, fan out to sinks, yield. The order is the
contract  -  an event a consumer has seen is already persisted, so a consumer that spots a
``seq`` gap can always refetch it.

Engines only yield payloads, and so does this: the store stamps the envelope, assigning
``seq`` and ``ts`` in the same indivisible step that writes the row (ADR-D11). Nothing here
holds a counter, which is what makes the refetch promise above true  -  a number that cannot be
allocated without being persisted cannot leave a hole behind.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from contextlib import aclosing, asynccontextmanager, suppress
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Final
from uuid import uuid4

from agentdeck.core.content import as_answer
from agentdeck.core.context import RunContext
from agentdeck.core.control import CONTROL_POLL_INTERVAL, Gate, Signal
from agentdeck.core.events import (
    TERMINAL_KINDS,
    AnswerRefused,
    ControlRequested,
    Event,
    RunCancelled,
    RunCompleted,
    RunFailed,
    RunInterrupted,
    RunResumed,
    RunStarted,
    Usage,
)
from agentdeck.core.reporting import Reporter
from agentdeck.core.status import (
    PRECONDITIONS,
    SUSPENDED_KINDS,
    Action,
    Operation,
    Ruling,
    RunStatus,
    Verdict,
    can_resume,
    decide,
)
from agentdeck.errors import DOCS_URL, ConfigError, NotFoundError, RunStateError, SessionBusyError, StoreError
from agentdeck.runtime.dispatch import SinkDispatch

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Mapping, Sequence
    from typing import Any

    from agentdeck.core.content import Input
    from agentdeck.core.control import ControlSignal
    from agentdeck.core.events import InterruptReason, KnownPayload
    from agentdeck.core.invocable import InvocableSpec
    from agentdeck.core.ports import ControlPort, EventStorePort, Executor, LeasePort, Observer
    from agentdeck.core.ports.store import RunSummary

logger = logging.getLogger(__name__)

# Mirrors ``RuntimeSettings.stale_run_after_seconds``'s own default (60.0 * 60.0)  -  duplicated
# rather than imported so a bare ``Runtime()`` needs no settings at all; ``build_runtime`` is
# the caller that resolves the configured value and passes it in.
_DEFAULT_STALE_RUN_AFTER = timedelta(hours=1)

# Mirrors ``RuntimeSettings.lease_ttl_seconds``, duplicated for the same reason as above.
_DEFAULT_LEASE_TTL = timedelta(seconds=90)

# How many renewals fit in one TTL. Six gives a 90-second lease a 15-second heartbeat: five
# consecutive misses before a live run is declared dead, which is the margin a GC pause or a
# briefly slow store gets to not cost a takeover.
_RENEWALS_PER_TTL = 6
_SESSIONS_DOCS = f"{DOCS_URL}/runs-and-control/sessions"

MAX_DELEGATION_DEPTH: Final[int] = 3
"""How many levels of delegation a run may sit under. Three covers orchestrator to worker to
helper, which is already past where a delegation tree stays legible in the log."""

MAX_DELEGATION_FANOUT: Final[int] = 8
"""How many child runs one run may start."""


@dataclass(slots=True)
class _Delegation:
    """One live run's place in the delegation tree.

    ``spent`` is what this run's *settled children* have used, folded into its own total when it
    ends; ``children`` counts every one it started, not the ones still running, because the bound
    is on the tree a run can build and not on how much of it is in flight at once.
    """

    parent: str | None
    depth: int
    invocable: str
    children: int = 0
    spent: Usage | None = None


@dataclass(frozen=True, slots=True)
class PendingRun:
    """One run currently ``WAITING_ANSWER``  -  what :meth:`Runtime.pending` lists."""

    run_id: str
    session_id: str | None
    invocable: str
    """The catalog name of what this run is waiting inside. General because one catalog holds
    agents, workflows and tools, so every run names its target the same way whatever kind it is."""

    thread_id: str
    payload: dict[str, Any]
    reason: InterruptReason = "human"
    """What kind of answer this run is waiting for. An ``approval`` takes a yes or a no and
    nothing else, which is checked before the answer is recorded rather than after."""


class Runtime:
    """Runs invocables and emits one canonical event stream, whatever engine did the work.

    Sinks are optional and buffered  -  each gets its own bounded queue, so the run is never
    pinned to one. The store stamps every event's ``ts`` in the write that persists it
    (ADR-D11); a caller that wants to hold time injects a clock into the store instead  -
    ``MemoryEventStore(clock=...)``, ``RedisEventStore(clock=...)``.

    ``lease`` is how a killed worker is recognised as one instead of waited out. A live run
    holds a lease and renews it while it plays; a process killed outright stops renewing, and
    the next turn on that session can then assert positively that nobody is executing the run
    it finds open. Without a lease backend the two mechanisms below are all there is, which is
    why ``stale_run_after`` stays: a backend that knows nothing about a run never reports it,
    so this degrades to the timer rather than guessing.

    ``stale_run_after`` is how long a **running** run may go silent before it stops holding its
    session  -  never a suspended one, which holds until resumed, answered or cancelled.
    ``Runtime`` takes no ambient configuration at all  -  it defaults to one hour and never reads
    settings itself; ``build_runtime`` is the caller that resolves
    ``AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS`` and passes the configured value in, the same
    as its five peer arguments. ``control_poll_interval`` is how long a run may reuse the
    control answer it already has: it trades cancel latency against the read rate a run costs
    a shared ``ControlPort``, and ``0`` buys the tightest latency at one read per safe point.
    """

    def __init__(
        self,
        executors: Sequence[Executor],
        store: EventStorePort,
        invocables: Mapping[str, InvocableSpec],
        sinks: Sequence[Observer] = (),
        control: ControlPort | None = None,
        stale_run_after: timedelta = _DEFAULT_STALE_RUN_AFTER,
        control_poll_interval: float = CONTROL_POLL_INTERVAL,
        lease: LeasePort | None = None,
        lease_ttl: timedelta = _DEFAULT_LEASE_TTL,
    ) -> None:
        self._executors = {executor.name: executor for executor in executors}
        self._store = store
        self._invocables = invocables
        self._sinks = tuple(SinkDispatch(sink) for sink in sinks)
        self._control = control
        self._stale_run_after = stale_run_after
        self._control_poll_interval = control_poll_interval
        self._lease = lease
        self._lease_ttl = lease_ttl
        # Runs :meth:`close_cancelled` closed from outside. Only ``Deck.aclose`` abandons a run,
        # and only at teardown, so this is bounded by the runs one process had in flight.
        self._abandoned: set[str] = set()
        # The delegation tree, live: one entry per run in flight, dropped when it ends. Held here
        # rather than read off the log because every reader acts while the run is still running  -
        # a bound has to refuse before the child is claimed, a cancel has to reach a child now,
        # and a total has to be right before ``run.completed`` is written rather than after.
        self._tree: dict[str, _Delegation] = {}

    @property
    def store(self) -> EventStorePort:
        """The event log this Runtime plays every run against  -  the read side of what
        :meth:`run`, :meth:`resume` and :meth:`resume_run` write to, for a caller that wants
        to read a run back directly (e.g. ``App.store``) rather than only watching it live.
        """
        return self._store

    async def run(
        self,
        name: str,
        input: Input,
        *,
        context: object = None,
        session_id: str | None = None,
        namespace: str | None = None,
        key: str | None = None,
        run_id: str | None = None,
        parent_run_id: str | None = None,
    ) -> AsyncGenerator[Event, None]:
        """Play one run of ``name``, yielding every event it produced, ``run.started`` first.

        ``context`` is the application's own value for this run, reaching a callable that declares
        a ``ToolCtx[...]`` parameter and nothing else. It is held by reference for the run's whole
        life and never written to the log  -  the record says what a run was asked to do, not which
        live objects it held.

        ``key`` is the caller's optional stable application identifier  -  for lookup and
        idempotency, never the run's address. The run's own ``id`` is always minted here, never
        derived from ``key``, so two namespaces reusing one key still get two distinct runs.
        ``(namespace, key)`` is a permanent claim: reusing one whose run already started raises
        ``DuplicateKeyError`` rather than handing back the run that holds it.

        ``run_id`` is for the one caller that needs the id before this generator is drawn from:
        ``ctx.invoke`` hands its body a child ``Run`` synchronously, so the handle exists before
        the opening claim lands. Still minted, and minted by AgentDeck  -  see
        :meth:`_new_run_context`.

        ``parent_run_id`` is which run delegated this one, from the same caller. It is recorded on
        ``run.started`` and nowhere else, and it is what a cancel cascades along and what a
        delegated turn's cost rolls up through.

        One turn per session at a time: opening the run is a conditional append that fails if
        the session already has one in flight, so a second concurrent turn raises
        ``SessionBusyError`` instead of running against a conversation that is still changing.

        The engine's exception, if any, reaches the caller  -  but ``run.failed`` is recorded
        first, so the log tells the whole story even when nobody was listening. Every exit
        closes the run in the log: a consumer that walks away gets ``run.cancelled``, whether it
        closed this generator or had its own task cancelled under it.
        """
        spec, executor = self._resolve(name)
        ctx, reports = self._bind(
            self._new_run_context(run_id=run_id, key=key, session_id=session_id, namespace=namespace, data=context),
            spec,
        )
        self.delegate(ctx.run_id, parent_run_id, spec.name)
        # ponytail: whole log per run  -  window it (or hand the engine a summary) once a
        # session's history outgrows one read, which a real store will notice long before this does
        history = await self._history(ctx)

        opening = RunStarted(
            invocable=spec.name,
            kind_of_invocable=spec.kind.value,
            input=input,
            parent_run_id=parent_run_id,
        )
        try:
            claimed = await self._claim_session(opening, spec, ctx, history)
        except asyncio.CancelledError:
            # The claim commits this run before anything is yielded, and it is awaited in the
            # caller's own coroutine  -  the one an ASGI server cancels when a client disconnects
            # before the response starts. A cancellation landing between the two would leave the
            # run open in the log and its session held for a whole staleness window.
            # Anything not None is a run the claim opened, and it is owed a terminal event.
            if await self._store.run_status(ctx) is not None:
                await self._close_cancelled(spec, ctx, "cancelled during the claim")
            raise

        async with aclosing(
            self._play(claimed, executor.execute(spec, input, history, ctx), spec, ctx, executor, reports)
        ) as run:
            async for event in run:
                yield event

    async def resume(
        self,
        name: str,
        value: Any,
        *,
        context: object = None,
        run_id: str,
        session_id: str | None = None,
        namespace: str | None = None,
    ) -> AsyncGenerator[Event, None]:
        """Continue a run this Runtime suspended earlier.

        ``context`` is resupplied, never recovered: the value is held by reference for one run
        and deliberately never written to the log, so a run picked up here starts with whatever
        this caller hands it. Omitting it is not "keep what the run had"  -  it is ``None``, and a
        node that read ``ctx.data`` before the interrupt reads ``None`` after it.

        The store's conditional append makes the ``WAITING_ANSWER`` -> ``RUNNING`` transition
        atomic, so exactly one caller wins even when the callers are separate processes; the
        winner opens the run with ``run.resumed``, seq recovered from the log's own
        ``max(seq)`` so it stays contiguous across a process restart  -  never reset to 0.
        From there the executor plays on exactly like ``run()`` plays an opening: same
        terminal/suspended/exception handling  -  and through the same
        :meth:`~agentdeck.core.ports.Executor.execute`, which reads the answer and the thread
        off the log rather than taking either as an argument.

        A stray resume  -  already resumed by a racing caller, or a completed run  -  is a no-op:
        nothing is read from the executor, nothing is yielded. A run an operator asked to *stop*
        is not stray, and refuses instead: honoring the answer would let it silently override
        somebody who said stop. Both intents survive the refusal  -  the run is still waiting, and
        the pause is still pending for whoever reads next.

        A **cancel** recorded while the run waited ends it here rather than answering it, for
        the reason :meth:`resume_run` gives: this claim is the only thing that will ever look.
        """
        spec, executor = self._resolve(name)
        ctx, reports = self._bind(
            self._context(run_id=run_id, session_id=session_id, namespace=namespace, data=context), spec
        )
        status = await self._store.run_status(ctx)
        if status is None:
            return
        # No precondition check here: which states admit an answer is the front door's business
        # (``Deck._answer``), and this method is also where a *loser* lands  -  a caller whose run
        # was answered out from under it reads ``RUNNING`` and must still no-op, which is what
        # the claim below does for it. Refusing here would turn every lost race into an error.
        #
        # The routing refusal is different and has to be read *before* the claim, which is the
        # one place this path departs from resume_run's order: the claim is the ``run.resumed``
        # carrying the answer, so once it lands the answer cannot be taken back.
        refusal, _ = await self._peek(ctx.id, status)
        if refusal.action is Action.REFUSE:
            raise RunStateError(f"run {run_id!r} cannot be answered: {refusal.why}")
        # Before the claim, exactly as :meth:`resume_run` reads it before its own: a bail-out
        # after the claim would leave a run flipped to RUNNING with nobody playing it, owed a
        # terminal event forever and still holding its session.
        events = await self._store.read_run(ctx)
        opened = next((event.payload for event in events if isinstance(event.payload, RunStarted)), None)
        if opened is None:
            return
        self.delegate(ctx.run_id, opened.parent_run_id, spec.name)
        if (why := _refuses(events, value)) is not None:
            # Recorded, then raised, and both before the claim: the run is still waiting, so the
            # answerer can send a real one  -  and the log keeps the fact that somebody tried.
            await self._record(AnswerRefused(reason=why), spec, ctx)
            raise ValueError(why)
        opening = await self._claim_resume(spec, ctx, value)
        if opening is None:
            return
        ruling, pending = await self._route(ctx.id, status)
        if ruling.action is Action.TERMINATE and pending is not None:
            yield opening
            yield await self._record(ControlRequested(verb="cancel", reason=pending.reason), spec, ctx)
            yield await self._record(RunCancelled(reason=pending.reason), spec, ctx)
            return
        # Any other ruling plays the run on, including a pause that landed inside the window the
        # peek left open: the answer is recorded by now, so the run resumes and meets that pause
        # at its first safe point instead.
        # Read after the claim, so the ``run.resumed`` carrying the answer is in it: that event
        # is how the executor learns there is an answer at all, and what it is.
        history = await self._history(ctx)
        stream = executor.execute(spec, opened.input, history, ctx)
        async with aclosing(self._play(opening, stream, spec, ctx, executor, reports)) as resumed:
            async for event in resumed:
                yield event

    async def resume_run(
        self,
        run_id: str,
        *,
        context: object = None,
        namespace: str | None = None,
        reason: str | None = None,
    ) -> AsyncGenerator[Event, None]:
        """Continue a run that paused at a safe point: same ``run_id``, same log, ``seq``
        counting on from where it stopped.

        ``context`` is resupplied here for the same reason it is on :meth:`resume`  -  the value
        never reached the log, so the caller lifting the pause is the only one who still has it.

        The engine is re-entered rather than un-suspended, because a paused turn left no stack
        to return to: the log is the checkpoint, so the run is played again from its own
        ``run.started`` input with the log as history. What that means for a caller is stated
        where the safe points are  -  a step the paused turn already took can be taken twice, so
        a tool with side effects has to tolerate being called again.

        Which states admit a resume is :data:`PRECONDITIONS`' business, not this method's. A
        run that is already running or already over is a no-op  -  that covers the ordinary races
        without a second answer for a caller to branch on  -  while one waiting for a *value*
        refuses, naming ``run.answer(...)``, because silence there reads as "resumed" to a
        caller who is in fact holding the run's only answer.

        A **cancel** recorded while the run was paused is honored here instead of resuming it,
        because this claim is the only thing that will ever look for it: a paused run has no
        loop reaching safe points, so nothing else can turn that request into an effect. The
        run ends ``cancelled`` and is never played on  -  cancel stays terminal, and asking to
        resume a run somebody cancelled does not quietly override them.
        """
        ctx = self._context(run_id=run_id, namespace=namespace, data=context)
        summary = await self._find(run_id, ctx)
        if summary is None:
            return
        allowed = PRECONDITIONS[summary.status, Operation.RESUME]
        if allowed.verdict is Verdict.REFUSED:
            raise RunStateError(f"run {run_id!r} cannot be resumed: {allowed.why}")
        if allowed.verdict is Verdict.NO_OP:
            return
        started = await self._opening_of(run_id, ctx)
        if started is None:
            return
        session_id, opened = started
        spec, executor = self._resolve(opened.invocable)
        run_ctx, reports = self._bind(replace(ctx, run_id=run_id, session_id=session_id), spec)
        self.delegate(run_id, opened.parent_run_id, spec.name)
        opening = await self._claim_resume(spec, run_ctx, None, reason)
        if opening is None:
            return
        # Read control only after the claim: the claim is what makes this caller the one actor
        # on the run, so an answer read before it could belong to somebody else's turn.
        ruling, pending = await self._route(run_ctx.id, summary.status)
        if ruling.action is Action.TERMINATE and pending is not None:
            yield opening
            # No ``control.observed``: that event says the run reached a safe point and acted
            # there, and this run reached none  -  it was already stopped when the cancel landed.
            # The request and the effect are the whole honest story of a cancel served here.
            yield await self._record(ControlRequested(verb="cancel", reason=pending.reason), spec, run_ctx)
            yield await self._record(RunCancelled(reason=pending.reason), spec, run_ctx)
            return
        history = await self._history(run_ctx)
        stream = executor.execute(spec, opened.input, history, run_ctx)
        async with aclosing(self._play(opening, stream, spec, run_ctx, executor, reports)) as resumed:
            async for event in resumed:
                yield event

    async def signal(
        self, run_id: str, verb: Signal, reason: str | None = None, *, namespace: str | None = None
    ) -> bool:
        """Record a control request for ``run_id`` in ``namespace``, wherever it is running  -
        except a cancel against a run already suspended, which ends it right here instead.

        ``run_id`` here is a run's own minted, globally unique id  -  the same value its Gate
        polls under (bound in :meth:`_bind`)  -  so two namespaces can never collide over one:
        each run's id is unrelated to the other's from the moment it was minted.

        ``False`` means this Runtime has no ``ControlPort`` and nothing was recorded  -  the one
        answer a caller has to act on. Everything else is deliberately not an answer here: the
        run may be inside a tool call, in another process, or already over, and which of those
        it is cannot be known at the moment a caller asks. A signal that loses the race with a
        terminal event is a no-op, since nothing polls the gate once the run loop has exited.

        Not for lifting a pause: a paused run has no loop left to notice anything, so
        :meth:`resume_run` is what continues it (and writes ``RESUME`` itself).

        A **cancel** cannot wait the same way: a suspended run has no loop that will ever poll
        the gate again, so merely recording the signal is betting on somebody else calling
        :meth:`resume`/:meth:`resume_run` later to notice it  -  which may never happen, wedging
        the very session the cancel was meant to free. :meth:`_cancel_suspended` claims and
        terminates such a run directly. A **pause**, by contrast, stays merely recorded even
        against a suspended run: it has nothing to do until something next resumes or answers
        that run, per the routing table (``docs/design/run-lifecycle.md``).

        A **cancel cascades** to the runs this one delegated, and theirs in turn. A parent that
        stops while a child keeps burning tokens is worse than not offering cancel at all, and the
        parent cannot do it for itself: it is inside the call that is waiting on the child.

        A **pause does not** cascade, and what that leaves behind differs by executor, so it is
        stated per executor rather than as one rule:

        | the paused parent is | its in-flight child | why |
        |---|---|---|
        | an agent turn | runs on, and the parent does not reach a safe point until it is done | resuming replays the turn from the log, so a suspended child would be delegated a second time |
        | a native workflow | runs on, while the parent parks wherever it next reaches a safepoint | the body is a live coroutine, so ``await child`` survives the pause and reads the same child on resume |

        Either way the child keeps running and a caller that wants it stopped cancels or pauses
        that child by its own id. What is common to both is only the refusal to cascade; the
        re-delegation above is an agent-turn fact and not a workflow one.
        """
        if verb is Signal.CANCEL:
            for child in [child for child, placed in self._tree.items() if placed.parent == run_id]:
                await self.signal(child, verb, reason, namespace=namespace)
        if verb is Signal.CANCEL and await self._cancel_suspended(run_id, reason, namespace):
            return True
        if self._control is None:
            logger.warning("no ControlPort is wired: %s for run %s was not recorded", verb.value, run_id)
            return False
        id = self._context(run_id=run_id, namespace=namespace).id
        await self._control.signal(id, verb, reason)
        return True

    async def _cancel_suspended(self, run_id: str, reason: str | None, namespace: str | None) -> bool:
        """Claim ``run_id``'s suspended -> ``RUNNING`` transition and terminate on top of it,
        the same shape :meth:`resume`/:meth:`resume_run` already use when *they* are the ones
        to find a cancel pending. ``False`` for anything not currently suspended (``RUNNING``,
        terminal, or a run ``namespace`` has never heard of)  -  ``signal`` falls through to
        recording those the ordinary way.

        Losing the claim to a concurrent resume/answer is not an error: that caller is now the
        one actor on the run, and the recorded signal ``signal`` falls through to afterwards is
        exactly what its own routing reads.
        """
        ctx = self._context(run_id=run_id, namespace=namespace)
        summary = await self._find(run_id, ctx)
        if summary is None or not can_resume(summary.status):
            return False
        started = await self._opening_of(run_id, ctx)
        if started is None:
            return False
        session_id, opened = started
        spec, _ = self._resolve(opened.invocable)
        run_ctx, _ = self._bind(replace(ctx, run_id=run_id, session_id=session_id), spec)
        if await self._claim_resume(spec, run_ctx, None, reason) is None:
            return False
        await self._record(ControlRequested(verb="cancel", reason=reason), spec, run_ctx)
        await self._record(RunCancelled(reason=reason), spec, run_ctx)
        return True

    async def _play(
        self,
        opening: Event,
        stream: AsyncGenerator[KnownPayload, None],
        spec: InvocableSpec,
        ctx: RunContext,
        executor: Executor,
        reports: deque[KnownPayload],
    ) -> AsyncGenerator[Event, None]:
        """Yield ``opening``, then everything ``stream`` produces  -  and close the run in the
        log whichever way it ends.

        One body for all three openings (a start, a resumed interrupt, a lifted pause) because
        every one of them owes the log the same four endings: a terminal event, a suspension, a
        consumer that walked away, or an exception. ``reports`` is drained the same way for all
        three, and for the same reason there is one body at all.

        The lease is held here, around the whole of it, for exactly that reason: a run is being
        executed precisely while this body is reading its engine, and every one of the four
        endings leaves through this method. A run that parks at a suspension releases too  -
        nothing is executing it then, and #311's rule is that the log alone decides how long
        a suspended run holds its session.
        """
        last = opening.kind
        try:
            async with self._holding(ctx.run_id):
                yield opening
                async with aclosing(stream) as payloads:
                    async for payload in payloads:
                        async for report in self._drain(reports, spec, ctx):
                            yield report
                        yield await self._record(payload, spec, ctx)
                        last = payload.kind
                        if last in TERMINAL_KINDS:
                            # Terminal means terminal: stop reading so nothing can follow it
                            # into the log. An engine yielding more after this gets it
                            # discarded.
                            break
        except GeneratorExit:
            # Nobody is listening any more, so there is no event to yield  -  but an unclosed
            # run in the log is indistinguishable from one still in flight.
            logger.info("run %s abandoned by its consumer after %r", ctx.run_id, last)
            await self._record(RunCancelled(reason="consumer stopped reading"), spec, ctx)
            raise
        except asyncio.CancelledError:
            # The other way a consumer walks away, and the one a real ASGI server delivers: it
            # cancels the task streaming the response rather than closing the generator. This
            # arm exists because ``CancelledError`` is a BaseException, so the one below never
            # saw it and the run stayed open in the log forever.
            logger.info("run %s cancelled after %r", ctx.run_id, last)
            await self._close_cancelled(spec, ctx, "consumer cancelled")
            raise
        except Exception as exc:
            # The exception is the caller's, the event is the record  -  both, always. The type
            # name only: an exception message can carry content that must not reach a sink.
            logger.exception("run %s failed in engine %r", ctx.run_id, executor.name)
            yield await self._record(_failed(exc, executor.name), spec, ctx)
            raise

        if last not in TERMINAL_KINDS and last not in SUSPENDED_KINDS:
            # An engine that just stops leaves consumers waiting forever; close the run for it.
            logger.error("engine %r ended run %s after %r, not a terminal event", executor.name, ctx.run_id, last)
            yield await self._record(_engine_failed(f"engine {executor.name!r} ended after {last!r}"), spec, ctx)

    @asynccontextmanager
    async def _holding(self, run_id: str) -> AsyncIterator[None]:
        """Assert, for as long as the body runs, that this worker is executing ``run_id``.

        The mechanism is the port's; the acquire/renew/release cycle is policy and lives here,
        the same way :class:`~agentdeck.core.control.Gate` sits above ``ControlPort``. Every
        exit releases, including the ones that end the run badly  -  and the one exit that cannot
        release, the process being killed, is the whole reason the lease has a TTL.

        Nothing here can fail a turn. A lease is an *improvement* on the staleness timer, so a
        lease backend that is unreachable, or that refuses the acquire, leaves the run playing
        under the timer alone rather than taking a working turn down with it.
        """
        lease = self._lease
        if lease is None:
            yield
            return
        with suppress(StoreError):
            if not await lease.acquire(run_id, self._lease_ttl):
                # A run id is minted per run and globally unique, so this means two workers
                # believe they are playing one run. Worth saying; not worth refusing, since
                # the log's own conditional append already decided which of them plays.
                logger.warning("run %s is already leased by another worker", run_id)
        renewing = asyncio.create_task(self._renew(lease, run_id))
        try:
            yield
        finally:
            renewing.cancel()
            with suppress(StoreError):
                await lease.release(run_id)

    async def _renew(self, lease: LeasePort, run_id: str) -> None:
        """Say, every ``lease_ttl / _RENEWALS_PER_TTL``, that this run is still being executed.

        # ponytail: an asyncio renewer cannot run while the event loop is blocked, so a tool
        # doing synchronous I/O or CPU-bound work for longer than one TTL lets a *live* run's
        # lease expire and be taken over. Raise ``AGENTDECK_RUNTIME_LEASE_TTL_SECONDS`` above
        # the longest stall a deployment can produce; move the renewer to its own thread if
        # that ceiling is ever the binding one.
        """
        interval = self._lease_ttl.total_seconds() / _RENEWALS_PER_TTL
        while True:
            await asyncio.sleep(interval)
            try:
                if not await lease.renew(run_id, self._lease_ttl):
                    logger.warning("run %s lost its lease while still playing", run_id)
            except StoreError:
                # Keep renewing: one unreachable moment is not a reason to stop asserting
                # liveness for the rest of the run, and the TTL is the backstop if it persists.
                logger.exception("could not renew the lease for run %s", run_id)

    async def _dead_runs(self, history: Sequence[Event]) -> frozenset[str]:
        """Which runs in this log a lease can positively assert nobody is executing.

        Empty whenever there is no lease port, whenever the backend never saw these runs, and
        whenever it cannot be reached  -  three different kinds of ignorance, all of which must
        read as "no knowledge" rather than "dead", or a session gets taken from a live worker.
        """
        if self._lease is None or not history:
            return frozenset()
        try:
            return await self._lease.dead({event.run_id for event in history})
        except StoreError:
            logger.exception("could not read the run leases; falling back to the staleness timer")
            return frozenset()

    async def _find(self, run_id: str, ctx: RunContext) -> RunSummary | None:
        """Where a run lives and what state it is in. ``None`` means no run of this namespace
        answers to that id.

        Addressed by ``run_id`` within ``ctx``'s namespace  -  the same value the control plane
        addresses by ``id``, since the two are one field now  -  so the store's own status
        projection is what locates it: a caller holding a ``run_id`` from a stream it was
        watching has neither the log key nor the invocable's name. Deliberately *unfiltered*:
        narrowing the listing to one status was how a run in every other state came back
        indistinguishable from one that does not exist, which is what let a resume against a
        parked run report nothing at all.
        """
        for summary in await self._store.list_runs(ctx):
            if summary.run_id == run_id:
                return summary
        return None

    async def _opening_of(self, run_id: str, ctx: RunContext) -> tuple[str | None, RunStarted] | None:
        """Whose session this run holds and what it was asked to do  -  its own ``run.started``.
        Read only once the state machine has admitted the operation, so a refused or no-op call
        never pays for a run's log."""
        for event in await self._store.read_run(replace(ctx, run_id=run_id)):
            if isinstance(event.payload, RunStarted):
                return event.session_id, event.payload
        return None

    async def _peek(self, id: str, status: RunStatus) -> tuple[Ruling, ControlSignal | None]:
        """Read the control port and rule on what is there, taking nothing. For the one decision
        that has to be made before a claim  -  whether the operation is refused at all."""
        pending = None if self._control is None else await self._control.poll(id)
        return decide(status, None if pending is None else pending.verb), pending

    async def _route(self, id: str, status: RunStatus) -> tuple[Ruling, ControlSignal | None]:
        """The one way a stopped run's pending intent is read: poll, decide, and take the intent
        the ruling acted on.

        Taking it is a compare-and-set rather than a clear, so an intent that changed under this
        caller is not destroyed by it. Losing that set means the ruling was made about somebody
        else's signal, so the port is read once more and ruled on again  -  the second ruling acts
        without taking anything, which leaves whatever is pending now for the gate to meet at the
        run's first safe point.
        """
        ruling, pending = await self._peek(id, status)
        if pending is None or self._control is None or not ruling.consume:
            return ruling, pending
        if await self._control.consume(id, pending.verb):
            return ruling, pending
        logger.info("control intent for run %s changed under this caller; re-reading it", id)
        return await self._peek(id, status)

    async def _claim_session(
        self, opening: RunStarted, spec: InvocableSpec, ctx: RunContext, history: Sequence[Event]
    ) -> Event:
        """Open this run, or refuse the turn: the store decides, in one conditional append.

        A session's engine state is one conversation, and only its engine can lock it  -  so the
        platform admits one turn at a time and the write that opens a run is the write that
        tests whether the session is free. A check followed by an append would let two servers
        both find it idle; here only one ``run.started`` can land, so the loser is told, not
        interleaved with the winner. Refusing raises rather than yielding nothing, because a
        caller cannot tell an empty stream apart from a turn that produced no events.

        An open run nobody is coming back for would otherwise hold its session for good: every
        graceful exit closes its run, so this is the process that was killed outright. Such a
        run stops holding the session on either of two findings  -  a lease this worker can see
        has expired, or, failing that, silence for longer than ``stale_run_after``  -  and this
        turn closes it. Loudly, and accepting that a takeover can be premature, because a
        session wedged forever is the worse failure. Failing to close it is not worth failing
        this turn over: the next one meets the same stale run and tries again.

        ``history`` is the read this turn already did, reused rather than repeated: it names
        every run in this session, which is the set to ask the lease about. A run that opened
        between that read and this claim is simply not in the set, so it is judged by the timer
        and holds its session  -  the safe direction to be wrong in.
        """
        claim, event = await self._store.claim_start(
            opening, ctx, spec.name, self._stale_run_after, dead=await self._dead_runs(history)
        )
        if claim.held_by is not None or event is None:
            raise SessionBusyError(await self._session_busy_message(ctx, claim.held_by))
        for tail in claim.overridden:
            try:
                await self._close_abandoned(tail, ctx)
            except StoreError:
                # This run is already open in the log, so letting a failed piece of bookkeeping
                # out here would leave it with no terminal event and wedge the session for a
                # whole window. The abandoned run stays open instead, and the next turn  -  which
                # finds it just as stale  -  closes it then.
                logger.exception("could not close abandoned run %s; leaving it for the next turn", tail.run_id)
        await self._fan_out(event)
        return event

    async def _session_busy_message(self, ctx: RunContext, held_by: str | None) -> str:
        """What ``SessionBusyError`` says, which depends on why the holder is still open.

        A ``RUNNING`` holder really is "in flight." One parked at ``PAUSED`` or
        ``WAITING_ANSWER`` is not  -  nothing is executing it, and no ``stale_run_after`` will
        ever free it  -  so the message names the verb that actually unsticks that run instead
        of repeating a claim that is false of it.

        ``ctx.session_id`` is never ``None`` here: only a session can be busy, so only a run that
        named one can be refused for it. It used to read the log key instead, which for a run
        with no session is that run's own id  -  a message naming a session that does not exist,
        held by the very run being refused.
        """
        status = None if held_by is None else await self._store.run_status(replace(ctx, run_id=held_by))
        if status is RunStatus.WAITING_ANSWER:
            return (
                f"session {ctx.session_id!r} is held by run {held_by!r}, parked waiting for an answer  -  "
                f"supply it with run.answer(...) or end it with run.cancel(...), see {_SESSIONS_DOCS}"
            )
        if status is RunStatus.PAUSED:
            return (
                f"session {ctx.session_id!r} is held by run {held_by!r}, paused  -  "
                f"lift it with run.resume() or end it with run.cancel(...), see {_SESSIONS_DOCS}"
            )
        return (
            f"session {ctx.session_id!r} already has run {held_by!r} in flight, "
            f"so run {ctx.run_id!r} cannot start on it  -  see {_SESSIONS_DOCS}"
        )

    async def _close_abandoned(self, tail: Event, ctx: RunContext) -> None:
        """Close a run this turn took the session from. Nobody else can: its process is gone,
        and an open run in the log is indistinguishable from one still in flight.

        Written in *that* run's context, not this turn's, so the store stamps it with the
        abandoned run's own ``run_id``, ``session_id`` and next ``seq``, and it inherits that
        run's ``origin``. The event belongs to its story: a reader must not find this invocable
        blamed for it. ``tail`` is the run's last event, handed over by the claim that stepped
        over it  -  the store had already read it to decide the run was stale, so nothing here
        goes back for it.
        """
        logger.warning(
            "run %s went silent holding session %s; run %s took it over and closed it as failed",
            tail.run_id,
            ctx.session_id,
            ctx.run_id,
        )
        payload = RunFailed(
            error_code="cancelled_hard",
            message=f"abandoned: the session was taken over by run {ctx.run_id}",
            retryable=False,
        )
        abandoned = replace(ctx, run_id=tail.run_id, session_id=tail.session_id)
        event = (await self._store.append([payload], abandoned, tail.origin))[0]
        await self._fan_out(event)
        if self._lease is not None:
            # The dead worker's expired row has done its job and nothing else prunes it. Not
            # required for correctness  -  the run is terminal now, so no claim ever consults it
            # again  -  but a lease table that only grows is a table nobody trusts.
            with suppress(StoreError):
                await self._lease.release(tail.run_id)

    async def _close_cancelled(self, spec: InvocableSpec, ctx: RunContext, reason: str) -> None:
        """Write the closing ``run.cancelled`` while this task is already being cancelled.

        Shielded because the append suspends  -  a durable store hands it to a thread, and the
        in-memory one yields a turn  -  and an unshielded await inside a cancelled task is
        re-cancelled before the write can land. Best effort by construction: the write survives
        the cancellation, but not the event loop, so a process dying with the request leaves the
        run open in the log for whatever reconciles it later.
        """
        recording = asyncio.ensure_future(self._record(RunCancelled(reason=reason), spec, ctx))
        with suppress(asyncio.CancelledError):
            await asyncio.shield(recording)

    async def close_cancelled(self, run_id: str, reason: str, *, namespace: str | None = None) -> None:
        """Close a run this deck abandoned, addressed by ``run_id`` alone. Its task is still
        alive, so the mark comes first, before this method's own first await: every turn between
        the mark and the write is one the run can still get an append into, terminal ones
        included. That is also why the write goes to the store directly, the way
        :meth:`_close_abandoned` writes for a run that is not its own: :meth:`_record` would
        refuse this one along with the rest.
        """
        self._abandoned.add(run_id)
        ctx = self._context(run_id=run_id, namespace=namespace)
        started = await self._opening_of(run_id, ctx)
        if started is None:
            logger.error(
                "run %s is being abandoned but has no run.started to close it against; it stays "
                "open in the log until something reconciles it (#419)",
                run_id,
            )
            return
        if await self._store.run_status(ctx) is not RunStatus.RUNNING:
            return
        session_id, opened = started
        closing = replace(ctx, session_id=session_id)
        event = (await self._store.append([RunCancelled(reason=reason)], closing, opened.invocable))[0]
        await self._fan_out(event)

    async def _claim_resume(
        self, spec: InvocableSpec, ctx: RunContext, value: Any, reason: str | None = None
    ) -> Event | None:
        """Take the run's suspended -> ``RUNNING`` transition, or ``None`` if someone else
        already has it. Suspended is ``WAITING_ANSWER`` or ``PAUSED``: the same claim serves
        both, because both are one run owed a terminal event and only one caller may continue
        it (``can_resume``).

        The store decides, in one conditional append: whoever's ``run.resumed`` lands is the
        one caller that gets to play the run on. That holds across processes, where a check
        followed by a separate append never could  -  two servers sharing a store would both
        read the suspended status and both write. A loser reads nothing from the engine and
        yields nothing, so a stray resume stays a no-op rather than an error.

        The claim carries the answer, not just the fact of it: this one append is what flips
        the status, so a value written anywhere else would leave a window in which the log
        says the run was answered and no longer holds what the answer was  -  and the engine,
        still parked at its interrupt, could never be brought back in line with it.

        ``seq`` continues across a process restart rather than resetting, because the store
        assigns it from the run's own log (ADR-D11)  -  there is no counter here to recover.
        """
        resumed = RunResumed(reason=reason, value=as_answer(value))
        event = await self._store.claim_resume(resumed, ctx, spec.name)
        if event is None:
            return None
        await self._fan_out(event)
        return event

    async def pending(self, *, namespace: str | None = None) -> list[PendingRun]:
        """Every run currently ``WAITING_ANSWER`` in this namespace.

        Asks the store to project which runs are waiting rather than keeping an in-memory
        registry  -  a registry would go stale the moment a process restarted, which is
        exactly the bug this avoids. Only the matched runs get a (bounded, per-run) read,
        to pull the interrupt's ``thread_id`` and ``payload``.

        The listing and those reads are two snapshots, so a run can be resumed between them
        and come back already answered. That is harmless: the resume claim itself is what
        checks status, so acting on a stale entry is a no-op, not a double resume.
        """
        # ponytail: every parked run's whole log, per call, and an approval inbox polls this  -
        # so the cost is (parked runs x their length) on a path a UI hits on a timer. Fine while
        # a deployment parks tens of runs; the upgrade is a store-side projection of each run's
        # last interrupt, and the trigger is the first inbox that pages or that a poll can't
        # answer inside its own refresh interval.
        ctx = self._context(namespace=namespace)
        out: list[PendingRun] = []
        for summary in await self._store.list_runs(ctx, status=RunStatus.WAITING_ANSWER):
            found = _last_interrupt(await self._store.read_run(replace(ctx, run_id=summary.run_id)))
            if found is None:
                continue
            event, interrupted = found
            out.append(
                PendingRun(
                    run_id=summary.run_id,
                    session_id=event.session_id,
                    invocable=event.origin,
                    thread_id=interrupted.thread_id or summary.run_id,
                    payload=interrupted.payload,
                    reason=interrupted.reason,
                )
            )
        return out

    async def find(self, run_id: str, *, namespace: str | None = None) -> RunSummary | None:
        """Public wrapper over :meth:`_find`, for a caller that only needs to know where a run
        lives and what state it is in  -  :meth:`status`, and ``deck.runs.get``'s own lookup."""
        return await self._find(run_id, self._context(run_id=run_id, namespace=namespace))

    def suspends(self, name: str) -> bool:
        """Whether the executor behind ``name`` can pause and later continue a run of it  -  the
        capability half of ``run.can`` (:func:`~agentdeck.core.status.can_of`). Sync, because
        it is a lookup in the catalog this Runtime was built with and never a store read."""
        _, executor = self._resolve(name)
        return executor.suspendable

    async def status(self, run_id: str, *, namespace: str | None = None) -> RunStatus | None:
        """This run's current status, or ``None`` if this namespace has never heard of it.

        Reads :meth:`find` rather than a dedicated index: a run addressable by ``id`` alone
        (docs/design/run-identity.md §1) needs no store-side lookup keyed any other way, and
        this is the same projection :meth:`resume_run`/:meth:`_cancel_suspended` already fold
        before they claim anything.
        """
        summary = await self.find(run_id, namespace=namespace)
        return summary.status if summary is not None else None

    async def drain(self) -> None:
        """Flush what the sinks have not taken yet, then close them.

        The composition root calls this at shutdown: without it, queued emits are destroyed
        with the event loop and the last few audit or cost events are silently lost, and a sink
        that buffers internally never gets the one ``Observer.close`` that tells it to
        write its buffer out. Never called per event  -  that would be exactly the join the
        fan-out exists to avoid. It is terminal: closed sinks stay closed, and a run after this
        one reaches none of them.
        """
        await asyncio.gather(*(dispatch.close() for dispatch in self._sinks), return_exceptions=True)

    async def _history(self, ctx: RunContext) -> list[Event]:
        """What an executor is played with: the conversation this run is part of, or  -  when it
        is part of none  -  the run's own events.

        The derivation the log key used to hide. Written out because the two are different
        questions: a run in a session is re-entered knowing what was said before it, and a
        standalone run is re-entered knowing only what it itself already did.
        """
        if ctx.session_id is None:
            return await self._store.read_run(ctx)
        return await self._store.read_session(ctx)

    def _context(
        self,
        *,
        run_id: str | None = None,
        session_id: str | None = None,
        namespace: str | None = None,
        data: object = None,
    ) -> RunContext:
        """Build a context for addressing a run whose id is already known  -  resume, signal,
        answer, a lookup-only read. ``run_id`` here is that known id, carried through as-is;
        it is minted only by :meth:`_new_run_context`, never by this one.
        """
        return RunContext(run_id=run_id or str(uuid4()), session_id=session_id, namespace=namespace, data=data)

    def _new_run_context(
        self,
        *,
        run_id: str | None = None,
        key: str | None = None,
        session_id: str | None = None,
        namespace: str | None = None,
        data: object = None,
    ) -> RunContext:
        """Mint a fresh run's context  -  the one place a new run's ``run_id`` is minted.

        ``key`` is the caller's optional identifier, carried through unchanged as ``ctx.key``:
        it is never the source of ``run_id``, which is why this is a separate method from
        :meth:`_context` rather than that one falling back to ``uuid4()``. A caller-supplied
        value reaching ``run_id`` is exactly the derivation this design retired  -  two namespaces
        given the same ``key`` must still mint two different, unrelated ids.

        ``run_id`` is that same mint moved one step earlier, never a derivation: the invoker
        behind ``ctx.invoke`` mints it so a child's handle can exist before its opening claim
        lands. Nothing an application supplies reaches it.
        """
        return RunContext(run_id=run_id or str(uuid4()), key=key, session_id=session_id, namespace=namespace, data=data)

    def _bind(self, ctx: RunContext, spec: InvocableSpec) -> tuple[RunContext, deque[KnownPayload]]:
        """Give this run its control gate, its report buffer and the agent it plays.

        The Runtime, not the caller, decides whether a run is cancellable and where its status
        and progress reports go  -  a caller builds a plain ``RunContext`` and never has to know
        that a ``ControlPort`` or a buffer exists. The buffer is per run and returned rather than
        stored, so two concurrent runs on one Runtime can never drain into each other.

        ``ctx.agent`` comes off the resolved spec rather than from whoever called in, which is
        what makes it the same instance on a fresh play, a lifted pause and an answered interrupt:
        every one of those resolves the spec, and none of them could be relied on to pass it.
        """
        reports: deque[KnownPayload] = deque()
        gate = (
            ctx.gate
            if self._control is None
            else Gate(self._control, ctx.id, poll_interval=self._control_poll_interval)
        )
        return replace(ctx, gate=gate, reporter=Reporter(reports), agent=spec.metadata.get("agent")), reports

    def delegate(self, run_id: str, parent_run_id: str | None, invocable: str) -> None:
        """Place ``run_id`` in the delegation tree, refusing it if that puts the tree past a bound.

        Called wherever a run is played, so a child answered or resumed in a later segment is
        still known to be one: the first call has the edge from its invoker, the rest read it back
        off the ``run.started`` the first one wrote. A run already placed is left alone  -  a second
        segment of one run is not a second child.

        Public and synchronous because the invoker behind ``ctx.invoke`` has to be refused *at the
        call*: it hands back a handle without awaiting anything, so a bound raised later would
        surface as a handle on a run that was never opened.

        The bounds are deliberately conservative and deliberately not configurable. Two agents
        that can delegate to each other will, and each level multiplies: at depth 3 and fan-out 8
        the worst case before this refuses is 512 child runs, where 5 and 16 is over a million.
        """
        if run_id in self._tree:
            return
        above = self._tree.get(parent_run_id) if parent_run_id is not None else None
        depth = 0 if parent_run_id is None else (above.depth + 1 if above is not None else 1)
        if above is not None:
            if depth > MAX_DELEGATION_DEPTH:
                raise ConfigError(
                    f"{above.invocable!r} cannot delegate to {invocable!r}: that is {depth} levels "
                    f"of delegation and a tree is bounded at {MAX_DELEGATION_DEPTH}. Mutual "
                    f"delegation is the usual cause  -  have this level do the work, or start the "
                    f"deeper task as a run of its own."
                )
            if above.children + 1 > MAX_DELEGATION_FANOUT:
                raise ConfigError(
                    f"{above.invocable!r} cannot delegate to {invocable!r}: it has already started "
                    f"{above.children} child runs and one run is bounded at "
                    f"{MAX_DELEGATION_FANOUT}. Give it fewer, larger tasks, or hand the fan-out to "
                    f"a workflow that starts them as runs of its own."
                )
            above.children += 1
        self._tree[run_id] = _Delegation(parent=parent_run_id, depth=depth, invocable=invocable)

    def _rolling_up(self, payload: KnownPayload, ctx: RunContext) -> KnownPayload:
        """A terminal payload with what this run delegated folded into it, and its own total
        handed on to whoever delegated *it*.

        One level at a time is the whole tree: a child's total already carries its own children's
        by the time its parent reads it. A child that ends any way but ``run.completed`` has no
        total to contribute, and one still running when its parent ends is not in the fold  -
        which is what ``run.started.parent_run_id`` is for, since a reader of the log can follow
        the edge afterwards and add up what the live sum could not.
        """
        if payload.kind not in TERMINAL_KINDS:
            return payload
        placed = self._tree.pop(ctx.run_id, None)
        if placed is None or not isinstance(payload, RunCompleted):
            return payload
        total = payload.usage if placed.spent is None else _summed(payload.usage, placed.spent)
        above = self._tree.get(placed.parent) if placed.parent is not None else None
        if above is not None:
            above.spent = total if above.spent is None else _summed(above.spent, total)
        return payload if placed.spent is None else payload.model_copy(update={"usage": total})

    async def _drain(
        self, reports: deque[KnownPayload], spec: InvocableSpec, ctx: RunContext
    ) -> AsyncGenerator[Event, None]:
        """Record whatever the run reported about itself since the last event.

        Called just *before* each engine payload, never after: that payload may be terminal, and
        nothing may follow a terminal event into the log. So a report is always in order and
        always inside the run, and one emitted after the engine's final payload is dropped  -  the
        ceiling ``core/reporting.py`` states.

        The count is taken once. A report arriving while these are being written belongs to the
        next payload's batch, so an emitter in a loop cannot starve the engine's own event.

        A store that refuses a report costs the report, never the run: an advisory event is not
        worth a run, and the alternative is a store that dislikes one *kind* turning a run that
        would have completed into ``run.failed``. It costs the report only  -  a refused append
        never took a number, so the log this leaves behind is dense.
        """
        for _ in range(len(reports)):
            payload = reports.popleft()
            try:
                yield await self._record(payload, spec, ctx)
            except StoreError:
                logger.warning("run %s could not record its %s; dropping the report", ctx.run_id, payload.kind)

    def _resolve(self, name: str) -> tuple[InvocableSpec, Executor]:
        spec = self._invocables.get(name)
        if spec is None:
            raise NotFoundError(f"no invocable named {name!r}")
        executor = self._executors.get(spec.executor)
        if executor is None:
            raise NotFoundError(f"{name!r} needs executor {spec.executor!r}, which is not registered")
        return spec, executor

    async def _record(self, payload: KnownPayload, spec: InvocableSpec, ctx: RunContext) -> Event:
        """Persist, fan out, return the event to yield  -  in that order.

        The store stamps it (ADR-D11): ``seq`` and ``ts`` are assigned in the same indivisible
        step that writes the row, so a refused append cannot leave a number spent. Nothing here
        holds a counter to get wrong.

        A run :meth:`close_cancelled` abandoned is cancelled here instead, at its next write. The
        check is synchronous and every writer for such a run shares this event loop, so no append
        that starts here can land past the terminal event written for it. One that started before
        the mark and is still suspended inside the store can, which is #421 and needs the store's
        own conditional append rather than a second guard above it.

        Every terminal event in this class is written here, which is why the delegation roll-up
        hangs off this one call rather than off the engine loop: a run that failed or was
        cancelled ends the edge exactly as a completed one does. It runs after the refusal above,
        never before, because folding a total into a parent for an append that is about to be
        refused would bill the parent for an event the log never gets.
        """
        if ctx.run_id in self._abandoned:
            raise asyncio.CancelledError(f"run {ctx.run_id} was abandoned by the deck closing")
        event = (await self._store.append([self._rolling_up(payload, ctx)], ctx, spec.name))[0]
        await self._fan_out(event)
        return event

    async def _fan_out(self, event: Event) -> None:
        """Sinks get a copy of the stream and no say in it: never called inline, never fatal.

        Each sink gets a queue put rather than an ``emit``, and a full queue costs one loop
        turn before it starts dropping  -  so the run is never waiting on a sink, only ever on
        the loop it already shares with one.
        """
        for dispatch in self._sinks:
            await dispatch.submit(event)


def _summed(one: Usage, other: Usage) -> Usage:
    """Two totals as one. ``usd`` stays ``None`` unless somebody set it: agentdeck never does, and
    a zero would read as a priced call that cost nothing."""
    priced = [amount for amount in (one.usd, other.usd) if amount is not None]
    return Usage(
        input_tokens=one.input_tokens + other.input_tokens,
        output_tokens=one.output_tokens + other.output_tokens,
        usd=sum(priced) if priced else None,
    )


def _failed(exc: Exception, engine: str) -> RunFailed:
    """The record for an exception the Runtime caught. The type name only  -  an exception message
    can carry content that must not reach a sink.

    A log that could not be written is not the engine misbehaving, so the record does not say it
    was. ``error_code`` stays ``engine_error`` either way: the closed set has no entry for a store
    fault, and minting one is a schema change rather than this line's business.
    """
    if isinstance(exc, StoreError):
        return _engine_failed(f"{type(exc).__name__} recording this run")
    return _engine_failed(f"{type(exc).__name__} in engine {engine!r}")


def _engine_failed(message: str) -> RunFailed:
    return RunFailed(error_code="engine_error", message=message, retryable=False)


def _refuses(events: Sequence[Event], value: Any) -> str | None:
    """Why ``value`` is not an answer to what this run asked, or ``None`` when it is.

    Only a run that named its options can refuse anything: a plain question takes whatever it is
    given, because nothing here can judge a free-form answer better than the body can. The options
    travel on the ``run.interrupted`` itself, so this reads the run's own record of what it asked
    rather than a second place that could disagree with it.

    The message names the type that arrived, never the value: a refused answer can carry whatever
    the answerer typed, and this reaches every sink.
    """
    last = _last_interrupt(events)
    if last is None:
        return None
    options = last[1].payload.get("options")
    if not isinstance(options, list) or value in options:
        return None
    return (
        f"this run is waiting for one of {options!r} and got a {type(value).__name__}; the run is "
        f"still waiting, so answering it again with one of them still works."
    )


def _last_interrupt(events: Sequence[Event]) -> tuple[Event, RunInterrupted] | None:
    for event in reversed(events):
        if isinstance(event.payload, RunInterrupted):
            return event, event.payload
    return None


__all__ = ["PendingRun", "Runtime"]
