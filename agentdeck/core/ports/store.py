"""The event log  -  the platform's record of what happened.

Not the engine's execution state: an engine that needs its exact prior items keeps them
privately (ADR-D5). This log is what replay, audit and every surface read from.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from agentdeck.core.status import RunStatus, status_of

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import timedelta

    from agentdeck.core.context import RunContext
    from agentdeck.core.events import Event, KnownPayload, RunResumed, RunStarted


@dataclass(frozen=True, slots=True)
class RunSummary:
    """One run as :meth:`EventStorePort.list_runs` projects it  -  never a stored row of its
    own (ADR-D5: the log stays the sole source of truth).

    ``session_id`` is the conversation this run belongs to, or ``None`` when it stands alone. Not
    a partition key to decode: a caller reads it, it does not compare it to ``run_id`` to work out
    whether a session existed.
    """

    run_id: str
    session_id: str | None
    status: RunStatus


@dataclass(frozen=True, slots=True)
class SessionClaim:
    """What one :meth:`EventStorePort.claim_start` decided.

    ``held_by`` names the open run that refused the claim, ``None`` when the claim won.

    ``overridden`` is the **last event of** every run a winning claim stepped over as abandoned  -
    not merely its id. The store wrote nothing for those: closing a run means deciding it is
    abandoned, which is judgement, and the store only reports what it saw. It saw these events
    already, having compared each one's ``ts`` to decide the run was stale, so handing them back
    costs nothing and saves the caller a read it would otherwise need to reconstruct the closing
    event's envelope (ADR-D11 §5).
    """

    held_by: str | None = None
    overridden: tuple[Event, ...] = ()


class EventStorePort(ABC):
    """Append-only. A log holds every run of one session, so it is ordered by *append*, not
    by ``seq``  -  ``seq`` restarts at 0 for each run and only orders events within it.

    Where an event goes is ``ctx``'s to say and never a parameter: ``run_id`` is what it belongs
    to, ``session_id`` is the conversation it is part of when there is one. A store that was
    handed one encoded key instead could not tell a session named after a run from the run
    itself, and had to be asked twice for what one context already knew.

    **The store assigns ``seq`` and ``ts``** (ADR-D11), in the same indivisible step that
    persists the event. Callers hand over payloads and get finished events back. That is what
    makes ``seq`` dense: a number allocated and persisted together cannot be allocated and not
    persisted, so a gap means an event was genuinely lost and refetching it converges.

    Every other envelope field comes from ``ctx``  -  ``run_id``, ``session_id``, ``namespace``  -  plus
    ``origin``, which is the invocable the caller addressed. A store never decides what an event
    *means*; it refuses what would corrupt the log and reports what it saw.
    """

    @abstractmethod
    async def append(self, payloads: Sequence[KnownPayload], ctx: RunContext, origin: str) -> list[Event]:
        """Stamp and write ``payloads`` in the order given, returning the finished events.

        Each gets this run's next ``seq`` and the store's own ``ts``, assigned inside whatever
        the backend uses to make the write indivisible. Must return only once they are durable,
        because the Runtime yields to consumers immediately after.

        Raises ``RunStateError`` for a run that is already ``CANCELLED``, decided inside the same
        indivisible step as the write. A cancel is written from outside a run whose task is still
        alive, so one of that run's own appends can already be suspended in here when the terminal
        event lands, and the write step is the only place left that can still stop it.

        A takeover's ``run.failed`` deliberately refuses nothing: it is written for a run only
        *believed* dead, and one that turns out to be alive goes on writing (ADR-D11 §5).

        Run identity comes from ``ctx`` alone. The one write that belongs to a *different* run  -
        the terminal event a takeover stamps for a run it stepped over  -  passes a ``ctx`` built
        for that run, from the event :class:`SessionClaim` handed back. There is no override
        parameter, because a caller that could address any run could file an event under a run it
        is not playing.
        """

    @abstractmethod
    async def read_session(self, ctx: RunContext, offset: int = 0, limit: int | None = None) -> list[Event]:
        """Every run of ``ctx.session_id``, in append order, oldest first  -  empty when this run
        has no session, which is a run that is part of no conversation rather than a missing one.

        ``offset`` counts from the start of the log, not a ``seq`` cursor  -  ``seq`` restarts per
        run, so it cannot address a position in a log holding several. Safe to page with a plain
        counter: the log only grows at the end.

        A negative ``offset`` means 0; a negative ``limit`` raises ``ValueError`` rather than
        quietly meaning "all" in one store and "none" in another.
        """

    @abstractmethod
    async def read_run(self, ctx: RunContext, from_seq: int = 0) -> list[Event]:
        """``ctx.run_id``'s events from ``from_seq`` onward, inclusive  -  what a consumer calls
        to refetch after spotting a gap.

        The context says which run, here and everywhere else on this port: a second ``run_id``
        parameter beside it is one fact with two sources, and the two can disagree. A caller
        reading a run other than the one it is playing says so by building the context for it
        (``dataclasses.replace(ctx, run_id=...)``), which is what the takeover write already did.
        """

    @abstractmethod
    async def claim_start(
        self,
        opening: RunStarted,
        ctx: RunContext,
        origin: str,
        stale_after: timedelta,
        *,
        dead: frozenset[str] = frozenset(),
    ) -> tuple[SessionClaim, Event | None]:
        """Stamp and append ``opening``  -  a run's ``run.started``  -  if and only if this run's
        session has no open run, in one indivisible step. One session runs one turn at a time. The
        event is returned when the claim won, ``None`` when it lost.

        A run with no session claims nothing: there is no conversation for a second turn to
        collide over, and its own ``run_id`` was minted, so nothing else can be opening it.

        Condition and write must be one operation, which is what carries this across processes:
        two servers sharing a store would both read an idle session and both open a run on it.

        An **open run** recorded a lifecycle transition and no terminal one. ``WAITING_ANSWER``
        counts  -  an interrupted run still owns its engine's thread, and a second run against it
        would overwrite the checkpoints the first resumes from. A run with no transition at all has
        no status, indistinguishable from one the store never saw, so it holds nothing.

        Losing never raises  -  two turns at once is a double-clicked send button, so the refusal is
        data. An unreachable store does raise: it cannot know whether anybody holds anything.

        ``dead`` names the runs the caller **positively knows** are not being executed  -  a
        :class:`~agentdeck.core.ports.lease.LeasePort` held and watched expire. Such a run stops
        holding the session at once, whatever its last event's age, and comes back in
        ``overridden``. Deciding a run is dead is the caller's, not the store's: the store has no
        witness of any process but the one asking.

        ``stale_after`` is the backstop for every run nothing knows anything about, which is all
        of them when no lease backend is shared: how long a **running** open run may be silent
        before it stops holding the session. One whose last event is older than that comes back in
        ``overridden`` too. A duration rather than a cutoff instant, because the caller no longer
        owns the clock the comparison is made in  -  the store does, and only it can subtract from
        its own now. Without either of these a process killed mid-run wedges its session for good.

        **Neither applies to ``PAUSED`` or ``WAITING_ANSWER``**, and suspension is checked first:
        both are suspended by definition  -  no worker is executing them  -  so there is no worker to
        be dead, silence is not evidence of anything, and a lease that expired because the run
        parked says only that. A parked run holds its session
        until something acts on it: :meth:`claim_resume`, or a cancel recorded against it. Held
        forever if nobody ever does either, which is the deliberate trade  -  a wedged session is
        recoverable by an explicit cancel; a silently destroyed approval is not.

        Also adopts ``ctx.key`` when one is given: ``(namespace, key)`` is a second, permanent
        claim made in the same indivisible step as the session claim above, so two callers racing
        on one key can never both open a run for it. Raises :class:`~agentdeck.errors.
        DuplicateKeyError` when the key is already taken  -  by a run in this log or any other  -
        rather than returning the run that holds it, matching the deterministic-failure reading
        of "a lost race never yields a second run for one logical identity". Enforced by the
        store's own uniqueness, not a read-then-write check in Python, so it holds across
        processes the same way the session claim does.
        """

    @abstractmethod
    async def claim_resume(self, resumed: RunResumed, ctx: RunContext, origin: str) -> Event | None:
        """Stamp and append ``resumed`` if and only if ``ctx.run_id`` is ``WAITING_ANSWER``, in
        one indivisible step. The event when appended, ``None`` when not.

        The write publishing the ``WAITING_ANSWER`` -> ``RUNNING`` transition is the same write
        that tests for it, which is what makes double-resume protection hold between processes
        and not merely between tasks. A store that cannot do both indivisibly must not implement
        this port. A concurrent loser reads ``RUNNING``  -  the winner's append is what flipped it  -
        and gets ``None``.

        Status is the whole condition now. It used to be paired with a stale-``seq`` check, which
        existed because the caller stamped before claiming; the caller no longer holds a ``seq`` to
        go stale. Note what that check did *not* cover and still does not: a resume can answer an
        interrupt other than the one in flight, because nothing here names which interrupt is being
        answered. Recording that is #94's business, in a schema PR.

        ``None`` on any other status or a lost race; a stray resume is a no-op by design
        (``can_resume``). An unreachable store raises instead, because ``None`` claims somebody
        else recorded this resume, which it cannot know.
        """

    @abstractmethod
    async def list_runs(
        self, ctx: RunContext, status: RunStatus | None = None, limit: int | None = None
    ) -> list[RunSummary]:
        """Every run in this namespace that recorded a lifecycle transition, sessioned or not,
        optionally narrowed to one status and capped at ``limit`` entries.

        A run with no transition at all is left out, being indistinguishable from one the store
        never heard of.
        Index this however the store can: finding waiting runs must not cost a fold of every run
        the namespace owns.

        A negative ``limit`` raises ``ValueError``, the same pin :meth:`read_session` uses rather
        than one store treating it as "no limit" and another as "none".
        """

    @abstractmethod
    async def find_by_key(self, ctx: RunContext, key: str) -> str | None:
        """The run id claimed under ``(ctx.namespace, key)``, or ``None`` if nothing has.

        The read side of the permanent claim :meth:`claim_start` enforces on ``ctx.key``  -  what
        ``deck.runs.get(namespace=, key=)`` resolves against to reach the same run
        :meth:`claim_start` refused to open twice. ``ctx.run_id`` is not part of this lookup;
        only ``ctx.namespace`` scopes it, the same throwaway-context shape :meth:`list_runs`
        already takes for a query that has no one run of its own.

        A namespace that never claimed ``key`` and a claim made under a different namespace
        both answer ``None``  -  indistinguishable, as everywhere else in this port.
        """

    async def run_status(self, ctx: RunContext) -> RunStatus | None:
        """One run's status, derived from its own events only  -  never the whole log. ``None``
        when the run has no events at all: a run this store never heard of, which is also what
        a run that exists but has logged no lifecycle transition yet looks like. No status
        names that case, because ``run.started`` is a run's row 0.

        Default projection: fold :meth:`read_run` through ``status_of`` (ADR-D5: a projection,
        not a second store). A store with a cheaper way to answer may override it.
        """
        events = await self.read_run(ctx)
        return status_of(events) if events else None
