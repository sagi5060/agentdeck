"""The event log in a dict: the default for dev, tests and the contract suite."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from agentdeck.adapters.stores import _refuse_if_cancelled
from agentdeck.core.events import Event
from agentdeck.core.ports import EventStorePort, RunSummary, SessionClaim
from agentdeck.core.status import LIFECYCLE_KINDS, STATES, RunStatus, can_resume, status_of
from agentdeck.errors import DuplicateKeyError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import timedelta

    from agentdeck.core.context import RunContext
    from agentdeck.core.events import KnownPayload, RunResumed, RunStarted


def _now() -> datetime:
    return datetime.now(UTC)


class MemoryEventStore(EventStorePort):
    """Append-only lists: one per run, and one per session holding the same events again.

    Two indexes rather than one keyed by "the session, or the run when there is none": that
    encoding cannot tell a session named after a run from the run itself, and a caller reading a
    run back had to know which of the two it was looking at.

    Keyed by namespace as well, so two namespaces that pick the same session id cannot read each
    other's runs  -  isolation is not something a store gets to skip.

    Process exit is data loss, by design.
    """

    def __init__(self, clock: Callable[[], datetime] = _now) -> None:
        # Every run's own events, in seq order. The authority: a session list holds the same
        # Event objects again, which is a projection for reading a conversation in append order.
        self._runs: dict[tuple[str | None, str], list[Event]] = {}
        self._sessions: dict[tuple[str | None, str], list[Event]] = {}
        self._clock = clock
        # `(namespace, key)` is the store's own permanent claim, set once by whichever
        # `claim_start` first adopts a key and never cleared  -  this dict *is* the enforcement,
        # not a cache of something re-derivable from the events.
        self._keys: dict[tuple[str | None, str], str] = {}

    async def append(self, payloads: Sequence[KnownPayload], ctx: RunContext, origin: str) -> list[Event]:
        # The fold and ``_stamp`` are plain dict work with no suspension point between them, as in
        # ``claim_resume``, so the refusal and the write are one step.
        _refuse_if_cancelled(status_of(self._runs.get((ctx.namespace, ctx.run_id), [])), ctx)
        events = self._stamp(payloads, ctx, origin)
        # Fidelity, not correctness (issue #87): every real store suspends here (SQLite's own
        # `to_thread`), so a caller whose liveness secretly depends on that turn is caught by
        # this store too, instead of only by measurement in production. Placed after the
        # mutation in `_stamp`, so it opens no window in either claim's atomicity.
        await asyncio.sleep(0)
        return events

    def _stamp(self, payloads: Sequence[KnownPayload], ctx: RunContext, origin: str) -> list[Event]:
        """Assign, build and write, with no suspension point anywhere in between.

        That is this store's whole atomicity mechanism, and it is why every caller here  -  both
        claims included  -  goes through this rather than through ``append``: an ``await`` between
        reading the run's last ``seq`` and extending the log is all it would take for two tasks to
        be handed the same number.
        """
        run = self._runs.setdefault((ctx.namespace, ctx.run_id), [])
        seq = run[-1].seq if run else -1
        events = []
        for payload in payloads:
            seq += 1
            events.append(
                Event(
                    kind=payload.kind,
                    seq=seq,
                    run_id=ctx.run_id,
                    session_id=ctx.session_id,
                    namespace=ctx.namespace,
                    origin=origin,
                    ts=self._clock(),
                    payload=payload,
                )
            )
        run.extend(events)
        if ctx.session_id is not None:
            self._sessions.setdefault((ctx.namespace, ctx.session_id), []).extend(events)
        return events

    async def read_session(self, ctx: RunContext, offset: int = 0, limit: int | None = None) -> list[Event]:
        if limit is not None and limit < 0:
            raise ValueError(f"limit must be None or >= 0, got {limit}")
        if ctx.session_id is None:
            return []
        log = self._sessions.get((ctx.namespace, ctx.session_id), ())
        page = log[max(offset, 0) :]
        return list(page if limit is None else page[:limit])

    async def read_run(self, ctx: RunContext, from_seq: int = 0) -> list[Event]:
        run = self._runs.get((ctx.namespace, ctx.run_id), ())
        return [event for event in run if event.seq >= from_seq]

    async def claim_start(
        self,
        opening: RunStarted,
        ctx: RunContext,
        origin: str,
        stale_after: timedelta,
        *,
        dead: frozenset[str] = frozenset(),
    ) -> tuple[SessionClaim, Event | None]:
        """Atomic for free, like ``claim_resume``: the scan and ``_stamp`` are plain dict work
        with no suspension point between them, so no other task can open a run in the gap.

        A busy session wins over a reused key: the session scan runs first and can return a
        refusal before ``ctx.key`` is even looked at, matching sqlite/postgres, where the
        session check is a read and the key check is a constraint the INSERT itself enforces.
        Only once the session is free does a reused key raise instead of silently starting.
        """
        stale_before = self._clock() - stale_after
        overridden: list[Event] = []
        for events in self._session_runs(ctx):
            status = status_of(events)
            if status is None or STATES[status].terminal:
                continue
            if STATES[status].suspended:
                # No worker to be dead: PAUSED and WAITING_ANSWER have no engine polling a
                # clock, so silence is not evidence of anything and neither the timer nor an
                # expired lease applies  -  checked before both, for that reason. The log
                # deciding alone is what makes this session's hold permanent.
                return SessionClaim(held_by=events[-1].run_id), None
            if events[-1].run_id in dead:
                overridden.append(events[-1])
                continue
            if events[-1].ts > stale_before:
                return SessionClaim(held_by=events[-1].run_id), None
            overridden.append(events[-1])
        if ctx.key is not None and (holder := self._keys.get((ctx.namespace, ctx.key))) is not None:
            raise DuplicateKeyError(f"key {ctx.key!r} is already used by run {holder!r} in namespace {ctx.namespace!r}")
        event = self._stamp([opening], ctx, origin)[0]
        if ctx.key is not None:
            self._keys[(ctx.namespace, ctx.key)] = ctx.run_id
        await asyncio.sleep(0)
        return SessionClaim(overridden=tuple(overridden)), event

    def _session_runs(self, ctx: RunContext) -> list[list[Event]]:
        """The runs this one's session already holds, each still in seq order  -  and nothing at
        all when there is no session, which is what makes a standalone run claim nothing: there
        is no conversation for a second turn to collide over, and its ``run_id`` was minted.
        """
        if ctx.session_id is None:
            return []
        runs: dict[str, list[Event]] = {}
        for event in self._sessions.get((ctx.namespace, ctx.session_id), ()):
            runs.setdefault(event.run_id, []).append(event)
        return list(runs.values())

    async def claim_resume(self, resumed: RunResumed, ctx: RunContext, origin: str) -> Event | None:
        """Atomic for free: the status fold and ``_stamp`` are plain dict work with no suspension
        point between them, so no other task can slip in and claim the same run.
        """
        if not can_resume(status_of(self._runs.get((ctx.namespace, ctx.run_id), []))):
            return None
        event = self._stamp([resumed], ctx, origin)[0]
        await asyncio.sleep(0)
        return event

    async def list_runs(
        self, ctx: RunContext, status: RunStatus | None = None, limit: int | None = None
    ) -> list[RunSummary]:
        if limit is not None and limit < 0:
            raise ValueError(f"limit must be None or >= 0, got {limit}")
        summaries = []
        for (namespace, run_id), events in self._runs.items():
            if namespace != ctx.namespace or not any(event.kind in LIFECYCLE_KINDS for event in events):
                # A run with no lifecycle event is left out for the reason `run_status` returns
                # None for it: indistinguishable from a run this store never heard of.
                continue
            found = status_of(events)
            assert found is not None  # the guard above kept only runs that have one
            summaries.append(RunSummary(run_id=run_id, session_id=events[0].session_id, status=found))
        filtered = [summary for summary in summaries if status is None or summary.status is status]
        return filtered if limit is None else filtered[:limit]

    async def find_by_key(self, ctx: RunContext, key: str) -> str | None:
        return self._keys.get((ctx.namespace, key))


# The unique-index equivalent this store used to need is gone: no caller supplies a ``seq``, and
# ``_stamp`` reads the run's last one and extends its list with no suspension in between, so two
# events at one ``seq`` is unconstructible rather than merely refused (ADR-D11 §6).


__all__ = ["MemoryEventStore"]
