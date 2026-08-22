"""Store contract: the focused queries  -  ``run_status``, ``list_runs`` and paginated ``read``  -
plus the two claims and the assignment of ``seq`` itself, behaving identically on every store,
parametrized the same way the engine cases are. Ordering/tenancy/round-trip invariants for
``append``, ``read`` and ``read_run`` already live in ``tests/test_memory_store.py`` and
``tests/test_sqlite_store.py``; this file covers only the newer focused ops.

Parametrized over all four stores: memory, SQLite, and  -  on real servers, skipping with a
reason when there is none  -  Redis and Postgres (``live_stores``). Backend-specific evidence
that needs no second store lives beside each one instead: ``tests/test_sqlite_store.py``,
``tests/test_redis_store.py``, ``tests/test_postgres_store.py``.

Callers here hand over payloads and never envelopes: the store assigns ``seq`` and ``ts`` in the
same indivisible step that persists the event, and every other envelope field comes from the
``RunContext``. So a case that writes for a second run passes a context built for it, and a case
about a second namespace passes that namespace's context  -  there is no field left to mis-stamp.

The last case is a boundary invariant rather than a query one, and covers both SQLite-backed
ports by shape: whatever fails underneath, callers see the harness's own error type.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from time import monotonic
from typing import TYPE_CHECKING, Any

import live_stores
import pytest

from agentdeck.adapters.control.sqlite import SqliteControlPort
from agentdeck.adapters.stores.sqlite import SqliteEventStore
from agentdeck.adapters.stores.sqlite import store as sqlite_store
from agentdeck.core.context import RunContext
from agentdeck.core.control import Signal
from agentdeck.core.events import (
    Event,
    KnownPayload,
    RunCancelled,
    RunCompleted,
    RunFailed,
    RunInterrupted,
    RunPaused,
    RunResumed,
    RunStarted,
    TextDelta,
)
from agentdeck.core.ports import SessionClaim
from agentdeck.core.status import RunStatus
from agentdeck.errors import DuplicateKeyError, RunStateError, StoreError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Sequence
    from pathlib import Path

    from agentdeck.core.ports import EventStorePort

ORIGIN = "Greeter"


@pytest.fixture(params=live_stores.BACKENDS)
async def event_store(request: pytest.FixtureRequest) -> AsyncIterator[EventStorePort]:
    """Every case against every store  -  including Redis and Postgres on real servers, which
    skip with a reason naming the env var when there is none (``live_stores``)."""
    async with live_stores.event_store(request.param) as store:
        yield store


@pytest.fixture(params=live_stores.BACKENDS)
async def two_event_stores(request: pytest.FixtureRequest) -> AsyncIterator[tuple[EventStorePort, EventStorePort]]:
    """Two handles on one keyspace, for the promises that only hold between two writers."""
    async with live_stores.two_event_stores(request.param) as pair:
        yield pair


def _ctx(
    namespace: str = "acme", run_id: str = "r-1", session_id: str | None = "s-1", key: str | None = None
) -> RunContext:
    """The context a write is made in  -  which is now the whole envelope bar ``origin``."""
    return RunContext(namespace=namespace, run_id=run_id, session_id=session_id, key=key)


def _started() -> RunStarted:
    return RunStarted(
        invocable=ORIGIN,
        kind_of_invocable="agent",
        input=[],
        context={"trace_id": "tr-1"},
    )


def _completed() -> RunCompleted:
    return RunCompleted(output=[], usage={"input_tokens": 1, "output_tokens": 1})


def _interrupted(interrupt_id: str = "i-1", thread_id: str | None = "t-1") -> RunInterrupted:
    return RunInterrupted(interrupt_id=interrupt_id, reason="human", payload={"q": "ok?"}, thread_id=thread_id)


async def _write(store: EventStorePort, payloads: Sequence[KnownPayload], ctx: RunContext) -> list[Event]:
    """One append into ``ctx``'s own run and log  -  the shape nearly every case here wants."""
    return await store.append(payloads, ctx, ORIGIN)


async def test_redis_keyspace_prefix_is_disjoint_across_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two processes racing `make check` must never hand out the same prefix.

    Simulates that by swapping ``live_stores._run``  -  the per-process seed  -  between two calls
    on one live Redis, standing in for two pytest processes without actually spawning them.
    """
    monkeypatch.setattr(live_stores, "_run", "aaaa")
    async with live_stores.redis_keyspace() as (_, first_prefix):
        pass
    monkeypatch.setattr(live_stores, "_run", "bbbb")
    async with live_stores.redis_keyspace() as (_, second_prefix):
        pass

    assert first_prefix != second_prefix
    assert first_prefix.startswith("agentdeck:test:aaaa-")
    assert second_prefix.startswith("agentdeck:test:bbbb-")
    # The ordered suffix the comment promises to keep is still shared and still increasing,
    # not reset per seed  -  only the seed makes two processes' prefixes disjoint.
    assert int(second_prefix.rsplit("-", 1)[1]) > int(first_prefix.rsplit("-", 1)[1])


async def test_run_status_with_no_events_is_none(event_store: EventStorePort) -> None:
    """A run this store never heard of has no status at all. There is deliberately no member
    naming that case: it would be indistinguishable from a run that exists but has logged no
    lifecycle transition yet, and ``run.started`` is a run's row 0 so neither ever happens."""
    assert await event_store.run_status(replace(_ctx(), run_id="r-1")) is None


async def test_run_status_follows_the_last_lifecycle_transition(event_store: EventStorePort) -> None:
    ctx = _ctx()
    await _write(event_store, [_started(), _interrupted()], ctx)
    assert await event_store.run_status(replace(ctx, run_id="r-1")) is RunStatus.WAITING_ANSWER


async def test_run_status_is_scoped_to_one_run_not_the_whole_log(event_store: EventStorePort) -> None:
    ctx = _ctx()
    await _write(event_store, [_started()], ctx)
    await _write(event_store, [_started(), _completed()], _ctx(run_id="r-2"))
    assert await event_store.run_status(replace(ctx, run_id="r-1")) is RunStatus.RUNNING
    assert await event_store.run_status(replace(ctx, run_id="r-2")) is RunStatus.COMPLETED


async def test_find_by_key_of_an_unclaimed_key_is_none(event_store: EventStorePort) -> None:
    assert await event_store.find_by_key(_ctx(), "order-1234") is None


async def test_find_by_key_resolves_to_the_run_that_claimed_it(event_store: EventStorePort) -> None:
    """The read side of the permanent claim ``claim_start`` makes on ``ctx.key``  -  plain
    ``append`` never sets it, only the claim that adopts a key does."""
    ctx = _ctx(run_id="r-1", key="order-1234")
    claim, event = await event_store.claim_start(_started(), ctx, ORIGIN, timedelta(hours=1))
    assert claim.held_by is None and event is not None

    assert await event_store.find_by_key(ctx, "order-1234") == "r-1"


async def test_find_by_key_never_reaches_into_another_namespaces_claim(event_store: EventStorePort) -> None:
    """The same key claimed in one namespace is not another namespace's to find  -  the isolation
    ``(namespace, key)``'s uniqueness only promises within one namespace."""
    ctx = _ctx("acme", run_id="r-1", key="order-1234")
    await event_store.claim_start(_started(), ctx, ORIGIN, timedelta(hours=1))

    assert await event_store.find_by_key(_ctx("globex"), "order-1234") is None


async def test_find_by_key_tells_two_namespaces_sharing_one_key_apart(event_store: EventStorePort) -> None:
    """#324: two namespaces may each claim the identical key and get two different runs; each
    namespace's own lookup resolves to its own run, never the other's."""
    acme = _ctx("acme", run_id="r-acme", key="order-1234")
    globex = _ctx("globex", run_id="r-globex", key="order-1234")
    await event_store.claim_start(_started(), acme, ORIGIN, timedelta(hours=1))
    await event_store.claim_start(_started(), globex, ORIGIN, timedelta(hours=1))

    assert await event_store.find_by_key(acme, "order-1234") == "r-acme"
    assert await event_store.find_by_key(globex, "order-1234") == "r-globex"


# --- session_id=None: a run that belongs to no conversation -----------------------------------


async def test_read_session_of_a_standalone_run_is_empty(event_store: EventStorePort) -> None:
    """A run with no session has no conversation to read: ``read_session`` answers empty rather
    than falling back to the run's own log."""
    ctx = _ctx(session_id=None)
    await _write(event_store, [_started()], ctx)

    assert await event_store.read_session(ctx) == []


async def test_a_standalone_run_is_still_read_by_name_and_answers_its_own_status(
    event_store: EventStorePort,
) -> None:
    """No session to read it through is not no way to reach it: ``read_run`` and ``run_status``
    answer from the run's own log alone, which is the whole point of naming a run by run_id."""
    ctx = _ctx(session_id=None)
    await _write(event_store, [_started()], ctx)

    assert [event.kind for event in await event_store.read_run(ctx)] == ["run.started"]
    assert await event_store.run_status(ctx) is RunStatus.RUNNING


async def test_list_runs_reports_a_standalone_run_with_no_session(event_store: EventStorePort) -> None:
    ctx = _ctx(session_id=None)
    await _write(event_store, [_started()], ctx)

    [summary] = await event_store.list_runs(ctx)
    assert (summary.run_id, summary.session_id) == ("r-1", None)


# ``stale_after`` is a duration measured against the store's own clock, so a case decides
# staleness by asking for a window nothing written in this test can fall outside  -  or inside.
# The two cases that need a run stale *beside* a live one arrange a real age gap instead.
NOTHING_IS_STALE = timedelta(hours=1)
EVERYTHING_IS_STALE = timedelta(0)


async def test_find_by_key_resolves_a_standalone_runs_key(event_store: EventStorePort) -> None:
    ctx = _ctx(session_id=None, key="order-1234")
    claim, event = await event_store.claim_start(_started(), ctx, ORIGIN, NOTHING_IS_STALE)
    assert claim == SessionClaim() and event is not None

    assert await event_store.find_by_key(ctx, "order-1234") == "r-1"


async def test_two_standalone_runs_never_see_each_other_as_a_session_already_held(
    event_store: EventStorePort,
) -> None:
    """A run with no session claims nothing: unlike two runs sharing a session, opening one
    standalone run after another must never refuse with ``held_by`` the first run's id."""
    await event_store.claim_start(_started(), _ctx(run_id="r-1", session_id=None), ORIGIN, EVERYTHING_IS_STALE)

    claim, event = await event_store.claim_start(
        _started(), _ctx(run_id="r-2", session_id=None), ORIGIN, EVERYTHING_IS_STALE
    )
    assert claim == SessionClaim() and event is not None


# Long enough that the fresher event is comfortably inside the window while the older one is
# outside it, short enough to cost a fraction of a second per backend.
AGE_GAP = 0.5


def _window_between(older: Event, newer: Event) -> timedelta:
    """A ``stale_after`` landing strictly between two real events, measured from the stamps the
    store wrote rather than from how long this process believes it slept.

    Nine tenths of the measured gap, not half. The window has to stay wider than the delay
    between the last write and the claim's own clock read, and that delay is the one quantity a
    case here cannot bound  -  it is where a fixed window flakes. Taking most of the gap buys the
    largest margin the two events allow, and reading the gap back means a stall between the two
    writes widens the window instead of eating into it.
    """
    return (newer.ts - older.ts) * 0.9


async def test_claim_start_opens_a_run_on_an_idle_session(event_store: EventStorePort) -> None:
    ctx = _ctx()
    claim, event = await event_store.claim_start(_started(), ctx, ORIGIN, NOTHING_IS_STALE)

    assert claim == SessionClaim()
    assert event is not None and (event.run_id, event.seq, event.kind) == ("r-1", 0, "run.started")
    assert [event.kind for event in await event_store.read_session(ctx)] == ["run.started"]
    assert await event_store.run_status(replace(ctx, run_id="r-1")) is RunStatus.RUNNING


async def test_claim_start_refuses_a_session_that_already_has_a_run_going(event_store: EventStorePort) -> None:
    """The refusal names the run holding the session, and writes nothing: one turn per session,
    decided by the same write that would have opened the second one."""
    ctx = _ctx()
    await _write(event_store, [_started()], ctx)

    claim, event = await event_store.claim_start(_started(), _ctx(run_id="r-2"), ORIGIN, NOTHING_IS_STALE)
    assert (claim, event) == (SessionClaim(held_by="r-1"), None)
    assert [event.run_id for event in await event_store.read_session(ctx)] == ["r-1"]


async def test_claim_start_refuses_a_session_whose_run_is_waiting_on_a_human(event_store: EventStorePort) -> None:
    """``WAITING_ANSWER`` is not free: the interrupted run still owns its engine's thread, and a
    second run against it would write over the checkpoints that run resumes from."""
    ctx = _ctx()
    await _interrupt(event_store, ctx)

    claim, event = await event_store.claim_start(_started(), _ctx(run_id="r-2"), ORIGIN, NOTHING_IS_STALE)
    assert (claim, event) == (SessionClaim(held_by="r-1"), None)
    assert [event.run_id for event in await event_store.read_session(ctx)] == ["r-1", "r-1"]


@pytest.mark.parametrize(
    "closing",
    [
        pytest.param(_completed(), id="completed"),
        pytest.param(RunFailed(error_code="engine_error", message="boom", retryable=False), id="failed"),
        pytest.param(RunCancelled(reason="consumer stopped reading"), id="cancelled"),
    ],
)
async def test_claim_start_wins_once_the_previous_run_is_closed(
    event_store: EventStorePort, closing: KnownPayload
) -> None:
    """Every terminal event frees the session  -  a turn after a failed or cancelled one is the
    ordinary case, not a special one."""
    ctx = _ctx()
    await _write(event_store, [_started(), closing], ctx)

    claim, event = await event_store.claim_start(_started(), _ctx(run_id="r-2"), ORIGIN, NOTHING_IS_STALE)
    assert (claim, event is not None) == (SessionClaim(), True)
    assert [event.run_id for event in await event_store.read_session(ctx)] == ["r-1", "r-1", "r-2"]


async def test_a_run_that_recorded_no_transition_holds_no_session(event_store: EventStorePort) -> None:
    """A run with no transition is indistinguishable from one the store never saw, so it
    cannot hold anything  -  the same line ``list_runs`` draws."""
    ctx = _ctx()
    await _write(event_store, [TextDelta(message_id="m1", text="hi")], _ctx(run_id="r-0"))

    claim, event = await event_store.claim_start(_started(), ctx, ORIGIN, NOTHING_IS_STALE)
    assert (claim, event is not None) == (SessionClaim(), True)


async def test_claim_start_is_scoped_to_one_log_not_the_whole_store(event_store: EventStorePort) -> None:
    """A run going in one session says nothing about another: sessions are the unit that runs
    one turn at a time."""
    await _write(event_store, [_started()], _ctx())

    elsewhere = _ctx(run_id="r-2", session_id="s-2")
    claim, event = await event_store.claim_start(_started(), elsewhere, ORIGIN, NOTHING_IS_STALE)
    assert (claim, event is not None) == (SessionClaim(), True)


async def test_claim_start_never_sees_another_namespaces_open_run(event_store: EventStorePort) -> None:
    """Two namespaces are free to pick the same session id; neither may hold the other's session,
    and the run each claim opens is filed under the namespace that asked for it."""
    await _write(event_store, [_started()], _ctx("acme"))
    intruder = _ctx("globex", run_id="r-9")

    claim, event = await event_store.claim_start(_started(), intruder, ORIGIN, NOTHING_IS_STALE)
    assert (claim, event is not None) == (SessionClaim(), True)
    assert [event.namespace for event in await event_store.read_session(intruder)] == ["globex"]
    assert [event.namespace for event in await event_store.read_session(_ctx("acme"))] == ["acme"]


async def test_a_run_silent_past_the_cutoff_stops_holding_its_session(event_store: EventStorePort) -> None:
    """The hard-kill case: a process that died leaves a run nothing will ever close, so an open
    run that has gone quiet long enough is stepped over  -  and reported, because closing it means
    stamping an event, which is the caller's job and not a store's.

    What comes back is the abandoned run's *last event*, not its id: the store had to read that
    event to decide the run was stale, and the caller needs its envelope to write the closing
    event in that run's own name rather than this turn's.
    """
    ctx = _ctx()
    (opening,) = await _write(event_store, [_started()], ctx)

    claim, event = await event_store.claim_start(_started(), _ctx(run_id="r-2"), ORIGIN, EVERYTHING_IS_STALE)
    assert claim.held_by is None and event is not None
    assert claim.overridden == (opening,)
    assert [event.run_id for event in await event_store.read_session(ctx)] == ["r-1", "r-2"]
    assert [event.kind for event in await event_store.read_run(replace(ctx, run_id="r-1"))] == ["run.started"]


async def test_staleness_is_measured_from_the_last_event_of_a_run_not_its_last_transition(
    event_store: EventStorePort,
) -> None:
    """A run streaming for hours has an old ``run.started`` and a very recent delta. Judging it
    by the transition would take a working turn's session away from it mid-stream.

    The age gap is arranged by waiting, because the store owns the clock now  -  nothing here can
    backdate an event. The window then sits between the two writes: a store judging by the
    transition finds this run stale, one judging by its last event finds it live.
    """
    ctx = _ctx()
    [opening] = await _write(event_store, [_started()], ctx)
    await asyncio.sleep(AGE_GAP)
    [delta] = await _write(event_store, [TextDelta(message_id="m1", text="still here")], ctx)

    window = _window_between(opening, delta)
    claim, event = await event_store.claim_start(_started(), _ctx(run_id="r-2"), ORIGIN, window)
    assert (claim, event) == (SessionClaim(held_by="r-1"), None)
    assert [event.run_id for event in await event_store.read_session(ctx)] == ["r-1", "r-1"]


async def test_one_live_run_refuses_a_claim_even_beside_an_abandoned_one(event_store: EventStorePort) -> None:
    """A refused claim overrides nothing: a session with one dead run and one live one is busy,
    and stepping over the dead one anyway would leave a takeover half-done."""
    [dead] = await _write(event_store, [_started()], _ctx(run_id="r-dead"))
    await asyncio.sleep(AGE_GAP)
    [live] = await _write(event_store, [_started()], _ctx(run_id="r-live"))

    window = _window_between(dead, live)
    claim, event = await event_store.claim_start(_started(), _ctx(run_id="r-2"), ORIGIN, window)
    assert (claim, event) == (SessionClaim(held_by="r-live"), None)
    assert [event.run_id for event in await event_store.read_session(_ctx())] == ["r-dead", "r-live"]


@pytest.mark.parametrize(
    "opening",
    [
        pytest.param([_started(), _interrupted()], id="waiting_answer"),
        pytest.param([_started(), RunPaused(reason="operator stepped away")], id="paused"),
    ],
)
async def test_a_parked_run_is_never_overridden_however_stale(
    event_store: EventStorePort, opening: Sequence[KnownPayload]
) -> None:
    """``PAUSED`` and ``WAITING_ANSWER`` have no worker to be dead: unlike a ``RUNNING`` run,
    silence there is not evidence of anything, so no ``stale_after``  -  however small  -  ever
    steps over one.

    The regression this guards: before this fix, every store applied the timer to *any* open
    run, so a parked human approval was destroyed by the very next turn asked for on its
    session once the window had passed  -  this test fails against that code with
    ``EVERYTHING_IS_STALE``, which stales a run the instant it is written.
    """
    ctx = _ctx()
    await _write(event_store, opening, ctx)

    claim, event = await event_store.claim_start(_started(), _ctx(run_id="r-2"), ORIGIN, EVERYTHING_IS_STALE)
    assert (claim, event) == (SessionClaim(held_by="r-1"), None)
    assert [event.run_id for event in await event_store.read_session(ctx)] == ["r-1"] * len(opening)


async def test_a_parked_run_refuses_a_claim_even_beside_a_genuinely_stale_running_one(
    event_store: EventStorePort,
) -> None:
    """The mixed case: one run truly abandoned (``RUNNING``, silent past the cutoff) and one
    parked waiting on a human, in the same log. ``EVERYTHING_IS_STALE`` makes both look old
    enough to step over by timestamp alone  -  which is exactly why this has to be the case that
    proves suspension is checked *before* the timer and not folded into the same comparison:
    the parked run still holds the session, so the claim must refuse and close neither run, the
    same principle as ``test_one_live_run_refuses_a_claim_even_beside_an_abandoned_one``.
    """
    await _write(event_store, [_started()], _ctx(run_id="r-dead"))
    await _write(event_store, [_started(), _interrupted()], _ctx(run_id="r-parked"))

    claim, event = await event_store.claim_start(_started(), _ctx(run_id="r-new"), ORIGIN, EVERYTHING_IS_STALE)
    assert (claim, event) == (SessionClaim(held_by="r-parked"), None)
    assert [event.run_id for event in await event_store.read_session(_ctx())] == ["r-dead", "r-parked", "r-parked"]


async def test_two_claims_gathered_on_one_session_have_exactly_one_winner(event_store: EventStorePort) -> None:
    """The claim's own atomicity, not the Runtime's use of it: an ``await`` slipped between the
    scan and the append would let both of these find the session idle, and every other test here
    would still pass."""
    outcomes = await asyncio.gather(
        event_store.claim_start(_started(), _ctx(run_id="r-1"), ORIGIN, NOTHING_IS_STALE),
        event_store.claim_start(_started(), _ctx(run_id="r-2"), ORIGIN, NOTHING_IS_STALE),
    )

    assert [claim.held_by is None for claim, _ in outcomes].count(True) == 1, outcomes
    assert [event is not None for _, event in outcomes].count(True) == 1, outcomes
    assert len(await event_store.read_session(_ctx())) == 1


# --- ctx.key: a second, permanent claim over (namespace, key)  -  #324 -----------------------


async def test_claim_start_without_a_key_never_touches_the_key_index(event_store: EventStorePort) -> None:
    """The ordinary case, and the one every test above already exercises implicitly: a run
    started with no key must never collide with another run started with no key either."""
    first, event1 = await event_store.claim_start(_started(), _ctx(run_id="r-1"), ORIGIN, NOTHING_IS_STALE)
    second, event2 = await event_store.claim_start(
        _started(), _ctx(run_id="r-2", session_id="s-2"), ORIGIN, NOTHING_IS_STALE
    )

    assert first == SessionClaim() and event1 is not None
    assert second == SessionClaim() and event2 is not None


async def test_claim_start_refuses_a_key_already_used_by_a_different_run(event_store: EventStorePort) -> None:
    """The strict reading of `run-identity.md` §15: a key is consumed for good the moment a run
    opens with it, and a second start reusing it raises rather than handing back the run that
    holds it or silently refusing the session claim instead."""
    claim, event = await event_store.claim_start(
        _started(), _ctx(run_id="r-1", key="order-1234"), ORIGIN, NOTHING_IS_STALE
    )
    assert claim == SessionClaim() and event is not None

    with pytest.raises(DuplicateKeyError, match="order-1234.*r-1"):
        await event_store.claim_start(
            _started(), _ctx(run_id="r-2", session_id="s-2", key="order-1234"), ORIGIN, NOTHING_IS_STALE
        )
    # The refused claim wrote nothing under its own log  -  a duplicate key is refused before
    # anything about the losing run is recorded, the same "fails deterministically" promise a
    # lost session claim already gives.
    assert await event_store.read_session(_ctx(session_id="s-2")) == []


async def test_claim_start_refuses_a_reused_key_even_under_a_fresh_session(event_store: EventStorePort) -> None:
    """`(namespace, key)` is a namespace-wide claim, not a per-log one: reusing a key is refused
    even when the second attempt targets a session the first one never touched, which is what
    tells this apart from the ordinary per-log session-busy refusal above."""
    await event_store.claim_start(_started(), _ctx(run_id="r-1", key="order-1234"), ORIGIN, NOTHING_IS_STALE)

    with pytest.raises(DuplicateKeyError):
        await event_store.claim_start(
            _started(),
            _ctx(run_id="r-2", session_id="s-brand-new", key="order-1234"),
            ORIGIN,
            NOTHING_IS_STALE,
        )


async def test_two_namespaces_sharing_one_key_get_two_unrelated_runs(event_store: EventStorePort) -> None:
    """The whole point of splitting `id` from `key`: two tenants using the same application
    identifier never collide, because the key plays no part in either run's own address."""
    acme_claim, acme_event = await event_store.claim_start(
        _started(), _ctx("acme", run_id="r-1", key="order-1234"), ORIGIN, NOTHING_IS_STALE
    )
    globex_claim, globex_event = await event_store.claim_start(
        _started(), _ctx("globex", run_id="r-2", key="order-1234"), ORIGIN, NOTHING_IS_STALE
    )

    assert acme_claim == SessionClaim() and acme_event is not None
    assert globex_claim == SessionClaim() and globex_event is not None
    assert acme_event.run_id != globex_event.run_id


async def test_a_busy_session_refuses_before_a_reused_key_ever_raises(event_store: EventStorePort) -> None:
    """The two refusals are independent, but a store only gets to check one first: a live run
    still holding its session wins over the key check, so the caller sees the ordinary
    `held_by` refusal rather than `DuplicateKeyError`, on every backend alike."""
    first_claim, first_event = await event_store.claim_start(
        _started(), _ctx(run_id="r-1", key="order-1234"), ORIGIN, NOTHING_IS_STALE
    )
    assert first_claim == SessionClaim() and first_event is not None

    claim, event = await event_store.claim_start(
        _started(), _ctx(run_id="r-2", key="order-1234"), ORIGIN, NOTHING_IS_STALE
    )
    assert (claim, event) == (SessionClaim(held_by="r-1"), None)


async def test_two_concurrent_claim_starts_on_one_key_yield_exactly_one_winner(event_store: EventStorePort) -> None:
    """The enforcement's own atomicity, not a caller's discipline: an ``await`` slipped between
    reading whether the key is free and writing it down would let both of these through."""
    outcomes = await asyncio.gather(
        event_store.claim_start(_started(), _ctx(run_id="r-1", key="order-1234"), ORIGIN, NOTHING_IS_STALE),
        event_store.claim_start(
            _started(), _ctx(run_id="r-2", session_id="s-2", key="order-1234"), ORIGIN, NOTHING_IS_STALE
        ),
        return_exceptions=True,
    )

    winners = [outcome for outcome in outcomes if not isinstance(outcome, BaseException)]
    losers = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
    assert len(winners) == 1, outcomes
    assert len(losers) == 1 and isinstance(losers[0], DuplicateKeyError), outcomes


# Wide enough that a store handing two callers one number would be caught, small enough that the
# case costs milliseconds. Every task appends a single payload, so the log must come back as
# exactly this many events numbered 0..N-1 however the tasks were scheduled.
_CONCURRENT_APPENDS = 20


async def test_many_appends_at_once_leave_one_run_contiguous_from_zero(event_store: EventStorePort) -> None:
    """The promise the whole port change rests on: assignment happens inside the write, so no two
    callers can be handed the same ``seq`` and no number can be handed out and then not persisted.

    A store that read the run's last ``seq`` and then appended  -  with any suspension point in
    between  -  fails this and passes every sequential case in this file. Both halves are asserted:
    the seqs the store *handed back* are distinct and cover the range, and the log *reads back*
    dense and in order. Only the first catches a store that returns a number it never wrote.
    """
    ctx = _ctx()
    batches = await asyncio.gather(
        *(
            _write(event_store, [TextDelta(message_id=f"m{which}", text=str(which))], ctx)
            for which in range(_CONCURRENT_APPENDS)
        )
    )

    handed_back = sorted(event.seq for batch in batches for event in batch)
    assert handed_back == list(range(_CONCURRENT_APPENDS)), handed_back
    stored = await event_store.read_run(replace(ctx, run_id="r-1"))
    assert [event.seq for event in stored] == list(range(_CONCURRENT_APPENDS)), [event.seq for event in stored]


async def test_every_run_in_one_log_counts_its_own_seq_from_zero(event_store: EventStorePort) -> None:
    """``seq`` is per run, so every run in a session log starts at 0  -  a store that counted per
    log would number the second run's opening event 1 and break its consumers' loss check."""
    await _write(event_store, [_started()], _ctx(run_id="r-1"))
    await _write(event_store, [_started()], _ctx(run_id="r-2"))

    stored = await event_store.read_session(_ctx())
    assert [(event.run_id, event.seq) for event in stored] == [("r-1", 0), ("r-2", 0)]


async def test_a_batch_is_numbered_in_the_order_it_was_handed_over(event_store: EventStorePort) -> None:
    """One append of several payloads is one write, and the caller's order is the log's order  -
    the events come back numbered from the run's next ``seq`` with nothing skipped."""
    ctx = _ctx()
    events = await _write(event_store, [_started(), TextDelta(message_id="m1", text="hi"), _completed()], ctx)

    assert [(event.seq, event.kind) for event in events] == [
        (0, "run.started"),
        (1, "text.delta"),
        (2, "run.completed"),
    ]
    assert [event.seq for event in await event_store.read_run(replace(ctx, run_id="r-1"))] == [0, 1, 2]


async def test_a_second_batch_carries_on_from_where_the_first_stopped(event_store: EventStorePort) -> None:
    """The run's counter lives in the log, not in a caller  -  so a fresh store handle, a restarted
    process or a second worker all continue the same run rather than restarting it at 0."""
    ctx = _ctx()
    await _write(event_store, [_started(), TextDelta(message_id="m1", text="hi")], ctx)

    later = await _write(event_store, [_completed()], ctx)
    assert [event.seq for event in later] == [2]


async def test_an_append_to_a_cancelled_run_is_refused(event_store: EventStorePort) -> None:
    """A cancel is written from outside the run while its own task is still alive, so the run goes
    on writing and the store is the last thing that can stop it (#421). The refusal is the state's,
    so it is a ``RunStateError`` rather than a store failure, and it writes nothing.
    """
    ctx = _ctx()
    await _write(event_store, [_started(), RunCancelled(reason="the deck closed")], ctx)

    with pytest.raises(RunStateError, match="r-1"):
        await _write(event_store, [TextDelta(message_id="m1", text="late")], ctx)

    assert [event.kind for event in await event_store.read_run(ctx)] == ["run.started", "run.cancelled"]


async def test_an_append_racing_the_cancel_lands_before_it_or_not_at_all(event_store: EventStorePort) -> None:
    """The window opened deliberately rather than waited for: the write and the cancel are handed
    over together, so on a real store one of them is genuinely in flight when the other commits.
    Whichever order the store picks, nothing may end up behind the terminal event.
    """
    ctx = _ctx()
    await _write(event_store, [_started()], ctx)

    await asyncio.gather(
        _write(event_store, [TextDelta(message_id="m1", text="in flight")], ctx),
        _write(event_store, [RunCancelled(reason="the deck closed")], ctx),
        return_exceptions=True,  # whichever write lost the race raises, and that is the point
    )

    kinds = [event.kind for event in await event_store.read_run(ctx)]
    assert kinds[-1] == "run.cancelled", kinds
    assert await event_store.run_status(ctx) is RunStatus.CANCELLED


async def test_an_append_past_a_taken_over_runs_failure_still_lands(event_store: EventStorePort) -> None:
    """The carve-out, on every store. A takeover's ``run.failed`` is advisory: it is written for a
    run only *believed* dead, and one that turns out to be alive goes on writing and may reclaim
    its own session. Only a cancel seals a log (ADR-D11 §5).
    """
    ctx = _ctx()
    await _write(
        event_store, [_started(), RunFailed(error_code="cancelled_hard", message="abandoned", retryable=False)], ctx
    )

    resurrected = await _write(event_store, [_interrupted()], ctx)

    assert [event.seq for event in resurrected] == [2]
    assert await event_store.run_status(ctx) is RunStatus.WAITING_ANSWER


async def _interrupt(event_store: EventStorePort, ctx: RunContext, run_id: str = "r-1") -> None:
    """Leave one run parked in ``WAITING_ANSWER``  -  the only status a resume may claim."""
    parked = ctx if ctx.run_id == run_id else _ctx(ctx.namespace, run_id=run_id, session_id=ctx.session_id)
    await _write(event_store, [_started(), _interrupted()], parked)


async def test_claim_resume_appends_the_event_and_wins_when_the_run_is_waiting(event_store: EventStorePort) -> None:
    ctx = _ctx()
    await _interrupt(event_store, ctx)

    event = await event_store.claim_resume(RunResumed(reason=None), ctx, ORIGIN)
    assert event is not None and (event.seq, event.kind) == (2, "run.resumed")
    assert [event.kind for event in await event_store.read_run(replace(ctx, run_id="r-1"))][-1] == "run.resumed"
    assert await event_store.run_status(replace(ctx, run_id="r-1")) is RunStatus.RUNNING


async def test_a_second_claim_on_the_same_run_loses_and_writes_nothing(event_store: EventStorePort) -> None:
    """The invariant double-resume protection rests on: the check and the append are one step, so
    the loser cannot append a second ``run.resumed``  -  it reads the ``RUNNING`` the winner's own
    append published."""
    ctx = _ctx()
    await _interrupt(event_store, ctx)
    assert await event_store.claim_resume(RunResumed(reason=None), ctx, ORIGIN) is not None

    assert await event_store.claim_resume(RunResumed(reason=None), ctx, ORIGIN) is None
    stored = await event_store.read_run(replace(ctx, run_id="r-1"))
    assert [event.kind for event in stored].count("run.resumed") == 1
    assert [event.seq for event in stored] == [0, 1, 2]


async def test_claim_resume_wins_on_a_paused_run_too(event_store: EventStorePort) -> None:
    """``PAUSED`` is the other *suspended* status, and one claim serves both: a paused run is
    owed a terminal event just as a parked approval is, and only one caller may continue it.

    This is the positive half of the guard below  -  without it, a store could refuse every
    status but ``WAITING_ANSWER`` and no test would notice that pause had stopped resuming.
    """
    ctx = _ctx()
    await _write(event_store, [_started(), RunPaused(reason="operator")], ctx)
    assert await event_store.run_status(replace(ctx, run_id="r-1")) is RunStatus.PAUSED

    assert await event_store.claim_resume(RunResumed(reason=None), ctx, ORIGIN) is not None
    assert await event_store.run_status(replace(ctx, run_id="r-1")) is RunStatus.RUNNING


@pytest.mark.parametrize("kind", ["pending", "running", "completed", "cancelled"])
async def test_claim_resume_refuses_a_run_that_is_not_suspended(event_store: EventStorePort, kind: str) -> None:
    """A resume against any status that is not suspended is a no-op, not an error  -  including a
    run this store has never heard of, which is indistinguishable from one that never started.

    ``cancelled`` is the one that carries a promise rather than just a rule: cancel is terminal,
    so this guard is what makes "a cancelled run cannot be resumed" true across processes, where
    a caller's own status check could always go stale between reading and appending.

    Status is the whole condition now  -  there is no ``seq`` left for a caller to get wrong, so
    every one of these refusals is the status guard and nothing else.
    """
    ctx = _ctx()
    if kind == "running":
        await _write(event_store, [_started()], ctx)
    elif kind == "completed":
        await _write(event_store, [_started(), _completed()], ctx)
    elif kind == "cancelled":
        await _write(event_store, [_started(), RunCancelled(reason="user closed the tab")], ctx)

    assert await event_store.claim_resume(RunResumed(reason=None), ctx, ORIGIN) is None
    assert [event.kind for event in await event_store.read_run(replace(ctx, run_id="r-1"))].count("run.resumed") == 0


async def test_a_claim_names_its_run_once(event_store: EventStorePort) -> None:
    """The guarantee that used to need a guard. ``claim_resume`` took a ``run_id`` beside the
    context and raised when the two disagreed  -  the store answering about one run and writing
    the other. The parameter is gone, so the context is the only thing that says which run, and
    the disagreement is unrepresentable rather than refused."""
    ctx = _ctx()
    await _interrupt(event_store, ctx)

    event = await event_store.claim_resume(RunResumed(reason=None), ctx, ORIGIN)

    assert event is not None
    assert event.run_id == ctx.run_id


async def test_claim_resume_is_scoped_to_one_run_not_the_whole_log(event_store: EventStorePort) -> None:
    """One waiting run in a log must not license a resume of a different run beside it."""
    ctx = _ctx()
    await _interrupt(event_store, ctx, run_id="r-1")
    other = _ctx(run_id="r-2")
    await _write(event_store, [_started()], other)

    assert await event_store.claim_resume(RunResumed(reason=None), other, ORIGIN) is None
    assert await event_store.claim_resume(RunResumed(reason=None), ctx, ORIGIN) is not None


async def test_claim_resume_never_reaches_into_another_namespaces_waiting_run(event_store: EventStorePort) -> None:
    """Same isolation as every other query: another namespace's interrupt is not claimable, and the
    intruder's own view of that run is a run nobody ever started."""
    await _interrupt(event_store, _ctx("acme"))
    intruder = _ctx("globex")

    assert await event_store.claim_resume(RunResumed(reason=None), intruder, ORIGIN) is None
    assert [event.kind for event in await event_store.read_run(replace(_ctx("acme"), run_id="r-1"))].count(
        "run.resumed"
    ) == 0


async def test_list_runs_scopes_to_one_namespace(event_store: EventStorePort) -> None:
    await _write(event_store, [_started()], _ctx("acme"))
    await _write(event_store, [_started()], _ctx("globex"))

    acme_runs = await event_store.list_runs(_ctx("acme"))
    assert [summary.run_id for summary in acme_runs] == ["r-1"]


async def test_list_runs_filters_by_status(event_store: EventStorePort) -> None:
    ctx = _ctx()
    await _write(event_store, [_started()], ctx)
    await _write(event_store, [_started(), _interrupted(thread_id="t-2")], _ctx(run_id="r-2"))

    waiting = await event_store.list_runs(ctx, status=RunStatus.WAITING_ANSWER)
    assert [(summary.run_id, summary.status) for summary in waiting] == [("r-2", RunStatus.WAITING_ANSWER)]

    everyone = await event_store.list_runs(ctx)
    assert {summary.run_id for summary in everyone} == {"r-1", "r-2"}


async def test_list_runs_of_an_empty_store_is_empty(event_store: EventStorePort) -> None:
    assert await event_store.list_runs(_ctx()) == []


async def test_list_runs_limit_caps_the_listing(event_store: EventStorePort) -> None:
    await _write(event_store, [_started()], _ctx(run_id="r-1"))
    await _write(event_store, [_started()], _ctx(run_id="r-2", session_id="s-2"))
    await _write(event_store, [_started()], _ctx(run_id="r-3", session_id="s-3"))

    assert len(await event_store.list_runs(_ctx())) == 3
    assert len(await event_store.list_runs(_ctx(), limit=2)) == 2
    assert await event_store.list_runs(_ctx(), limit=0) == []


async def test_list_runs_negative_limit_is_refused(event_store: EventStorePort) -> None:
    with pytest.raises(ValueError, match="limit"):
        await event_store.list_runs(_ctx(), limit=-1)


async def test_list_runs_enumerates_runs_across_every_log_key_of_the_namespace(event_store: EventStorePort) -> None:
    """A namespace's waiting runs live in as many logs as it has sessions  -  a listing that only
    looked in one log key would silently hide every other session's interrupts."""
    ctx = _ctx()
    await _write(event_store, [_started(), _interrupted(thread_id=None)], ctx)
    await _write(event_store, [_started(), _interrupted(thread_id=None)], _ctx(run_id="r-2", session_id="s-2"))

    waiting = await event_store.list_runs(ctx, status=RunStatus.WAITING_ANSWER)
    assert {(summary.session_id, summary.run_id) for summary in waiting} == {("s-1", "r-1"), ("s-2", "r-2")}


async def test_list_runs_skips_a_run_whose_log_holds_no_lifecycle_event(event_store: EventStorePort) -> None:
    """Such a run folds to no status at all, which no listing can tell apart from a run the
    store never saw  -  both stores leave it out rather than one inventing it."""
    ctx = _ctx()
    await _write(event_store, [TextDelta(message_id="m1", text="hi")], ctx)
    assert await event_store.list_runs(ctx) == []


def _deltas(count: int) -> list[KnownPayload]:
    return [TextDelta(message_id="m1", text=str(which)) for which in range(count)]


async def test_paginated_read_offset_skips_the_first_n_events(event_store: EventStorePort) -> None:
    ctx = _ctx()
    await _write(event_store, _deltas(5), ctx)
    page = await event_store.read_session(ctx, offset=2)
    assert [event.seq for event in page] == [2, 3, 4]


async def test_paginated_read_limit_caps_the_page(event_store: EventStorePort) -> None:
    ctx = _ctx()
    await _write(event_store, _deltas(5), ctx)
    page = await event_store.read_session(ctx, limit=2)
    assert [event.seq for event in page] == [0, 1]


async def test_paginated_read_offset_and_limit_compose_into_the_next_page(event_store: EventStorePort) -> None:
    ctx = _ctx()
    await _write(event_store, _deltas(5), ctx)
    page = await event_store.read_session(ctx, offset=2, limit=2)
    assert [event.seq for event in page] == [2, 3]


async def test_paginated_read_past_the_end_is_empty(event_store: EventStorePort) -> None:
    ctx = _ctx()
    await _write(event_store, [_started()], ctx)
    assert await event_store.read_session(ctx, offset=10) == []


async def test_a_page_already_read_does_not_shift_when_the_log_grows(event_store: EventStorePort) -> None:
    """The promise paging rests on: a log only ever grows at its end, so an offset a reader
    has passed keeps meaning the same event. A store that ordered by anything a later write
    can slot in front of  -  or that published its ordering out of the order it assigned it  -
    would move an unread event behind the cursor and deliver its neighbour twice.

    Sequential here, which is all one store instance can show, and it passes on all four with
    the concurrent half of the promise broken: that half  -  a write committing while another
    writer's batch is still in flight  -  is the case below, on two handles.
    """
    ctx = _ctx()
    await _write(event_store, [_started(), RunResumed(reason=None)], ctx)
    first_page = await event_store.read_session(ctx, offset=0, limit=2)

    await _write(event_store, [TextDelta(message_id="m1", text="later")], ctx)

    assert await event_store.read_session(ctx, offset=0, limit=2) == first_page
    assert [event.seq for event in await event_store.read_session(ctx, offset=2)] == [2]


# Wide enough that inserting it takes far longer than the peer's round trip, so the peer writes
# while it is still open. That margin is what reproduces the hazard, and it is a margin and not
# a guarantee: nothing here forces the peer's write to be numbered inside the batch. A machine
# that closes the gap loses the hazard, which is why the report below says whether it happened  -
# a lost hazard must never read as a passing store.
_INTERLEAVED_BATCH = 200

# Five peer writes rather than one, spread through that window instead of betting on one moment.
# Cheap insurance rather than a fix for anything measured: one write reproduces the hazard too.
_TRAILING_WRITES = 5

# A bound on a broken run, never part of an assertion: a reader that has lost an event to a
# shift will never reach its count, and has to stop asking at some point.
_PAGING_DEADLINE = 10.0


def _keys(events: Iterable[Event]) -> list[tuple[str, int]]:
    """Each event as its identity  -  one ``(run, seq)`` per event, ever."""
    return [(event.run_id, event.seq) for event in events]


async def _append_each(store: EventStorePort, ctx: RunContext, payloads: Iterable[KnownPayload]) -> None:
    """One committed write per payload, so a run of them straddles whatever a peer has open."""
    for payload in payloads:
        await store.append([payload], ctx, ORIGIN)


async def _page_the_log(store: EventStorePort, ctx: RunContext, until: int) -> list[list[Event]]:
    """Page a log the way the port says is safe  -  a plain counter as the cursor  -  until
    ``until`` events have come back or the deadline gives up on them.

    The cursor being the count delivered so far is the whole point: an event that lands
    *behind* it is never delivered, and whatever it displaced is delivered twice. Returns the
    pages rather than the events, because how the delivery was split across them is the
    evidence that anything was read while the log was still being written.
    """
    pages: list[list[Event]] = []
    delivered = 0
    deadline = monotonic() + _PAGING_DEADLINE
    while delivered < until and monotonic() < deadline:
        page = await store.read_session(ctx, offset=delivered)
        if not page:
            await asyncio.sleep(0.001)  # nothing new committed yet, so let the writer get on with it
            continue
        pages.append(page)
        delivered += len(page)
    return pages


async def test_a_page_already_read_does_not_shift_when_a_second_writer_commits(
    two_event_stores: tuple[EventStorePort, EventStorePort],
) -> None:
    """The promise above in the shape one instance cannot show: a second writer commits while
    the first's batch is still in flight, and a reader pages across both.

    Every event is delivered exactly once and in one order, or paging is not safe to do with a
    counter. What can break it is a store that orders by a number assigned at insert and
    published at commit  -  Postgres's ``BIGSERIAL``  -  because the peer's row can be given a
    *later* number and still be published *first*: the reader takes it at an offset it then
    leaves behind, so the batch's first event is never delivered and the peer's arrives twice.
    Serializing a log's writes is what keeps it growing only at its end.

    Memory and SQLite pass this by construction rather than by luck, and are here because a
    backend added later gets asked the same question before it is trusted.

    The interleave is arranged with a margin, not forced: the batch takes far longer to insert
    than the peer takes to commit, which is why the peer lands inside it. So the report at the
    end says how the delivery was split  -  a machine that closes that margin delivers the whole
    settled log in one page, and this case would then pass without having asked anything.
    """
    batching, peer = two_event_stores
    ctx = _ctx()
    batch = _deltas(_INTERLEAVED_BATCH)
    # A run of their own, so the peer's writes are told apart from the batch's by ``run_id``
    # rather than by a ``seq`` neither writer chooses any more.
    behind_ctx = _ctx(run_id="r-2")
    trailing = [TextDelta(message_id="m2", text=str(which)) for which in range(_TRAILING_WRITES)]
    # Both handles connected and set up before the race  -  a cold one spends its first call
    # creating a schema, which is a lap the other does not run.
    await batching.read_session(ctx)
    await peer.read_session(ctx)

    writing = asyncio.create_task(batching.append(batch, ctx, ORIGIN))
    await asyncio.sleep(0)  # into its write before the peer opens one, so the peer's rows are numbered behind it
    behind = asyncio.create_task(_append_each(peer, behind_ctx, trailing))

    # Reads go through the writing peer's own handle, so every page is taken between two of its
    # commits  -  the moment a shift would be visible in  -  and never inside one.
    pages = await _page_the_log(peer, ctx, until=len(batch) + len(trailing))
    # Bounded like the reader: a wedged writer must fail this case, not hang the suite in a
    # gather nothing ever returns from.
    async with asyncio.timeout(_PAGING_DEADLINE):
        await asyncio.gather(writing, behind)
    settled = _keys(await peer.read_session(ctx))

    seen = _keys(event for page in pages for event in page)

    # Reported, not asserted, for the same reason the resume race reports its overlap: the split
    # is this machine's timing. A store that keeps the promise holds the peer's writes behind the
    # open batch, so delivering them last *is* the promise being kept, and the broken one shows
    # the opposite  -  the peer's writes first, at an offset the batch then takes. What the report
    # is for is neither: one page holding the settled log means the batch was never open long
    # enough to be interleaved with, and the case passed without asking anything. Printed before
    # the assertions, so a failing run carries it too.
    first_trailing = next((offset for offset, (run_id, _) in enumerate(seen) if run_id == "r-2"), None)
    print(
        f"interleaved paging on {type(peer).__name__}: pages {[len(page) for page in pages]}, "
        f"the peer's first write delivered at offset {first_trailing} of {len(seen)}"
    )

    twice = [key for key, times in Counter(seen).items() if times > 1]
    never = [key for key in settled if key not in set(seen)]
    assert not twice and not never, (
        f"paging a log of {len(settled)} delivered {len(seen)}: {twice} twice, {never} never  -  "
        "a write landed behind the reader's cursor"
    )
    assert seen == settled, "the reader's order is not the order the log settled into"


async def test_paginated_read_zero_limit_is_an_empty_page(event_store: EventStorePort) -> None:
    ctx = _ctx()
    await _write(event_store, [_started()], ctx)
    assert await event_store.read_session(ctx, limit=0) == []


async def test_a_negative_offset_reads_from_the_start_and_a_negative_limit_is_refused(
    event_store: EventStorePort,
) -> None:
    """Left to the underlying store these mean opposite things  -  a Python slice counts back
    from the end, SQLite reads a negative LIMIT as "no limit"  -  so the port pins both."""
    ctx = _ctx()
    await _write(event_store, _deltas(3), ctx)

    assert [event.seq for event in await event_store.read_session(ctx, offset=-2)] == [0, 1, 2]
    with pytest.raises(ValueError, match="limit"):
        await event_store.read_session(ctx, limit=-1)


async def test_the_focused_queries_never_answer_from_another_namespaces_log(event_store: EventStorePort) -> None:
    """One namespace's populated log must read as untouched emptiness to another  -  the same
    isolation ``read``/``read_run`` already promise, on the queries that skip them."""
    acme = _ctx("acme", key="order-1234")
    await event_store.claim_start(_started(), acme, ORIGIN, timedelta(hours=1))
    intruder = _ctx("globex")

    assert await event_store.run_status(replace(intruder, run_id="r-1")) is None
    assert await event_store.list_runs(intruder) == []
    assert await event_store.read_session(intruder, offset=0) == []
    assert await event_store.read_run(replace(intruder, run_id="r-1")) == []
    assert await event_store.find_by_key(intruder, "order-1234") is None


# Every public method of both SQLite-backed ports, so a method added later without the
# boundary wrapper is a missing case here rather than a silent leak.
_SQLITE_CALLS = [
    pytest.param(SqliteEventStore, lambda port: port.append([_started()], _ctx(), ORIGIN), id="append"),
    pytest.param(SqliteEventStore, lambda port: port.read_session(_ctx()), id="read"),
    pytest.param(SqliteEventStore, lambda port: port.read_run(replace(_ctx(), run_id="r-1")), id="read_run"),
    pytest.param(SqliteEventStore, lambda port: port.run_status(replace(_ctx(), run_id="r-1")), id="run_status"),
    pytest.param(
        SqliteEventStore,
        lambda port: port.claim_resume(RunResumed(reason=None), _ctx(), ORIGIN),
        id="claim",
    ),
    pytest.param(
        SqliteEventStore,
        lambda port: port.claim_start(_started(), _ctx(), ORIGIN, NOTHING_IS_STALE),
        id="claim_start",
    ),
    pytest.param(SqliteEventStore, lambda port: port.list_runs(_ctx()), id="list_runs"),
    pytest.param(SqliteEventStore, lambda port: port.find_by_key(_ctx(), "order-1234"), id="find_by_key"),
    pytest.param(SqliteControlPort, lambda port: port.signal("r-1", Signal.CANCEL), id="signal"),
    pytest.param(SqliteControlPort, lambda port: port.poll("r-1"), id="poll"),
]


@pytest.mark.parametrize(("port_type", "call"), _SQLITE_CALLS)
async def test_a_failed_statement_reaches_the_caller_as_a_store_error(
    port_type: Callable[[], Any], call: Callable[[Any], Coroutine[Any, Any, object]]
) -> None:
    """A ``sqlite3`` exception is a library type and must not cross a port: callers of either
    SQLite-backed port catch ``StoreError``, with the original kept only as the cause.

    ``claim_resume`` is the case that makes this load-bearing  -  it promises a loser a clean
    ``None``, so an unreachable store has to be distinguishable from a claim somebody won.
    Forced by closing the connection, which fails whichever statement the method reaches for:
    the shape of a database gone unreadable mid-run, without waiting out a real lock.
    """
    port = port_type()
    port.close()

    with pytest.raises(StoreError) as raised:
        await call(port)
    assert isinstance(raised.value.__cause__, sqlite3.Error)
    assert not isinstance(raised.value, sqlite3.Error)


async def test_a_write_lock_held_past_the_busy_timeout_is_a_store_error_not_a_lost_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failure that motivated the wrapping, in its own shape rather than a closed
    connection's: a peer holds the file's write lock longer than this store will wait for it.

    ``claim_resume`` must not answer that with ``None``. ``None`` means somebody else won and
    the resume is already recorded, so returning it here would discard a human's approval while
    reporting a race that never happened. The timeout is shortened so the wait costs milliseconds.
    """
    monkeypatch.setattr(sqlite_store, "_BUSY_TIMEOUT_MS", 50)
    store = SqliteEventStore(tmp_path / "events.sqlite3")
    ctx = _ctx()
    await _interrupt(store, ctx)

    peer = sqlite3.connect(tmp_path / "events.sqlite3")
    peer.execute("BEGIN IMMEDIATE")
    peer.execute("INSERT INTO events (namespace, session_id, run_id, seq, data) VALUES ('acme', 's-1', 'r-9', 0, '{}')")
    try:
        with pytest.raises(StoreError) as raised:
            await store.claim_resume(RunResumed(reason=None), ctx, ORIGIN)
        assert isinstance(raised.value.__cause__, sqlite3.OperationalError)
        assert "locked" in str(raised.value.__cause__)
    finally:
        peer.rollback()
        peer.close()
        store.close()
