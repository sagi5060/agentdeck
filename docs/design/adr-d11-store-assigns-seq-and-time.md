# ADR-D11  -  The store assigns `seq` and `ts`

**Status:** accepted
**Date:** 2026-08-08 · **Amended:** 2026-08-08 (#149), 2026-08-22 (#421) · **Relates to:** ADR-D5, design doc §4.2 ·
§5, `core/ports/store.py`, `runtime/service.py`, coding-standards §6
**Supersedes:**

- coding-standards §6 (`docs/coding-standards.md:113`)  -  *"the Runtime is the **only** assigner of
  `seq`, one counter per run, recovered from `max(seq)` on resume"*.
- ADR-D5's *Explicitly unchanged* clause (`adr-d5-two-stores.md:151`)  -  *"`Runtime` still stamps and
  appends every event"*. D5's two-store rule itself is untouched.
- `prompts/pr1-event-schema-prompt.md:34` and `:121`, which state the same rule. Prompts are frozen
  as history rather than edited (index §6), so the supersession is recorded here and in the index.
- The design doc's division of envelope stamping.

The sentence this ADR originally named  -  *"A store never reads a clock"* in
`EventStorePort.claim_start`  -  was cut by #150 (`4063bdd`) as restated prose before this amendment
landed. Recorded because a superseded rule that disappears without a trace is indistinguishable from
one that was never written.

---

## 1. Decision, in one line

**Assigning a run's next `seq` and stamping `ts` happen inside the store, in the same indivisible
operation that persists the event.** No component outside a store holds a sequence counter or
decides an event's time.

The division this rests on, stated once because everything below follows from it:

> **The store is mechanism and observation. The Runtime is judgement.**
> A store refuses what would corrupt the log and reports what it saw. It never decides what anything
> means  -  what "abandoned" is, what a resume carries, when a run is over  -  and it never acquires a
> decision it can be asked to make later.

Engines are unchanged: they yield payloads, never envelopes, so an engine still cannot forge `seq`
or `tenant`.

## 2. Why

`seq` has two jobs (`core/events.py`): it is the ordering authority *and* the loss check. The
second only works if the sequence is dense  -  a gap means an event is missing. Today the Runtime
holds the counter, and allocation is separated from the write by an `await`:

```python
yield await self._record(payload, spec, ctx, next(seq))  # seq taken here, append may fail
```

Measured on the committed code, with a store that refuses one append mid-run:

```
in the log       : [(0, 'run.started'), (1, 'text.delta'), (3, 'run.failed')]
check_contiguous : [2]        ← a gap no refetch can ever fill
check_terminal   : None       ← the run is otherwise correctly closed
```

That gap is documented (`_drain`'s docstring, `service.py:536-538`) and pinned
(`test_runtime_service.py:890`), and it makes the module's own promise  -  *"a consumer that spots a
`seq` gap can always refetch it"* (`service.py:5`)  -  false: a consumer cannot tell a dropped event
from a permanent hole.

The counter is also spread thin: seven functions take `(spec, ctx, seq)` purely to pass them on and
ten call sites each decide when to advance it, which is why the failure path has to guess whether
the one it took was used. A seq allocated and persisted in one step cannot be allocated and not
persisted.

## 3. What this buys

- **A gap means an event was genuinely lost.** `check_contiguous` becomes the loss check it is
  documented to be, and the refetch promise becomes true.
- **`claim_resume` loses half its contract.** Its stale-`seq` guard exists because a caller stamps
  before claiming; when the store assigns, that race cannot occur. One question remains: is this
  run `WAITING_ANSWER`?
- **`last_seq` comes off the port**  -  but only one of its three callers is arithmetic, and the ADR
  originally mis-stated this. `service.py:451` is the arithmetic and stops existing.
  `service.py:152` is an *existence probe* on the cancellation path (*did the claim commit before
  the client disconnected?*), answered by `run_status() != PENDING`, since `PENDING` is
  indistinguishable from a run the store never saw and every run records `run.started` first.
  `service.py:383` is a *bounded tail read* for envelope fields, answered by `SessionClaim` (§5).
  Postgres keeps a private equivalent for its own assignment.
- **One clock per store, not one per worker.** N workers sharing a Postgres stamp with N clocks
  today; `stale_after` comparisons are only as good as the agreement between them.
- **The Runtime shrinks.** No counter, no `Iterator[int]` threaded through seven signatures.

## 4. What it costs  -  accepted deliberately

- **Clock seams multiply, and two of them stop being in-process.** There are three injected clocks
  today, not one: `service.py:92` (wall) and `dispatch.py:111` / `core/control.py:120` (both
  monotonic). Golden snapshots do **not** pin `ts` through any of them  -  `tests/core/snapshots/` is
  built from hand-constructed events with a fixed `TS` (`tests/core/conftest.py:45,129`), and
  `tests/golden/` is v1's SSE wire, which carries no `ts` at all. The real exposure is
  `agentdeck/composition.py:44,58`, where `clock` is a public keyword of `build_runtime` forwarded
  to `Runtime`: it stops meaning "the clock every event is stamped with".
  Memory and Redis take an injected callable. **SQLite and Postgres stamp with the backend's own
  clock** (`CURRENT_TIMESTAMP`, `clock_timestamp()`), keeping the seam for tests only  -  an
  in-process callable gives N workers N disagreeing clocks, which is precisely what the
  `stale_before: datetime` → `stale_after: timedelta` change cannot tolerate.
- **Stores construct envelopes.** SQLite persists an opaque JSON blob today; it will build the
  `Event` after knowing the seq. Adapters import and construct core models.
- **`append` gains a return value.** The Runtime yields what it wrote, so it needs the finished
  events back.
- **`SessionClaim` grows.** `overridden` carries events instead of ids (§5).
- **Four implementations must each prove atomicity.** That is the point of the decision, and §6 is
  how it is enforced rather than asserted.

## 5. The port, after

```python
async def append(self, log_key, payloads: Sequence[KnownPayload], ctx, origin: str) -> list[Event]
async def claim_start(self, log_key, opening: RunStarted, ctx, origin,
                      stale_after: timedelta) -> tuple[SessionClaim, Event | None]
async def claim_resume(self, log_key, run_id, resumed: RunResumed, ctx, origin) -> Event | None
```

`read`, `read_run`, `list_runs` and `run_status` are unchanged. `last_seq` is removed, leaving seven
methods.

**`append` derives run identity from `ctx` alone  -  including for the one write that addresses
another run.** `Runtime._close_abandoned` (`service.py:376-411`) stamps a `run.failed` for a run
this turn took over, inheriting that run's `run_id`, `session_id` and `origin`; its own docstring
says *"never this turn's"*. An `append` reading those from `ctx` would file the terminal event under
the **live** turn: `check_terminal` reports a violation, the live stream reads as closed while it is
still emitting, and the abandoned run keeps no terminal event  -  so it stays open and is
re-overridden by every subsequent turn, and the takeover loop never converges.

It needs no override parameter. **`SessionClaim.overridden` carries each abandoned run's last event
instead of just its id**  -  which every store already holds, having compared that event's `ts` to
decide the run was stale (`memory:74`, `sqlite:199`, `postgres:250`, all iterating
`list[tuple[str, Event]]`), and currently throws away. The closer builds that run's own
`RunContext` from it and calls the ordinary `append`. No foreign addressing, no second query, and
`claim_start` keeps exactly one job.

**Amended 2026-08-22 (#421): `append` refuses a run that is already `CANCELLED`.** A spent `seq`
no longer refuses anything, and a cancel is written from outside a run whose task is still alive, so
one of that run's writes can already be suspended inside `append` when the terminal event lands. The
condition is folded into each backend's own write step, beside the `seq` read it already makes there.
The takeover's `run.failed` above deliberately seals nothing: it is written for a run only *believed*
dead, and one that turns out to be alive goes on writing and may reclaim its own session.

**The two claims stay, as named methods.** They are not extra operations  -  they are conditional
appends, and they are the only place mutual exclusion can live without adding a second piece of
infrastructure. Measured: two workers that read "session idle" and then append both open a run on
one session; `claim_start` refuses one of them. This is not the store taking a decision  -  the
Runtime has already decided the resume is meaningful. It is the store answering *did yours land, or
someone else's*, which only the thing holding the data can answer.

## 6. Per-backend mechanism  -  what each must guarantee

| store | seq | atomicity |
|---|---|---|
| memory | per-run counter | no `await` between assign and append |
| sqlite | `COALESCE(MAX(seq), -1) + 1` in `INSERT…SELECT` | **the append path must take `BEGIN IMMEDIATE`, which it does not today** |
| postgres | the same `INSERT…SELECT` | transaction + the existing `UNIQUE` index |
| redis | next score on the per-run **ZSET** it already keeps | inside the existing `WATCH`/`MULTI`/`EXEC` |

Two rows corrected from the original, both of which named mechanisms that do not exist:

- **sqlite.** The only `BEGIN IMMEDIATE` statements are in `_claim_start` and `_claim`
  (`sqlite/store.py:196,236`); `append` → `_insert` (`:185-189`) runs an implicit **deferred**
  transaction. A `MAX(seq)` read inside a deferred transaction upgrades read→write and returns
  `SQLITE_BUSY_SNAPSHOT` without honouring `busy_timeout` (#84) whenever a peer committed in
  between. Taking the write lock first is a change to the append path, not an existing property.
- **redis.** The per-run seq key is a ZSET, not a counter  -  `zadd` at `:122`, `zscore` at `:145`,
  `zrange … withscores` at `:193,262`. `INCR` against it returns `WRONGTYPE`, i.e. every append
  would raise `StoreError`.

The `UNIQUE (tenant, log_key, run_id, seq)` indexes stay. They are no longer the guard  -  they
become the proof that assignment is correct.

**Enforcement:** the shared store contract suite grows a concurrency case  -  many tasks appending to
one run at once, asserting the result is contiguous with no duplicates. A backend that cannot pass
it must not implement the port. This is the same gap #127 describes from the other side; it lands
as one test, not two.

## 7. Consequences to land with the change

**All applied 2026-08-08**; the ledger is `00-project-index.md` §3.

- `test_runtime_service.py:890`'s gap assertion flips `== [2]` → `== []`, and `_drain`'s
  *"not this arm's to close"* paragraph is deleted  -  it stops being true.
- `coding-standards.md:113` (**§6**, not §7 as originally cited) states the rule this ADR overturns,
  and §1's precedence enumeration has to name D11 or this ADR outranks nothing by that doc's own
  ordering.
- `agentdeck/composition.py:44,58` exposes `clock` as a public keyword of `build_runtime`; its
  meaning changes and its docstring must say so.
- ADR-D5's *Explicitly unchanged* clause (`:151`) and the architecture doc's envelope-stamping split
  each get a dated amendment.
- CHANGELOG: logs no longer carry gaps after a dropped report or a transient append failure.

## 8. What was considered and rejected

- **Keep the counter in the Runtime, own it in one object** (a per-run writer that advances only
  after a successful append). Fixes the gap for a fraction of the cost, inside one file. Rejected
  only because it leaves the counter outside the store, which this decision moves on purpose; it
  remains the fallback if the port change proves too invasive.
- **A lock at the port**  -  `acquire`/`release`, many-readers-one-writer, so the Runtime could run
  the whole decision itself. Rejected on three counts. A lock held across the Runtime's decision is
  a *distributed* lock, and a process that stalls past its lease expiry wakes and writes while a
  second holder is live; the only fix is a fencing token the store validates on write, which is the
  conditional write with three more methods in front. Backends cannot supply it uniformly  -  SQLite's
  `BEGIN IMMEDIATE` is a transaction, not a lease held across an `await`; a Postgres advisory lock
  pins a pool connection for the whole decision; Redis offers only Redlock-style leases, the unsafe
  case. And it leaks like a `transaction()` port: anything the store can retry pushes "your block
  may run twice" into every caller.
- **A generic optimistic seam**  -  `append_if_unchanged(log_key, expected_lifecycle_version, …)`,
  with the Runtime reading, deciding and retrying on a stale version. This is the only shape that
  puts *all* lifecycle judgement in the Runtime, and it is cheap to build: every store already
  indexes lifecycle kinds (`memory:102`, `redis:123`, sqlite/postgres `_select_*_lifecycle`).
  Rejected because it does not achieve what it is for  -  the version may only move on lifecycle
  events, so the store still needs `LIFECYCLE_KINDS`, and `list_runs(status=…)` and the staleness
  scan need status folding in SQL regardless. It buys one fewer method and a retry contract at every
  call site, while leaving the coupling exactly where it was.
- **Optimistic append-then-reconcile**  -  write first, discover you lost, clean up. The vocabulary
  exists (`RunResumed`'s docstring already describes recording an interrupt again as the rollback).
  Rejected: a loser that crashes between its write and its cleanup leaves an open run
  indistinguishable from a real one, wedging the session for a staleness window  -  manufacturing
  more of the problem the takeover machinery exists to mop up.
- **Dropping the duplicated `kind`** from the wire. Separate decision, deliberately not taken:
  measured, the released `v2.0.0b4` reader cannot read a row missing the payload copy.
