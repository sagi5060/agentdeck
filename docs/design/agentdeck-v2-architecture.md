# AgentDeck v2 — Architecture Design

**Status:** proposal · **Baseline:** `agentdeck` 1.2.1 (`Sagi5060/agentdeck@60b95b6`) · **Date:** 2026-08-03

This document specifies the target architecture for evolving agentdeck from a declarative
harness over two SDKs into an agent development and runtime platform, following Clean
Architecture and SOLID. It is grounded in the current tree: every migration step names the
real module it moves. Three worked examples validate the design — a chat turn end-to-end,
adding pause/resume/cancel, and adding the Agent Client Protocol (ACP).

---

## 1. Goals and non-goals

The finish line is a platform that can absorb everything on the roadmap — sessions and
chat, tools and protocols, generated UI, integrations, observability, cost, audit,
multi-tenancy — **without any of those features requiring changes to the code that runs
agents**. The strategy is therefore not to design the platform; it is to design a small
core whose contracts every future feature can consume.

Explicit non-goals, recorded so they stay refused: no YAML/JSON agent-definition DSL
(Python authoring by convention is a strength, not a gap); no auth system in the core (a
`Principal` on the context plus a `PolicyPort` is the whole footprint); no marketplace; no
dashboard before the event schema is stable; no hosted "control plane" until there is an
organization willing to operate one.

---

## 2. Principles

**The Dependency Rule.** Source-code dependencies point inward only. The core imports
nothing from the OpenAI Agents SDK, LangGraph, FastAPI, Redis, or any protocol library.
Adapters import the core plus exactly one external system each. Surfaces import the
runtime service and render its events. The current tree violates this everywhere —
`app.py` imports `agents.SQLiteSession` directly, `serve.py` knows the shape of both
runner outputs — and that violation is precisely why every feature today costs two
implementations.

**Clean Architecture mapping.** Entities are the core nouns (§4.1). Use cases are the
`Runtime` service (§4.6) — the application-specific orchestration of "start a run, fan out
events, honor signals." Interface adapters are Ring 2. Frameworks and drivers are Ring 3
together with the SDKs themselves.

**One event log, many readers.** The single highest-leverage design decision is the
canonical, versioned `Event` union. Streaming chat, workflow node updates, interrupts,
tool traces, generated UI, cost accounting, audit, replay, evals, the approvals inbox, and
the dashboard are all the same log read differently. It is the platform's real public
contract — more so than the Python API — and it is versioned from day one.

**Unify observable behavior, not programming models.** The Agents SDK loop and a LangGraph
graph have genuinely different execution models. Forcing one execution abstraction over
both produces a lowest-common-denominator that makes each engine worse. The port therefore
sits at the **lifecycle and event boundary** — start, stream, interrupt, resume,
checkpoint, cancel — and each engine stays idiomatic behind it.

---

## 3. The three rings

```text
┌────────────────────────────────────────────────────────────────────┐
│ RING 3 · SURFACES        serve (FastAPI/SSE) · acp (stdio) · cli   │
│   thin, dumb, logic-free — they render Ring-1 events               │
├────────────────────────────────────────────────────────────────────┤
│ RING 2 · ADAPTERS                                                  │
│   engines/     openai_agents · langgraph · <next year's thing>     │
│   stores/      memory · sqlite · redis · postgres                  │
│   control/     memory · redis                                      │
│   tools/       mcp · functions · http                              │
│   protocols/   sse · acp · ag-ui · a2a                             │
│   caps/        sandbox_fs · sandbox_shell · acp_client_fs …        │
│   telemetry/   langfuse · otel                                     │
├────────────────────────────────────────────────────────────────────┤
│ RING 1 · CORE  (zero I/O, pydantic + stdlib only)                  │
│   nouns:  Invocable · Session · Run · Event · RunContext ·         │
│           ContentBlock · Interrupt · Artifact                      │
│   ports:  EnginePort · EventStorePort · EventSinkPort ·            │
│           ControlPort · capability ports · ToolSourcePort ·        │
│           PolicyPort · SecretsPort                                 │
│   use case: Runtime (run / resume / signal / replay)               │
└────────────────────────────────────────────────────────────────────┘
        dependencies point downward-in only, never outward
```

A Ring-2 adapter must be **independently deletable**: removing `adapters/protocols/acp/`
deletes ACP support and breaks nothing else. That property is the day-to-day test of
whether the layering is being respected.

---

## 4. Ring 1 — the core

### 4.1 Nouns

`Invocable` is the root noun — deliberately not `Agent`. An agent, a workflow, a skill, a
sub-agent, and (later) a remote A2A agent are all things that can be started with input in
a context and that emit events until they finish. Making `Agent` the root is the trap that
produced today's bifurcation; `agents/registry.py` and `workflows/registry.py` are the
same class written twice because the shared noun was missing.

`Session` is the durable conversation/state container keyed by `session_id`, holding the
ordered event log and engine-specific thread state. `Run` is one invocation within a
session, addressable by `run_id` — a first-class noun because out-of-band control (§9)
requires naming a run from another process. `ContentBlock` is the input/output atom (text,
image, resource reference, audio), adopted now because ACP, A2A, and multimodal input all
require it and retrofitting it later touches every surface. `Interrupt` and `Artifact`
carry over from the current design with types instead of raw dicts.

```python
# core/invocable.py
class InvocableKind(StrEnum):
    AGENT = "agent"; WORKFLOW = "workflow"; SKILL = "skill"

class InvocableSpec(BaseModel):
    """Engine-neutral description. The authoring layer (BaseAgent/BaseWorkflow
    class attributes) compiles down to this; engines consume only this."""
    name: str
    kind: InvocableKind
    engine: str                        # "openai-agents" | "langgraph" | ...
    capabilities: CapabilityRequest    # what it needs (fs, shell, approval, tools)
    metadata: dict[str, Any] = {}
    native: Any                        # opaque engine payload; core never inspects it
```

```python
# core/content.py
class TextBlock(BaseModel):      type: Literal["text"] = "text"; text: str
class ImageBlock(BaseModel):     type: Literal["image"] = "image"; media_type: str; data_b64: str
class ResourceBlock(BaseModel):  type: Literal["resource"] = "resource"; uri: str; media_type: str | None = None
class DataBlock(BaseModel):      type: Literal["data"] = "data"; data: JsonValue

ContentBlock = Annotated[TextBlock | ImageBlock | ResourceBlock | DataBlock, Field(discriminator="type")]
Input = list[ContentBlock]           # replaces `message: Any` / `input: str` everywhere
```

*(Amended 2026-08-06, issue #101: `DataBlock` added.* Structured data had no canonical shape
and two engines had routed around that — an `output_type` agent's validated result rode a
namespaced `custom` event, and a workflow's final state arrived as `str(dict)`. Both needed
the *input* direction too (a workflow's initial state is posted JSON), which a field on
`run.completed` could not serve, so the block won: it fits `Input` everywhere `Input`
already appears, in both directions, and the terminal event gets it for free. `JsonValue` is
the type, so a value that could not survive the wire is rejected at construction rather than
failing later in a store — plus a validator for the one class of value `JsonValue` itself
lets through: `NaN` and `±Infinity` have no JSON literal and serialize as `null`, which would
make a consumer's copy of an event differ, silently, from the store's. Producers degrade such
a leaf to text under their own declared ceiling instead. **Content policy:** text and data
blocks are stored **in full** —
they are the caller's own input and the run's own declared result, and a truncated one
cannot be replayed or reconciled; the preview + size + hash treatment stays specific to
*tool* results, where the bytes are unbounded and engine-chosen. **D8: additive minor**, no
`v` bump — no field was renamed, removed, or given a new meaning.*)

### 4.2 The Event schema — the keystone

*(Amended 2026-08-04 to match the frozen schema in `pr1-event-schema-prompt.md`, which is
the authoritative statement; this section is its summary.)*

Every event shares a versioned envelope of exactly eight fields; the payload is a
discriminated union **nested** under `payload`, so envelope and payload fields never
share a namespace. Evolution rules (D8): adding a new `kind` or an optional field is a
minor change; renaming, removing, or changing the meaning of a field bumps `v`.
Consumers must ignore kinds they don't know — implemented inside `Event` itself, whose
validator falls back to `UnknownEvent(kind, raw_payload)` instead of raising, which is
what lets a v1 dashboard render a stream from a v1.4 engine. The envelope is **closed**
(D9): new needs go into payloads or into `run.started`; an envelope addition must
demonstrate that routing/ordering/isolation itself is impossible without it.

```python
# core/events.py
class Event(BaseModel):
    v: int = 1
    kind: str                # discriminator, mirrors the payload class
    seq: int                 # per-run, CONTIGUOUS from 0; assigned by the store (decision A, ADR-D11)
    run_id: str
    session_id: str | None
    tenant: str              # stamped outside-in from RunContext; engines cannot set it
    origin: str              # which invocable produced it (never the engine) — multi-agent attribution
    ts: AwareDatetime        # informational; ordering authority is seq, never ts
    payload: <discriminated union by kind>
```

| Category   | Kinds | Notes |
|---|---|---|
| Lifecycle  | `run.started`, `run.completed`, `run.failed`, `run.paused`, `run.resumed`, `run.cancelled` | `run.started` is the per-run join point: `invocable`, `parent_run_id`, context snapshot (`principal`, `trace_id`, `budget`, `triggered_by`); `run.completed` carries `output: Input` and the **authoritative** usage aggregate; `run.failed` carries structured `error_code` + `retryable`; exactly one terminal event per run, always last |
| Content    | `text.delta`, `thought.delta`, `message.completed` | every delta carries `message_id` (runs can hold multiple messages); `message.completed` carries the **full final text** (decision B) — deltas are streaming UX, this event is the record; one `message_id` never spans two origins |
| Tools      | `tool.call.started`, `tool.call.completed` | `call_id`, `tool`, `args`; results as bounded `result_preview` + `result_size` + `result_sha256` + optional `artifact_id` — never raw bytes inline |
| Workflow   | `node.updated`, `custom` | `node.updated`: `node`, `state_patch` (shallow merge); `custom` payloads carry a namespaced `name` — per D10, kinds are minted only in core; engines translate into existing kinds or use `custom`, and recurring `custom` usage is a promotion signal, not a precedent |
| Control    | `run.interrupted`, `input.appended`, `control.requested`, `control.observed` | interrupt: `interrupt_id`, `reason: "human" \| "pause" \| "approval"`, typed `payload`, `thread_id` — the approvals inbox is a filter on this kind; `input.appended` records mid-turn steering; the two `control.*` kinds are the request and the safe-point observation for **any** verb (`cancel`, `pause`, `resume`, `steer`), with the effect staying the verb's own kind |
| Data       | `artifact.created`, `usage.reported` | artifacts by reference (id, media type, uri, size); `usage.reported` is per-model-call and advisory — the terminal aggregate wins |
| Reporting  | `status.reported`, `progress.reported` | what the run says it is doing: `status.reported` carries a non-empty human-readable `message`, `progress.reported` a required `step` plus optional `current`/`total` (never a percentage). Advisory in the same sense `usage.reported` is — neither is a lifecycle kind, neither is terminal, and status still folds from lifecycle kinds alone |

Engines emit payloads; the `Runtime` stamps the envelope (`seq`, `tenant`, `ts`,
`origin` as reported by the adapter). Engines therefore cannot lie about ordering or
tenancy. Contiguous `seq` upgrades ordering into **loss detection**: a consumer seeing
`0,1,2,4` knows 3 is missing and refetches from the store. Supporting invariants:
persist-before-yield (an event a consumer has seen is already in the store); an
interrupted-then-resumed run keeps the same `run_id` with continuing `seq`.

*(Amended 2026-08-08, ADR-D11.)* The stamping is split differently now: **the store** assigns
`seq` and `ts`, in the same indivisible step that persists the event, and the `Runtime` supplies
the rest of the envelope through the `RunContext` it hands the store (`run_id`, `session_id`,
`tenant`) plus `origin`. The engine's position is unchanged — it still yields payloads and still
cannot forge `seq` or `tenant` — and so is everything the paragraph above claims about ordering
and loss detection. What changes is that those claims become true rather than nearly true: with
allocation separated from the write by an `await`, a refused append left the number it had taken
spent, and the resulting hole was indistinguishable from an event lost in transit. `last_seq`
came off the port with the counter it existed to recover.

*(Ruled 2026-08-05, Milestone 0 checkpoint, issue #57.)* `origin` is invocable-scoped:
"speaker" means the invocable the caller addressed, not the SDK's internal sub-agent, so
an internal handoff (one invocable delegating to another inside its own run) does not
change `origin` for the rest of that run. This is the contract, not a gap — see
`milestone-0-findings.md` §3 for the analysis and the alternative (an additive,
payload-level speaker field) that remains available later if a concrete consumer needs
sub-agent-level attribution.

*(Amended 2026-08-06, issue #101.)* A **structured result** is a `DataBlock` in
`run.completed.output` (§4.1) — a validated `output_type` result, a workflow's final state —
not a namespaced `custom` event and not a stringified dict. No payload class changed and no
kind was added, so this is D8-additive; of the three golden snapshots that carry an `Input`
(`run.started`, `run.completed`, `input.appended`), the first two gained a `data` block to
freeze its wire shape on both the input and the result channel. One asymmetry, since closed (#109): `Event` tolerates an unknown *kind* and
`ContentBlock` now tolerates an unknown block `type` the same way. Before that fix a
reader older than a new block type rejected the whole event rather than skipping the
block. Measured, the blast radius is wider than one event: `SqliteEventStore.list_runs`
deserializes each run's last lifecycle row in one comprehension, so one structured
`run.completed` in a shared store makes an older process's listing fail for the whole tenant,
runs it wrote itself included. Mixed-version readers against one event store are therefore
unsupported across this change. Bumping `v` would not help that reader (nothing branches on
`v` yet); the fix is issue #109, closed below.

*(Amended 2026-08-06, issue #109.)* The asymmetry above is closed, D8-additive, before the
`v2.0.0` stable tag — the last point where "one version of this schema exists" made the fix
free. `ContentBlock` in `core/content.py` gained an `UnknownBlock` member
(`{type: str, raw_block: dict}`), mirroring `UnknownEvent`: a `WrapValidator` on the
`ContentBlock` annotation falls back to it whenever a block's `type` isn't one of the known
literals, so the failure is caught at the block itself rather than propagating up through
whichever payload (`run.started.input`, `run.completed.output`, `run.resumed.value`, …)
happens to hold it — one mechanism instead of one per call site. A malformed *known* block
still raises, the same trap `UnknownEvent` closes for a payload named `raw_payload`. Measured
against `origin/dev`'s own `ContentBlock` (`tests/core/test_old_reader_block_compat.py`): that
reader really does reject a block type this addition introduces, and this tree's reader
parses the same wire event, keeps the raw block, and leaves `status_of` and the terminal
invariant unchanged.

*(Amended 2026-08-06, issues #44 and #94.)* Run control is **three phases, one vocabulary**:
`control.requested {verb, reason?}` records that a signal was written, `control.observed
{verb, safe_point}` records that the run reached a safe point and is acting on it, and the
effect stays the verb's own kind (`run.cancelled`, `run.paused`, `run.resumed`,
`input.appended`). Two kinds carry all four verbs — `cancel`, `pause`, `resume`, `steer` —
because minting them per verb would mean revising the set when Story 3 ships pause and
steering. Neither is a lifecycle kind: a request leaves §4.4's status exactly where it was,
which is the distinction the phases exist for, since under cooperative control "recorded" and
"stopped" can be a whole tool call apart. A request that loses the race with a terminal event
records nothing at all — the terminal event is a run's last event by invariant, so the no-op
signal has nowhere to land, and that is why there is no `control.rejected`. Both `verb` and
`safe_point` (`stream_item`, `tool_dispatch`, `node_boundary`) are **closed and complete at
birth**: adding a member later is additive for a writer but not for a reader, which raises on
a known kind rather than skipping it the way it can with an unknown one.

Separately, `run.resumed` gained `value: Input | None` — the answer the resume carried, stored
in full under §4.1's content policy. It rides on that event because the same append is the
`WAITING_HUMAN` → `RUNNING` transition: recording the value anywhere else leaves the window
#94 measured, where the log says a run was answered, no longer holds the answer, and the
engine parked at its interrupt can never be brought back in line — a run stranded, recoverable
only by hand. The reverse direction needs no new vocabulary either: status is a fold over an
append-only log, so a resume that cannot be carried through returns the run to
`WAITING_HUMAN` by recording its interrupt again. **D8: additive minor**, no `v` bump — two
new kinds and one new optional field, nothing renamed, removed or redefined. Measured against
`origin/dev`'s own parser: a v2.0.0b4 reader parses the new `run.resumed` and drops `value` as
an unknown field, and reads both new kinds as `UnknownEvent` with no status effect — so
#107's mixed-reader outage does *not* recur here. The analogous edge is one level deeper and
still open: a future *block* type inside `value` would be rejected by a reader that knows the
field but not the block, and because `run.resumed` is a lifecycle kind that is again the
tenant-wide listing failure — the same `UnknownBlock` gap (#109), not a new one.

*(Amended 2026-08-06, issue #47.)* Two **reporting** kinds joined the table above,
`status.reported` and `progress.reported`, so a run can say what it is doing instead of leaving
every client to infer it from tool calls. Additive under D8 — two kinds, nothing renamed, removed
or redefined — and measured against the released parser rather than argued:
`tests/core/test_old_reader_compat.py` loads `core/events.py` and `core/status.py` out of git at
the newest released tag (`v2.0.0b4`) and asserts that reader reads both new kinds as
`UnknownEvent`, keeps their payloads, folds the same status, still sees the run as open, and
*still parses every kind it already knew*. A tag rather than a branch: a measurement pinned to
`dev` falsifies itself the moment it merges into `dev`, which #112 landing between this branch's
first push and its merge would already have demonstrated. CI checks out full history so this gate
runs rather than skips. The deliberate line: **neither is a lifecycle kind**, so `LIFECYCLE_KINDS`
and `TERMINAL_KINDS` are unchanged and a store that indexes by kind — the thing #101 showed can
fail a whole tenant's listing — has nothing new to deserialize. The naming departs from the
issue's suggested `agent.status`/`agent.progress` on purpose: a workflow node and a tool emit
these too, and `origin` already names the invocable, so the kind names what the event is about.
The emitter side is §4.3's `reporter`.

### 4.3 RunContext — thread it everywhere, today

```python
# core/context.py
@dataclass(frozen=True, slots=True)
class RunContext:
    tenant: str                        # single hardcoded tenant is fine — the field is not optional
    principal: Principal               # who asked; Principal is a frozen dataclass, not an auth system
    run_id: str
    session_id: str | None
    trace_id: str
    deadline: AwareDatetime | None
    budget: Budget | None              # max tokens / max $ / max tool calls
    idempotency_key: str | None        # forwarded to side-effecting tools
    gate: Gate                         # cooperative pause/cancel (§9)
    caps: CapabilityProvider           # caller-injected capabilities (§10)
```

This is the highest-leverage hour in the plan. Multi-tenancy, rate limiting, budget caps,
audit attribution, and distributed tracing are each trivial later *if and only if* this
parameter already flows through every call. Every port method below takes `ctx`; no
exceptions, no "we'll add it when we need it."

*(Amended 2026-08-05, as built: the data fields are all there, plus `parent_run_id` and
`triggered_by` so `run.started` can be filled from the context alone; `principal` is a `str`
until an auth story needs more. `gate` and `caps` are the two fields that are behavior rather
than data — they wait for Story 3 and Story 4, which build the things they would point at.)*

*(Amended 2026-08-06, issue #47.)* A third behavior field: `reporter: Reporter`
(`core/reporting.py`), the mirror image of `gate` and here for the same reason. A cooperative
seam has to reach code the Runtime never sees — control flows *in* through the gate, status and
progress flow *out* through the reporter — and neither can be threaded any other way, because a
tool six frames inside an engine cannot yield an event and must not import a Runtime. Both
default to doing nothing, so a `RunContext` built outside a run stays a value object: the
reporter validates its arguments and drops the result. `Runtime.run`/`resume` bind both, and the
reporter's buffer is per run and returned to the run, never held on the Runtime, so two
concurrent runs cannot drain into each other. The bargain runs both ways: the emitter never
awaits the store, and the Runtime drops a report the store refuses rather than failing the run
over it — an advisory event is not worth a run. What that costs is stated on `Reporter`: reports
are drained at the engine's next payload, so one made inside a single long tool call surfaces
when the call ends and not while it runs. The alternative considered and rejected was a `ContextVar` — no threading at
all, but a context var set inside an async generator leaks into the task that resumes it, so two
runs iterated from one task would report into each other's logs, and a report is tenant-scoped
data.

### 4.4 Run lifecycle

```text
                 signal PAUSE          signal RESUME
  PENDING → RUNNING ──────────→ PAUSED ──────────→ RUNNING
               │                                     │
               │ engine emits run.interrupted        │
               ├────────────→ WAITING_HUMAN ─────────┘  (resume with value)
               │
               ├──→ COMPLETED      (terminal)
               ├──→ FAILED         (terminal)
               └──→ CANCELLED      (terminal; reachable from RUNNING, PAUSED, WAITING_HUMAN)
```

*(Amended 2026-08-06, issue #44: the two `control.*` kinds are deliberately **not** in this
machine. A recorded signal moves nothing — a paused run is one that emitted `run.paused`, not
one somebody asked to pause — so `LIFECYCLE_KINDS` stays the seven kinds it already was.)*

Transitions are guarded in one place (`core/status.py`): a signal arriving after a
terminal state is a **no-op, not an error** — the race is inherent and the state machine
absorbs it. `PAUSED` (operator pressed pause) and `WAITING_HUMAN` (the run itself asked a
question) are distinct states because they resume differently: the first resumes with
nothing, the second resumes with a value.

### 4.5 Ports

Ports are small and role-shaped (ISP): a surface that only reads events depends on
`EventSinkPort`, never on a god `Platform` interface.

*(Amended 2026-08-05, as built: `EventStorePort` keys on `log_key`, not `session_id` — a
run without a session is its own log, so persist-before-yield holds for one-off runs too. Its
reads split in two: `read(log_key, ctx)` is the session's whole history in append order, and
`read_run(log_key, run_id, ctx, from_seq=0)` is the inclusive range a consumer uses to refetch
after a gap. A range read has to name the run, because `seq` restarts at 0 per run and a
seq range over a whole log would splice together the tail of every run in it — the design's
`read(session_id, after_seq)` had both that bug and an off-by-one, since `after_seq=0` would
have excluded event 0. `EnginePort` ships with `start` only; `supports` and `resume` land with
the engines that need them, in Stories 2–3. `start` returns an `AsyncGenerator`, not a bare
`AsyncIterator`: the Runtime closes the stream when it stops reading early — at a terminal
payload, or when its own consumer walks away — so an engine's cleanup runs either way. `ControlPort` and the capability ports are
unbuilt: they arrive with Story 3 and Story 4 respectively.)*

*(Amended 2026-08-05, as built: `EventStorePort` also grew `last_seq(log_key, run_id,
ctx)`, a `run_status(log_key, run_id, ctx)` projection (default: fold the bounded,
indexed result of `read_run` through `core/status.py`'s `status_of` — a projection, never
a second store, per this doc's own two-stores decision), `list_runs(ctx, status=None)`
scoped to one tenant, and `offset`/`limit` pagination on `read`. `offset` counts events
from the start of the log rather than naming a `seq`, for the same reason `read_run` exists:
`seq` is per run, so it cannot address a position in a log holding several. Status
derivation stays in `core/status.py`, but a store may answer `list_runs`/`run_status` with
whatever it can index over the lifecycle kinds that module exports — SQLite reads the last
lifecycle row per run in one statement. The Runtime's resume path and `Runtime.pending` use
these instead of folding a whole log to answer one run's status or find every run waiting on
a human; the now-unused `list_log_keys` is gone.)*

*(Amended 2026-08-05, as built: `EventStorePort` also owns the resume claim —
`claim_resume(log_key, run_id, event, ctx) -> bool`, a conditional append that records
`run.resumed` if and only if the run is `WAITING_HUMAN` *and* the event's `seq` is still the
run's next one, indivisibly. The Runtime's `WAITING_HUMAN` -> `RUNNING` transition is that
one call: the write that publishes the transition is the write that tests for it, so two
processes sharing a store cannot both claim one interrupt. `status_of` still derives status,
so this is not a second store; a store only has to make the two checks and the append one
step — SQLite in a single `BEGIN IMMEDIATE` transaction, the dict store for free, since
neither the fold nor the append suspends. The `seq` half matters because a caller stamps its
event before claiming: a run that was resumed and interrupted again between those two
moments is waiting once more, and letting that stale claim through would write a `seq` twice.
A loser gets `False`, never an exception, and never writes.)*

*(Amended 2026-08-06, as built: both SQLite adapters — the event log and the control-signal
table — open in WAL mode with an explicit 5-second busy timeout, and translate `sqlite3.Error`
into `errors.StoreError` at every public method, so no library type crosses a port (§5 of the
coding standards applied to the store boundary). This is what makes the sentence above hold
in practice: a losing `claim_resume` waits for the winner's transaction to commit and then
reads the `RUNNING` status it published, instead of meeting a raw `database is locked`. A lock
held past the busy timeout is a `StoreError` — a store nobody can write to, deliberately not
folded into the `False` that means somebody else won. Two operational consequences the design
doc did not state: WAL puts `-wal`/`-shm` files beside each database (they belong to it for
backup and deletion), and it relies on cross-process shared memory, so a SQLite store on NFS
or SMB is unsupported — that deployment wants the Redis or Postgres store. Converting a file
*into* WAL needs an exclusive lock that SQLite refuses outright while a peer is writing, so a
connection that cannot switch the mode keeps the one the file has: slower under contention,
never wrong, and never a failure to open.)*

*(Amended 2026-08-06, #83 — as built: **one session runs one turn at a time**, and
`EventStorePort` owns that claim too — `claim_start(log_key, event, ctx, stale_before) ->
SessionClaim`, the resume claim's sibling. It appends a run's opening `run.started` if and only
if that log has no open run, indivisibly; SQLite in one `BEGIN IMMEDIATE` transaction, the dict
store for free. A session is **busy** when one of its runs has recorded a lifecycle transition
and not a terminal one, `WAITING_HUMAN` included: an interrupted run still owns the engine
thread it will resume on, and a second run against that thread would overwrite the checkpoints
the resume needs. A run with no transition at all is `PENDING`, which no store can tell from a
run it never saw, so it holds nothing — the line `list_runs` already draws.*

*Busy-ness is **derived from the log**; there is no lease table, TTL row or heartbeat, which is
what keeps §4.4's status a projection rather than a second store. Cross-process holds by
construction, for the same reason the resume claim does: the condition and the write are one
store operation, so two servers cannot both find a session idle. The refusal is **data, not a
store failure** — `SessionClaim.held_by` names the run holding the session and nothing is
written; only an unreachable store raises (`StoreError`, as above). The Runtime turns that into
`errors.SessionBusyError` naming the session and the holding run, raised from `run()` before any
event is yielded: a caller that asked for a turn and got an empty stream could not tell it apart
from a turn that produced nothing. Over HTTP that has to be decided *before* the response begins —
a `StreamingResponse` has already committed `200` and `text/event-stream`, so a refusal after it
can only reach a client as a body that stops — hence the surface pulls the opening event and
answers **409** with the holding run named. The same claim is covered by the cancellation arm that
closes a run whose consumer walked away: a client that disconnects between committing the claim and
receiving anything gets its run closed as `run.cancelled`, rather than leaving it open and holding
the session for a window.*

*Queueing the loser is deliberately **not** built. In-process queueing does not survive a second
worker, and a store-level queue is a lease with ordering — fencing, expiry, stale-entry
reclamation — which is real distributed machinery for a problem whose usual cause is a
double-clicked send button. A client retry already is a queue.*

*The one state the log cannot distinguish is a run whose process was **killed outright**: every
graceful exit closes its run, so silence is all that is left to go on. Hence a **staleness
window** — `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS`, one hour by default, a settings value
and not a constant, and required to be **positive** (at zero a run's own opening event is already
stale to the next caller a moment later, which turns the guarantee back into a race). An open run
whose own last event (any kind, so a streaming turn keeps resetting it) is older than that stops
holding its session, comes back in `SessionClaim.overridden`, and is closed by the claiming turn
as `run.failed` /`cancelled_hard` under **its own** `origin` and next `seq` — the store never
stamps an event, since the Runtime is the only assigner of `seq`. The takeover is logged at
WARNING, and it can always be premature: that trade is chosen on purpose, because a permanently
wedged session is the worse failure. Failing to *close* the overridden run is not worth failing
the new turn over either — the claim is already committed, so the close is dropped with a log line
and the next turn, which meets the same stale run, tries again.*

*Four consequences worth stating for whoever operates this. A session a killed process left
claimed is refused until the window elapses. A run waiting on a human for longer than the window
is closed as failed the next time somebody starts a turn on that session, so an installation with
slower approvals raises the setting. The window is **not skew-free**: each worker compares its
own `clock()` against a `ts` a peer stamped, so across machines the effective window is the
configured one minus the worst clock skew, and a worker running more than a window fast would take
over live sessions on sight. Keep the fleet on NTP and treat skew as eating into the budget. And
**the window has a floor the code cannot enforce**: shortened below the longest quiet stretch of a
healthy turn, an open run looks abandoned while it is still working, so the next turn takes the
session from a live one and both run on the same conversation — one turn per session stops holding
altogether, rather than merely cleaning up early. How long a turn may be quiet is a property of the
deployment, so only positivity is validated; the rest is the setting's docstring and this note. An
explicit operator "abandon run" route can follow if it is ever asked for.*

*Because a takeover can be premature, the run stepped over may still be alive and write again at
a `seq` its own closing event already used — the one corruption `check_contiguous` cannot see,
since it looks for gaps and not for duplicates. So `(tenant, log_key, run_id, seq)` is now
**unique** in the SQLite log (the per-run index carries the constraint) and the dict store refuses
the same pair in `append`: a resurrected run fails loudly with `StoreError` at its next write
instead of putting two different events at one `seq`, which would make every consumer's refetch of
that `seq` a coin toss. Such a run does then end twice in the record — its own failure lands after
the takeover's — which is detectable, unlike the duplicate. `seq` stays per run, so two runs of one
session log both counting from 0 is unaffected.*

*Consequence for tests and for anything that shells a second turn into a live session: two
concurrent runs in one log are no longer reachable through the Runtime, only through a stale
takeover. The engine-side lock that protects a session's execution state from two turns at once
(`adapters/engines/openai_agents/reconcile.py`) is therefore no longer the first line of defence,
but it is still the last one, and keeps its own test.)*

*(Amended 2026-08-06, #75 — as built: `adapters/stores/redis/` and `adapters/stores/postgres/`
implement the same port, so the sentences above about "two servers sharing a store" now describe
a deployment that does not share a filesystem. Both claims are atomic per backend, by the
mechanism that backend actually has, and neither reimplements the status projection:*

- ***Postgres** decides and writes inside one transaction that takes a transaction-scoped
  advisory lock on `(tenant, log_key)` before reading anything. Lock first, then read: a lock
  taken after the decision would leave exactly the check-then-write window it exists to close.
  The isolation level is **pinned to `READ COMMITTED`** rather than inherited, and that is
  load-bearing — under a snapshot taken at the transaction's first statement (`REPEATABLE READ`
  or stricter) the loser's reads predate the winner's commit even though it waited for the lock,
  so it decides on a log that no longer exists. The pin is the *whole* defence rather than a
  second layer of one, because the transaction's literal first statement is the `lock_timeout`
  setting, not the lock. That timeout is Postgres's spelling of the SQLite busy timeout, same 5
  seconds and same reasoning: wait a peer out, but surface a wedged holder as `StoreError` rather
  than hanging — with one gap, first-use schema setup, whose lock is session-scoped and taken
  before any transaction exists. The alternatives were considered and rejected: `SELECT ... FOR
  UPDATE` has no row to lock on an idle session's empty log, and a conditional `INSERT ... WHERE
  NOT EXISTS` would have to express `status_of` in SQL — a second copy of the status machine,
  which is the thing ADR-D5 forbids.*
- **The plain `append` is under that same lock**, which the design doc's paging guarantee turns
  out to require: row order is `BIGSERIAL`, assigned at insert and published at commit, so an
  unlocked append can take a *later* number than a claim's in-flight insert and still commit
  *first*. The claim's event then appears at an offset a reader has already passed — never
  delivered, while a neighbour is delivered twice. "The log only ever grows at the end" is a
  promise about commit order, not insert order, and only serializing writes per log keeps it.*
- ***Redis** uses `WATCH`/`MULTI`/`EXEC`, not a Lua script, for that same reason: the decision
  runs in Python so `core/status.py` stays the one place a status is derived, and `EXEC` refuses
  the write if any key the decision read moved under it. A losing round re-reads and answers from
  what the peer actually wrote. Bounded rounds, then `StoreError` — an unbounded optimistic retry
  is a hang. **Every write goes through the same machinery, `append` included**, because one
  `seq` per run is the other thing a store has to enforce and Redis has no unique index to
  enforce it with: the run's `seq` index is watched and each `(run_id, seq)` checked, so a peer
  that spends a `seq` under this write aborts it. That check belongs to the *conditional* writes
  too — SQLite and Postgres get it on every path from one blanket UNIQUE index, so a Redis store
  that guarded only `append` would diverge on the claim paths alone, and a duplicate `seq` is
  invisible from the log either way. A spent `seq` is a `StoreError` and not a claim's
  refusal-as-data: it is corruption, not a race somebody lost. The log carries its own indexes (per-log and per-run lists, that
  `seq` sorted set, the run's latest lifecycle **event**, and the run sets each log and tenant
  own) written in one `MULTI` with the event, so no reader sees a log entry missing from its run's
  index; what is stored is always an event, never a status. `MULTI` is atomic against other
  clients, not against a queued command of its own failing — which on this store's own keys and
  types cannot happen on well-formed input.*

*ADR-D5's operational separation is expressed as the one thing each backend can enforce: a
Postgres **schema** (`agentdeck_events`) and a Redis **key prefix** (`agentdeck:events`), so a
database holding LangGraph checkpoint tables or an instance holding the openai-agents adapter's
`RedisSession` conversations shares nothing with the log. Three things the design doc did not
say: Redis is only as durable as the instance is configured to be, so a deployment using it as
its record wants `appendonly yes` **and** `maxmemory-policy noeviction` — an evicted lifecycle
key makes a live run stop looking like it holds its session; Postgres adds `psycopg[binary]` to
the `[durability]` extra, which until now meant checkpointer savers only, and is imported inside
`resolve_event_store`'s own branch so selecting any other backend does not need it; and both
backends are reachable as `AGENTDECK_EVENTS_BACKEND=redis|postgres` through the composition root
(#74). The cross-store contract suite is the merge gate for all of this and runs every case
against all four backends on **real servers** — service containers on the gate job, skipping with
a named env var locally, and the gate's skip-count ceiling is what turns "the containers quietly
went away" red.)*

```python
# core/ports/engine.py — the lifecycle/event boundary
class EnginePort(ABC):
    engine: ClassVar[str]

    @abstractmethod
    def supports(self, spec: InvocableSpec) -> bool: ...

    @abstractmethod
    def start(self, spec: InvocableSpec, input: Input,
              history: list[Event], ctx: RunContext) -> AsyncIterator[EventPayload]: ...

    @abstractmethod
    def resume(self, spec: InvocableSpec, thread_id: str,
               value: Any, ctx: RunContext) -> AsyncIterator[EventPayload]: ...
```

```python
# core/ports/store.py
class EventStorePort(ABC):
    async def append(self, session_id: str, events: Sequence[Event], ctx: RunContext) -> None: ...
    async def read(self, session_id: str, ctx: RunContext,
                   after_seq: int = 0) -> list[Event]: ...

# core/ports/sink.py — bounded fan-out; a slow sink must never stall a run (see the §4.6
# amendment: fed from a queue that drops rather than wait, so a sink is a lossy tap)
class EventSinkPort(ABC):
    async def emit(self, event: Event) -> None: ...
    async def close(self) -> None: ...  # optional, default no-op (#99): a buffering sink's
                                        # one bounded chance to flush at shutdown

# core/ports/control.py
class Signal(StrEnum): PAUSE = "pause"; RESUME = "resume"; CANCEL = "cancel"

class ControlPort(ABC):
    async def signal(self, run_id: str, sig: Signal, ctx: RunContext) -> None: ...
    async def poll(self, run_id: str) -> Signal | None: ...
    async def status(self, run_id: str) -> RunStatus: ...
    async def set_status(self, run_id: str, status: RunStatus) -> None: ...
```

*(Amended 2026-08-08, `docs/delivery/plan-v2-cutover.md` phase 3: shipped as one `SandboxPort`
in `core/ports/sandbox.py`, not a `capabilities.py` holding a pre-split
`FilesystemPort`/`TerminalPort`. Three consumers justify the seam; none of them needs half of it,
and the openai-agents engine needs none of it — it takes the session as an opaque SDK handle off
the adapter, so no port method describes it. `write_text`, `ApprovalPort` and per-call
`RunContext` are absent for the same reason: nothing called them. Split when a consumer genuinely
wants one half.)*

```python
# core/ports/sandbox.py — caller-injected (§10); the sandbox is ONE implementation
class SandboxPort(ABC):
    async def read_text(self, path: str | Path, encoding: str = "utf-8") -> str: ...
    async def write_bytes(self, path: str | Path, content: bytes) -> None: ...
    async def mount_dir(self, src: Path, at: str | Path, *, read_only: bool = True) -> None: ...
    async def exec(self, *cmd: str, timeout: float | None = None) -> ExecResult: ...

class ApprovalPort(ABC):
    async def request(self, req: ApprovalRequest, ctx: RunContext) -> Decision: ...

class CapabilityProvider(BaseModel, arbitrary_types_allowed=True):
    filesystem: FilesystemPort | None = None
    terminal: TerminalPort | None = None
    approval: ApprovalPort | None = None
    def require(self, name: str) -> Any: ...   # raises CapabilityUnavailable with a clear message
```

`ToolSourcePort` (MCP servers, plain functions, HTTP tools all yield `ToolSpec`s),
`PolicyPort` (`allow(principal, action, resource) -> Decision`), and `SecretsPort`
(`get(ref) -> SecretValue`) complete the set. Each has an in-memory/no-op implementation
so the core is runnable with zero infrastructure.

*(Amended 2026-08-05, #78 — as built, `ToolSourcePort` is
`resolve(spec: InvocableSpec) -> ToolSet` and there is no `ToolSpec`. A `ToolSet` carries
`tools` (engine-native handles, opaque to core), `unavailable` (the names the invocable asked
for and did not get) and `notice` (prose to put in front of the model when something is
missing, empty otherwise). Decomposing an MCP server into declarative `ToolSpec`s would mean
agentdeck dispatching tool calls itself — execution, which stays in the SDK — so the handles
travel opaque and the ring that attaches them is the one that understands them. The method is
**sync**: a source resolves from state the composition root already connected, and prompt
assembly, its only caller, is synchronous — and it is the only sync method in `core/ports/`.
Connect/close are deliberately **not** on the port: `App` drives the MCP lifecycle in its
lifespan exactly as it does today, so resolving tools never connects a source. `ToolSet.tools`
is typed `tuple[Any, ...]` — the second blessed opaque field after `InvocableSpec.native`, for
the same reason.

Two costs, named now rather than after source #2. (i) Opaque handles are only as portable as
the engine that made them: an MCP server off this source is attachable by an openai-agents
`Agent` and nothing else, so a second engine cannot consume these tools without pulling in
openai-agents — the day one does, either it grows its own MCP source or `ToolSet` gains a
declarative shape and agentdeck takes on tool dispatch. (ii) With lifecycle off the port,
every source must expose its own out-of-band `startup`/`shutdown`, which the composition root
wires **by concrete type** — there is no uniform "connect all sources" call, and adding one is
what a `ToolSourceRegistry` would be for.)*

### 4.6 The Runtime service — the use-case layer

One class owns the orchestration that today is smeared across `app.py`, both runner
hierarchies, and `serve.py`:

*(Amended 2026-08-05, as built: `Runtime(engines, store, invocables, sinks=(), clock=_now)` —
the Runtime takes a `Mapping[str, InvocableSpec]` rather than the registry object, and
`control`/`tools` arrive with the stories that build them. `InvocableRegistry` is real as of
issue #73 and is what builds that mapping: `InvocableRegistry(engines).load()` in
`runtime/discovery.py` returns it, so `self.registry.get(name)` in the sketch below is
`self._invocables.get(name)` as built. Two behaviors the
sketch below leaves out: the Runtime **emits `run.started` itself** at `seq` 0, because the
payload's context snapshot is `RunContext` data no engine should be trusted to copy; and it
**closes every run in the log**, on all four exits — a terminal payload ends the read there
(anything an engine yields after one is discarded, so terminal-is-last holds by construction),
an engine that stops on neither a terminal nor a suspending kind gets `run.failed` recorded on
its behalf, an engine that raises gets `run.failed` before the exception is re-raised, and a
consumer that abandons the stream gets `run.cancelled`. A run left open in the log is
indistinguishable from one still in flight, which is the failure all four guard against.
`Runtime.drain()` awaits the sink emits still in flight, for the composition root to call at
shutdown; it is never called per event.)*

*(Amended 2026-08-05, as built: the fan-out is **bounded** — one queue and one consumer task
per sink (`runtime/dispatch.py`), not one task per event. Handing an event over is a queue put;
a full queue yields exactly one loop turn and then drops the stalest event, and the turn is
what separates a sink that is behind from a producer that has simply not suspended yet, since
nothing on the event path has to. That is the only behavior: nothing here ever waits for a
sink, so NFR-6 holds literally rather than by policy. Drops and failed emits are counted per
sink and logged (rate-limited: one stack trace per failure streak), and a sink that fails
`FAILURE_LIMIT` times in a row is disabled rather than retried for the process's lifetime.
Because each sink is fed by a single consumer, `emit` is called one event at a time in
submission order, never re-entered; a consumer killed by a `CancelledError` escaping `emit`
is replaced on the next submit. `Runtime.drain()` flushes the queues and then stops the
consumers, racing each flush against its consumer so one dead consumer cannot hang shutdown
for every other sink.)*

*(Amended 2026-08-05, hardening: every wait on this path is now bounded, because a sink
blocked inside `emit` defeated every exit condition the flush had and hung shutdown outright.
An `emit` that does not return within `EMIT_TIMEOUT` (5s) is abandoned and counted as a
failed emit, so a wedged sink reaches the same breaker a raising one does; the shutdown flush
has its own deadline (`SHUTDOWN_TIMEOUT`, 10s) and gives up rather than waiting. The dispatch's
lifecycle is now explicit: `flush(timeout)` waits for the queued events to be *attempted* and
leaves the dispatch usable, `close(timeout)` is terminal — after it, a submit is counted as a
drop instead of starting a fresh consumer, and events still queued or in flight are added to
the drop count so the counters match the loss reported in the log. A `CancelledError` a sink
raises from its own `emit` is now a counted failure rather than a silently dead consumer; only
a genuine cancellation (`close`, loop shutdown) still ends a consumer, and that path replaces
it on the next submit as before.)*

*(Amended 2026-08-06, #89/#90 — the breaker is no longer one-way, and the failure log is no
longer rate-limited by streak. A disabled sink is offered one event once `BREAKER_COOLDOWN`
(30s) has passed; taking it re-enables the sink, failing it re-arms the cooldown from that
failure, so a dead endpoint costs two emit attempts a minute instead of one per event. The
probe is a real event, not a synthetic one, and the events the open breaker covered are still
counted as drops — nothing is replayed. The cooldown is a deadline compared against a
monotonic clock rather than anything slept on, so no wait on a sink is added anywhere and the
sink's recovery is noticed by whichever submit happens to arrive after it. Stack traces are
capped at one per sink per `LOG_WINDOW` (60s) with the unlogged failures counted in the next
one: the per-streak limit bounded nothing for a sink failing every other event, which builds
no streak and trips no breaker either. The disable decision is untouched by that change.)*

*(A sink with guaranteed delivery is a deliberate non-goal today. A blocking/backpressure
policy was built and then removed before merge: no sink implementation needs it, and the only
ways to keep it were a producer that waits forever or an amendment to NFR-6. Sinks are a lossy
tap; a consumer that must see every event reads the event store. If a real sink ever demands
delivery, it is added on top of this — never by making a run wait.)*

```python
# runtime/service.py
class Runtime:
    def __init__(self, engines: list[EnginePort], store: EventStorePort,
                 control: ControlPort, sinks: list[EventSinkPort],
                 tools: list[ToolSourcePort], registry: InvocableRegistry): ...

    async def run(self, name: str, input: Input, ctx: RunContext) -> AsyncIterator[Event]:
        spec   = self.registry.get(name)
        engine = self._engine_for(spec)                 # OCP: new engines register, nothing changes here
        history = await self.store.read(ctx.session_id, ctx) if ctx.session_id else []
        await self.control.set_status(ctx.run_id, RunStatus.RUNNING)
        async for payload in engine.start(spec, input, history, ctx):
            event = self._stamp(payload, ctx)           # envelope: seq, tenant, ts
            await self.store.append(ctx.session_id, [event], ctx)
            self._fan_out(event)                        # sinks, non-blocking
            yield event
        # terminal status recorded from the terminal event, races absorbed by the state machine

    async def resume(self, name: str, thread_id: str, value: Any,
                     ctx: RunContext) -> AsyncIterator[Event]: ...
    async def signal(self, run_id: str, sig: Signal, ctx: RunContext) -> None: ...
    async def replay(self, session_id: str, ctx: RunContext) -> list[Event]: ...
    async def pending_interrupts(self, ctx: RunContext) -> list[Event]: ...
```

`App` remains the public entry point but shrinks to a **composition root**: it discovers
bundles (the existing `PluginRegistry` convention survives intact), builds the adapter set
from `Settings`, wires the `Runtime`, and re-exposes `run_agent` / `run_workflow` /
`chat` / `chat_stream` as thin compatibility wrappers so existing `.agentdeck/` projects
keep working through the migration.

*(Amended 2026-08-06, #74 — as built, the assembly itself is a separate module,
`agentdeck/composition.py`, and `App` is one caller of it: `build_runtime(engines=...)`
takes the parts and returns the wired Runtime, defaulting the invocable mapping to
discovery and the store to `Settings.events`. `App.load()` calls it and exposes the result
as `App.runtime`. The split is what keeps a second front door — the deferred code-first
`Deck()` — a second caller rather than a rewrite of `App`, and it is why the demo script
and the composition tests no longer hand-assemble `Runtime(...)`.*

*`App` also registers **no event sinks**, which settles the question the telemetry slice deferred
to this one: the Langfuse sink reads the canonical stream, and the v1 bridge still opens v1's own
`trace_run` around every chat turn, so registering both would report each v1 agent run twice.
While v1's runner glue exists, v1's tracing is the one that runs on the v1 surface; the sink
becomes `App`'s once the bridge is deleted, at which point workflow runs get traced for free —
which is the whole reason the sink exists. `agentdeck-serve` therefore ships with no sink, and a
code-first caller that wants one passes it to `build_runtime(sinks=...)`.*

*Two things `App` was expected to hand the Runtime, it does not yet. The engine set is built
by `v1_engines()`, whose langgraph engine keeps its own in-memory checkpointer instead of the
configured one: nothing routes a workflow through the Runtime yet, and resolving the settings
checkpointer eagerly would make the `[durability]` extra mandatory for anyone who only chats.
And the event store defaults to `memory` — the log an operator wants to keep is opt-in via
`AGENTDECK_EVENTS_BACKEND=sqlite`, because no default file path is safe when `.agentdeck/` is
mounted read-only.)*

*(Amended 2026-08-06, #74 — there are now two `EnginePort` implementations under the one
`openai-agents` engine name, and a composition root registers exactly one.
`OpenAIAgentsEngine` (the adapter) builds a minimal `RunConfig` of its own.
`agentdeck/v1bridge/engine.py::V1CompatEngine` resolves the run the way v1's `HeadlessRunner`
does — settings-driven provider, temperature, `max_turns` and CA bundle, v1's sandbox scope,
v1's Langfuse observation, and v1's own session lookup — so a turn served through the v1
surface is configured and traced exactly as before, and it is what `v1_engines()` registers.*

*It lives in a new top-level `compat/` rather than beside the adapter it subclasses, and the
import law is why: it reaches into v1's runner glue and v1's Langfuse integration, and an
engine adapter may do neither (`langfuse-is-telemetry-private` fails on the indirect chain
through `agents/runners/base.py`). Keeping the bridge outside `adapters/` is what keeps the
adapter honest about depending on one external system. A contract
(`v1bridge-is-composition-root-only`) now forbids every ring from importing it, so the
pre-stable cleanup deletes one directory rather than unpicking dependencies. The engine's own
override seams — `_session`, `_launch`, `_translate`, `_terminal`, and the `Launch` handle
whose `finished` flag is the only trustworthy "this run reached its terminal event" signal —
are the adapter's public-to-subclasses surface.)*

---

## 5. Ring 2 — adapters

**`engines/openai_agents/`** is the only place in the codebase that imports `agents`.
Today's `HeadlessRunner.run_streamed` yields `str | StreamDone`; its successor walks the
same SDK stream items (the helpers in `runtime/events.py` already do the extraction) and
yields typed payloads: `text.delta`, `tool.call.*`, `message.completed`, then
`run.completed` with usage. It checks `ctx.gate` between stream items and before each tool
dispatch. Session history *(amended per ADR-D5)*: the adapter owns SDK-native session
state as its private execution store — the relocated `SessionFactory` — so the model
receives its exact prior items (reasoning items, paired tool-call IDs) rather than a
reconstruction; the event log remains the platform-facing record, and the two are tied by
the transcript-fidelity invariant (everything entering execution state is recorded in the
log at message level; log written first, engine state second). The Chat-Completions
shims currently living inside
`capabilities/compaction.py` and `capabilities/filesystem.py` move here too: they are
engine quirks, and quirks belong inside the adapter that owns them.

**`engines/langgraph/`** absorbs `workflows/` nearly whole — `base.py` graph compilation,
`nodes.py`, `state.py`, `interrupts.py`, `timers.py`, and `runtime/checkpointer.py`. Its
mapping is thin because the shapes already correspond: `astream` updates →
`node.updated`, LangGraph `interrupt` → `run.interrupted` (reason `"human"`),
checkpointer `thread_id` → the resume token. Durable timers (`wake_at_of`) surface as a
scheduled `resume` — which is why `resume` lives on `Runtime`, not on the serve layer.

**`stores/`** implements `EventStorePort` over the **event log** — new code, not a port
of `SessionFactory` (which relocates into the openai-agents adapter as its execution
store, per ADR-D5): `memory` and `sqlite` for
dev, `redis` and `postgres` for deployment, all implementing `EventStorePort` over the
event log. **`control/`**: `memory` for dev; `redis` for anything multi-worker — Redis is
what makes "pause from another process" possible at all. **`tools/mcp/`** is today's
`agents/mcp/` (lifecycle, transport, wiring) re-homed behind `ToolSourcePort`; its
lifecycle hooks into `App.open()` exactly as now. *(Amended 2026-08-05, #78 — as built, this
adapter is the one exception to "`engines/openai_agents/` is the only place that imports
`agents`": an MCP server has to be an SDK `MCPServer` to be attachable to an SDK agent, and
the client hardening lives here rather than being duplicated per engine. The import law it is
held to instead: nothing outside `adapters/tools/mcp/` may import the MCP SDK, and this adapter
imports no other engine's SDK — both linter-enforced. v1's `agentdeck.agents.mcp` path
re-exports the moved names until the pre-stable cleanup drops the shim.)* **`caps/sandbox/`** wraps the existing
`Workspace`/`SandboxSession` machinery as the default `FilesystemPort` + `TerminalPort`.
**`telemetry/langfuse/`** is `runtime/observability.py` rebuilt as an `EventSinkPort` —
note the direction reversal: instrumentation stops hooking the SDK and starts reading the
log, which is why it will cover workflows too, for free.
*(Amended 2026-08-06, #77 — as built, this is new code beside v1's module rather than a
rebuild of it: `runtime/observability.py` still instruments the SDK for v1 runs and is
untouched, so a v1 agent run traced by both would be reported twice; unifying them is the
facade slice's call. Three details the design didn't fix. The trace id is derived from
`run_id`, not minted by the SDK, so the sink can never nest a run under whatever span
happens to be current on its consumer task and a run resumed in another process reopens the
same trace. A suspended run closes its root observation and its resume opens a second root
under that same trace id, because a span held open until a human answers is a trace nobody
can see while it waits. And the run's token total from `run.completed` becomes a
`run.usage` generation, since Langfuse accounts usage on generations only. Buffering and
delivery are the Langfuse SDK's batching processor. Like `tools/mcp/`, the adapter reads
`runtime.settings` for its own config group.)*
*(Amended 2026-08-06, #99 — that buffer no longer depends on the SDK's exit hook:
`EventSinkPort` grew an optional `close()`, called once per sink by `SinkDispatch.close()`
after the backlog is handed over and the consumer reaped, and the Langfuse sink uses it to
finish the traces still open — an unfinished observation is never shipped — and flush the SDK
itself. Bounded by `CLOSE_TIMEOUT` and non-fatal, like every other wait on that path, so the
worst a sink's flush can cost a shutdown is one more deadline. A disabled sink is closed too:
the breaker's verdict is about taking events, not about writing out the ones already taken.
The event log stays the complete record regardless.)*
**`protocols/`** hosts the
per-protocol serializers: `sse` (extracted from `serve.py`), `acp` (§11), later `ag-ui`
and `a2a`.

---

## 6. Ring 3 — surfaces

`surfaces/serve/` keeps the existing HTTP contract — `/health`, `/agents/{name}/chat`
(+`?stream=true` SSE), `/workflows/{name}` (+stream, +`thread_id`), `/pending`,
`/resume` — but every handler becomes the same four lines: build `RunContext`, call
`Runtime`, hand the event iterator to the `sse` protocol adapter, return. The
`_jsonable`/frame-shaping logic in today's `serve.py` disappears into `protocols/sse/`.
New routes: `POST /runs/{id}/pause|resume|cancel`, `GET /runs/{id}`. `surfaces/acp/` is a
stdio entrypoint (§11), a sibling of `agentdeck-serve` and roughly the same size.

*(Amended 2026-08-06, #74 — as built, the chat handlers are those four lines and the workflow
handlers are not. `POST /agents/{name}/chat` (streamed and not) builds a `RunContext`, calls
`Runtime.run`, and hands the events to `surfaces/serve/compat.py`, which renders v1's `delta`
/ `done` / `error` frames; the golden suite passes unchanged, so the wire is byte-identical.
The frame shaping lives in that surface module rather than in `protocols/sse/`, because what
it encodes is v1's frame vocabulary, not SSE — a generic SSE adapter is worth extracting when
a second surface needs the same frames, not before.*

*The workflow endpoints still call v1's workflow runner, and the reason is the engine, not the
surface: v1 posts an arbitrary JSON state and gets the final state back, while the langgraph
adapter takes `Input` content blocks (text only, mapped to `{"input": text}`) and reports the
final state as `str(dict)` on `run.completed`. Rerouting `/workflows/*` byte-identically
therefore needs a state-shaped input channel and a structured final state in the adapter
first — attempting it here would have meant either changing a golden or regressing every
existing workflow client. Same root cause as the one thing the chat reroute could not carry
canonically: `RunCompleted.output` is `Input`, so an `output_type` agent's validated model
rides alongside as one namespaced `custom` event (`openai_agents.structured_output`) that the
surface renders. Two recurrences of "core has no shape for structured data" is the promotion
signal D10 describes; a `DataBlock` (or a structured field on `run.completed`) is the schema
decision that would retire both the custom event and this note.)*
`surfaces/cli/` gains `agentdeck run`, `agentdeck sessions replay`, `agentdeck runs
signal` for free, because they too are event readers.

### Target layout

```text
agentdeck/
├── core/                    # Ring 1 — pydantic + stdlib ONLY (enforced by import-linter in CI)
│   ├── events.py  content.py  context.py  invocable.py  status.py  errors.py
│   └── ports/     engine.py  store.py  sink.py  control.py  sandbox.py  tools.py  policy.py  secrets.py
├── runtime/                 # use cases + discovery
│   ├── service.py           # Runtime
│   ├── discovery.py         # InvocableRegistry, over the same bundle conventions
│   └── settings.py          # layered settings, now also selects adapters
├── authoring/               # the user-facing declarative API
│   ├── agent.py             # BaseAgent / BaseSandboxAgent  → compile to InvocableSpec
│   ├── workflow.py          # BaseWorkflow                  → compile to InvocableSpec
│   └── capabilities.py      # CapabilitiesSpec → CapabilityRequest
├── adapters/
│   ├── engines/{openai_agents,langgraph}/
│   ├── stores/{memory,sqlite,redis,postgres}/
│   ├── control/{memory,redis}/
│   ├── tools/{mcp,functions}/
│   ├── caps/{sandbox}/
│   ├── protocols/{sse,acp}/
│   └── telemetry/{langfuse}/
├── skills/                  # bundles/executor stay; executor consumes FilesystemPort/TerminalPort
├── surfaces/{serve,acp,cli}/
└── app.py                   # composition root + backward-compat facade
```

The user-visible `.agentdeck/` project convention — `agents/<bundle>/agent.py`,
`workflows/<bundle>/workflow.py`, `skills/*/SKILL.md`, no `__init__.py`, no
registration — **does not change at all**.

*(Amended 2026-08-05, as built: `runtime/discovery.py` is the `InvocableRegistry` — it calls
v1's `PluginRegistry` (still in `runtime/registry.py`, along with the project-dir mount both
now share) rather than replacing it, because v1's `App` keeps using it until `App` itself
becomes the composition root. The migration-map row below therefore lands in two steps: the
`InvocableRegistry` first, the generic scanner's rename with the rest of v1. Since a spec's
`native` is engine-built — an `agents.Agent`, an uncompiled `StateGraph` — the registry
reaches through the v1 bundle classes that build them, which is why it sits at the
composition layer's edge and not inside the Runtime's own import fence. Skills are not
discovered: no engine plays a `SKILL.md` bundle, so a `SKILL` spec could only fail when run.)*

---

## 7. Worked example 1 — a chat message, end to end

```text
WhatsApp POST /agents/Greeter/chat {session_id:"wa-123", message:"hi"}
  │
  ▼  surface: parse → Input=[TextBlock("hi")], build RunContext(tenant, run_id, gate, caps=sandbox defaults)
Runtime.run("Greeter", input, ctx)
  │   registry → InvocableSpec(engine="openai-agents")
  │   store.read("wa-123")            → prior events (the record); engine loads its own SDK session (execution state, ADR-D5)
  │   control.set_status(RUNNING)
  ▼
engines/openai_agents.start(...)      # the only file importing `agents`
  │   SDK stream item ──translate──▶  text.delta / tool.call.started / tool.call.completed / …
  ▼
Runtime stamps envelope (seq, tenant, ts) → store.append → fan out to sinks → yield
  │
  ├─▶ surface: text.delta → WhatsApp message chunks;  run.completed → done
  ├─▶ telemetry/langfuse sink → trace
  ├─▶ cost sink: usage from run.completed → budget ledger
  └─▶ store: the log itself = audit + replay + session/load, no extra code
```

Seven features, one stream, each consumer under ~80 lines. Swap `Greeter` for a LangGraph
workflow and every consumer above works unchanged — the stream just also contains
`node.updated` events, which chat surfaces ignore by rule (§4.2).

---

## 8. Worked example 2 — pause / resume / cancel

Ring 1 additions: `run_id` addressable from outside (already a noun), three event kinds
(`run.paused`, `run.resumed`, `run.cancelled`), the status machine of §4.4, `ControlPort`,
and the engine-facing `Gate`:

```python
class Gate(Protocol):
    async def checkpoint(self) -> None:
        """Returns immediately; blocks while PAUSED; raises RunCancelled on CANCEL.
        Engines call this at every safe point."""
```

Ring 2 is where the real work is, isolated per engine. LangGraph: nearly free — pause maps
onto `interrupt` at the next node boundary, the checkpointer already provides durable
resume, cancel is a graph abort; the human-in-the-loop machinery generalizes. OpenAI
Agents SDK: the actual work — the adapter inserts `await ctx.gate.checkpoint()` between
stream items and before each tool call. Ring 3: four thin routes; and because pause/cancel
are just three more event kinds, the CLI, the dashboard, and every protocol adapter
inherit the feature with zero changes — that is the payoff of §2's "one log, many readers."

Four semantic contracts, decided here rather than discovered in production. *Safe points:*
"pause" means "at the next safe point" — between stream items, before a tool call, at a
node boundary — never "right now, mid-token." This is a documented contract, uniform
across engines. *Resume without a stack:* the Agents SDK has no checkpoint; the event log
**is** the checkpoint, and resume means re-entering the loop with accumulated history —
the same rule the README already states for interrupted LangGraph nodes ("keep the node
pure, side effects earlier"), now promoted from per-engine footnote to platform-wide
invariant. *Cancel is cooperative:* a tool that already POSTed a payment does not un-POST;
`ctx.idempotency_key` exists so retried or resumed side effects deduplicate at the
receiver. *Races are no-ops:* pausing a completed run does nothing, by state-machine rule.

Note what the example shows: every hard part was semantics, none was wiring. In the
current tree this feature would touch `agents/runners/`, `workflows/runners/`, `serve.py`,
and both registries — and agents and workflows would end up with subtly different pause
behavior that leaks into every consumer.

*(Amended 2026-08-05, #54 — as built, M0's slice of this example is cancel only: `Signal`
has one member (`CANCEL`); pause/resume/steering stay Story 3. `Gate` is a concrete class,
not a `Protocol` — with no `ControlPort` behind it, `checkpoint()` is a no-op, so
`RunContext.gate` defaults to one and every existing caller is unaffected. That default is
also why callers never construct a working `Gate` themselves: `Runtime.run`/`resume` rebind
`ctx.gate` to the `Runtime`'s own `ControlPort` before an engine ever sees it, which is what
keeps `surfaces/serve/app.py` and `surfaces/cli/chat.py` untouched by this feature. `ControlPort`
itself is narrower than sketched above: `signal(run_id, sig)` / `poll(run_id)` only — no
`ctx`, no `status`/`set_status`, because status is derived from the event log
(`core/status.py`) once, never duplicated in a second store. One finding worth recording:
`httpx.ASGITransport` runs a request's whole ASGI call before returning any response bytes,
so it cannot interleave a live signal with an in-flight SSE stream — the cross-process proof
(`tests/test_uc3_slowpoke.py`) drives `Runtime` directly and a real `subprocess` for the CLI
instead of routing through that transport.)*

*(Amended 2026-08-08 — the `core/ports/control.py` sketch above splits in two as built.
`ControlPort`, the transport an outer ring implements, is the only thing left in
`core/ports/control.py`; `Signal`, `ControlSignal`, `Gate`, `ControlSignalled` and
`CONTROL_POLL_INTERVAL` are in `core/control.py`, one ring in. None of them was ever
implemented by an outer ring — `Gate` is core's own concrete class and `ControlSignalled`
mints event payloads, which is core policy — and `RunContext` needs `Gate` as a field type,
so core was importing from its own ports package to build a value object. `core/control.py`
and `core/reporting.py` are now visibly the pair the latter's docstring already claimed they
were: control in on `RunContext`, updates out the same way.)*

*(Amended 2026-08-06, #45 and #85 — pause, resume and cancel as built. Four departures from
the sketch above, all deliberate.)*

**`Gate.checkpoint()` never blocks.** The sketch said "blocks while PAUSED"; it does not. A
paused run raises out of the gate, unwinds to the Runtime, records `run.paused` and lets the
process go, because a pause held in a parked coroutine dies with the worker and cannot be lifted
by any other one. §4.4's `PAUSED` is therefore a suspended status like `WAITING_HUMAN`, and
`can_resume` admits both: one conditional append (`run.resumed`) serves both transitions, which
is also what keeps two racing resumes from playing a turn twice. Resume then re-enters the engine
with the run's own `run.started` input and the log as history — "resume without a stack" applied
to an operator's pause exactly as this section applies it to an interrupt, replay cost included.

**The run's own loop records `control.requested`, not the caller that asked.** §4.2 says the
request is in the log; what it cannot be is *written* by the requester, because only the run's
owner may assign that run's `seq`, and a caller holding a `run_id` from a stream it was watching
has neither the log key nor the tenant to write with. So the reason travels on the `ControlPort`
and the request is recorded at the safe point where the run finds it, one append before
`control.observed`. The ceiling this leaves: the log's own gap between "asked" and "acted" is the
poll interval, not the true wait, so a run that sat inside a 30-second tool call shows the delay
as silence before `control.requested` rather than as a gap after it. Upgrade trigger — an
operator needing "asked but still inside a tool" visible in the log — is a Runtime-owned watcher
task appending off the run path, which needs a per-run append lock and a merge into the streamed
generator; neither is needed for anything shipped here.

**Control reads are bounded by time (#85).** The gate polls at most once per
`CONTROL_POLL_INTERVAL` (200ms), reusing the answer in between, and always reads at a run's first
safe point so a signal that beat the run out of the gate is honored at once. Cancel still lands
*at* a safe point — this changes when the gate learns of a signal, never where it acts. Measured
either way, at a real model's pace (~30ms per chunk): a 400-chunk answer costs 400 control reads
per-safe-point and 58 under the bound. The resulting user-facing latency bound (one interval,
plus the time the current step needs to reach a safe point) is stated on the docs-site
run-control page, which is also where the safe-point contract now lives.

**`tool_dispatch` and `node_boundary` are declared and unproduced.** The openai-agents adapter
checkpoints between stream items only: the SDK dispatches a tool inside its own loop, so a
checkpoint the adapter can reach is never honestly "before dispatch". A pause during a tool call
therefore lands after that call returns — which is the documented behavior either way, and the
`safe_point` field says which boundary was used. The langgraph adapter makes no checkpoint at
all, so a workflow run has no safe point yet (#128, with the resume semantics that gap needs).

---

## 9. Worked example 3 — supporting ACP

ACP: JSON-RPC 2.0 over stdio, editor spawns the agent as a subprocess; baseline methods
`session/new`, `session/prompt`, `session/cancel`, streaming `session/update`; and — the
interesting part — **bidirectional**: the agent calls the client (`fs/read_text_file`,
permission requests) and awaits answers, because the editor owns unsaved buffers and
permission policy.

What the architecture gives away for free:

| ACP | Maps to | Cost |
|---|---|---|
| `session/new` / `session/load` | `Session` + `Runtime.replay` of the event log | free |
| `session/prompt` (content blocks) | `Runtime.run(name, Input, ctx)` | free — §4.1 chose blocks for this |
| `session/update` stream | event payloads reserialized per kind | ~1 mapping table |
| `session/cancel` | `Runtime.signal(run_id, CANCEL)` | free — §8 built it |
| `session/request_permission` | `ApprovalPort` → `run.interrupted(reason="approval")` | free |
| per-session MCP servers | existing `tools/mcp` wiring | free |

The one genuine gap — and the one Ring-1 change: everything so far pushes events
*outward*, but ACP needs the run to make requests *back to its caller* and use the
**editor's** filesystem, not the sandbox. Hence §4.5's caller-injected
`CapabilityProvider`. The ACP surface supplies a `FilesystemPort` whose `read_text` sends
`fs/read_text_file` down the JSON-RPC pipe and awaits the reply, and an `ApprovalPort`
that issues `session/request_permission`. Same port, same agent code, different backing;
HTTP callers keep the sandbox implementation. This one generalization is also what later
unlocks AG-UI and A2UI — the same shape, a caller offering surfaces the run can drive.

Concretely: `adapters/protocols/acp/` holds JSON-RPC framing, method dispatch, the
event→`session/update` mapper (the single churn-absorbing file, with the protocol version
pinned — parts of the spec are still marked unstable), and the client-backed capability
ports. `surfaces/acp/` is the `agentdeck acp` stdio entrypoint. Capability negotiation:
what the client declares at `initialize` decides which ports go into `ctx.caps`, with
sandbox fallback; the agent's advertised capabilities are *derived* from what is
registered, never hardcoded. ACP is point-to-point, so this surface runs single-tenant —
but `RunContext.tenant` is populated anyway; the core is never forked for a surface.

Scoreboard: one core field (`caps`), one adapter directory, one entrypoint. Zero changes
to engines, serve, telemetry, or any existing consumer.

---

## 10. SOLID scorecard

| Principle | Where it shows up | Current violation it fixes |
|---|---|---|
| **S**ingle Responsibility | Each adapter has one reason to change: SDK upgrade → `engines/openai_agents` only; ACP spec churn → one mapper file | `serve.py` changes for SDK, LangGraph, *and* SSE reasons; `app.py` is discovery + sessions + MCP + two runner APIs |
| **O**pen/Closed | New engine, protocol, or store = new adapter package + registration; `Runtime`, core, surfaces untouched. Event consumers ignore unknown kinds, so new kinds don't break old readers | Adding any engine today means a third parallel silo with its own registry and runner |
| **L**iskov Substitution | Engines are substitutable behind `EnginePort` — enforced by a **shared contract-test suite** parametrized over every engine: event ordering, exactly-one-terminal-event, interrupt/resume round-trip, cancel semantics, gate honored | Agents and workflows return different shapes (`str \| StreamDone` vs state dicts), so nothing downstream can treat them uniformly |
| **I**nterface Segregation | Many small ports; a cost sink depends on `EventSinkPort` alone; the skill executor sees only `FilesystemPort` + `TerminalPort`, not the workspace | `Workspace` is ambient via ContextVar — everything can reach everything |
| **D**ependency Inversion | Core defines ports; adapters implement them; `import-linter` in CI forbids `core/` importing `agents`, `langgraph`, `fastapi`, `redis` | `app.py` imports `agents.SQLiteSession`; capabilities import SDK internals to shim them |

The contract-test suite deserves emphasis: it is LSP made executable, and it is the
artifact that makes "add a third engine next year" a safe claim rather than a hope.

---

## 11. Migration from the current tree

| Current module | Destination | Change |
|---|---|---|
| `agents/runners/headless.py` | `adapters/engines/openai_agents/runner.py` | emits event payloads instead of `str \| StreamDone`; gains gate checks |
| `runtime/events.py` (SDK stream helpers) | same adapter | unchanged logic, new output types |
| `agents/base.py`, `agents/capabilities/spec.py` | `authoring/` | user-facing API frozen; compiles to `InvocableSpec` + `CapabilityRequest` |
| `agents/capabilities/{shell,filesystem}.py` | `adapters/caps/sandbox/` | become `FilesystemPort`/`TerminalPort` impls; SDK shims move into the engine adapter |
| `agents/mcp/*` | `adapters/tools/mcp/` | behind `ToolSourcePort`; lifecycle unchanged *(amended 2026-08-05, #78: moved, plus `source.py` implementing the port; v1's path re-exports until the shim is dropped, and no engine consumes the port yet)* |
| `workflows/*` (base, nodes, state, interrupts, timers, runners) | `adapters/engines/langgraph/` | interrupts map to `run.interrupted`; timers schedule `Runtime.resume` |
| `runtime/checkpointer.py` | `adapters/engines/langgraph/` | engine-private |
| `runtime/sessions.py` (`SessionFactory`) | `adapters/engines/openai_agents/sessions.py` | kept as the engine's execution store (ADR-D5); event-log stores in `adapters/stores/` are new code |
| `runtime/observability.py` | `adapters/telemetry/langfuse/` | becomes an `EventSinkPort`; stops instrumenting the SDK directly *(amended 2026-08-06, #77: the sink exists as new code and covers agent and workflow runs alike; v1's module has not moved and still instruments the SDK for v1 runs)* |
| `runtime/registry.py` (`PluginRegistry`) | `runtime/discovery.py` | conventions untouched *(amended 2026-08-05: `discovery.py` exists now as the `InvocableRegistry` calling into `PluginRegistry`; the scanner itself moves when v1's `App` does)* |
| `serve.py` | `surfaces/serve/` + `adapters/protocols/sse/` | handlers become Runtime calls; SSE wire format preserved |
| `errors.py`, `skills/*` | `core/errors.py`; `skills/` (executor re-targeted to ports) | mechanical |
| `app.py` | `app.py` (composition root) | public API preserved as facade |

**Phases**, each shippable and each keeping `make test` green:

*Phase 1 — nouns.* Introduce `core/` (events, context, content, status, ports as ABCs
with in-memory impls) plus the contract-test suite skeleton. Thread `RunContext` through
every existing call with a default single-tenant context. No behavior change; the
cheapest phase and the one that makes every later phase possible.

*Phase 2 — the seam.* Convert both runners to emit event payloads behind `EnginePort`;
build `Runtime`; move code into `adapters/`; shrink `App` to composition root with the
compat facade; rewrite `serve.py` on `Runtime` while byte-preserving the SSE frames.
This is the big one — the bifurcation dies here.

*Phase 3 — control.* `ControlPort` (memory + redis), `Gate` checks in both engines, the
status machine, four new routes. First visible new feature, and the proof that features
now cost one implementation instead of two.

*Phase 4 — capabilities.* `CapabilityProvider` on the context; sandbox becomes the
default implementation; skill executor re-targeted to the ports.

*Phase 5 — ACP.* The protocol adapter and stdio surface, per §9. First external proof of
"speak every protocol."

---

## 12. Decisions and refusals (recorded)

**D1** Unify engines at the lifecycle/event boundary, not the programming model — each
engine stays idiomatic; only observable behavior is standardized. **D2** Python authoring
by convention; no config DSL — a DSL is a large surface that reimplements Python badly.
**D3** `Input` is content blocks from day one — ACP/A2A/multimodal all require it and it
is brutal to retrofit. **D4** Capabilities are caller-injected ports; the sandbox is one
implementation, not the definition. **D5 *(revised — see `adr-d5-two-stores.md`)*** Two stores, both authoritative in their
own jurisdiction: the event log is the platform-facing *record* (the only thing
consumers read); engine-native session state (SDK session, LangGraph checkpointer) is
each loop's private *working memory*, owned by its adapter. Tied by the
transcript-fidelity invariant; log written first; rebuild-from-log is disaster recovery
only. **D6** Cancellation is cooperative with documented
safe points; resume re-enters with history (no stack restore) — uniform across engines.
**D7** `RunContext` is threaded everywhere immediately, even single-tenant. **D8** Events
are versioned; unknown kinds are ignored by every consumer; breaking changes bump `v`.
**D9** The envelope is closed at eight fields; new needs go into payloads or
`run.started`. **D10** Event kinds are minted only in core; engines translate into
existing kinds or use namespaced `custom`; recurring `custom` usage is a promotion
signal, not a precedent.

**Refused, deliberately:** auth in the core; a marketplace; a dashboard before the event
schema stabilizes; "control plane" positioning while there is no hosted offering; a third
registry for anything — there is exactly one `InvocableRegistry`.

**Open questions** for the first design review: whether `usage.reported` should be
per-model-call or aggregated at `run.completed` only (affects budget enforcement
granularity); whether `node.updated` state patches should be JSON-Patch or shallow merge
(affects dashboard diffing); and where skill executions sit — as `InvocableKind.SKILL`
runs with their own `run_id` (auditable, my recommendation) or as tool calls inside the
parent run.

---

## Appendix A — Prebuilt content: the standard library

### A.1 Where it lives

Prebuilt tools, agents, workflows, and skills are **content, not architecture**. They do
not get a ring, privileged internal APIs, or a fourth registry. They form a standard
library that sits on top of the authoring API exactly as user code does — the Python
stdlib model: shipped in the box, held to a higher quality bar, obeying the same rules
as everyone else.

```text
agentdeck/
├── stdlib/
│   ├── tools/        # engine-neutral function tools → ToolSpec
│   ├── agents/       # bundles in the SAME format users author
│   ├── workflows/
│   └── skills/       # SKILL.md bundles, identical to user skills
└── templates/        # scaffolding: copied by `agentdeck new --template`, then user-owned
```

Three rules make the stdlib work, and each is enforceable rather than aspirational.

**Rule 1 — stdlib depends only on `authoring/` + core ports, never on adapter
internals.** This is what guarantees a stdlib agent runs on any engine/store/protocol
combination, and it is checked by the same import-linter contract that protects `core/`.
Where a piece of content is genuinely engine-specific, it declares that honestly instead
of hiding it: the current `agents/web_search.py` wraps an OpenAI-hosted tool, so it moves
to `stdlib/tools/web_search.py` with `engines=["openai-agents"]` on its `ToolSpec`, and
the registry refuses to attach it to a LangGraph run with a clear build-time error rather
than a runtime surprise. A second implementation of the *same spec name* — a plain HTTP
search tool that works everywhere — can be added later and resolved per engine at build
time; that is DIP applied to tools.

**Rule 2 — stdlib bundles ship in the user bundle format and are found by the same
discovery.** No special internal loading path: the platform dogfoods its own authoring
API, which keeps that API honest. The only extension required is teaching
`runtime/discovery.py` to read Python entry points (`agentdeck.bundles`) in addition to
the `.agentdeck/` directory. That one change is load-bearing far beyond the stdlib: it
means *any* pip package can ship agents, skills, and tools — `agentdeck-contrib-*`
packages, internal company packages, third parties — which implements the diagram's
entire Plugins / Marketplace / Ecosystem row as ordinary Python packaging, with no
marketplace infrastructure built or operated.

**Rule 3 — stdlib and templates are different things with different lifecycles.** Stdlib
content is referenced at runtime and updated with the package; templates are copied into
the user's project by the CLI scaffold and owned by the user from that moment on. The
sorting test: a `fetch_url` tool is stdlib (users call it as-is, forever); a
customer-support agent is a template (users will gut it on day two). Getting this wrong
in either direction hurts — stdlib that users need to fork rots in their trees, and
templates referenced at runtime break user projects on every upgrade.

### A.2 The user experience this buys

Day one, zero code, a tested platform agent:

```bash
pip install agentdeck[toolkit]
agentdeck run Summarizer "summarize this file..."
```

Because a stdlib agent is an ordinary bundle, everything users can do with their own
agents works on platform agents with no additional machinery — chat with them over HTTP,
use them as workflow nodes via the node factories, or hand off to them:

```python
class SupportBot(BaseAgent):
    handoffs = [stdlib.agents.Researcher]      # platform-built agent as a peer
```

Customization has two deliberate gradients. Lightweight — extend in place, tracking
upstream improvements:

```python
from agentdeck.stdlib.agents import Researcher

class MyResearcher(Researcher):
    instructions = Researcher.instructions + "\nAlways answer in Hebrew."
    tools = [*Researcher.tools, my_internal_search]
```

Heavyweight — take ownership via scaffold:

```bash
agentdeck new my-support --template customer-support
```

which copies a full working starting point (agent, skills, workflow, evals) into the
user's `.agentdeck/`. None of this requires stdlib-specific features; it all falls out of
Rule 2. Had the stdlib been built on privileged internal hooks instead, users could run
platform agents but never crack them open.

### A.3 Placement decisions for existing and planned content

**`BaseSandboxAgent` dissolves.** The class split exists only because the underlying SDK
distinguishes `Agent` from `SandboxAgent` — an engine detail leaking into the authoring
API. In the target design there is one `BaseAgent`; declaring
`capabilities = CapabilitiesSpec(shell=True, ...)` is what makes an agent sandboxed. The
openai-agents engine adapter inspects the compiled `CapabilityRequest` and decides
internally which SDK class to build, and the sandbox itself is simply the default
`FilesystemPort`/`TerminalPort` implementation in `adapters/caps/sandbox/`.
`BaseSandboxAgent` survives as a deprecated alias so existing bundles keep working.

**Reusable workflow components split the work from the wiring.** A bare LangGraph node in
the stdlib would be a reusable component usable in exactly one engine, defeating the
point. The *work* — e.g. parsing a document — ships as an engine-neutral unit: a skill
(`stdlib/skills/parse-doc/`, a deterministic script running against `FilesystemPort`) or
a function tool. The *node-ness* is provided by generic factories owned by the LangGraph
adapter — `skill_node("parse-doc")`, `agent_node("Summarizer")`, `tool_node(...)` — thin
wrappers adapting any invocable into a graph node (today's `workflows/nodes.py` already
gestures at this and becomes that adapter's official surface). Only genuinely
graph-specific glue — routing logic, state mapping — belongs inside a workflow bundle
itself. The rule of thumb: ask *what is the reusable part?* If it is the work, it goes in
the stdlib, engine-neutral; if it is the wiring, it is a factory in the engine adapter.
The payoff is concrete: one `parse-doc` implementation is simultaneously a workflow node,
an agent capability, and a standalone `Runtime.run("parse-doc", ...)`.

### A.4 The quality bar and distribution

"Prebuilt" is a claim, and it is what separates a standard library from example code, so
it is enforced in CI: every stdlib item must pass the shared contract-test suite (§10)
and ship with its own evals in the repo. An agent without evals does not enter
`stdlib/agents/`; it lives in `templates/` or `examples/` until it earns promotion.

Distribution keeps the core lean: the base install contains no stdlib content beyond
what core functionality needs; `pip install agentdeck[toolkit]` pulls the batteries; and
heavier or fast-moving bundles live as separate `agentdeck-contrib-*` packages discovered
through the same entry points. Versioning follows the package — stdlib content is
released, changelogged, and deprecated exactly like API surface, because per Rule 2 it
*is* API surface.

---

## Appendix B — The platform from the user's perspective

Everything above is architecture. This appendix is the same design seen from the other
side of the API: what a user actually does, and why each moment of that experience is
only possible because of a specific decision in §§2–9. It doubles as the product pitch,
and every claim in it is load-bearing — if a future change breaks one of these examples,
the change is wrong.

The one-line premise: **the user writes the agent; the platform provides everything
around the agent** — sessions, surfaces, control, governance, observability. Engines
compete on making one loop smarter; they structurally cannot own what happens *between*
loops, *across* engines, *over* weekends, and *between* companies. That territory is the
platform's, and all of it is the one event log wearing different hats.

### B.1 A day in the life

**Twenty lines, three surfaces.** The user drops one file into
`.agentdeck/agents/support/agent.py`:

```python
class SupportBot(BaseAgent):
    instructions = "Help users with flytrucks shipments."
    tools = [lookup_shipment]
    capabilities = CapabilitiesSpec(filesystem=True)
```

No registration, no server code. Then:

```bash
agentdeck run SupportBot "where is order 4412?"   # terminal
agentdeck serve                                    # HTTP + streaming chat API
agentdeck acp                                      # the same agent inside an editor
```

Three integrations that would each be hand-written elsewhere are the same class here,
because surfaces render events and never contain logic (§3, §6).

**Memory without plumbing.** A customer chats on WhatsApp on Monday and emails on
Wednesday; both hit `session_id="cust-881"` and the agent remembers exactly — including
tool results — because the caller passed an ID and the store did the rest (§4.1, ADR-D5).

**The Friday-evening approval.** A refund workflow reaches "approve payouts over ₪500"
at 17:40 and stops with `run.interrupted`. Nothing runs over the weekend — no process, no
open connection. On Monday someone taps Approve in the pending inbox and the run resumes
precisely where it stopped, across two server restarts, courtesy of the checkpoint +
interrupt machinery (§4.4, §8).

**The runaway agent.** An agent loops on a tool, burning tokens. From any terminal:
`agentdeck runs signal r_7f3a cancel` — it stops at the next safe point and the log shows
exactly what it did (§8). `Budget(max_usd=2)` on the context prevents the runaway in the
first place (§4.3).

**Observability nobody wrote.** Every run is automatically a trace, a cost line, and a
replayable transcript, because those are three sink readers of the same log (§7). "It
gave a weird answer yesterday" becomes replaying that session event by event instead of
guessing.

**The bet against lock-in.** A better engine ships next year; the user changes `engine=`
on one invocable, and sessions, pause, ACP, tracing, and the HTTP API keep working —
none of them ever knew which engine ran (§2, D1). This is the deepest "why": agent SDKs
churn yearly, but the user's accumulated assets — conversations, tools, skills,
integrations, dashboards — should not churn with them. AgentDeck is where the durable
parts live.

### B.2 Full systems from simple definitions

Because agents, workflows, skills, and remote agents are all `Invocable` (§4.1),
everything composes with everything. One worked system — shipment claims:

```python
class ClaimsAgent(BaseAgent):
    tools = [lookup_shipment, stdlib.tools.fetch_url]     # own function + stdlib
    mcp_servers = [MCPServer.stdio("uvx", ["jira-mcp"])]  # a whole toolset, one line

class FrontDesk(BaseAgent):                               # triage in four lines
    instructions = "Route: damage claims → ClaimsAgent, delays → TrackingAgent."
    handoffs = [ClaimsAgent, TrackingAgent]

class ClaimPipeline(BaseWorkflow):                        # the governed spine
    nodes = {
        "assess":  agent_node(ClaimsAgent),               # LLM judgment
        "report":  skill_node("damage-report"),           # deterministic, no LLM
        "approve": approval_node("Payout > ₪500"),        # durable human gate
        "payout":  tool_node(issue_refund),               # idempotency key from ctx
    }
    edges = [("assess", "report"), ("report", "approve"), ("approve", "payout")]
```

plus a `.agentdeck/skills/damage-report/` folder, and:

```bash
agentdeck serve        # web app chats with FrontDesk over SSE
agentdeck acp          # ops runs ClaimsAgent inside their editor
```

Total user-written code: two agent classes, one workflow class, one skill folder, two
functions — roughly eighty lines. Provided without writing: routing, streaming chat,
durable approval that survives restarts, pause/cancel on any run, per-run cost, audit,
replay, a Jira integration, an editor integration.

The compositional property to protect: the arrows point *any* direction. The workflow
calls agents as nodes; an agent can trigger the workflow as a tool ("file this as a
formal claim"); the skill runs standalone via `Runtime.run("parse-doc", ...)`; a remote
agent slots into `handoffs` like a local class. No adapters between the user's own
pieces.

An honest caveat belongs in the pitch: the LLM parts still need evals and iteration. The
platform makes the *system* cheap, not the prompt engineering — which is what the stdlib
quality bar (A.4) and the eval harness exist for.

### B.3 Above the engines: A2A, A2UI, and platform-only abilities

**A2A outbound-facing — the system becomes an agent others can hire.**
`agentdeck serve --protocols sse,a2a` exposes `ClaimPipeline` as an A2A task endpoint. A
partner company's agent negotiates, submits, and receives artifacts; on this side it is
just events — the partner's task lands in *this* pending inbox, *these* approval gates
apply, *this* audit log records it. Zero new code: the A2A adapter is a serializer over
the same stream (§5, §9 pattern).

**A2A inbound-facing — remote agents as first-class citizens.**

```python
customs = RemoteAgent(url="https://partner.example/a2a", name="CustomsBot")

class ClaimPipeline(BaseWorkflow):
    nodes = {..., "customs": agent_node(customs)}   # a remote node in this graph

class FrontDesk(BaseAgent):
    handoffs = [ClaimsAgent, customs]               # or a handoff target
```

This is the `Invocable` bet paying out: a remote company's agent slots in exactly like a
local class — implemented as an `EnginePort` whose "engine" is HTTP
(`adapters/engines/a2a_remote/`) — and its tool calls still land in the local event log,
so cost and audit cover work the platform did not even execute.

**A2UI — agents that build their own interface.** Instead of describing a form in prose:

```python
await ctx.caps.ui.render(Form("Claim details",
    fields=[Text("order_id"), Photo("damage"), Select("severity", ...)]))
```

That is a `ui.surface.created` event; whatever client is listening renders it, and the
user's submission flows back through the same caller-injected capability that powers ACP
file reads (§4.5, §9). The consequential detail: the approval node can now present a
rich approval card — claim summary, photos, amount, Approve/Reject — in Slack, the
dashboard, or a partner's client, because it is one event rendered by every listener.

**Abilities that exist only above the loop**, each impossible for any single SDK because
each SDK *is* one execution model:

*Cross-engine orchestration* — one `ClaimPipeline` run containing a LangGraph spine,
openai-agents nodes, a deterministic skill, and a remote A2A agent: four execution
models, one run, one trace, one cancel button.

*Uniform governance* — `Budget`, `deadline`, and `PolicyPort` rules such as "payout
tools require an approval event earlier in this run," enforced by the Runtime
identically across every engine, local or remote (§4.3, §4.5).

*Steering* — "actually it was 2 boxes, not 3" mid-investigation:
`POST /runs/{id}/messages` drains at the next safe point instead of forcing a restart
(Story 3b; ADR-D5 write ordering).

*Replay as a superpower* — the log becomes regression testing (re-run last month's
fifty claim sessions against `ClaimsAgent` v2 and diff outcomes before deploying) and
time-travel debugging (branch a recorded session at event 34 and try a different path).

*Fleet operations* — `agentdeck runs list --status waiting_human`; signal any run from
anywhere; one inbox spanning agents, workflows, and partner-submitted A2A tasks; and
event-driven automation, e.g. a ~15-line sink that pings Slack when any run crosses 80%
of its budget.
