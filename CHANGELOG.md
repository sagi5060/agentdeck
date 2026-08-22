# Changelog

All notable changes to agentdeck. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow
[SemVer](https://semver.org/). Entries are user-facing  -  what changed for
someone using the package, in `Added / Changed / Deprecated / Removed /
Fixed / Security` order  -  and are written to be attached to a release as-is.

## [Unreleased]

### Fixed

- **Correction to 5.0.0: no executor is named `"langgraph"`.** The "engine port is `Executor`"
  entry below lists `LangGraphEngine` becoming `LangGraphExecutor` and keeps `"langgraph"` among
  the wire values. Both went with the engine in that same release; the executors a 5.x deck names
  are `"native"`, `"openai-agents"` and `"stub"`.
- **Known Issues no longer documents removed machinery.** The restart entry named
  `AGENTDECK_CHECKPOINT`, a setting 5.0 removed along with the checkpointer, and one row described
  a graph `interrupt()`. Both are gone from [Known Issues](/resources/known-issues).
- **`run.answer()` says what an ask without options does with the value.** An ask that named
  `options` refuses anything outside them; one that named none hands the value to the body, which
  is the only thing that can judge it. `PendingRun.invocable` now says why the name is general.

## [5.0.0] - 2026-08-22

### Added

- **Lifecycle & Control docs now include a capability matrix.** A table on the
  [Lifecycle & Control](/runs-and-control/lifecycle-and-control) page shows which of
  `pause()`, `resume()`, `cancel()`, and `answer()` are legal from each run status, for quick
  reference alongside the existing prose.
- **Agents can select OpenAI, Anthropic, Gemini, Ollama, or OpenRouter by model prefix.** Each
  provider reads its own environment credential, and `Deck.build()` reports every missing
  requirement before execution. Runtime settings now come only from environment variables or
  the project `.env`; the undocumented `config.yaml` source is removed.
- **`run.can` says which lifecycle controls are available before you call one.** `run.can.pause`,
  `run.can.resume` and `run.can.cancel` combine the engine's own capability with the run's current
  state, so a UI can enable its buttons and a workflow can branch without guessing. Informational
  by design: it reads the status the handle last saw, and the lifecycle methods stay the
  authoritative answer.
- **`@tool` and `@workflow` declare AgentDeck-native code: ordinary Python, no engine.** A
  `@workflow` body takes `WorkflowCtx` and runs as a coroutine that suspends in place  -  at
  `ctx.safepoint()`, or at `ctx.ask(question, options=...)`  -  and continues on the next line
  rather than replaying, because its own locals are the checkpoint. A `@tool` takes `ToolCtx` and
  is a leaf capability: it can report and safepoint, but not `ask` or start other runs. Both are
  played by a new native executor that parks a suspended body's coroutine in memory and cancels
  it on `Deck.aclose()`.
- **`ctx.invoke()` starts a child run from inside a `@workflow`, and `ctx.parallel()` composes
  several.** `ctx.invoke(target, *args, **kwargs)` binds to the target's own signature and hands
  back a `Run`: `await ctx.invoke(...)` is the result, `child = ctx.invoke(...)` is the handle,
  with its own id, its own log and its own `child.can.*` / `pause` / `resume` / `cancel`. It takes
  a catalog name or a `@tool` / `@workflow` definition the deck holds; any other object waits for
  the invocation resolver. `ctx.parallel(*runs)` is all-or-nothing: the first failure cancels its
  siblings and propagates, so no child is left running behind a parent that gave up.
- **`Agent(subagents=[...])` lets an agent delegate a bounded task to another agent.** Names
  resolve against the catalog at `build()`, like `handoffs=`, and each becomes one tool the model
  may call with a task; calling it runs that agent as a child run and hands its final output back.
  Not a handoff: the conversation stays with the parent, and the child sees only the task.
- **`ctx.agents.create()` and `ctx.agents.fork()` mint an agent the catalog does not hold.**
  `create(**declaration)` takes the keywords `Agent(...)` takes, `fork(source, **overrides)` copies
  a catalog name, an `Agent` or another instance, and either is invoked with `ctx.invoke` like any
  other target. The deck holds what it minted, which is what makes a child run of it answerable,
  resumable and cancellable. `ctx.agent` is the agent whose turn is running, or `None`.
- **`run.started` carries `parent_run_id`.** A delegated turn's cost rolls up into its parent's
  `run.completed.usage`, cancelling a parent cancels the children it started, and a reader of the
  log follows the same edge afterwards. Delegation is bounded at depth 3 and fan-out 8, with an
  error naming which bound was hit and who hit it; there is no setting for either.

### Changed

- **`EventSinkPort` is renamed `Observer`, and it gains `agentdeck.views`.** `views.all`,
  `views.chat`, `views.tools`, `views.reports`, `views.lifecycle`, `views.errors` and
  `views.usage` are composable predicates over the event stream (`|`, `&`, `~`); pass
  `view=` to any observer to filter what it receives. `agentdeck.observers.Langfuse` is
  renamed `LangfuseObserver`, and `ConsoleObserver` / `FileObserver` join it: one prints a
  line per event, the other appends one JSONL line per event, both with no configuration.
- **The event schema is `major=4`, and v5.0.0 does not read a log 4.x wrote.** The payload
  vocabulary moved (see the reporter entry below, and `RunStarted.kind_of_invocable` gaining
  `"tool"`), so the major bumps, and this release does not carry a compatibility window: `Event`
  refuses any major but its own, by name, saying that the log has to be replayed into a new store
  or read with the version that wrote it. Every store behaves the same way, so there is one rule to
  know rather than one per backend.
- **One executor method: `execute`.** `start` and `resume` are one call. `resume` was never the
  pause's resume  -  lifting a pause already re-entered `start` with the log as history  -  so it
  existed only to answer an interrupt, which the log already records: the answer rides on
  `run.resumed` and the thread on `run.interrupted`. An executor now reads which of the three
  plays it is (fresh, a replayed pause, an answered interrupt) off `history`, and
  `Runtime.resume(...)` no longer takes a `thread_id`. A custom executor implements one method,
  and a target that never suspends implements nothing extra.
- **An answer the log cannot hold is refused.** `run.answer(...)` with a value JSON cannot carry
  used to log a warning, record nothing, and hand the value to the engine in memory anyway  -  a
  run resumed on an answer no replay and no other process could reproduce. It raises `ValueError`
  now, before anything is claimed, and the run stays answerable.
- **The engine port is `Executor`.** `agentdeck.core.ports.EnginePort` becomes `Executor`,
  `adapters/engines/` becomes `adapters/executors/`, `LangGraphEngine`/`OpenAIAgentsEngine`/
  `StubEngine` become `LangGraphExecutor`/`OpenAIAgentsExecutor`/`StubExecutor`, and
  `InvocableSpec.engine` becomes `InvocableSpec.executor`. One word for the thing that executes a
  target, in the type, the module path and the spec. Wire values are untouched: an executor is
  still named `"langgraph"`, `"openai-agents"` or `"stub"`, and `run.failed` still carries
  `error_code="engine_error"`.

- **The reporter has four methods and one event kind.** `ctx.reporter.status(...)` and
  `ctx.reporter.progress(...)` are replaced by `info`, `warning`, `error` and `report`, each taking
  arbitrary keyword fields: `ctx.reporter.warning("Primary source unavailable", source="drive")`,
  `ctx.reporter.report("candidate_found", score=0.91)`. The `status.reported` and
  `progress.reported` events become one `report` event carrying `level`, `message` and `fields`.
  A stage count is now `report("reviewing", current=2, total=4)`, and nothing validates the pair.
- **`Context[T]` is `ToolCtx[T]`, and `ctx.checkpoint()` is `ctx.safepoint()`.** A tool declares
  `ToolCtx[T]`; the orchestration surface an imperative workflow gets is `WorkflowCtx[T]`, so one
  type no longer has to mean both. The method rename follows the concept the docstrings already
  used: a safe point is where a run can be stopped.
- **`run.pause()` and `run.cancel()` refuse loudly instead of returning `False`.** Both used to
  answer `bool`, where `False` meant "this deck has no control backend" and was indistinguishable
  from "the run had already ended". They now return `None`, raise `RunStateError` when the run's
  state refuses the operation, and raise the new `UnsupportedControlError` when the control can
  never be applied. An operation with nothing to do (pausing a paused run, cancelling a finished
  one) still returns quietly. `run.resume()` is strict on the same terms and no longer ignores a
  run that is waiting for an answer.
- **A run that belongs to no conversation now belongs to no session.** `RunContext.log_key`
  (`session_id or run_id`) is gone. It answered "which stream do these events go in" by encoding
  two different things as one string, and a store handed it could not tell a session named after a
  run from that run itself. Event stores hold a nullable `session_id` instead, and every
  `EventStorePort` method takes `ctx` plus only what is genuinely not identity: `read(log_key, ctx)`
  is `read_session(ctx)`, `read_run(log_key, run_id, ctx)` is `read_run(ctx)`,
  `claim_resume(log_key, run_id, resumed, ctx, origin)` is `claim_resume(resumed, ctx, origin)`, and
  `RunSummary.log_key` is `RunSummary.session_id`. A caller reading another run builds the context
  for it (`replace(ctx, run_id=...)`). **No migration**: 5.0 does not read a log 4.x wrote, on any
  store. Opening a SQLite or Postgres log 4.x wrote raises a clear `StoreError`; a Redis log
  written by 4.x reads as empty, since its keys were shaped by the old encoding. Drain or discard
  a 4.x log, or replay it into a new store, before upgrading.
- **A completed handoff is the core event `agent.changed`, not a namespaced `custom` one.** The
  OpenAI Agents executor stopped emitting `custom` named `openai_agents.handoff`; a handoff between
  agents in the same run and conversation now lands as `agent.changed`, carrying `previous_agent`
  and `next_agent`, and only once the handoff has actually happened. A handoff that was requested
  but failed or was refused still emits nothing. The schema is `major=4, minor=1`.

### Removed

- **LangGraph is gone: `Workflow`, `WorkflowDeclaration`, `graph=`, `durable=`, the executor, the
  checkpointer and `AGENTDECK_CHECKPOINT`.** What hung off them goes too: `sleep_until` and the
  timer sweep (`AGENTDECK_RUNTIME_SWEEP_INTERVAL_SECONDS`), `Workflow.pending()`, `as_tool()`, the
  `AgentNode`/`LoadFileNode` graph nodes, the `node.updated` event, and the HTTP surface's
  `/workflows/*` routes. A fresh install pulls no `langgraph`, `langchain-core` or
  `langgraph-checkpoint-sqlite`. A 4.x graph user stays on 4.x, or ports the graph to an
  imperative `@workflow`.
- **The `durability` extra is now `postgres`.** Install `agentdeck-sdk[postgres]`: what remains in
  it is the Postgres event log's driver, and the checkpointer the old name meant is gone.

### Fixed

- **`deck.runs.get` no longer corrupts a session id that happens to equal a run id.** It recovered
  the session by comparing the stored key against the run id, so a caller-chosen `session_id` that
  matched came back as `None`.
- **A busy standalone run no longer names a session that does not exist.** The refusal read
  `session '<the run's own id>' is held by run '<the same id>'`; a run in no session now says so.
- **`Deck.aclose()` always returns.** A run whose task never took the cancellation `aclose()` sent
  it held the close open forever, wedging a shutting-down server. The close now asks twice, a
  second apart, and then stops waiting: it logs the run id and records the run's own
  `run.cancelled`, so an abandoned run does not stay open in the log with nothing playing it. The
  run's own later writes are refused rather than appended past that event.

## [4.0.5] - 2026-08-19

### Added

- **Jack's citations are links.** He names the pages he used at the end of an answer in several
  shapes (bare slug, backticked, `[slug]`, a markdown link, sometimes on the wrong host); every
  shape that names a real documentation page now links to it. An invented or misspelled slug
  still renders as plain text.

## [4.0.4] - 2026-08-19

### Added

- **`examples/jack` bounds its conversation memory.** A session re-sends its whole history every
  turn, and tool results here are documentation pages, one of which is 30 KB. Fifteen turns
  reached a 320 KB request and the endpoint then rejected every later turn in that session
  permanently. `jack.session.BoundedSessions` is a `Deck(session_factory=...)` that keeps only the
  most recent whole exchanges, cutting at user-message boundaries so a tool call is never
  separated from its result.
- **`examples/jack` can trace itself.** Setting `AGENTDECK_LANGFUSE_PUBLIC_KEY` attaches the
  packaged `Langfuse` observer, and setting `AGENTDECK_EVENTS` to a `sqlite://` URL makes the run
  log durable. Both are off by default, so a clone still runs with no backend. Both store what
  callers typed; the example's README says what that means for a public endpoint.

### Fixed

- **Three documentation pages stated things the code does not do.** `runs-and-control/lifecycle-and-control`
  listed a `QUEUED` state and uppercase names; the six real values are `running`, `paused`,
  `waiting_answer`, `completed`, `failed` and `cancelled`. `reference/events` listed
  `message.delta`, `tool.call` and `tool.result`, none of which exist, and omitted 14 kinds that
  do; it now covers all 21 with their payloads. The Quickstart printed an invented transcript,
  read `run.status` as a property when it is a coroutine, and iterated `run.events()` without
  `follow=True`, which returns only what the log already holds.

## [4.0.3] - 2026-08-19

### Added

- **Documentation pages that are finished can ask for their next piece.** A `Contribute`
  component marks one specific, scoped improvement at the end of a page and links the issue that
  specifies it. It appears only on pages whose authoritative part is already written, so it
  invites an addition rather than standing in for missing documentation. Live on
  [How Jack is built](https://agentdecksdk.com/jack),
  [Quickstart](https://agentdecksdk.com/meet-agentdeck/quickstart),
  [Known Issues](https://agentdecksdk.com/resources/known-issues) and
  [Implementation notes](https://agentdecksdk.com/jack/notes).

### Changed

- **The README follows the landing page again.** Jack is built with the three tools he actually
  has, the division of labour lists the six concerns AgentDeck owns rather than four, and his
  section links [How Jack is built](https://agentdecksdk.com/jack) and
  [Implementation notes](https://agentdecksdk.com/jack/notes) ahead of the source directory.

### Fixed

- **The landing page's release chip said 4.0.1 on a 4.0.2 site.** It now tracks the release it
  ships in.

## [4.0.2] - 2026-08-19

### Changed

- **`examples/jack` reads `JACK_*` environment variables.** `ASK_AGENTDECK_ORIGINS`,
  `ASK_AGENTDECK_SESSIONS_PER_DAY` and `ASK_AGENTDECK_TURNS_PER_SESSION` are now
  `JACK_ORIGINS`, `JACK_SESSIONS_PER_DAY` and `JACK_TURNS_PER_SESSION`, correcting 4.0.1,
  which said they would keep their names. A deployment that sets the old names falls back to
  the defaults silently, so rename them when you upgrade.
- **The documentation landing page is one continuous build rather than nine chapters.** It
  builds Jack once, from an `Agent` to a live run, and the execution tree beside the code is
  that code's own tree.
- **[How Jack is built](https://agentdecksdk.com/jack) is its own page**, with the decisions
  and the alternatives they beat in
  [Implementation notes](https://agentdecksdk.com/jack/notes). The README follows the same
  build.

### Fixed

- **Jack could not be reached from the published site.** The docs build baked
  `http://localhost:8100` as its API origin whenever `NEXT_PUBLIC_AGENTDECK_API_URL` was
  unset, so the panel called the visitor's own machine. `docs-site/.env.production` now
  carries the origin, and a real environment variable still wins.
- **A second question to Jack replaced the first.** The panel keeps the transcript, so a
  conversation reads as one.

## [4.0.1] - 2026-08-18

### Changed

- **The `ask-agentdeck` example is now `jack`.** `examples/ask-agentdeck/` is
  `examples/jack/`, its package is `jack`, and the agent is named `Jack`, so the docs
  assistant has one name rather than two. Run it with
  `uvicorn jack.server:app --port 8100`. Its `ASK_AGENTDECK_*` environment variables keep
  their names.
- **The documentation landing page is rebuilt around one worked example.** It builds Jack from
  an `Agent` to a live run instead of listing features, and every code block states the release
  it belongs to, so the examples that are not in 4.0.0 yet say so.

## [4.0.0] - 2026-08-16

**Hardening, and it cost a major version.** Thirty-seven issues, most of them findings
from people using the SDK rather than reading it. Nothing here adds a user-facing
capability; what it adds is the right to trust what was already there. Three things
had to change shape to fix the defect underneath them: a run's identity, the
run-scoped API, and the control plane. Read **Upgrading** before you bump.

### Upgrading

- **Breaking: `deck.runs` is now `start`/`get`/`list`, and a `Run` handle owns every op that
  acts on a run already in flight** (#322). `deck.runs.pause/cancel/resume/answer/status/pending`
  are removed, not deprecated. `await deck.runs.start(name, input, ...)` begins a run and hands
  back a `Run` (`.id`, `.key`, `.namespace`, `.session_id`) whose own methods replace them:
  `run.status()`, `run.pause(reason)`, `run.resume()`, `run.cancel(reason)`, `run.pending()`,
  `run.answer(value)`, `run.events(from_seq=0, follow=False)`, and `await run` for the result  -  a
  `TurnResult` for an agent, the graph's own state for a workflow. `deck.runs.get(id)` (optionally
  `namespace=`) or `deck.runs.get(namespace=, key=)` rehydrates a handle to a run that already
  exists; it never mutates and raises `NotFoundError` for one this namespace has never heard of.
  `deck.runs.list(namespace=, status=, limit=)` replaces the old `pending()` inbox and stays
  scoped to one namespace. Two handles on one run always agree  -  the durable store is the only
  thing either reads from. `deck.run()`/`deck.stream()` are unchanged in behavior (still return
  an interrupt as a value rather than raising); `await run` on a `Run` that is `PAUSED` or
  `WAITING_ANSWER` instead raises the new `RunSuspendedError` (a `RunStateError`), carrying
  `.pending`, since there is no timeout parameter to wait either state out. `context=` is retained
  on the handle `runs.start()` returns for that handle's whole life  -  `resume()`/`answer()` no
  longer take one, and a handle from `get()` always resupplies `None`. `PendingRun` is no longer
  public (`deck.runs.list(status=RunStatus.WAITING_ANSWER)` replaces it); `InterruptResult` gains
  the canonical `id` alongside its existing fields. `EventStorePort.locate()` is removed (no
  caller left once `Deck._status` went with it) and replaced by `find_by_key(ctx, key)`, the read
  side of the `(namespace, key)` claim, across all four stores.
- **Breaking: a run's `id` is now minted, never derived from a caller-supplied value** (#324).
  `deck.run(...)`/`deck.stream(...)` no longer accept `run_id=`: the keyword is `key=`, an
  optional stable application identifier for lookup and idempotency, and it plays no part in
  the run's own address any more. Every run gets a fresh, globally unique `id` regardless of
  `key`, so two namespaces reusing one key now get two unrelated runs instead of the collision
  risk `run_id=` carried. `(namespace, key)` is a permanent claim once a run starts with it  -  a
  second `deck.run(..., key=...)` reusing one raises `DuplicateKeyError` rather than replaying
  the run that holds it, and the pairing survives a restart. The `events` table gains a `key`
  column and its run-scoped uniqueness tightens from `(namespace, log_key, run_id, seq)` to
  `(namespace, run_id, seq)`, so one logical run can no longer be split across two log keys. An
  existing SQLite events database is migrated in place on open (`key` column added, the tightened
  index rebuilt); a database with rows that genuinely violate the tighter constraint raises
  `StoreError` naming the conflict instead of silently picking a survivor. `list_runs` gains a
  `limit` parameter across all four stores.
- **Breaking: `deck.run(...)`/`deck.stream(...)` now raises `SessionBusyError` on a session
  held by a run parked `PAUSED` or `WAITING_ANSWER`, however long ago it went quiet** (#311).
  Every store's `claim_start` applied `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS` to *any* open
  run, including one suspended waiting for a human  -  so a parked approval was silently closed
  `failed` (destroying it) by the very next turn started on its session once the window had
  passed, contradicting the README's own promise that an approval outlives the process that
  asked for it. The timer now only ever applies to `RUNNING`; a parked run holds its session
  until `deck.runs.answer`/`deck.runs.resume` continues it or `deck.runs.cancel` ends it,
  however long that takes. `SessionBusyError`'s message reflects it too: a parked holder names
  the call that frees it instead of claiming it is "in flight", which was never true of it.
  If your deployment relied on a stale approval being cleaned up automatically, call
  `deck.runs.cancel(run_id)` on it explicitly instead  -  see [Sessions and
  Memory](https://agentdecksdk.com/concepts/sessions-and-memory#one-turn-at-a-time).
- **`redis` is no longer installed by `pip install agentdeck-sdk`** (#253). A deployment with
  `AGENTDECK_SESSION=redis://...` or `AGENTDECK_EVENTS=redis://...` now raises `ImportError` at
  boot  -  `Deck.__aenter__` resolves both through `SessionFactory.from_settings()` and
  `resolve_event_store()` before it opens  -  not on first use. It was a base dependency because a
  Redis-backed session (`agents.extensions.memory.RedisSession`) was imported unconditionally on
  every agent run, whatever `AGENTDECK_SESSION` was set to. That import is now deferred to the
  point a `redis://` URL is actually configured, and the client moves to a new `[redis]` extra:
  `pip install "agentdeck-sdk[redis]"`. Selecting a `redis://` session or event log without it
  raises a clear `ImportError` naming the install command, the way the durability extras already
  do.

### Added

- **A fourth example, `examples/existing-langgraph-agent`**  -  a LangGraph graph written
  without agentdeck, wrapped in four lines and gaining the event log, streaming and run
  control without a change to the graph module. It documents the two things wrapping asks
  for: `graph=` takes an uncompiled `StateGraph` factory (so agentdeck can attach a
  checkpointer when a workflow is `durable`), and a sibling module inside a bundle is
  imported relatively (`from .pipeline import …`). A `TypedDict` state is fine; a pydantic
  model is not required.
- **`AGENTDECK_RUNNER_HANDOFF_ENDS_ON_USER_TURN`** (#178). agentdeck collapses a handoff's
  transcript into a single assistant-role message before handing it to the next agent, and some
  OpenAI-compatible endpoints (Gemini's, for one) reject a request that carries no user role at
  all. Setting this to `true` appends a closing user turn after the collapsed transcript, via
  `RunConfig.handoff_history_mapper`. Off by default: it changes what every model sees on every
  handoff, including against OpenAI, so it stays opt-in rather than becoming everyone's new
  default behavior. Wired into both places agentdeck sets `nest_handoff_history`  -  a
  Runtime-driven run and a workflow node driving an agent of its own.
- **`AGENTDECK_RUNNER_HANDOFF_CLOSING_TURN`** (#178), defaulting to `"Please continue."`  -  the
  content of the user turn `AGENTDECK_RUNNER_HANDOFF_ENDS_ON_USER_TURN` appends. Override it for
  a deployment whose conversations aren't English: the default is otherwise an English sentence
  injected into every handoff regardless of the conversation's own language. An empty (or
  whitespace-only) value refuses to start rather than silently producing an empty user turn  -
  the shape a provider strict enough to need the setting is likely to reject too.

### Changed

- **Breaking: `RunStatus.WAITING_HUMAN` is now `RunStatus.WAITING_ANSWER`**, value
  `waiting_answer` (#295). The state pairs with the verb that leaves it, and covers a timer, a
  webhook or another agent as honestly as a person  -  `sleep_until` parks here, so a wall-clock
  wait was being recorded as a human one. An ordinary API break, not a schema change: status is
  derived by folding the log and is never serialised into a payload, so no golden file and no
  snapshot moves. `RunInterrupted.reason`'s `"human"` literal *is* in the schema and is
  unchanged; renaming it is a separate versioned change.
- **Breaking: `ControlPort` gains `consume(run_id, expected) -> bool`** (#295), the
  compare-and-set that takes the intent a caller just ruled on and only that one. A third-party
  adapter must implement it; both shipped adapters (`memory`, `sqlite`) do. It replaces
  `resume_run` writing `RESUME` over whatever was pending  -  an unconditional write that could
  overwrite, and silently destroy, a cancel that arrived while the run was suspended. A gate that
  honors a signal now takes it too, so the port is empty afterwards rather than holding a
  sentinel.
- **A cancel or pause recorded against a *stopped* run is now read where that run is picked up**
  (#295). A run that has already stopped has no loop polling the gate, so the operation
  continuing it  -  an answer, or a resume  -  reads the control port at its claim and rules on what
  it finds. Every such read ends in an event or an explicit no-op, never in silence.
- **Breaking: `EventStorePort` gains `locate(run_id, ctx) -> log_key | None`** (#316), so finding
  the log holding a run id is an indexed lookup rather than a scan of every run in the namespace
   -  `log_key` is the session id for a run under one, so a run id alone never named its own log.
  A third-party adapter must implement it; all four shipped ones (`memory`, `sqlite`, `redis`,
  `postgres`) do, adding no data any of them didn't already hold: SQLite and Postgres gain an
  index over `events`' own `namespace`/`run_id` columns (`CREATE INDEX IF NOT EXISTS`, so it
  applies cleanly to a database an earlier build already created), and memory/Redis keep a
  derived `(namespace, run_id) -> log_key` mapping a replay of the log rebuilds. `Deck._status`
  (behind `deck.runs.status`) uses it now instead of walking `list_runs`.
- **Breaking: `deck.run(...)`/`deck.stream(...)` no longer stop a run when its caller stops
  reading it** (#325). Execution used to *be* consuming the event generator, so closing
  `stream()`'s frame (or having the task reading it cancelled, as a real HTTP disconnect does)
  closed the run underneath it as `run.cancelled`. A run now advances in a deck-owned task from
  the moment it starts, independent of whether anyone is still watching  -  the same task any
  number of readers may observe through the store without stealing its events from one another
  or advancing it, and without needing to have started it themselves. A client that disconnects
  mid-stream therefore no longer stops the turn it was reading: the run keeps executing to its
  own natural end (bounded to one turn, its session freed once it reaches one), and the explicit
  `deck.runs.cancel(run_id)` is how a caller who wants that back gets it. `deck.stream()`'s wire
  bytes are unchanged (`tests/golden/` proves it byte-for-byte) and `deck.run()`'s propagated
  exception on a failed turn is unchanged; only the disconnect-cancels-execution coupling is
  gone. `Deck.aclose()` now settles or cancels whatever it is still executing before closing the
  store, and logs which happened per run.

- **`agentdeck.testing.scripted_model_server`'s `tool_name=` now also accepts a sequence of
  names** (#248), one tool call per request in order, then plain text once the sequence is
  exhausted  -  the shape a multi-step tool chain or a handoff round trip needs to script.
  A single name keeps its existing one-shot behavior unchanged.
- **Error messages a first-time user hits during composition or a first run now name the one
  docs page that answers them** (#238): the skill frontmatter/discovery `ConfigError`s (missing
  `description`, a name that doesn't match its directory, a duplicate name across skill roots),
  `SessionBusyError`, the store/checkpoint `ImportError`s for the `durability` and `redis`
  extras, the unknown-scheme `ValueError`s for `AGENTDECK_CONTROL`/`AGENTDECK_EVENTS`/
  `AGENTDECK_CHECKPOINT`, and the durable-workflow missing-`thread_id` `ValueError` (both the
  direct-call and the langgraph-engine copy). No error type or field changed, only the
  message text. In passing, the two durability install hints now say `agentdeck-sdk[durability]`
  (the actual distribution name) instead of the pre-rename `agentdeck[durability]`.

### Deprecated

- **`Usage.usd` is documented as reserved, not populated** (#177). agentdeck does not price
  model calls  -  no provider returns dollars in a response body, and a price depends on a
  contract, a tier and a date rather than on the call  -  so the field is `None` unless a caller
  sets its own cost. No behavior changes; it was always `None` in practice. Slated for removal
  at the next major.

### Removed

- **Breaking: `RunStatus.PENDING` is deleted, and `status_of([])` returns `None`** (#295). It
  was the fold's identity element for an empty sequence, never a state a run is in: `run.started`
  is a run's row 0, so there is no moment between "does not exist" and `RUNNING` for it to name.
  A store already answered `None` for a run it never saw (#294); `status_of` now agrees, so
  `status_of` is typed `RunStatus | None` and `can_resume` accepts `None`.

- **Breaking: the six run-scoped verbs move from flat `Deck` methods to `deck.runs.*`, and
  `tick`/`due_resumes` leave the public surface entirely** (#294). `deck.pause`, `deck.cancel`,
  `deck.resume`, `deck.answer`, `deck.status` and `deck.pending` are gone; call
  `deck.runs.pause(...)`, `deck.runs.cancel(...)`, `deck.runs.resume(...)`,
  `deck.runs.answer(...)`, `deck.runs.status(...)` and `deck.runs.pending(...)` instead  -  same
  signatures, same behavior, just grouped under the noun they act on rather than sitting flat
  beside the catalog and the two verbs (`run`/`stream`) that start a turn. `deck.tick()` and
  `deck.due_resumes()`  -  the timer sweep nothing in agentdeck calls yet  -  are no longer public at
  all; `sleep_until` keeps working, since the underlying sweep is unchanged, just no longer
  reachable from outside `Deck`.

### Fixed

- **The add-a-tool guide now states the SDK boundary and the tool-failure contract** (#241).
  The first `from agents import ...` examples now say that `agents` is the OpenAI Agents SDK,
  and the tool guide shows a recoverable failure returned as data before spelling out the actual
  raising path: SDK-handled tool errors are logged as `tool.call.completed`, while only an
  exception that escapes the SDK reaches `run.failed`, `Deck.run()`, and the HTTP error surfaces.
- **A worker killed outright held its session for up to an hour** (#244). Liveness was inferred
  from silence, and a healthy turn can be quiet for a long time  -  so the staleness window had to
  be generous, and one crashed process locked one user out of one conversation for
  `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS` (3600 by default), with no way to shorten it after
  the fact. A run now holds a **lease** while it plays and renews it six times per TTL, so the
  next turn on that session can positively assert that nobody is executing the run it found open,
  instead of waiting out a timer. With `AGENTDECK_CONTROL=sqlite:///<path>` a killed worker's
  session is claimable within one lease TTL (**90 seconds** by default, set with
  `AGENTDECK_RUNTIME_LEASE_TTL_SECONDS`). The new `LeasePort` reports only runs it **held and
  watched expire**  -  a run it
  has never seen is never reported dead  -  so with the `memory://` default, which knows nothing
  about any other process, behavior is exactly as before and the staleness timer remains the only
  backstop; boot warns when that is the case. Suspended runs are unaffected: `PAUSED` and
  `WAITING_ANSWER` have no worker to be dead, so they still hold their session until resumed,
  answered or cancelled. No new public API on `Deck` or `deck.runs`. Redis and Postgres lease
  backends follow when `AGENTDECK_CONTROL` gains those schemes.
- **A cancel or pause could land on the wrong tenant's run when two namespaces shared a
  caller-supplied `run_id`** (#315). Both `ControlPort` adapters (`memory`, `sqlite`) kept one
  pending signal per bare `run_id`  -  `acme/order-1234` and `globex/order-1234` shared a row, so
  a cancel meant for one could land on the other, and `consume()`'s compare-and-set made the two
  fight over the same slot. The control plane now addresses a run by its `id`, an opaque address
  that `RunContext.id` supplies  -  `Gate`, `Runtime.signal`/`resume`/`resume_run` and both
  `ControlPort` adapters all key by it, and no path takes a bare caller-supplied `run_id`.
  **Unnamespaced deployments see no change at all**: an unnamespaced id is byte-identical to
  today's `run_id`, so stored ids, the unnamespaced CLI (`agentdeck runs signal`) and the frozen
  v1 HTTP wire are unaffected. A caller-supplied `run_id` starting with `adr:` is now refused  -
  that prefix marks a namespaced id, and without the reservation an unnamespaced one could be
  crafted to collide with it. `agentdeck runs signal`
  now builds a `RunContext` to reach that same refusal, rather than writing straight to the
  `ControlPort`: a forged `run_id` shaped like a real `encode(namespace, run_id)` could otherwise
  reach a live namespaced run's `Gate` with no validation at all, from the one caller-facing
  surface that talks to a `ControlPort` without going through a `Runtime`.

  **Breaking, sqlite only:** the `signals` table's primary key is now `id`, not `run_id`. A
  file with no pending signal migrates automatically in place. A file with one or more pending
  signals refuses to open instead: the old schema never recorded a namespace at all, so a
  pending row cannot be told apart from one that collided under the very bug being fixed here,
  and carrying it forward under a guessed identity could silently re-address it to an unrelated
  run. Let every in-flight run settle (or clear the `signals` table) before upgrading.

- **A tool that raises is now recorded on `tool.call.completed.error`** (#250). The field has
  been in the schema since v3.0.0 and nothing ever set it, so a database call that timed out or
  an API that 500'd left no machine-readable trace anywhere: the run completed, HTTP answered
  200, and the only sign of failure was whatever prose the model chose to write about it  -  which
  a model that paraphrases past the word "error" omits entirely. `compile_tool` now passes its
  own `failure_error_function` to the Agents SDK, records the exception type and message, and
  the openai-agents translator moves it onto the paired `tool.call.completed`, capped at
  `RESULT_PREVIEW_MAX` like `result_preview` beside it.

  **Nothing the model sees changes**, deliberately: the formatter delegates to the SDK's own
  `default_tool_error_function`, so the failure text and the agent's freedom to retry are
  byte-identical to before. A tool failure is still not a run failure, the run still ends
  `completed`, and no event kind was added. One gap, by design: a tool the author decorated with
  `@function_tool` themselves is passed to the engine untouched and keeps its own failure
  handling, so its exceptions stay unrecorded  -  that is the existing trade for handing in a
  pre-built SDK tool, not a new one.

- **`sleep_until` now actually wakes up** (#303). An open `Deck` sweeps for its own lifetime  -
  started in `__aenter__`, cancelled in `__aexit__`  -  resuming any durable workflow parked past
  its wake moment with no cron or scheduler wired in by the user. Previously `_tick`/`_due_resumes`
  (the mechanism behind the sweep) were never called by anything, so a parked timer held
  `WAITING_ANSWER` forever, keeping its session claim, until something else happened to call the
  now-private `_tick`. The interval is `AGENTDECK_RUNTIME_SWEEP_INTERVAL_SECONDS` (default 30s) on
  `RuntimeSettings`, on by default  -  there is no deployment for which silently never waking a timer
  is the safer choice. A sweep that raises is logged and retried on the next interval rather than
  ending the loop; a process that opens the deck, takes a turn and closes within one interval never
  sweeps at all, and the deadline fires on whoever next holds the deck open past that.
- **A cancel against a run waiting for an answer is honored instead of vanishing** (#229, #295,
  #311). `deck.runs.cancel` on a parked run returned `True`, recorded the signal, and the run
  answered on anyway: only `resume_run` polled the control port, and an approval does not come
  back that way. `deck.runs.cancel` against a suspended run now claims and terminates it right
  there, recording `control.requested` then `run.cancelled`  -  no `control.observed`, because the
  run reached no safe point; it was already stopped when the cancel landed. Ends the same way for
  a *paused* run. Claiming happens at the cancel itself rather than being deferred to whoever
  next answers or resumes: once #311 stopped a stale timer from ever reclaiming a parked run's
  session, a deferred cancel could sit unread forever if nobody happened to touch the run again.
  `deck.runs.cancel(run_id, reason, namespace=...)` takes the same `namespace` `deck.runs.pending`
  already does, needed to locate a suspended run opened outside the default namespace at all.
- **`deck.runs.resume` on a run that is waiting for an answer now refuses, naming
  `deck.runs.answer`** (#295), and `deck.runs.answer` on a paused run refuses naming
  `deck.runs.resume`. Both raise the new `agentdeck.errors.RunStateError`, which the HTTP surface
  answers as `409`. `resume` used to return `[]` for a parked run  -  silence, to a caller holding
  that run's only answer  -  because the lookup behind it listed `PAUSED` runs only.
- **A pause recorded against a run waiting for an answer now refuses the answer** (#295) rather
  than being silently lifted by it, and stays pending. Lifting would let an answer override an
  operator who said stop; refusing costs the answerer one round trip and keeps both intents
  intact.

- **`EventStorePort.run_status` no longer returns `PENDING` for a run the store never heard
  of** (#294). It now returns `None` for that case, distinguishing it from a run that exists but
  hasn't logged a lifecycle transition yet  -  the two used to fold into the same value. Only the
  default projection changes (no adapter overrides `run_status`); `RunStatus.PENDING` and
  `status_of()`'s own contract are unchanged.
- **The documentation entry path now points readers to skills, sessions, durable stores and the
  API reference instead of ending at a two-link dead end** (#239). The getting-started page now lists the
  next concepts to read, the concepts overview names the reference as the source for exact API
  details, and the how-to guides link onward to the specific reference pages behind the APIs they
  use.
- **`pip install agentdeck-sdk` now runs a `durable=True` workflow with no extra** (#232).
  `langgraph-checkpoint-sqlite`  -  what `AGENTDECK_CHECKPOINT`'s default (`sqlite://...`) needs  -
  moves from the optional `[durability]` extra into base dependencies, so the default that every
  human-approval workflow relies on is installable by default. `[durability]` now covers the
  Postgres checkpointer and event store only.
- **The non-streamed HTTP surface now answers every server-side failure with the documented
  500 `{"detail": "internal error"}`, not just `AgentdeckError` ones** (#243). A workflow
  node's plain exception, an SDK error, or an `httpx` transport failure used to fall through to
  Starlette's bare-text `Internal Server Error` on the non-streamed chat and workflow endpoints,
  while the streamed path already reported the identical failure correctly as an in-band SSE
  `error` event. A catch-all handler beside the existing one closes that gap; a tool's own
  exception is a separate, still-open gap (#250)  -  the SDK's default `failure_error_function`
  swallows it into a successful 200 before it ever reaches this handler. 404/409/422 and the
  existing `AgentdeckError` 500 are unchanged, and no exception message reaches the response
  body.
- **The human-approval guide now shows `answer()` re-supplying a context, and says what omitting
  it does** (#255). Two rules meet on resume  -  the interrupt node re-runs from its start, and the
  context is never serialized with the run  -  so a node reading `ctx.data` after an approval gets
  `None` rather than an error, and the run continues with a quietly wrong value. Both rules were
  already documented separately and correctly; their interaction was stated once in prose and
  never demonstrated, and two clean-room reviewers missed the consequence anyway.
- **The openai-agents engine no longer refuses a `DataBlock` on input** (#226). It used to raise
  `ConfigError` there  -  a `DataBlock` was an output-only block in practice, so the typed way to
  hand a model structured per-run context did not exist and every embedded application invented
  its own prose preamble. It now renders as its own part, `json.dumps(data, ensure_ascii=False)`
  with nothing wrapped around it: each block is already a separate entry in the SDK's content
  list, so the boundary between it and a neighbouring `TextBlock` is the API's own rather than a
  delimiter this adapter
  invents, and there is no open/close token embedded data could spoof to escape early.
  `ResourceBlock` still raises  -  a `uri` is a pointer the engine never fetches, and the message now
  says so, rather than reading identically to the data case. Crash reconciliation renders a
  `DataBlock` the same way on its log-side transcript, so a turn that carries one does not read as
  a permanent session divergence on every turn after it.
- **A langgraph workflow run now has a safe point, so `pause` and `cancel` can reach it**
  (#128). `LangGraphEngine` checkpoints the run's control gate between two `updates` chunks
  (langgraph's own node boundary), which is what produces `control.observed{safe_point:
  "node_boundary"}`; a workflow run previously had no safe point at all, so a signal against it
  sat unread until the graph finished on its own.

  **A resumed pause continues from that boundary: it never replays.** Unlike an interrupted
  run, which re-enters from its start, a paused workflow's checkpoint already has everything
  before the pause, so `deck.runs.resume` re-enters langgraph with `None` (its own idiom for
  continuing a thread) rather than the run's original input, and no already-completed node
  runs again. That guarantee holds for `durable=True` from any process; a `durable=False`
  workflow can only be resumed from the process that paused it (its checkpoint lives in that
  engine's own memory, ADR-D5), and is refused, naming `durable = True`, if resumed from
  another one instead of being silently replayed from the entry node with empty state.
- **The docs site is swept against the whole v4.0.0 surface.** Three claims were stale rather than
  merely thin: `definitions.mdx` named `Deck.runs.answer()`, removed by #322; `run-control.mdx`
  still called a run's identity `run_id`, renamed by #324; and `choosing-a-store-backend.mdx` said
  the control port "only has to outlive the seconds between a request and that safe point", which
  #244 made false by putting each run's liveness lease in the same backend. Two v4 changes had no
  page at all: a reader no longer drives the run it is reading (#325), now in
  `runs-and-the-event-log.mdx` and `serve-over-http.mdx`, and a raising tool's exception landing on
  `tool.call.completed.error` (#250), now in `add-a-tool.mdx` with the `@function_tool` opt-out
  named. `known-issues.mdx` retitles its fixed table to v4.0.0 and moves #244 and #178 into it,
  both closed; `roadmap.mdx` is rewritten around the shipped v4.0.0 and the v5.0.0/v5.1.0/v5.2.0
  milestones that replace v3.3/v3.4/v3.5. `AGENTDECK_CONTROL`'s own description now says it holds
  the lease port too, so the generated settings reference says it as well.
- **Docs swept against nine issues closed since the last pass** (#317). `known-issues.mdx` gains
  a `Fixed in v3.2.0` section (#250, #229, #232, #243, #255, #253, #226); `usage.usd` moves off
  the page entirely, since #177 ruled it a design position rather than a defect. The one entry
  that stayed open got reworded rather than removed: a tool's non-serializable return still
  reaches the model as a `repr()`  -  #251 was closed by folding it into #250, but #250's fix
  shipped only the raise half, so this half is untracked by any open issue today. `run-control.mdx`
  and `runs-and-the-event-log.mdx` drop their last `waiting_human`/`pending` references, both
  renamed away by #295. `README.md`'s extras line now matches `pyproject.toml` (SQLite
  checkpointer in base, `redis` its own extra) and its run-control bullet says "agent or workflow".
- **`tests/test_generated_reference.py` now covers all five files `generate_docs_reference.py`
  writes, not two** (#317). `settings.mdx` and `cli.mdx` stay pinned byte for byte; `llms.txt`
  joins them. `changelog.mdx` and `llms-full.txt` only assert the generator still produces them,
  rather than pinning them too: both derive from `CHANGELOG.md`, which is `merge=union` so
  concurrent PRs can each add an entry, and a byte pin would fail every open PR the moment any
  other one merged one. The three previously untested pages could drift from `CHANGELOG.md`/the
  site's own pages for a whole release with `make check` green throughout  -  reported as unrelated
  churn by two different agents this week when they regenerated one page and were surprised by
  the other four changing too.

### Added

- **`examples/agent-with-a-skill/`**  -  an agent with two tools and one skill, the first shipped
  example to include a `SKILL.md`. Skills were the only thing `Deck.from_project()` discovers with
  no runnable example, so the frontmatter contract could only be learned from a build error
  ([#242](https://github.com/agentdecksdk/agentdeck/issues/242)).
- **`docs/delivery/review-v3-outsider.md`**  -  the v3.0.0 clean-room review: three reviewers given
  only the wheel, the README, the docs site and `examples/`, each building a small app and reporting
  what broke. Source of the `finding:`-labelled issues opened against v3.0.0.

## [3.1.0] - 2026-08-13

**AgentDeck is on PyPI, under the name `agentdeck-sdk`.**

```bash
pip install agentdeck-sdk          # was: an install from the repository at a tag
```

**The import does not change.** `import agentdeck` is what it always was, and no code needs
editing. Only the line that installs it moves.

### Added

- **`agentdeck.testing`**, the exported stub-runner test harness (#26): `ScriptedModel` (text
  deltas, an optional tool call, mid-stream failure, a `hold` gate for catching a consumer
  mid-await, configurable usage counts), `patch_model()` (swaps the SDK's model provider for the
  duration of a `with` block), and `scripted_model_server()` (a local Chat-Completions-compatible
  HTTP endpoint for a test that must run agentdeck as a real subprocess or a real HTTP client).
  Every one of this repo's own hand-rolled scripted models now builds on it.

### Changed

- **The distribution is renamed `agentdeck` → `agentdeck-sdk`.** Not a preference: PyPI refuses
  the name `agentdeck` as *"too similar to an existing project"*  -  an abandoned `agent-deck`
  placeholder (one release, author `"Your Name"`, summary *"A placeholder package"*) that its
  similarity check treats as the same name once separators are stripped. `agentdeck-sdk` also
  matches the brand.

  Install and import names now differ, which puts AgentDeck in company it did not choose but is
  used to seeing  -  `openai-agents` imports as `agents`, `beautifulsoup4` as `bs4`,
  `python-dateutil` as `dateutil`. Extras keep their names: `agentdeck-sdk[serve]`,
  `[durability]`, `[observability]`.

  A PEP 541 request for the squatted name is open. If it is granted, `agentdeck` becomes the
  distribution and `agentdeck-sdk` becomes a shim that depends on it  -  no import changes then
  either.

- **Every install line is now a PyPI install**, not a pinned `git+https://…@vX.Y.Z` URL. The
  git form still works and is still what a contributor uses for an unreleased commit.

### Fixed

- **`agentdeck.__version__` would have reported `0+unknown` after the rename.** It resolves
  through `importlib.metadata.version()`, which takes the *distribution* name  -  renaming the
  distribution without updating that call makes the lookup miss and fall through to the
  not-installed fallback, silently. Caught before release; the fallback now only fires when the
  package genuinely is not installed, which is what it is for.
- **`Agent(model=...)` now actually runs on the model it names, instead of being silently
  overridden by `OPENAI_MODEL` on every turn (#247).** The host Agents SDK's `RunConfig.model`
  overrides *every* agent's own model once set, and every run's config set it from
  `OPENAI_MODEL` unconditionally  -  so a per-agent override was accepted, type-checked, and then
  discarded one layer later. The default is now resolved onto the compiled agent instead, at
  build time: an agent that declares its own model keeps it, one that declares none still gets
  `OPENAI_MODEL`, and this holds across handoffs and `as_tool()` since each agent resolves its
  own model independently of which one is playing.
- **A checkpoint connection failure now raises `StoreError` naming `AGENTDECK_CHECKPOINT`,
  instead of a bare driver exception** (#233). An unwritable sqlite checkpoint path raised
  `sqlite3.OperationalError: unable to open database file`, and a bad Postgres DSN raised
  `psycopg.OperationalError`  -  neither said which setting caused it or that agentdeck had
  resolved the path at all, unlike every other store, which already answers connection failures
  this way. The sqlite message also names the resolved file path; the Postgres one does not,
  since a DSN can carry a password. A driver error raised mid-run is unaffected.
- `agentdeck-serve --help` (and `-h`) now prints usage and exits 0 instead of crashing with a
  `FileNotFoundError` for a missing `./.agentdeck` project. `--host`/`--port` flags were added,
  defaulting to the existing `HOST`/`PORT` env vars, so passing neither behaves exactly as
  before; an unrecognized argument now exits 2 with usage instead of being silently ignored.
  (#245)
- **`AGENTDECK_RUNNER_WORKFLOW_NAME` no longer defaults to `local-sandbox-repl`.** That value named
  v1's sandboxed local REPL, deleted in #71  -  every untuned run was labeling its tracing (e.g.
  Langfuse) after a development tool that no longer exists. The default is now `agentdeck`.


## [3.0.1] - 2026-08-12

**No functional change to the package.** `agentdeck/` is byte-identical to v3.0.0  -  this release
carries documentation, brand and discoverability work, and exists so the published site and the
release train agree. Upgrading from v3.0.0 changes nothing at runtime.

### Added

- **Three project pages on the documentation site.** [Known
  Issues](https://agentdecksdk.com/known-issues) is the one worth bookmarking: every
  entry is reproduced and open against this release, most of them fail *silently*  -  a tool that
  raises still completes the run, a non-serializable tool return reaches the model as a memory
  address, `Agent(model=...)` is accepted and ignored  -  and each says what to do until it is
  fixed. Also [Roadmap](https://agentdecksdk.com/roadmap) and a
  [Changelog](https://agentdecksdk.com/changelog) generated from this file.
- **`/llms.txt` and `/llms-full.txt`**, generated from the same Markdown the site renders, for
  coding agents and LLM search. Plus `context7.json`, whose rules encode the mistakes an assistant
  actually makes: no `agentdeck-sdk` package, a `Context[T]` tool must not be
  `@function_tool`-wrapped, `durable = True` needs the `[durability]` extra.
- **`sitemap.xml`, `robots.txt`, canonical URLs and Open Graph tags**, all generated. One
  canonical origin behind `NEXT_PUBLIC_SITE_URL`, so moving domain is a variable rather than a
  sweep.
- **A changelog tool for the documentation assistant.** *"When was `Context` added?"* is a real
  question, and its answer is only in release history  -  which must never ground a documentation
  answer, because history names APIs that were later removed.
- **`docs/brand/`**  -  the mark as vectors, with the reasoning. No rasters: this repository tracks
  no binaries.

### Changed

- The documentation site carries the brand: Agent Blue, the mark in the navbar and as the favicon,
  and **AgentDeck SDK** in titles and descriptions. The package and import stay `agentdeck`.

### Fixed

- **Code blocks on the documentation site rendered washed grey.** `nextra-code` is on three
  elements, so a rule written for inline code laid a pale wash over the dark block, and the
  syntax theme then did not match the surface it was on.

## [3.0.0] - 2026-08-11

The stable v3. Everything below landed on top of `3.0.0b1`: multimodal input, a versioned event
envelope, one variable per configuration decision, `Context[T]` injection over both engines, and
observability declared where the deck is declared.

**Ask AgentDeck** ships alongside it  -  a documentation assistant built on the v3 public surface,
in [`examples/ask-agentdeck/`](examples/ask-agentdeck/). It exists as much to test the surface as
to answer questions: building a real application against it produced three findings, all recorded
rather than smoothed over, and none of them blocking.

### Upgrading

- **A retired v2 environment variable now refuses to start, rather than being ignored.** Nothing
  binds `AGENTDECK_EVENTS_BACKEND`/`_URL`, `AGENTDECK_CONTROL_*`, `AGENTDECK_CHECKPOINT_*`,
  `AGENTDECK_SESSION_REDIS_URL`, `AGENTDECK_LANGFUSE_HOST` or `APP_CONFIG_PATH` any more, so a
  deployment that still exports one would fall back to the default  -  and for the three store
  variables that default is in-process memory, i.e. a durable event log silently becoming
  ephemeral on upgrade. Setting one *without* its replacement is now an error naming both. A
  leftover alongside a correctly-set replacement is fine: the migration has happened, and a stale
  name inherited from a container environment should not stop a working process from booting.


- **An event log written before v3.0.0 cannot be read by v3.0.0.** The envelope's `v` was a plain
  integer up to and including v3.0.0b1 and is now `{major, minor}`, which is a major bump  -  and a
  major bump means exactly this: the two are not mutually readable. Only durable stores are
  affected (`sqlite`, `postgres`, `redis`); the default `memory` store keeps nothing across a
  restart, so most callers have nothing to migrate. An affected store must be replayed into a new
  one, or read with the version that wrote it. The first read of an old event says so by name
  rather than failing as a validation error on a model you have not met.

- **`Runtime(clock=...)` and `build_runtime(clock=...)` are gone.** Both keywords stopped
  deciding anything once ADR-D11 moved timestamp assignment into the store; a caller that held
  time through either one now gets a `TypeError` instead of a run whose timestamps quietly kept
  moving. Pass the clock to the store instead: `MemoryEventStore(clock=...)`,
  `RedisEventStore(clock=...)`.

- **A client built on the openai-agents engine's own session (`agents.SQLiteSession`,
  `agents.extensions.memory.RedisSession`) that inspects or prunes its content parts must add
  `input_image`/`input_audio` to its matcher** (#161). A turn carrying an image or audio block
  now writes the SDK's own canonical part types into the session; a matcher written against the
  old raw shapes (`image_url`, `input_audio` tuples, or similar) silently stops matching, which
  for a pruning pass means the same image gets re-sent, and re-billed, on every later turn of
  that conversation instead of being dropped after the turn that needed it.

- **A program holding two `Deck` instances at once now raises instead of quietly misbehaving**
  (#204). One deck at a time is unchanged, including a deck mounted inside an existing service
  through `asgi()`. What breaks is a script that validates several projects in a loop, or a
  notebook that re-runs its `Deck.from_project()` cell: close the first (`await deck.aclose()`,
  or run it under `async with`) before constructing the next. Today those programs appear to
  work while the second deck reads the first one's bundles, so the raise is the change you want.


### Added

- **Context injection: `agentdeck.Context`, `Deck(context=T)` and `context=` on every run**
  (#166). An application value  -  a database handle, a client, whatever the code a run reaches
  needs  -  enters once at the run boundary and is delivered to any callable that *declares* it.

  **Declaring it.** Annotate a parameter `Context[T]`, whatever you name it: a tool, an
  `instructions=` callable, an agent hook (first parameter, where the SDK's own wrapper goes), or
  a workflow node alongside its `state`. `ctx.data` is the very object passed in, by reference;
  `ctx.reporter`, `ctx.run_id`, `ctx.session_id` and `await ctx.checkpoint()` come with it. A
  plain function in `tools=` is now compiled rather than rejected  -  a context-declaring tool
  cannot be pre-wrapped with `@function_tool`, since that would put the context parameter in the
  model-visible schema.

  **The model never sees it.** The context parameter is absent from the tool schema sent to the
  model, an instructions callable contributes only its return value to the prompt, and the value
  is never written to the event log.

  **Supplying it.** `deck.run(..., context=obj)` and `deck.stream(...)`, plus
  `answer(run_id, value, context=...)` and `resume(run_id, context=...)`  -  resupplied, never
  recovered, because the value is deliberately never serialized, so the caller picking a paused
  run back up is the only one who still has it.

  **Both engines, one contract.** The value travels on each engine's own runtime-context channel
   -  the SDK's `RunContextWrapper`, LangGraph's `Runtime[T]`  -  never on `configurable`, which
  keeps `thread_id`, the reporter and the stream flag exactly as before. A contract test
  parametrized over both engines pins that the two bridges deliver the same thing.

  **Checking it.** `Deck(context=MiddleContext)` declares the context *type* (the class, not an
  instance), and `build()` then checks every `Context[...]` in the catalog against it, raising
  `ContextTypeError` naming both types. It decides only what the runtime can decide  -  exact type,
  subtype, `Any`, a runtime ABC's origin, a protocol `issubclass` will rule on, a union arm by arm
   -  and defers everything else (a structural protocol, a `TypeVar`, an engine-native tool object)
  to invocation rather than guessing. A deck that declares no `context=` is unchanged: nothing is
  checked, and `run(context=...)` works exactly the same.

  **Where it does not reach**, all documented in the `Deck` reference: a context cannot cross the
  HTTP surface at all (no wire form for a live object, so a served run carries `None`); `tick()`
  takes none, so *durable + `sleep_until` + `Context[T]`* is unsupported in v3.0.0 and a timer
  resume replays with `ctx.data` set to `None`; the headless `Agent.run()` and
  `Workflow.run()`/`as_tool()` paths pass none either; and a skill is prose, not a callable, so
  there is nothing there to inject into.

- **`Deck(observers=[...])`  -  the event stream has a declared set of observers, and they start
  with the deck** (#181). An observer is any `EventSinkPort`  -  telemetry, cost accounting, audit
   -  and the Runtime fans every run out to all of them, each with its own bounded queue.
  `agentdeck.observers.Langfuse` is the one agentdeck ships.

  ```python
  from agentdeck import Deck
  from agentdeck.observers import Langfuse

  deck = Deck(agents=[booking], observers=[Langfuse(), my_audit_observer])
  async with deck:  # every observer starts here, once, before any run
      await deck.run("booking", "hello")
  ```

  Three states: `observers=None` (the default) starts the configured Langfuse observer if
  `AGENTDECK_LANGFUSE_*` names one and nothing otherwise, exactly as before; a sequence starts
  exactly those, in order, and suppresses the settings-derived one; `observers=()` starts none.

  The altitude is the point. Observers start during `__aenter__`, before the Runtime exists and
  before any run can begin  -  they are no longer assembled underneath the composition root from
  settings, nor started by whichever run happened to come first. `build()` shape-checks
  `observers=` and does nothing else: nothing is started, no telemetry client is constructed and
  no exporter contacted, so a deck with Langfuse configured still validates where Langfuse is
  unreachable. There is no `deck.observers` property, for the same reason there is no `runtime`
  or `store`.

  `Langfuse(sdk_spans=True)` adds the raw layer on top of the semantic one: OpenInference maps
  every agent, generation and tool call the Agents SDK makes, with its input and output  -  detail
  the event log does not record. **It arrives as a second, separate trace per run rather than
  nested under the first**, because nesting would need the engine to establish an OTel context and
  the engines are barred from the Langfuse SDK by design. Off by default and documented as
  unnested, so nobody meets a trace they did not ask for  -  which is what #162 was filed about.
  Correlating the two layers is #218.

- **`EventSinkPort.start()`** (#181)  -  an `async` no-op by default, called once while the Deck
  opens, before any run, and pairing with the existing `close()`. A sink that holds a client, a
  connection or a file opens it there rather than on whichever event it happens to see first.
  Additive: an existing sink that only implements `emit` is unaffected. Raising from `start()`
  refuses the open rather than leaving a deck running with an observer that silently never
  worked  -  which is what `Langfuse()` does when no keys are configured.

- **`SECURITY.md` and `CODE_OF_CONDUCT.md`** (#132). The security policy says where to report a
  vulnerability and what is in scope  -  including the two things that are deliberately *not*: a
  model-chosen tool call runs with the full privileges of the host process, and nothing is
  sandboxed. The code of conduct is the Contributor Covenant 2.1, unmodified.
- **Package classifiers**, so PyPI and every metadata reader can see what agentdeck is and which
  Pythons it supports (#132). A test keeps the classified Python versions in step with
  `requires-python`, and asserts the built metadata still names the MIT license.
- **`examples/`: two decks you can copy** (#132)  -  a chat agent with a tool, and a workflow that
  pauses for a human approval. Each is a complete project directory with a `run.py` and a README,
  and each is built by the test suite on every run, so neither can quietly stop working. The
  approval example makes no model call at all and runs offline.
- **A docs-site page on choosing a store backend**: the four independent storage decisions, one
  environment variable each, and the trap where `durable=True` parks an approval that a second
  process cannot see because the event log is still in memory.

- **`AudioBlock`** (#159): a fifth content-block kind, mirroring `ImageBlock` field-for-field
  (`media_type`, `data_b64`)  -  the same problem (opaque bytes with a MIME type), so a different
  shape would be asymmetry with no payoff. Additive/minor (`CURRENT_VERSION.minor` 0 → 1): a
  reader that predates this still parses the event and meets an audio block as `UnknownBlock`.
- **`ImageBlock` and `AudioBlock` now cap inline data at 1 MB decoded**, raising at construction
  and naming `ResourceBlock` as the by-reference alternative for anything larger (#159). Base64
  in an event lands in an append-only log and replays down every SSE connection for the life of
  that run, so a documented-only limit shipped violated; the cap is deliberately low, since
  raising it later is compatible and lowering it is not.

### Changed

- **`tools=` now takes plain functions, and compiles them** (#166)  -  reversing the guardrail
  #172 shipped, which rejected a bare callable and told you to wrap it with `@function_tool`.
  A function annotated `Context[...]` *cannot* be pre-decorated, because `@function_tool` would
  put that parameter in the schema the model sees, so the plain callable had to become the
  canonical declaration. An already-built Agents SDK tool object is still accepted, unchanged
  and passed straight through  -  it is engine-native, introspected by nothing, and carries no
  portability guarantee. The one thing still refused at `build()` is a callable whose signature
  cannot be read (a decorator that dropped `functools.wraps` is the usual cause): there is no
  honest schema to show the model, and no way to tell "declares no context" from "could not
  look", so compiling it would silently drop an argument the function needs.

- **Breaking:** **`build_runtime(sinks=…)` no longer defaults to the configured telemetry**
  (#181, #162). It defaults to no sinks at all, and the composition *function* reads no Langfuse
  keys  -  an observer opens a live client, so when one is constructed is a lifecycle decision, and
  doing it while a Runtime was assembled is what #162's first defect was. Resolving from settings
  moved to `agentdeck.composition.resolve_observers()`, which `Deck.__aenter__` calls (and then
  `start()`s the result) as it opens; a `Deck` behaves exactly as before. A caller that hand-wired
  `build_runtime(...)` and relied on it picking Langfuse up from the environment now gets an
  untraced Runtime  -  pass the observers yourself, remembering to `await observer.start()` first,
  or open a `Deck`. `sinks=None` is no longer accepted; the parameter is a plain sequence.

- **The README now says what agentdeck is before it shows any code** (#132): what it is, who it
  is for, what it deliberately does not do, and how it divides work with the OpenAI Agents SDK
  and LangGraph. It links `CONTRIBUTING.md`, `SECURITY.md` and the docs site rather than
  restating them, and its install pin, its Python example and its docs links are all checked by
  the test suite.
- **Breaking:** **one `Deck` per process, enforced at construction** (#204). Constructing a
  second `Deck` while the first is still live now raises `ConfigError` naming both projects;
  before, it succeeded and the second deck silently inherited the first one's bundles, because
  every project mounts under a single module alias and MCP servers are registered process-wide.
  A deck built but never opened holds the process just the same  -  the claim is taken at
  construction and released by `aclose()`. Two decks side by side is a capability we intend to
  add (#213); until then a deck per tenant is a process per tenant, which is what the code has
  in fact always done.

- **Breaking: one env var per infrastructure decision, not a `_BACKEND`/`_URL` pair that can
  disagree** (#155). `AGENTDECK_EVENTS_BACKEND=postgres` with `AGENTDECK_EVENTS_URL=redis://...`
  used to boot clean and fail on the first event of the first run; the URL's own scheme now
  *is* the backend, so that mismatch cannot be expressed at all, not merely rejected. No
  deprecation shim  -  this is the one breaking-release window where renaming is free  -  so an old
  name is simply never looked up: `_BACKEND` had no field to bind to at all once the pair
  collapsed to one, and the renamed field (`url`) maps to a different literal env var name than
  the one it replaced, so setting the old name alongside the new one has no effect either way:

  | Old | New |
  | --- | --- |
  | `AGENTDECK_EVENTS_BACKEND` + `AGENTDECK_EVENTS_URL` | `AGENTDECK_EVENTS=memory://` / `sqlite://<path>` / `redis://<url>` / `rediss://<url>` / `postgresql://<dsn>` |
  | `AGENTDECK_CONTROL_BACKEND` + `AGENTDECK_CONTROL_URL` | `AGENTDECK_CONTROL=memory://` / `sqlite://<path>` |
  | `AGENTDECK_CHECKPOINT_BACKEND` + `AGENTDECK_CHECKPOINT_URL` | `AGENTDECK_CHECKPOINT=memory://` / `sqlite://<path>` (default: `sqlite://.agentdeck/checkpoints.sqlite3`) / `postgresql://<dsn>` |
  | `AGENTDECK_SESSION_REDIS_URL` | `AGENTDECK_SESSION=redis://<url>` |
  | `APP_CONFIG_PATH` | `AGENTDECK_CONFIG_PATH`  -  unprefixed and generic; any other tool claiming that name silently repointed agentdeck's config |

  Selecting `memory://` for `AGENTDECK_EVENTS`/`AGENTDECK_CONTROL` now logs one WARNING at
  composition time (`resolve_event_store`/`resolve_control_port`) naming what it costs  -  no
  cross-process signals, no log after a restart  -  instead of that being discoverable only in
  production. `agentdeck-serve`'s own startup-time version of this same warning is gone; the
  composition-time one covers it and every other entry point besides.
- **`Runtime.__init__` no longer reads settings** (#155): the five-parameter, ambient-config-free
  constructor now has a sixth, `stale_run_after`, defaulted to one hour with no `get_settings()`
  call at all. `build_runtime` resolves `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS` and passes it
  in, the same as its other adapters  -  an embedder constructing `Runtime(...)` directly, bypassing
  `build_runtime`, now gets the literal one-hour default rather than whatever the process's
  settings happened to say.
- **The prefix rule is written down** (#155): `docs/coding-standards.md` §9 and `CLAUDE.md` now
  state that `OPENAI_*`/`TAVILY_*` keep their own names because the respective SDKs read them
  natively  -  the only exceptions to `AGENTDECK_*`, not an open pattern.
- **The openai-agents engine accepts image and audio input, not just text** (#161).
  `TextBlock`/`ImageBlock`/`AudioBlock` map onto the SDK's own canonical multimodal input parts
  (`input_text`/`input_image`/`input_audio`), which the SDK's chat-completions converter already
  accepts and maps down for either API path  -  agentdeck writes no converter of its own. An
  all-text turn still sends the identical joined string it always has; only a turn that actually
  carries media takes the new shape. `ResourceBlock`, `DataBlock`, and any block a newer writer
  invented still raise `ConfigError`, naming the block kind and the engine, never silently
  dropped  -  and an `AudioBlock` under `use_responses=True` raises naming the Responses API,
  which has no audio input member at this `openai-agents==0.17.0` pin; `use_responses=False`
  (chat-completions) accepts it. Output is unchanged and stays text/data only: nothing on this
  path produces an image or audio block.

- **The event envelope's `v` is now a `{major, minor}` object, not an integer** (#156). `major`
  is what a reader must already understand to parse the envelope at all  -  `Event` refuses one it
  does not carry, even for a kind it has never seen, because a major bump can move or remove
  envelope fields the unknown-kind fallback never checks. `minor` records an additive change (a
  new kind, a new optional field) that an old reader already tolerates by construction and never
  needs to consult. This is an intentional wire break: a reader built against the previous
  scalar `v` cannot parse an event this tree writes.

- **Breaking: `Runtime.__init__` and `build_runtime` no longer accept `clock`** (#158). ADR-D11
  moved timestamp assignment into the store, so the keyword has decided nothing since #154,
  which made it inert and warn rather than remove it outright; a caller still passing it now
  gets a `TypeError` instead of a silently-ignored no-op. Holding time still works at the seam
  that owns the clock  -  `MemoryEventStore(clock=...)`, `RedisEventStore(clock=...)`  -  which is
  unaffected by this change.

### Removed

- **`agentdeck.runtime.observability` is gone** (#181, #162), and with it `init_observability`,
  `trace_run`, `RunTrace`, `degrade_export_quietly` and the `_should_export_span` filter. It was
  a second place tracing was assembled, below the composition root and started by the first run
   -  the source of both defects fixed below. Traces are rendered from the canonical event stream
  by `agentdeck.adapters.telemetry.langfuse`, which was already the design of record; what this
  removes is the parallel mechanism. A direct `Workflow.run()` or `Agent`-node turn, which
  bypasses the event stream the same way it bypasses the event log, is therefore no longer
  traced  -  run it through a `Deck` to trace it.

- **The sandbox scaffolding is gone** (#71). Sandboxing left v3 by ruling (#163 stays open as
  the design issue), and the tree was carrying a port with no consumer, an adapter with no
  caller and spec classes nothing constructed. Deleted: `agentdeck.core.ports.sandbox` in full
  (`SandboxPort`, `ExecResult`, `bind_sandbox`, `current_sandbox`, `require_sandbox`, plus the
  `SandboxPort`/`ExecResult` re-exports from `agentdeck.core.ports`); `agentdeck.adapters.caps`
  in full (`UnixSandbox`, `open_sandbox`, `input_file_targets`); `agentdeck.authoring.capabilities`
  in full (`CapabilitiesSpec`, `ShellSpec`, `FilesystemSpec`, `MemorySpec`, `CompactionSpec`);
  and `agentdeck.runtime.capture.CAPTURE_ENV`, whose only reader was the deleted adapter
  (`Capture` and `CaptureActor` stay  -  the tracer still uses them). Re-adding a designed port
  later is additive, so nothing here is a one-way door.
- **`LoadFileNode` now refuses a relative path** (#71) instead of resolving it through the
  sandbox. That branch could only ever raise  -  nothing in v3 opened a sandbox for it to find  -
  so the node raises the refusal itself, still a `RuntimeError`, with a message that names the
  absolute path it wants. It deliberately does *not* fall back to the process working
  directory: quietly reading the host filesystem for a path a model influenced is the widening
  the sandbox existed to prevent.
- **`agentdeck.runtime.observability.sandbox_trace_env()` is gone** (#71): it built the
  `LANGFUSE_*`/`TRACEPARENT` env for a sandboxed skill subprocess, and had no callers left once
  sandboxing left v3. Same finding as `Settings.sandbox_env()` below, which #155 took early.
- **`agentdeck.surfaces.serve.compat.resume_result()` is gone** (#71): the v1 resume endpoint
  answers through `Deck.answer()` and has not gone through this helper since the v3 cutover.
  The v1 resume wire format is unchanged  -  it is covered by the golden replay, which is
  byte-identical.
- **`LangfuseSettings.host` and its `endpoint` property are gone** (#155): a pre-4.x
  compatibility alias for the Langfuse endpoint, with no reason to survive a major version.
  `base_url` is the only endpoint field now, and it carries `host`'s old default
  (`http://localhost:3000`) so an unconfigured deployment's effective endpoint is unchanged.
- **`Settings.sandbox_env()` and the unbounded `SKILL_*` env namespace are gone** (#155).
  Sandboxing left v3 in #163; `SkillExecutor`, `sandbox_env()`'s only caller, was already
  deleted in #164, so this was a deletion rather than the `AGENTDECK_SKILL_*` rename the issue
  originally proposed. `SkillsSettings` and the `skill:` `config.yaml` section go with it.
- **`check_contiguous`/`check_terminal` are no longer part of `agentdeck.core`** (#156). Neither
  was read by a production path  -  `seq` contiguity follows from how the store assigns it, and
  the one-terminal-event-last invariant is enforced by `Runtime.run`/`resume` stopping the read
  loop at a terminal payload  -  so keeping them in the schema module read as contract they were
  never part of. Embedders who imported them for their own log-auditing should inline the same
  two checks (each a few lines over a `list[Event]`) locally.

- **A `durable=True` workflow used as an agent tool now fails `build()`** (#193) instead of
  raising the first time a model calls it. `Workflow.as_tool()` invokes `run(args)` with no
  `thread_id`, which a durable workflow requires to load and persist its checkpoint, so
  `Agent(tools=[durable_workflow])` built clean and then threw mid-turn. The error names the
  agent and the workflow and points at `durable=False`, or calling it as a root invocable via
  `deck.run()` where a session can be supplied. Giving a tool-invoked workflow a thread of its
  own remains an open design question.


### Removed

- **The `openai_agents.structured_output` `custom` event** (#105). #101 gave
  `RunCompleted.output` a `DataBlock` for an `output_type` agent's validated result; the
  `custom` event carried the same value the older way and was kept only because retiring it
  inside #101 would have meant editing an open PR's files. `Deck.run()`'s `TurnResult.output`
  and v1's chat endpoints now read the `DataBlock` straight off `run.completed`  -  their
  response bodies are unchanged, but a canonical event stream (`Deck.stream()`, the event
  store) for a structured-output run now has one fewer event.


### Fixed

- **A resumed run silently lost its application context** (#166). `resume()` and `resume_run()`
  minted a fresh `RunContext` with no `data=` at all, so a run paused or interrupted with a
  `context=` came back with `None`  -  and because the value is never serialized, nothing in the
  log could be compared against what should have been there. A callable written defensively as
  `if ctx.data:` would have returned a plausible wrong answer with no error anywhere. Only ever
  reachable on this development line, since `run(context=)` and this landed in the same release.

- **The Langfuse span filter never applied, because the client that carried it was never the
  one that ran** (#162). `build_runtime()` constructed a client from settings while the Runtime
  was assembled, with no `should_export_span`; `init_observability()` constructed a second one
  later, on the first run, with the filter. Confirmed against langfuse 4.14.1: the SDK caches
  one `LangfuseResourceManager` per public key and returns it from every later `Langfuse(...)`,
  discarding that call's arguments  -  so the second construction was not a second client and its
  filter was silently dropped. There is one construction point now, reached only when a deck
  opens, and the filter is gone with the `sandbox.*` spans it existed to drop (sandboxing left
  v3 in #163/#71, so nothing emits them any more).

- **An agent turn exported one trace too many with Langfuse on** (#162). `init_observability()`
  installed the OpenInference `OpenAIAgentsInstrumentor`, but the `trace_run` root that used to
  group those spans and carry `session.id` was gone  -  so the SDK's spans exported as a second,
  sessionless tree beside the sink's own trace. Nothing instruments the Agents SDK any more and
  the direct-call runners open no observations, so one run is one trace again.

- `Deck.from_project()` reading a *previous* project's bundle files (#204). Mounting a project
  rebound the module alias but left its already-imported submodules cached, so a second project
  whose bundle directory happened to share a name  -  two `agents/greeter/`  -  got the first one's
  module back from `sys.modules`. Stale submodules are now evicted on mount, which also means
  editing a bundle and rebuilding in the same process picks the edit up.

- **Docs: `/reference` pages corrected against the current API** (#192). `/reference` no
  longer claims MCP is covered by the Workflows page  -  it now points at `/reference/deck`
  (`Deck(mcp=...)`) and `/concepts/agents` (MCP status at build vs. open), where the actual
  coverage lives. `/reference/deck` no longer says `stream()` assembles a `TurnResult`  -  only
  `run()` does; `stream()` yields `AsyncGenerator[Event]`, as the page's own table already
  said two sections up. `/reference/deck` also now documents the timer/event-log limit:
  `due_resumes()`/`tick()` still list due timers off each workflow's own checkpointer, but
  `tick()`'s resume now goes through the Runtime whenever a logged run matches (#191)  -
  closing that run's log entry and freeing its session claim  -  falling back to the direct
  checkpointer resume only for a thread with no logged run.
- `AGENTDECK_LANGFUSE_SERVICE_NAME`'s description no longer says "host process and sandboxed
  skills"  -  sandboxed skills were removed in #164; the generated `/reference/settings` page is
  regenerated to match.
- `tests/test_docs_site.py::test_pinned_install_versions_match_the_package_version` now also
  requires every fenced `pip install`/`uv pip install`/`uv add` line naming `agentdeck` on the
  docs site to carry a version pin matching `pyproject.toml`, not just validating pins that
  already exist  -  closing the gap that let `/guides/serve-over-http` ship an unpinned
  `agentdeck[serve]` install example.
- **Three docs pages corrected against the current v3 code (#192).** `/guides/human-approval`'s
  cross-process example now says what it actually needs: `durable=True` makes the checkpointer
  file-backed, but `pending()`/`answer()` read the event store instead, so a second process only
  sees a paused run if `AGENTDECK_EVENTS` is pointed at a shared backend too  -  the
  in-process default is not enough on its own. `/guides/serve-over-http`'s install line now pins
  a version (`git+...@v3.0.0b1`), matching `getting-started.mdx` instead of an unqualified
  `agentdeck[serve]`. `/operating/pause-resume-cancel` no longer describes the `503 no control
  backend configured` response as a deployment state operators can hit: `resolve_control_port()`
  always wires a real `ControlPort` for `Deck`/`agentdeck-serve`, so that response is reachable
  only by an embedder constructing a bare `Runtime` outside `Deck`.
- **Seven more docs pages corrected against the current v3 code (#192).** `/` no longer calls
  skills Python definitions (they are `SKILL.md` directories) or claims `session_id` survives a
  restart by default (it needs `AGENTDECK_SESSION`). `/concepts` and
  `/concepts/runs-and-the-event-log` no longer describe SQLite's cross-process story as "shared
  memory"  -  it is a shared file, openable by several processes on one machine, not across
  machines. `/concepts/agents` describes MCP status as it works today: `build()` stays
  network-free, and status is re-resolved when the `Deck` opens (`__aenter__`, right after
  `MCPLifecycle.startup`), not "dropped at build time". `/concepts/protocols-and-surfaces` no
  longer says an HTTP handler builds a `RunContext`  -  the `Runtime` mints it for every caller.
  `/concepts/run-control` no longer recommends `ctx.idempotency_key`, a field `RunContext` does
  not have. `/concepts/runs-and-the-event-log` and `/concepts/workflows` now document that
  `Deck.due_resumes()`/`Deck.tick()`'s *listing* of due timers reads each workflow's own
  checkpointer, not the event log, for the same #22-driven reason as the `Deck.tick()` fix
  below.
- **`Deck.tick()`** no longer leaves a ghost `WAITING_HUMAN` run in the event log when it
  resumes a timer-paused thread that a `Deck.run()`/HTTP call parked (#120): it now resumes
  such a thread through the Runtime, the same as `Deck.answer()` already did, so the run's
  log entry closes and its session claim releases instead of blocking a fresh run on the
  same thread until `stale_run_after` expires. `Deck.due_resumes()`/`Deck.tick()`'s *listing*
  is unchanged  -  still each workflow's own checkpointer, not the event log, because by
  default the checkpoint backend is durable (`sqlite`) while the event store is not
  (`memory`), and #22's guarantee that a due timer survives a process restart depends on
  that. A thread with no logged run at all (parked by calling a durable `Workflow`'s own
  `run`/`resume` directly  -  a deliberately log-free path, out of scope here) still resumes
  the way it always did.
- **A fan-out workflow whose one branch interrupts while a sibling completes now reports the
  sibling's `node_update` before the `interrupt`, instead of silently dropping it** (#122). The
  langgraph engine reports a pause as soon as the interrupting branch asks for one, not once
  every branch in the same step has finished; a slower sibling's completion used to arrive on
  the drained tail of that call and be discarded there, even though its write had already
  landed in the engine's own checkpoint  -  the checkpoint and the canonical event log disagreed
  about what had run. The pause is still reported last, and a run with a suspended branch never
  reports `done`: `RunStatus.status_of` already derived `waiting_human`, non-terminal, from
  `run.interrupted` alone, so nothing there needed to change. Pinned with a new golden fixture,
  `FanoutInterruptFlow`, streamed and non-streamed.
- **`UnknownBlock` now dumps its original payload verbatim instead of nesting it under
  `raw_block`** (#200). Parsing an unfamiliar content block and dumping it straight back used to
  produce `{"type": ..., "raw_block": {...}}` rather than the block that was actually read  -
  harmless today since nothing relays events, but a relay (#129's protocol adapters) would have
  nested the payload one level deeper on every hop it passed through, silently. Known block
  kinds (`text`/`image`/`resource`/`data`/`audio`) are unaffected.

## [3.0.0b1] - 2026-08-10

First public beta of v3. `Deck` replaces `App` as the single composition root;
v1's `agents/`, `workflows/` and `app.py` are gone with no re-export shim.


### Removed

- **`Deck(context=...)`** (#182): the parameter was accepted at construction and then refused  -
  `run`/`stream`/`resume` raised on any non-`None` value, because nothing injects a context yet.
  A constructor parameter nobody can use is a false promise, so it is gone until `Context[T]`
  lands (#166), at which point it returns additively. No working code passed it.


v2.0.0 shipped with an explicit compatibility promise for v1's Python API; this entry
starts retiring it, one PR at a time (#137). First slice: `App`'s turn-starting methods
now play on the same Runtime the HTTP surface always has, so a Python caller's turn is
recorded  -  every bit of it  -  in the same event log a running server would show, instead
of vanishing the moment the call returns.

### Changed

- The Runtime now plays every turn on the real engine adapters  -  `OpenAIAgentsEngine` and
  `LangGraphEngine`  -  instead of the v1 compatibility subclasses that stood in for them, and
  `agentdeck.v1bridge` is removed. What a run is configured with (model provider, CA bundle,
  temperature, turn and token caps, workflow name) is now resolved at the composition root
  and handed to the adapter, so a caller can wire a different endpoint without touching
  process state. Behavior is unchanged: the same settings resolve to the same run config,
  pinned field by field by `tests/test_run_config_parity.py`.
- A workflow's `durable = True` now travels to the engine on its spec, and the configured
  checkpointer is built at the first durable run rather than when a Runtime is assembled  -
  so naming a `sqlite`/`postgres` backend still costs a project that only chats nothing, and
  the `[durability]` extra stays optional.
- **Breaking:** `App.session_for(session_id)` now returns the engine's own session for that
  id, keyed by tenant (`local:<session_id>`) the way every other entry point already keys it.
  One conversation is now one conversation whether the turn arrived through `App.chat` or
  through HTTP  -  and a Redis-backed deployment gets its sessions on the Runtime path, which
  it silently did not before. Conversations written under an unprefixed Redis key by an
  earlier version are not read back; start them fresh or re-key them.
- **Breaking:** `agentdeck.runtime` no longer re-exports `OpenAISettings`, `PluginRegistry`,
  `RunnerSettings`, `Settings`, `SkillsSettings`, `Workspace`, `get_settings` or
  `reset_settings_cache`. Import each from the module that defines it, e.g.
  `from agentdeck.runtime.settings import get_settings`; nothing about how any of them
  behaves changed. Part of the v3 cutover's prep to put a package-wide "`agentdeck.runtime`
  stays adapter-free" import-linter contract on the whole package, rather than just today's
  `service`/`dispatch` carve-out (`docs/delivery/plan-v2-cutover.md`).
- **Breaking:** `agentdeck.runtime.sessions` and `agentdeck.runtime.checkpointer` (forwarders
  left behind when `SessionFactory` and `resolve_checkpointer` relocated to their engine
  adapters) are removed. Import `SessionFactory` from
  `agentdeck.adapters.engines.openai_agents.sessions` and `resolve_checkpointer` from
  `agentdeck.adapters.engines.langgraph.checkpointer`. `agentdeck.agents.mcp.lifecycle`, the
  equivalent forwarder for `MCPLifecycle`, is removed the same way  -  import it from
  `agentdeck.adapters.tools.mcp.lifecycle`.
- **Breaking:** `OpenAISettings.tracing_api_key` (`OPENAI_TRACING_API_KEY`) is removed  -  it
  was never read anywhere in the codebase.
- **Breaking:** `agentdeck.runtime.workspace.runtime_capture` and `current_capture` are
  removed. Nothing ever bound the ContextVar behind them, so `current_capture()` always
  answered `None`; a run's identity now reaches telemetry through the event envelope.
- **Breaking:** the sandbox is a port. `agentdeck.runtime.workspace` and its `Workspace` class
  are removed, replaced by `SandboxPort` (`agentdeck.core.ports.sandbox`) and the
  `agentdeck.adapters.caps.sandbox` adapter that implements it. Open one with
  `async with open_sandbox(...) as sandbox:` instead of `Workspace.open(...)`, and reach the
  ambient one with `require_sandbox()` instead of `Workspace.require()`. The port carries only
  what callers actually use  -  `read_text`, `write_bytes`, `mount_dir`, `exec`  -  so
  `write_text`, `write_output`, `read_output`, `output_path` and `OUTPUT_FILES_DIR` are gone
  (nothing in the package or its tests called them), `exec` no longer takes `shell`, and
  mounting a host directory now grants access to it in the same call rather than requiring a
  separate `extra_path_grants=`. `materialize()` and `input_file_entries()`, which took the
  Agents SDK's own manifest-entry types, are replaced by `mount_dir()` and
  `input_file_targets()`; `Workspace.open`'s unused `capture`, `client` and `client_factory`
  arguments are gone. A sandbox's environment is unchanged, including the rule that
  host-supplied trace carriers win over a caller's stale copy.
- An agent turn no longer opens its Langfuse observation inside the engine  -  the sink builds
  the run's trace from its events instead, so a turn is reported once rather than twice.
- **Breaking:** `App.run_agent` and `App.chat` no longer return the OpenAI Agents SDK's
  `RunResult`. Both return a `TurnResult` (`output`, `usage`, `run_id`, `session_id`) built
  from the run's own `run.completed` event. Update `result.final_output` to `result.output`;
  `result.usage` is now a `Usage` model (`.input_tokens` / `.output_tokens` / `.usd`), not a
  dict. A validated `output_type` result now arrives as plain JSON data (a `dict`/`list`),
  not the SDK's validated model instance.
- **Breaking:** `App.chat_stream` no longer yields raw text deltas followed by a
  `StreamDone` sentinel. It yields the run's own canonical `Event`s (`text.delta` per token,
  `run.completed` last, `run.failed` in place of both if the turn raises).
- **Breaking:** `run_agent`, `chat`, `chat_stream`, `run_workflow` and `resume_workflow` no
  longer take arbitrary `**runner_options`. Configure a run on the agent/workflow class or
  through settings instead of per call.
- `App.run_workflow` and `App.resume_workflow` keep their return shapes (the final state, or
  an `InterruptResult` while paused) but now play on the Runtime instead of driving the
  compiled graph directly  -  every workflow turn is recorded, and a second concurrent call on
  the same `thread_id` now raises `SessionBusyError` instead of racing the first. A workflow
  with no `state` argument keeps defaulting to no updates.
- `App.run_agent`, `App.chat`, `App.chat_stream`, `App.run_workflow` and
  `App.resume_workflow` compose the Runtime on first use (calling `load()` themselves) if
  `App.load()` was never called by hand.
- **Breaking:** run control's vocabulary and safe point moved out of the ports package to
  `agentdeck.core.control`  -  `Signal`, `ControlSignal`, `Gate`, `ControlSignalled`,
  `RunCancelledError`, `RunPausedError` and `CONTROL_POLL_INTERVAL`. Only `ControlPort`, the
  transport an adapter implements, stays in `agentdeck.core.ports`. Import from
  `agentdeck.core.control` instead; nothing about how control behaves changed.
- `InvocableSpec` and `ToolSet` now raise on a keyword they don't have, instead of dropping it.
  Both are built in-process, so an unknown keyword is a typo  -  and a dropped `tools=` used to
  yield an empty `ToolSet`, degrading a run exactly like an unreachable tool source. Event and
  content payloads keep ignoring unknown fields: they are parsed off a wire, where a field a
  newer writer added has to land rather than raise.

### Added

- **Langfuse now traces workflow runs.** `build_runtime` registers the Langfuse sink itself
  when `AGENTDECK_LANGFUSE_PUBLIC_KEY` and `AGENTDECK_LANGFUSE_SECRET_KEY` are both set, so
  every run played through a Runtime  -  workflow as well as agent  -  becomes a trace built from
  the run's own events, carrying its session id, its principal as the Langfuse user, its
  nodes, its tool calls and its token usage. Workflow runs previously produced either no trace
  or an anonymous one. Nothing is registered and the Langfuse SDK is never imported without
  both keys, so the `[observability]` extra stays optional. Pass `sinks=()` to `build_runtime`
  to opt out.
- **`App.store`**: the event log every recorded turn appends to. Read a turn back with
  `await app.store.read(log_key, ctx)`, where `log_key` is a `TurnResult`'s `session_id` (or
  `run_id`, for a session-less run).
- Docs: `reference/settings.mdx` and `reference/cli.mdx` are now generated from the code  -
  every `AGENTDECK_*` (and `OPENAI_*`/`TAVILY_*`/`SKILL_*`) setting and the `agentdeck` CLI's
  own `--help` output  -  and verified against the code on every `make check`, so the published
  pages cannot drift from what the package actually does (#133).

### Changed

- Every `LayeredSettings` field in `agentdeck/runtime/settings.py` now carries a
  `Field(description=...)`, the source the new generated settings reference renders from.
- **Breaking:** `parse_event` is removed. `Event.model_validate(data)` does the same job  -
  an unfamiliar `kind` still lands as `UnknownEvent` rather than raising  -  so the forward-
  compatibility promise is now a property of the type instead of something a reader has to
  remember to call. Replace `parse_event(row)` with `Event.model_validate(row)`.
- Two `kind` values that disagree are refused instead of silently relabelled. When the
  envelope's `kind` was one this version didn't know, the payload's own claim used to be
  overwritten with the envelope's and buried in `raw_payload`, so a row was accepted under a
  name it never carried. Only reachable from rows this package didn't write.
- `Event.kind` and `UnknownEvent.kind` now have to look like a kind (`run.started`,
  `a2a.task.started`); `""`, `"Run Started"` and `"run..started"` were accepted before. A
  shape, not a fixed set  -  an unfamiliar kind from a newer writer still parses.
- Every free-form JSON field holds only what a store hands back unchanged:
  `NodeUpdated.state_patch`, `ToolCallStarted.args`, `RunInterrupted.payload`, `Custom.data`,
  `UnknownEvent.raw_payload` and `UnknownBlock.raw_block`. All six were `dict[str, Any]`, so a
  `NaN` reached the log as `null`, a set as a list and a datetime as a string  -  the divergence
  `DataBlock` has always refused. They now carry the same `JsonData` type `DataBlock` does.
  The two `raw_*` fields matter most: `UnknownEvent` and `UnknownBlock` exist so this version
  survives a newer writer, which they cannot do while free to alter that writer's data on the
  way through. Every engine adapter already sanitized before constructing these, so nothing the
  package produces changes; a caller building one by hand from non-JSON values now gets a
  `ValidationError`.
- The cost and budget fields validate like the token counts always did: `Usage.usd`,
  `Budget.max_usd` and `Budget.max_tokens` reject negatives, and the two dollar fields also
  reject `NaN` and `±Infinity`. Those have no JSON literal, so they serialized as `null`  -  a
  consumer read *no cost* where the producer wrote nonsense. Nothing in the package produced
  such a value, so this closes a trap rather than fixing a live bug; a caller that built a
  `Usage` or `Budget` by hand with one now gets a `ValidationError` at construction. No
  serialized shape changed.
- `POST /v2/invocables/{name}/chat` answers **422** to an empty `session_id` instead of
  accepting it. A run's log key is `session_id or run_id`, so `""` was not an error anywhere
  downstream  -  it quietly gave the turn a private log, and the caller's next message found no
  history with nothing saying why. v1's `POST /agents/{name}/chat` is unchanged.
- **Breaking (for anyone who implemented `EventStorePort`):** the store now assigns `seq` and
  `ts`, in the same indivisible step that persists the event, and the port went from eight
  methods to seven. `append(log_key, payloads, ctx, origin)` takes payload objects instead of
  finished `Event`s and **returns** the events it wrote; `claim_start` takes the opening
  `RunStarted` plus `origin` and returns `(SessionClaim, Event | None)`; `claim_resume` takes
  the `RunResumed` plus `origin` and returns the event it wrote or `None` instead of a bool;
  `last_seq` is **removed**, having existed only to recover a counter nothing holds any more.
  `SessionClaim.overridden` now carries each abandoned run's last `Event` rather than its id,
  and `claim_start`'s cutoff is a `stale_after: timedelta` rather than a `stale_before:
  datetime`  -  the store owns the clock, so only it can subtract from its own now. The four
  bundled stores are unchanged in behavior; a store built outside this package needs porting.
  `read`, `read_run`, `list_runs` and `run_status` are untouched.
- `Runtime(clock=...)` and `build_runtime(clock=...)` no longer decide anything. Every event's
  `ts` is assigned by the store, so a caller that wants to hold time still builds the store
  with a clock  -  `MemoryEventStore(clock=...)`, `RedisEventStore(clock=...)`  -  while the SQLite
  and Postgres stores read their own backend's clock, so N workers sharing one database compare
  one clock instead of N. Both keywords are still accepted and do nothing.

### Deprecated

- `Runtime(clock=...)` and `build_runtime(clock=...)` now raise a `DeprecationWarning` when
  passed explicitly. They are inert (see above), and a keyword that silently ignores a frozen
  clock is how a caller ends up asserting against wall time believing it held time still.
  Removal is tracked in #158; pass the clock to the store instead.

### Fixed

- A log no longer carries a permanent gap after a dropped report or a transient append failure.
  The `seq` was taken before the write and stayed spent when the write failed, so a run that
  otherwise completed cleanly left a hole in its sequence  -  and a consumer seeing that hole
  could not tell "an event was lost in transit, refetch it" from "this gap is permanent and
  refetching will never converge". A number is now allocated and persisted together, so it
  cannot be allocated and not persisted, and `check_contiguous` is the loss check it is
  documented to be.

### Known limits

- `Deck.tick()` and `Deck.due_resumes()` still resume a paused workflow through its LangGraph
  checkpointer rather than the Runtime (#120), so a timer-paused run started through `run` is
  resumed outside the log: its own log entry stays `WAITING_HUMAN` until `stale_run_after`
  reclaims it.

### Changed

- **Breaking (v3.0.0 in progress, #164):** a bundle's `agent.py`/`workflow.py` now builds an
  `Agent(...)`/`Workflow(...)` instance from `agentdeck.authoring` instead of subclassing
  `BaseAgent`/`BaseWorkflow`. `agentdeck.agents` and `agentdeck.workflows` are removed;
  `LoadFileNode` and `AgentNode` move to `agentdeck.authoring.nodes`, the capability mixins
  move to `agentdeck.authoring.capabilities`, and `web_search` moves to
  `agentdeck.authoring.web_search`. `Agent.mcp` replaces `BaseAgent.mcp_server_names`.
  Subagent delegation (`BaseAgent.subagents`) and the sandboxed agent path
  (`BaseSandboxAgent`, `SandboxAgentNode`) are dropped rather than ported  -  sandboxing is
  disabled and tracked separately (#163); a workflow that needs a sub-run composes another
  `Agent`/`Workflow` and calls it directly. `SkillNode` is removed along with it  -  a workflow
  invokes a skill through its executor, not a graph node. This is the first slice of the
  `Deck` composition API (#164); `App` still serves `.agentdeck/` projects unchanged for now
  and is removed once `Deck` replaces it later in the same effort.
- **Breaking (v3.0.0 in progress, #164): a skill is disclosed into an agent's own execution, not
  run as a program.** `agentdeck.skills.Skills` is the new capability object  -  one or more root
  directories, scanned direct-child only (`<root>/<name>/SKILL.md`, never recursive) and merged
  into one name-keyed registry at `build()`; a name declared under two roots fails naming both
  paths. `build()` also enforces what a permissive scan would not: a `SKILL.md`'s frontmatter
  `name` must match its directory name, and it must declare a non-empty `description`  -  pass
  `validate=False` for the old lenient fallback. `agentdeck.skills.SkillRegistry` (single root,
  no validation) is removed; `App.skills` is now a `Skills` over `.agentdeck/skills`. The
  executable skill model  -  `SkillExecutor`, `SkillOutputSchema`, the `skill_runtime` subprocess
  package, and the sandboxed `scripts/run.py` contract they wrapped  -  is removed outright rather
  than ported: nothing in the package or its tests used it, and sandboxing is disabled and
  tracked separately (#163). An activated skill now reaches an agent as an instructions-block
  (name + description per declared skill) plus a `load_skill(name)` tool that reads the full
  `SKILL.md` body on demand, scoped to that agent's own `skills=[...]`; `SkillError` (the base
  exception a workflow node may still raise itself) is unaffected.
- **Breaking (v3.0.0 in progress, #164): named MCP servers move to a `.mcp.json` file,
  reversing #78.** `agentdeck.mcp.MCP` parses one file's `mcpServers` object (the shape Claude
  Code already uses) and validates every entry; `Agent.mcp` resolves names against it through
  `Deck`'s own `build()`. The `mcp:` section of `config.yaml`/`config.default.yaml` and the
  `AGENTDECK_MCP_SERVERS` env var are removed  -  `McpSettings` is gone, and `McpServerSettings`
  (the per-server shape, unchanged) moves from `agentdeck.runtime.settings` to `agentdeck.mcp`.
  `App` now reads `.mcp.json` from the project root (a sibling of `.agentdeck/`, not inside it)
  when present, and boots with no servers when it is absent  -  the same fail-open behavior an
  empty `mcp.servers` always had. `MCPLifecycle.configure`/`.startup` no longer fall back to
  process settings; a caller now always hands them the config to use.

### Added

- **`agentdeck.deck.Deck`**, the v3 composition root (#164): `Deck(agents=..., workflows=...,
  skills=..., mcp=..., context=...)` builds and runs a catalog from Python objects with no
  `.agentdeck/` project on disk, and `Deck.from_project(path)` discovers the same four arguments
  from today's directory layout  -  both end at the same constructor, so there is one catalog
  mechanism either way. Lifecycle is `NEW -> build() -> BUILT -> (async with) -> OPEN -> CLOSED`:
  `build()` validates every name a catalog references (an unknown skill, MCP server, or
  workflow-as-tool name; an agent and a workflow sharing a root name) and compiles every
  agent/workflow to an `InvocableSpec`, reading only local files  -  no network call, no MCP
  server started, and idempotent, so it doubles as a CI check. `deck.agents`/`deck.workflows`
  are read-only mappings once built; `run`/`stream`/`pause`/`cancel`/`resume`/`status`/`pending`
  require an opened deck (`async with deck: ...`), which is also what starts every configured
  MCP server and composes the Runtime. Closing tears down only what a Deck itself instantiated
   -  an `MCP(...)` it holds, always, and an event store it built from settings  -  never a store
  handed in through the (private, test-only) `_store=` seam. `Deck.run`/`.stream`/`.resume`
  accept `context=` for forward compatibility but raise on a non-`None` value: full `Context[T]`
  injection is its own, larger effort (`docs/delivery/plan-context-injection.md`) and is not
  wired into this slice. `App` is unchanged and still serves `.agentdeck/` projects; `Deck`
  replaces it as the documented entry point once `agentdeck serve` and the CLI move onto it.
- **`Deck.asgi()`**: the ASGI app `agentdeck serve` runs, built from a `Deck` instead of an
  `App` (#164). `agentdeck.serve.create_app()`  -  the console script's entry point, and every
  existing test's  -  is now `Deck.from_project().asgi()`; the lifespan opens and closes that same
  `Deck` (`async with deck: ...`) instead of building an `App` of its own. The HTTP contract has
  not moved: every route, status code and event-stream shape is identical, which
  `tests/golden/`'s byte-for-byte snapshots confirm. `GET /health`'s inventory now reads off
  `Deck.agents`/`.workflows`/`.skills` directly rather than a cached dict, with the same three
  keys in the same shape.
- **`Deck.answer(run_id, value)`** (#164): answers the interrupt the run named by `run_id` is
  paused on, in place of `resume_workflow(name, thread_id, value)`'s five-argument shape.
  Pairs with `pending()`  -  list the inbox, then answer one run by the `run_id` it named there;
  the lookup a caller used to do by hand (which invocable, which thread, which session) now
  travels with the pending entry.
- `agentdeck.__init__` now also exports `Agent` and `Workflow` alongside `Deck` (#164), so
  `from agentdeck import Agent, Deck, Workflow` covers the whole composition surface without
  reaching into `agentdeck.authoring`.

### Removed

- **Breaking (v3.0.0, #164): `App`, `agentdeck.app`, `agentdeck.agents` and
  `agentdeck.workflows` are gone, with no re-export shim.** `Deck` (`agentdeck.deck.Deck`, also
  exported as `agentdeck.Deck`) is the one composition root now: `Deck(...)` in place of
  `App()`, `Deck.from_project()` in place of discovery-on-construction. `agentdeck serve`'s
  console script and HTTP contract are unchanged from the previous slice
  (`Deck.from_project().asgi()`); everything else that called `App` migrates to `Deck` per the
  surface change below.
- **Breaking (v3.0.0, #164): `Deck`'s Python API is `run`/`stream`/`pause`/`cancel`/`status`/
  `resume`/`pending`/`answer` only.** `run_agent`, `chat`, `chat_stream`, `run_workflow`,
  `run_workflow_stream` and `resume_workflow`  -  v1's method names, carried onto `Deck`
  unchanged in the previous slice  -  are removed outright, and `pending_interrupts` is no
  longer public (folded into `due_resumes`, which stays). None of the six were ever called by
  `agentdeck.serve`  -  it always drove the Runtime directly  -  so the HTTP surface and
  `tests/golden/`'s byte-for-byte wire are unaffected; only the Python API changes. `run`
  covers `run_agent`/`chat`/`run_workflow` uniformly (pass `session_id=` for a conversational
  or threaded turn), and `stream` covers `chat_stream` the same way  -  including, now, what
  `run_workflow_stream` used to do outside the Runtime: a workflow's stream is canonical
  `Event`s, not the old dict shape, whichever method starts it.

### Fixed

- **`Agent(tools=[...])` now rejects a tool it cannot compile at `build()`, instead of building
  clean and failing at run time inside the SDK (#172).** A bare function or `lambda` used to
  reach the Agents SDK unwrapped, where it only failed once a run actually started, with a
  `UserError` about "hosted tools" that named nothing a caller recognised. `build()` (both
  `Deck.build()` and standalone `Agent.build()`) now raises `ConfigError` naming the agent and
  the offending tool, pointing at `@function_tool`  -  structurally, by checking the tool is one
  of the SDK's own tool types, so the check still constructs no engine and touches no network.
- **Every `tools=` example in the docs now shows the real contract (#179).** `Deck`'s module
  docstring and `docs-site/content/reference/definitions.mdx` / `concepts/agents.mdx` documented
  `tools=[find_slots, book_slot]` with plain callables  -  a form that has never run, and now fails
  `build()` per the fix above instead of the SDK at run time. They now show a tool built with
  `@function_tool` (`from agents import function_tool`), the only place agentdeck asks you to
  reach for the SDK directly; `Agent`'s own docstring gains the same statement of the contract.
- **A bundle that defines only an `AgentDeclaration`/`WorkflowDeclaration` subclass, and never
  instantiates an `Agent`/`Workflow`, now fails `Deck.from_project()` loudly (#174) instead of
  contributing nothing with no error or warning.** v1 scanned for a subclass  -  a bare
  `class Ghost(AgentDeclaration): ...` *was* the agent  -  so this is the natural shape of an
  existing bundle ported to v3, which scans for instances instead. The error names the bundle
  file and what to add (e.g. `` greeter = Agent(...) ``). A bundle directory that legitimately
  holds shared code and no invocable of its own opts out the same way it already could for the
  import/collision checks: give it a leading `_`/`.`.
- **A discovered agent's or workflow's compile failure now names its bundle path** (#119,
  following up #82/#117, which wrapped only import failures). `Deck.from_project()` (and any
  bare `InvocableRegistry.load()` that discovers its own catalog) wraps a `compile_agent`/
  `build_graph()` exception in a `ConfigError` naming the offending `agents/<bundle>/agent.py`
  or `workflows/<bundle>/workflow.py`, chaining the original exception as `__cause__`. A
  code-first `Agent`/`Workflow` has no bundle to name, so its build failures are unchanged.

- **An agent declaring `mcp=` opened with `async with deck:` never actually got its MCP
  servers, even when they connected successfully.** `Deck.build()` compiles every agent
  before `Deck.__aenter__` connects anything, so the compiled agent's tools and its
  strict-protocol banner were fixed at build time  -  permanently "unavailable"  -  regardless
  of what connected later. `Deck.__aenter__` now refreshes MCP status on every already-
  compiled agent right after `MCPLifecycle.startup` connects the real servers, so the agent
  that actually runs turns carries the servers it is, in fact, connected to. `Deck.build()`
  also registers the MCP server specs up front (`MCPLifecycle.configure`, still network-free),
  so a name declared in `mcp=` no longer logs a false "not found in config" warning at build
  time for a server that will, in fact, connect once the deck opens.
### Added

- **`agentdeck.__version__`** (#176): the installed distribution's version
  (`importlib.metadata.version("agentdeck")`), so it can never drift from what
  `pip`/`uv` actually installed. Falls back to `"0+unknown"` rather than raising when the
  package has no installed distribution to read (e.g. a bare source checkout).
- Docs: `deck.stream()`'s worked example ([Agents](/concepts/agents)) now discriminates
  events by `match`ing on `event.payload` instead of printing the envelope, and
  [Deck](/reference/deck) says so in prose  -  `type(event)` is always `Event`; the
  discriminator is the payload's own `kind`, reachable as `event.payload` or `event.kind`
  (#175).

## [2.0.0] - 2026-08-06

The release where agentdeck becomes a platform rather than a harness. Every turn  -  chat
or workflow  -  now runs on one Runtime and leaves one canonical event log behind, which is
what makes the rest of this list possible: a run you can pause, resume or cancel from
another process; an approvals inbox that survives a restart; a log you can point at
Postgres or Redis and share between workers; status and progress a client can render
instead of inferring. The v1 Python API, the `.agentdeck/` layout and the SSE wire are
unchanged and verified against recorded baselines, so a v1.2.1 project keeps working.

Known limits worth reading before you upgrade, each with an issue rather than a footnote:
run control covers **agent** runs  -  a workflow run has no safe point yet, so it pauses
through its own interrupt/resume instead (#128). Telemetry still flows through v1's
tracer, not the event-stream sink, so the b4 note claiming Langfuse covered workflow runs
described a sink nothing had wired (#124). The HTTP approvals inbox and
`App.pending_interrupts()` read different sources and will disagree if you drive
approvals through both (#120). There is still no auth on the endpoints (#25) and no
tenancy  -  one tenant, one principal.

### Added
- **Pause, resume and cancel a run that is already in flight**, by `run_id`, from
  Python or over HTTP:

  ```python
  await app.pause_run(run_id, reason="operator stepped away")
  events = await app.resume_run(run_id)
  await app.cancel_run(run_id, reason="user closed the tab")
  ```

  ```
  POST /runs/{run_id}/pause    {"reason": "..."}  -> {"run_id", "verb", "recorded": true}
  POST /runs/{run_id}/cancel   {"reason": "..."}  -> {"run_id", "verb", "recorded": true}
  POST /runs/{run_id}/resume   {"reason": "..."}  -> {"run_id", "status", "events"}, 409 if not paused
  ```

  **Asking is not stopping, and the log says both.** `pause_run` and `cancel_run`
  record a request and return; they cannot tell you when the run will stop, because
  it may be halfway through a tool call. So a controlled run writes
  `control.requested`, then `control.observed(safe_point)` once it reached a safe
  point, then the effect  -  `run.paused` / `run.cancelled` / `run.resumed`. Watch the
  events for the effect to learn that it stopped. A request leaves the run
  `running`; only the effect moves it.

  **Nothing is force-killed.** A signal is honored at the next safe point  -  between
  two stream items, so the chunk in flight is always delivered whole  -  and a tool
  call already running is never interrupted: the call finishes and the run stops
  before the step that would have used its result. `safe_point` on
  `control.observed` is what distinguishes "cancel took eight seconds" from "cancel
  took eight seconds *because a tool call did*".

  **A paused run is suspended in the log, not parked in a process.** The worker is
  free to exit, and any worker sharing the event store can lift the pause. Because
  there is no stack to return to, resuming re-enters the engine with the run's own
  input and the log as history: same `run_id`, `seq` carrying on. **Work the paused
  turn had already done can therefore happen again**  -  the model is asked again, and
  a tool it had already called may be called a second time, so keep tools idempotent
  and put side effects behind `ctx.idempotency_key`. Exactly one caller can resume a
  paused run; a second gets nothing rather than a second turn. Cancel is terminal
  and cannot be resumed, and `paused` stays distinct from `waiting_human` (that one
  resumes *with* a value).

  **Cancelling a paused run works, with one caveat worth knowing.** Pause, think,
  give up is the ordinary path, and a cancel recorded against a paused run is
  honored by the next resume  -  which ends the run `cancelled` rather than playing it
  on, so a resume can never quietly override whoever cancelled. But a paused run has
  no loop reaching safe points, so nothing else can turn that request into an effect:
  a paused run that nobody ever resumes stays paused, holding its session until
  `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS` takes it over.

  **Races are no-ops, never errors.** A signal that arrives after the run ended does
  nothing and records nothing; the same pause sent twice is one request; resuming a
  run that is not paused returns nothing (409 over HTTP).

  Pending signals live in a control port, which is **in-process by default**  -  one
  worker can control its own runs, a second worker cannot see them. Set
  `AGENTDECK_CONTROL_BACKEND=sqlite` and `AGENTDECK_CONTROL_URL=<file>` for signals
  that cross processes, which is also the file
  `agentdeck runs signal <run_id> <cancel|pause|resume> --control-db <file>
  [--reason ...]` writes to. Agent runs honor safe points today; a workflow
  (LangGraph) run has none yet, so pause and cancel do not reach one.
- **A run can say what it is doing**  -  two new event kinds, `status.reported` (a
  human-readable line: `"Searching GitHub"`) and `progress.reported` (a named stage,
  optionally counted: `step="Reviewing issues", current=2, total=4`), so a client can
  show a long run's activity instead of inferring it from tool calls. Both are
  **advisory**: they carry no meaning for the platform, and a run's status still
  folds from its lifecycle events alone  -  a run that reports is still `RUNNING`, and
  neither kind is terminal.
  Emitters reach the stream through the run context, which they already have:
  `await ctx.reporter.status("Searching GitHub")` and
  `await ctx.reporter.progress("Reviewing issues", current=2, total=4)`. An
  openai-agents function tool gets that context as the SDK's own  -  declare a first
  parameter of type `RunContextWrapper[RunContext]` and use `wrapper.context.reporter`.
  A langgraph node declares a `config: RunnableConfig` parameter and reads
  `config["configurable"]["reporter"]` (the key is
  `agentdeck.adapters.engines.langgraph.REPORTER_KEY`). Nothing imports the Runtime, and
  a `RunContext` built outside a run has a reporter that validates and drops.
  `current` past `total` raises immediately, at the call, whether or not a Runtime is
  listening. Reports are recorded in order, always before the run's terminal event, and
  the CLI reference renderer prints them (`[status] …` / `[progress] … (2/4)`).
  Three honest limits. A report is written at the engine's next event, so one emitted
  inside a single long tool call surfaces when that call **ends** rather than while it
  runs  -  enough for a client to show what a run has been doing, not enough to narrate a
  single slow call as it happens. Reports are best-effort: more than 64 waiting at once
  are dropped with a warning rather than growing without bound, and a report the event
  log refuses is dropped too, because an advisory event is never worth failing a run
  that would otherwise have completed.
  Reading a stream that contains them needs no change: a reader older than this release
  parses both as `UnknownEvent` and skips them, and because neither is a lifecycle kind
  a mixed-version deployment folds status identically on both sides.
- **`control.requested` and `control.observed`** (`agentdeck.core`): run control
  is now three events, not one. `control.requested(verb, reason=None)` records
  that a signal was written; `control.observed(verb, safe_point)` records that the
  run reached a safe point and is acting on it; the effect stays the kind it
  always was (`run.cancelled`, `run.paused`, `run.resumed`, `input.appended`). So
  "we asked it to stop" and "it stopped" are finally different facts in the log  -
  under cooperative control they can be seconds apart, and `safe_point`
  (`stream_item`, `tool_dispatch`, `node_boundary`) says what the run was in the
  middle of. One pair of kinds carries every verb  -  `cancel`, `pause`, `resume`,
  `steer`  -  so pause/resume and mid-run steering add no further vocabulary when
  they ship. Neither kind is a status transition: a request leaves a run `RUNNING`
  until its terminal event says otherwise, and neither is terminal.
  The vocabulary landed first and the producers followed in the same release (see
  the pause/resume/cancel entry above): a run that is signaled now emits both
  kinds. The CLI renderer prints both phases and the Langfuse sink puts them on the
  run's timeline.
- `run.resumed` now carries **the answer it was resumed with**, as content
  (`value: list[ContentBlock] | None`)  -  content passes through as sent, a string
  arrives as a `TextBlock`, any other JSON answer as a `DataBlock`, and lifting an
  operator's pause carries nothing. Stored **in full**, like a run's own input,
  because a truncated answer cannot be replayed. This is what makes a
  previously unrecoverable window *repairable*: the single write that moved a run
  from `waiting_human` to `running` recorded *that* it was answered and not *what*
  the answer was, so a process dying between that write and the engine consuming
  the value left the log saying `running` while the engine was still parked at its
  interrupt  -  every later resume then rejected as stray, with no recovery but a
  manual one. The answer is now in the log at the instant the claim commits, before
  the engine is asked for anything, so a successor process has what it needs. **The
  repair itself is not built here**  -  nothing yet reads `value` back to bring an
  engine into line  -  so treat this as the prerequisite, not the fix. An answer JSON
  cannot carry (an arbitrary object, a `datetime`, `NaN`) is logged as a warning and
  recorded as no value rather than failing a resume that would otherwise work; such
  a run keeps the old stranding risk.
  Compatible in both directions, and measured rather than assumed: a `run.resumed`
  written before this release still parses (no `value` means none), and a 2.0.0b4
  reader handed one of the new events parses it and drops the field it does not
  know  -  no listing or dashboard outage like the one `DataBlock` caused, and the
  new kinds arrive as unknown kinds a consumer skips. The one caveat is what that
  dropping implies: only a process new enough to *see* `value` can use it to
  repair a resume, so upgrade the workers that reconcile before relying on it.
- **`UnknownBlock`** (`agentdeck.core`): a content block of a type this version
  doesn't recognize now falls back to `UnknownBlock(type, raw_block)`  -  keeping the
  raw block for a store to hold and a consumer to skip  -  instead of rejecting the
  whole event, mirroring how an unknown event `kind` already becomes `UnknownEvent`.
  Closes the one asymmetry `DataBlock` (#101) exposed: `ContentBlock` was a strict
  discriminated union, so a reader older than a new block type raised on the entire
  event rather than skipping the block, and because `SqliteEventStore.list_runs`
  deserializes a run's last lifecycle row in one comprehension, one such event in a
  shared store could fail a whole tenant's listing. A malformed *known* block still
  raises. Measured against `origin/dev`'s own `ContentBlock`, not asserted: that
  reader really does reject a block type this addition introduces, and this tree's
  reader parses the same wire event, keeps the raw block, and leaves `status_of` and
  the terminal invariant unchanged (`tests/core/test_old_reader_block_compat.py`).
### Changed
- **A run reads its pending control signal at most once every 200ms**, instead of
  once per streamed item. A 500-chunk answer used to cost 500 control reads whose
  answer was "no" 499 times  -  one file read each with the SQLite control port, and a
  network round trip each once the port is shared. Measured at a real model's pace
  (~30ms a chunk), a 400-chunk answer now costs 58 reads instead of 400.
  What this trades is **latency, not correctness**: a cancel is noticed up to 200ms
  after it is recorded, and still acted on *at* a safe point, never mid-token. The
  first safe point of a run always reads, so a signal that beat the run out of the
  gate is honored immediately. Anyone who was relying on the previous
  read-every-item behavior  -  a test asserting a cancel lands within a stream shorter
  than 200ms, for instance  -  can pass `Runtime(..., control_poll_interval=0)` to get
  it back at the old read cost.
- **A sink the breaker disables is no longer disabled for good.** A telemetry
  endpoint that failed five events in a row used to be dead for the rest of the
  process; now the dispatch waits 30 seconds and then lets one event through to
  see whether it is back. A sink that takes that event starts receiving the
  stream again; one that fails it keeps its events dropped and is offered
  another event 30 seconds later, so a genuinely dead endpoint costs two emit
  attempts a minute rather than one per event. Coming back is logged as loudly as
  going away was, and says how many events the outage dropped, so a stream that
  resumes mid-run is not a gap with nothing to explain it. The cooldown is a
  deadline read off a clock and never a wait  -  a run is not slowed by a sink's outage or by
  its recovery  -  and nothing is replayed: the events the outage covered are
  still lost, and still counted as drops. A sink therefore needs no retry logic
  of its own for a transient outage, and one that cannot lose events reads the
  event log, which is the complete copy.
- **A flapping sink can no longer flood the log with stack traces.** Failure
  logging was rate-limited per failure *streak*, which bounded nothing for a
  sink that fails every other event  -  each success reset the streak, so every
  failure printed a fresh traceback and the run's length decided the log
  volume. Tracebacks are now limited to one per sink per 60 seconds, and each
  one reports how many failures went unlogged since the last, so a throttled
  log still says how much it is standing in for. The breaker's disable decision
  is unchanged by this.
- **The workflow HTTP endpoints run on the v2 Runtime.** `POST
  /workflows/{name}/run`, `GET /workflows/{name}/pending` and `POST
  /workflows/{name}/{thread_id}/resume` were the last surface still calling v1's
  runner directly, so a workflow turn left **no event log behind at all**  -  it
  streamed to the caller and vanished. Every workflow turn is now recorded like a
  chat turn: one run in the log, node updates, stream writes, interrupts and the
  final state, readable by the same listings, replays and dashboards. The wire is
  unchanged  -  the same `node_update` / `custom` / `interrupt` / `done` SSE frames
  and the same JSON bodies, checked against the recorded baselines rather than by
  inspection.
  Three consequences worth knowing before upgrading. A workflow's `thread_id` is
  now its **session**, and a session runs one turn at a time, so posting a second
  run to a thread whose previous turn has not finished answers **409** instead of
  interleaving two turns over one graph state. Read "not finished" broadly: a
  thread sitting *idle* on an unanswered approval is not finished either, and holds
  its session until somebody answers it  -  so the case an approval UI actually hits
  is a 409, for as long as the approval goes unanswered (or until that run has been
  silent for `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS`, one hour by default,
  after which the next turn takes the session over). A resume against a thread with
  no paused run answers **404** where it previously surfaced v1's runner error.
  And a `get_stream_writer()` write now reaches the log as a namespaced `custom`
  event (`langgraph.stream_write`) on its way to the unchanged `custom` frame.
  A `durable = True` workflow still resumes on the configured checkpointer  -  the
  bridge plays v1's own compiled graph, which carries it  -  and the `[durability]`
  extra stays optional for a project that only chats.
- **The HTTP approval inbox and `App.pending_interrupts()` are now separate sources
  of truth**, and will be until they are joined. `GET /workflows/{name}/pending`
  and the HTTP resume project the event log; `App.pending_interrupts()`,
  `App.due_resumes()` and `App.tick()` still read the graph's checkpointer. They
  agree only as long as one of them is used: an interrupt created headlessly is
  invisible over HTTP, and one answered headlessly leaves an entry behind in the
  HTTP listing. Answering such a leftover entry over HTTP is a **404** rather than
  the stale final state a replayed thread would otherwise hand back, so no answer
  is silently dropped  -  but a deployment that drives approvals through both doors
  will see the two listings disagree. Joining them  -  routing the Python API's inbox
  through the Runtime too  -  is tracked in #120.
- The v2 `LangGraphEngine` (not v1's endpoints, whose final state always came from
  `ainvoke`) now reports a final state for a graph compiled **without** a
  checkpointer, which it previously could not: the terminal state is read from the
  run's own event stream instead of from a checkpoint that never existed.

### Removed
- **`agentdeck.runtime.REPO_ROOT` / `agentdeck.runtime.settings.REPO_ROOT`**  -  it only
  ever pointed at the repo root in a source checkout and at the installed package's
  `site-packages` directory otherwise; nothing in agentdeck needs that path, and
  nothing outside it should have depended on it either. (#16)
- **`agentdeck.runtime.ENV_FILE` / `agentdeck.runtime.settings.ENV_FILE`**  -  was a path
  frozen at import time (see Fixed below for why that was itself unsafe); replaced by
  `resolve_env_file()`, resolved fresh every time `get_settings()` actually builds a
  `Settings` object. (#16)

### Fixed
- **`.env` and `config.yaml` now resolve from the project's current working
  directory, not from wherever `agentdeck` itself is installed.** Previously
  both were located relative to `runtime/settings.py`'s own file path, which
  is the repo root in a source checkout but lands inside `site-packages` once
  `agentdeck` is `pip install`ed as a dependency  -  so a consumer project's
  `.env` (API keys, `OPENAI_MODEL`, …) was silently ignored, typically
  surfacing as `OPENAI_API_KEY ... must be set` despite a valid `.env` in the
  project. **If you were exporting the same values as real shell/CI
  environment variables to work around this, nothing changes**  -  a real env
  var still outranks the file. But if you have a `.env` sitting unused next
  to an installed `agentdeck`, it will now take effect. `.env` is also now read
  at first use rather than at `import agentdeck` time, so a `chdir` between
  importing the package and first building settings still resolves against the
  right project. (#16)
- Two bundles of the **same kind** (two agents, or two workflows) exporting a
  class of the same name used to collapse silently into one invocable, in
  sorted bundle order  -  copying `agents/greeter/` to `agents/greeter-v2/` to
  iterate and forgetting to rename the class made the original vanish from the
  registry with no error, no warning, no log line. `App.load()` (and anything
  that discovers a project, including `InvocableRegistry`) now raises
  `ConfigError` naming both bundle paths and the class name they share. A
  project relying on the old shadowing to hide one bundle behind another now
  fails at load instead of routing requests to the wrong agent; rename one of
  the classes to fix it.
- A bundle whose `agent.py` or `workflow.py` raises while importing (a
  `SyntaxError`, a missing dependency, anything at module scope) used to
  surface as a raw traceback through the import machinery. It's now a
  `ConfigError` naming the offending bundle path, with the original exception
  chained as the cause.

## [2.0.0b4] - 2026-08-06

The release where v1 starts running on v2. `App` is now the composition root and
the chat endpoints are served by the v2 Runtime with a byte-identical wire  -  same
SSE frames, same JSON bodies, verified against the recorded baselines rather than
by inspection. Around that: an event log you can point at Redis or Postgres and
share between workers, a session that runs one turn at a time instead of letting
two answers overwrite each other, telemetry that covers workflows and flushes
what it buffered at shutdown, a canonical shape for structured data, and a turn
that repairs its own history after a crash between two writes. The v1 Python API
is unchanged and still runs its own path.

### Added
- **Redis and Postgres event logs**  -  `AGENTDECK_EVENTS_BACKEND=redis` or
  `=postgres`, with `AGENTDECK_EVENTS_URL` as the Redis URL or the Postgres DSN,
  puts the canonical event log somewhere **several workers can share**. That was
  not possible before: SQLite's durability rests on cross-process shared memory,
  so one events file behind more than one machine is unsupported. The store
  classes are `RedisEventStore(url)` (`agentdeck.adapters.stores.redis`) and
  `PostgresEventStore(dsn)` (`agentdeck.adapters.stores.postgres`) for anyone
  wiring a `Runtime` directly.
  Both implement the whole store port, including the two atomic claims that keep
  one resume and one turn per session correct *between processes* and not merely
  between tasks: Postgres decides and writes inside one transaction holding that
  log's lock, Redis over `WATCH`/`MULTI`/`EXEC`. Every case in the cross-store
  contract suite runs against all four backends on real servers, so the four
  answer identically  -  one `seq` per run refused a second time included.
  Each keeps to its own keyspace  -  a Postgres schema (`agentdeck_events` by
  default, overridable with `schema=`) and a Redis key prefix
  (`agentdeck:events`, overridable with `prefix=`)  -  so a database or instance
  shared with LangGraph checkpoints or the agent-conversation store keeps the
  log separate, and either side can be dropped without touching the other.
  Redis needs no new dependency; Postgres needs the `[durability]` extra, which
  now also installs `psycopg[binary]` (nothing else pays for it  -  the driver is
  imported only when you select that backend). Three things to know when
  operating them: a Redis instance used as the record wants `appendonly yes`
  (the port promises an event a consumer has seen is already stored, and the
  default snapshot-only persistence can lose the last seconds of a log) **and**
  `maxmemory-policy noeviction` (this is a log, not a cache  -  an evicted key can
  cost a live run its session); and a store call that cannot reach its server
  raises `StoreError` rather than reporting a claim somebody else won.
- `DataBlock` (`agentdeck.core`): structured data is now content, alongside
  `TextBlock`, `ImageBlock` and `ResourceBlock`. `DataBlock(data=...)` carries any
  JSON value, so anywhere the v2 API takes or returns content blocks  -
  `Runtime.run(...)`, `run.started.input`, `run.completed.output`,
  `input.appended`  -  a validated `output_type` result or a workflow's state
  travels as itself instead of being squeezed through text. Data that could not
  survive the wire (a `datetime`, a `set`, an arbitrary object, and `NaN` /
  `±Infinity`  -  floats with no JSON literal, which would otherwise be written as
  `null`) is refused at construction rather than failing later, or silently
  changing value, in a store or a trace. Text and data blocks are stored **in
  full**: they are the caller's own input and the run's own declared result, and a
  truncated copy cannot be replayed  -  only *tool* results stay bounded to a
  preview, size and hash.
  Additive for writers: no existing block, payload or field changed. **Not
  backward-compatible for readers, and wider than one event**  -  content blocks are
  a strict discriminated union, so a process running an older agentdeck cannot
  parse an event containing a `data` block at all. Because a run listing parses
  each run's last lifecycle event, one structured `run.completed` in a shared
  event store makes the older process's `list_runs` fail *for the whole tenant*,
  including runs it wrote itself  -  a listing or dashboard outage on the old half
  of a fleet, and a rollback after the first structured run lands in a state the
  old code cannot read. Do not run mixed agentdeck versions against one event
  store across this change: upgrade every reader first, then start producing
  `data` blocks.
- The chat endpoints now run on the v2 Runtime. `POST /agents/{name}/chat` and
  `?stream=true` are served by the same Runtime, event schema, and event log the
  `/v2/...` routes use, with the v1 wire format rendered at the surface: the
  `delta` / `done` / `error` frames, the `{"output", "usage"}` payloads and the
  404/422/500 bodies are byte-for-byte what 1.2.x sent (the golden replay suite
  is unchanged, and is what enforces that). Nothing to change in a client. What
  you gain is what the Runtime keeps: every turn is now recorded as a canonical
  event log  -  `run.started`, text deltas, tool calls, per-model-call
  `usage.reported`, `run.completed`  -  so a chat turn is finally as inspectable as
  a `/v2` one. Sessions, Langfuse traces, sandboxes, `max_turns`, the model
  provider and every other setting resolve exactly as they did before, and a
  conversation is still one conversation whether the turn arrived through
  `App.chat` or over HTTP. The workflow endpoints still run on v1's workflow
  runner, unchanged.
- `App` is the composition root, and the wiring behind it is one function:
  `build_runtime(engines=...)` (`agentdeck.composition`) takes the parts  -  the
  invocable mapping, engines, event store, sinks, control port  -  and returns a
  wired `Runtime`, defaulting the mapping to discovery over `./.agentdeck` and
  the store to your settings. `App.load()` calls it and exposes the result as
  `App.runtime`, so an application that wants the canonical event stream for a
  project no longer has to assemble a Runtime by hand. `App.aclose()` drains the
  Runtime before closing Redis and MCP, so a sink registered through the seam
  flushes at shutdown instead of dying with the event loop  -  `App` registers none
  of its own yet, so today that drain is a no-op that keeps its own promise.
- `AGENTDECK_EVENTS_BACKEND` / `AGENTDECK_EVENTS_URL` (YAML: `events:`) choose
  where the canonical event log goes: `memory` (the default  -  no configuration,
  no files, and a log that lives and dies with the process) or `sqlite` with
  `url` pointing at a file, for a log that survives a restart, or `redis` /
  `postgres` for one several workers can share (see the entry above). The default
  never evicts and is lost on restart, so a long-lived server keeps every event it
  saw and re-reads the whole conversation each turn  -  `agentdeck-serve` says so
  once at startup rather than leaving you to find out.
- Langfuse tracing for **workflow** runs, not only agent runs
  (`agentdeck.adapters.telemetry.langfuse`). `langfuse_sink()` hands back an
  event sink  -  or `None` when Langfuse has no keys  -  to register where you build
  the v2 `Runtime`: `Runtime(..., sinks=[s for s in (langfuse_sink(),) if s])`.
  Each run becomes one Langfuse trace: the run itself is the trace, tool calls
  are spans carrying their arguments and their result preview, hash and size
  (an inline `data:...;base64,` payload in either is described, never sent  -
  Langfuse would otherwise upload the bytes to its media store),
  workflow node updates are points on the timeline named for the node and the
  state keys it touched, and reported token usage becomes Langfuse generations
  so cost lands where the UI accounts it. It reads nothing but the event
  stream, so an agent run and a workflow run are traced by exactly the same
  code  -  and a run waiting on a human is visible while it waits, its answer
  continuing the same trace even when it arrives in another worker. Sessions
  map to Langfuse sessions and the run's principal to its user, so a
  conversation is one filter away. Configuration is the `AGENTDECK_LANGFUSE_*`
  settings you already have; with no keys, no sink is registered, and the
  Langfuse SDK is never even imported. Needs the `[observability]` extra. v1's
  tracing is unchanged  -  a v1 agent run with both paths active is reported
  twice.
- `InvocableRegistry` (`agentdeck.runtime.discovery`): the v2 Runtime's list of
  what it can run is now discovered from your `./.agentdeck/` project instead of
  written out by hand at every entry point. `InvocableRegistry(engines).load()`
  reads the same bundles v1 always has  -  `agents/<bundle>/agent.py`,
  `workflows/<bundle>/workflow.py`  -  and returns the name-to-invocable mapping
  `Runtime` takes, with each bundle pointed at the engine its shape belongs to.
  Adding an agent or a workflow to a project no longer means editing wiring code.
  An agent and a workflow claiming one name, and a project whose bundles need an
  engine the Runtime wasn't given, both fail at load with a message naming the
  offender, rather than at the moment somebody runs it. (Two bundles of the same
  kind exporting one class name still collapse to a single invocable, as in v1.)
  Skills are not discovered as invocables yet  -  no engine runs a `SKILL.md`
  bundle. v1's `App` and its discovery are unchanged.
- `ToolSourcePort` (`agentdeck.core.ports`): tools now arrive from a source
  behind one small interface  -  `resolve(spec)` hands back a `ToolSet` of the
  tools an invocable gets, the names of the ones it asked for and did not get,
  and the notice to put in front of the model when something is missing. MCP is
  the first source, and its behavior is unchanged: an unconfigured or
  unreachable server still degrades a run instead of failing it, and an agent
  whose servers are all up gets its instructions back byte-for-byte, so upstream
  prompt caches keep hitting.
- `agentdeck.SessionBusyError`: the error raised when a turn is asked for on a
  session that already has one running. It names the session and the run holding
  it, and is an `AgentdeckError` like every other, so `except AgentdeckError`
  already covers it.
- `EventSinkPort.close()` (`agentdeck.core.ports`): the hook a sink that buffers
  needs to get its buffer out at shutdown. `Runtime.drain()` now calls it once per
  sink  -  after the sink's queued events have been handed over and its consumer
  retired  -  so a sink whose `emit` only buffers, which is what the emit contract
  pushes any sink with real work to do into, has a deterministic last chance to
  ship what it holds instead of hoping the process exits cleanly enough for an
  `atexit` hook to run. **Optional**: it defaults to doing nothing, so existing
  sinks need no change. What a sink may assume is now stated and enforced  -  `close`
  is called at most once, and no `emit` is ever started after it, not even by a
  consumer that outlived the cancellation retiring it. It is also called on a sink
  that never saw an event, since a process can shut down without running anything.
  One caveat worth knowing if your sink buffers: an `emit` that has not *finished*
  when the dispatch stops waiting for it still overlaps `close`  -  whether it
  swallowed the cancellation sent to end it, or simply awaits something while
  unwinding (an `await` in a `finally` or an `except`, such as salvaging a partial
  result). Read-`await`-clear inside `close` can therefore drop what that emit adds
  in between; guard the buffer instead. Bounded and non-fatal like every
  other wait on the sink path: a `close` still running after `CLOSE_TIMEOUT` (5s) is
  abandoned, anything it raises is logged and flagged (`SinkDispatch.close_failed`),
  and neither can delay a shutdown further or break it. A sink the failure breaker
  already disabled is closed too  -  the events it buffered before it started
  failing are still worth writing out, and being bad at *taking* events says
  nothing about being able to flush the ones already taken.
- `agentdeck.StoreError`: the error a durable store raises when it cannot be
  read or written. `except StoreError` (or `except AgentdeckError`) now covers
  the SQLite event log and the SQLite control-signal database; the underlying
  `sqlite3` exception is kept as the cause for diagnosis.

### Changed
- A v2 workflow run's final state is now a `DataBlock` on `run.completed`
  instead of a stringified Python dict, and a workflow can be *started* from a
  state-shaped input: pass one `DataBlock` whose data is a JSON object and it
  becomes the graph's initial state, whole. Plain text still fills the single
  `{"input": text}` channel, so text-in workflows are unchanged. Anyone reading
  the canonical stream (the `/v2/*` preview surface, a sink) gets the state as
  data it can index instead of a repr it would have to parse; a state value that
  is not JSON still becomes its `str()`, exactly as before, so no workflow that
  completed before now fails. v1's `/workflows/*` endpoints and Python API are
  untouched.
- `POST /agents/{name}/chat` now answers **422** for a `message` or a `session_id`
  that is not a string. `message` used to accept two more shapes  -  a message object
  (`{"role": ..., "content": ...}`) and a list of SDK input items  -  and a
  non-string `session_id` (say the integer `7`) used to be passed through as a
  session key; both now fail the request with `{"detail": "message must be a
  string, got dict"}` / `{"detail": "session_id must be a string, got int"}`
  instead of a server error. Coercing the id instead would have quietly moved that
  caller's conversation to a new session, so it is a 4xx you can see. Multi-part
  input (images, resources) returns as typed content blocks in a later release; a
  string in both fields is unaffected.
- A project where an agent class and a workflow class share one name now fails at
  `App.load()` with a message naming both, instead of loading two invocables that
  the HTTP surface could not tell apart. Two bundles of the same kind exporting
  one class name still collapse to a single invocable, as before.
- The streamed `done` frame serializes a structured `output_type` result the same
  way the non-streamed body always has, so the two agree. Only nested values
  whose JSON form differs from `str()` change: a `datetime` in a structured output
  is now `"2026-08-06T12:34:56Z"` on the streamed frame, where it used to be
  `"2026-08-06 12:34:56+00:00"`. Text output  -  the overwhelming majority  -  is
  byte-identical.
- `POST /agents/{name}/chat` without `?stream=true` drives the SDK's streaming
  API internally (the streamed and non-streamed endpoints are now one code path
  that differs only in how it answers). Same model, same settings, same result;
  worth knowing if your provider behaves differently between its streaming and
  non-streaming endpoints, or gates streaming behind account verification.
- `MemoryEventStore.append` now yields one scheduling turn (`await asyncio.sleep(0)`)
  before returning, matching what every durable store already does (SQLite's own
  `to_thread`). Fidelity, not correctness: a caller whose liveness secretly depended
  on the in-memory store never suspending  -  the way the bounded sink dispatch briefly
  did, before its own fix  -  is now exercised the same way it would be against a real
  deployment, in dev and in tests, instead of only by measurement in production.
- MCP now lives in `agentdeck.adapters.tools.mcp` (registry, hardened HTTP
  transport, agent wiring  -  all unchanged). `from agentdeck.agents.mcp import ...`,
  `from agentdeck.agents.mcp.lifecycle import ...` and `from agentdeck.agents
  import ...` keep working and hand back the same objects; both paths will be
  dropped in a later release. The deeper module paths
  `agentdeck.agents.mcp.transport` and `agentdeck.agents.mcp.wiring` are gone  -
  import those names from the package instead.
- `EventSinkPort.emit` must now return promptly: an emit that blocks longer
  than the dispatch's `emit_timeout` (5s) is abandoned and counted as a
  failure, and a sink that does it repeatedly is disabled like any other
  broken sink. A sink whose work is slow buffers internally and flushes on
  its own schedule.
- `Runtime.drain()` is now terminal  -  it closes each sink rather than
  pausing it, and returns within a bounded time even against a sink whose
  `emit` never returns. Runs after a `drain()` reach no sinks.
- Langfuse traces no longer depend on the SDK's exit hook to leave the process.
  `Runtime.drain()` now closes the sink: any trace still open is finished as
  interrupted by the shutdown  -  an unfinished observation is never shipped at all,
  so a run cut short showed up nowhere before  -  and the SDK's batch is flushed on
  the spot. A process killed after its `drain` no longer silently loses the last
  seconds of telemetry. Nothing to configure; a flush that hangs or fails is
  bounded and logged like any other sink work, and the event log stays the
  complete record either way.
- The SQLite event log and the SQLite control-signal database now open in
  **WAL** mode with an explicit 5-second busy timeout. Readers no longer wait
  behind a writer, so a second process tailing or replaying a log costs the one
  writing it far less: in a saturated benchmark, read latency at the 99th
  percentile and in the worst case improved by roughly an order of magnitude.
  Two things to know about the files: SQLite keeps
  `<db>-wal` and `<db>-shm` alongside each database  -  back them up
  and move them together, not the one file on its own  -  and WAL depends on
  shared memory that network filesystems (NFS, SMB) do not provide reliably, so
  keep these databases on local disk. In-memory databases are unaffected.
- **One turn per session at a time.** Starting a turn on a session that already
  has a run in flight now fails immediately with `SessionBusyError`, naming the
  session and the run that holds it, instead of running the second turn against a
  conversation the first one is still changing  -  which silently corrupted the
  model's context and could lose a message from either turn. A session counts as
  busy until its run reaches a terminal event, and a run waiting on a human answer
  is still busy: it owns the thread its resume continues from. Sequential turns,
  resumes and runs without a session are unaffected, and two different sessions
  never contend. This holds across processes, because the check and the write that
  opens the run are one store operation, so it is not defeated by a second worker.
  What a caller should do with the refusal is retry or report it; the losing turn
  is not queued (that is deliberately deferred, not forgotten). Over HTTP the v2
  chat route answers **409 Conflict** with the holding run named in `detail`,
  before the event stream starts. A client that disconnects in that window  -  after
  the turn was admitted but before the first event reached it  -  has its run closed
  as cancelled, so the session is free for the retry rather than held.
- The event log now enforces **one `seq` per run**: `(tenant, session, run, seq)`
  is unique in the SQLite store and refused by the in-memory one, so a write that
  would put a second event at a `seq` a run has already used fails with
  `StoreError` instead of landing. A duplicate is the one corruption a gap check
  cannot see, and it would make refetching that `seq`  -  the whole point of
  contiguous `seq`  -  return whichever copy came back first. `seq` is still per
  run, so runs sharing a session log all count from 0 as before. Note for existing
  installations: only event databases created by this version carry the
  constraint, since v2 has no schema migration yet.
- A run whose process was killed outright  -  the one exit that cannot close its own
  run in the log  -  no longer holds its session for good. An open run that has
  written nothing for `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS` (one hour by
  default) stops blocking new turns; the next turn takes the session over, closes
  the abandoned run as `run.failed` with error code `cancelled_hard`, and logs the
  takeover at WARNING. Two things follow from that window, both tunable with the
  same setting: a session a crashed process left claimed is refused until the
  window elapses, and an approval that has been waiting on a human for longer than
  the window is closed as failed when somebody starts a new turn on that session  -
  installations with slower approvals should raise it. Keep it comfortably above the
  longest a healthy turn can go without emitting an event: shortened below that, a
  turn that is merely quiet looks abandoned and the next turn takes its session,
  which loses the one-turn-per-session guarantee instead of tuning it. Only
  positivity is checked  -  the real floor depends on your workload. Running several
  workers on machines whose clocks disagree shortens the window by the worst skew
  between them for the same reason, so keep them on NTP and leave headroom.

### Fixed
- A durable LangGraph checkpointer can now be used by more than one event loop in
  one process. The sqlite and postgres savers were cached for the process
  lifetime, and each holds an internal lock that binds to the first event loop to
  contend for it  -  so a script or test that called `asyncio.run()` twice against
  the same durable graph failed the second time with `RuntimeError: ... is bound
  to a different event loop`, usually only once real concurrency showed up. Each
  loop now gets its own saver, and a loop that asks repeatedly still shares one
  connection. The in-memory saver is unchanged and still shared, so
  `durable = True` on the memory backend keeps resuming across `asyncio.run`
  calls as before.
- An `output_type` agent run through the v2 `Runtime` no longer fails at its last
  step. The openai-agents engine refused any non-`str` final output, which turned
  a documented feature into a failed run; the validated result (pydantic model,
  dataclass, or plain JSON) now arrives as a `DataBlock` on `run.completed`.
  v1's `App.chat` / `run_agent` never had this problem and are unchanged.
- A run whose consumer goes away is now closed in the event log even when the
  consumer was *cancelled* rather than closed  -  which is what a real ASGI server
  does when a client disconnects mid-stream. `Runtime.run` and `Runtime.resume`
  caught `GeneratorExit` and `Exception`, and `CancelledError` is neither, so a
  disconnected stream used to leave a run with no terminal event: indistinguishable
  from one still in flight, for status projections, `pending()` and anything
  reading the log. Both now record `run.cancelled` (shielded, so the write is not
  itself cancelled) and re-raise. A process that dies with the request still leaves
  the run open  -  no in-process write can outlive its own event loop.
- Shutdown no longer hangs forever on a wedged sink: every wait on the sink
  path has a deadline, including the last one  -  the wait for the sink's
  consumer to stop. A sink whose `emit` swallows cancellation can delay a
  shutdown but no longer block it, and a cancellation aimed at whoever is
  shutting down is no longer absorbed by the shutdown itself. That last deadline
  needed a second fix to hold: a deadline fires by cancelling the task that is
  waiting, and a task waiting *on another task* hands that cancellation straight
  to it  -  into the same sink that had just swallowed one, spending the deadline
  with nothing left to fire again. A sink that ate cancellation from inside a
  still-running `emit` could therefore keep a shutdown waiting for as long as it
  kept working, and no outer `wait_for` could end it either. The consumer is now
  waited on from the outside, so the deadline expires on time whatever the sink
  does; and the consumer has a second way out that needs no cancellation at all  -
  once the dispatch is closed, its own loop ends.
- Sink loss counters no longer under-report. Events still queued (and the one
  in flight) when a sink is closed are counted as dropped, so the counters
  agree with the log line that reports them; a sink that raises
  `CancelledError` from its own `emit` is counted as a failure instead of
  silently killing its consumer; and a clean shutdown with an empty queue no
  longer logs a spurious "queued events go undelivered" error. Nor do they
  over-report: a consumer abandoned mid-`emit` retires at its next turn instead
  of draining a backlog the shutdown had already written off, which would have
  counted every event in it a second time.
- A SQLite failure inside the event log or the control-signal database no longer
  surfaces as a raw `sqlite3` exception: it is raised as `StoreError`, with the
  original chained as its cause. This matters most when two processes answer the
  same human-in-the-loop interrupt: the one that loses gets the documented
  "somebody else claimed it" answer, and a store that genuinely cannot be
  reached raises `StoreError`  -  two outcomes a raw `sqlite3.OperationalError:
  database is locked` used to blur together.
- Crash recovery for conversations on the OpenAI Agents engine: a process that
  died mid-turn used to leave that conversation permanently short of whatever the
  event log had already recorded  -  the question it was killed on, or the answer it
  had just given. The model then answered later turns with a hole in its context
  and nothing reported a problem. Each turn now checks the log against the
  engine's own conversation state and replays the messages that are missing before
  the model runs, so a restarted process picks the conversation up whole.
  Messages only, in content and order: tool results and model reasoning are not
  reconstructed, so a conversation repaired this way carries the *text* of a tool
  answer without the tool call behind it  -  worth knowing if you read model context
  back. A turn a client disconnected from before the first token is never replayed,
  so retrying that question does not send it twice; a turn that was answered before
  the client went away keeps both its messages. Conversation state that has diverged
  from the log rather than fallen behind it is left untouched and reported on the run
  as `custom` / `openai_agents.session_diverged`. LangGraph workflows are unaffected  -
  a checkpoint is written by the graph step itself, so there is no gap between two
  writes to repair.

## [2.0.0b3] - 2026-08-05

A hardening release: no new surface, sturdier runtime. Cancel a run from
another process, answer an approval from either of two servers without the
workflow running twice, and keep telemetry from growing memory or losing
events behind a wedged endpoint. Every guarantee here is enforced by the
event store itself rather than by in-process locks, so it holds when a
second worker joins. The v1 public surface remains byte-for-byte unchanged.

### Added
- Run control (`agentdeck.core.ports.control`, `agentdeck.adapters.control`): a
  `ControlPort` for cross-process cancel signals, backed by an in-memory adapter
  for dev/tests and a SQLite-backed one durable enough for a second OS process to
  reach a run it never held a reference to. The OpenAI Agents engine checks a
  cooperative gate between stream items and stops cleanly on cancel, emitting a
  single `run.cancelled` and leaving a truncated-but-coherent replay behind (no
  `message.completed` for the interrupted message). New `agentdeck runs signal
  <run_id> cancel --control-db <path>` CLI command to send that signal from a
  second terminal.
- `EventStorePort` (not yet part of any stable public API) gains focused
  queries alongside its whole-log reads: `last_seq` (a run's highest
  recorded `seq`), `run_status` (one run's status, derived from its own
  events), `list_runs` (every run for a tenant, optionally filtered by
  status), and pagination (`offset`/`limit`) on `read`. Both the memory and
  SQLite stores implement all four; the SQLite ones use the existing
  run/log indexes.
- `EventStorePort.claim_resume` (not yet part of any stable public API): a
  conditional append that records `run.resumed` only if the run is still waiting
  on a human answer *and* the event's `seq` is still the run's next one, as one
  indivisible step, and reports whether it won. The memory store gets that for
  free; the SQLite store does it in a single `BEGIN IMMEDIATE` transaction, so
  the events file itself picks the winner.

### Changed
- Internal: the v2 event-log port (not yet part of any stable public API) is
  now named `EventStorePort` instead of `SessionStorePort`, to avoid confusion
  with the OpenAI Agents engine's own session-scoped storage. No behavior
  change and nothing outside the package imports this port.
- Internal: the Runtime's resume path and the `/pending` listing now use
  `EventStorePort`'s focused queries instead of folding a whole log to answer
  one run's status or find waiting runs. Same results, much less work per call:
  a resume deserializes only its own run's events instead of the whole session's
  (22 instead of 4,400 on a 200-run session), and the pending listing is one
  indexed statement returning each run's last lifecycle event  -  one event parsed
  per run instead of every event of every log (4.2 ms instead of 32 ms for the
  same 201 runs).
- Event sinks are now fed from a bounded queue with one worker each, instead of a
  fresh task per event per sink. A wedged sink (telemetry endpoint down, audit
  store backpressured) now costs a fixed backlog and one task rather than growing
  memory for as long as the process runs. A run still never waits on a sink: when a
  sink's queue is full its stalest event is dropped rather than the run delayed  -  but
  only once the sink has been given a turn to catch up, so a sink that is keeping up
  loses nothing however fast the run produces events. Dropped events and failed
  emits are counted per sink and reported in the logs  -  never discarded silently, and
  never one stack trace per event  -  and a sink that raises five times in a row is
  disabled instead of being retried for the rest of the process's life. Two side
  effects worth knowing: each sink's `emit` is now called one event at a time in
  submission order and is never re-entered, and `Runtime.drain()` flushes the queues
  and stops the workers. Sinks remain a lossy tap by design; a consumer that must
  see every event reads the event store, which is the complete copy.

### Removed
- `EventStorePort.list_log_keys` (not yet part of any stable public API), along
  with the log-by-log pending scan that was its only caller. `list_runs` answers
  the same question without enumerating logs first.

### Fixed
- Duplicate-resume protection now holds between processes, not just between tasks
  in one process. Two servers (or a server and a second tool) sharing one SQLite
  event store can answer the same interrupt at the same instant and exactly one of
  them resumes the run; the other is a clean no-op, not an error. Previously the
  guard was a process-local lock, so each process could claim the same waiting run
   -  running the workflow's next node twice and writing two `run.resumed` events
  with the same `seq`. A claim that was slow enough to miss a whole
  interrupt-resume-interrupt round of its run now loses too, instead of answering
  the run's *second* question with the first one's value.
- The OpenAI Agents engine no longer runs the SDK's default trace exporter on
  keyless/fake-model runs (tests, CI, the M0 demo): it now passes a `RunConfig`
  with tracing disabled unless `AGENTDECK_OPENAI_AGENTS_TRACING_ENABLED=true` is
  set, so a bare checkout no longer logs a non-fatal `Tracing client error 401`
  or attempts an unsanctioned outbound HTTPS call.

## [2.0.0b2] - 2026-08-05

Second beta of the v2 line: both real engines now run behind the
engine-agnostic core  -  the same event stream, store, and surfaces serve
OpenAI Agents chats and LangGraph workflows alike. The v1 public surface
remains byte-for-byte unchanged.

### Added
- LangGraph engine adapter (`agentdeck.adapters.engines.langgraph`): runs graph
  workflows behind `EnginePort`, surviving process restarts  -  an interrupted run's
  status and resume point persist, and resuming continues the same event sequence
  with no duplicates. A new `GET /pending` / `POST /resume` surface lists and
  answers interrupted runs; resuming the same run twice at once resolves exactly
  once, and resuming a run that already finished is a no-op rather than an error.
- OpenAI Agents engine adapter (`agentdeck.adapters.engines.openai_agents`):
  runs a pre-built `agents.Agent`  -  handoffs and tools included  -  behind
  `EnginePort`, streaming the canonical events (`text.delta`,
  `message.completed`, `tool.call.started`/`.completed`, and a namespaced
  `custom` event per handoff). SDK session keys are tenant-scoped, so two
  tenants reusing the same session id never share a conversation.
- SQLite event store (`agentdeck.adapters.stores.sqlite`): a durable event
  log with the same append-only, per-run-`seq` contract as the in-memory
  store.
- Minimal v2 surfaces (`agentdeck.surfaces.serve`, `agentdeck.surfaces.cli`):
  an SSE route and a compact CLI chat renderer for v2 runs  -  speakers are
  distinguished by `origin` + `message_id` alone, and transcripts rebuild
  from `message.completed` events without delta assembly. The v1 server is
  untouched.
- Docs site: a Concepts section (agents, capabilities, skills, workflows)
  written against the shipped v1 surface, and the AgentDeck brand  -  palette,
  type scale, self-hosted fonts so the exported site makes no external
  requests.

### Removed
- Docs site: the empty Guides and Examples sections; each returns when its
  first real page exists.

## [2.0.0b1] - 2026-08-05

First beta of the v2 line: the engine-agnostic core and the Runtime land
alongside the shipped v1 harness. **The v1 public surface is unchanged**  -
`App`, `run_agent`, `run_workflow`, `chat`, `chat_stream`, the `./.agentdeck/`
project layout and the SSE wire format are byte-for-byte what 1.2.1 served.
The bump marks the start of the v2 rebuild, not a break in what already works.

### Added
- `agentdeck.core`: canonical event schema v1  -  a closed eight-field `Event`
  envelope over payload classes discriminated by `kind`, `parse_event()`
  tolerating unknown kinds and fields, content blocks, and the
  `check_contiguous` / `check_terminal` ordering invariants. Nothing imports
  it yet; v1 runtime behavior is unchanged.
- `agentdeck.core`: `RunContext` (the run's identity and limits, passed to
  every port), `InvocableSpec` / `InvocableKind` (one noun for agents,
  workflows and skills), and the first three ports  -  `EnginePort`,
  `SessionStorePort`, `EventSinkPort`.
- `agentdeck.runtime.service.Runtime`: the v2 run loop  -  stamp the envelope,
  append to the log, fan out to sinks, yield, in that order, so an event a
  consumer has seen is already persisted. Every run is closed in the log: an
  engine that raises or stops early gets `run.failed`, an abandoned stream
  gets `run.cancelled`, and nothing follows a terminal event. Sinks never
  stall a run; `Runtime.drain()` flushes in-flight emits at shutdown.
- In-memory event store (`agentdeck.adapters.stores.memory`) and scripted
  stub engine (`agentdeck.adapters.engines.stub`)  -  the stub is the reference
  implementation of the engine contract.
- Cross-engine contract test suite (`tests/contract/`): first event at
  `seq` 0, contiguous `seq`, exactly one terminal event and it is last,
  persist-before-yield. Every engine added later inherits it.
- Golden wire baselines (`tests/golden/`): byte-level snapshots of the v1
  HTTP/SSE surface, replayed on every test run; re-recorded only
  deliberately via `make golden`.
- Import-linter contracts wired into `make check` / CI, enforcing the
  architecture's import boundaries.
- Docs site: working search (a Pagefind index built at export, guarded by a
  CI check) and anti-rot tests  -  published Python samples are parsed and
  their imports resolved, links must resolve, and navigation must match the
  pages.

### Fixed
- Docs site: Getting Started installs from a git tag and documents provider
  configuration instead of describing a contributor clone; the overview's
  examples run as printed; `.env.example` no longer claims a legacy default
  for `OPENAI_BASE_URL` (empty means the SDK default).

## [1.2.1] - 2026-08-03

No changes to the `agentdeck` package itself  -  this version covers the
documentation platform and its CI.

### Added
- `docs-site/`: MDX documentation platform (Nextra 4, Next.js App Router),
  statically exported to GitHub Pages under `/agentdeck`  -  deployed on
  release, build-checked on every PR that touches it.

### Fixed
- Docs build no longer fails to prerender (zod pinned to 4.3.5 via
  `overrides`, lockfile committed, workflows install with `npm ci`).
- "Edit this page" links point at `dev` instead of a feature branch.

## [1.2.0] - 2026-07-28

### Added
- `AGENTDECK_LANGFUSE_BASE_URL`: Langfuse 4.x endpoint name; wins over
  `AGENTDECK_LANGFUSE_HOST` (kept as the legacy alias) and is mirrored to
  sandboxed skills as both `LANGFUSE_BASE_URL` and `LANGFUSE_HOST`.

## [1.1.0] - 2026-07-27

### Added
- `BaseAgent.handoffs` entries may be a `str` registry name, resolved lazily
  at `build()` time  -  two agents that hand off to each other no longer need
  to import each other's module. Unknown names raise `NotFoundError` naming
  the available agents; mutual handoffs resolve without recursing forever.
- Durable timer waits: `agentdeck.workflows.sleep_until(when)` pauses a
  `durable = True` workflow node until a timezone-aware wall-clock moment.
  `App.due_resumes()` lists timer threads whose wake time has passed;
  `App.tick()` resumes every due thread. Callers own the scheduling cadence
  (cron, systemd timer, a loop)  -  agentdeck runs no daemon. Naive datetimes
  are rejected with a clear `ValueError`.

### Fixed
- `App.chat(..., session_id=...)` turns now carry that session id on the
  root Langfuse trace instead of always tracing with a null session  -
  per-customer trace grouping was silently broken.

## [1.0.0] - 2026-07-27

### Added
- Human-in-the-loop for `durable = True` workflows: a node calling
  `agentdeck.workflows.interrupt(payload)` pauses the run; `run_workflow`
  returns `{"type": "interrupt", "payload": ..., "thread_id": ...}` instead
  of a final state. `App.resume_workflow(name, thread_id, value)` answers it;
  `App.pending_interrupts()` lists every thread still waiting. Same trio on
  `BaseWorkflow` as `run` / `resume` / `pending`.
- `GET /workflows/{name}/pending` and
  `POST /workflows/{name}/{thread_id}/resume`; `POST /workflows/{name}`
  takes an optional `thread_id` query parameter so durable runs can start
  over HTTP.
- `App.run_workflow_stream(name, state=None, thread_id=None)`: async
  iterator yielding a `node_update` event per completed node, a `custom`
  event per stream-writer call, then one terminal `done` event with the
  final state. A paused run ends with an `interrupt` event in place of
  `done`, over HTTP too (`POST /workflows/{name}?stream=true`).
- `AgentNode` forwards its nested agent's text deltas into the workflow's
  custom stream, so a workflow-driven chat streams tokens the same as a
  direct agent chat.
- `subagents = [...]` on `BaseAgent`: an opt-in `spawn_subagent` tool that
  lets the model delegate a one-shot task to another registered agent
  (isolated run, no shared history, depth-limited). Disallowed or nested
  spawns return an `error: ...` string instead of raising.

### Changed
- **Breaking:** `.agentdeck/` project layout now uses top-level type
  subdirectories  -  `agents/<bundle>/agent.py` and
  `workflows/<bundle>/workflow.py`. `skills/*/SKILL.md` is unchanged. No
  migration shim: an old-layout project raises `ConfigError` pointing at the
  new paths instead of silently discovering nothing.
- A non-durable workflow whose node calls `interrupt()` raises `ConfigError`
  instead of silently returning an unresumable state.

### Fixed
- Building a `durable=True` sqlite workflow from sync code no longer raises
  `RuntimeError: no running event loop`.

## [0.2.0] - 2026-07-26

### Added
- `App.chat_stream(name, session_id, message)`: async iterator of text
  deltas with a terminal `StreamDone(final_output, usage)`, same session
  semantics as `chat()`; the run is cancelled cleanly when the iterator is
  closed or abandoned.
- `POST /agents/{name}/chat?stream=true`: `text/event-stream` response with
  incremental `delta` events and a final `done` event; mid-stream failures
  emit an `error` event; invalid requests are rejected with 422 before the
  stream starts. Sent with anti-buffering headers for proxies.
- `agentdeck/errors.py`: one exception hierarchy  -  `AgentdeckError` base,
  `NotFoundError`, `SkillError` (with `SkillExecutionError`, `SkillEnvError`),
  `ConfigError`.
- `App.open()` async context manager and idempotent `App.aclose()`;
  `agentdeck-serve` wires them through a FastAPI lifespan so SIGTERM shuts
  down Redis and MCP servers cleanly.
- `App(session_factory=...)` DI seam for tests.
- Workflow durability: `BaseWorkflow.durable = True` compiles the graph with
  a checkpointer from the new `AGENTDECK_CHECKPOINT_*` settings (`sqlite` |
  `postgres` | `memory`); `run_workflow` / `BaseWorkflow.run` accept
  `thread_id` so a run can resume, including across a real process restart.
  New optional `[durability]` extra with a clear `ImportError` when missing.

### Changed
- The streamed `done` event's `"output"` is the SDK's `final_output`
  (matching non-streamed `chat()`), not re-joined text deltas.
- `agentdeck-serve` answers `503` before startup completes instead of
  raising; `NotFoundError` maps to 404; other errors return a fixed 500 body
  with the detail logged server-side instead of echoed to the client.
- Registries raise `NotFoundError` instead of bare `KeyError`; invalid
  configuration raises `ConfigError` instead of `ValueError`; skill failures
  raise `SkillExecutionError` / `SkillError` instead of bare `RuntimeError`.

## [0.1.0] - 2026-07-26

### Added
- `App` single entry point: discovers and builds agents / workflows / skills
  from the `./.agentdeck` project dir; `run_agent`, `run_workflow`, `chat`
  (session memory via Redis or in-process SQLite fallback), `session_for`.
- `agentdeck-serve` FastAPI surface: `/health`, `/agents/{name}/chat`,
  `/workflows/{name}` (`[serve]` extra).
- `web_search` function tool (Tavily-backed, model-agnostic).
- `runtime/capture.py`: the `Capture` / `CaptureActor` / `CAPTURE_ENV`
  host↔sandbox wire contract.
- Packaging: pyproject with `serve` / `dev` / `observability` extras,
  Makefile, Dockerfile + compose (app + Redis), `.env.example`, pre-commit,
  CI + tag-driven release workflow.

### Changed
- Extracted from SysAgentsHarness and renamed: package `sysagent` →
  `agentdeck`, env prefixes `SYSAGENT_*` → `AGENTDECK_*`.
- Neutralized donor defaults (private endpoints, model, MCP hosts); empty
  `OPENAI_BASE_URL` means the SDK default.
- Pinned `openai==2.32.0` to match `openai-agents==0.17.0` (2.33+ crashes
  the run loop).
- `BaseAgent.run()` is a one-shot headless run.

### Removed
- Dead donor code: `backends/`, `db/`, `DevRunner`, `runtime/events.py`,
  `runtime/tools.py`, `PluginRegistry.pick`, `skill_runtime` LLM/batch
  helpers; deps typer, rich, prompt-toolkit.

[Unreleased]: https://github.com/agentdecksdk/agentdeck/compare/v5.0.0...HEAD
[5.0.0]: https://github.com/agentdecksdk/agentdeck/compare/v4.0.5...v5.0.0
[4.0.5]: https://github.com/agentdecksdk/agentdeck/compare/v4.0.4...v4.0.5
[4.0.4]: https://github.com/agentdecksdk/agentdeck/compare/v4.0.3...v4.0.4
[4.0.3]: https://github.com/agentdecksdk/agentdeck/compare/v4.0.2...v4.0.3
[4.0.2]: https://github.com/agentdecksdk/agentdeck/compare/v4.0.1...v4.0.2
[4.0.1]: https://github.com/agentdecksdk/agentdeck/compare/v4.0.0...v4.0.1
[4.0.0]: https://github.com/agentdecksdk/agentdeck/compare/v3.1.0...v4.0.0
[3.1.0]: https://github.com/agentdecksdk/agentdeck/compare/v3.0.1...v3.1.0
[3.0.1]: https://github.com/agentdecksdk/agentdeck/compare/v3.0.0...v3.0.1
[3.0.0]: https://github.com/agentdecksdk/agentdeck/compare/v3.0.0b1...v3.0.0
[3.0.0b1]: https://github.com/agentdecksdk/agentdeck/compare/v2.0.0...v3.0.0b1
[2.0.0]: https://github.com/agentdecksdk/agentdeck/compare/v2.0.0b4...v2.0.0
[2.0.0b4]: https://github.com/agentdecksdk/agentdeck/compare/v2.0.0b3...v2.0.0b4
[2.0.0b3]: https://github.com/agentdecksdk/agentdeck/compare/v2.0.0b2...v2.0.0b3
[2.0.0b2]: https://github.com/agentdecksdk/agentdeck/compare/v2.0.0b1...v2.0.0b2
[2.0.0b1]: https://github.com/agentdecksdk/agentdeck/compare/v1.2.1...v2.0.0b1
[1.2.1]: https://github.com/agentdecksdk/agentdeck/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/agentdecksdk/agentdeck/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/agentdecksdk/agentdeck/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/agentdecksdk/agentdeck/compare/v0.2.0...v1.0.0
[0.2.0]: https://github.com/agentdecksdk/agentdeck/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/agentdecksdk/agentdeck/releases/tag/v0.1.0
