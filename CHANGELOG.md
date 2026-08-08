# Changelog

All notable changes to agentdeck. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow
[SemVer](https://semver.org/). Entries are user-facing — what changed for
someone using the package, in `Added / Changed / Deprecated / Removed /
Fixed / Security` order — and are written to be attached to a release as-is.

## [Unreleased]

v2.0.0 shipped with an explicit compatibility promise for v1's Python API; this entry
starts retiring it, one PR at a time (#137). First slice: `App`'s turn-starting methods
now play on the same Runtime the HTTP surface always has, so a Python caller's turn is
recorded — every bit of it — in the same event log a running server would show, instead
of vanishing the moment the call returns.

### Changed

- The Runtime now plays every turn on the real engine adapters — `OpenAIAgentsEngine` and
  `LangGraphEngine` — instead of the v1 compatibility subclasses that stood in for them, and
  `agentdeck.v1bridge` is removed. What a run is configured with (model provider, CA bundle,
  temperature, turn and token caps, workflow name) is now resolved at the composition root
  and handed to the adapter, so a caller can wire a different endpoint without touching
  process state. Behavior is unchanged: the same settings resolve to the same run config,
  pinned field by field by `tests/test_run_config_parity.py`.
- A workflow's `durable = True` now travels to the engine on its spec, and the configured
  checkpointer is built at the first durable run rather than when a Runtime is assembled —
  so naming a `sqlite`/`postgres` backend still costs a project that only chats nothing, and
  the `[durability]` extra stays optional.
- **Breaking:** `App.session_for(session_id)` now returns the engine's own session for that
  id, keyed by tenant (`local:<session_id>`) the way every other entry point already keys it.
  One conversation is now one conversation whether the turn arrived through `App.chat` or
  through HTTP — and a Redis-backed deployment gets its sessions on the Runtime path, which
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
  equivalent forwarder for `MCPLifecycle`, is removed the same way — import it from
  `agentdeck.adapters.tools.mcp.lifecycle`.
- **Breaking:** `OpenAISettings.tracing_api_key` (`OPENAI_TRACING_API_KEY`) is removed — it
  was never read anywhere in the codebase.
- **Breaking:** `agentdeck.runtime.workspace.runtime_capture` and `current_capture` are
  removed. Nothing ever bound the ContextVar behind them, so `current_capture()` always
  answered `None`; a run's identity now reaches telemetry through the event envelope.
- **Breaking:** the sandbox is a port. `agentdeck.runtime.workspace` and its `Workspace` class
  are removed, replaced by `SandboxPort` (`agentdeck.core.ports.sandbox`) and the
  `agentdeck.adapters.caps.sandbox` adapter that implements it. Open one with
  `async with open_sandbox(...) as sandbox:` instead of `Workspace.open(...)`, and reach the
  ambient one with `require_sandbox()` instead of `Workspace.require()`. The port carries only
  what callers actually use — `read_text`, `write_bytes`, `mount_dir`, `exec` — so
  `write_text`, `write_output`, `read_output`, `output_path` and `OUTPUT_FILES_DIR` are gone
  (nothing in the package or its tests called them), `exec` no longer takes `shell`, and
  mounting a host directory now grants access to it in the same call rather than requiring a
  separate `extra_path_grants=`. `materialize()` and `input_file_entries()`, which took the
  Agents SDK's own manifest-entry types, are replaced by `mount_dir()` and
  `input_file_targets()`; `Workspace.open`'s unused `capture`, `client` and `client_factory`
  arguments are gone. A sandbox's environment is unchanged, including the rule that
  host-supplied trace carriers win over a caller's stale copy.
- An agent turn no longer opens its Langfuse observation inside the engine — the sink builds
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
  compiled graph directly — every workflow turn is recorded, and a second concurrent call on
  the same `thread_id` now raises `SessionBusyError` instead of racing the first. A workflow
  with no `state` argument keeps defaulting to no updates.
- `App.run_agent`, `App.chat`, `App.chat_stream`, `App.run_workflow` and
  `App.resume_workflow` compose the Runtime on first use (calling `load()` themselves) if
  `App.load()` was never called by hand.
- **Breaking:** run control's vocabulary and safe point moved out of the ports package to
  `agentdeck.core.control` — `Signal`, `ControlSignal`, `Gate`, `ControlSignalled`,
  `RunCancelledError`, `RunPausedError` and `CONTROL_POLL_INTERVAL`. Only `ControlPort`, the
  transport an adapter implements, stays in `agentdeck.core.ports`. Import from
  `agentdeck.core.control` instead; nothing about how control behaves changed.
- `InvocableSpec` and `ToolSet` now raise on a keyword they don't have, instead of dropping it.
  Both are built in-process, so an unknown keyword is a typo — and a dropped `tools=` used to
  yield an empty `ToolSet`, degrading a run exactly like an unreachable tool source. Event and
  content payloads keep ignoring unknown fields: they are parsed off a wire, where a field a
  newer writer added has to land rather than raise.

### Added

- **Langfuse now traces workflow runs.** `build_runtime` registers the Langfuse sink itself
  when `AGENTDECK_LANGFUSE_PUBLIC_KEY` and `AGENTDECK_LANGFUSE_SECRET_KEY` are both set, so
  every run played through a Runtime — workflow as well as agent — becomes a trace built from
  the run's own events, carrying its session id, its principal as the Langfuse user, its
  nodes, its tool calls and its token usage. Workflow runs previously produced either no trace
  or an anonymous one. Nothing is registered and the Langfuse SDK is never imported without
  both keys, so the `[observability]` extra stays optional. Pass `sinks=()` to `build_runtime`
  to opt out.
- **`App.store`**: the event log every recorded turn appends to. Read a turn back with
  `await app.store.read(log_key, ctx)`, where `log_key` is a `TurnResult`'s `session_id` (or
  `run_id`, for a session-less run).
- Docs: `reference/settings.mdx` and `reference/cli.mdx` are now generated from the code —
  every `AGENTDECK_*` (and `OPENAI_*`/`TAVILY_*`/`SKILL_*`) setting and the `agentdeck` CLI's
  own `--help` output — and verified against the code on every `make check`, so the published
  pages cannot drift from what the package actually does (#133).

### Changed

- Every `LayeredSettings` field in `agentdeck/runtime/settings.py` now carries a
  `Field(description=...)`, the source the new generated settings reference renders from.
- **Breaking:** `parse_event` is removed. `Event.model_validate(data)` does the same job —
  an unfamiliar `kind` still lands as `UnknownEvent` rather than raising — so the forward-
  compatibility promise is now a property of the type instead of something a reader has to
  remember to call. Replace `parse_event(row)` with `Event.model_validate(row)`.
- Two `kind` values that disagree are refused instead of silently relabelled. When the
  envelope's `kind` was one this version didn't know, the payload's own claim used to be
  overwritten with the envelope's and buried in `raw_payload`, so a row was accepted under a
  name it never carried. Only reachable from rows this package didn't write.
- `Event.kind` and `UnknownEvent.kind` now have to look like a kind (`run.started`,
  `a2a.task.started`); `""`, `"Run Started"` and `"run..started"` were accepted before. A
  shape, not a fixed set — an unfamiliar kind from a newer writer still parses.
- Every free-form JSON field holds only what a store hands back unchanged:
  `NodeUpdated.state_patch`, `ToolCallStarted.args`, `RunInterrupted.payload`, `Custom.data`,
  `UnknownEvent.raw_payload` and `UnknownBlock.raw_block`. All six were `dict[str, Any]`, so a
  `NaN` reached the log as `null`, a set as a list and a datetime as a string — the divergence
  `DataBlock` has always refused. They now carry the same `JsonData` type `DataBlock` does.
  The two `raw_*` fields matter most: `UnknownEvent` and `UnknownBlock` exist so this version
  survives a newer writer, which they cannot do while free to alter that writer's data on the
  way through. Every engine adapter already sanitized before constructing these, so nothing the
  package produces changes; a caller building one by hand from non-JSON values now gets a
  `ValidationError`.
- The cost and budget fields validate like the token counts always did: `Usage.usd`,
  `Budget.max_usd` and `Budget.max_tokens` reject negatives, and the two dollar fields also
  reject `NaN` and `±Infinity`. Those have no JSON literal, so they serialized as `null` — a
  consumer read *no cost* where the producer wrote nonsense. Nothing in the package produced
  such a value, so this closes a trap rather than fixing a live bug; a caller that built a
  `Usage` or `Budget` by hand with one now gets a `ValidationError` at construction. No
  serialized shape changed.
- `POST /v2/invocables/{name}/chat` answers **422** to an empty `session_id` instead of
  accepting it. A run's log key is `session_id or run_id`, so `""` was not an error anywhere
  downstream — it quietly gave the turn a private log, and the caller's next message found no
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
  datetime` — the store owns the clock, so only it can subtract from its own now. The four
  bundled stores are unchanged in behavior; a store built outside this package needs porting.
  `read`, `read_run`, `list_runs` and `run_status` are untouched.
- `Runtime(clock=...)` and `build_runtime(clock=...)` no longer decide anything. Every event's
  `ts` is assigned by the store, so a caller that wants to hold time still builds the store
  with a clock — `MemoryEventStore(clock=...)`, `RedisEventStore(clock=...)` — while the SQLite
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
  otherwise completed cleanly left a hole in its sequence — and a consumer seeing that hole
  could not tell "an event was lost in transit, refetch it" from "this gap is permanent and
  refetching will never converge". A number is now allocated and persisted together, so it
  cannot be allocated and not persisted, and `check_contiguous` is the loss check it is
  documented to be.

### Known limits

- `App.run_workflow_stream` is unchanged: it still drives the compiled graph directly and
  writes nothing to the event log, so a run started there cannot be found — and so cannot be
  resumed — by the now-Runtime-backed `resume_workflow`. Start on `run_workflow` (or
  `resume_workflow`) instead if one thread needs both the log and a live stream.
- `App.tick()` and `App.due_resumes()` still resume a paused workflow through its LangGraph
  checkpointer rather than the Runtime (#120), so a timer-paused run started through the new
  `run_workflow` is resumed outside the log: its own log entry stays `WAITING_HUMAN` until
  `stale_run_after` reclaims it.

## [2.0.0] - 2026-08-06

The release where agentdeck becomes a platform rather than a harness. Every turn — chat
or workflow — now runs on one Runtime and leaves one canonical event log behind, which is
what makes the rest of this list possible: a run you can pause, resume or cancel from
another process; an approvals inbox that survives a restart; a log you can point at
Postgres or Redis and share between workers; status and progress a client can render
instead of inferring. The v1 Python API, the `.agentdeck/` layout and the SSE wire are
unchanged and verified against recorded baselines, so a v1.2.1 project keeps working.

Known limits worth reading before you upgrade, each with an issue rather than a footnote:
run control covers **agent** runs — a workflow run has no safe point yet, so it pauses
through its own interrupt/resume instead (#128). Telemetry still flows through v1's
tracer, not the event-stream sink, so the b4 note claiming Langfuse covered workflow runs
described a sink nothing had wired (#124). The HTTP approvals inbox and
`App.pending_interrupts()` read different sources and will disagree if you drive
approvals through both (#120). There is still no auth on the endpoints (#25) and no
tenancy — one tenant, one principal.

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
  point, then the effect — `run.paused` / `run.cancelled` / `run.resumed`. Watch the
  events for the effect to learn that it stopped. A request leaves the run
  `running`; only the effect moves it.

  **Nothing is force-killed.** A signal is honored at the next safe point — between
  two stream items, so the chunk in flight is always delivered whole — and a tool
  call already running is never interrupted: the call finishes and the run stops
  before the step that would have used its result. `safe_point` on
  `control.observed` is what distinguishes "cancel took eight seconds" from "cancel
  took eight seconds *because a tool call did*".

  **A paused run is suspended in the log, not parked in a process.** The worker is
  free to exit, and any worker sharing the event store can lift the pause. Because
  there is no stack to return to, resuming re-enters the engine with the run's own
  input and the log as history: same `run_id`, `seq` carrying on. **Work the paused
  turn had already done can therefore happen again** — the model is asked again, and
  a tool it had already called may be called a second time, so keep tools idempotent
  and put side effects behind `ctx.idempotency_key`. Exactly one caller can resume a
  paused run; a second gets nothing rather than a second turn. Cancel is terminal
  and cannot be resumed, and `paused` stays distinct from `waiting_human` (that one
  resumes *with* a value).

  **Cancelling a paused run works, with one caveat worth knowing.** Pause, think,
  give up is the ordinary path, and a cancel recorded against a paused run is
  honored by the next resume — which ends the run `cancelled` rather than playing it
  on, so a resume can never quietly override whoever cancelled. But a paused run has
  no loop reaching safe points, so nothing else can turn that request into an effect:
  a paused run that nobody ever resumes stays paused, holding its session until
  `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS` takes it over.

  **Races are no-ops, never errors.** A signal that arrives after the run ended does
  nothing and records nothing; the same pause sent twice is one request; resuming a
  run that is not paused returns nothing (409 over HTTP).

  Pending signals live in a control port, which is **in-process by default** — one
  worker can control its own runs, a second worker cannot see them. Set
  `AGENTDECK_CONTROL_BACKEND=sqlite` and `AGENTDECK_CONTROL_URL=<file>` for signals
  that cross processes, which is also the file
  `agentdeck runs signal <run_id> <cancel|pause|resume> --control-db <file>
  [--reason ...]` writes to. Agent runs honor safe points today; a workflow
  (LangGraph) run has none yet, so pause and cancel do not reach one.
- **A run can say what it is doing** — two new event kinds, `status.reported` (a
  human-readable line: `"Searching GitHub"`) and `progress.reported` (a named stage,
  optionally counted: `step="Reviewing issues", current=2, total=4`), so a client can
  show a long run's activity instead of inferring it from tool calls. Both are
  **advisory**: they carry no meaning for the platform, and a run's status still
  folds from its lifecycle events alone — a run that reports is still `RUNNING`, and
  neither kind is terminal.
  Emitters reach the stream through the run context, which they already have:
  `await ctx.reporter.status("Searching GitHub")` and
  `await ctx.reporter.progress("Reviewing issues", current=2, total=4)`. An
  openai-agents function tool gets that context as the SDK's own — declare a first
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
  runs — enough for a client to show what a run has been doing, not enough to narrate a
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
  "we asked it to stop" and "it stopped" are finally different facts in the log —
  under cooperative control they can be seconds apart, and `safe_point`
  (`stream_item`, `tool_dispatch`, `node_boundary`) says what the run was in the
  middle of. One pair of kinds carries every verb — `cancel`, `pause`, `resume`,
  `steer` — so pause/resume and mid-run steering add no further vocabulary when
  they ship. Neither kind is a status transition: a request leaves a run `RUNNING`
  until its terminal event says otherwise, and neither is terminal.
  The vocabulary landed first and the producers followed in the same release (see
  the pause/resume/cancel entry above): a run that is signaled now emits both
  kinds. The CLI renderer prints both phases and the Langfuse sink puts them on the
  run's timeline.
- `run.resumed` now carries **the answer it was resumed with**, as content
  (`value: list[ContentBlock] | None`) — content passes through as sent, a string
  arrives as a `TextBlock`, any other JSON answer as a `DataBlock`, and lifting an
  operator's pause carries nothing. Stored **in full**, like a run's own input,
  because a truncated answer cannot be replayed. This is what makes a
  previously unrecoverable window *repairable*: the single write that moved a run
  from `waiting_human` to `running` recorded *that* it was answered and not *what*
  the answer was, so a process dying between that write and the engine consuming
  the value left the log saying `running` while the engine was still parked at its
  interrupt — every later resume then rejected as stray, with no recovery but a
  manual one. The answer is now in the log at the instant the claim commits, before
  the engine is asked for anything, so a successor process has what it needs. **The
  repair itself is not built here** — nothing yet reads `value` back to bring an
  engine into line — so treat this as the prerequisite, not the fix. An answer JSON
  cannot carry (an arbitrary object, a `datetime`, `NaN`) is logged as a warning and
  recorded as no value rather than failing a resume that would otherwise work; such
  a run keeps the old stranding risk.
  Compatible in both directions, and measured rather than assumed: a `run.resumed`
  written before this release still parses (no `value` means none), and a 2.0.0b4
  reader handed one of the new events parses it and drops the field it does not
  know — no listing or dashboard outage like the one `DataBlock` caused, and the
  new kinds arrive as unknown kinds a consumer skips. The one caveat is what that
  dropping implies: only a process new enough to *see* `value` can use it to
  repair a resume, so upgrade the workers that reconcile before relying on it.
- **`UnknownBlock`** (`agentdeck.core`): a content block of a type this version
  doesn't recognize now falls back to `UnknownBlock(type, raw_block)` — keeping the
  raw block for a store to hold and a consumer to skip — instead of rejecting the
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
  answer was "no" 499 times — one file read each with the SQLite control port, and a
  network round trip each once the port is shared. Measured at a real model's pace
  (~30ms a chunk), a 400-chunk answer now costs 58 reads instead of 400.
  What this trades is **latency, not correctness**: a cancel is noticed up to 200ms
  after it is recorded, and still acted on *at* a safe point, never mid-token. The
  first safe point of a run always reads, so a signal that beat the run out of the
  gate is honored immediately. Anyone who was relying on the previous
  read-every-item behavior — a test asserting a cancel lands within a stream shorter
  than 200ms, for instance — can pass `Runtime(..., control_poll_interval=0)` to get
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
  deadline read off a clock and never a wait — a run is not slowed by a sink's outage or by
  its recovery — and nothing is replayed: the events the outage covered are
  still lost, and still counted as drops. A sink therefore needs no retry logic
  of its own for a transient outage, and one that cannot lose events reads the
  event log, which is the complete copy.
- **A flapping sink can no longer flood the log with stack traces.** Failure
  logging was rate-limited per failure *streak*, which bounded nothing for a
  sink that fails every other event — each success reset the streak, so every
  failure printed a fresh traceback and the run's length decided the log
  volume. Tracebacks are now limited to one per sink per 60 seconds, and each
  one reports how many failures went unlogged since the last, so a throttled
  log still says how much it is standing in for. The breaker's disable decision
  is unchanged by this.
- **The workflow HTTP endpoints run on the v2 Runtime.** `POST
  /workflows/{name}/run`, `GET /workflows/{name}/pending` and `POST
  /workflows/{name}/{thread_id}/resume` were the last surface still calling v1's
  runner directly, so a workflow turn left **no event log behind at all** — it
  streamed to the caller and vanished. Every workflow turn is now recorded like a
  chat turn: one run in the log, node updates, stream writes, interrupts and the
  final state, readable by the same listings, replays and dashboards. The wire is
  unchanged — the same `node_update` / `custom` / `interrupt` / `done` SSE frames
  and the same JSON bodies, checked against the recorded baselines rather than by
  inspection.
  Three consequences worth knowing before upgrading. A workflow's `thread_id` is
  now its **session**, and a session runs one turn at a time, so posting a second
  run to a thread whose previous turn has not finished answers **409** instead of
  interleaving two turns over one graph state. Read "not finished" broadly: a
  thread sitting *idle* on an unanswered approval is not finished either, and holds
  its session until somebody answers it — so the case an approval UI actually hits
  is a 409, for as long as the approval goes unanswered (or until that run has been
  silent for `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS`, one hour by default,
  after which the next turn takes the session over). A resume against a thread with
  no paused run answers **404** where it previously surfaced v1's runner error.
  And a `get_stream_writer()` write now reaches the log as a namespaced `custom`
  event (`langgraph.stream_write`) on its way to the unchanged `custom` frame.
  A `durable = True` workflow still resumes on the configured checkpointer — the
  bridge plays v1's own compiled graph, which carries it — and the `[durability]`
  extra stays optional for a project that only chats.
- **The HTTP approval inbox and `App.pending_interrupts()` are now separate sources
  of truth**, and will be until they are joined. `GET /workflows/{name}/pending`
  and the HTTP resume project the event log; `App.pending_interrupts()`,
  `App.due_resumes()` and `App.tick()` still read the graph's checkpointer. They
  agree only as long as one of them is used: an interrupt created headlessly is
  invisible over HTTP, and one answered headlessly leaves an entry behind in the
  HTTP listing. Answering such a leftover entry over HTTP is a **404** rather than
  the stale final state a replayed thread would otherwise hand back, so no answer
  is silently dropped — but a deployment that drives approvals through both doors
  will see the two listings disagree. Joining them — routing the Python API's inbox
  through the Runtime too — is tracked in #120.
- The v2 `LangGraphEngine` (not v1's endpoints, whose final state always came from
  `ainvoke`) now reports a final state for a graph compiled **without** a
  checkpointer, which it previously could not: the terminal state is read from the
  run's own event stream instead of from a checkpoint that never existed.

### Removed
- **`agentdeck.runtime.REPO_ROOT` / `agentdeck.runtime.settings.REPO_ROOT`** — it only
  ever pointed at the repo root in a source checkout and at the installed package's
  `site-packages` directory otherwise; nothing in agentdeck needs that path, and
  nothing outside it should have depended on it either. (#16)
- **`agentdeck.runtime.ENV_FILE` / `agentdeck.runtime.settings.ENV_FILE`** — was a path
  frozen at import time (see Fixed below for why that was itself unsafe); replaced by
  `resolve_env_file()`, resolved fresh every time `get_settings()` actually builds a
  `Settings` object. (#16)

### Fixed
- **`.env` and `config.yaml` now resolve from the project's current working
  directory, not from wherever `agentdeck` itself is installed.** Previously
  both were located relative to `runtime/settings.py`'s own file path, which
  is the repo root in a source checkout but lands inside `site-packages` once
  `agentdeck` is `pip install`ed as a dependency — so a consumer project's
  `.env` (API keys, `OPENAI_MODEL`, …) was silently ignored, typically
  surfacing as `OPENAI_API_KEY ... must be set` despite a valid `.env` in the
  project. **If you were exporting the same values as real shell/CI
  environment variables to work around this, nothing changes** — a real env
  var still outranks the file. But if you have a `.env` sitting unused next
  to an installed `agentdeck`, it will now take effect. `.env` is also now read
  at first use rather than at `import agentdeck` time, so a `chdir` between
  importing the package and first building settings still resolves against the
  right project. (#16)
- Two bundles of the **same kind** (two agents, or two workflows) exporting a
  class of the same name used to collapse silently into one invocable, in
  sorted bundle order — copying `agents/greeter/` to `agents/greeter-v2/` to
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
the chat endpoints are served by the v2 Runtime with a byte-identical wire — same
SSE frames, same JSON bodies, verified against the recorded baselines rather than
by inspection. Around that: an event log you can point at Redis or Postgres and
share between workers, a session that runs one turn at a time instead of letting
two answers overwrite each other, telemetry that covers workflows and flushes
what it buffered at shutdown, a canonical shape for structured data, and a turn
that repairs its own history after a crash between two writes. The v1 Python API
is unchanged and still runs its own path.

### Added
- **Redis and Postgres event logs** — `AGENTDECK_EVENTS_BACKEND=redis` or
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
  answer identically — one `seq` per run refused a second time included.
  Each keeps to its own keyspace — a Postgres schema (`agentdeck_events` by
  default, overridable with `schema=`) and a Redis key prefix
  (`agentdeck:events`, overridable with `prefix=`) — so a database or instance
  shared with LangGraph checkpoints or the agent-conversation store keeps the
  log separate, and either side can be dropped without touching the other.
  Redis needs no new dependency; Postgres needs the `[durability]` extra, which
  now also installs `psycopg[binary]` (nothing else pays for it — the driver is
  imported only when you select that backend). Three things to know when
  operating them: a Redis instance used as the record wants `appendonly yes`
  (the port promises an event a consumer has seen is already stored, and the
  default snapshot-only persistence can lose the last seconds of a log) **and**
  `maxmemory-policy noeviction` (this is a log, not a cache — an evicted key can
  cost a live run its session); and a store call that cannot reach its server
  raises `StoreError` rather than reporting a claim somebody else won.
- `DataBlock` (`agentdeck.core`): structured data is now content, alongside
  `TextBlock`, `ImageBlock` and `ResourceBlock`. `DataBlock(data=...)` carries any
  JSON value, so anywhere the v2 API takes or returns content blocks —
  `Runtime.run(...)`, `run.started.input`, `run.completed.output`,
  `input.appended` — a validated `output_type` result or a workflow's state
  travels as itself instead of being squeezed through text. Data that could not
  survive the wire (a `datetime`, a `set`, an arbitrary object, and `NaN` /
  `±Infinity` — floats with no JSON literal, which would otherwise be written as
  `null`) is refused at construction rather than failing later, or silently
  changing value, in a store or a trace. Text and data blocks are stored **in
  full**: they are the caller's own input and the run's own declared result, and a
  truncated copy cannot be replayed — only *tool* results stay bounded to a
  preview, size and hash.
  Additive for writers: no existing block, payload or field changed. **Not
  backward-compatible for readers, and wider than one event** — content blocks are
  a strict discriminated union, so a process running an older agentdeck cannot
  parse an event containing a `data` block at all. Because a run listing parses
  each run's last lifecycle event, one structured `run.completed` in a shared
  event store makes the older process's `list_runs` fail *for the whole tenant*,
  including runs it wrote itself — a listing or dashboard outage on the old half
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
  event log — `run.started`, text deltas, tool calls, per-model-call
  `usage.reported`, `run.completed` — so a chat turn is finally as inspectable as
  a `/v2` one. Sessions, Langfuse traces, sandboxes, `max_turns`, the model
  provider and every other setting resolve exactly as they did before, and a
  conversation is still one conversation whether the turn arrived through
  `App.chat` or over HTTP. The workflow endpoints still run on v1's workflow
  runner, unchanged.
- `App` is the composition root, and the wiring behind it is one function:
  `build_runtime(engines=...)` (`agentdeck.composition`) takes the parts — the
  invocable mapping, engines, event store, sinks, control port — and returns a
  wired `Runtime`, defaulting the mapping to discovery over `./.agentdeck` and
  the store to your settings. `App.load()` calls it and exposes the result as
  `App.runtime`, so an application that wants the canonical event stream for a
  project no longer has to assemble a Runtime by hand. `App.aclose()` drains the
  Runtime before closing Redis and MCP, so a sink registered through the seam
  flushes at shutdown instead of dying with the event loop — `App` registers none
  of its own yet, so today that drain is a no-op that keeps its own promise.
- `AGENTDECK_EVENTS_BACKEND` / `AGENTDECK_EVENTS_URL` (YAML: `events:`) choose
  where the canonical event log goes: `memory` (the default — no configuration,
  no files, and a log that lives and dies with the process) or `sqlite` with
  `url` pointing at a file, for a log that survives a restart, or `redis` /
  `postgres` for one several workers can share (see the entry above). The default
  never evicts and is lost on restart, so a long-lived server keeps every event it
  saw and re-reads the whole conversation each turn — `agentdeck-serve` says so
  once at startup rather than leaving you to find out.
- Langfuse tracing for **workflow** runs, not only agent runs
  (`agentdeck.adapters.telemetry.langfuse`). `langfuse_sink()` hands back an
  event sink — or `None` when Langfuse has no keys — to register where you build
  the v2 `Runtime`: `Runtime(..., sinks=[s for s in (langfuse_sink(),) if s])`.
  Each run becomes one Langfuse trace: the run itself is the trace, tool calls
  are spans carrying their arguments and their result preview, hash and size
  (an inline `data:...;base64,` payload in either is described, never sent —
  Langfuse would otherwise upload the bytes to its media store),
  workflow node updates are points on the timeline named for the node and the
  state keys it touched, and reported token usage becomes Langfuse generations
  so cost lands where the UI accounts it. It reads nothing but the event
  stream, so an agent run and a workflow run are traced by exactly the same
  code — and a run waiting on a human is visible while it waits, its answer
  continuing the same trace even when it arrives in another worker. Sessions
  map to Langfuse sessions and the run's principal to its user, so a
  conversation is one filter away. Configuration is the `AGENTDECK_LANGFUSE_*`
  settings you already have; with no keys, no sink is registered, and the
  Langfuse SDK is never even imported. Needs the `[observability]` extra. v1's
  tracing is unchanged — a v1 agent run with both paths active is reported
  twice.
- `InvocableRegistry` (`agentdeck.runtime.discovery`): the v2 Runtime's list of
  what it can run is now discovered from your `./.agentdeck/` project instead of
  written out by hand at every entry point. `InvocableRegistry(engines).load()`
  reads the same bundles v1 always has — `agents/<bundle>/agent.py`,
  `workflows/<bundle>/workflow.py` — and returns the name-to-invocable mapping
  `Runtime` takes, with each bundle pointed at the engine its shape belongs to.
  Adding an agent or a workflow to a project no longer means editing wiring code.
  An agent and a workflow claiming one name, and a project whose bundles need an
  engine the Runtime wasn't given, both fail at load with a message naming the
  offender, rather than at the moment somebody runs it. (Two bundles of the same
  kind exporting one class name still collapse to a single invocable, as in v1.)
  Skills are not discovered as invocables yet — no engine runs a `SKILL.md`
  bundle. v1's `App` and its discovery are unchanged.
- `ToolSourcePort` (`agentdeck.core.ports`): tools now arrive from a source
  behind one small interface — `resolve(spec)` hands back a `ToolSet` of the
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
  sink — after the sink's queued events have been handed over and its consumer
  retired — so a sink whose `emit` only buffers, which is what the emit contract
  pushes any sink with real work to do into, has a deterministic last chance to
  ship what it holds instead of hoping the process exits cleanly enough for an
  `atexit` hook to run. **Optional**: it defaults to doing nothing, so existing
  sinks need no change. What a sink may assume is now stated and enforced — `close`
  is called at most once, and no `emit` is ever started after it, not even by a
  consumer that outlived the cancellation retiring it. It is also called on a sink
  that never saw an event, since a process can shut down without running anything.
  One caveat worth knowing if your sink buffers: an `emit` that has not *finished*
  when the dispatch stops waiting for it still overlaps `close` — whether it
  swallowed the cancellation sent to end it, or simply awaits something while
  unwinding (an `await` in a `finally` or an `except`, such as salvaging a partial
  result). Read-`await`-clear inside `close` can therefore drop what that emit adds
  in between; guard the buffer instead. Bounded and non-fatal like every
  other wait on the sink path: a `close` still running after `CLOSE_TIMEOUT` (5s) is
  abandoned, anything it raises is logged and flagged (`SinkDispatch.close_failed`),
  and neither can delay a shutdown further or break it. A sink the failure breaker
  already disabled is closed too — the events it buffered before it started
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
  that is not a string. `message` used to accept two more shapes — a message object
  (`{"role": ..., "content": ...}`) and a list of SDK input items — and a
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
  `"2026-08-06 12:34:56+00:00"`. Text output — the overwhelming majority — is
  byte-identical.
- `POST /agents/{name}/chat` without `?stream=true` drives the SDK's streaming
  API internally (the streamed and non-streamed endpoints are now one code path
  that differs only in how it answers). Same model, same settings, same result;
  worth knowing if your provider behaves differently between its streaming and
  non-streaming endpoints, or gates streaming behind account verification.
- `MemoryEventStore.append` now yields one scheduling turn (`await asyncio.sleep(0)`)
  before returning, matching what every durable store already does (SQLite's own
  `to_thread`). Fidelity, not correctness: a caller whose liveness secretly depended
  on the in-memory store never suspending — the way the bounded sink dispatch briefly
  did, before its own fix — is now exercised the same way it would be against a real
  deployment, in dev and in tests, instead of only by measurement in production.
- MCP now lives in `agentdeck.adapters.tools.mcp` (registry, hardened HTTP
  transport, agent wiring — all unchanged). `from agentdeck.agents.mcp import ...`,
  `from agentdeck.agents.mcp.lifecycle import ...` and `from agentdeck.agents
  import ...` keep working and hand back the same objects; both paths will be
  dropped in a later release. The deeper module paths
  `agentdeck.agents.mcp.transport` and `agentdeck.agents.mcp.wiring` are gone —
  import those names from the package instead.
- `EventSinkPort.emit` must now return promptly: an emit that blocks longer
  than the dispatch's `emit_timeout` (5s) is abandoned and counted as a
  failure, and a sink that does it repeatedly is disabled like any other
  broken sink. A sink whose work is slow buffers internally and flushes on
  its own schedule.
- `Runtime.drain()` is now terminal — it closes each sink rather than
  pausing it, and returns within a bounded time even against a sink whose
  `emit` never returns. Runs after a `drain()` reach no sinks.
- Langfuse traces no longer depend on the SDK's exit hook to leave the process.
  `Runtime.drain()` now closes the sink: any trace still open is finished as
  interrupted by the shutdown — an unfinished observation is never shipped at all,
  so a run cut short showed up nowhere before — and the SDK's batch is flushed on
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
  `<db>-wal` and `<db>-shm` alongside each database — back them up
  and move them together, not the one file on its own — and WAL depends on
  shared memory that network filesystems (NFS, SMB) do not provide reliably, so
  keep these databases on local disk. In-memory databases are unaffected.
- **One turn per session at a time.** Starting a turn on a session that already
  has a run in flight now fails immediately with `SessionBusyError`, naming the
  session and the run that holds it, instead of running the second turn against a
  conversation the first one is still changing — which silently corrupted the
  model's context and could lose a message from either turn. A session counts as
  busy until its run reaches a terminal event, and a run waiting on a human answer
  is still busy: it owns the thread its resume continues from. Sequential turns,
  resumes and runs without a session are unaffected, and two different sessions
  never contend. This holds across processes, because the check and the write that
  opens the run are one store operation, so it is not defeated by a second worker.
  What a caller should do with the refusal is retry or report it; the losing turn
  is not queued (that is deliberately deferred, not forgotten). Over HTTP the v2
  chat route answers **409 Conflict** with the holding run named in `detail`,
  before the event stream starts. A client that disconnects in that window — after
  the turn was admitted but before the first event reached it — has its run closed
  as cancelled, so the session is free for the retry rather than held.
- The event log now enforces **one `seq` per run**: `(tenant, session, run, seq)`
  is unique in the SQLite store and refused by the in-memory one, so a write that
  would put a second event at a `seq` a run has already used fails with
  `StoreError` instead of landing. A duplicate is the one corruption a gap check
  cannot see, and it would make refetching that `seq` — the whole point of
  contiguous `seq` — return whichever copy came back first. `seq` is still per
  run, so runs sharing a session log all count from 0 as before. Note for existing
  installations: only event databases created by this version carry the
  constraint, since v2 has no schema migration yet.
- A run whose process was killed outright — the one exit that cannot close its own
  run in the log — no longer holds its session for good. An open run that has
  written nothing for `AGENTDECK_RUNTIME_STALE_RUN_AFTER_SECONDS` (one hour by
  default) stops blocking new turns; the next turn takes the session over, closes
  the abandoned run as `run.failed` with error code `cancelled_hard`, and logs the
  takeover at WARNING. Two things follow from that window, both tunable with the
  same setting: a session a crashed process left claimed is refused until the
  window elapses, and an approval that has been waiting on a human for longer than
  the window is closed as failed when somebody starts a new turn on that session —
  installations with slower approvals should raise it. Keep it comfortably above the
  longest a healthy turn can go without emitting an event: shortened below that, a
  turn that is merely quiet looks abandoned and the next turn takes its session,
  which loses the one-turn-per-session guarantee instead of tuning it. Only
  positivity is checked — the real floor depends on your workload. Running several
  workers on machines whose clocks disagree shortens the window by the worst skew
  between them for the same reason, so keep them on NTP and leave headroom.

### Fixed
- A durable LangGraph checkpointer can now be used by more than one event loop in
  one process. The sqlite and postgres savers were cached for the process
  lifetime, and each holds an internal lock that binds to the first event loop to
  contend for it — so a script or test that called `asyncio.run()` twice against
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
  consumer was *cancelled* rather than closed — which is what a real ASGI server
  does when a client disconnects mid-stream. `Runtime.run` and `Runtime.resume`
  caught `GeneratorExit` and `Exception`, and `CancelledError` is neither, so a
  disconnected stream used to leave a run with no terminal event: indistinguishable
  from one still in flight, for status projections, `pending()` and anything
  reading the log. Both now record `run.cancelled` (shielded, so the write is not
  itself cancelled) and re-raise. A process that dies with the request still leaves
  the run open — no in-process write can outlive its own event loop.
- Shutdown no longer hangs forever on a wedged sink: every wait on the sink
  path has a deadline, including the last one — the wait for the sink's
  consumer to stop. A sink whose `emit` swallows cancellation can delay a
  shutdown but no longer block it, and a cancellation aimed at whoever is
  shutting down is no longer absorbed by the shutdown itself. That last deadline
  needed a second fix to hold: a deadline fires by cancelling the task that is
  waiting, and a task waiting *on another task* hands that cancellation straight
  to it — into the same sink that had just swallowed one, spending the deadline
  with nothing left to fire again. A sink that ate cancellation from inside a
  still-running `emit` could therefore keep a shutdown waiting for as long as it
  kept working, and no outer `wait_for` could end it either. The consumer is now
  waited on from the outside, so the deadline expires on time whatever the sink
  does; and the consumer has a second way out that needs no cancellation at all —
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
  reached raises `StoreError` — two outcomes a raw `sqlite3.OperationalError:
  database is locked` used to blur together.
- Crash recovery for conversations on the OpenAI Agents engine: a process that
  died mid-turn used to leave that conversation permanently short of whatever the
  event log had already recorded — the question it was killed on, or the answer it
  had just given. The model then answered later turns with a hole in its context
  and nothing reported a problem. Each turn now checks the log against the
  engine's own conversation state and replays the messages that are missing before
  the model runs, so a restarted process picks the conversation up whole.
  Messages only, in content and order: tool results and model reasoning are not
  reconstructed, so a conversation repaired this way carries the *text* of a tool
  answer without the tool call behind it — worth knowing if you read model context
  back. A turn a client disconnected from before the first token is never replayed,
  so retrying that question does not send it twice; a turn that was answered before
  the client went away keeps both its messages. Conversation state that has diverged
  from the log rather than fallen behind it is left untouched and reported on the run
  as `custom` / `openai_agents.session_diverged`. LangGraph workflows are unaffected —
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
  indexed statement returning each run's last lifecycle event — one event parsed
  per run instead of every event of every log (4.2 ms instead of 32 ms for the
  same 201 runs).
- Event sinks are now fed from a bounded queue with one worker each, instead of a
  fresh task per event per sink. A wedged sink (telemetry endpoint down, audit
  store backpressured) now costs a fixed backlog and one task rather than growing
  memory for as long as the process runs. A run still never waits on a sink: when a
  sink's queue is full its stalest event is dropped rather than the run delayed — but
  only once the sink has been given a turn to catch up, so a sink that is keeping up
  loses nothing however fast the run produces events. Dropped events and failed
  emits are counted per sink and reported in the logs — never discarded silently, and
  never one stack trace per event — and a sink that raises five times in a row is
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
  — running the workflow's next node twice and writing two `run.resumed` events
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
engine-agnostic core — the same event stream, store, and surfaces serve
OpenAI Agents chats and LangGraph workflows alike. The v1 public surface
remains byte-for-byte unchanged.

### Added
- LangGraph engine adapter (`agentdeck.adapters.engines.langgraph`): runs graph
  workflows behind `EnginePort`, surviving process restarts — an interrupted run's
  status and resume point persist, and resuming continues the same event sequence
  with no duplicates. A new `GET /pending` / `POST /resume` surface lists and
  answers interrupted runs; resuming the same run twice at once resolves exactly
  once, and resuming a run that already finished is a no-op rather than an error.
- OpenAI Agents engine adapter (`agentdeck.adapters.engines.openai_agents`):
  runs a pre-built `agents.Agent` — handoffs and tools included — behind
  `EnginePort`, streaming the canonical events (`text.delta`,
  `message.completed`, `tool.call.started`/`.completed`, and a namespaced
  `custom` event per handoff). SDK session keys are tenant-scoped, so two
  tenants reusing the same session id never share a conversation.
- SQLite event store (`agentdeck.adapters.stores.sqlite`): a durable event
  log with the same append-only, per-run-`seq` contract as the in-memory
  store.
- Minimal v2 surfaces (`agentdeck.surfaces.serve`, `agentdeck.surfaces.cli`):
  an SSE route and a compact CLI chat renderer for v2 runs — speakers are
  distinguished by `origin` + `message_id` alone, and transcripts rebuild
  from `message.completed` events without delta assembly. The v1 server is
  untouched.
- Docs site: a Concepts section (agents, capabilities, skills, workflows)
  written against the shipped v1 surface, and the AgentDeck brand — palette,
  type scale, self-hosted fonts so the exported site makes no external
  requests.

### Removed
- Docs site: the empty Guides and Examples sections; each returns when its
  first real page exists.

## [2.0.0b1] - 2026-08-05

First beta of the v2 line: the engine-agnostic core and the Runtime land
alongside the shipped v1 harness. **The v1 public surface is unchanged** —
`App`, `run_agent`, `run_workflow`, `chat`, `chat_stream`, the `./.agentdeck/`
project layout and the SSE wire format are byte-for-byte what 1.2.1 served.
The bump marks the start of the v2 rebuild, not a break in what already works.

### Added
- `agentdeck.core`: canonical event schema v1 — a closed eight-field `Event`
  envelope over payload classes discriminated by `kind`, `parse_event()`
  tolerating unknown kinds and fields, content blocks, and the
  `check_contiguous` / `check_terminal` ordering invariants. Nothing imports
  it yet; v1 runtime behavior is unchanged.
- `agentdeck.core`: `RunContext` (the run's identity and limits, passed to
  every port), `InvocableSpec` / `InvocableKind` (one noun for agents,
  workflows and skills), and the first three ports — `EnginePort`,
  `SessionStorePort`, `EventSinkPort`.
- `agentdeck.runtime.service.Runtime`: the v2 run loop — stamp the envelope,
  append to the log, fan out to sinks, yield, in that order, so an event a
  consumer has seen is already persisted. Every run is closed in the log: an
  engine that raises or stops early gets `run.failed`, an abandoned stream
  gets `run.cancelled`, and nothing follows a terminal event. Sinks never
  stall a run; `Runtime.drain()` flushes in-flight emits at shutdown.
- In-memory event store (`agentdeck.adapters.stores.memory`) and scripted
  stub engine (`agentdeck.adapters.engines.stub`) — the stub is the reference
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
  CI check) and anti-rot tests — published Python samples are parsed and
  their imports resolved, links must resolve, and navigation must match the
  pages.

### Fixed
- Docs site: Getting Started installs from a git tag and documents provider
  configuration instead of describing a contributor clone; the overview's
  examples run as printed; `.env.example` no longer claims a legacy default
  for `OPENAI_BASE_URL` (empty means the SDK default).

## [1.2.1] - 2026-08-03

No changes to the `agentdeck` package itself — this version covers the
documentation platform and its CI.

### Added
- `docs-site/`: MDX documentation platform (Nextra 4, Next.js App Router),
  statically exported to GitHub Pages under `/agentdeck` — deployed on
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
  at `build()` time — two agents that hand off to each other no longer need
  to import each other's module. Unknown names raise `NotFoundError` naming
  the available agents; mutual handoffs resolve without recursing forever.
- Durable timer waits: `agentdeck.workflows.sleep_until(when)` pauses a
  `durable = True` workflow node until a timezone-aware wall-clock moment.
  `App.due_resumes()` lists timer threads whose wake time has passed;
  `App.tick()` resumes every due thread. Callers own the scheduling cadence
  (cron, systemd timer, a loop) — agentdeck runs no daemon. Naive datetimes
  are rejected with a clear `ValueError`.

### Fixed
- `App.chat(..., session_id=...)` turns now carry that session id on the
  root Langfuse trace instead of always tracing with a null session —
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
  subdirectories — `agents/<bundle>/agent.py` and
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
- `agentdeck/errors.py`: one exception hierarchy — `AgentdeckError` base,
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

[Unreleased]: https://github.com/sagi5060/agentdeck/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/sagi5060/agentdeck/compare/v2.0.0b4...v2.0.0
[2.0.0b4]: https://github.com/sagi5060/agentdeck/compare/v2.0.0b3...v2.0.0b4
[2.0.0b3]: https://github.com/sagi5060/agentdeck/compare/v2.0.0b2...v2.0.0b3
[2.0.0b2]: https://github.com/sagi5060/agentdeck/compare/v2.0.0b1...v2.0.0b2
[2.0.0b1]: https://github.com/sagi5060/agentdeck/compare/v1.2.1...v2.0.0b1
[1.2.1]: https://github.com/sagi5060/agentdeck/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/sagi5060/agentdeck/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/sagi5060/agentdeck/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/sagi5060/agentdeck/compare/v0.2.0...v1.0.0
[0.2.0]: https://github.com/sagi5060/agentdeck/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/sagi5060/agentdeck/releases/tag/v0.1.0
