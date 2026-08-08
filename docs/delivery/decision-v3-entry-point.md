# Decision brief — the v3 entry point

**Status:** recommendation, needs one ruling · **Date:** 2026-08-08 · **Branch:** `feat/v3-cutover`
**Resolves:** #88 (design spec, open decisions) · **Blocks:** `plan-v2-cutover.md` phase 4 (`authoring/`)
**Frozen on ship:** this is the v3.0.0 public API. Every doc example, the README, and the
migration guide are written against it.

This brief does not invent a design. #88 already specified one; phases 0–3 changed the ground
under it. Part 1 says which of #88's assumptions survived. Part 2 says what `App` actually does
today, method by method. Part 3 gives three concrete resolutions. Part 4 closes #88's five open
decisions plus the four that phases 0–3 created. No production code was written.

---

## TL;DR

**Ship `Deck` — one class, two constructors, flat methods, one ownership rule.** It is #88's
proposed shape with the sketch's `serve()`, `tools=`, and `engines=` dropped, `agents=`/
`workflows=` collapsed into one `invocables=`, and defaults resolved from settings rather than
hard-coded. Roughly 220 lines of new code, most of it lifted out of `app.py`.

```python
from agentdeck import Deck

async with Deck.from_project() as deck:            # today's ./.agentdeck, unchanged
    turn = await deck.chat("Greeter", "sess-1", "hello")
    print(turn.output)
```

Two deviations from #88's written sketch need your sign-off — they are the **one question**
at the end. Everything else in this brief is reconciliation against evidence on the branch.

---

## Part 1 — what changed under #88 while it sat open

#88 was written on 2026-08-05, before v2.0.0 shipped and before phases 0–3 of this branch.
Six of its stated assumptions no longer hold.

| # | #88 assumed | Reality now | Consequence for the design |
| - | ----------- | ----------- | -------------------------- |
| 1 | "**#74 is re-scoped** to implement this shape … before any facade code is written" | **#74 is closed and shipped.** `App` *is* the composition root (`app.py:284` calls `build_runtime`), the HTTP compat facade exists at `surfaces/serve/compat.py`, and the golden suite passes byte-identical. | #88's third "Done when" is dead. The facade was built once, correctly, and it is an **HTTP** facade — nothing about it is a Python-API facade. It survives v3 untouched (plan phase 5: "wire default unchanged"). |
| 2 | "Does `App` survive? Cheapest: `App` becomes a thin wrapper over `AgentDeck.from_project()`" | **Superseded by `plan-v2-cutover.md` ruling 1** (2026-08-08): v1's public API is dropped, not facaded. `App`, `BaseAgent`, `BaseWorkflow` go; `agents/` and `workflows/` are deleted whole. | This open decision is already closed *against* #88's cheapest answer. No `App` alias, no deprecation shim. The migration guide is the compatibility story. |
| 3 | "`engines=None` — **inferred** from what you passed" | Phase 2 moved run-config resolution into the adapters, *injected from the composition root*. An engine is no longer default-constructible: `OpenAIAgentsEngine(sessions, settings=resolve_run_settings(), sandbox=resolve_agent_sandbox())`, `LangGraphEngine(durable_checkpoint=resolve_checkpoint(), workspace=resolve_workflow_workspace())`. | "Inferred" now means "the deck builds the two default engines from settings" — which is exactly `App.load()`'s body. Still cheap, but it is *construction*, not inference, and `engines=` must remain an override (the stub engine in `tests/contract/` depends on it). |
| 4 | "Defaults are **ephemeral and single-process** — memory store, in-memory control, **no sinks**" | Phase 1 wired `LangfuseSink` at the composition root: `build_runtime`'s default `sinks` is `langfuse_sink()` when keys are present, `()` otherwise. `store`/`control` default through `resolve_event_store()`/`resolve_control_port()`, which read settings (memory unless configured). | Defaults are **settings-resolved, not literal**. A deck built in a process with `AGENTDECK_EVENTS_BACKEND=postgres` is durable without any argument — and that is correct, not a bug. Restate the rule as: *no argument means "whatever this deployment configured"; an argument always wins.* |
| 5 | "a `durable=True` workflow on a memory deck should be **refused at construction**" | `durable` now travels as spec metadata (`runtime/discovery.py:DURABLE_KEY`) and the langgraph adapter resolves the checkpointer lazily at the first durable run, via `resolve_checkpoint()` — **a different backend from the event store**. | The corollary **dissolves**. "Memory deck" and "durable workflow" are orthogonal; there is nothing coherent to refuse at construction. Checkpoint-vs-event-store coherence is #155's territory (env surface restructure before 3.0.0), not the entry point's. |
| 6 | "`tools=[…]` cut to deck-level **tool sources** (MCP)" | `MCPLifecycle` is a **class-level process-wide registry** (`adapters/tools/mcp/lifecycle.py:81` — `_servers`, `_failed`, `_connected`, `_config` are all class attributes) with a `# ponytail:` note saying its config still comes from process-global settings because agents resolve servers at import time. | `tools=` **cannot ship in 3.0.0 even as sources.** A deck-per-tenant with its own MCP servers is not expressible until that registry is instance-scoped. Keep it cut, and say so in the docs as a known limit rather than leaving it as an unexplained gap. |

Two things #88 never mentioned that the design must now cover:

- **The second store (ADR-D5).** `App` constructs `SessionFactory.from_settings(...)` and an
  `ExecutionStore` — the engine-private conversation memory, distinct from the event store.
  It owns a Redis client. #88's `store=` names only the event store. The deck must own,
  expose, and close this too; `session_factory=` stays the DI seam it already is
  (`app.py:215`).
- **The sandbox port (phase 3).** `core/ports/sandbox.py` now exists with one implementation.
  It is resolved at the composition root (`resolve_agent_sandbox`, `resolve_workflow_workspace`)
  and passed into engines. It is **not** a deck constructor argument: one implementation, and
  the plan's own ledger entry says the seam is earned by DIP, not by a second impl.

### Cross-checks against other open issues (so this brief contradicts nothing filed)

- **#129** (A2A / MCP-server / OpenAI-compatible surfaces as adapters over the event stream) is
  exactly the trigger #88 predicted for its cut `interfaces=[…]` list. Recommendation below keeps
  `asgi()` singular; #129 is where a protocol list is reconsidered, on evidence.
- **#120** (one approval inbox — `App.pending_interrupts` reads the checkpointer, HTTP reads the
  event log) **collapses in v3's favor**: deleting `App` deletes the checkpointer-reading side.
  `deck.pending()` is `Runtime.pending()`, event-log only. Point in favor of the recommendation.
- **#26** (`agentdeck.testing` stub-runner harness) is #88's flow 5. `Deck(invocables=…, engines=…,
  store=…)` is most of it; #26 becomes "package the fixtures", not "invent a seam".
- **#155** (one env var per decision, before 3.0.0) and this brief are coupled by the "defaults
  resolve from settings" rule. They must land in a consistent order; this brief assumes #155 may
  change *which* env vars exist, not that arguments stop overriding them.
- **#131 / #71 / #132** (simplification pass, legacy cleanup, release readiness) all want the
  entry point smaller, not larger. Every option below is judged against that.
- **#119** (bundle build failures surface bare exceptions) applies to `Deck.from_project()`
  unchanged — same discovery path, same gap.

---

## Part 2 — what `App` actually does today

`app.py` is 536 lines. Split three ways:

**(a) Composition-root work — survives as `Deck`, ~120 lines**
`__post_init__` (mount project dir, three registries, `SessionFactory`, `ExecutionStore`) ·
`load()` (discover → build every agent, compile every graph, validate every skill schema →
`build_runtime`) · `_ensure_runtime` (lazy compose on first use) · `open()` (context manager +
`MCPLifecycle.startup`) · `aclose()` (`runtime.drain()` → `sessions.aclose()` → `MCPLifecycle.
shutdown` if this App started it) · `runtime` / `store` / `settings` properties.

**(b) v1 convenience API — decided per method, table below, ~250 lines**
`run_agent` · `chat` · `chat_stream` · `run_workflow` · `resume_workflow` ·
`run_workflow_stream` · `pending_interrupts` · `due_resumes` · `tick` · `session_for` ·
the `agents`/`workflows`/`skills` registry attributes · `inventory`.

**(c) Already pure Runtime operations — one-line delegations, ~30 lines**
`pause_run` → `runtime.signal(id, PAUSE)` · `cancel_run` → `runtime.signal(id, CANCEL)` ·
`resume_run` → `runtime.resume_run(...)` · `_paused_workflow_run` → filter over
`runtime.pending()`.

Group (c) is the tell: a third of `App` is already a pass-through. `Runtime` today exposes
`run`, `resume`, `resume_run`, `signal`, `pending`, `drain`, `store` — the whole operation set.
What `Runtime` **cannot** do, structurally, is own lifecycle for things it did not construct
(MCP servers, the Redis session client) — it lives in the `runtime/` ring and may not import an
adapter. **That is why a lifecycle owner above `Runtime` is genuinely required, not speculative.**

### Migration table — every method gets a ruling

The v3 migration guide is written from this table. Nothing is left unmapped.

| `App` member | v3 | Recipe / note |
| --- | --- | --- |
| `App()` | `Deck.from_project()` | Same `./.agentdeck` discovery, same failure mode. |
| `App.open()` | `async with Deck.from_project() as deck` | The deck *is* the context manager; no separate classmethod. |
| `load()` | folded into construction | `from_project()` discovers eagerly, as `load()` does. `inventory` returned by `Deck.invocables` (a mapping, already the `InvocableSpec` one). |
| `runtime` / `store` / `settings` | kept, same names | `store` stays the read side of the log. |
| `run_agent(name, msg)` | `deck.run(name, msg)` → `TurnResult` | One-shot, no session. `TurnResult` type kept verbatim. |
| `chat(name, sid, msg)` | `deck.chat(name, sid, msg)` → `TurnResult` | Unchanged signature. |
| `chat_stream(name, sid, msg)` | `deck.stream(name, msg, session=sid)` | Yields canonical `Event`s, as today. Renamed because it also streams workflows in v3. |
| `run_workflow(name, state, thread_id=)` | `deck.run(name, state, thread=…)` | One method, both kinds — `InvocableSpec` already erases the distinction; keeping two front doors reintroduces the split the registry removed. Workflow state travels as a `DataBlock`, as `app.py:335` already does. |
| `resume_workflow(name, thread, value)` | `deck.resume(name, thread, value)` | Same "not applied → `NotFoundError`" guard; the logic is already written. |
| `run_workflow_stream(...)` | **dropped** | It is the one method that bypasses the Runtime entirely (`app.py:419` says so): no event log, invisible to `resume`. Recipe: `deck.stream(name, state, thread=…)` — the logged equivalent, which is what everyone actually wanted. **Frame shapes differ**; the migration guide must show the mapping. |
| `pending_interrupts(name=None)` | `deck.pending(invocable=None)` | Now reads the **event log** (`Runtime.pending`), not the checkpointer. **This closes #120** and is a behavior change to declare in the CHANGELOG. |
| `due_resumes(now=)` | **dropped** | Recipe (three lines, in the guide): filter `deck.pending()` on `wake_at_of(p.payload)`. `wake_at_of` moves to `authoring/` with the timer nodes. |
| `tick(now=)` | **dropped** | Recipe: `for p in due: await deck.resume(...)`. AgentDeck runs no daemon; a for-loop is not an API. |
| `session_for(sid)` | `deck.session_for(sid)` | Kept — it is the ADR-D5 second store's only public door, and it is the reason a deck must own that store. |
| `pause_run` / `cancel_run` | `deck.signal(run_id, Signal.PAUSE\|CANCEL, reason)` | One method, the verb is already an enum. Two named wrappers over a one-line enum call is the kind of thing #131 exists to delete. |
| `resume_run(run_id)` | `deck.resume_run(run_id)` | Kept as-is — semantically distinct from `resume` (continues a *paused* run, not an interrupt). |
| `agents` / `workflows` / `skills` registries | **dropped** | They expose `BaseAgent` classes, which v3 deletes. `deck.invocables` is the replacement view. Skills were never invocables. |
| `TurnResult` | kept, unchanged | Public type, no reason to churn it. |

**Coverage risk, per the plan's own risk note:** `tests/test_app.py` is 17 KB written against the
Python API being deleted. Its invariants must be rewritten against `Deck` **before** `app.py` is
removed, or v3 silently ships with less coverage than v2. Same for the 7 gate-executed
` ```python run ` fences in `docs-site/content/` — `test_docs_examples.py` executes them, so a
stale example is a **red gate**, not a stale doc. That is a feature here: the docs rewrite is
mechanically verified.

---

## Part 3 — three resolutions

### Option A — `Deck`: one class, two constructors, flat methods **(recommended)**

#88's proposed shape, minus the parts phases 0–3 or YAGNI removed.

```python
# 1. the directory project — what every v2 user has today
from agentdeck import Deck

async with Deck.from_project() as deck:
    turn = await deck.chat("Greeter", "sess-1", "hello")
    print(turn.output, turn.usage)


# 2. code-first — no directory, no discovery, no filesystem
from agentdeck import Deck
from agentdeck.authoring import BaseAgent

class Greeter(BaseAgent):
    instructions = "You are a friendly scheduling assistant."

async with Deck(invocables=[Greeter]) as deck:
    async for event in deck.stream("Greeter", "hello", session="sess-1"):
        ...


# 3. embedded in someone else's service, explicit infrastructure
from contextlib import asynccontextmanager
from fastapi import FastAPI
from agentdeck import Deck
from agentdeck.adapters.stores.postgres import PostgresEventStore

deck = Deck(invocables=[Greeter, ClaimPipeline], store=PostgresEventStore(DSN))

@asynccontextmanager
async def lifespan(api: FastAPI):
    async with deck:
        yield

api = FastAPI(lifespan=lifespan)
api.mount("/agents", deck.asgi())


# 4. deck-per-tenant over shared infrastructure
store = PostgresEventStore(DSN)                      # constructed by you
decks = {t: Deck(invocables=specs_for(t), store=store) for t in tenants}
# each deck closes only what it built; `store` outlives all of them
```

**Constructor:** `Deck(*, invocables=(), store=None, sinks=None, control=None, engines=None,
session_factory=None, clock=None)`. That is `build_runtime`'s signature plus the two things
`build_runtime` cannot own (the session store, engine construction). `from_project(path=".agentdeck")`
is the same primitive with `invocables` filled by `InvocableRegistry`.

**Methods:** `chat` · `run` · `stream` · `resume` · `resume_run` · `pending` · `signal` ·
`session_for` · `asgi()` · `aclose()` / `__aenter__` / `__aexit__` · properties `runtime`,
`store`, `invocables`, `settings`.

**Ownership rule, stated once and enforced by construction:** *the deck closes what it
constructed and never closes what you passed in.* That is the whole reason deck-per-tenant is
safe without a resource-manager abstraction. #88 got this right and nothing has changed it.

- **Cost to build:** ~220 lines, of which ~150 is `app.py` group (a) + (c) moved almost verbatim
  (the event-stream reducers `_turn_result`/`_workflow_result` come across unchanged). Plus the
  docs rewrite: 7 runnable fences, `reference/app.mdx` → `reference/deck.mdx`, `concepts/index.mdx`,
  README. `test_app.py` rewritten against `Deck`.
- **Cost to migrate:** mechanical for the common path — `App.open()` → `Deck.from_project()`,
  three method renames, two drops with three-line recipes. The only genuinely lossy move is
  `run_workflow_stream` (different frames, in exchange for being recorded).
- **Forecloses:** almost nothing. `deck.agent("X").chat(...)` handles, `interfaces=[…]`,
  `tools=[…]`, `serve()`, and a `from_package()` constructor are all **additive** later. The one
  thing it commits to is *flat namespace on one object* — if handles are ever added, `chat` and
  `agent("X").chat` coexist, which is two ways to do one thing.

### Option B — no facade: `Runtime` is the entry point

Ship only what is structurally required: an async context manager that builds a wired `Runtime`
and closes what it built. No convenience layer at all.

```python
import uuid
from agentdeck import open_runtime
from agentdeck.core.content import coerce_input
from agentdeck.core.context import RunContext

async with open_runtime() as rt:
    ctx = RunContext(
        tenant="local", principal="user:local",
        run_id=str(uuid.uuid4()), trace_id=str(uuid.uuid4()),
        session_id="sess-1",
    )
    async for event in rt.run("Greeter", coerce_input("hello"), ctx):
        ...   # reduce the stream yourself
```

- **Cost to build:** ~60 lines. Cheapest option by a wide margin, and the most YAGNI-faithful
  reading of CLAUDE.md ("stop at the first thing that works").
- **Cost to migrate:** severe. Every user writes a `RunContext` with two UUIDs and their own
  terminal-event reducer, or gives up the Python API and uses HTTP. All 7 runnable doc examples
  become ~15 lines each.
- **Forecloses:** nothing — `Deck` can be added in 3.1 additively. That is the real argument for
  it: shipping less at the freeze point is strictly safer.
- **Why rejected:** it fails the repo's own stated bar. `app.py:302` — *"the recorded path has to
  be at least as convenient as the one it replaces, or the old shape survives by being easier."*
  Here there is no old shape to survive, so what actually happens is worse: users write their own
  reducers, each subtly different, and the platform's Appendix B pitch ("twenty lines, three
  surfaces") becomes false in its first code block. YAGNI is about not building speculative
  *flexibility*; the terminal-event reducer is not speculative — it exists three times already
  (`app.py`, `surfaces/serve/compat.py`, and every test that drains a stream). Deleting the one
  shared copy and asking users to re-derive it is not simplification, it is externalizing cost.

### Option C — `AgentDeck` as #88's full sketch

`AgentDeck(agents=(), workflows=(), skills=(), tools=(), engines=(), interfaces=…)` with
`serve(http=8080)`, handle-style access (`deck.agent("Greeter").chat(...)`), and a lifespan-
registering `asgi()`.

- **Cost to build:** ~450 lines plus an MCP refactor. `tools=` requires making `MCPLifecycle`
  instance-scoped (it is process-global today, by class attribute); `serve()` duplicates the
  `agentdeck-serve` console script; handles add a per-invocable proxy object.
- **Cost to migrate:** same as A for the common path, larger surface to document.
- **Forecloses:** the opposite problem — it freezes five extra decisions at 3.0.0 that nothing
  currently pressures. `serve()` blocking-vs-handle is an unforced choice between two APIs;
  `interfaces=` is a registry for the two surfaces that exist.
- **Why rejected:** #88's own cut table already argued against most of it, and phases 0–3 added
  a hard blocker (`tools=` cannot work). CLAUDE.md: *"no interface with one implementation, no
  config for a value that never changes, no scaffolding for later."*

---

## Part 4 — rulings

### #88's five open decisions

1. **Does `App` survive? → No.** Already ruled by `plan-v2-cutover.md` ruling 1 and superseding
   #88's own "cheapest" answer. Deleted outright, no alias, no deprecation shim. The migration
   table in Part 2 is the compatibility story. *(Rationale: an alias would keep `agents/` and
   `workflows/` alive, which is the whole thing v3 exists to delete.)*
2. **Flat or handle? → Flat.** `deck.chat("Greeter", …)`. Handles are additive and nothing today
   pressures them; shipping both is two ways to do one thing at the freeze point.
3. **Does `serve()` block or return a handle? → Neither: no `serve()` at all.** The
   `agentdeck-serve` console script is already the process entry point and becomes a `Deck` caller;
   embedders get `asgi()` and write `uvicorn.run(...)` themselves — one line they were writing
   anyway. **Deviates from #88's sketch; see the question below.**
4. **Does `asgi()` register a lifespan hook? → No. The deck is the context manager; `asgi()`
   closes nothing.** Concrete reason, not taste: Starlette does **not** run a mounted sub-app's
   lifespan, so a lifespan hook on `asgi()` would fire in the standalone case and silently not
   fire in the `api.mount(...)` case — the exact scenario #88 wrote the ownership rule for. One
   mechanism, always the same: `async with deck` (or `lifespan=` on the host app, as in sample 3).
5. **Naming: → `Deck`.** `agentdeck.Deck` reads; `agentdeck.AgentDeck` stutters. It matches how
   the team already talks about it and how `plan-v2-cutover.md` and `composition.py`'s docstring
   already refer to it ("a code-first `Deck()`"). **Deviates from #88's title; see the question.**

### Four decisions phases 0–3 created

6. **One `invocables=` or `agents=`/`workflows=`? → One `invocables=`.** `InvocableSpec` exists
   precisely so the Runtime never learns which shape a name was authored in
   (`runtime/discovery.py`'s own docstring). Two constructor parameters would reinstate the split
   at the front door, and a third kind (a remote A2A agent, per Appendix B.3) would need a third
   parameter.
7. **Who owns the ADR-D5 session store? → The deck.** It constructs `SessionFactory`/
   `ExecutionStore`, so it closes them — the same ownership rule, applied to the second store.
   `session_factory=` stays the DI seam it is today.
8. **`tools=` → stays cut, with a documented reason.** `MCPLifecycle` is a process-wide class-level
   registry; per-deck tool sources are not expressible until it is instance-scoped. Ship
   deck-per-tenant with shared MCP, say so in the docs, and file the follow-up.
9. **Defaults → resolved from settings, arguments always win.** Replaces #88's "memory store, no
   sinks" literal. `store=None` means `resolve_event_store()`, `sinks=None` means
   `langfuse_sink()`-when-configured, `control=None` means `resolve_control_port()`. #88's
   "refuse a durable workflow on a memory deck" corollary is dissolved (see Part 1, row 5).

### Named open decisions this brief deliberately does not close

- **Who builds `InvocableSpec.native`?** Coding standards §3 says `authoring/` imports `core/`
  only, but `native` is engine-built (an `agents.Agent`, an uncompiled `StateGraph`), and today's
  registry "reaches through the v1 bundle classes … which is why it sits at the composition
  layer's edge" (design doc §6, amended 2026-08-05). `Deck(invocables=[Greeter])` works under
  either resolution *because the deck is the composition root* — but **phase 4 cannot lay out
  `authoring/` without this ruling.** It is an `authoring/` question, not an entry-point question.
- **Where the terminal-event reducer lives.** `_turn_result`/`_workflow_result` exist in `app.py`
  and again in `surfaces/serve/compat.py`, duplicated on purpose so neither depends on the other's
  ring. The deck inherits one copy. Whether the two become one shared module (it is pure over
  `core/` types) is a phase-4/5 implementation call, worth ~40 lines.
- **#155 interaction.** If the env surface is restructured before 3.0.0, "resolved from settings"
  changes *which* variables, not the override rule. Order the two so the docs are written once.

---

## Part 5 — what this unblocks

With rulings 1–9 taken, phase 4 can proceed: `authoring/` is laid out as the thing `Deck` exposes,
`BaseAgent`/`BaseWorkflow`/nodes/capabilities move, and each compiles to `InvocableSpec` — subject
to the `native` question above, which phase 4's own prompt must carry.

Sequencing note for phase 6: `Deck` must exist and `test_app.py`'s invariants must be rebuilt
against it **before** `app.py` is deleted, or coverage drops silently between phases.

CLAUDE.md's standing rule — *"Single entry point: `App`… always serves `./.agentdeck/`. No other
catalog mechanism."* — needs a dated amendment in the phase-4 PR, with the primitive/sugar split
as the reason there is still exactly one catalog mechanism underneath.

---

## The one question

**Do you sign off on the two places this brief deviates from #88's written sketch?**

1. **No `serve()`.** #88 sketched `deck.serve(http=8080)` and left blocking-vs-handle open. I
   recommend deleting the method: `agentdeck-serve` is already the process entry point, `asgi()`
   covers embedding, and `uvicorn.run(deck.asgi())` is the one line it would have saved. Keeping it
   means choosing between two APIs (context manager + `wait_closed()`, or `serve_forever()`) and
   freezing that choice at 3.0.0.
2. **`Deck`, not `AgentDeck`.** #88's title says `AgentDeck`; `plan-v2-cutover.md` and
   `composition.py`'s docstring both already say `Deck()`. `agentdeck.Deck` does not stutter.
   Renaming after 3.0.0 is a breaking change, so it is worth ten seconds now.

Everything else above is reconciliation against evidence on this branch and does not need a taste
call. If both deviations are accepted, phase 4 is unblocked the moment the `native` question in
Part 4 is answered.
