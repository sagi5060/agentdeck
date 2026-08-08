# Plan — the v3 cutover: ports, engines, runtime, surfaces

**Status:** proposed · **Date:** 2026-08-08 · **Target: v3.0.0 (breaking)**
Executes epic Story 2 + the `authoring/` move it scopes, against
`docs/design/agentdeck-v2-architecture.md` §6 target layout.

## Two rulings taken (2026-08-08)

1. **v1's public API is dropped, not facaded.** `App`, `BaseAgent`, `BaseWorkflow` go.
   `authoring/` is the only way to declare an agent or workflow. `agents/` and `workflows/`
   are deleted whole. Breaking release, migration guide required.
2. **The sandbox becomes a port.** One `core/ports/sandbox.py` + `adapters/caps/sandbox/`.
   Not the fat `capabilities.py` the design-doc layout block names — see "Sandbox shape".

## What "stop depending on old code" means, measurably

One import-linter contract, package-wide, replacing today's two-module carve-out:

```ini
[importlinter:contract:runtime-is-adapter-free]
source_modules = agentdeck.runtime      # whole package, not just service+dispatch
forbidden_modules = agents, langgraph, fastapi, redis, psycopg,
                    agentdeck.adapters, agentdeck.authoring,
                    agentdeck.skills, agentdeck.surfaces
```

Green, **with no `ignore_imports` exemption**, is the definition of done. The v3 ruling is
what makes the no-exemption part achievable: at v2.x the sandbox would have needed a carve-out.

## Open question this plan does not answer

**v3 has no entry point yet.** Deleting `App` leaves users with `authoring/` and
`build_runtime()`. `composition.py`'s own docstring anticipates *"a code-first `Deck()`"* as a
second front door. **Decide this before phase 4** — it is the v3 public API, and every doc
example and the migration guide are written against it.

## The actual gap

The v2 adapters exist and are contract-gated, but `app.py:275` wires `v1_engines()`.
`V1CompatEngine`/`V1CompatWorkflowEngine` **subclass** the real adapters and inject six
things the adapters lack. Those six are the whole migration:

| # | v1 injects                                                                     | v2 home                                                | phase |
| - | ------------------------------------------------------------------------------ | ------------------------------------------------------ | ----- |
| 1 | Langfuse observation wrap (`trace_run`)                                      | `LangfuseSink` — **already built, unwired**   | 1     |
| 2 | Run config: model provider, CA bundle, temperature, max_turns, max_tokens      | openai_agents adapter, injected from composition root  | 2     |
| 3 | `usage.reported` per-model-call token aggregation                            | openai_agents adapter                                  | 2     |
| 4 | Checkpointer laziness (`durable=True` only, keeps `[durability]` optional) | langgraph adapter                                      | 2     |
| 5 | `session_for` — one conversation across `App.chat` and HTTP               | adapter's`ExecutionStore` keying                     | 2     |
| 6 | Shared`Workspace` (three consumers, see below)                               | `core/ports/sandbox.py` + `adapters/caps/sandbox/` | 3     |

**Already retired, do not re-plan:** the design doc §6 amendment says the workflow reroute is
blocked on structured state. It is not — `RunCompleted.output: Input` and both adapters emit
`DataBlock` (`langgraph/engine.py:203`, `openai_agents/engine.py:213`). Stale; gets a dated
correction in phase 5.

## Sandbox shape (phase 3)

Three consumers, three *different* needs — which is why one fat port is wrong:

| consumer                     | needs                                                   | surface                      |
| ---------------------------- | ------------------------------------------------------- | ---------------------------- |
| openai-agents engine         | an opaque handle for`SandboxRunConfig`                | passthrough, no port methods |
| `LoadFileNode` (authoring) | `read_text`                                           | port                         |
| `skills/executor.py`       | `read_text`, `write_bytes`, `exec`, env injection | port                         |

**One `SandboxPort` carrying only the operations actually called.** Do *not* pre-split into
`FilesystemPort`/`TerminalPort`: three consumers justify the seam, not the split. Split when a
consumer genuinely needs half.

Justification is **DIP, not OCP** — there is one implementation (`UnixLocalSandboxClient`) and
CLAUDE.md forbids interfaces with one implementation. The exemption is earned by consumer
count and dependency direction: three consumers across three rings currently import a concrete
class out of `runtime/`. Record this as a judgment-ledger entry.

## Phases

One integration branch, draft PR from commit 1, `make check` green between phases, merged whole
(epic: *"do not split this story across releases"*).

### Phase 0 — cleanup (no behavior change, ~100 lines)

Delete `runtime/sessions.py`, `runtime/checkpointer.py`, `agents/mcp/lifecycle.py` (forwarders
that invert the ring); empty `runtime/__init__.py` to a docstring; delete `mark_sandbox_tool`,
`OpenAISettings.tracing_api_key`. Repoint 6 imports.

### Phase 1 — telemetry cutover (cheapest, satisfies an acceptance criterion outright)

Wire `LangfuseSink` into `build_runtime(sinks=...)`. Remove `trace_run` from both compat
engines. Retires the inert `runtime_capture`/`current_capture` ambient mechanism — the sink
reads identity off the event envelope.
**Done when:** epic AC *"Langfuse traces now cover workflow runs too"* holds; workflow and skill
spans carry `session_id`, which they do not today.

### Phase 2 — engine parity (load-bearing)

Move items 2–5 into the adapters, settings resolved at the composition root and injected —
matching how `store`/`control` already resolve there. `v1bridge/` is deleted here.
**Done when:** `app.py` wires `OpenAIAgentsEngine`/`LangGraphEngine` directly; contract suite
green on both; transcript-fidelity and crash-reconciliation tests unchanged.

### Phase 3 — sandbox port

`core/ports/sandbox.py` + `adapters/caps/sandbox/`; `runtime/workspace.py` deleted. Contract
test parametrized over implementations (real + fake), matching `tests/contract/`'s engine
pattern — otherwise a second sandbox silently diverges.

### Phase 4 — `authoring/` **(needs the entry-point decision first)**

`BaseAgent`/`BaseSandboxAgent` → `authoring/agent.py`, `BaseWorkflow` → `authoring/workflow.py`,
`SkillNode`/`LoadFileNode`/`AgentNode` → `authoring/nodes.py`, `CapabilitiesSpec` →
`authoring/capabilities.py`. Each compiles to `InvocableSpec`. **No re-export facades** —
`agents/` and `workflows/` are deleted, not forwarded.

### Phase 5 — surfaces

`serve.py` → `surfaces/serve/`; reroute `/workflows/*` through the Runtime (unblocked);
`cli.py` → `surfaces/cli/`. Dated correction to design doc §6's stale amendment.
Last, per the epic's own risk note.
**Wire default: unchanged.** Dropping the Python API does not require changing HTTP frames, so
`tests/golden/` stays the safety net. If the workflow reroute forces a frame diff, that is a
deliberate `make golden` with a PR justification — not a silent baseline update.

### Phase 6 — deletion, contract, release

Deletion list below; package-wide import-linter contract; migration guide; CHANGELOG;
version → 3.0.0.

## Moved vs deleted

**Moved (~1,150 lines)** — `agents/base.py`→`authoring/agent.py`, `workflows/base.py`→
`authoring/workflow.py`, `workflows/nodes.py`→`authoring/nodes.py`, `agents/capabilities/`→
`authoring/capabilities.py` + adapters, `runtime/workspace.py`→`adapters/caps/sandbox/`.

**Deleted (~2,000 lines)**

```
agentdeck/v1bridge/                     311
agentdeck/app.py                        513   → composition root only; facade dropped
agentdeck/agents/runners/               307
agentdeck/serve.py                      301   → surfaces/serve/
agentdeck/runtime/observability.py      284   → adapters/telemetry/langfuse/
agentdeck/workflows/runners/            152
agentdeck/runtime/checkpointer.py        30   forwarder
agentdeck/agents/registry.py             21   → runtime/discovery.py
agentdeck/workflows/registry.py          21   → runtime/discovery.py
agentdeck/runtime/sessions.py            12   forwarder
agentdeck/agents/mcp/lifecycle.py        12   forwarder
```

## Risks

- **Phase 2 regresses silently.** Run config is settings-driven; a dropped field (CA bundle,
  max_tokens) fails only against a real endpoint. Mitigation: a parity test asserting the
  adapter's `RunConfig` equals `HeadlessRunner.from_agent`'s, added *before* the move and
  deleted with `v1bridge/`.
- **v3 removes the facades that made goldens sufficient.** `tests/golden/` covers the SSE wire
  only; `tests/test_app.py` covers the Python API being deleted. Phase 4 needs equivalent tests
  written against `authoring/` *before* `app.py` goes, or coverage silently drops.
- **The entry-point question blocks phase 4** and is not answered here.
