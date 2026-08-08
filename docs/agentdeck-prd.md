# AgentDeck — Product Requirements Document (PRD)

**Version:** 1.0 · **Date:** 2026-08-04 · **Owner:** Sagi
**Companion docs:** `project-brief.md` (why/when), `design/agentdeck-v2-architecture.md` (how),
`00-project-index.md` (doc map). Requirements here are product-level; architecture
choices live in the design doc and are referenced, not restated.

---

## 1. Vision

AgentDeck is the platform where the durable parts of agent systems live. Users write
small Python definitions — agents, workflows, skills — and the platform provides
everything around them: memory, streaming, human approvals, run control, protocols,
cost, audit, and replay. Engines are interchangeable; the user's assets outlive SDK
churn. One sentence: **build once, run anywhere, speak every protocol.**

## 2. Users

**P1 — the builder (primary persona).** A developer or small team building agent systems
for real operations (e.g. logistics claims handling). Wants working systems from ~80
lines of definitions, not infrastructure projects. Measures the platform by "what did I
NOT have to write."

**P2 — the operator.** Runs deployed agents. Needs a pending-approvals inbox, the
ability to pause/cancel any run from anywhere, cost visibility per agent/tenant, and
replay when something looks wrong.

**P3 — the end user / counterparty.** Chats with agents over web/WhatsApp/editor, or is
a partner organization's agent calling in over A2A. Never sees AgentDeck; sees fast
streaming, remembered context, and safe approval gates.

## 3. Product principles

Definitions over configuration (Python conventions, no DSL). Everything composes:
agents, workflows, skills, and remote agents are one kind of thing and can call each
other in any direction. Features are readers of one event log — a feature that requires
a new privileged pathway is suspect. Backward compatibility is a feature: existing
projects keep working through every release. Human control is first-class: any run can
be paused, cancelled, steered, or gated on approval — durably, across restarts.

## 4. Functional requirements

Priorities: **P0** = this initiative (Milestone 0 + core epic) · **P1** = next epics,
already designed · **P2** = designed at concept level, scheduled later.

### 4.1 Authoring & composition

- **FR-1 (P0)** Define an agent/workflow/skill by dropping a conventional file into `.agentdeck/`; no registration. Existing v1.2.1 projects load unchanged.
- **FR-2 (P0)** Any invocable can be run from CLI, HTTP/SSE, and (FR-14) ACP with zero per-surface code.
- **FR-3 (P0)** Agents declare tools (functions, MCP servers) and capabilities (filesystem, shell) declaratively; sandbox is the default capability backing.
- **FR-4 (P1)** Handoffs and **advisors**: an agent may transfer a conversation to a peer, or consult a peer and resume, with child-run cost attribution and cancel-cascade. (Design: child runs via `parent_run_id`.)
- **FR-5 (P1)** Reusable work units: one skill implementation usable as a workflow node, an agent capability, and a standalone run.

### 4.2 Sessions, memory & the record

- **FR-6 (P0)** Conversations persist by `session_id` across turns, surfaces, and process restarts; agents receive exact prior context (ADR-D5 fidelity guarantee).
- **FR-7 (P0)** Every run is fully replayable from the event log: ordered, loss-detectable (contiguous seq), attributable per speaker (`origin`) and per customer (`tenant`).
- **FR-8 (P0)** Tool results are recorded with preview + hash + size; large artifacts by reference.

### 4.3 Run control & governance

- **FR-9 (P0)** Pause, resume, and cancel any run by id, from any process; signals on finished runs are safe no-ops; documented safe-point semantics.
- **FR-10 (P0)** Human-in-the-loop interrupts survive restarts; a pending inbox lists them; resume with a value continues exactly where stopped.
- **FR-11 (P0→P1)** Steering: append user input to an in-flight run, applied at the next safe point (schema in P0, endpoint in Story 3b).
- **FR-12 (P0)** Budgets (tokens/USD) and deadlines on any run; structured failure codes (`budget_exceeded`, `deadline`, …) for alerting and retry policy.

### 4.4 Protocols & surfaces

- **FR-13 (P0)** HTTP + SSE chat/workflow API, wire-compatible with v1.2.1.
- **FR-14 (P0)** ACP: any agent usable inside ACP editors, with editor-owned filesystem and permission prompts (caller-injected capabilities).
- **FR-15 (P1)** A2A serving (partner agents call our invocables; their tasks appear in our inbox/audit) and A2A consuming (`RemoteAgent` as a first-class invocable in handoffs/nodes).
- **FR-16 (P1)** A2UI: agents emit structured UI surfaces (forms, approval cards) rendered by any listening client; submissions flow back as capability calls.

### 4.5 Collaboration (group sessions)

- **FR-17 (P1)** Group sessions: multiple agents + humans in one session; participants mention each other; agent questions to humans park durably as interrupts.
- **FR-18 (P1)** Pluggable **Moderator** (itself an invocable) owns turn-taking: v1 policy = resolve-interrupts + route-by-mention; termination budget (max agent turns between human messages); silence rule (unaddressed messages go to the human).
- **FR-19 (P2)** Bid-round moderation: cheap relevance probes let agents volunteer ("standing attention" triggers), moderator serializes winners. Opt-in per session.

### 4.6 Automation & standing intents

- **FR-20 (P1)** Triggers as one mechanism (`TriggerPort` + trigger service) with types: cron ("DigestAgent daily 07:00"), webhook (per-invocable inbound URL), log-pattern ("on any run.failed run DiagnosticAgent"), timer/sleep ("wake me in 3 days unless replied"). Trigger owner is the run's principal for budget/audit (`triggered_by`).
- **FR-21 (P2)** TTL/escalation policies on waiting runs; recurring sessions with memory.

### 4.7 Observability, quality & operations

- **FR-22 (P0)** Traces, per-run cost, and audit exist for every engine automatically (event-sink consumers; no per-engine instrumentation).
- **FR-23 (P1)** Eval & regression harness: replay recorded sessions against a new agent version and diff outcomes; gate for stdlib promotion.
- **FR-24 (P1)** Stdlib & templates: `pip install agentdeck[toolkit]` provides tested, eval-gated agents/skills; `agentdeck new --template` scaffolds owned starting points; third-party bundles via entry points.
- **FR-25 (P2)** Operations console: live sessions, run trees, interrupt inbox, cost, time-travel replay. Only after schema stability is proven in production.

## 5. Non-functional requirements

- **NFR-1 Compatibility:** v1.2.1 public API and SSE wire format preserved (golden-file verified); `.agentdeck/` convention unchanged.
- **NFR-2 Extensibility proof:** adding engine/protocol/consumer #N+1 touches zero existing components — verified by diff on the ACP story and enforced thereafter.
- **NFR-3 Substitutability:** one contract-test suite passes identically on every engine.
- **NFR-4 Determinism & testability:** all platform tests run without network or API keys (scripted fake models).
- **NFR-5 Schema stability:** event schema versioned (D8), envelope closed (D9), kinds minted only in core (D10); consumers tolerate unknown kinds.
- **NFR-6 Streaming latency:** event fan-out must not add perceptible latency to token streaming; slow sinks never stall a run.
- **NFR-7 Data governance:** tenant on every event; log and engine state have independent retention; deleting engine state never deletes the record.

## 6. Release phasing

| Release | Contents | Gate |
|---|---|---|
| **M0 — Skeleton** | Schema v1, crude runtime + both engines, UC1–3 demos | falsifier review passes |
| **v2.0 — Core epic** | FR-1/2/3/6–14, 22 · NFR-1–6 | epic demo: one agent, three surfaces, cross-process control · *tagged 2026-08-06* |
| **v3.0 — One way to work** | No new FRs: v1's authoring surface and `App` are deleted, `authoring/` and `Deck` replace them, every adapter sits behind a port | `make check` green with no import-linter exemption; migration guide published |
| **v3.1 — Batteries** | FR-24 stdlib + FR-23 eval harness + FR-4/5 | first eval-gated stdlib agents ship |
| **v3.2 — Rooms & reach** | FR-15/16/17/18 + FR-20 triggers | group-session demo; partner A2A round-trip |
| **v3.3 — Operate** | FR-25 console + FR-19/21 | console reads only the log (no new pathways) |

**Amendment 2026-08-08.** A breaking release was inserted between v2.0 and the batteries,
so the last three rows renumbered from v2.1/v2.2/v2.3 — contents unchanged. v3.0 carries no
functional requirement of its own: it is the release that spends the breaking-change budget
once, deliberately, so v3.x can be additive. See `delivery/plan-v2-cutover.md`.

## 7. Success metrics

Lines of user code for the reference claims system ≤ 100 (Appendix B target ~80).
Time-to-first-running-agent from install < 5 minutes. New protocol adapter ≤ 1 week and
zero core diff. 100% of runs replayable; transcript-fidelity test green in CI
continuously. Zero breaking changes experienced by v1.2.1 projects across v2.x.

**Amendment 2026-08-08.** That last metric is retired, not missed: v3.0 deletes v1's public
API by ruling (`delivery/plan-v2-cutover.md` ruling 1), so a v1.2.1 project migrates rather
than upgrades. It is replaced by the same promise one major later — **zero breaking changes
across v3.x**, which is what the v3.0 API freeze exists to make payable.

## 8. Out of scope (standing refusals)

YAML/JSON agent DSL; auth/identity system inside the core; marketplace infrastructure;
hosted multi-tenant control plane; dashboard before schema stability. Rationale in
design doc §12; changes to this list require a design review, not a feature request.

## 9. Open product questions

Group-session pricing/attribution when a partner's A2A agent participates (whose budget?
— current answer: session owner's, revisit with real usage). Moderator defaults for
consumer-facing vs. internal rooms. Which stdlib agents ship first (proposal: Researcher,
Summarizer, DocParser — validate against real demand before building).
