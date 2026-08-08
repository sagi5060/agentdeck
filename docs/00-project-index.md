# 00 — AgentDeck v2 Project Index

**The one file to read first.** It maps every document, establishes which one wins when
they disagree, reconciles the known deltas between them, and gives the execution order.
Date: 2026-08-04.

---

## 1. The document set

| # | File | What it is | Audience |
|---|------|------------|----------|
| 1 | `project-brief.md` | One-page what/why/scope/risks | anyone, 3 minutes |
| 2 | `agentdeck-prd.md` | Product requirements FR-1…25, personas, release phasing | product view |
| 3 | `design/agentdeck-v2-architecture.md` | The design: three rings, core nouns, ports, worked examples, SOLID scorecard, migration map. Appendix A: stdlib. Appendix B: user's-view examples (load-bearing) | engineers |
| 4 | `design/adr-d5-two-stores.md` | Decision record: event log vs engine execution state. **Supersedes** D5 in doc #3 | engineers |
| 5 | `delivery/epic-agentdeck-v2-core.md` | Delivery plan: 1 epic, 5 stories, acceptance criteria, dependency graph | delivery |
| 6 | `delivery/milestone-0-walking-skeleton.md` | Pre-epic validation spike: 3 adversarial use cases, per-step gates, falsifier checklist | delivery |
| 7 | `prompts/pr0-baseline-prompt.md` / `prompts/pr0-review-prompt.md` | Executable handoff: author + reviewer prompts for the safety-net PR | coding agent |
| 8 | `prompts/pr1-event-schema-prompt.md` | Executable handoff: the frozen event schema spec as an author prompt. **Most current schema statement** | coding agent |
| 9 | `delivery/docs-site-plan.md` | External docs site (`docs-site/`): IA, content rules, anti-rot tests, phases DS-0…DS-4 | delivery, docs |
| 10 | `delivery/milestone-0-findings.md` | M0's go/no-go checkpoint: falsifier review, schema-as-built diff, learning note, decision log, keep/harden/discard | delivery, engineers |
| 11 | `design/adr-d11-store-assigns-seq-and-time.md` | Decision record: the store assigns `seq` and `ts` in the same atomic step that persists an event. **Supersedes** `claim_start`'s "a store never reads a clock" and the envelope-stamping split in doc #3 | engineers |

**Reading orders.** New engineer: 1 → 3 (through §9) → 4 → 8 → 6 → 10 → 5. Product/stakeholder:
1 → 2 → 3 Appendix B. Implementer starting today: 7 → 8 → 6 → 10.

## 2. Precedence — which document wins

Documents were written in conversation order and later ones refine earlier ones. Rule:
**the more specific and more recent artifact wins**, concretely:

1. `prompts/pr1-event-schema-prompt.md` **is** the event schema. Where design doc §4.2 differs
   (it predates several decisions), PR #1 wins.
2. `design/adr-d5-two-stores.md` **is** the session-state rule. Design doc §5/§11/§12-D5 read
   through the ADR's §5 amendment list until edited.
3. `design/adr-d11-store-assigns-seq-and-time.md` **is** the rule for who assigns `seq` and `ts`,
   and it outranks rule 1 on that one question. It supersedes coding-standards §6
   (`docs/coding-standards.md:113`), ADR-D5's *Explicitly unchanged* clause (`:151`, "`Runtime`
   still stamps and appends every event"), `prompts/pr1-event-schema-prompt.md:34` and `:121`
   (frozen as history, so superseded here rather than edited), and the design doc's
   envelope-stamping split. **ADR-D5's two-store rule is untouched**, as is the engine boundary —
   engines yield payloads, never envelopes.
4. `delivery/milestone-0-walking-skeleton.md` reorders early delivery: the epic's Phase 1/2 now
   execute *through* the skeleton (see §4 below). Epic story content is unchanged;
   sequencing defers to this index.
5. The PRD owns *what and for whom*; the design doc owns *how*; neither restates the
   other. A conflict between them is a bug in one of them — flag it, don't guess.

## 3. Known deltas (recorded so nothing is silently inconsistent)

| Where | Delta | Resolution |
|---|---|---|
| Design doc §4.2 envelope & kinds | Predated `origin`, nested payload, `UnknownEvent`/`parse_event`, D9, D10, decisions A/B, `input.appended`, `run.started` join point | **Applied 2026-08-04** — §4.2 rewritten as a summary of the PR #1 schema (which remains authoritative) |
| Design doc D5 + §5 + §7 example + §11 migration row | Superseded by ADR-D5 (two stores; `SessionFactory` moves into the openai-agents adapter) | **Applied 2026-08-04** — all five passages amended; D9/D10 added to §12 |
| Epic Story 2 | Scope sentence re-homed `sessions.py` into `adapters/stores/` (pre-ADR); ADR-required tests absent *(note: an earlier version of this row misdescribed the delta as an obsolete acceptance criterion — no such AC existed)* | **Applied 2026-08-04** — scope corrected; transcript-fidelity + crash-reconciliation ACs added |
| Epic Story 3 | Steering (Story 3b: mailbox gate, `POST /runs/{id}/messages`) decided after the epic was written | **Applied 2026-08-04** — Story 3b added to Story 3 scope, same release |
| Milestone 0 §3 (UC2) | Two seq checks decided later: seq continuity across the kill/restart; double-resume race → exactly one winner | **Applied 2026-08-04** — added to UC2's make-sure list |
| Milestone 0 header | "A=contiguous, B=full-text — pending confirmation" | **Applied 2026-08-04** — confirmed; UC1/UC3 still test them empirically |
| PRD FR-17–21 (group sessions, moderator, advisors, triggers) | Designed in conversation, not yet in the architecture doc | Open — each gets a feature spec doc at its epic (v2.1/v2.2); architecture impact already assessed: zero new kinds, one new port (`TriggerPort`), one new component (Moderator as Invocable) |
| coding-standards §6 (`:113`) | Read "the Runtime is the **only** assigner of `seq`" — superseded by ADR-D11 | **Applied 2026-08-08** — §6 now states the store's assignment as the law |
| coding-standards §1 precedence | Enumerates "D1–D10 and ADR-D5"; by its own ordering D11 outranks nothing | **Applied 2026-08-08** — D11 named |
| ADR-D5 `:151` *Explicitly unchanged* | "`Runtime` still stamps and appends every event" — the exact sentence D11 overturns | **Applied 2026-08-08** — dated amendment added; D5's two-store rule untouched |
| `prompts/pr1-event-schema-prompt.md:34,121` | "assigned by the Runtime" / "seq is assigned only by the Runtime" | Superseded by precedence rule 3 above. Prompts are frozen (§6 below), so **not edited** |
| `core/ports/store.py` docstrings, `runtime/service.py:5,536-538`, `test_runtime_service.py:890` | All asserted or pinned Runtime-assigned `seq` | **Applied 2026-08-08** — port and `_drain` docstrings rewritten; the gap assertion flips `[2]` → `[]` |
| Design doc envelope-stamping split | Predated ADR-D11 | **Applied 2026-08-08** — dated amendment added beside it; the envelope line in §4.2 names the store |
| `Runtime(clock=...)` / `build_runtime(clock=...)` | Inert since ADR-D11: nothing above the store stamps a `ts`, so the keyword is accepted and forwarded to nothing | Open — documented as inert rather than removed; dropping a public keyword is a breaking change owed its own PR |

## 4. Execution order (single source of truth for "what's next")

```text
    PR #0  safety net: golden SSE baselines + import-linter          [prompt: doc 7]
    PR #1  event schema v1 = Skeleton Step 1                         [prompt: doc 8]
    M0 Steps 2–5: Runtime+stub → openai-agents+UC1 →
        langgraph+UC2 → control+UC3                                  [gates: doc 6 §5]
    M0 finish: demo script · falsifier review (GO) ·
        schema-as-built diff · findings note · keep/harden/discard    [doc 6 §6 —
        DONE, `delivery/milestone-0-findings.md`, `scripts/m0_demo.py`, #57]
    Epic Story 2 (the seam, full quality — re-sequenced per the
        findings note) → v2.0.0 tagged 2026-08-06
NOW ──▶ v3.0.0 the cutover: phases 0–3 done on `feat/v3-cutover`;
        phase 4 (`authoring/`) blocked on the entry-point ruling      [#88 —
        plan: `delivery/plan-v2-cutover.md`, brief:
        `delivery/decision-v3-entry-point.md`]
    ──▶ v3.1 batteries → v3.2 rooms & reach → v3.3 operate          [PRD §6]
```

**Amendment 2026-08-08.** v3.0.0 was not in this order when it was written: the epic
planned v2.1 next. `plan-v2-cutover.md` ruling 1 (v1's public API is dropped, not facaded)
makes the next release breaking, so the batteries train renumbers behind it — v2.1 → v3.1,
and so on. Nothing about the *contents* of those releases changed, only their numbers.
GitHub milestones mirror this exactly: `v3.0.0 — one way to work` (the cutover plus the
release hygiene that only makes sense against the surface being frozen), `v3.1 — batteries`
(additive on the frozen API), and `docs-site` (parallel, never release-blocking).

Note the relationship between skeleton and epic: Milestone 0 *is* Phase 1 plus a crude
Phase 2/3 slice. After go/no-go, epic Story 2 hardens the skeleton's adapters and Runtime
to production quality rather than starting fresh (per M0's keep/harden/discard decision).

## 5. Decision log (index of numbered decisions across the set)

- **D1–D8** — design doc §12 (engine boundary, no DSL, content blocks, caller-injected
  capabilities, two-store rule *(as revised by ADR-D5)*, cooperative cancel, ctx
  everywhere, event versioning).
- **D9** — the envelope is closed (8 fields); new needs go in payloads or `run.started`.
  Stated in PR #1 prompt.
- **D10** — kinds are minted only in core; engines translate or use namespaced `custom`;
  recurring `custom` = promotion signal. Stated in PR #1 prompt. *(Fired once, 2026-08-06,
  issue #101: two engines routing structured data around the schema promoted `DataBlock`
  into `core/content.py` — design doc §4.1/§4.2, additive under D8.)*
- **Schema review decisions 1–9 + A + B** — enumerated in the PR #1 prompt (nested
  envelope, UnknownEvent, contiguous Runtime-assigned seq, origin, message_id, usage
  per-call+aggregate, preview+hash results, structured run.failed, naming; A=contiguous,
  B=full text).
- **Standing refusals** — PRD §8 / design doc §12; changing the list requires design
  review.

## 6. Housekeeping rules for the doc set

One statement of each fact: requirements live in the PRD, mechanisms in the design doc,
sequencing here. When implementation diverges from a doc, the doc gets a dated amendment
in the same PR (the fold-back pass after M0 is the first scheduled instance). Every new
feature epic (group sessions, triggers, stdlib) opens with its own spec doc and one row
added to §1 and §4 here. Prompts (docs 7–8) are frozen once their PR merges — history,
not living docs.
