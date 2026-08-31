---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m1
---

# M4 Grill 35: predicate ownership, receipts, and final design freeze

Date: 2026-08-31

## Status and authority

This note preserves the complete Round 35 question frontier and complete response from the same fixed
`gpt-5.6-sol` / `max` reviewer used for M4 Grills 32 through 34. The reviewer was reused as one read-only agent; it
made no file, commit, push, evidence, or runtime change. Its result is synchronized into the
[M4 index](../detailed_design/m4/README.md) and
[M4-D normative detailed design](../detailed_design/m4/m4-d-evidence-ownership-and-freeze.md).

This session record is non-normative. Accepted ADRs and M4-A/B/C/D outrank it. Every item explicitly left OPEN remains
an implementation/evidence selection; inclusion here does not make it a final runtime contract or evidence claim.

## Submitted independent frontier

| Question | Recommended alternative | Required cut |
| --- | --- | --- |
| Q35.1 | B | move exact protection-release CAS into M4 with its complete read-quiescence predicate; leave physical deletion/GC to M5 |
| Q35.2 | B | give M4 one terminal/proof/fold/release writer-verifier chain and forbid alternate eligibility rows or receipt authority |
| Q35.3 | B | freeze exact predicates for all `V2-READ-001..015`, five M4 promotion candidates, and non-promotable shared predicates |
| Q35.4 | B | freeze four semantic M4 child responsibilities and one aggregate Final while leaving exact receipt bytes under OPEN-READ-09 |
| Q35.5 | B | keep OPEN-READ-08/09/15 and physical selections explicitly evidence-blocked without blocking implementation start |
| Q35.6 | B | add a read-only historical M3 dependency checker that validates immutable e5 evidence without weakening current-source `v2M3Check` |
| Q35.7 | B | add a bounded eight-file freeze manifest and a design-only gate that cannot emit M4 PASS or Final |
| Q35.8 | B | audit the complete design for redundant authorities, hot-path cost, control-plane bounds, and any remaining blocker |

The reviewer was required to return `FREEZE_READY` only if no blocking design question remained and to enumerate every
condition that must pass on the clean pushed freeze commit.

## Fixed reviewer response preserved verbatim

<!-- BEGIN FIXED REVIEWER RESPONSE -->

Round 35 verdict: accept recommendation B for Q35.1–Q35.8, with two clarifications:

1. M4 owns the irreversible exact-generation protection-release CAS because it owns every read-quiescence predicate authorizing that transition. M5 begins from exact `RELEASED` state and owns provider deletion/GC.
2. Anchor pruning and terminal/proof/fold cleanup remain M4-semantic operations. A shared scheduler or pure-domain kernel may execute them, but M5 cannot create an alternate eligibility authority.

This resolves milestone ownership without changing ADR semantics. No ADR amendment is required.

## Q35.1 — Protection-release milestone ownership

Accept B.

The smallest coherent split is:

| Cut | Owner | Result |
|---|---|---|
| Fused `PWF E -> PO E+1`, anchor, retirement-batch activation | M4 | Stops new fallback admission and fixes exact interval |
| Current-owner fallback-bearing slot drain | M4 | Local drained-through fact |
| Terminal/proof/window/fold and capability verification | M4 | Durable contiguous quiescence evidence |
| Exact source-i `[first_i,sharedLast]` verifier | M4 | Fail-closed release predicate |
| Exact protection-generation release CAS and unknown-response reread | M4 | Irreversible `RELEASED` |
| Final provider/source revalidation | M5 | Delete attempt admission |
| Physical delete, orphan handling, GC and delete response-loss recovery | M5 | Physical lifecycle |

This aligns `V2-READ-001 | M4`, which explicitly includes protection release, with milestone ownership. M4 still cannot delete a source.

Moving release adds only low-frequency control cost:

```text
N protected sources
  -> at most N exact-generation release CAS operations
  -> bounded O(N) authoritative reconciliation
```

It adds no ordinary-read allocation, atomic, fence, metadata lookup, or provider HEAD. M5 becomes simpler because it consumes one exact state rather than reconstructing read eligibility.

Required documentation reconciliation:

- M4 index and M4-C must assign release execution/reconciliation to M4.
- The implementation plan must move read-quiescence capability verification and exact protection release from M5 to M4.
- M5 must be described as consuming `RELEASED`, not calculating or persisting an alternate eligibility decision.
- `V2-READ-001` remains M4; shared `V2-READ-006/008..015` remain M4/M5 and `PLANNED`.

Frozen: the transition boundary and ownership. OPEN: source-protection wire, controller/class placement, concurrency, batch caps, and retry cadence. ADR amendment: none; ADRs 0071/0078 already define the semantic transition without fixing milestone placement.

## Q35.2 — Runtime writer/verifier boundary

Accept B with M4 ownership of read-control cleanup predicates.

| Runtime artifact or action | Authorized writer | Verifier / consumer | Failure behavior |
|---|---|---|---|
| Materialization/retention closure request | Materialization controller | M4 selector kernel | Request has no authority by itself |
| Fused selector closure, anchor, batch activation | M4 | Exact selector/transaction verifier | Conflict rereads; unknown response stops E admission |
| Local drained-through view cut | M4 owner runtime | M4 terminal verifier | Missing/incomplete drain retains protection |
| `ReadAdmissionEpochTerminalCut` | M4 | Closed terminal verifier | Create-only; mismatch/invalid occupant quarantines |
| Source-independent quiescence proof | M4 | Closed proof verifier | First valid wins; unknown response exact-rereads |
| Proof-window/head/fold | M4 | M4 contiguous-coverage verifier | Gap, regression, mismatch or cap exhaustion means `RETAIN` |
| Capability admission/evidence generation | M4 read-control plane | M4 closed verifier; M5 audit consumer | Missing/revoked/unadmitted generation means `RETAIN` |
| Per-source `[first_i,sharedLast]` verification | M4 pure kernel | Protection-release writer | Batch minimum is never authority |
| Exact protection-generation release CAS | M4 | M4 response-loss reconciler; M5 consumes `RELEASED` | Wrong/missing generation or unknown mismatch means `RETAIN` |
| Closure-anchor pruning | M4 selector writer | M4 terminal/proof predicate | Batched CAS conflict requeues; never enters read path |
| Terminal/proof/fold cleanup | M4-semantic pure predicate; physical scheduler may be shared | M4 receipt/verifier ownership | Cleanup waits for every interval/recovery/audit reference |
| `FULL_V1 -> RETIRED_V1` batch compaction | M5 | Exact member/reference scan | Any unresolved member/reference retains `FULL_V1` |
| Provider revalidation and physical deletion/GC | M5 | M5 deletion audit/recovery | Missing proof, stale source or response uncertainty retains/reconciles |
| Evidence receipts | Evidence tooling only | Final validators | Never read as runtime authority |

There is no persisted `ReleaseEligible`, `HeadReclaimDecision`, released bitmap, mutable remaining count, or receipt-driven runtime decision.

Pure predicate code may live in a shared domain module, and a shared Cell reconciler may schedule M4 and M5 work. That does not merge authority: the invoked kernel and written state determine ownership. Exact package/class/executor placement remains OPEN.

Tradeoff: M4’s durable control plane is larger, but it eliminates duplicated M4/M5 eligibility state and prevents M5 from reinterpreting read proof. All added work is low-frequency.

Frozen: writers, verifier ownership and failure semantics. OPEN: physical modules, backend transactions, canonical encoding, scheduling and caps. ADR amendment: none.

## Q35.3 — Scenario and predicate ownership

Accept B.

| Scenario | M4 predicate | M5 predicate | Earliest promotion |
|---|---|---|---|
| `V2-READ-001` | Complete captured-view/source/fallback matrix, locator and fallback-view drain, exact protection release; no deletion | None | M4 Final |
| `V2-READ-002` | No promotable predicate; exact `RELEASED` may be an input | Trim/GC never deletes live or ambiguously owned source, including response loss | M5 Final |
| `V2-READ-003` | Immutable historical M3 dependency plus fresh M4-source affected regression; active-tail locator publication/readability and pin-safe retirement | None | M4 Final |
| `V2-READ-004` | Allocation-free logical capture, bounded generations, separate locator/fallback protection drains and backpressure | None | M4 Final |
| `V2-READ-005` | StoreLoad capture, stable scan, multi-Binding reservation, complete async lifetime and performance | None | M4 Final |
| `V2-READ-006` | Fused closure, local drain, exact capability/interval proof and per-source release CAS | M5 consumes exact `RELEASED`, revalidates and executes Object-WAL GC | M5 Final |
| `V2-READ-007` | Exact lease ABA, cancellation gate, terminal source/buffer drain, quarantine and performance | None | M4 Final |
| `V2-READ-008` | Read Admission Epoch order, reusable proof window/fold, interval coverage, caps and release behavior | M5 proves only exact covered/released sources enter GC | M5 Final |
| `V2-READ-009` | Immutable capability binding through selector, epoch, proof, fold, batch and release; missing/revoked means `RETAIN` | Deletion audit consumes the same capability/protection identities; no reinterpretation | M5 Final |
| `V2-READ-010` | Exact selector CAS, first-epoch inheritance, mixed-first intervals, fallback-conditional liability | Retention/GC controller integration cannot bypass selector-derived batch/released state | M5 Final |
| `V2-READ-011` | Irreversible terminal cut, closed verifier, deterministic create-only proof, invalid-occupant quarantine | M5 recovery/deletion consumes only the exact verified proof chain | M5 Final |
| `V2-READ-012` | Four-action fused closure, anchor, `ADMITTING/STOPPED`, response-unknown and emergency reserve | M5 controller integration respects the frozen selector/batch result | M5 Final |
| `V2-READ-013` | Per-source interval verification, exact release CAS, immutable batch identity and derived release state | Full-batch reference scan and retirement/compaction after every prerequisite | M5 Final |
| `V2-READ-014` | Inline anchor set, terminal convergence, proof/fold lifecycle and batched anchor pruning | M5 supplies only downstream reference-consumption facts; cannot prune or prove eligibility independently | M5 Final |
| `V2-READ-015` | Exact released-member facts and proof that tombstones grant no release/GC authority | Exact `FULL_V1 -> RETIRED_V1`, response-loss recovery, permanence and capacity behavior | M5 Final |

M4’s eventual scenario promotion allowlist is exactly:

```text
V2-READ-001
V2-READ-003
V2-READ-004
V2-READ-005
V2-READ-007
```

`V2-READ-003` cannot be promoted from the historical M3 Final alone. It requires a fresh affected-path regression at the M4 tested source covering the shared VerifiedExtent, active-tail publication/read seam, locator retirement and relevant Kafka/Pulsar Object-WAL integration.

For `V2-READ-006/008..015`, M4 emits exact non-promotable predicate receipts. Their scenario rows remain `PLANNED` with no promotable evidence receipt until M5 supplies fresh compatible predicates and composes the whole scenario.

`V2-KAF-DATA-013` and other M4/M5/M6 rows may reference M4 predicates, but remain `PLANNED` until every named milestone is complete. Existing `V2-BK-*` statuses and receipts remain unchanged; M4 P4 regressions do not repromote or replace M2 evidence.

Frozen: scenario/predicate ownership and promotion allowlists. OPEN: physical test inventory and receipt schema. ADR amendment: none.

## Q35.4 — M4 evidence/receipt hierarchy

Accept B. Four children are minimal, not overdesigned, provided their boundaries remain distinct.

| Child | Exclusive semantic responsibility |
|---|---|
| `READ_VIEW_HAZARD` | M4-A/C memory order, stable cell/scan, slot ABA, terminal lifetime, cancellation and concurrency micro-performance |
| `SOURCE_PLAN_EXECUTION` | M4-B profile/protocol routes, validation, semantic equivalence, failure precedence, one-shot fallback, observability and delegated P4 regressions |
| `QUIESCENCE_PROTECTION_RELEASE` | Selector/epoch/anchor/terminal/proof/window/fold/capability, per-source interval verification, release CAS and response-loss reconciliation |
| `CURRENT_SOURCE_INTEGRATION_PERFORMANCE` | Current-source Kafka/Pulsar/Object-WAL integration, fresh affected M3 seam regression, end-to-end allocation/atomic/latency/capacity results |

The fourth child must not duplicate every microbenchmark from the first two; it owns cross-component/current-source integration and end-to-end performance.

Each child is Final-admissible but individually non-scenario-promoting. A partial child or predicate receipt cannot set a scenario to PASS.

Aggregate M4 Final rules:

- All four children bind one identical M4 tested commit T and source-lock SHA S.
- Every behavior-affecting child test runs from T.
- Raw attachments are immutable, safe-relative, length/SHA-bound, closed-kind and revalidated by the aggregate.
- A closed M4-only evidence-publication descendant chain may carry receipts and scenario synchronization, but may change no runtime, configuration or normative M4 design file.
- Any behavior-affecting change creates a new T and requires fresh affected evidence; M4 Final does not bless a later source by ancestry alone.
- The aggregate binds exact child paths/SHAs, T/S, the frozen historical M3 identity, the five-scenario M4 promotion allowlist, the exact non-promotable shared-predicate inventory, and exclusions for M5 deletion, M6 and M8.
- Missing/extra/duplicate child, source mismatch, scenario borrowing, stale attachment, or partial-predicate promotion fails closed.
- The design gate and historical M3 dependency checker are prerequisites, not children, receipts or scenario evidence.

M5 freshness defaults to rerunning every affected M4 predicate at its tested source. It may reuse a predicate only if a separately frozen compatibility rule proves the relevant behavior and receipt contract unchanged. No such compatibility shortcut is frozen now.

Exact canonical fields, wire sizes and caps remain blocked by `V2-OPEN-READ-09`; this round freezes responsibility and freshness, not bytes.

Frozen: four-child hierarchy, common source, Final allowlists and freshness rules. OPEN: receipt encoding, attachment kinds/caps, concrete suites and environments. ADR amendment: none.

## Q35.5 — Evidence-blocked OPEN frontier

Accept B.

| OPEN item | Blocks design freeze? | Blocks implementation start? | Blocks activation/promotion |
|---|---:|---:|---|
| `V2-OPEN-READ-08` proof-window/head/fold physical evidence | No | No; candidate implementations and benchmarks may start | Yes: blocks production representation selection, active release path depending on it, `QUIESCENCE_PROTECTION_RELEASE`, and M4 Final |
| `V2-OPEN-READ-09` capability/receipt encoding and backend admission | No | No; codecs/verifiers/candidates may be implemented fail-closed | Yes: blocks production capability admission, release activation, final canonical proof/capability/receipt wire and related M4/M5 Finals |
| `V2-OPEN-READ-15` tombstone deletion | No | No | Blocks only future tombstone deletion; permanent `RETIRED_V1` is valid 0.2 behavior and may be promoted with deletion excluded |
| Single-ref/seqlock, VarHandles, pool layout, lifecycle family, numeric budgets, provider mappings, buffer accounting | No | No | Each blocks only the child/Final that owns its selection and evidence |

No evidence-selected OPEN item blocks implementation start after the hard design gate. Implementation must begin with fail-closed defaults:

- no production proof/capability activation before 08/09 evidence;
- missing or revoked capability remains `RETAIN`;
- no tombstone deletion;
- no unmeasured physical candidate may be described as selected.

Frozen: logical semantics and default-off/retain behavior. OPEN: the physical selections and evidence. ADR amendment: none.

## Q35.6 — Historical M3 dependency checker

Accept B.

`v2M4HistoricalM3DependencyCheck` must be read-only, non-promotable and purpose-specific. It may reuse the canonical M3 value, child, attachment and scenario validators, but must not call the wrapper that invokes `validate_descendants(tested,current HEAD)`.

Required validations:

| Category | Exact requirement |
|---|---|
| Final identity | Exact e5 path and SHA `81c7004a...f84a`; reject the legacy root path |
| Tested source | `e5e53e62865c21845621037bea5f18c092bd4259` |
| Source lock | SHA `2a46f31c...c240b` derived from the exact Git blob at e5 |
| Allocator | Final mode `RANGE`, selected candidate `RANGE_64`, required fault/native/10k/100k evidence |
| Children | Exact ordered closed inventory of 11 |
| Scenarios | Exact ordered 26-scenario M3 allowlist and current scenario receipt/status bindings |
| Exclusions | Exactly M6 process activation and M8 native parity |
| Provider evidence | Exact real Provider/KMS and non-promotable C2 semantics |
| Current immutable files | Final, child receipts and attachments at current checkout exactly match bound lengths/SHAs and safe paths |
| Ancestry | `e5 -> efab430aed37b3f7c32d09b88ae935c1aea1c902 -> current HEAD` |
| Side effects | No evidence write, campaign, formal task, publication, scenario mutation or source-lock rewrite |

Only the Final-bound e5 source-lock blob must retain exact historical bytes. The current working `docs/v2/source-locks.json` may legitimately evolve for M4 and must not be required to equal e5. It happens to match today, but that is not a frozen historical-dependency rule. Current M4 source-lock freshness belongs to future M4 children/Final.

Minimum fail-closed tests:

- missing, alternate-path, modified or noncanonical Final;
- wrong fixed Final SHA or tested commit;
- wrong/missing e5 source-lock blob or digest;
- child omission/addition/reorder, changed receipt, attachment, length or SHA;
- duplicate, absolute, escaping, symlinked or oversized evidence path;
- allocator mode/candidate or required evidence mismatch;
- scenario omission/addition/reorder, borrowed receipt, wrong status or receipt path;
- changed exclusions/provider evidence;
- broken `e5 -> efab` or `efab -> HEAD` ancestry;
- dirty/modified immutable M3 evidence path;
- a normal M4 source/design descendant must pass this checker while still causing the unchanged `v2M3Check` descendant policy to fail;
- a changed current M4 source-lock file with an unchanged exact e5 blob must not fail the historical checker.

Frozen: identity, ancestry, validations and non-promotable behavior. OPEN: script factoring and test implementation. ADR amendment: none.

## Q35.7 — Hard-freeze design gate

Accept B and retain a small freeze manifest. It is useful rather than overdesigned if it binds only immutable design records.

Recommended manifest contents:

```text
docs/v2/detailed_design/m4/m4-a-read-view-authority.md
docs/v2/detailed_design/m4/m4-b-source-plan-and-fallback.md
docs/v2/detailed_design/m4/m4-c-hazard-slot-reclamation.md
docs/v2/detailed_design/m4/m4-d-evidence-ownership-and-freeze.md
docs/v2/grill-notes/32-m4-read-snapshot-authority.md
docs/v2/grill-notes/33-m4-read-path-fallback-matrix.md
docs/v2/grill-notes/34-m4-hazard-slot-reclamation-races.md
docs/v2/grill-notes/35-m4-final-design-freeze.md
```

The manifest should contain sorted unique safe-relative path/SHA-256 rows, not bind itself, and contain no commit identity. It must not include the living M4 index, implementation plan, scenario registry, open-questions page, Gradle/scripts, or evidence paths; those must remain editable and are validated semantically by the gate.

A changed bound design file requires a deliberate manifest update and design amendment/review. The gate must never auto-refresh hashes.

`v2M4DesignCheck` requirements:

| Gate predicate | Required value |
|---|---|
| Documentation | M4 index/A/B/C/D included in `v2DocumentationCheck` and links resolve |
| Design status | Index and A/B/C/D accepted; implementation `NotStarted`; evidence `NotRun` |
| Reviewer records | Grills 32–35 present with exactly one complete begin/end response block |
| Design frontier | No remaining blocking design question; physical/evidence OPEN items remain explicitly open |
| Scenario boundary | `001/003/004/005/007` PLANNED M4 candidates; `002` M5; `006/008..015` shared and PLANNED; no M4 receipt |
| Existing M2/P4 | Referenced `V2-BK-*` PASS rows and receipts unchanged |
| OPEN boundary | 08/09/15 remain open with the exact scope above |
| M3 history | `v2M4HistoricalM3DependencyCheck` passes |
| Freeze manifest | Exact closed eight-file set, canonical order, unique safe paths and matching hashes |
| Runtime/evidence absence | No M4 implementation claim, receipt, Final or scenario promotion |
| Output | Exactly `DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED` as the design-check result |

The design check must not invoke or alias `v2M3Check`, run evidence, or register a passing placeholder `v2M4Check`.

Minimum negative cases:

- missing or hash-changed bound design/review file;
- extra, duplicate, escaping or reordered manifest entry;
- missing/duplicate/truncated reviewer marker;
- `Proposed`, `InProgress`, implemented or evidence-run status;
- premature scenario PASS or evidence receipt;
- shared/M5 scenario borrowed into the M4 allowlist;
- 08/09/15 removed or falsely resolved;
- changed M3 identity or ancestry;
- M4/M5 ownership regression such as returning protection release to M5;
- unexpected M4 Final/receipt;
- design check emitting M4 PASS or Final.

The gate itself need not reject unrelated dirty implementation files forever. The actual hard-freeze publication condition separately requires a clean commit, push and verified `HEAD == origin/main`.

Frozen: gate semantics and bounded manifest scope. OPEN: script language and manifest serialization. ADR amendment: none.

## Q35.8 — Final complexity and performance audit

Accept B. The combined design has no remaining redundant authority if Q35.1–Q35.7 are synchronized.

Complexity findings:

- No per-read remote control-metadata I/O, HEAD, durable ACK or receipt lookup.
- No required per-read heap snapshot, pin or source-plan graph.
- No global read revision, process-wide refcount or shard-wide ticket increment.
- `SlotLeaseWord` remains only `FREE|PINNED(L)`; async phases remain in the batch lifecycle.
- Selector state remains only `ADMITTING|STOPPED`; no closing/draining progress state.
- The shared source layer is a typed plan/result, not a generic BK/Object state machine.
- Pulsar P4 remains delegated; its inner native pin and the outer M4 generation lease protect different authorities and are not duplicate pins.
- Fallback is sequential, single-transfer and pre-observability; no candidate racing or generic double pin.
- Selector, terminal cut, proof, protection state and retirement tombstone each have one distinct job. No `ReleaseEligible`, mutable progress bitmap or receipt authority is admitted.
- Historical M3 validates immutable dependency identity; fresh M4 regression proves current affected behavior. Neither substitutes for the other.

Non-negotiable per-Binding read-batch hot-path target:

```text
one fresh FREE -> PINNED(L) acquisition
one exact {Binding,G,L} hazard publication
one StoreLoad/full fence
two exact authority/generation acquire loads
one stable generation-tagged cell capture
equality loads during source use/callbacks
one unique terminal CAS(L -> FREE)
zero ordinary-read remote control metadata
zero required heap snapshot/plan/pin allocation
zero per-callback slot CAS
```

The outstanding-use implementation family may add source-use RMWs only if evidence beats owner/event-loop serialization without violating allocation or cache-line targets. The two families must not be combined.

Bounded control-plane cost for one retirement batch with N sources, E fallback-relevant epochs and bounded anchor set K:

```text
activation:
  inline       = 1 selector CAS
  referenced   = 1 immutable create + 1 transactionally validated selector CAS

per relevant epoch:
  <= 1 terminal create
  <= 1 source-independent proof create

proof maintenance:
  bounded window/head/fold work selected under OPEN-READ-08
  no owner x batch accumulator

release:
  <= N exact protection-release CAS operations
  bounded O(N) authoritative reconciliation

cleanup:
  bounded O(K) selector payload/copy
  batched anchor-prune CAS
  terminal/proof/fold cleanup only after exact no-reference predicate

M5:
  <= 1 exact FULL_V1 -> RETIRED_V1 CAS per batch
  O(N) provider deletion/reconciliation as physically required
```

Hard caps convert overload, proof gaps, invalid evidence, provider nonresponse and quarantine into `STOPPED`, `RETAIN`, admission backpressure or safe failure. They never create early release or deletion.

No blocking design question remains. The remaining questions are implementation/evidence selections already assigned to 08/09, child evidence, or provider-specific execution.

## FREEZE_READY

M4 is design-freeze ready only after all of the following occur on one synchronized commit:

1. M4-D and Grill 35 preserve this complete response.
2. M4 index, M4-C, implementation plan and scope docs assign protection release/verifier ownership to M4 and deletion/GC to M5.
3. The exact scenario/predicate and four-child receipt tables are synchronized without changing any current scenario status.
4. `v2M4HistoricalM3DependencyCheck` and its fail-closed contract tests exist without weakening `v2M3Check`.
5. `v2DocumentationCheck`, the bounded freeze manifest and `v2M4DesignCheck` enforce the design boundary and emit only `DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED`.
6. A/B/C/D are `Accepted`, all four review records are complete, and 08/09/15 remain explicitly OPEN.
7. No M4 runtime implementation, evidence receipt, Final or scenario promotion is claimed by the freeze commit.
8. The design/documentation gates pass from a clean worktree, the commit is pushed, and local `HEAD` is verified equal to `origin/main`.

After those conditions, implementation may start. Proof-window/capability evidence, physical layouts, provider mappings, numeric caps and runtime receipts remain future work and do not become true by design freeze.

<!-- END FIXED REVIEWER RESPONSE -->

## Synchronized decision

Round 35 closes the final blocking design frontier. M4 owns the complete read-quiescence authority through exact
protection release; M5 owns physical deletion/GC. The hard-freeze gate is documentation/governance only and cannot
stand in for implementation or evidence. Every evidence-selected physical choice remains OPEN until its owning child
and Final gates pass.
