---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M4-D evidence ownership, protection release, and design freeze

## Status and authority

This document synchronizes the accepted result of
[M4 Grill 35](../../grill-notes/35-m4-final-design-freeze.md) with ADRs 0069 through 0080 and M4-A/B/C. Those ADRs
remain higher authority. This document freezes milestone ownership, predicate composition, semantic receipt
responsibilities, historical M3 dependency validation, and the design-only hard-freeze gate. It selects no runtime
wire, physical proof-window layout, provider mapping, numeric cap, or evidence result. No ADR amendment is required.

The freeze authorizes implementation to start only after `v2M4DesignCheck` passes on the clean pushed freeze commit.
It is not implementation, evidence, a child receipt, scenario promotion, M4 Final, protection-release execution, or
physical deletion.

## M4/M5 ownership boundary

M4 owns the complete read-quiescence authority through the irreversible exact protection-generation release CAS. M5
starts only from exact `RELEASED` state and cannot reconstruct, reinterpret, or persist another read-eligibility
decision.

| Cut | Milestone owner | Authoritative result |
| --- | --- | --- |
| fused `PWF E -> PO E+1`, closure anchor, retirement-batch activation | M4 | new fallback admission stops; exact intervals freeze |
| current-owner fallback-bearing slot drain | M4 | local drained-through fact |
| terminal/proof/window/fold plus capability verification | M4 | durable contiguous quiescence evidence |
| source i `[first_i,sharedLast]` verification | M4 | fail-closed release predicate |
| exact protection-generation release CAS and response-loss reread | M4 | irreversible `RELEASED` |
| final provider/source revalidation | M5 | physical delete attempt admission |
| provider delete, orphan handling, GC, and delete response-loss recovery | M5 | physical lifecycle |

For N sources, M4 performs at most N exact release CAS operations plus bounded O(N) authoritative reconciliation.
This is low-frequency control work and adds no ordinary-read allocation, atomic, fence, metadata lookup, or provider
HEAD. M4 never deletes a source. Pulsar P4 retains its independent M2 source-pin/delete authority.

## Writer and verifier table

| Runtime artifact/action | Authorized writer | Verifier/consumer | Fail-closed outcome |
| --- | --- | --- | --- |
| materialization/retention closure request | materialization controller | M4 selector kernel | request alone has no authority |
| fused selector closure, anchor, batch activation | M4 | exact selector/transaction verifier | conflict rereads; unknown response stops E admission |
| local drained-through cut | M4 owner runtime | M4 terminal verifier | missing/incomplete drain retains protection |
| `ReadAdmissionEpochTerminalCut` | M4 | closed terminal verifier | create-only; invalid/mismatch quarantines |
| source-independent quiescence proof | M4 | closed proof verifier | first valid wins; unknown response exact-rereads |
| proof-window/head/fold | M4 | M4 contiguous-coverage verifier | gap/regression/mismatch/cap exhaustion returns `RETAIN` |
| capability admission/evidence generation | M4 read-control plane | M4 closed verifier; M5 audit consumer | missing/revoked/unadmitted generation returns `RETAIN` |
| per-source interval verification | M4 pure kernel | protection-release writer | batch minimum never authorizes |
| exact protection-generation release CAS | M4 | M4 response-loss reconciler; M5 consumes | wrong/missing generation or unresolved mismatch returns `RETAIN` |
| closure-anchor pruning | M4 selector writer | M4 terminal/proof predicate | conflict requeues; never enters the read path |
| terminal/proof/fold cleanup | M4-semantic pure predicate; scheduler may be shared | M4 receipt/verifier ownership | waits for all interval/recovery/audit references |
| `FULL_V1 -> RETIRED_V1` batch compaction | M5 | exact member/reference scan | unresolved input retains `FULL_V1` |
| provider revalidation and delete/GC | M5 | M5 deletion audit/recovery | stale/uncertain input retains or reconciles |
| evidence receipt | evidence tooling only | Final validators | never runtime authority |

A materialization controller can request closure but cannot synthesize read proof. Pure code may live in a shared
domain module and a shared Cell reconciler may schedule both milestones; the invoked kernel and written state still
determine authority. Exact package, class, executor, transaction mapping, scheduling, and caps remain OPEN.

The design rejects a persisted `ReleaseEligible`, authoritative `HeadReclaimDecision`, released bitmap, mutable
remaining count, batch-progress row, or receipt-driven runtime decision. Anchor pruning and terminal/proof/fold cleanup
remain M4 semantics. M5 may contribute downstream no-reference facts but cannot publish an alternate eligibility
authority.

## Scenario and predicate ownership

| Scenario | M4-owned predicate | M5-owned predicate | Earliest promotion |
| --- | --- | --- | --- |
| `V2-READ-001` | captured-view/source/fallback matrix, locator/fallback-view drain, exact protection release; no deletion | none | M4 Final |
| `V2-READ-002` | no promotable predicate; exact `RELEASED` may be input | trim/GC never deletes live or ambiguous source, including response loss | M5 Final |
| `V2-READ-003` | immutable M3 dependency plus fresh M4-source affected regression; active-tail publication/readability and pin-safe retirement | none | M4 Final |
| `V2-READ-004` | allocation-free capture, bounded generations, distinct locator/fallback protection drains and backpressure | none | M4 Final |
| `V2-READ-005` | StoreLoad capture, stable scan, multi-Binding reservation, complete async lifetime/performance | none | M4 Final |
| `V2-READ-006` | fused closure, local drain, exact capability/interval proof, per-source release CAS | consume exact `RELEASED`, revalidate, execute Object-WAL GC | M5 Final |
| `V2-READ-007` | lease ABA, cancellation gate, terminal source/buffer drain, quarantine/performance | none | M4 Final |
| `V2-READ-008` | Read Admission Epoch order, reusable proof window/fold, interval coverage, caps/release | only covered/released sources enter GC | M5 Final |
| `V2-READ-009` | immutable capability binding through selector/epoch/proof/fold/batch/release; missing/revoked is `RETAIN` | deletion audit consumes identical identities without reinterpretation | M5 Final |
| `V2-READ-010` | exact selector CAS, first inheritance, mixed-first intervals, fallback-only liability | controller cannot bypass selector-derived batch/released state | M5 Final |
| `V2-READ-011` | irreversible terminal, closed verifier, deterministic create-only proof, invalid-occupant quarantine | recovery/delete consumes exact verified proof chain | M5 Final |
| `V2-READ-012` | four-action fused closure, anchor, `ADMITTING/STOPPED`, unknown response, emergency reserve | controller integration respects frozen selector/batch | M5 Final |
| `V2-READ-013` | per-source interval verifier/release CAS, immutable batch, derived release state | full-batch reference scan and retirement/compaction | M5 Final |
| `V2-READ-014` | inline anchors, terminal convergence, proof/fold lifecycle, batched pruning | supplies downstream reference facts only | M5 Final |
| `V2-READ-015` | exact released-member facts and proof tombstone grants no release/GC authority | exact `FULL_V1 -> RETIRED_V1`, response-loss, permanence/capacity | M5 Final |

The eventual M4 scenario promotion allowlist is exactly:

```text
V2-READ-001
V2-READ-003
V2-READ-004
V2-READ-005
V2-READ-007
```

`V2-READ-003` requires both the immutable historical M3 dependency and a fresh affected-path regression at the M4
tested source covering shared `VerifiedExtent`, active-tail publication/read seam, locator retirement, and relevant
Kafka/Pulsar Object-WAL integration. Historical Final alone cannot promote it.

Rows `V2-READ-006/008..015` remain `PLANNED` after M4 and carry no promotable scenario receipt. M4 may publish exact
non-promotable predicates; M5 later supplies fresh compatible predicates and composes the whole scenario. A partial
receipt never changes scenario status. `V2-KAF-DATA-013` and other multi-milestone rows remain `PLANNED` until every
named milestone closes. Existing `V2-BK-*` status/receipt authority is unchanged; M4 regressions do not repromote M2.

## Semantic evidence hierarchy

Exact canonical receipt bytes, attachment kinds, and caps remain blocked by `V2-OPEN-READ-09`. The following four
exclusive child responsibilities are nevertheless frozen:

| Logical child | Exclusive responsibility |
| --- | --- |
| `READ_VIEW_HAZARD` | M4-A/C memory order, cell/scan stability, slot ABA, terminal lifetime, cancellation and concurrency micro-performance |
| `SOURCE_PLAN_EXECUTION` | M4-B routes, validation, semantic equivalence, failure precedence, fallback/observability, delegated P4 regressions |
| `QUIESCENCE_PROTECTION_RELEASE` | selector/epoch/anchor/terminal/proof/window/fold/capability, interval verification, release CAS and response-loss |
| `CURRENT_SOURCE_INTEGRATION_PERFORMANCE` | current-source Kafka/Pulsar/Object-WAL integration, affected M3 seam regression, end-to-end allocation/atomic/latency/capacity |

The integration child owns cross-component and end-to-end behavior; it does not duplicate all child microbenchmarks.
Each child is Final-admissible but individually non-scenario-promoting.

An eventual M4 Final must:

- bind all four children to one exact tested commit T and current source-lock SHA S;
- rerun every behavior-affecting test at T and revalidate every immutable safe-relative length/SHA-bound attachment;
- bind exact child paths/SHAs, T/S, the frozen M3 identity, five-scenario promotion allowlist, exact non-promotable
  shared-predicate inventory, and exclusions for M5 deletion, M6, and M8;
- reject missing/extra/duplicate children, source mismatch, scenario borrowing, stale attachments, or partial-predicate
  promotion; and
- treat the design gate and historical checker only as prerequisites, never children, receipts, or evidence.

Only a closed M4 evidence-publication descendant chain may add receipts/scenario synchronization after T; it may not
change runtime, configuration, or bound M4 design files. Any behavior-affecting change creates a new T and requires
fresh affected evidence. M5 defaults to rerunning every affected M4 predicate at its own tested source. No stale reuse
or compatibility shortcut is frozen.

## Evidence-blocked OPEN gates

| Gate/selection | Blocks design freeze | Blocks implementation start | Blocks activation/promotion |
| --- | --- | --- | --- |
| `V2-OPEN-READ-08` proof-window/head/fold representation/evidence | no | no; candidates/benchmarks may start | yes: representation selection, active release path, quiescence child, M4 Final |
| `V2-OPEN-READ-09` capability/receipt encoding/backend admission | no | no; fail-closed codecs/verifiers may start | yes: capability/release activation, canonical proof/capability/receipt wire, M4/M5 Finals |
| `V2-OPEN-READ-15` optional tombstone deletion | no | no | tombstone deletion only; permanent `RETIRED_V1` remains accepted 0.2 behavior |
| single-ref/seqlock, VarHandles, pool/layout, lifecycle family, numeric budgets, provider/buffer mappings | no | no | the owning child/Final until evidence selects it |

Implementation begins fail-closed: no production proof/capability/release activation before 08/09 evidence, missing or
revoked capability is `RETAIN`, tombstones are permanent, and no unmeasured candidate is described as selected.

## Historical M3 dependency contract

`v2M4HistoricalM3DependencyCheck` is a read-only, non-promotable dependency check. It reuses the canonical M3
Final-value, child, attachment, source-lock, and scenario validators but deliberately does not apply the M3 wrapper's
`validate_descendants(tested,current HEAD)` rule. Normal M4 descendants must pass this dependency check while the
unchanged exact-source `v2M3Check` continues rejecting them.

It fixes:

| Identity | Required value |
| --- | --- |
| tested source | `e5e53e62865c21845621037bea5f18c092bd4259` |
| closure/navigation ancestor | `efab430aed37b3f7c32d09b88ae935c1aea1c902` |
| Final path | `docs/v2/evidence/v2-m3/final/e5e53e62865c21845621037bea5f18c092bd4259/m3-final.json` |
| Final SHA-256 | `81c7004a923e5b96cab0a3c8b4f1fa26d71606a2208bbabe779f0d872f84f84a` |
| e5 source-lock SHA-256 | `2a46f31c90912f3f3f10d2365b9f9ffcc8070a847c4c7e202177ea903cdc240b` |
| allocator | `RANGE` / `RANGE_64`, with fault/native/10k/100k evidence |
| closure | ordered 11 children, ordered 26 scenarios, real Provider/KMS, C2 non-promotable, exact M6/M8 exclusions |

It validates current immutable Final/child/attachment bytes and current M3 scenario bindings, then proves ancestry
`e5 -> efab430a -> HEAD`. It writes nothing and runs no evidence or formal task. Only the e5 Git blob must have the
Final-bound source-lock bytes; the current working `source-locks.json` may evolve for M4 and is governed by future M4
freshness.

## Design-only hard-freeze gate

The governance-only [freeze manifest](m4-design-freeze.json) binds exactly M4-A/B/C/D and the complete Grill 32–35
records as sorted unique safe-relative path/SHA rows. It binds no commit, does not bind itself, and excludes the living
index, plan, scenario registry, open-question log, scripts/Gradle, and evidence paths. A bound design change requires
an explicit reviewed manifest update; the gate never auto-refreshes it.

`v2M4DesignCheck` composes documentation, historical M3 dependency, manifest, review-record, scenario, ownership,
OPEN-gate, and no-evidence checks. It emits exactly:

```text
DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED
```

It never invokes or aliases `v2M3Check`, runs evidence, registers a placeholder `v2M4Check`, or emits M4 PASS/Final.
Unrelated future implementation files need not remain permanently clean; the publication of this freeze requires a
clean commit, push, and `HEAD == origin/main`.

## Complexity and performance freeze

The non-negotiable per-Binding normal-path target is:

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

Owner serialization adds no source-use RMW. Outstanding-use accounting may add RMWs only if evidence beats the owner
family without violating allocation/cache-line targets; the families are not combined. The shared source layer remains
a typed plan/result, P4 stays delegated, and fallback is sequential/single-transfer/pre-observability.

For one retirement batch with N sources, E relevant epochs, and bounded anchor set K, control work is bounded by one
inline selector CAS or one immutable create plus transactionally verified CAS; at most one terminal and proof create
per relevant epoch; evidence-selected bounded proof maintenance with no owner-by-batch accumulator; at most N release
CAS operations plus bounded O(N) reconciliation; bounded O(K) selector copy and batched pruning; then at M5 at most one
exact batch-compaction CAS and physically required O(N) deletion/reconciliation. Hard caps yield `STOPPED`, `RETAIN`,
backpressure, or safe failure—never early release or deletion.

## Freeze conclusion

No blocking design question remains. M4-A/B/C/D are sufficient implementation input once the documentation,
historical-dependency, freeze-manifest, and design gates pass on the clean pushed commit. Physical layouts, proof and
capability encodings, provider mappings, numeric limits, concrete tests, receipts, and runtime evidence remain OPEN
under their explicit gates and do not become true by this design freeze.
