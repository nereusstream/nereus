---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m1
---

# M4 Grill 33: read-path authority, validation, and fallback matrix

Date: 2026-08-31

## Status and authority

This note preserves the complete Round 33 response from the same fixed `gpt-5.6-sol` / `max` reviewer used for
M4 Grill 32. The reviewer was reused as one read-only agent; it made no file, commit, push, evidence, or runtime
change. Its answer was synchronized into the [M4 index](../detailed_design/m4/README.md) and
[M4-B normative detailed design](../detailed_design/m4/m4-b-source-plan-and-fallback.md).

This session record is non-normative. Accepted ADRs and the M4 detailed design outrank it. Every item marked OPEN in
the response remains open; inclusion here does not silently turn a tentative implementation or evidence choice into
a final contract.

## Submitted independent frontier

| Question | Recommended alternative | Required cut |
| --- | --- | --- |
| Q33.1 | B | share only a typed logical plan/result; retain distinct BK-only, Kafka async-Object, Pulsar P4, and Object-WAL authorities |
| Q33.2 | B | validate cached source/open, per range, and full-object retirement cuts separately; ordinary reads add no remote metadata lookup |
| Q33.3 | B | derive one deterministic position-ordered `BindingReadSourcePlan` without persistence or mandatory heap allocation |
| Q33.4 | B | allow one exact pre-observability fallback under the same outer slot and original cumulative budget; never loop or splice |
| Q33.5 | B | require exact semantic equivalence, safe-fail conflicts, and quarantine/repair preferred-source corruption even after valid fallback |
| Q33.6 | B | keep proof-window work in the low-frequency retirement control plane and out of ordinary route selection |
| Q33.7 | B | freeze canonical authority/execution matrices and map informal aliases onto canonical scenario subcases |

The reviewer was required to identify overdesign, hot-path cost, performance hazards, complexity, ADR boundaries,
canonical scenario ownership, and every remaining implementation/evidence selection that must stay OPEN.

## Fixed reviewer response preserved verbatim

<!-- BEGIN FIXED REVIEWER RESPONSE -->

Round 33 verdict: accept recommendation B for Q33.1–Q33.7, with three important tightenings:

1. The shared M4 layer is a typed plan/result contract, not a generic BK/Object state machine.
2. “No per-read HEAD” does not repeal Pulsar P4’s already-verified lazy child-open HEAD/root/data validation; it forbids new remote metadata I/O for each ordinary range or batch.
3. `NOT_ELIGIBLE` is an admission/fence result, not a generic data-failure fallback trigger.

## Q33.1 — Profile/protocol authority split

Accept B.

The common abstraction should contain only:

- a typed protocol interval;
- an exact primary `SourceRef`;
- an optional exact protected fallback `SourceRef`;
- the primary failure classes that permit one transfer;
- protocol upper bound and validation obligations;
- a closed attempt outcome taxonomy.

Protocol/profile policy remains outside that abstraction:

- `BOOKKEEPER_WAL_ONLY` never manufactures Object eligibility.
- Kafka asynchronous Object reads derive from the captured Kafka Source Map/materialized frontier and protection identity.
- Pulsar asynchronous Object reads delegate whole-range selection, source pins, fallback, and delete-state interpretation to the existing ManagedLedger-owned P4 `DualSourceReadHandle`.
- `OBJECT_WAL` consumes captured active-tail/manifest/materialized Object identities and never infers a BK copy.

M4 may expose P4’s chosen primary/fallback in a logical plan for tracing and testing, but it must call the verified composite handle rather than recreate its error table, child handles, pins, or deletion race. That prevents redundant authority and avoids a second hot-path state machine.

Tradeoff: protocol adapters remain distinct, but the cost is small compared with the correctness risk and branch/metadata overhead of a universal backend machine.

OPEN: adapter dispatch/inlining, allocation-free physical representation, source-reader pooling, and exact provider-error normalization. No new ADR is needed. Changing P4 eligibility, fallback classes, or deletion behavior would require an ADR amendment and new M2 evidence.

## Q33.2 — Candidate validation without per-read HEAD

Accept B with the lazy-open qualification above.

Validation has three distinct cuts:

1. **Cached source/open validation**

   Validate exact Binding, Topic Incarnation, Storage Epoch, typed Position Domain, publication generation, descriptor/root identity, source key/version, lifecycle attempt, and cached owner/selector fences. Pulsar P4 may retain its verified lazy child-open HEAD/root/data validation bound to the exact attempt and immutable version. Reuse is legal only for that validated child-handle lifetime.

2. **Per-range validation**

   Before exposure, validate range coverage, protocol upper bound, expected byte count, and the source-specific wire contract:

   - Kafka BK: exact run/index floor result, ledger/entry identity, BK digest, NBKE2 framing/CRC, Kafka RecordBatch header/CRC/offset coverage and leader facts.
   - Pulsar offload: exact attempt/root/data version, intersecting block identity, NPB1 digest/authentication and contiguous entry coverage.
   - Object WAL: Root-bound directory/frame identity, AEAD/authentication, frame CRC/native framing and typed coverage.

   A cached cursor or index entry remains only a hint and must exact-match the captured descriptor.

3. **Full-object/offload revalidation**

   This is reserved for recovery, response-loss reconciliation, offload verification, and the deletion/retirement cut. It is not an ordinary read-route step. A provider token or ETag is authoritative only when the admitted Root/provider-proof mode binds its semantics; ETag presence alone is not authority.

Deterministic classification:

- `MISSING`: an authoritative source-specific not-found, such as P4’s exact native/object absence mapping.
- `UNAVAILABLE`: timeout, throttling, transport failure, or incomplete transfer where the provider contract does not prove stored-byte corruption.
- `CORRUPT_OR_FORMAT`: returned bytes violate identity, length, digest, AEAD, CRC, canonical framing, native framing, or declared coverage.
- `NOT_ELIGIBLE`: profile, lifecycle, attempt, owner, generation, or fence rejects source use before I/O.

A short read is `UNAVAILABLE` when it is an incomplete transport result, but `CORRUPT_OR_FORMAT` when the provider conclusively returned a complete immutable body with the wrong length. That mapping must be deterministic per provider.

OPEN: provider-proof modes, cache/open lifetime, checksum acceleration, range coalescing, and provider-specific short-read/error mappings. No new ADR is needed unless a provider token becomes new persisted authority.

## Q33.3 — Deterministic plan shape

Accept B. Prefer the logical name `BindingReadSourcePlan` to make its scope explicit.

The pure total derivation is:

`captured Binding view + typed requested range + protocol upper bound + captured lifecycle/source/protection facts -> protocol-native empty/invalid result | SAFE_FAILURE before I/O | ordered source intervals`

Each interval contains:

- a nonempty typed range;
- one exact primary source identity;
- optional exact protected fallback identity;
- allowed primary-failure mask;
- source-purity unit;
- validation obligations.

Invariants:

- The admitted request is covered completely, in protocol position order, by non-overlapping intervals.
- Existing protocol-native empty/invalid-range behavior runs before source planning.
- An interior gap is `SAFE_FAILURE`; execution must not query “latest” metadata to fill it.
- An overlap is resolved only by captured accepted authority: active-tail/sealed coverage, selected manifest or compaction generation, and profile preference. Equal-precedence ambiguity or inconsistent identities fail closed before I/O.
- Same inputs yield the same plan and attempt order. Cache presence, provider latency, proof-window state, or first completion never changes it.
- Execution is sequential by declared preference; candidates are never raced.

This is not a new authority and does not require a heap graph. It can be implemented as a cursor over captured primitive tables or borrowed immutable references, with coalescing as an optimization.

OPEN: cursor versus primitive array, maximum interval count, coalescing thresholds, stack/thread-local representation, and allocation/branch targets. No ADR is needed because no persisted/wire authority is added.

## Q33.4 — Fallback and external-observability cut

Accept B, with P4’s narrower table unchanged.

For one source-purity unit:

1. Acquire/use the declared primary under the outer M4 generation lease.
2. If the primary produces an allowed failure, release every partial primary buffer/result and its source-specific pin.
3. Recheck the cached exact attempt/generation/fence and secondary eligibility without recapturing metadata.
4. Acquire the exact declared secondary and retry the whole unit once under the original cumulative byte/time/retry budget.
5. Never transfer back and never loop.

External observability begins when the caller can retain a native result/buffer, an irrevocable transport write is published, or protocol-visible completion/state advancement occurs. Internally owned response assembly is not yet observable. Once any state from the affected purity unit is observable, fallback is forbidden; return/close with the native primary failure rather than splice.

For multi-range responses, observability is scoped to the declared purity/fallback unit:

- A completed earlier disjoint interval does not prohibit the already-planned fallback of a later disjoint interval.
- No affected interval may mix primary and fallback bytes.
- Pulsar P4’s whole inclusive requested range is one unit, so any exposure from that range cuts off fallback for the entire range.

The outer M4 generation slot remains pinned throughout the complete batch, including both attempts, decode, and source-backed buffer drain. Only the inner source-specific pin transfers. P4 performs that transfer itself; M4 must not add another P4 pin layer.

OPEN: numeric budgets, buffer ownership mechanics, and non-P4 source-pin API shape. No ADR is needed; permitting fallback after exposure or widening P4’s error table would require one.

## Q33.5 — Corruption, semantic equivalence, and repair signal

Accept B.

The captured preference owns routing. Fallback legality additionally requires the exact protected source to represent the same declared semantics for that captured range.

- Byte-preserving Kafka materialization may use BK and Object as alternatives when both bind the same immutable RecordBatch identity and logical coverage.
- A selected compaction generation is protocol-semantically authoritative even though physical bytes, batches, CRCs, or indexes may differ. A pre-compaction BK/Object extent is not automatically equivalent and must not reintroduce records removed by the selected generation. Physical retention alone does not make it a readable fallback.
- If two sources claim the same immutable identity but validate to conflicting bytes or semantics, fail closed and quarantine/repair-signal the conflict. Never choose by latency.
- Any preferred-source integrity/format failure is quarantined and repair-signaled even if a valid fallback completes the client read. Failure to record that signal must retain source protection and block retirement; it need not discard already validated fallback bytes.
- Corrupt bytes are never exposed. Integrity validation completes before the purity unit becomes observable.

Error precedence is fixed:

- Primary fails, fallback succeeds: return fallback; record the primary failure diagnostically, and quarantine/repair it when corrupt.
- Both fail: return the primary failure; attach the fallback and cleanup/release failures as suppressed causes.
- Fallback is corrupt: keep the primary as terminal, suppress the fallback error, and quarantine/repair the fallback source too.
- A valid/valid semantic conflict is a safe failure, not `PRIMARY` or `FALLBACK`.

OPEN: quarantine record encoding, repair queue/deduplication, retention backpressure, and provider/native error-to-signal mapping. No ADR is needed unless semantic equivalence or compaction fallback authority is broadened.

## Q33.6 — Proof-window placement

Accept B.

`ProofWindowInput -> ProofWindowOutput`, head scanning, and folding are low-frequency control-plane evidence for source retirement/protection release. They are not ordinary read-plan inputs and must not perform per-read backend selection.

An ordinary read consumes only its already-validated captured generation and protection identity. Missing, stale, or capacity-exhausted proof:

- retains the old source and protection;
- blocks or backpressures retirement/handoff;
- may eventually backpressure admission when bounded liability is exhausted;
- does not redirect an admitted read to another backend.

Vocabulary:

- `ReadPathDecision`: map to either the pure `BindingReadSourcePlan` or terminal `ReadExecutionResult`. Reject a separate persisted/cached route authority.
- `HeadCompactionStats`: retain only as proof-window telemetry/evidence, never as a read input or deletion permission.
- `HeadReclaimDecision`: reject as authoritative vocabulary. If a diagnostic DTO is useful, call it a reclaim-eligibility assessment; actual release remains governed by the accepted proof/protection CAS contract.

OPEN under `V2-OPEN-READ-08`: proof-window/head/fold physical layout, wire encoding, caps, terminal-row retirement, response-loss behavior, and performance thresholds. No new ADR is needed to enforce this placement.

## Q33.7 — Canonical matrices and failure precedence

### 1. Authority matrix

| Profile / protocol / captured lifecycle | Admitted primary | Admitted fallback | Source-purity unit | Controlling authority |
|---|---|---|---|---|
| BK-only / Kafka / any | Exact targeted BK run/entry | None | Kafka atomic append unit | Captured Kafka publication cell and M2 run/index snapshot |
| BK-only / Pulsar / any | Native BK ReadHandle | None | Native requested range | ManagedLedger ledger chain/attempt |
| Async Object / Kafka / unmaterialized or active tail | Exact BK extent | None | Kafka atomic append unit | Captured Source Map and materialized frontier |
| Async Object / Kafka / materialized, exact BK protection live | Materialized Object | Exact protected BK extent | Atomic append unit; any declared fallback interval wholly one source | Captured Source Map, selected Object generation, protection identity |
| Async Object / Kafka / materialized, BK protection released | Materialized Object | None | Atomic append unit | Captured selected Object generation |
| Async Object / Kafka / selected compaction generation | Selected compacted Object generation | No raw/pre-compaction fallback; only an explicitly proven same-semantic generation | Selected compaction range/atomic unit | Captured compaction selection and protocol-semantic coverage |
| Async Object / Pulsar / offload incomplete | BK | None | Whole inclusive native requested range | Native ManagedLedger attempt/offload state |
| Async Object / Pulsar / offload complete and delete state `NONE` | Native configured preference: Object or BK | Other source, under ADR-0036’s asymmetric error table | Whole inclusive native requested range | ManagedLedger-owned P4 composite handle, exact attempt/version and source pins |
| Async Object / Pulsar / delete state `INTENT` or `DONE` | Object | None, even if BK residue exists | Whole inclusive native requested range | Native delete state and P4 handle |
| Object WAL / Kafka / captured active-tail coverage | Exact verified Object-WAL active-tail locator | None unless the captured view explicitly declares an equivalent protected Object source | Kafka atomic append unit | Kafka publication cell plus captured verified locator/Root |
| Object WAL / Kafka / manifest/materialization transition | Captured selected Object generation | Exact semantically equivalent protected Object generation only | Atomic unit or declared whole fallback interval | Binding selector/view, manifest selection, exact protection |
| Object WAL / Kafka / preferred generation only | Selected Object generation | None | Kafka atomic append unit | Captured manifest/source generation |
| Object WAL / Pulsar / captured active-tail coverage | Exact verified Object-WAL locator in virtual ledger chain | None unless an equivalent protected Object source is declared | Pulsar append/entry unit | Pulsar protocol cell/virtual ledger chain plus verified locator |
| Object WAL / Pulsar / manifest/materialization transition | Captured selected Object generation | Exact semantically equivalent protected Object generation only | Atomic unit or declared whole fallback interval | Binding selector/view and virtual-ledger manifest authority |
| Object WAL / Pulsar / preferred generation only | Selected Object generation | None | Pulsar append/entry unit | Captured manifest/source generation |

`OBJECT_WAL` never routes through the Pulsar NPD1/NPO1 P4 authority and never assumes BK exists.

### 2. Execution matrix

`Yes/allowed` means the exact captured plan names the secondary and includes the primary outcome in its immutable allowed-failure mask. `Obs` refers to the affected purity unit.

| Scope | Primary outcome | Fallback declaration | Obs | Fallback outcome | Terminal | Quarantine / repair | Cause rule |
|---|---|---:|---:|---|---|---|---|
| Any | `VALID` | Yes or no | Either | Not run | `PRIMARY` | None | No secondary attempt |
| Any | `MISSING` / `UNAVAILABLE` / `CORRUPT_OR_FORMAT` / `NOT_ELIGIBLE` | No | Either | — | `SAFE_FAILURE` | Primary iff corrupt/format | Primary terminal |
| Any | Same nonvalid set | Yes, class denied | Either | Not run | `SAFE_FAILURE` | Primary iff corrupt/format | Primary terminal |
| Any allowed transfer | `MISSING` / `UNAVAILABLE` / `CORRUPT_OR_FORMAT` | Yes/allowed | Yes | Not run | `SAFE_FAILURE` | Primary iff corrupt/format | Primary terminal; no splice |
| Kafka async or Object-WAL exact equivalent fallback | `MISSING` / `UNAVAILABLE` / `CORRUPT_OR_FORMAT` | Yes/allowed | No | `VALID` | `FALLBACK` | Primary iff corrupt/format | Primary recorded diagnostically |
| Same | Same | Yes/allowed | No | `MISSING` | `SAFE_FAILURE` | Primary iff corrupt/format | Primary terminal; fallback suppressed |
| Same | Same | Yes/allowed | No | `UNAVAILABLE` | `SAFE_FAILURE` | Primary iff corrupt/format | Primary terminal; fallback suppressed |
| Same | Same | Yes/allowed | No | `CORRUPT_OR_FORMAT` | `SAFE_FAILURE` | Every corrupt source | Primary terminal; fallback suppressed |
| Same | Same | Yes/allowed | No | `NOT_ELIGIBLE` | `SAFE_FAILURE` | Primary iff corrupt/format | Primary terminal; fallback suppressed |
| Any current profile | `NOT_ELIGIBLE` | Yes or no | No | Not run | `SAFE_FAILURE` | None unless descriptor itself is corrupt | Fence/admission failure; no generic fallback |
| Pulsar P4, Object primary, `complete + NONE` | Object `MISSING`, timeout/unavailable/short-read, or integrity/format | Yes/allowed | No | `VALID` | `FALLBACK` | Object only for integrity/format | Native P4 transfer to BK |
| Pulsar P4, Object primary, `complete + NONE` | Same | Yes/allowed | No | Any nonvalid outcome | `SAFE_FAILURE` | Each corrupt source | Object primary terminal; BK failure suppressed |
| Pulsar P4, BK primary, `complete + NONE` | Native `BKNoSuchLedgerExists` only | Yes/allowed | No | `VALID` | `FALLBACK` | None unless secondary corrupt | Native P4 transfer to Object |
| Pulsar P4, BK primary, `complete + NONE` | Native `BKNoSuchLedgerExists` only | Yes/allowed | No | Any nonvalid outcome | `SAFE_FAILURE` | Secondary iff corrupt/format | BK primary terminal; Object failure suppressed |
| Pulsar P4, BK primary | Generic BK unavailable/timeout/transient, corruption/format, non-native missing, or `NOT_ELIGIBLE` | Yes but class denied, or no | Either | Not run | `SAFE_FAILURE` | BK repair signal iff integrity/format | Native primary error; no Object fallback |
| Pulsar P4, either allowed primary failure | Yes/allowed | Yes | Not run | — | `SAFE_FAILURE` | Primary iff corrupt/format | External observability forbids transfer |
| Any immutable-identity conflict | Nominally valid but conflicts with another validated representation | Either | No | Conflicting valid representation | `SAFE_FAILURE` | Quarantine/repair conflict | Neither source wins |

There is no proof-window pass/fail dimension because proof-window evidence governs later retirement and protection release, not the route of an already admitted read. Adding it would create a second route authority and ordinary-read control-plane dependency.

The four pasted labels must not become registry IDs or evidence keys. Replace them in normative plans/receipts with canonical ID-prefixed subcases:

- `read-mixed-bk-object` → `V2-READ-001/mixed-disjoint-source-ranges`.
- `read-object-corruption-fallback` → `V2-READ-001/preferred-object-corruption-protected-fallback`; use `V2-BK-008/object-integrity-whole-range-fallback` for the Pulsar P4 regression.
- `read-bk-corruption-object-preferred` is misleading and should be split: `V2-BK-008/bk-first-corruption-does-not-fallback` for P4, or `V2-BK-014/object-preferred-bk-not-selected` for Kafka.
- `read-pin-concurrency` conflates two pin domains; use `V2-BK-010/source-pin-delete-race`, `V2-READ-005/generation-slot-publication-race`, or `V2-READ-007/terminal-drain-aba` as applicable.

The old strings may remain only as local test-method aliases attached to one of those canonical subcases; they carry no status or evidence authority.

No additional ADR is needed for the matrices. Provider mappings, test names, plan layout, numeric budgets, and evidence thresholds remain OPEN implementation/evidence selections.

## Round 33 conclusion

The frontier is closed enough to synchronize as M4 design input. All seven recommendations are accepted as B with the adjustments above: profile/protocol authorities remain distinct, Pulsar P4 is delegated rather than reimplemented, ordinary reads add no remote metadata lookup, the plan is pure and potentially allocation-free, fallback is one-shot and pre-observability, and proof-window evidence never selects the read route. `V2-BK-008/010/014/016` remain their existing `PASSED_CURRENT_SOURCE` M2 contracts; `V2-READ-001/003/004/005/007` remain `PLANNED`. This round establishes no implementation, evidence receipt, scenario promotion, or M4 Final.

<!-- END FIXED REVIEWER RESPONSE -->

## Synchronized accepted cuts

- The shared M4 layer is only a typed logical plan/result, not a universal BK/Object state machine.
- Ordinary reads add no new remote metadata lookup; existing P4 lazy-open validation remains intact.
- Fallback is exact, one-shot, source-pure, budget-preserving, and forbidden after the affected unit is observable.
- P4 retains its existing asymmetric authority, source pins, and PASSED M2 scenario contracts.
- Proof-window results govern later protection release; they never route an already admitted read.
- Scenario authority uses canonical IDs/subcases rather than the informal aliases supplied to this round.

## Still OPEN

The following remain OPEN rather than final contracts:

- adapter dispatch/inlining, source-reader pooling, and allocation-free plan layout;
- cursor versus primitive array, interval cap, coalescing, and branch/allocation targets;
- cached child/open lifetime and provider-proof/checksum acceleration;
- provider error normalization and short-read mapping;
- numeric byte/time/retry budgets and buffer ownership mechanics;
- non-P4 source-specific pin API;
- quarantine record encoding, repair queue/deduplication, and retention backpressure; and
- proof-window/head/fold representation and thresholds under `V2-OPEN-READ-08`.

M4 implementation/evidence selects these within the frozen semantics. This document creates no receipt, scenario PASS,
M4 Final, M5 physical deletion, or new P4 authority.
