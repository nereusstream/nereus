---
productLine: V2
designStatus: AcceptedInputUnchanged
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: CurrentImplementationRecord
sourceTuple: current-m4-source
receipt: docs/v2/evidence/v2-m4/final/final-source-595c8b34779d1e88187eb0084bf18e65ab2dd742/m4-final.json
---

# M4 implementation and evidence log

This record describes the completed implementation descendants of the immutable M4-A/B/C/D and Grill 32-35 hard
freeze. It does not amend those files or grant M5 physical-deletion, M6 process-activation, M8 native-parity, or
production-deployment authority.

## Milestone 1: read-view hazard and source-plan kernel

The first implementation slice selects the following candidate family:

- one immutable-reference `BindingReadAuthorityV1` and generation-tagged `BindingReadPublicationCellV1`;
- a bounded cross-Binding `BindingReadHazardPoolV1` with one 64-bit exact lease word per slot, a lease-associated
  payload publication marker, `VarHandle.fullFence()`, exact authority-reference revalidation, stable
  lease/payload/lease scans, and per-slot reuse generations;
- caller-owned reusable `BindingReadBatchContextV1` and `BindingReadPlanBufferV1`, so the stable capture/plan/clear
  sequence allocates zero heap bytes after warm-up;
- the owner/event-loop serialized source-lifetime family, with plain batch accounting and no source-use RMW; and
- deterministic position-ordered route derivation plus one sequential fallback only for a captured exact semantic
  equivalent, an admitted closed failure class, proven primary cleanup, and a pre-observability purity unit.

The focused suite currently covers generation pin/drain, admission close and pool exhaustion, exact successor capture,
route gaps/capacity, one-shot fallback, observability, quarantine, all-or-release multi-Binding reservation,
cross-thread lifecycle rejection, semantic mismatch, and zero current-thread allocated bytes across 100,000 warmed
capture/plan/clear iterations.

This is implementation progress, not formal evidence. Exact pool/plan/capture retry caps remain candidate values until
the `READ_VIEW_HAZARD` and `CURRENT_SOURCE_INTEGRATION_PERFORMANCE` children bind measurements at the final tested
source. `V2-OPEN-READ-08/09` remain open, every M4 scenario remains `PLANNED`, and no protection release is active.

After implementation begins, `v2M4FrozenDesignInputsCheck` verifies the immutable manifest bytes, preserved Grill
records, M4-D ownership literals, and historical M3 dependency without repeating the pre-start-only
`DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED` claim. The original `v2M4DesignCheck` remains the freeze-commit gate.

## Milestone 2: durable selector and quiescence-control candidate

The second implementation slice adds a fail-closed canonical M4 control-plane candidate below the existing exact-byte
`CanonicalControlMetadataStore` and expands the source-locked Oxia adapter's closed key grammar only for those M4
families. It selects:

- one inline canonical `BindingReadSelector` capped at 32 KiB, seven ordinarily consumable pending-anchor/batch slots,
  an eighth emergency slot, and a dedicated 2 KiB `STOPPED` byte reserve;
- one exact selector CAS for takeover, fallback introduction, membership-neutral view publication, the fused
  `PWF E -> PO E+1` closure/anchor/batch transition, bounded anchor pruning, and fresh-epoch recovery from `STOPPED`;
- immutable capability generations that bind backend admission/configuration, adapter, admission contract, verifier,
  receipt identity/SHA, time semantics, and numeric lifetime/skew/grace bounds; revocation preserves the evidence
  digest and fails later verification closed;
- create-only terminal/proof rows, typed planned-drain versus qualified-expiry facts, deterministic proof identities,
  a 64-entry live proof window, 32-entry contiguous folds, 64-fold cap, and a 4,096-epoch interval bound; and
- exact-generation source protection with per-source `[first_i, sharedLast]` verification, current hazard-scan drain,
  capability-qualified historical proof revalidation, irreversible release CAS, and exact reread convergence after
  response loss.

The deterministic suites cover all canonical record round trips, non-canonical rejection, all selector transition
families, stale closure conflict, applied-but-unknown convergence, emergency stop/resume, valid terminal substitution,
invalid-occupant quarantine, proof gaps/folding, historical capability changes and revocation, local hazard retention,
and release response loss. The Oxia test additionally proves the exact M4 key allowlist and rejects neighboring or
malformed key families.

This remains a candidate implementation, not activation or formal evidence. The frozen `V2-OPEN-READ-08/09` gates are
not closed until the final evidence run binds these exact encodings, limits, tests, and measurements to one tested
source. M4 still performs no physical deletion; M5 batch compaction and provider GC remain absent.

## Milestone 3: typed protocol planning and local selector coupling

The third implementation slice closes two implementation-level gaps without changing the frozen authority:

- Pulsar planning now preserves `(virtualLedgerId, entryId)` as a typed two-coordinate position. It never flattens a
  ledger and entry into the Kafka `long` offset domain, and it fails closed on gaps, overlap, range, or caller-owned
  capacity limits.
- Source attempt identities are caller-selected and monotonically increasing across every interval in one batch.
  Successful nonterminal intervals leave the source-use gate open, and observability is scoped to the affected
  source-purity unit, so an earlier completed interval cannot incorrectly forbid an already-planned fallback for a
  later disjoint interval.
- `BindingReadSelectorRuntimeV1` closes owner-local admission before dispatching the fused durable fallback closure.
  Unknown or conflicting outcomes keep the old epoch locally closed; only an exact durable successor can reopen the
  successor epoch. An already-captured predecessor generation remains pinned and usable under its frozen rules.
- Planned pool close rejects new capture but never clears a live lease. Deterministic tests also select permanent slot
  retirement at lease-word wrap and require `INCONCLUSIVE` while a claimed lease has not yet published a stable
  payload.

Focused tests cover a near-`Long.MAX_VALUE` Pulsar ledger, typed range gaps and plan capacity, multi-interval fallback,
local close-before-unknown-response ordering, old-generation survival, pool close, lease wrap, and the exact
lease/payload publication race. These are still focused implementation results: current-source Kafka/Pulsar adapters,
formal child receipts, final measurements, OPEN-gate synchronization, scenario promotion, and M4 Final remain ahead.

## Milestone 4: current-source Object-WAL read integration

The fourth implementation slice connects the M4 cell and lifetime rules to the actual M3/P4 Object-WAL structures:

- `BindingReadAsyncExecutorV1` serializes capture, cancellation-gate closure, provider completion, exact lease clear,
  and post-drain retirement reconciliation on one owner event loop. Cancellation never equates a cancel request with
  provider termination and cannot force-clear a lease.
- Kafka publishes routes only from an exact `KafkaObjectCoherentProtocolSnapshotV1`, verifies the root-bound active
  tail digest, retains the actual `KafkaObjectExtentLocatorV1`, takes the existing M3 source-protection pin before
  provider use, and holds both inner pin and outer generation lease through validated range completion.
- Pulsar publishes typed routes from an immutable bridge read view, retains the exact manifest source or active
  locator, and calls `readCaptured` so a concurrent manifest handoff cannot silently replan an accepted read. The
  existing P4 inner pin remains the source-specific lifetime authority.
- Both integrations register a conservative Binding-wide M4 hazard guard around locator retirement. This closes the
  capture-to-inner-pin race: a visible outer lease or an inconclusive slot retains the locator, then a post-terminal
  reconciliation releases manifest-covered locators only after the exact outer lease has drained.

The fresh affected regressions exercise the shared M3 coherent root and active-tail digest, Kafka locator
publication/readability, provider-lifetime pinning and root-CAS retirement, plus Pulsar P4 active-to-manifest switching,
old captured-source survival, and the outer-hazard/inner-pin admission race. Full Kafka and Pulsar module suites pass.
This is not yet a child receipt or scenario promotion; formal source-bound evidence and the remaining OPEN selections
still follow.

## Milestone 5: exact-source evidence contract and physical selection candidate

The fifth implementation slice makes the frozen four-child hierarchy executable without changing its ownership:

- `READ_VIEW_HAZARD` and `SOURCE_PLAN_EXECUTION` use disjoint method-level selectors over the M4 kernel suite;
- `QUIESCENCE_PROTECTION_RELEASE` composes the control coordinator with the exact-key Oxia adapter suite;
- `CURRENT_SOURCE_INTEGRATION_PERFORMANCE` owns only the fresh Kafka/Pulsar Object-WAL integration regressions;
- every child contains governed raw JUnit XML plus closed typed facts, is individually non-promotable, and binds one
  exact tested Nereus commit and that commit's `source-locks.json` SHA-256; and
- the Final validator reopens all child/attachment bytes, verifies the immutable M3 identity, permits only a linear
  evidence-only descendant chain, promotes exactly five M4-only scenarios, and preserves all shared rows as
  `PLANNED` with null receipts; and
- historical M3 replay binds the exact frozen Final SHA and closure-commit blob, reopens every repository-owned child
  and attachment, and recomputes the V5 allocator cross-links from its sealed byte/SHA inventory. It does not require
  the original 124.9-MB payload at workstation-specific absolute paths and does not recertify M3.

The tested candidate parameters are source-locked as 32 KiB selector bytes, 2 KiB dedicated STOPPED reserve, eight
anchors, eight active batches, 64 sources per batch, a 64-entry proof window, 32-entry contiguous folds, 64 folds,
and at most 4,096 epochs per verified interval. Kafka/BookKeeper Object-WAL, the exact Pulsar source composite, and the
locked Oxia client adapter are admitted only for `DURABLE_DRAIN_ONLY_V1`; `AUTHORITY_EXPIRY_V1` remains non-admitted
despite codec/verifier coverage. None of these admissions grants production deployment, M5 physical deletion, M6, or
M8 authority.

This milestone installs the formal runner/publishers and negative contract tests. The values do not close
`V2-OPEN-READ-08/09`, create a child receipt, or promote a scenario until a clean immutable tested source executes the
formal run and publishes a validated Final.

## Milestone 6: formal exact-source closure

Exact tested source `595c8b34779d1e88187eb0084bf18e65ab2dd742` executed the four mutually exclusive evidence
children against source-lock SHA-256 `02601b3de76857d5f0c8657b285bc91584486d08077e201a4d9fd34f377b07a2`.
The child totals are 13, 6, 19, and 5 tests with zero failure, error, or skip.

The governed measurements include zero allocated bytes across 100,000 warmed capture/plan/clear iterations, 84-ns
hot-path p99, 80,000 concurrent hazard operations at 16,375-ns p99, proof capacity through 2,112 admitted epochs with
selector stop on attempt 2,113, and a 32-row cleanup plan blocked by all six reference classes. Current-source
end-to-end p99 is 183,125 ns for Kafka and 135,000 ns for Pulsar; measured caller-plus-owner allocation is 2,045 and
9,776 bytes per operation respectively.

The current immutable
[M4 Final](../../evidence/v2-m4/final/final-source-595c8b34779d1e88187eb0084bf18e65ab2dd742/m4-final.json) has
SHA-256 `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07`. It closes
`V2-OPEN-READ-08/09` and promotes exactly `V2-READ-001/003/004/005/007`. `V2-READ-002` and shared
`V2-READ-006/008..015` remain `PLANNED` with null receipts. M5 physical deletion remains outside M4.
