---
productLine: V2
designStatus: AcceptedInputUnchanged
implementationStatus: InProgress
evidenceStatus: FocusedTestsOnly
authority: CurrentImplementationRecord
sourceTuple: current-m4-source
---

# M4 implementation and evidence log

This living record describes current implementation descendants of the immutable M4-A/B/C/D and Grill 32-35 hard
freeze. It does not amend those files, close an OPEN evidence gate, publish a child receipt, promote a scenario, or
grant M5 physical-deletion authority.

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
