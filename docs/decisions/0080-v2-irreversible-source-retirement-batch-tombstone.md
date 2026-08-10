# ADR 0080: V2 irreversible Source Retirement Batch tombstone

## Status

Accepted for the 0.2 `OBJECT_WAL` same-key `FULL_V1 -> RETIRED_V1` Source Retirement Batch lifecycle, exact-version
replacement prerequisites, lost-response convergence, permanent compact-tombstone retention, capacity admission, and
strict separation from source protection and physical GC. Tombstone deletion and any retired-through/frontier
authority are not accepted for 0.2 and remain evidence-blocked under `V2-OPEN-READ-15`; implementation has not started
at M0.

## Context

ADR 0078 makes batch completion a derived fact after every exact member protection and reference retires. Keeping every
full member row forever is unnecessary, but deleting all evidence would make a delayed create, response-loss recovery,
or stale residue indistinguishable from a never-created batch. A compact same-key tombstone preserves that distinction
without introducing another source-lifecycle authority.

## Decision

`SourceRetirementBatchId` is the immutable logical identity. Its storage value has one irreversible closed lifecycle:

```text
FULL_V1
  -- exact-version CAS -->
RETIRED_V1
```

This means immutable logical identity plus irreversible storage lifecycle; it does not claim that the physical bytes
at the key never change. `RETIRED_V1 -> FULL_V1`, replacement with a different BatchId/full-batch identity, delete by
age, and key reuse are forbidden.

The exact-version replacement is eligible only after a bounded authoritative scan proves every member protection is
`RELEASED` or otherwise proven retired and every selector, lineage, recovery, and response-loss reference to the full
record has disappeared. A quarantined, missing, unknown, or mismatched member/reference retains `FULL_V1`. The compact
tombstone binds at least the same `SourceRetirementBatchId` and `fullBatchSha`; exact canonical wire fields and hard
caps remain M4/M5 evidence work.

Replacement response loss converges as follows:

- exact `RETIRED_V1` with matching BatchId and `fullBatchSha` means the full batch was committed and later retired;
- recovery or a delayed create must adopt that fact and must not reconstruct `FULL_V1` or treat it as an ordinary
  create conflict;
- a mismatched BatchId or `fullBatchSha`, an invalid state/tag, or a non-canonical value fails closed; and
- exact `FULL_V1` means replacement did not become visible or remains unresolved under the exact-version protocol.

`RETIRED_V1` proves only that full batch metadata was safely compressed after all replacement prerequisites passed. It
does not set a source protection to `RELEASED`, fill a quiescence-proof gap, release a read pin, authorize Object or
BookKeeper deletion, or prove physical GC eligibility. Those authorities remain independent and are never inferred
from the tombstone.

0.2 retains every valid compact tombstone. No tombstone-delete retired-through frontier, ordered activation scan, or
absence-based stale-create rule is admitted. Binding/Cell lifetime tombstone count and bytes are
hard admission budgets. Reaching either limit stops new fallback/handoff admission and retains existing metadata; age,
cache absence, local scan results, Topic policy, and operational pressure cannot delete a tombstone or enlarge the
budget.

Tombstone deletion may be reconsidered only if M4/M5 proves an unacceptable long-lived capacity cost and a concrete
backend proves a gap-free authoritative activation history, monotonic conditional authority, incarnation/selector-
lineage binding, never-reactivation, stale-create behavior, and recovery convergence. Any future accepted mechanism
must be named and treated as metadata-deletion authority only; it can never participate in protection release or
source GC.

## Consequences

- The compact transition is fixed without creating a mutable batch-progress row or a metadata-absence authority.
- Delayed creates and unknown replacement responses converge against a permanent same-key fact. The conservative cost
  is lifetime compact-tombstone storage and admission backpressure at the hard cap.
- `V2-OPEN-READ-15` remains evidence-blocked only for possible tombstone deletion; it no longer leaves the 0.2
  `FULL_V1 -> RETIRED_V1` transition undecided.
- M4/M5 must test every replacement prerequisite and exact-version race, both response outcomes, delayed create,
  mismatched identities, irreversible state rejection, permanent retention and cap exhaustion, and that the tombstone
  grants neither protection release nor physical GC.

This decision refines ADR 0078 and is tracked by `T-MANIFEST-01`, `T-HANDOFF-01`, `V2-READ-006/013/015`, and
`V2-OPEN-READ-15`.
