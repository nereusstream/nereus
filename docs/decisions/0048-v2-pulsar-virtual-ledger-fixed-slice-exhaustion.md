# ADR 0048: V2 Pulsar virtual-ledger fixed-slice exhaustion

## Status

Accepted for Pulsar `OBJECT_WAL` in 0.2. Implementation and runtime evidence are not started at M0.

## Context

ADR 0041 assigns one immutable aligned `2^k` slice to each Pulsar Protocol Cell but leaves resize and additional slices
open. Resize, relocation, in-place extension, or attaching a second interval would introduce multi-slice allocation,
ordering, takeover, and recovery semantics that are not part of the current never-overlap/never-reuse proof.

## Decision

0.2 forbids slice resize, relocation, in-place extension, and second-slice attachment. A Pulsar Protocol Cell owns
exactly one slice for its lifetime; `sliceAssignmentId` and inclusive bounds never change.

When the allocator reaches the slice end, allocation fails closed before assigning another virtual ledger ID. It does
not wrap, search another interval, borrow from a retired Cell, or mutate registry geometry.

Additional capacity inside the current reservation domain requires a new immutable Pulsar Protocol Cell ID and unused
registry assignment. After that domain's bounded lifetime registry is exhausted, a new logical name is insufficient:
ADR 0054 requires either a new reservation domain with bootstrap-proven non-overlapping ledger-ID namespace or an
independent deployment/cluster. Moving an existing Topic Incarnation or ManagedLedger requires a future explicit
migration contract and is not implied by creating either one.

## Consequences

- `V2-OPEN-PUL-OBJ-07` is resolved.
- Conservative capacity sizing and an unavailable exhausted Cell are accepted for one interval, one allocator, and one
  simple never-reuse proof.
- 0.2 has no hidden multi-slice chain ordering, recovery, or ownership-transfer semantics.
- Exact geometry is refined by ADR 0054. Allocator wire/mode, allocation response-loss recovery, ledger-head
  publication, rollover, takeover, and `RETIRING -> RETIRED` proof remain downstream gates.
- M1/M3 must prove rejection of every geometry mutation, fail-closed boundary allocation, new-Cell independence, and
  absence of automatic topic/ledger migration.

This decision is refined by ADRs 0054/0055, refines ADRs 0022, 0027, 0032, and 0041, and is tracked by
`T-POSITION-01`, `V2-POSITION-003..018`.
