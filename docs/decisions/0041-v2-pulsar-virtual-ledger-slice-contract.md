# ADR 0041: V2 Pulsar virtual-ledger slice identity, lifecycle, and geometry

## Status

Accepted for Pulsar `OBJECT_WAL` in 0.2. Implementation and runtime evidence are not started at M0.

## Context

ADR 0032 selects one bounded deployment registry but leaves durable owner identity, retirement, and capacity geometry
open. Binding a slice to a broker, session, display alias, or provider configuration would consume the finite reserved
domain during ordinary operations. Deleting retired assignments or varying interval sizes would complicate the global
never-overlap/never-reuse proof and bounded canonical encoding.

## Decision

Each assignment owner is the stable tuple
`{deploymentId, reservationDomainId, protocol=PULSAR, immutable PulsarProtocolCellId}`. It has an immutable
`sliceAssignmentId`; inclusive bounds are part of that assignment's identity. Broker/process/session, display alias,
provider endpoint/credential, and Cell Provider Scope are runtime/admission attributes and never change slice ownership.
Reusing a display name after retirement requires a new Protocol Cell ID and new assignment.

Slice lifecycle is the irreversible sequence `ACTIVE -> RETIRING -> RETIRED`:

- only ACTIVE may allocate new virtual ledger IDs;
- RETIRING immediately stops new allocation but retains every existing ledger, MessageId, and chain fact;
- RETIRED is a permanent registry tombstone whose assignment and bounds are never deleted or reused.

`EXHAUSTED` is derived from the allocator counter and bounds; it is not a lifecycle state. Broker/session drain does not
retire a slice.

Every 0.2 Cell receives exactly one immutable, equal-size contiguous slice of `2^k` IDs, aligned relative to reserved
base `2^62`. The reserved domain `[2^62, 2^63 - 2]` has cardinality `2^62 - 1`, so
`maxSlicesNumeric = floor((2^62 - 1) / 2^k)` and the top `2^k - 1` IDs remain unallocated. The registry separately
enforces hard `maxRegistryBytes` and lifetime `maxAssignmentsEver`, both counting RETIRED assignments. The deployment
capacity is the minimum of numeric and canonical-encoding limits.

ADR 0054 fixes `k=40`, 65,536 canonical registry bytes, 256 lifetime assignments, and a 192-byte maximum assignment
row as bootstrap contracts. ADR 0048 forbids resize, relocation, extension, and a second slice in 0.2.
RETIRING-to-RETIRED proof and allocator/chain epoch protocols remain downstream gates.

## Consequences

- `V2-OPEN-PUL-OBJ-04`, `V2-OPEN-PUL-OBJ-05`, and `V2-OPEN-PUL-OBJ-06` are resolved.
- Fixed equal slices waste some numeric space and the registry grows for the deployment lifetime, buying simple aligned
  overlap audit and durable never-reuse proof.
- Normal restart, scale, and provider rotation do not consume another slice.
- M1/M3 must prove stable Cell ownership, illegal lifecycle transitions, allocation stop at RETIRING/exhaustion,
  permanent tombstones, alignment/math, numeric/encoded capacity, and provider-configuration independence.

This decision is refined by ADRs 0048/0054/0055/0061, refines ADRs 0027/0032, and is tracked by `T-POSITION-01`,
`V2-POSITION-003..011`.
