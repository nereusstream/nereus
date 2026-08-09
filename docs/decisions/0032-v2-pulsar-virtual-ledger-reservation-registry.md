# ADR 0032: V2 Pulsar virtual-ledger reservation registry

## Status

Accepted for Pulsar `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0027 reserves one numeric interval and requires non-overlapping, never-reused cell slices. The pinned
MetadataStore/Oxia surface provides single-key conditional updates rather than an all-conditions-or-no-writes
transaction across independently allocated cell keys. Per-cell authority records therefore cannot alone prevent two
concurrent administrators from assigning overlapping intervals.

## Decision

0.2 uses one bounded, deployment-wide `PulsarVirtualLedgerNamespaceRegistryRecord` as the only slice-allocation
authority. Its canonical value contains:

- deployment/domain identity and the fixed total reserved interval;
- the digest and operator evidence proving native allocation excludes that interval;
- a monotonically increasing `registryEpoch`;
- a bounded assignment table canonically sorted by `startInclusive`;
- value/evidence integrity facts required for exact reread and audit.

Every assignment retains its cell/slice identity, inclusive bounds, and non-reuse/lifecycle evidence. Before a
single-key CAS, the complete candidate table is validated for total-range containment, canonical order, no overlap,
valid lifecycle transitions, and no reuse of any retired interval. ADR 0054 freezes `k=40`,
`maxRegistryBytes=65,536`, `maxAssignmentsEver=256`, and `maxAssignmentRowBytes=192` for one reservation domain.
Every retired row counts forever.

Per-cell lookup records, caches, and watches are repairable projections only. They may accelerate admission but cannot
allocate a range or overrule the exact registry value/version. An uncertain CAS rereads the one registry key and accepts
only exact candidate equality; a mismatch follows the committed winner or fails the administrative operation without
constructing a merged table locally.

## Consequences

- `V2-OPEN-PUL-OBJ-03` is resolved.
- Allocation updates serialize on one bounded record and cannot scale beyond its explicit capacity.
- The design proves global non-overlap without relying on an unavailable multi-key transaction.
- Slice identity/lifecycle/geometry and fail-closed no-expansion behavior are refined by ADRs 0041/0048/0054.
  Registry/slice/allocator epochs, allocation response loss, and Ledger Chain publication remain downstream gates.
- M1 must prove concurrent assignment, response loss, canonical ordering, overlap/range/reuse rejection, derived-index
  loss/rebuild, stale watch rejection, capacity exhaustion, and native-exclusion evidence drift.

This decision is refined by [ADR 0041](0041-v2-pulsar-virtual-ledger-slice-contract.md),
[ADR 0048](0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md), and
[ADR 0054](0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md), with allocator-mode evidence refined by
[ADR 0055](0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md), with RANGE reservation takeover constrained
by [ADR 0061](0061-v2-pulsar-range-grant-owner-takeover.md); it refines ADR 0027 and is tracked by `T-POSITION-01`,
`V2-POSITION-003..011`.
