# ADR 0032: V2 Pulsar virtual-ledger reservation registry

## Status

Accepted for Pulsar `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0027 reserves one numeric interval and requires non-overlapping, never-reused cell slices. The pinned
MetadataStore/Oxia surface provides single-key conditional updates rather than an all-conditions-or-no-writes
transaction across independently allocated cell keys. Per-cell authority records therefore cannot alone prevent two
concurrent administrators from assigning overlapping intervals.

## Decision

0.2 uses one bounded `PulsarVirtualLedgerNamespaceRegistryRecord` as the only slice-allocation authority for one
immutable `ledgerIdCompatibilityNamespaceId`, the actual ledger-ID space shared by native, BookKeeper, and custom
writers. In 0.2 that 32-byte identity is a domain-separated SHA-256 derivation of the exact BookKeeper ledger root's
native `INSTANCEID`; the exact hash separator/framing remains a pre-implementation wire descendant. A format-created
new INSTANCEID changes the derived identity but does not prove an empty namespace. M1 admits only a genuinely fresh
ledger root that is authoritatively absent immediately before initialization: either never created or removed by a
qualified non-force nuke bound to the expected old identity, with every old writer/admin principal fenced and an exact
absent-root postcondition. Format, missing/recreated INSTANCEID, force nuke, or an unqualified direct nuke cannot
manufacture fresh-bootstrap evidence.

The namespace may have zero Registry before V2 admission; while V2 allocation is admitted it has exactly one selected
reservation Registry, and a second Registry in that namespace is forbidden. No second independently surviving Nereus
namespace marker exists. Its key and canonical value both bind and cross-check compatibility-namespace, deployment,
and reservation-domain identity. The value contains:

- complete key identity and the fixed total reserved interval;
- one bounded inline canonical writer set; 0.2 has no external writer-set snapshot/reference;
- typed native-exclusion/admission evidence rather than a bare digest;
- a monotonically increasing `registryEpoch` that advances by exactly one on every successful mutation;
- a bounded assignment table canonically sorted by `startInclusive`;
- value/evidence integrity facts required for exact reread and audit.

Every assignment row directly retains cell/slice identity, inclusive bounds, and lifecycle; M1 does not add a future
retirement-proof wire to that row. Before a single-key CAS, the complete candidate table is validated for total-range
containment, canonical order, no overlap,
valid lifecycle transitions, and no reuse of any retired interval. ADR 0054 freezes `k=40`,
`maxRegistryBytes=65,536`, `maxAssignmentsEver=256`, and `maxAssignmentRowBytes=192` for the one selected Registry in
that compatibility namespace. Every retired row counts forever.

Per-cell lookup records, caches, and watches are versioned derived slice views only. They may accelerate allocator
admission but cannot allocate a range or overrule the exact registry value/version. Rollover never rereads or copies the
64-KiB Registry. An uncertain CAS rereads the one Registry key and converges only through ADR 0082's closed exact result;
the client never constructs a merged table locally.

Native exclusion is deployment admission evidence for every writer that shares the compatibility namespace. Each row
contains only writer kind/entry identity, allocator/exclusion contract version, independently revocable principal
generation/digest, interlock-policy generation/digest, and a typed conformance-evidence reference. Source commit and
artifact SHA remain receipt facts rather than long-lived Registry identity. Writer count and row bytes have independent
hard caps in addition to the total 64-KiB value cap; their numbers remain open before M1 implementation. The namespace
hash excludes root URI/path, deployment/reservation-domain identity, and source/artifact SHA; copying an INSTANCEID is
therefore conservatively the same compatibility namespace. Exact hash framing and INSTANCEID grammar remain open.

ACL, credential, or an equivalent admission interlock prevents every writer outside the selected commitment from
allocating IDs. Shared credentials are insufficient. First activation establishes exclusive ACL/admin interlock,
proves/initializes the fresh root, rereads and derives the exact INSTANCEID, upgrades all writers, revokes the
unrestricted legacy principal, proves negative allocation, and activates the complete Registry last. After activation,
format/nuke/INSTANCEID/root mutation is forbidden; missing or changed identity fences Registry admission and every
derived view. A new writer is committed before start; removal fences/drains and independently revokes it before
changing the commitment; rolling upgrade may contain both old and new writer entries. Patching one
`PulsarLedgerIdGenerator` is necessary but cannot prove completeness. Missing/drifted membership or interlock evidence
blocks activation. The old V1 global allocator is removed or isolated and cannot be renamed into this authority.

`PulsarVirtualLedgerNamespaceRegistryStore` is a dedicated production capability. Its Registry correctness evidence is
`REGISTRY_CONFORMANCE`; an allocator `HARNESS_CONFORMANCE_ONLY` receipt cannot substitute for it.

## Consequences

- `V2-OPEN-PUL-OBJ-03` is resolved.
- Allocation updates serialize on one bounded record and cannot scale beyond its explicit capacity.
- The design proves global non-overlap without relying on an unavailable multi-key transaction.
- Slice identity/lifecycle/geometry and fail-closed no-expansion behavior are refined by ADRs 0041/0048/0054.
  Registry/slice/allocator epochs, allocation response loss, and Ledger Chain publication remain downstream gates.
- M1 must prove INSTANCEID-derived compatibility identity and truly fresh root admission; refusal of format/missing-ID/
  force-nuke shortcuts; key/value identity; one-Registry admission; inline canonical writer completeness and typed
  references; independently revocable unauthorized-writer interlock; add/remove/rolling-upgrade cuts; epoch +1;
  concurrent assignment, response loss, canonical ordering, overlap/range/reuse rejection, derived-view loss/rebuild/
  staleness, capacity
  exhaustion, separate Registry receipt, and absence of the V1 allocator authority.

This decision is refined by [ADR 0041](0041-v2-pulsar-virtual-ledger-slice-contract.md),
[ADR 0048](0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md), and
[ADR 0054](0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md), with allocator-mode evidence refined by
[ADR 0055](0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md), with RANGE reservation takeover constrained
by [ADR 0061](0061-v2-pulsar-range-grant-owner-takeover.md) and M1 control authority by
[ADRs 0082](0082-v2-m1-domain-and-control-authority-contracts.md) and
[0083](0083-v2-m1-wire-control-and-evidence-bounds.md); it refines ADR 0027 and is tracked by `T-POSITION-01`,
`V2-POSITION-003..018`.
