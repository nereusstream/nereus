# ADR 0032: V2 Pulsar virtual-ledger reservation registry

## Status

Accepted for Pulsar `OBJECT_WAL`. The 2026-08-12 M1.1c-R0 deterministic readiness evidence closes the writer-count
and canonical-capacity inputs for R1. The R1 production Registry authority, real Oxia conformance, allocator selection,
and runtime evidence remain pending.

## Context

ADR 0027 reserves one numeric interval and requires non-overlapping, never-reused cell slices. The pinned
MetadataStore/Oxia surface provides single-key conditional updates rather than an all-conditions-or-no-writes
transaction across independently allocated cell keys. Per-cell authority records therefore cannot alone prevent two
concurrent administrators from assigning overlapping intervals.

## Decision

0.2 uses one bounded `PulsarVirtualLedgerNamespaceRegistryRecord` as the only slice-allocation authority for one
immutable `ledgerIdCompatibilityNamespaceId`, the actual ledger-ID space shared by native, BookKeeper, and custom
writers. In 0.2 that 32-byte identity is:

```text
SHA-256(NLI1 || u32be(36) || canonicalInstanceIdAscii[36])
```

The admitted fresh-only `INSTANCEID` is exactly 36 ASCII bytes, byte-for-byte equal to lowercase canonical UUID
parse/render output, and non-zero. Whitespace, uppercase, alternative forms, NUL, and trailing bytes are rejected; UUID
version 4 is not required. The Registry retains the exact bytes and derived ID and verifies both. A format-created
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

Native exclusion is deployment admission evidence for every writer that shares the compatibility namespace. The
closed writer kinds are `NATIVE_BOOKKEEPER_LEDGER_ID=1` and `NEREUS_VIRTUAL_LEDGER_ID=2`; there is no generic third
kind. A native row denotes one source-qualified writer cohort under one independently revocable principal generation,
not one process. External/custom writers, shared unrestricted credentials, or writers outside the admitted generator
fail admission.

There is no random `writerEntryId`. A row is identified by
`{writerKind, exclusionContractVersion, principalGeneration, principalSha256}` and has exactly 120 bytes:

```text
u16 writerKind
u16 exclusionContractVersion
u64 principalGeneration
principalSha256[32]
u64 interlockGeneration
interlockSha256[32]
u16 evidenceKind
u16 evidenceVersion
admissionEvidenceSha256[32]
```

Generations are positive, digests are non-zero, codes are closed, and rows sort by writer kind, principal generation,
then principal digest. Duplicate identities or one principal reused across writer kinds are illegal. A derived
`SHA-256(NWR1 || canonicalWriterRowBytes)` may identify a row in evidence but is not persisted back into it. Lifecycle
remains a Registry-predecessor/evidence fact rather than a mutable row field. Source commit and artifact SHA remain
receipt facts rather than long-lived Registry identity.

`RegistryAdmissionEvidenceV1` is a bounded create-only immutable content-addressed proof record, not allocation
authority. It binds the exact INSTANCEID/namespace, candidate predecessor/epoch and writer set, fresh-root proof,
ACL/principal/interlock generations, negative-allocation proof, and source-qualified writer evidence. The Registry
binds only its closed kind/version/SHA; a row reference must resolve its exact cohort section even if the whole Registry
and all rows share one evidence bundle. Allocators and normal rollover never read the bundle. The Registry conformance
receipt binds both final Registry and evidence bytes without writing itself back into either record.

`writerRowBytes=120` and `maxWriterCount=14` are fixed. The count is two closed writer kinds times seven bounded
source-qualified/principal-generation cohorts per kind: the full old/new-binary by old/new-credential matrix, one
fresh-principal rollback cohort, one fenced-but-not-cleaned residue, and at most one allocation-capable bootstrap/admin
cohort. A control-only admin that cannot allocate ledger IDs is interlock evidence rather than a row. The exact
capacity formula is `184 + writerCount * 120 + sum(assignmentRowCanonicalBytes)`, where at most 256 full row
contributions of at most 192 bytes are admitted. Therefore the largest legal v1 canonical Registry/Oxia value is
51,016 bytes and the unchanged 65,536-byte envelope retains 14,520 bytes of reserved compatibility margin. That margin
cannot admit a fifteenth writer, a hidden field, or a larger assignment row. The stable first rejection is
`REGISTRY_WRITER_COUNT_EXCEEDED` for row 15 and `REGISTRY_CANONICAL_BYTES_EXCEEDED` for byte 51,017; assignment count
and row-size errors retain their more specific precedence. There is no separate writer-set-byte cap. The namespace hash
excludes root URI/path, deployment/reservation-domain identity, and source/artifact SHA; copying an INSTANCEID is
therefore conservatively the same compatibility namespace.

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
[0083](0083-v2-m1-wire-control-and-evidence-bounds.md), and
[0084](0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md), and
[0085](0085-v2-m1-foundation-start-and-deferred-codec-bounds.md); it refines ADR 0027 and is tracked by `T-POSITION-01`,
`V2-POSITION-003..018`.
