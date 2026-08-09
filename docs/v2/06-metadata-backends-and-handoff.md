---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Metadata backends and handoff

## Capability boundaries

V2 does not expose one broad metadata API whose operations must map identically to KRaft and MetadataStore/Oxia.
Shared contracts are separated into:

- `TopicProtocolBindingStore`;
- `StorageEpochStore`;
- `TopicBindingAggregatePublisher`;
- `OwnershipAuthority`;
- `ManifestPublisher`;
- `TypedLogicalTrimStore`;
- `BackgroundWorkCoordinator`.

Conformance suites verify fencing, monotonic roots, idempotency, response-loss recovery, and bounded enumeration. They
do not require both backends to implement the same ephemeral lease primitive.

`TopicBindingAggregatePublisher` writes one immutable `TopicBindingAggregateRecord`. Kafka adds it to the atomic
`CreateTopics` result; MetadataStore/Oxia creates one key and resolves response loss by exact reread equality. Binding
and epoch stores are projections rather than independent authorities. This is create/open control-plane work, not
normal append.

ADR 0016 excludes Access Projection and Migration Link runtime from 0.2. The M1 model rejects a second Native Write
Authority but does not expose `ProjectionMapStore`. A future accepted runtime must keep its map and authority-transfer
contracts as separate capabilities and may not create a per-append cross-protocol metadata dependency.

## Kafka backend

Kafka uses KRaft as the durable authority for Topic Protocol Binding, Storage Epoch roots, partition ownership
projection, low-frequency manifest roots, and typed logical trim required by the Kafka runtime. Controller records must
be versioned and replay-deterministic.

High-churn materialization heartbeats, cache state, and per-append data do not belong in the KRaft log. Background work
uses deterministic assignment from durable roots or a separately bounded coordinator whose loss only delays work.
When a coordinator or executor serves multiple Protocol Cells, assignment roots, queues, quotas, fencing, and task
authority remain cell-scoped. Shared capacity never creates a cross-cell publication or deletion authority.

## Pulsar backend

Pulsar uses MetadataStore/Oxia for Nereus-owned Topic Protocol Binding, Storage Epoch, virtual-ledger identity/chain,
and lifecycle roots while retaining native ManagedLedger, cursor, and broker ownership semantics. A Nereus record
cannot overrule stock Pulsar metadata that still authorizes a ledger, cursor, transaction, or offload source. For Pulsar
`BOOKKEEPER_WAL_ASYNC_OBJECT`, native ManagedLedger ledger/offload metadata is the sole offload/lifecycle authority; any
Nereus manifest is derived.

A deployment-level Virtual Ledger Reservation registry assigns non-overlapping, never-reused cell slices from
`[2^62, 2^63 - 2]` and fences native allocation out of the entire interval. The cell's CAS allocator operates only
inside its current slice; missing, overlapping, drifted, or revoked reservation state blocks Object-WAL admission and
allocation. Reservation checks are low-frequency control-plane work, not normal append metadata I/O.

## Ownership token

Every admitted writer carries a token binding Protocol Cell, Topic Protocol Binding, Topic Incarnation, Storage Epoch,
Owner Epoch, backend version, and expiry/lease proof where applicable. Acquisition and renewal are control-plane
operations outside normal append.

A stale token fails before new protocol-position allocation. Any in-flight completion revalidates Storage Epoch and
Owner Epoch before advancing typed Durable/Readable Frontiers.

## Planned fast handoff

The old owner may seal admission and publish a bounded hint containing:

- Protocol Cell, Topic Protocol Binding, Topic Incarnation, and Position Domain version;
- Storage Epoch;
- source and target owner epochs;
- typed Durable/Readable Frontiers;
- active Physical Extent/run/ledger identity;
- manifest root/version;
- checksum and expiry.

The target validates every field against current authority. A missing, expired, duplicated, or mismatched hint is
ignored and recovery falls back to durable WAL and manifest roots. Consuming a hint must be idempotent; deleting the hint
is cleanup, not correctness.

## Metadata hot-path metric

For admitted normal append, both remote metadata read and mutation counters must remain zero. Ownership renewal,
topic-open, rollover publication, trim, and background lifecycle work are separately labeled and budgeted so they cannot
hide in an aggregate append metric.

Relevant tradeoffs: `T-META-01`, `T-HANDOFF-01`, and `T-FABRIC-01`. Required scenarios: `V2-META-001`,
`V2-META-002`, `V2-HO-001`, `V2-FABRIC-001`, and `V2-POSITION-002..003`.
