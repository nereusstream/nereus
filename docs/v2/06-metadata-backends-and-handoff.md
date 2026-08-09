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

The aggregate key is protocol/incarnation-scoped: native Kafka topic UUID or Pulsar canonical persistence-name digest
plus generation. Its value repeats the complete discriminated identity, and binding/initial-epoch IDs are deterministic
domain-separated SHA-256 derivations. Name-only keys, random IDs, time, log offsets, and backend versions cannot define
durable aggregate identity.

Logical `TopicBindingAggregateV1` has one whole-record schema version and a closed immutable binding plus ordinal-zero
epoch payload. Backend envelopes/records map through one validator: Oxia uses schema/min-reader 1, while Kafka uses
controller-record wire v0 for the same logical v1. Mutable lifecycle/owner/time/attempt/backend fields are excluded.

ADR 0016 excludes Access Projection and Migration Link runtime from 0.2. The M1 model rejects a second Native Write
Authority but does not expose `ProjectionMapStore`. A future accepted runtime must keep its map and authority-transfer
contracts as separate capabilities and may not create a per-append cross-protocol metadata dependency.

## Kafka backend

Kafka uses KRaft as the durable authority for Topic Protocol Binding, Storage Epoch roots, partition ownership
projection, low-frequency manifest roots, and typed logical trim required by the Kafka runtime. Controller records must
be versioned and replay-deterministic.

Kafka activates V2 only when a fresh storage format/bootstrap finalizes `nereus.storage.version=2`. A V2 node supports
only `[2,2]`; level 1 remains V1 and is rejected. Generic runtime 0/1-to-2 updates and 2-to-0/1 downgrades are forbidden.
At level 2, a successful native CreateTopics item publishes the aggregate in the same atomic result; validate-only and
native failed items publish nothing.

Kafka represents the aggregate as a generated typed wire-v0 `TopicBindingAggregateRecord` owned directly by
`TopicImage`. Completed snapshots write `TopicRecord -> TopicBindingAggregateRecord -> PartitionRecord*` for each
topic, and `RemoveTopicRecord` removes the aggregate with the topic. Complete batch/snapshot publication rejects every
missing, duplicate, unknown-topic, unknown-version, or invalid aggregate instead of publishing a partial image.

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

For topic incarnation ABA, one name-scoped `PulsarTopicGenerationSelector` permanently retains monotonic generation and
durable `DELETED(generation)`. An incarnation-scoped full aggregate may be exact-version CAS-replaced only after exact
reference-free retirement with a same-key `RetiredTopicIncarnationTombstone`; the key never becomes absent or reusable.
Selectors/tombstones count against hard lifetime metadata limits.

A single bounded deployment-level Virtual Ledger Namespace Registry is allocation authority for non-overlapping,
never-reused cell slices from `[2^62, 2^63 - 2]` and records native-exclusion evidence for the entire interval. Its
canonical complete assignment table uses one-key CAS and a monotonic registry epoch. Per-cell lookup/watch state is
derived. The cell's allocator operates only inside its current slice; missing, overlapping, drifted, revoked, or
capacity-exhausted registry state blocks allocation. Reservation checks are low-frequency control-plane work, not
normal append metadata I/O.

Every assignment is owned by an immutable Pulsar Protocol Cell tuple and follows
`ACTIVE -> RETIRING -> RETIRED`; retired rows and bounds remain forever. Each Cell has one immutable aligned `2^k`
slice, while numeric and encoded/lifetime registry limits jointly bound capacity. Broker/session/provider changes do not
change ownership or consume another assignment. 0.2 never resizes, relocates, extends, or attaches another slice;
exhaustion fails closed and additional capacity uses a new Protocol Cell.

## Object WalRun control records

Each Object-WAL shard stores a bounded immutable `WalRunRootRecord` in its Protocol Cell control-metadata backend. A
separate immutable `WalRunSealRecord` records terminal sequence and typed coverage; sealing never mutates the Root. A
successor Root binds predecessor Root+Seal identities, and one exact-version CAS advances `CurrentWalRunPointer` only
after the successor exists. Lost create/CAS responses converge by exact reread equality. These are rollover/recovery
cuts; normal admitted append performs no metadata read or mutation.

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

Relevant tradeoffs: `T-META-01`, `T-HANDOFF-01`, and `T-FABRIC-01`. Required scenarios: `V2-META-001..005`,
`V2-KAF-META-001..002`, `V2-HO-001`, `V2-FABRIC-001`, and `V2-POSITION-002..008`.
