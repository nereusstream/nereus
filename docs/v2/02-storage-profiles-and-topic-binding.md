---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Topic Protocol Binding, Storage Epochs, and profiles

## Immutable protocol binding

A Topic Incarnation is created with one durable protocol binding:

```text
TopicProtocolBinding {
  bindingId
  bindingVersion
  protocolCellId
  protocolKind
  topicIncarnationIdentity
  positionDomain
  payloadMapping
  nativeWriteAuthorityKind
}
```

The binding is resolved before I/O and is immutable for the incarnation. It fixes protocol identity and position
semantics, not physical placement. The current leader/broker holding the authority changes through Owner Epochs, while
the binding's Native Write Authority kind remains Kafka or Pulsar.

Missing, unknown, V1, or internally inconsistent values fail before append or read admission. A Topic Incarnation has
one Position Domain and one Native Write Authority at a time. Positions from another binding or incarnation are not
comparable.

`TopicIncarnationIdentity` is protocol-discriminated. Kafka uses its native topic UUID as authority and retains the
canonical topic name only for cross-checking. Pulsar uses canonical persistence/name facts plus a positive binding
generation, whose prior generation must be durably deleted before a new incarnation is created. Topic names alone are
never authority keys.

## Atomic initial aggregate

The Topic Protocol Binding and its one initial Storage Epoch are physically stored as one immutable
`TopicBindingAggregateRecord`. Kafka appends it inside the existing atomic `CreateTopics` controller result;
MetadataStore/Oxia creates one aggregate key with `putIfAbsent`. The logical binding and epoch stores are typed views of
that record and cannot mutate one component independently.

Aggregate authority keys include the protocol kind and complete typed incarnation: Kafka keys use the canonical topic
UUID, while Pulsar keys use a domain-separated canonical-persistence-name digest plus fixed-width generation. Values
repeat the identity and are validated against the key. `bindingId` and the ordinal-zero `storageEpochId` are separate
domain-separated SHA-256 derivations from canonical framed Cell/incarnation facts; random attempts, time, log offsets,
and backend versions cannot influence them.

A lost create response rereads the same record and requires exact equality. Missing, mismatched, unknown, or conflicting
aggregate state fails closed and never selects a default epoch. The generic `CREATING` fallback in ADR 0019 is not used
by this 0.2 single-record representation. Normal admitted append does not read or mutate the aggregate remotely. ADRs
0019, 0023, and 0028 are authoritative.

## Append-only Storage Epoch chain

Physical placement evolves through an append-only chain:

```text
StorageEpoch {
  storageEpochId
  bindingId
  storageProfile
  startProtocolFrontier
  sealedEndProtocolFrontier?
  primaryWalFormat
  payloadFormat
  objectExtentDigestFamily
  framePayloadChecksumFamily
  encryptionFamily
  lifecycleState/history
}
```

The profile and formats are immutable within one Storage Epoch. Its start frontier is binding-scoped and typed; a sealed
end frontier is assigned once and cannot be moved. Epoch coverage is ordered by the binding's Position Domain, not by a
universal offset. At most one epoch may admit newly allocated positions at a time, and a cutover may not create
overlapping Native Write Authorities or require synchronous dual write.

ADR 0015 fixes the 0.2 runtime to exactly one initial Storage Epoch per Topic Incarnation. The release exposes no online
profile-transition API/state machine and may not create a second epoch for an existing incarnation. The append-only
chain shape and typed-cut/single-admitting-epoch invariants remain durable model contracts for future evolution; they do
not advertise a 0.2 transition feature. Exact future transitions remain deferred in
[V2 open questions](open-questions.md).

## Operational policy

Operational policy is versioned separately and may change online:

- group linger and target bytes;
- per-topic and per-tenant admission budgets;
- cache and read-ahead limits;
- materialization concurrency and lag thresholds;
- retention duration and compaction cadence;
- observability sampling.

A policy change that affects a primary WAL profile, format, Object-extent digest family, Frame-payload checksum family,
or encryption family requires a new Storage Epoch at an exact Protocol Frontier. A materialization-only format or index
policy may create a new immutable generation without changing the append epoch. Neither operation rewrites the Topic
Protocol Binding.

## Profile contracts

### `OBJECT_WAL`

The Object group is the primary durable WAL. ACK waits for verified provider durability of the complete typed Protocol
Coverage.
The profile accepts batching latency in exchange for lower storage and request cost. Post-ack materialization remains
asynchronous and cannot weaken readability of acknowledged WAL ranges.

### `BOOKKEEPER_WAL_ONLY`

ACK waits for the configured BookKeeper quorum. Object storage is not required for append, recovery, read, or retention.
The profile explicitly accepts BookKeeper cost in exchange for low-latency quorum writes.

### `BOOKKEEPER_WAL_ASYNC_OBJECT`

ACK has the same BookKeeper boundary as the WAL-only profile. Sealed Protocol Coverage is asynchronously materialized
to Object storage. Lag policy may throttle or stop admission, but it never changes a completed ACK into a synchronous
Object wait.
For Kafka, BookKeeper source deletion requires the Nereus manifest and source-protection proof. For Pulsar, native
ManagedLedger ledger/offload metadata is the sole offload and deletion-eligibility authority; a Nereus manifest is
derived and cannot independently delete a native ledger. See [BookKeeper and Pulsar](04-bookkeeper-and-pulsar.md).

## System topics

Kafka internal topics and Pulsar system topics use explicit initial-epoch profile policy; they never inherit a tenant
default without validation. A protocol adapter may restrict the allowed initial profile set when its recovery or
transaction authority cannot satisfy the contract. No adapter exposes an online transition set in 0.2.

Relevant tradeoffs: `T-PROFILE-01`, `T-MIGRATION-01`, `T-OBJECT-01`, and `T-BK-01`. Required scenarios:
`V2-PROFILE-001`, `V2-MIGRATION-001`, and `V2-META-002..003`. See
[ADR 0019](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md) and
[ADR 0023](../decisions/0023-v2-topic-binding-aggregate-record.md) and
[ADR 0028](../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md).
