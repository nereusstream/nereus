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
  durableTopicId
  topicIncarnation
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
  checksumFamily
  encryptionFamily
  lifecycleState/history
}
```

The profile and formats are immutable within one Storage Epoch. Its start frontier is binding-scoped and typed; a sealed
end frontier is assigned once and cannot be moved. Epoch coverage is ordered by the binding's Position Domain, not by a
universal offset. At most one epoch may admit newly allocated positions at a time, and a cutover may not create
overlapping Native Write Authorities or require synchronous dual write.

This is a domain contract, not a claim that every transition is supported in 0.2. The initial transition matrix, exact
runtime state machine, rollback points, historical-data movement, and Pulsar BookKeeper/Object strategy remain
non-normative questions in [V2 open questions](open-questions.md).

## Operational policy

Operational policy is versioned separately and may change online:

- group linger and target bytes;
- per-topic and per-tenant admission budgets;
- cache and read-ahead limits;
- materialization concurrency and lag thresholds;
- retention duration and compaction cadence;
- observability sampling.

A policy change that affects a primary WAL profile, format, checksum family, or encryption family requires a new Storage
Epoch at an exact Protocol Frontier. A materialization-only format or index policy may create a new immutable generation
without changing the append epoch. Neither operation rewrites the Topic Protocol Binding.

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
BookKeeper source deletion requires the manifest/offload and protocol-native safety proof in
[BookKeeper and Pulsar](04-bookkeeper-and-pulsar.md).

## System topics

Kafka internal topics and Pulsar system topics use explicit initial-epoch profile policy; they never inherit a tenant
default without validation. A protocol adapter may restrict the allowed profile set or transition set when its recovery
or transaction authority cannot yet satisfy the contract.

Relevant tradeoffs: `T-PROFILE-01`, `T-MIGRATION-01`, `T-OBJECT-01`, and `T-BK-01`. Required scenarios:
`V2-PROFILE-001` and `V2-MIGRATION-001`.
