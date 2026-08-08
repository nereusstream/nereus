---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Storage profiles and topic binding

## Semantic binding

A topic incarnation is created with one durable semantic binding:

```text
TopicStorageBinding {
  bindingVersion
  protocol
  durableTopicId
  incarnation
  storageProfile
  primaryWalFormat
  payloadMapping
  tieringMode
  checksumFamily
  encryptionFamily
}
```

The binding is resolved before I/O and is immutable for the incarnation. Missing, unknown, V1, or internally
inconsistent values fail before append or read admission. V2 does not scan historical bytes to guess a profile.

## Operational policy

Operational policy is versioned separately and may change online:

- group linger and target bytes;
- per-topic and per-tenant admission budgets;
- cache and read-ahead limits;
- materialization concurrency and lag thresholds;
- retention duration and compaction cadence;
- observability sampling.

A policy change that affects physical layout, checksum, or encryption starts a new WAL run or segment generation at an
explicit range boundary. It does not rewrite the semantic profile.

## Profile contracts

### `OBJECT_WAL`

The Object group is the primary durable WAL. ACK waits for verified provider durability of the complete frame range.
The profile accepts batching latency in exchange for lower storage and request cost. Post-ack materialization remains
asynchronous and cannot weaken readability of acknowledged WAL ranges.

### `BOOKKEEPER_WAL_ONLY`

ACK waits for the configured BookKeeper quorum. Object storage is not required for append, recovery, read, or
retention. The profile explicitly accepts BookKeeper cost in exchange for low-latency quorum writes.

### `BOOKKEEPER_WAL_ASYNC_OBJECT`

ACK has the same BookKeeper boundary as the WAL-only profile. Sealed ranges are asynchronously materialized to Object
storage. Lag policy may throttle or stop admission, but it never changes a completed ACK into a synchronous Object wait.
BookKeeper source deletion requires the manifest/offload and protocol-native safety proof in
[BookKeeper and Pulsar](04-bookkeeper-and-pulsar.md).

## System topics

Kafka internal topics and Pulsar system topics use explicit profile policy; they never inherit a tenant default without
validation. A protocol adapter may restrict the allowed profile set when its recovery or transaction authority cannot
yet satisfy the profile contract.

Relevant tradeoffs: `T-PROFILE-01`, `T-OBJECT-01`, and `T-BK-01`. Required scenario: `V2-PROFILE-001`.
