# Kafka Context

The Kafka Context owns Kafka-native topic-partition behavior and positions while delegating physical durability and
lifecycle to the Shared Storage Context.

## Language

**Kafka Topic Partition**:
The Kafka-native ordered log aggregate bound to one Kafka Protocol Cell and one Topic Incarnation.
_Avoid_: Stream, ManagedLedger

**Kafka Offset**:
The only protocol position truth for one Kafka Topic Partition.
_Avoid_: BookKeeper entry ID, object byte offset, global logical offset

**Kafka Offset Range**:
A half-open range of Kafka Offsets within one Topic Protocol Binding.
_Avoid_: Physical extent, cross-topic range

**Kafka Position Domain**:
The Position Domain whose ordering and adjacency rules are defined by Kafka Offsets.
_Avoid_: Universal position domain

**Kafka Native Write Authority**:
The Kafka partition leadership and protocol state permitted to allocate Kafka Offsets for a bound Topic Incarnation.
_Avoid_: BookKeeper writer, Pulsar broker authority

**Kafka Frame**:
One complete raw Kafka RecordBatch after broker offset and leader-epoch assignment. Its exact batch header defines
coverage; record count does not derive the offset span.
_Avoid_: Produce request, individual record, transaction

**Kafka Append Commit Set**:
All Kafka Frames decoded from one partition's single MemoryRecords storage append. Every member is durable and valid
before any member becomes visible or acknowledged.
_Avoid_: Object group, partial batch-prefix success, cross-partition request atomicity
