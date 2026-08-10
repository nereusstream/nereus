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

**Kafka Nereus Feature Level**:
The KRaft finalized `nereus.storage.version=2` established only during fresh storage bootstrap. V2 rejects V1 level 1
and every runtime upgrade/downgrade.
_Avoid_: Online V1 migration, unsafe feature downgrade, absent feature as V2 activation

**Kafka Topic Binding Aggregate Record**:
The generated typed KRaft non-flexible wire-v0 record at API key 32000, owned by one `TopicImage`. MetadataLoader
validates touched topics at ordinary publication and all live topics only at snapshot/bootstrap. Completed snapshots
place it after `TopicRecord` and before partitions; `RemoveTopicRecord` removes it with the topic. Nereus CreateTopics
pseudo-config is exactly case-sensitive/no-trim `nereus.storage.profile`, is removed before native `ConfigRecord`
emission, persists only as resolved aggregate facts, and is exposed only as an optional read-only projection. Its
classifier v1 contains the three pinned Kafka built-ins only; other application/Admin-created topics use the user path.
Create admission sizes the actual final configuration-derived/aggregate/partition record list once in request-order
greedy linear time and leaves no rejected-candidate residue.
_Avoid_: Opaque attachment, parallel aggregate image, independent aggregate delete record, duplicate ConfigRecord
authority, mutable AlterConfigs pseudo-key

**Kafka Frame**:
One complete raw Kafka RecordBatch after broker offset and leader-epoch assignment. Its exact batch header defines
coverage; record count does not derive the offset span.
_Avoid_: Produce request, individual record, transaction

**Kafka Append Commit Set**:
All Kafka Frames decoded from one partition's single MemoryRecords storage append. Every member is durable and valid
before any member becomes visible or acknowledged.
_Avoid_: Object group, partial batch-prefix success, cross-partition request atomicity
