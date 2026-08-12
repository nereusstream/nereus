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
Duplicate exact pseudo-keys are last-wins, `CreateTopicPolicy` receives only native configs, and V2 admission requires
the stock remote-log system disabled rather than silently rewriting its topic.
Create admission sizes the actual final configuration-derived/aggregate/partition record list once in request-order
greedy linear time and leaves no rejected-candidate residue.
_Avoid_: Opaque attachment, parallel aggregate image, independent aggregate delete record, duplicate ConfigRecord
authority, mutable AlterConfigs pseudo-key, NO_OP validation evidence, automatic RLMM rewrite

**Kafka Frame**:
One complete raw Kafka RecordBatch after broker offset and leader-epoch assignment. Its exact batch header defines
coverage; record count does not derive the offset span.
_Avoid_: Produce request, individual record, transaction

**Kafka Append Commit Set**:
All Kafka Frames decoded from one partition's single MemoryRecords storage append. Every member is durable and valid
before any member becomes visible or acknowledged.
_Avoid_: Object group, partial batch-prefix success, cross-partition request atomicity

**Kafka BookKeeper Run**:
One ACTIVE/SEALED/RETIRED ledger-generation node in a Kafka partition's logical BookKeeper chain. It binds one
Binding/incarnation/partition, Storage Epoch, Owner Epoch, Provider Scope, ledger identity, and contiguous Kafka Offset
Range. It is the rollover, recovery, materialization, retention, and source-retirement unit; it never turns entry IDs
into Kafka offsets.
_Avoid_: Global mixed-partition ledger, BookKeeper Position as Kafka Position, per-append metadata row

**Kafka BookKeeper Range Index Block**:
One immutable checksummed BookKeeper control entry containing packed floor-search locators from complete assigned Kafka
RecordBatch ranges to exact DATA entries. The block is an asynchronous checkpoint and not an ACK prerequisite; the
owner-local active-tail locator view covers ACKed data after the last block.
_Avoid_: Oxia/KRaft row per Produce, one object per locator, record-count-derived coverage, ACK-time checkpoint

**Kafka BookKeeper Ordered Pipeline**:
The owner-local composition of pre-position capacity reservation, Kafka-native Offset Admission Sequencer, ordered
ledger-entry submission, bounded overlapping BookKeeper futures, and contiguous commit/publication queue. Completion
may be out of order; visibility and ACK may not advance around a gap.
_Avoid_: Remote offset allocator, serial wait-before-next-submit, out-of-order committed frontier

**Kafka BookKeeper Targeted Read**:
Run floor lookup followed by index-block floor lookup and packed-locator search, then one target DATA entry or the
minimum adjacent range. Routine validation uses BookKeeper digest, NBKE2 CRC32C, and Kafka RecordBatch header/CRC;
whole-run SHA belongs to seal, scrub, recovery, and materialization.
_Avoid_: Full append-extent read for one offset, range-wide SHA on every Fetch, cache as sole authority

**Kafka Partition Frontier Set**:
The coherent `LogStart <= LSO <= HW <= Readable/LEO <= Durable <= Allocated` state. Allocated is speculative admission,
Durable is the contiguous profile proof, LEO adds locator and protocol-state publication, HW is native ISR progress,
and LSO is transaction stability. Object materialization/checkpoint coverage are not visibility frontiers.
_Avoid_: One committedEndOffset, BookKeeper quorum as HW, Object materialization as LEO

**Kafka Ordered Protocol Commit**:
The partition-local cut that publishes a complete append commit set's locators, committed producer delta,
transaction/aborted delta, leader-epoch state, original append result, and Readable/Durable frontiers together.
Multiple storage futures may complete out of order; this cut never passes a predecessor gap.
_Avoid_: Data-visible-before-producer-state, producer-state-before-data, per-append Oxia CAS

**Kafka Shared-Storage Replica Observation**:
A follower's validated observation of one exact shared physical commit descriptor plus its producer/transaction /
leader-epoch effects. Kafka's native ISR/minISR surface consumes that observation to derive HW; the follower does not
write a duplicate WAL payload.
_Avoid_: Trusting leader-reported LEO, equating BookKeeper quorum with ISR, duplicate physical replica writes

**Kafka Fetch View**:
One allocation-free coherent local capture of Binding/incarnation, owner/leader/storage fences, run/active-tail/source
views, Log Start/LEO/HW/LSO, transaction/aborted index, leader-epoch index, and source protection for one partition read
batch, plus a versioned committed-producer-state reference when the native path needs it. Replica, read-uncommitted,
and read-committed upper bounds are LEO, HW, and LSO.
_Avoid_: Remote metadata per Fetch, mixed-generation torn state, connection-lifetime source pin

**Kafka Coverage-Aware Lookup**:
Run/index floor search followed by a coverage check and, when the floor does not contain the requested offset, the first
successor surviving RecordBatch across block/run boundaries. It handles compacted-away batches without changing
Kafka offsets.
_Avoid_: Floor-only lookup, record-count-derived span, splitting a compressed RecordBatch
