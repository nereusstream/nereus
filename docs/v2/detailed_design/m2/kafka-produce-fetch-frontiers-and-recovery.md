---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesignWithOpenEvidence
sourceTuple: v2-m0
---

# M2 Kafka Produce/Fetch frontiers and protocol recovery design

## Delivery boundary

This design implements [ADR 0087](../../../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md) on
top of [the ADR 0086 run/range-index design](kafka-bookkeeper-offset-range-index.md). M2 owns the engine primitives and
fault harness; M6 connects them to Kafka leadership, replicas, purgatory, errors, transactions, and broker lifecycle.
M4/M5 connect immutable read generations and source retirement. Existing V1 partition storage, reservation,
producer-recovery, and leader-epoch code is evidence only and is replaced rather than wrapped or dual-written.

The first coherent slice includes:

1. partition frontier state and one coherent publication cell;
2. pre-offset admission plus speculative producer/transaction deltas;
3. profile durability resolution and ordered multi-state commit;
4. logical replica observation/HW integration seam;
5. producer, transaction, range-index, and leader-epoch checkpoint vector;
6. immutable Fetch capture, isolation bounds, delayed-wakeup seam, and compaction-gap lookup;
7. random targeted reads, sequential cursor validation, and source-generation pinning;
8. deterministic failure cuts followed by real BookKeeper and native Kafka integration evidence.

## Partition state model

The owner keeps one partition-local state root. Logical fields are shown below; implementation may pack hot fields and
use immutable references, but it may not split publication into independently visible authorities.

```text
KafkaPartitionProtocolStateV1
  bindingId
  topicIncarnation
  partitionId
  bindingGeneration
  storageEpochId
  ownerEpoch
  kafkaLeaderEpoch
  stateVersion
  trimStartOffset
  allocatedEndOffset
  durableEndOffset
  readableEndOffset             // Kafka LEO
  highWatermark
  lastStableOffset
  runTableRef
  activeTailRef
  sourceMapRef
  committedProducerStateRef
  speculativeProducerQueueRef
  transactionIndexRef
  leaderEpochIndexRef
  checkpointVectorRef
```

`stateVersion` and the frontier tuple are captured coherently through a single immutable reference or seqlock-backed
publication cell. A reader may not combine the LEO from one version with the transaction index or Source Map from
another. Large maps are immutable/versioned references; capture does not copy them.

Checked validation enforces:

```text
trimStart <= LSO <= HW <= LEO/readable <= durable <= allocated
```

The component checkpoint vector is:

```text
KafkaRecoveryCheckpointVectorV1
  rangeIndexCoveredThrough
  producerStateCoveredThrough
  txnIndexCoveredThrough
  leaderEpochCoveredThrough
  compatibleRunIdentity
  bindingId
  storageEpochId
  creatorOwnerEpoch
  kafkaLeaderEpochAtWrite
```

Only a single compound checkpoint or the minimum mutually compatible component boundary seeds suffix recovery.

## Pre-offset admission

The admission method is side-effect ordered:

```text
validate request/batches/fences/producer/transaction
  -> reserve tracker + locator + request bytes + pending entries + provider permits
  -> allocate Kafka Offset Range
  -> reserve contiguous DATA entry IDs / Object group membership
  -> install PendingCommitSet and speculative protocol deltas
  -> submit storage I/O
```

One reservation object owns every permit and either transfers them into the commit set or releases them. Failure before
offset allocation consumes no position. Failure after allocation enters the ordered resolver; arbitrary cancellation
cannot delete the slot and let a successor publish around it.

Logical offset count is checked `lastOffsetDelta + 1` from assigned Kafka batch headers. Record count is never used.
Every batch in one partition storage append is included in one `KafkaAppendCommitSet` with one Kafka range.

## Producer and storage identities

The engine exposes separate closed identities:

```text
KafkaBatchAppendIdentityV1
  producerId
  producerEpoch
  baseSequence
  lastSequence

KafkaCommitSetIdentityVectorV1
  orderedBatchIdentities[]
  exactRawBatchDigest

StorageAttemptIdentityV1
  appendAttemptId
  ownerEpoch
  storageEpochId
```

The batch identity is per RecordBatch; one commit set binds an ordered vector and may not assume a single producer ID.
Admission consults a producer-state view composed from committed state and ordered speculative deltas. The pending
partition-subrequest identity table returns exactly one of:

- `NEW_VALID`: allocate and submit;
- `PENDING_EXACT`: join the existing completion future;
- `COMMITTED_EXACT`: return the original assigned result according to current response fencing;
- `OUT_OF_ORDER_SEQUENCE`, `DUPLICATE_CONFLICT`, or another native Kafka rejection;
- `INDETERMINATE`: fail closed until recovery resolves the predecessor.

No result compares only PID/sequence while ignoring exact raw batch identity. Payload equivalence uses the frozen
commit-set digest and assigned coverage. A new internal attempt ID never turns a conflicting Kafka retry into an exact
duplicate.

## Storage completion and ordered commit queue

Each pending slot progresses locally through closed states such as:

```text
ADMITTED -> IO_IN_FLIGHT -> DURABLE_EXACT -> PUBLISHED
                         -> DEFINITIVE_FAILURE
                         -> INDETERMINATE
```

State names are implementation-local; the semantic cuts are not. Storage callbacks validate exact run/Object,
entry/group, identity, length, digest, owner/storage fence, and profile proof. Completion order may differ from slot
order. Only the queue head may publish.

One serialized publication applies the complete delta:

```text
hidden locator ranges
+ committed producer delta
+ transaction/open-abort-complete delta
+ leader-epoch observation
+ append result/original offsets
+ durable/readable frontier
```

It then release-publishes the new coherent state, wakes local Fetch/replica waiters, rechecks the admission fence, and
completes eligible acknowledgements. A crash cannot expose a locator/frontier while retaining old producer or
transaction state.

If the head definitively fails, the writer fences new admission and resolves every successor as inert physical tail or
unacknowledged protocol work. It never publishes a later range. If the head is outcome-unknown, the queue remains
blocked under a hard age/count/bytes deadline until exact reconciliation or run fencing/recovery.

## Profile durability resolver

The ordered queue consumes a closed `ProfileDurabilityProof` whose type is fixed by the Storage Epoch:

- `OBJECT_WAL`: exact provider-resolved immutable Object/group identity under WalRun Root/key/LIST recovery plus
  complete commit-set membership;
- `BOOKKEEPER_WAL_ONLY`: every DATA member and terminal descriptor at BookKeeper quorum;
- `BOOKKEEPER_WAL_ASYNC_OBJECT`: the same BookKeeper proof; Object materialization is not in the Produce ACK cut.

The resolver performs no per-append Oxia/KRaft mutation. For Object WAL, asynchronous physical checkpoint pages and
long-lived manifest handoff remain outside the group ACK cut. The local readable publication is still mandatory.

## Acknowledgement and replica observation seam

The append result owns one `appendEndOffset` and native `acks` policy:

| Mode | Completion condition |
| --- | --- |
| `acks=0` | no response; internal work remains live through the same correctness path |
| `acks=1` | coherent state has `readableEndOffset >= appendEndOffset` |
| `acks=all` | native pre-admission ISR/minISR passed and Kafka HW reaches `appendEndOffset` |

M2 provides a validated commit descriptor stream and follower replay kernel. M6 connects it to the current Kafka
ReplicaManager/Partition/fetcher progress surface. A follower must validate exact Binding, incarnation, leader/owner /
storage epochs, run/Object identity, range, checksum, and producer/transaction/leader-epoch delta before reporting:

```text
ReplicaObservedProgress
  replicaId
  kafkaLeaderEpoch
  observedEndOffset
  validatedStateVersion
  descriptorDigest
```

This is not a new metadata authority and is not persisted to Oxia. Native Kafka owns ISR membership, minISR, HW,
timeouts, and errors. Shared storage eliminates duplicate payload writes, not logical replica validation.

## Transaction and leader-epoch state

Partition transaction state includes ongoing transactions, first unstable offset, completed/aborted ranges, control
markers, and the index used to filter `read_committed`. It advances only in the ordered publication cut. Cross-
partition commit/abort remains Transaction Coordinator authority.

The leader-epoch index stores Kafka `leaderEpoch -> startOffset`. Owner Epoch and Storage Epoch never substitute.
`OffsetForLeaderEpoch`, follower truncation, leader failover, and client epoch validation use the Kafka index.

Async `PRODUCER_STATE_CHECKPOINT`, `TXN_INDEX_BLOCK`, and `LEADER_EPOCH_CHECKPOINT` controls are inserted only between
complete commit sets by the shared ledger-entry sequencer. Each component carries a checked offset boundary and exact
identity. Checkpoint cadence is operational, but aggregate uncovered entries/bytes/age/time and seal completeness are
hard contracts.

## Takeover recovery

Recovery performs:

1. invalidate old admission and fence/open the prior physical run;
2. validate the run header and latest mutually compatible checkpoint vector;
3. scan the bounded suffix in physical order;
4. validate each complete commit-set descriptor and raw RecordBatch;
5. replay producer, transaction/control-marker, and leader-epoch deltas in Kafka offset order;
6. stop at the first definitive gap/conflict and quarantine later residue;
7. derive the same durable/readable frontier, producer map, transaction/aborted index, LSO, and leader-epoch index;
8. complete index/checkpoint/footer state, seal the old run, and open a new run/fence before admission.

Response loss is resolved by exact identity/bytes. A conclusively uncommitted speculative range may be reassigned only
after this recovery cut proves no visible/HW state and discards all coupled speculative deltas. The recovery envelope
is cumulative; falling back between checkpoint components cannot reset entry/byte/time counters.

## Fetch capture

The logical capture is:

```text
KafkaReadSnapshotV1
  binding/incarnation/generation
  ownerEpoch
  kafkaLeaderEpoch
  storageEpochId
  stateVersion
  runTableRef
  activeTailRef
  sourceMapRef
  logStartOffset
  logEndOffset                 // readableEndOffset
  highWatermark
  lastStableOffset
  committedProducerStateRef
  transactionIndexRef
  leaderEpochIndexRef
  sourceProtectionGeneration
  readViewPin
```

It uses the ADR 0069/0070 allocation-free generation pin/hazard contract for one partition read batch. No remote
metadata read occurs. Replica/read-uncommitted/read-committed upper bounds select LEO/HW/LSO respectively. The read
planner refuses any locator whose coverage crosses the captured bound.

## Delayed Fetch

M2 exposes frontier-versioned waiter registration; M6 adapts it to Kafka delayed-operation purgatory. A waiter binds
partition, isolation, requested offset, minimum bytes, deadline, captured leader/state version, and cancellation.
Registration and frontier publication use a lost-wakeup-safe sequence:

```text
capture state/version
  -> evaluate
  -> register against exact version
  -> re-evaluate version/frontier
  -> sleep only if unchanged and insufficient
```

LEO wakes replica waiters, HW wakes read-uncommitted waiters, and LSO wakes read-committed waiters. Log-start movement,
leader/owner change, read-view/source change, offline/delete, and timeout wake every affected class. Wakeup only
re-evaluates local state; it never polls BookKeeper, Object Storage, or Oxia.

## Locator lookup and read planning

The lookup kernel is coverage-aware:

```text
candidate = floor(requestedOffset)
if candidate != null && candidate.lastOffset >= requestedOffset:
    use candidate
else:
    candidate = successor(requestedOffset) across block/run
```

It rejects a candidate beyond the snapshot upper bound and returns offset-out-of-range against the captured Log Start /
end rules. Locator coverage is checked against the raw assigned RecordBatch header. A complete batch deleted by
compaction creates a gap and is skipped via successor; sparse offsets inside a surviving batch remain covered by
`lastOffsetDelta + 1`.

Random reads target the one DATA entry. Sequential reads may use a packed cursor:

```text
run identity + source generation + index block identity + locator ordinal
+ next entry ID + next Kafka offset + snapshot state version
```

Every new Fetch captures and pins a fresh snapshot, then accepts the cursor only on exact identity/version agreement.
The cursor is discarded on compaction, trim, source generation, leader/owner, run, or index change. It never pins a
generation across requests. Adjacent entries are coalesced by byte budget, not fixed entry count. A RecordBatch is
never split; native first-oversized-batch behavior applies.

## Object materialization and pinned source plans

One captured Fetch may plan an Object prefix and BookKeeper tail, or another set of non-overlapping Source Map ranges.
Generation change after capture does not alter that plan. Each atomic commit set and each whole-range fallback is
source-pure. Existing ADR 0069/0071-0080 pin, protection, handoff, and GC contracts apply.

Materialization must reproduce raw RecordBatch bytes and verified range, producer, transaction/aborted, and leader-
epoch indexes before publishing the successor generation. `ObjectMaterializedFrontier` is a routing optimization only.

## Implementation cuts

Recommended reviewable cuts are:

1. frontier/value validators and coherent publication cell;
2. producer identity, speculative delta queue, duplicate joining, and deterministic unit cuts;
3. profile durability proof and ordered multi-state commit queue;
4. transaction/leader-epoch indexes plus checkpoint vector and bounded recovery;
5. Fetch snapshot/isolation, coverage-aware floor+successor, and targeted/sequential reader;
6. lost-wakeup-safe delayed-Fetch seam;
7. logical follower validation/observed-progress seam and shared-storage ISR fault harness;
8. real BookKeeper plus native Kafka Produce/Fetch/transaction/failover integration;
9. performance/scale receipts and separate mechanical V1 removal.

No cut adds per-append control metadata, dual-write compatibility, a second offset authority, or a storage-native ISR
shortcut.

## Evidence gates

`V2-KAF-DATA-001..016` are mandatory. M2 receipts measure at least Produce p50/p99, pipeline depth, allocation bytes,
duplicate-join cost, active-tail/index bytes, recovery entries/bytes/time, targeted/random and sequential read
amplification, waiter registration/wakeup cost, and zero normal-path metadata calls. M6 receipts add native Kafka error
codes, ISR/minISR/HW behavior, transaction coordinator/control-marker flows, full client retries, broker restart,
leader transfer, Fetch purgatory, and comparison with the pinned Kafka baseline.

Exact queue/checkpoint/cursor sizes, compact encodings, and thresholds stay evidence-derived. Topic policy cannot enlarge
hard correctness/recovery bounds; Cell/host pressure may backpressure or seal early without changing persisted bytes or
Kafka visibility semantics.
