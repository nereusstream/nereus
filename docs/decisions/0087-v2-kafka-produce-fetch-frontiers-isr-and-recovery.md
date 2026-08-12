# ADR 0087: V2 Kafka Produce/Fetch frontiers, shared-storage ISR, and protocol recovery

## Status

Accepted for the 0.2 Kafka data path. This ADR freezes protocol-visible frontier semantics, Produce publication,
shared-storage ISR/HW behavior, producer/transaction recovery, Fetch isolation, and compaction-gap lookup. Exact Java
types, `NBKE2` field layout, checkpoint encoding, queue sizes, waiter implementation, and performance thresholds remain
M2/M6 implementation and evidence outputs. Implementation and executable evidence have not started.

## Context

ADR 0086 fixes the physical Kafka mapping:

```text
Kafka Offset Range
  -> BookKeeper run
  -> RangeIndexBlock
  -> BatchLocator
  -> complete raw Kafka RecordBatch
```

That mapping deliberately separates one partition append, one RecordBatch lookup unit, and one sealed ledger-run
lifecycle unit. It does not by itself define when data is the Kafka Log End Offset, High Watermark, or Last Stable
Offset. A single `committedEndOffset` would incorrectly collapse primary-WAL durability, owner-local readability,
Kafka replica observation, and transaction visibility.

V2 also cannot equate BookKeeper quorum with Kafka ISR. BookKeeper replicates physical bytes; Kafka ISR, `minISR`,
`acks=all`, HW, producer idempotency, transaction markers, and Fetch isolation remain Kafka protocol semantics. The
storage engine must preserve those semantics without writing the same payload once per Kafka replica or putting Oxia
in the normal Produce/Fetch path.

## Decision

### Partition frontier model

One Kafka Topic Partition owns these distinct half-open boundaries:

```text
KafkaPartitionFrontiers
  trimStartOffset
  allocatedEndOffset
  durableEndOffset
  readableEndOffset
  highWatermark
  lastStableOffset
```

Their meanings are:

- `trimStartOffset`: Kafka Log Start Offset and the first logically retained offset;
- `allocatedEndOffset`: next owner-local speculative offset after admission; it may include unresolved writes;
- `durableEndOffset`: greatest contiguous prefix with the selected profile's primary-WAL durability proof;
- `readableEndOffset`: greatest contiguous prefix whose locators, committed producer state, partition transaction
  state/index, and leader-epoch state were coherently published; this is Kafka LEO;
- `highWatermark`: greatest offset observed through Kafka's current ISR/minISR protocol; this is Kafka HW;
- `lastStableOffset`: Kafka LSO derived from HW and the first unstable transaction offset.

The invariant is:

```text
trimStartOffset <= lastStableOffset <= highWatermark
                <= readableEndOffset <= durableEndOffset <= allocatedEndOffset
```

Offsets from different Topic Incarnations, partitions, or Position Domains are not comparable. `ownerEpoch`, Kafka
`leaderEpoch`, and `storageEpochId`/binding generation are separate fences and never substitute for one another.

Materialization and recovery checkpoint state do not enter this ordering chain:

- `ObjectMaterializedFrontier` is only a derived greatest contiguous prefix with a verified preferred Object source;
  exact source coverage remains in the pinned Source Map;
- checkpoint coverage is a component vector, not one ambiguous `checkpointEndOffset`:
  `rangeIndexCoveredThrough`, `producerStateCoveredThrough`, `txnIndexCoveredThrough`, and
  `leaderEpochCoveredThrough`;
- `recoveryCoveredThrough` is the minimum compatible component boundary unless one atomic compound checkpoint proves
  the same cut.

Neither materialization nor a checkpoint can advance LEO, HW, or LSO.

### Produce admission and storage pipeline

For one partition subrequest, the owner performs:

1. native protocol parsing, authorization, quota, request-size, record-batch CRC, topic/partition, leader, Binding,
   Owner Epoch, and Storage Epoch validation;
2. Kafka producer ID/epoch/sequence and transaction validation;
3. reservation of completion tracker, active-tail locator, request bytes, BookKeeper pending entries, Object builder,
   and provider/concurrency capacity;
4. ordered assignment of one exact half-open Offset Range;
5. creation of one `KafkaAppendCommitSet` and one independent storage-attempt identity;
6. bounded overlapping BookKeeper/Object I/O in admission order;
7. exact profile durability resolution;
8. ordered publication through the greatest contiguous successful prefix;
9. one partition-local coherent publication of locators, producer state, transaction state/index, leader-epoch state,
   append result, `durableEndOffset`, and `readableEndOffset`;
10. local Fetch-waiter wakeup and Kafka replica/HW progression;
11. completion according to the requested `acks` mode.

Capacity is reserved before offset assignment. A request rejected by size, quota, memory, entry count, builder space,
or provider queue consumes no offset. `allocatedEndOffset` is owner-local speculative state and is not mutated through
one remote metadata CAS per Produce.

Offset admission is serialized, storage I/O is bounded and concurrent, and protocol publication is ordered. If B and
C become durable while predecessor A remains unresolved, neither B nor C becomes readable, advances LEO, or returns a
success ACK. A definitive A failure or non-converging gap fences the writer/run; later physical entries are inert
orphan tail. Recovery publishes only the greatest gap-free prefix and starts fresh admission under a new fence/run.

Speculative offsets may be reused only after fenced recovery proves they were never readable, HW-covered, or otherwise
committed and atomically rolls back locator, producer, transaction, and leader-epoch speculative state. An ambiguous
outcome remains fail-closed and is never reused merely because the client timed out.

### Kafka idempotency and speculative producer state

Kafka protocol idempotency and storage retry identity are different:

```text
KafkaBatchAppendIdentity = producerId + producerEpoch + baseSequence + lastSequence
StorageAttemptIdentity = appendAttemptId + ownerEpoch + storageEpochId
```

The Kafka identity is per RecordBatch and is validated before assigning a new offset. One multi-batch commit set binds
the complete ordered identity/result vector; it does not assume all batches have one producer ID. A committed exact
partition-subrequest duplicate returns its original result/Offset Range; a pending exact duplicate joins the existing
commit-set future; an attempt proven aborted and never visible may retry through a fresh internal storage attempt
without changing the Kafka request identity. Any mixed duplicate/new-batch behavior follows the native Kafka batch
validator and cannot be simplified by Nereus into one synthetic producer.

One producer may have multiple admitted commit sets in flight. Admission validates a new sequence against committed
producer state plus ordered speculative producer deltas, not committed state alone. The same ordered publication cut
that makes bytes readable commits the corresponding producer and transaction deltas. It is forbidden to publish data
without producer state or producer state without data.

The raw assigned Kafka RecordBatch remains producer/sequence/transaction payload authority. `NBKE2 DATA` may repeat
selected fields for bounded recovery and defensive validation only when exact equality with the raw batch is required;
those repeated fields never become a second Kafka authority. A commit-set descriptor binds the full Offset Range,
member ordinals/count, physical DATA range, identities, epochs, and aggregate payload digest. The common single-batch
path may carry its descriptor in that DATA entry; no extra control entry is required solely for that case.

### `acks`, logical ISR, and High Watermark

- `acks=0` emits no Produce response but still runs the complete durability, ordered publication, producer-state, and
  transaction-state path;
- `acks=1` completes only after `readableEndOffset >= appendEndOffset`; V2 may therefore provide stronger primary-WAL
  durability than local-disk Kafka while retaining the native response boundary;
- `acks=all` first applies Kafka's native `ISR.size >= min.insync.replicas` admission and completes only after
  `highWatermark >= appendEndOffset`;
- if ISR falls below `minISR` while an `acks=all` append waits, the native Kafka error semantics apply even if the
  shared physical bytes are already durable;
- a timeout is outcome-unknown, not proof of failure. An exact PID/epoch/sequence retry converges to the original
  result/offset if the append later became visible or HW-covered.

The default shared-storage mode uses logical Kafka replication over one shared physical copy:

1. the leader writes the payload once to the selected primary WAL;
2. a follower obtains the exact commit descriptor and validates Binding/incarnation, Kafka leader epoch, Owner/Storage
   fences, physical identity, coverage, integrity, and source accessibility;
3. it deterministically replays/installs the corresponding producer, transaction, and leader-epoch state;
4. it reports a `replicaObservedEndOffset` only through the native Kafka replica-progress surface;
5. Kafka HW is the native minimum eligible observation across the current ISR and obeys `minISR`.

A follower does not write a second copy of the payload to the shared WAL and does not trust a leader-reported offset
without validating the exact physical/protocol descriptor. For replication factor one, HW may advance with LEO after
the local publication cut. `BookKeeper quorum durable == Kafka HW` is not the default contract; any future storage-
native acknowledgement mode requires a separately named profile/ADR and cannot silently reinterpret ISR.

### Transaction visibility and recovery

A `KafkaAppendCommitSet` is partition-local. It does not make a Kafka transaction atomic across partitions. The
Transaction Coordinator decides commit/abort, and each involved partition persists native transactional batches plus
COMMIT/ABORT control batches.

Each partition maintains at least:

- producer epoch/sequence and recent-batch state;
- ongoing transaction first offsets and first unstable offset;
- completed and aborted transaction ranges/control markers;
- the transaction index required by `read_committed`;
- Kafka leader-epoch start offsets.

`PRODUCER_STATE_CHECKPOINT`, `TXN_INDEX_BLOCK`, and `LEADER_EPOCH_CHECKPOINT` are low-frequency authenticated control
entries. They carry their component coverage, Binding/incarnation, Owner Epoch, Storage Epoch, Kafka leader epoch where
applicable, and checksum. Async creation is allowed, but finite uncovered entries/bytes/age/time are mandatory;
exhaustion causes checkpoint, backpressure, or rollover before ACKed recovery becomes unbounded. A sealed run requires
a complete compatible checkpoint vector/footer.

Takeover selects the latest compatible vector and scans only the bounded suffix. It replays complete DATA/control
entries in Kafka offset order and reconstructs exactly the same LEO, producer state, open/completed/aborted transaction
state, LSO, and leader-epoch index. Post-gap residue is never adopted. Transaction-coordinator metadata in
`__transaction_state` remains coordinator authority and is distinct from these partition-local indexes and markers.

### Coherent Fetch snapshot and isolation

Every Fetch partition read captures one allocation-free, immutable logical snapshot/reference set containing at least:

```text
Binding/incarnation and generation
Owner Epoch, Kafka leader epoch, Storage Epoch
run table, active-tail view, and source-map generation
logStartOffset, LEO/readableEndOffset, HW, and LSO
committed producer-state generation
transaction/aborted-index generation
leader-epoch-index generation
source-protection/read-pin generation
```

This is a coherent publication cell plus immutable references, not a deep copy of producer maps or one heap object per
record. Ordinary Fetch performs no synchronous Oxia, Object metadata, or manifest-authority read.

The read upper bound is:

- replica Fetch: `readableEndOffset` / LEO;
- consumer `read_uncommitted`: HW;
- consumer `read_committed`: LSO.

Primary-WAL durable end, Object materialization, or latest index coverage never substitutes for these upper bounds.
The transaction/aborted index filters data according to native Kafka semantics within the captured snapshot.

### Delayed Fetch

When the selected upper bound does not pass the requested offset or available bytes do not satisfy `fetch.min.bytes`,
the broker registers a bounded local delayed-Fetch waiter/purgatory operation. It never polls Oxia, BookKeeper, or
Object Storage for progress.

Wakeup sources are isolation-specific LEO/HW/LSO advancement and also Log Start Offset movement, owner/leader epoch
change, source-view/readability change, partition offline/delete, and timeout. Multi-partition Fetch retains Kafka's
request-level byte/deadline behavior: a relevant partition event reevaluates the request, and completion occurs when
the native aggregate condition is satisfied.

### Offset lookup, compaction gaps, and sequential reads

Random lookup is not `floor(offset)` alone. It is:

1. floor-search the run and index/active-tail directory;
2. accept the floor locator only when its assigned `[baseOffset,lastOffset]` covers the request;
3. otherwise select the first successor surviving batch, possibly in the next index block or run;
4. read the target DATA entry and validate BookKeeper digest, `NBKE2` CRC, and raw Kafka batch header/CRC.

`lastOffsetDelta + 1`, rather than record count, represents coverage inside a sparse/compressed batch. Successor
lookup handles whole batches removed by log compaction. Missing/deleted offsets therefore advance to the first
surviving batch whose `lastOffset >= requestedOffset`, subject to the captured Fetch upper bound.

A complete Kafka RecordBatch is the minimum physical/response boundary; it is never split because a byte limit lands
inside it. Native Kafka first-oversized-batch behavior is retained. Random seek uses indexed targeted reads. Sequential
Fetch may retain a compact disposable cursor over run, index block, locator ordinal, next entry, and next Kafka offset,
then coalesce a byte-bounded adjacent entry range. A cursor never holds a source-generation pin across requests: every
Fetch captures a new coherent view and revalidates the cursor's exact identities/version, otherwise discarding it.

### Materialization and source selection

For `BOOKKEEPER_WAL_ASYNC_OBJECT`, source selection may be:

```text
[logStartOffset, ObjectMaterializedFrontier) -> Object preferred, protected BK fallback
[ObjectMaterializedFrontier, LEO)            -> BookKeeper active/sealed tail
```

One Fetch snapshot may intentionally plan disjoint non-overlapping Object and BookKeeper ranges. It must not replan
because a newer generation appears after capture. Source purity is required for each atomic append unit and each
declared whole-range fallback, not for an entire multi-range Fetch response.

Materialization preserves exact raw RecordBatch bytes plus range directory, producer-state checkpoint, transaction /
aborted index, leader-epoch index, and integrity roots. Publication requires exact coverage and side-index validation,
then durable generation publication and local view installation. Old BookKeeper extents remain protected until all old
view pins drain and source-protection/retirement/GC contracts permit deletion. Source switching never changes Kafka
offsets, LEO, HW, LSO, or consumer-group commits.

For `OBJECT_WAL`, one immutable Object group may contain multiple partition-local commit sets. ACK requires provider-
resolved immutable bytes, reconstructible Root/key identity, complete local protocol-state/locator publication, and
the existing bounded LIST/checkpoint recovery contract. It does **not** reintroduce one remote metadata mutation per
commit set or require an async checkpoint page in the ACK cut. Low-frequency manifest generations select long-lived
read views/materialized sources; they are not the normal per-group append linearization point.

### Consumer-group offsets, internal topics, and profile boundary

Consumer-group committed offsets remain `(groupId, topic, partition, logicalOffset, metadata)` and contain no ledger,
entry, Object key, or byte position. Fetch resolves that logical offset through the same run/directory/locator path.

The product may benchmark a low-latency BookKeeper profile for `__consumer_offsets` and `__transaction_state`, but no
new 0.2 default is frozen here. Their profile remains an explicit versioned Kafka internal-topic Deployment policy and
must satisfy compaction, coordinator recovery, and transaction evidence before selection.

Profile durability differs; protocol publication does not:

| Profile | `durableEndOffset` proof | `readableEndOffset` publication | Default physical source |
| --- | --- | --- | --- |
| `OBJECT_WAL` | provider-resolved immutable group under Root/key/recovery contract | locator plus producer/txn/leader state | Object |
| `BOOKKEEPER_WAL_ONLY` | complete commit set and terminal descriptor at BK quorum | locator plus producer/txn/leader state | BookKeeper |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | same as BK-only | same as BK-only | BookKeeper tail; Object preferred after generation handoff |

## Consequences and tradeoffs

- Gain: Kafka LEO/HW/LSO, idempotency, transactions, ISR/minISR, and leader-epoch behavior remain explicit rather than
  hidden inside a storage-specific committed offset.
- Gain: normal Produce/Fetch has zero remote control-metadata I/O, BookKeeper/Object I/O overlaps, and followers do not
  duplicate shared physical payload writes.
- Gain: random reads target one batch while sequential reads coalesce, and compaction gaps remain seek-correct.
- Cost: partition-local producer/transaction speculation, ordered multi-state publication, follower validation,
  checkpoint-vector compatibility, delayed-fetch wakeups, and source-pinned snapshots add state-machine complexity.
- Tradeoff: `acks=1` is stronger than local-disk leader acceptance under the selected profile; `acks=all` still waits
  for native logical ISR/HW and therefore may be slower than treating BookKeeper quorum as ISR.
- Tradeoff: async checkpoints remove normal ACK I/O but require hard uncovered-tail bounds and bounded takeover scan.

## Evidence and implementation boundary

M2 owns the storage-engine/frontier/producer/transaction/index/checkpoint/read primitives and deterministic fault
harness. M6 owns native Kafka broker/replica/purgatory/controller integration, exact error mapping, client-compatible
Produce/Fetch/transaction/leader-failover behavior, and full-process restart evidence. M4/M5 own materialized read-view
handoff and source retirement integration. No milestone may claim Kafka parity or superiority from document status.
The code-level cut is [the M2 Kafka Produce/Fetch detailed design](../v2/detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md).

Required scenarios are `V2-KAF-DATA-001..016`. They cover out-of-order durability, predecessor failure, response-loss
retry, speculative producer state, checkpoint crash recovery, LEO/HW/LSO isolation, abort filtering, ISR shrink,
delayed Fetch, compaction gaps, pinned source generation, Object/BK fallback and GC, pre-admission rejection,
leader-epoch recovery, and random/sequential full-batch reads.

This ADR refines ADRs 0009, 0011, 0031, 0067, 0069, and 0086. Exact wire and evidence-derived numeric bounds remain
under `V2-OPEN-BK-02`; they are not reopened as protocol semantic choices.
