# ADR 0087: V2 Kafka Produce/Fetch frontiers, shared-storage ISR, and protocol recovery

## Status

Accepted for the 0.2 Kafka data path. This ADR freezes protocol-visible frontier semantics, Produce publication,
shared-storage ISR/HW behavior, producer/transaction recovery, Fetch isolation, and compaction-gap lookup. It is
refined by the implementation-readiness corrections in this revision: fenced publication, election-bounded tail
adoption, Observed/Applied replica progress, native duplicate semantics, persisted Kafka leader epoch, and a
profile-neutral protocol-checkpoint contract. Exact Java types, `NBKE2`/`NWKCP1` field layout, queue sizes, waiter
implementation, and performance thresholds remain M2/M3/M6 implementation and evidence outputs. Implementation and
executable evidence have not started.

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

### Persisted Kafka leader-epoch boundary

Kafka leader epoch, Nereus Owner Epoch, and Storage Epoch are independent. A BookKeeper run is bound to exactly one
Kafka leader epoch; an epoch change stops/seals/reconciles the old ACTIVE run and opens a fresh run before admission,
even when the same broker remains owner. `KafkaBookKeeperRunV1`, `RUN_HEADER`, every `NBKE2 DATA`/commit descriptor,
range-index anchor, protocol checkpoint, and `RUN_FOOTER` bind or exactly derive that leader epoch.

An Object WalRun may mix partitions and therefore never stores one singular Kafka leader epoch in its Root. Each Kafka
append-unit directory row/context binds its exact partition and Kafka leader epoch, and every commit-set member must
cross-check it. The raw assigned RecordBatch leader-epoch field remains protocol-native authority where applicable.

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
9. one fence-protected partition-local coherent publication of locators, producer state, transaction state/index,
   leader-epoch state, append result, `durableEndOffset`, and `readableEndOffset`;
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

### Fenced coherent publication

Storage durability does not authorize a stale callback to publish Kafka state. Before anything becomes visible, the
ordered queue enters one partition publication cut and compares at least the exact Binding ID, Topic Incarnation,
Binding generation, Storage Epoch ID, Owner Epoch, Kafka leader epoch, and predecessor publication `stateVersion`.
The implementation uses either one state-root compare-and-set or one serialized publication lock with an equivalent
checked state-root replacement. Kafka leadership, ownership, and Storage-Epoch transitions compete on that same cut.

The linearization rules are:

- if the append publication wins first, the complete commit set legally belongs to the old leader epoch;
- if a fence/leadership transition wins first, the stale append cannot advance Durable/Readable/LEO, install protocol
  state, wake a Fetch/replica waiter, or produce a success ACK;
- a transition after successful publication does not undo legal old-epoch data. A final response-time fence check may
  withhold the response and report an outcome-unknown/native fenced error, but it cannot make the publication illegal.

Therefore `release-publish -> final fence check` is not the publication protocol. The mandatory order is `profile
durability -> fenced coherent publication -> waiter/progress notification -> optional response fence check -> ACK`.

### Kafka idempotency and speculative producer state

Kafka protocol idempotency and storage retry identity are different:

```text
KafkaBatchDuplicateIdentity = producerId + producerEpoch + baseSequence + lastSequence
StorageAttemptIdentity = appendAttemptId + assignedOffsetRange + storedAssignedBatchDigest + physicalExtentIdentity
```

The Kafka identity is per RecordBatch and is validated before assigning a new offset. Native Kafka producer-state
validation alone decides whether PID/epoch/sequence is a duplicate; Nereus does not add a payload-digest-based
`DUPLICATE_CONFLICT`. A native committed duplicate returns the original result/Offset Range even when a second valid
request carries different application payload bytes under the same native duplicate tuple. All native parsing, CRC,
epoch, sequence, and error-precedence rules still run. One multi-batch commit set binds the complete ordered native
identity/result vector; it does not assume all batches have one producer ID.

Two payload digests have deliberately narrower scopes:

- `IngressRequestDigest` covers canonical unassigned client bytes and may only protect joining the same explicit
  in-process request instance to its pending future. It never creates protocol-level deduplication;
- `StoredAssignedBatchDigest` covers final persisted bytes after broker-assigned offsets and any permitted timestamp,
  leader-epoch, header, or CRC rewrite. It is storage response-loss/recovery evidence only.

An internal storage attempt proven aborted and never visible may retry with a fresh attempt ID without changing the
Kafka request identity. A non-idempotent request with `NO_PRODUCER_ID` has no protocol duplicate guarantee: timeout
and retry may append twice, and equal payload digest never returns an earlier offset. A pending join for such a request
is legal only for the same explicit in-process request instance. Mixed duplicate/new-batch behavior follows the native
Kafka batch validator and cannot be collapsed into one synthetic producer or storage-digest rule.

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

The default shared-storage mode uses logical Kafka replication over one shared physical copy. The selected transport
is a compact ordered commit-descriptor stream carried through the native leader-to-follower replica Fetch/fetcher
channel. It is not one KRaft/Oxia record per append and the leader does not send a second raw payload copy. Exact wire
framing is an M6 evidence output, but the transport and progress semantics are fixed:

1. the leader writes the payload once to the selected primary WAL and publishes the exact commit descriptor only after
   the fenced local cut;
2. the follower validates Binding/incarnation, Kafka leader epoch, Owner/Storage fences, physical identity, coverage,
   integrity/durability proof, and source accessibility, then durably appends the compact descriptor to its bounded
   local `ReplicaObservationJournal`;
3. this advances `replicaObservedEndOffset` without requiring the follower to read and decode all raw payload bytes;
4. independently, the follower reads the referenced source, validates raw RecordBatch bytes, and applies producer,
   transaction, and leader-epoch state through `replicaAppliedEndOffset`;
5. Kafka HW uses eligible observed progress through the native replica-progress surface; election readiness uses
   applied progress.

The invariant is `replicaAppliedEndOffset <= replicaObservedEndOffset`. A profile/provider that cannot produce a
qualified descriptor/source/durability proof without reading the payload must conservatively collapse Observed to
Applied; it may not report a weaker offset as observed. Descriptor traffic is charged to native replication quotas;
provider reads/decode/catch-up are charged to explicit Cell/provider replica-read budgets. Missing descriptor-stream
history uses bounded checkpoint/source catch-up and never introduces normal per-append control metadata.

A follower does not write a second payload copy to the shared WAL and does not trust a leader-reported offset without
validating and locally journaling the exact descriptor. Before it may become leader it must have
`replicaAppliedEndOffset >= electionAdoptableEndOffset`. For replication factor one, HW may advance with LEO after the
local publication cut. `BookKeeper quorum durable == Kafka HW` is not the default contract; any future storage-native
acknowledgement mode requires a separately named profile/ADR and cannot silently reinterpret ISR.

The product meaning of replication knobs is therefore explicit:

| Contract | What it controls |
| --- | --- |
| Kafka `replication.factor` | logical broker replicas, leader candidates, ISR, and HW availability |
| Kafka `min.insync.replicas` | eligible logical replica observations required by `acks=all` |
| BookKeeper ensemble/write/ack quorum | physical BookKeeper redundancy and failure tolerance |
| Object Provider durability | physical Object redundancy/durability |
| Protocol Cell / Provider Scope | credentials, isolation, and provider failure domain |

Increasing Kafka replication factor does not create independent external-storage copies. Replicas sharing one
provider have a correlated storage failure domain; this limitation is part of the product contract and observability.

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

A profile-neutral logical `KafkaProtocolCheckpointStore` publishes/recover-checks producer, transaction/aborted, and
leader-epoch components at one compatible covered-through vector. The BookKeeper implementation uses authenticated
`NBKE2` control entries between complete commit sets. The Object-WAL implementation uses a distinct bounded,
Root-bound, content-addressed `NWKCP1` protocol-state checkpoint Object family. `NWKCP1` may batch bounded partition
rows, but each row binds Binding/incarnation, partition, Storage Epoch, Owner Epoch, Kafka leader epoch,
`coveredThrough`, producer snapshot, transaction/aborted snapshot, and leader-epoch index with canonical integrity.

Physical Object checkpoint pages and `WalRunSealRecord` remain physical-only. They never contain producer/transaction
state and cannot authorize a protocol checkpoint, ACK, omission of physical recovery, frontier advance, protection
release, or GC. `NWKCP1` is asynchronously discoverable under a separate exact WalRun Root sub-prefix and shares the
run's cumulative LIST/GET/decode/time recovery envelope. Missing/corrupt protocol checkpoints fall back to bounded
NWG1 suffix replay; exhaustion fails closed. A closed run has a complete terminal protocol-checkpoint vector without
adding logical fields to the physical Seal.

Async creation is allowed, but finite aggregate uncovered entries/bytes/age/time are mandatory. Exhaustion causes
checkpoint, backpressure, or rollover before ACKed recovery becomes unbounded.

Takeover first recovers the physical candidate tail, then applies the Kafka election adoption cut described below. WAL
replay reconstructs producer state, ongoing/completed/aborted transactions, first unstable offset, and leader-epoch
index. It does **not** independently recover final HW or claim the same LSO: native Kafka replica/election recovery
supplies HW, after which `LSO = min(HW, firstUnstableOffset)` is derived and HW/LSO are coherently published. Completed
transactions whose markers are not yet HW-covered remain in the native unreplicated/completed transaction state.
Transaction-coordinator metadata in `__transaction_state` remains coordinator authority and is distinct from these
partition-local indexes and markers.

### Election-bounded physical-tail adoption

Shared storage distinguishes:

```text
physicalRecoveredEndOffset
electedReplicaObservedEndOffset
replicaAppliedEndOffset
electionAdoptableEndOffset
```

`physicalRecoveredEndOffset` is only the greatest verified contiguous physical candidate. The native election path
derives `electionAdoptableEndOffset` from the elected replica's prior protocol observation and the exact clean/transfer
or unclean-election contract. Before admission, the candidate must apply through that boundary, and:

```text
newLeaderLEO = min(physicalRecoveredEndOffset, electionAdoptableEndOffset)
```

Physical bytes beyond the adoptable boundary are old-epoch inert/orphan residue. They do not enter locator coverage,
producer state, transaction state, or the leader-epoch index. A physical shortfall below a required adoptable boundary
fails closed unless the native unclean-election decision explicitly selected a lower truncation point.

- same-replica restart may recover only tail backed by its durable local observation/leader-epoch evidence;
- clean transfer first requires the target's Applied frontier to reach the declared transfer frontier;
- another ISR replica adopts at most its native election boundary and catches Applied up before serving;
- unclean election preserves Kafka's explicit data-loss/truncation semantics and never silently salvages extra shared
  bytes merely because the provider still contains them.

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
For `read_committed`, storage returns the exact protocol-native batch stream up to LSO plus the native aborted-
transaction metadata required by the Fetch response. Nereus does not silently rewrite this into a storage-only filter
that deletes aborted batches or control markers before Kafka response construction.

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

Byte-preserving materialization and Kafka compaction are separate operations. Materialization copies exact raw
RecordBatch bytes. Compaction may remove only some records, create sparse/empty batches or new batch boundaries, and
rewrite Kafka CRC/timestamp structures, but it must preserve logical offsets, producer sequence recovery, control
batch/coordinator-epoch semantics, transaction markers and aborted ranges, tombstone retention, and native
`ListOffsets`/timestamp behavior. It rebuilds the range, producer, transaction/aborted, leader-epoch, and timestamp
side indexes for the new generation. Exact-byte requirements do not apply to a compaction rewrite.

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

Non-compacting materialization preserves exact raw RecordBatch bytes plus range directory, producer-state checkpoint, transaction /
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

The 0.2 Kafka internal-topic Deployment policy fixes `__consumer_offsets` and `__transaction_state` to
`BOOKKEEPER_WAL_ONLY`. These low-latency, small-record, compaction-heavy coordinator topics do not initially pay Object
group linger or Object-recovery risk. A future change to `BOOKKEEPER_WAL_ASYNC_OBJECT` requires its own versioned policy
revision after internal-topic compaction generation, protocol checkpoint, Object fallback, coordinator restart, BK GC,
and marker parity evidence. No default for `__share_group_state` is inferred by this ADR; its explicit internal-topic
policy remains a fail-closed release gate rather than inheriting a user-topic default.

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
- Cost: partition-local producer/transaction speculation, fenced ordered multi-state publication, follower
  observation journaling/applied catch-up,
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

Required scenarios are `V2-KAF-DATA-001..022`. They cover out-of-order durability, predecessor failure, response-loss
retry, speculative producer state, checkpoint crash recovery, LEO/HW/LSO isolation, abort filtering, ISR shrink,
delayed Fetch, compaction gaps, pinned source generation, Object/BK fallback and GC, pre-admission rejection,
leader-epoch recovery, random/sequential full-batch reads, fence/publication races, election-bounded tail adoption,
Observed/Applied progress, native duplicate identity, Object protocol checkpoints, and partial-batch compaction.

This ADR refines ADRs 0009, 0011, 0031, 0067, 0069, and 0086. Exact wire and evidence-derived numeric bounds remain
under `V2-OPEN-BK-02`; they are not reopened as protocol semantic choices.
