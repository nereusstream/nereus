# 05 — Producer State, Transactions, Compaction and Retention

> 状态：F9-M4 all seven NKC1 canonical sections + strict V1 codecs/full composition + exact idempotent/transaction/control append encoding partial slices implemented；Kafka import/replay and request semantics remain in progress；F9-M5 designed target
> Recovery source：lossless `COMMITTED` bytes only
> Client compacted view：mandatory `TOPIC_COMPACTED` coverage + committed tail；never resurrect compacted records

## 1. Ownership decisions

| Concern | Owner | Storage form |
| --- | --- | --- |
| idempotent producer truth | committed Kafka RecordBatch bytes | Nereus stream |
| in-memory duplicate/sequence state | stock `ProducerStateManager` | rebuilt + checkpoint acceleration |
| partition transaction state / LSO | committed transactional/control batches | rebuilt + checkpoint |
| group coordinator truth | `__consumer_offsets` | ordinary F9 partition stream，Kafka compacted view |
| transaction coordinator truth | `__transaction_state` | ordinary F9 partition stream，Kafka compacted view |
| aborted transaction fetch index | derived from committed bytes | memory + NKC1 section |
| leader epoch/time/segment indexes | derived | memory + NKC1 sections |
| logical topic retention | Kafka config/DeleteRecords | Nereus stream trim |
| log compaction visibility | F4 NTC2 committed generation + binding coverage activation | sparse higher generation |
| physical source deletion | existing F4/F1-BK reference/GC protocols | never group-offset driven |

Native Kafka coordinator state is not duplicated into F9 Oxia records。Oxia binding/checkpoint pointers coordinate storage；
the internal topics remain replayable protocol truth。

## 2. Stock append ordering and post-commit failures

Locked Kafka `UnifiedLog.append` analyzes producer state before `localLog.append` but updates the state map、transaction
index and map-end offset only after append returns。F9 `NereusLocalLog.append` returns only after stable Nereus commit。

This gives three classes：

| Cut | Durable data | Kafka derived state | Action |
| --- | --- | --- | --- |
| validation/sequence error before local append | absent | unchanged | return stock client error |
| storage known-not-committed | absent | unchanged | timeout/backpressure per mapping |
| stable append then process/derived-index failure | present | possibly incomplete | treat `KNOWN_COMMITTED`，write-fence/replay |

After stable append，a failure updating ProducerStateManager/transaction index/virtual segment cannot be returned as a
normal non-committed retry while partition remains writable。`NereusUnifiedLog` catches this boundary，marks
`WRITE_FENCED_RECOVERY_REQUIRED`，and reopens from committed bytes。Client idempotence resolves retry after recovery。

The product storage boundary now enforces this ordering directly：durable completion advances only stable end/commit
version while retaining the previous HW/LSO；`publishDerivedOffsets(exactEnd, HW, LSO)` is required after stock producer/
transaction updates。Until that exact confirmation，the next same-partition append and `STABLE_APPEND` event remain
blocked。This closes the race where a transactional stable event could wake a read before first-unstable state existed。

## 3. `NereusProducerStateManager`

### 3.1 Responsibility

Target class extends the selected Kafka baseline `ProducerStateManager` only to replace snapshot file IO and hydrate a
verified state image。It does not change：

- sequence validation；
- producer epoch fencing；
- duplicate batch search；
- transaction verification guard semantics；
- producer ID expiration policy；
- first unstable offset calculation。

Any behavior change present only in the AutoMQ reference fork is not implicitly adopted。F9 compatibility compares against
the selected Apache Kafka baseline and records explicit patches separately。

### 3.2 Target methods

```java
// target shape
final class NereusProducerStateManager extends ProducerStateManager {
    ProducerStateSnapshot exportCanonical(long expectedMapEndOffset);
    void importCanonical(ProducerStateSnapshot snapshot);
    void resetForReplay(long logStartOffset);
    void applyRecoveredBatch(RecordBatch batch, AppendOrigin origin);
    void verifyMapEndOffset(long stableEndOffset);

    @Override Optional<File> takeSnapshot(boolean sync); // schedules NKC1 checkpoint; no truth-bearing file
}
```

The selected Kafka baseline needs one narrow inert stock-package constructor seam because `ProducerAppendInfo.appendDataBatch`
cannot represent a marker-updated `lastTimestamp` independently from the retained data-batch window：

```java
// org.apache.kafka.storage.internals.log.ProducerStateEntry
public static ProducerStateEntry fromSnapshot(
    long producerId,
    short producerEpoch,
    int coordinatorEpoch,
    long lastTimestamp,
    OptionalLong currentTxnFirstOffset,
    Collection<BatchMetadata> batchesOldestToNewest);
```

The factory requires at most `NUM_BATCHES_TO_RETAIN` non-null entries and copies them in supplied order without calling
`addBatch`，so it never overwrites the separately supplied `lastTimestamp`。It is storage-neutral and unused by the stock
path。`NereusProducerStateManager.importCanonical` converts each canonical batch directly to stock `BatchMetadata` and loads
this exact entry into a fresh manager；replaying synthetic data batches merely to populate the queue is forbidden because a
later transaction marker legitimately changes the producer timestamp without entering the duplicate window。Import must
still export byte-for-byte equal canonical state before publication。

`takeSnapshot` may return an ephemeral placeholder only if the Kafka signature requires it；restart never loads it。A sync
Kafka call means “state safely captured for the partition checkpoint coordinator” only when the caller explicitly requests
the F9 checkpoint barrier；ordinary producer append does not wait for object upload。

### 3.3 Producer-state NKC1 section V1

Big-endian canonical layout：

```text
sectionVersion:u16 = 1
mapEndOffset:i64
producerCount:u32
for producer sorted ascending producerId:
  producerId:i64
  producerEpoch:i16
  coordinatorEpoch:i32
  lastTimestamp:i64
  hasCurrentTxn:u8
  currentTxnFirstOffset:i64          present iff flag=1
  batchCount:u8                     0..5
  for batch oldest -> newest:
    lastSequence:i32
    lastOffset:i64
    offsetDelta:i32
    timestamp:i64
```

Validation：unique/sorted producer IDs；offset delta non-negative；first/last offsets within retained committed history or
valid pre-trim snapshot state；sequence arithmetic uses Kafka wrap rules；current transaction offset `< mapEndOffset`；
timestamps use Kafka sentinel allowlist。`lastTimestamp` is the stock producer-entry timestamp and may come from a later
commit/abort marker；the retained duplicate window contains data-batch metadata only，so equality with the newest retained
batch timestamp is neither required nor generally true。No `VerificationStateEntry` is persisted；transaction coordinator
verification is re-established by stock protocol。

`mapEndOffset == NKC1.checkpointOffset`。Import into a fresh manager only；merging with existing state is forbidden。

Current implementation（2026-07-27）：product side now owns the Kafka-artifact-neutral
`KafkaProducerTransactionState`、`KafkaProducerTransactionStateCodecV1` and deterministic
`f9ProducerStatePropertyTest` gate。The model preserves at most five batches in oldest-to-newest order，uses Kafka
sequence wrap arithmetic，requires producer IDs to be strictly sorted and requires `mapEndOffset` to equal the outer
checkpoint offset。The codec is strict big-endian and rejects count/length/flag/version/trailing-byte ambiguity before a
fork can import it。The local Kafka worktree contains `NereusProducerStateManager`/`NereusTransactionIndex` import/export
and replay scaffolding，but its current `ProducerAppendInfo`-based hydration cannot preserve an independent marker-updated
`lastTimestamp`；the `ProducerStateEntry.fromSnapshot` seam and exact restore regression above remain required before that
work can be committed or counted as stock import/replay evidence。

## 4. Transaction state and indexes

### 4.1 Data truth

Transactional data batch、commit marker and abort marker are exact Kafka batches in the same partition stream。Nereus does
not create a side transaction log。Stable append ordering guarantees a marker is not acknowledged before its bytes commit。

### 4.2 Open-transaction section V1

Although `currentTxnFirstOffset` appears per producer，a separate canonical section preserves the ordered first-unstable
structure needed to rebuild LSO without depending on Java collection internals：

```text
sectionVersion:u16 = 1
transactionCount:u32
for transaction sorted (firstOffset, producerId):
  producerId:i64
  firstOffset:i64
  hasLastOffset:u8
  lastOffset:i64                     present iff completed but not finalized in snapshot cut
```

Cross-check every entry with producer section。At a normal checkpoint barrier，completed-but-not-finalized entries should be
zero；the current decoder rejects them。A future crash-consistent captured boundary may support them only after an explicit
section flag is assigned and frozen；accepting an unflagged entry is forbidden。

### 4.3 Aborted transaction section V1

```text
sectionVersion:u16 = 1
entryCount:u32
for entries sorted by strictly increasing lastOffset:
  kafkaAbortedTxnVersion:i16
  producerId:i64
  firstOffset:i64
  lastOffset:i64
  lastStableOffset:i64
```

Fields match Kafka `AbortedTxn` semantics，not its Java buffer layout。Entries must be non-overflowing、within checkpoint
history，and monotonic for search。`lastStableOffset` is non-negative and may be lower than `lastOffset` when another earlier
transaction remains unstable；it must not be validated as the marker's successor。Entries with
`lastOffset < current logStartOffset` may be pruned at checkpoint creation。The current product codec implements this
canonical payload and requires strictly increasing marker offsets，matching stock `TransactionIndex.append`；the fork-side
`TransactionIndex` import/filter bridge remains open。

### 4.4 `NereusTransactionIndex`

It stores verified `AbortedTxn` values in an immutable/snapshot-friendly ordered structure plus append tail。Methods used by
Kafka retain signatures：

```text
append(AbortedTxn)
collectAbortedTxns(fetchOffset, upperBoundOffset)
allAbortedTxns()
reset()/close()
```

`append` is an in-memory derived update after stable log append；checkpoint persists it asynchronously。Any invariant failure
write-fences the partition。No independent stream/index append is part of producer acknowledgement。

### 4.5 Recovery replay

For every COMMITTED Kafka batch from checkpoint to stable end：

1. validate exact Nereus range vs Kafka base/last；
2. validate Kafka CRC and records；
3. feed stock producer-state recovery logic；
4. assign leader epoch start where message format supports it；
5. detect completed transactions/control markers；
6. append aborted facts and update open transactions/LSO；
7. update virtual segment/time/logical-byte indexes；
8. advance mapEndOffset only to batch end。

At completion：producer map end、local LEO、stable head end equal；LSO `<= HW`；all open transaction offsets are within
`[logStart,stableEnd)` or explicitly preserved by checkpoint semantics。Any mismatch is CORRUPT_OFFLINE。

## 5. Native coordinator internal topics

`__consumer_offsets` and `__transaction_state` are created by native Kafka/KRaft and bound exactly like user topics：

- same topicId-based stream identity；
- same authority session/stable append；
- same recovery/checkpoint；
- cleanup policy `compact` (plus Kafka-defined retention behavior)；
- same NTC2 compaction implementation；
- coordinator election only after log recovery completes。

Consumer group committed offsets do **not** create Nereus cursor/reference records and never protect user-topic retention。
Offset expiration/tombstones are written by native group coordinator to `__consumer_offsets`；compaction removes obsolete
coordinator keys through Kafka semantics。

Transaction coordinator recovery reads `__transaction_state` through the mandatory compacted view。If NTC2 coverage for an
activated internal-topic range is unavailable，coordinator election fails；it may not fall back to full COMMITTED bytes and
resurrect compacted state。

## 6. Leader epoch section

Canonical NKC1 V1：

```text
sectionVersion:u16 = 1
entryCount:u32
for entries sorted startOffset:
  leaderEpoch:i32
  startOffset:i64
```

Epoch and start offset strictly increase；first retained entry may start before logStart only when required to answer epoch
queries；V1 uses its unique first position plus the outer logStart bound as the explicit carried-forward marker，and no
later entry may start below logStart。An entry may start exactly at stable end to retain the current leader epoch before its
first batch。Tail replay adds/extends entries。`endOffsetForEpoch` derives end from next start or stable end。No local
leader-epoch checkpoint file is loaded as truth。

Current implementation（2026-07-27）：`KafkaLeaderEpochState` and `KafkaLeaderEpochStateCodecV1` implement section 3
without Kafka artifact types。The codec freezes the documented big-endian bytes and rejects missing/duplicate sections、
non-required or non-V1 headers、unsigned-count overflow、truncation、trailing bytes、non-monotonic epochs/offsets and
checkpoint-bound mismatches。A frozen digest plus 200 deterministic randomized round trips are in the partial M4 gate。

## 7. Virtual segments

### 7.1 Why segments remain

Kafka configs and APIs depend on segment roll for retention、time lookup、compaction boundaries and metrics。F9 keeps
virtual segment descriptors but stores all record bytes in one Nereus stream。A segment is an offset/byte/time interval，not a
data object or stream。

### 7.2 Descriptor V1

```text
segmentId = sha256(partitionIdentity + baseOffset + rollSequence)
baseOffset:i64
endOffset:i64                       exclusive；stable end for active
rollSequence:i64
createdAtMillis:i64
closedAtMillis:i64                  0 while active
rollJitterMillis:i64                chosen once for this segment
largestTimestamp:i64
maxTimestampOffset:i64
logicalBytes:i64                    exact Kafka batch bytes
firstCumulativeBytes:i64
lastCumulativeBytes:i64
configDigest[32]
rollReasonId:i32
stateId:i32                         ACTIVE=1, CLOSED=2
```

Descriptors sorted by base；ranges dense from retained segment floor to stable end；exactly one ACTIVE last descriptor。
`logicalBytes == lastCumulative-firstCumulative`；cumulative byte ranges are also dense。Closed segments are non-empty
and have `closedAtMillis > 0` and `closedAtMillis >= createdAtMillis >= 0`；the active segment has `closedAtMillis=0` and may be empty exactly at
stable end。Non-empty timestamp offsets remain inside `[baseOffset,endOffset)`。roll wire IDs：INITIAL=1、SIZE=2、
TIME=3、RELATIVE_OFFSET_OVERFLOW=4、INDEX_FULL=5、CONFIG=6、MANUAL=7、TEST=8；unknown fail closed。

### 7.3 Roll protocol

Stock `UnifiedLog.maybeRoll` executes before data append。F9：

1. decide roll using current descriptor and stock config；
2. create a pending in-memory next descriptor at expected first batch base；
3. append bytes stable；
4. only then finalize old/new descriptor facts and make them checkpoint-eligible；
5. if append fails known-not-committed，discard empty pending descriptor；
6. if outcome unknown，replay head determines whether new segment contains data；empty durable/local hints are discarded。

No root CAS is required per roll in the hot path；committed bytes plus previous checkpoint deterministically reconstruct
descriptors using batch offsets/timestamps and captured config epochs。To make dynamic config replay deterministic，runtime
maintains a bounded `KafkaLogConfigHistory` in the partition checkpoint and KRaft metadata offsets；a config delta forces a
checkpoint before old history can be pruned。

### 7.4 Segment/checkpoint section

NKC1 section 4 canonical payload：

```text
payloadVersion:u16 = 1
segmentCount:u32
segment[segmentCount]                fields in 7.2, including rollJitterMillis
configCount:u32
for config entries sorted metadataOffset, effectiveFromOffset nondecreasing:
  metadataOffset:i64
  effectiveFromOffset:i64
  segmentBytes:i64
  segmentMs:i64
  segmentJitterMillis:i64
  segmentIndexBytes:i32
  indexIntervalBytes:i32
  retentionBytes:i64                 -1 means unbounded
  retentionMs:i64                    -1 means unbounded
  fileDeleteDelayMs:i64
  deleteRetentionMs:i64
  minCompactionLagMs:i64
  maxCompactionLagMs:i64
  minCleanableDirtyRatioBits:i64      canonical IEEE-754 double bits
  cleanupPolicyFlags:i32             DELETE=1, COMPACT=2
  configDigest[32]
```

`configDigest = SHA-256("NEREUS_NKC1_LOG_CONFIG_V1\0" + big-endian effective config fields)`；metadata/effective
offsets are deliberately excluded so equal effective configs have equal digests。Every descriptor digest must resolve to
one retained history entry whose `effectiveFromOffset <= baseOffset`，and
`rollJitterMillis <= segmentJitterMillis`。The active config may be a later history entry
than the descriptor creation config，so history is ordered by KRaft metadata offset and separately records the exact
durable offset from which it took effect。No arbitrary `Properties` map、enum ordinal、float text or reflection layout is
encoded。

Current implementation（2026-07-27）：`KafkaVirtualSegmentState` validates dense offset/cumulative-byte ranges、strict
base/roll/time order、exact ACTIVE/CLOSED lifecycle、timestamp bounds、config-digest references and jitter bounds。
`KafkaVirtualSegmentStateCodecV1` rejects bad required/version/flags、unsigned counts、truncation、trailing bytes、
unknown reason/state IDs and non-canonical config digests before publication。A frozen section digest、targeted
corruption/invariant cases and 200 deterministic randomized round trips are in the partial M4 gate。

## 8. Time and logical-byte indexes

### 8.1 Time index section

Per segment，store sparse entries matching stock index interval：

```text
payloadVersion:u16 = 1
segmentCount:u32
for segments sorted segmentBaseOffset:
segmentBaseOffset:i64
entryCount:u32
for entries with strictly increasing offset and nondecreasing timestamp:
  timestamp:i64
  offset:i64
```

Every time-index `segmentBaseOffset` must exist in section 6。A time-index segment may be absent or have zero entries。
Only the first section-6 segment may carry a base below outer `logStartOffset`；entries themselves are in
`[max(segmentBaseOffset, logStartOffset), stableEndOffset)`。Negative fields、duplicate/unordered segments or entries、
unknown flags/version、unsigned-count overflow、truncation and trailing bytes fail closed。

Timestamp lookup chooses floor/candidate then scans exact COMMITTED batches to return first record timestamp `>= target`。
Index never returns final answer without payload verification。

### 8.2 Logical-byte position section

Used for size retention and Kafka `LogOffsetMetadata`：

```text
payloadVersion:u16 = 1
segmentCount:u32
for segments sorted segmentBaseOffset:
segmentBaseOffset:i64
segmentLogicalBytes:i64
sampleCount:u32
for samples at the configured index interval:
  entryStartOffset:i64
  cumulativeLogicalBytes:i64
```

Segment bases strictly increase and are at most outer `stableEndOffset`；only the first may be below
`logStartOffset`。A segment based exactly at `stableEndOffset` represents the current empty segment and therefore has
`segmentLogicalBytes=0` and no samples。Every sample offset is strictly increasing and in
`[max(segmentBaseOffset, logStartOffset), stableEndOffset)`；cumulative bytes are strictly increasing and in
`[0, segmentLogicalBytes)`。An empty segment cannot contain samples。

Missing sample falls back to message-offset-only metadata or bounded scan。It never controls data visibility。

Current implementation（2026-07-27）：`KafkaDerivedIndexState` is the shared Kafka-artifact-neutral image for section
5/6，so cross-section segment references and outer checkpoint bounds cannot be decoded independently into inconsistent
state。`KafkaDerivedIndexStateCodecV1` emits both required big-endian sections together and rejects missing/duplicate/
optional/non-V1 headers、malformed counts、truncation、trailing bytes and all model invariant violations。The partial
M4 gate includes a frozen combined digest、targeted corruption/cross-section tests and 200 deterministic randomized
decode/re-encode round trips。`KafkaCanonicalCheckpointState` additionally requires section 4/6 segment sets and logical
bytes to match exactly，and prevents section 5/6 entries from crossing their virtual segment；its composition codec emits
all seven required sections in wire-ID order with a frozen combined digest。Canonical publication request construction is
implemented；runtime index construction、staging/trigger composition and restart/takeover behavior remain pending。

## 9. Retention semantics

### 9.1 Policy inputs

`KafkaRetentionCoordinator` consumes one immutable snapshot：

```text
partition identity + binding version
current KRaft config metadata offset/digest
logStart/HW/LSO/stable end
virtual closed segments
latest verified checkpoint offset/ref
stream metadata/profile
current authority session
now from injected clock
```

It implements Kafka `retention.ms`、`retention.bytes` and cleanup-policy delete behavior over **closed** virtual segments。
Group offsets/cursors are not inputs。

### 9.2 Candidate calculation

- time：delete oldest closed segments whose largest timestamp/roll facts meet stock predicate；
- size：subtract oldest closed segment logical Kafka bytes until retained size <= configured value；
- `compact,delete` applies both compaction visibility and deletion retention；
- active segment never selected；
- candidate is next retained segment base，so scheduled retention is an entry boundary；
- candidate capped at HW/stable end and cannot decrease log start。

Policy computation is deterministic for the frozen snapshot。Before mutation re-read binding/head/config/authority and recompute
or abort on change。

The executable product boundary is deliberately Kafka-artifact-neutral：

```java
KafkaRetentionPlanner.Plan plan(KafkaRetentionPlanner.Snapshot snapshot);

record Snapshot(
    KafkaVirtualSegmentState virtualSegments,
    KafkaRetentionPlanner.Policy policy,
    long lastStableOffset,
    long highWatermark,
    long nowMillis) {}

record Policy(
    long metadataOffset,
    Checksum configDigest,
    long retentionBytes,
    long retentionMs,
    int cleanupPolicyFlags) {}
```

`Policy` must equal the final section-4 config-history entry by both metadata offset and canonical digest。For
`retention.ms` the exact stock predicate is `nowMillis - segmentRetentionTimestamp > retentionMs`（strict `>`）；
`segmentRetentionTimestamp` is `largestTimestamp` when records exist and the frozen close-time roll fact otherwise。
Evaluation stops at the first non-expired segment。For `retention.bytes`，the planner sums the logical bytes of all
currently retained descriptors（including the complete first descriptor when logStart is inside it），computes
`excess=max(total-retentionBytes,0)` and removes the oldest consecutive closed segments while the next whole segment fits
inside `excess`。This matches stock segment-size behavior and never estimates physical object bytes。

The time and size prefix counts are evaluated against the same immutable state and the farther prefix wins，which is
equivalent to stock size-then-time deletion because both predicates operate only on the oldest consecutive prefix。
Every selected descriptor must end at or below `min(HW, stableEnd)`；the final ACTIVE descriptor is excluded even for
`retention.bytes=0`。The plan freezes previous/candidate logStart、selected count/bytes、both prefix counts、ordered
TIME/SIZE reasons、config offset/digest and evaluation time。Its trim reason is canonical：

```text
KAFKA_RETENTION_V1:<TIME|SIZE|TIME+SIZE>:
  config=<metadataOffset>/<configSha256>:
  from=<old>:to=<candidate>:segments=<count>:bytes=<logicalBytes>:now=<millis>
```

Current implementation（2026-07-27）：`KafkaRetentionPlanner` implements the contract above。Its deterministic test covers
strict time equality、time+size union、exact size excess、HW boundary、compact-only policy、zero-byte-retention active
segment protection and config-history mismatch。The Kafka differential oracle and scheduler/coordinator integration remain
required before KF-RET-001/002/003 can be marked complete。

### 9.3 Checkpoint-before-trim barrier

Before advancing to candidate `T`：

1. require a verified NKC1 checkpoint with `checkpointOffset >= T` and state sufficient to recover after removing `<T`；
2. if absent，publish checkpoint at current stable end；
3. revalidate checkpoint root/reference and current head；
4. call `StreamStorage.trim(streamId,T, reason="KAFKA_RETENTION_V1:...")`；
5. response loss reloads stream head；success only when `trimOffset >= T`；
6. update local logStart/binding observed facts；
7. physical GC proceeds separately through existing protections。

If checkpoint publication fails，retention pauses；it does not trim and hope producer state can be reconstructed。

The product barrier has the following executable ports：

```java
CompletableFuture<KafkaTrimBarrier.Result> advance(
    KafkaTrimBarrier.Snapshot captured,
    KafkaRetentionPlanner.Plan exactPlan);

CompletableFuture<KafkaTrimBarrier.Result> advanceDeleteRecords(
    KafkaTrimBarrier.Snapshot captured,
    long normalizedRequestedOffset);

interface CheckpointGate {
    CompletableFuture<VerifiedCheckpoint> ensureVerified(Snapshot snapshot, long targetOffset);
}
interface SnapshotLoader {
    CompletableFuture<Snapshot> loadCurrent();
}
interface DurableTrimListener {
    CompletableFuture<Void> onDurableTrim(
        Snapshot revalidated, long durableTrimOffset, KafkaCheckpointReferenceRecord checkpoint);
}
```

The captured snapshot requires one exact ACTIVE binding、stream head、Kafka leader append authority、
`logStart/LSO/HW/stableEnd` view and planner input。`advance` recalculates and byte/value-compares the supplied plan before
calling the checkpoint gate。The returned reference must have `checkpointOffset >= T`、
`logStartOffsetAtCheckpoint <= captured trim` and an object SHA equal to the verifier result。After publication/selection
the barrier reloads the whole snapshot；partition incarnation、stream/payload/profile、leader/broker term、append authority、
current config offset/digest and the exact checkpoint root must still match，and the recomputed candidate must still reach
`T`。A later append or HW advance is allowed only through this recomputation；leadership/config changes abort before trim。

`StreamStorage.trim` is followed by `getStableHeadSnapshot` even when trim returned success。If trim failed or its response
was lost，the operation is nevertheless successful only when the reloaded durable `trimOffset >= T` under the same append
authority and a monotonic commit head。Only then is `DurableTrimListener` invoked to advance the process-local
`UnifiedLog.logStartOffset` and binding observed facts。A listener failure is retryable after a durable trim；the next attempt
observes `trimOffset >= T` and follows the already-applied path without a second mutation。

Current implementation（2026-07-27）：`KafkaTrimBarrier` implements these validation and response-loss rules，with tests for
checkpoint failure、insufficient/unrooted checkpoint、config race、normal trim、applied-but-response-lost and unapplied
failure。`KafkaRetentionCheckpointGate` scans rooted references newest first，skips offsets that cannot cover the candidate，
falls back only from confirmed not-found/checksum/format/invariant failures，pauses on transient metadata/storage failure，
and otherwise requires a newly published checkpoint at the captured stable end。`KafkaRetentionCoordinator.runOnce`
loads one frozen snapshot、returns without entering the barrier on a no-op plan and coalesces concurrent scheduler triggers
behind a cancellation-isolated future。`KafkaRetentionCheckpointServices` supplies the verifier/publisher composition
through pinned
`KafkaCheckpointRecoveryCoordinator.recoverReference`、the canonical publication factory/coordinator and an authoritative
binding-root reload。Its `CaptureProvider` remains the partition-lock seam that supplies the exact source/canonical image。
The existing local-file object-store integration test now publishes a canonical seven-section object through these services，
reloads its authoritative root and verifies the exact reference through a released reader pin。
`KafkaRetentionDurableTrimListener` publishes monotonic observed logStart through binding CAS before invoking the exact
local leader updater，recovers applied-but-response-lost CAS by reload and refuses changed leader terms。Periodic process
scheduling、the concrete partition capture/local-log updater and Kafka-fork invocation remain pending，so this is
deterministic partial M5 evidence rather than an end-to-end retention claim。

### 9.4 DeleteRecords

`Partition.deleteRecordsOnLeader` keeps stock validation：internal topic rejection、requested offset conversion (`-1` → HW)、
range/policy checks。F9 then：

1. choose exact requested logical offset，which may fall inside a Kafka batch；
2. enforce checkpoint-before-trim；
3. stable Nereus trim；
4. update `UnifiedLog.logStartOffset` under partition lock；
5. return RF1 low watermark equal to durable trim；
6. wake fetch operations with new log start。

Containing-entry read may physically retrieve a batch beginning before logStart，but `KafkaFetchAssembler` filters/rejects
records below current logStart。Requests below logStart get stock `OFFSET_OUT_OF_RANGE`。

The product boundary does not import Kafka server classes：

```java
CompletableFuture<KafkaDeleteRecordsCoordinator.Result> deleteTo(
    KafkaTrimBarrier.Snapshot captured,
    long normalizedRequestedOffset);

record Result(
    long requestedOffset,
    long durableLowWatermark,
    Optional<KafkaTrimBarrier.Result> trimResult) {}
```

`Partition.deleteRecordsOnLeader` remains responsible for the stock `cleanup.policy` check，leader check and
`DeleteRecordsRequest.HIGH_WATERMARK (-1)` conversion while holding the partition read lock。It captures the exact
`KafkaTrimBarrier.Snapshot` in that critical section，releases the lock before waiting，and passes only a non-negative
normalized offset。The product coordinator repeats the safety-critical checks against the frozen section-4 policy/HW：
`cleanup.policy` must contain `delete` and `requestedOffset <= captured HW`。A request at or below the current durable
logStart is an idempotent success without checkpoint publication or another trim；the returned RF1 low watermark is the
current durable logStart，not the lower requested value。

An advancing request calls the same checkpoint gate and durable-trim state machine as retention through
`KafkaTrimBarrier.advanceDeleteRecords`。Unlike retention，the target is not rounded to a virtual-segment or batch boundary；
the exact logical offset is legal even inside a containing entry。Before mutation the barrier requires the selected NKC1
reference to remain rooted，requires the KRaft config offset/digest and delete policy to remain identical，revalidates the
same partition incarnation/leader/broker/append authority and checks the target is still at or below current HW/stable end。
It never recomputes a retention candidate for this path。The canonical storage reason is：

```text
KAFKA_DELETE_RECORDS_V1:
  config=<metadataOffset>/<configSha256>:
  from=<capturedLogStart>:to=<requestedOffset>:
  leader=<leaderId>/<leaderEpoch>:brokerEpoch=<brokerEpoch>
```

After `StreamStorage.trim`，the common response-loss reload and `KafkaRetentionDurableTrimListener` publish the durable
binding logStart before the fork callback advances `UnifiedLog` and wakes delayed Fetch。The result returns the reloaded
durable trim as RF1 low watermark，so a concurrent farther trim is reported monotonically。

Current implementation（2026-07-27）：`KafkaDeleteRecordsCoordinator` and the shared barrier path implement the product
contract above。Deterministic tests cover an exact mid-segment target、normalized HW、already-deleted idempotence、
negative/unconverted and above-HW rejection、compact-only policy rejection and config-race abort before mutation。
Kafka-fork `Partition` capture/invocation、exact local log-start update/fetch wake-up and stock batch-start/middle/end/HW
integration remain required before KF-RET-006 is complete。

### 9.5 Trim vs materialization

Logical trim is immediate visibility truth；physical source release waits for F4 task/generation/checkpoint/reader/reference
proofs。Retention cannot directly delete Object WAL/BookKeeper ledger/NCP2/NTC2 objects。A protected in-flight
materialization may finish or be retired by existing recovery logic。

## 10. Compaction architecture

### 10.1 Stock cleaner is disabled

`LogCleaner` assumes local segments can be replaced。F9 never invokes it for Nereus partitions and never writes cleaner
checkpoint files。Dynamic cleaner config is mapped into `KafkaCompactionPolicy` consumed by F4 planner/worker。

### 10.2 New SPI and classes

Protocol-neutral additions in `nereus-materialization`：

```java
interface RangedTopicCompactionCodec {
    String codecId();
    long codecVersion();
    Checksum messageFormatSha256();
    void decode(ReadBatch rangedBatch, DecodedRecordConsumer consumer);
    RewrittenCompactionRecord rewrite(
        DecodedCompactionRecord survivor,
        CompactionRewriteContext context);
}

record DecodedCompactionRecord(
    long absoluteOffset,
    KeyKind keyKind,                 // KEYED / UNKEYED / CONTROL
    ControlKind controlKind,         // NONE / COMMIT / ABORT
    int coordinatorEpoch,            // -1 for data
    ByteBuffer taggedCompactionKey,
    boolean tombstone,
    OptionalLong eventTimeMillis,
    OptionalLong deleteHorizonMillis,
    long sourceBatchBaseOffset,
    int sourceRecordIndex,
    Checksum sourceBatchSha256,
    boolean transactional,
    long producerId,
    short producerEpoch,
    int sequence,
    ByteBuffer rewriteToken) {}

record CompactionRewriteContext(
    byte targetMagic,
    Checksum messageFormatSha256,
    boolean allowUncompressedFallback,
    OptionalLong deleteHorizonMillis) {}

record RewrittenCompactionRecord(
    long absoluteOffset,
    Disposition disposition,
    ByteBuffer taggedCompactionKey,
    ByteBuffer exactPayload,
    int payloadCrc32c,
    long sourceBatchBaseOffset,
    int sourceRecordIndex,
    Checksum sourceBatchSha256,
    OptionalLong eventTimeMillis) {}
```

Kafka-aware implementation in `nereus-kafka-adapter`：

```text
KafkaTopicCompactionCodecV1          strict decode + one-record rewrite
KafkaCompactionStrategyV1
KafkaCompactionPassOneCollector
KafkaCompactionTwoPassExecutor
KafkaCompactionRowMapper
KafkaCompactionWriteRequestFactory
KafkaCompactionPolicyProvider
KafkaCompactionPlanner
KafkaCompactionPlan
KafkaCompactionPlanCodecV1
KafkaCompactionCoverageCoordinator
KafkaCompactedFetchPlanner
```

The older F4 `TopicCompactionDecoder`/`CompactionRecord` contract assumes one source entry equals one logical record and is
not legal for F9 Kafka batches。The ranged SPI emits every logical record and keeps Kafka types module-local。Its tagged key
must keep keyed-empty、unkeyed and control namespaces distinct；source batch SHA + base + dense record index bind the opaque
rewrite token to the exact committed batch。Producer facts use Kafka sentinels only as a complete tuple；control markers are
transactional、producer-bound、carry explicit COMMIT/ABORT plus coordinator epoch and use sequence `-1`。Unknown control
types fail decode。All byte buffers are immutable copies。`RewrittenCompactionRecord`
requires non-empty fetchable protocol bytes and verifies payload CRC32C in its constructor。

Current implementation（2026-07-27）：these four protocol-neutral contracts are implemented。
`KafkaTopicCompactionCodecV1` is the first adapter implementation and freezes identity
`kafka-topic-compaction-codec-v1/1` plus message-format SHA-256
`2940e62ac155a477052b955c1b30a2e7e77862bb7383d240b792ca064f472104`。It requires exactly one complete valid
magic-v2 batch whose base/next/count equal the `ReadBatch` range，decompresses through Kafka 3.9 client wire code，validates
every record and emits KCK2 keyed/null/control identities with exact source SHA/index and owned batch bytes as the ephemeral
rewrite token。The main adapter therefore has an explicit `kafka-clients` implementation dependency；Kafka server classes
remain absent。

### 10.3 Plan ranges

One deterministic `KafkaCompactionPlan` freezes：

```text
outputCoverage      [start,end) closed virtual segments to rewrite
decisionHorizon     [start,horizonEnd) where horizonEnd = frozen stable end
sourceSetIdentity   exact COMMITTED targets for both ranges
binding/config/policy versions
min/max compaction lag facts
deleteRetention cutoff
current mandatory coverage state
decoder/strategy/key/rewrite/message-format digests
```

Decision horizon includes newer records beyond output coverage so an older key can be removed when a newer key exists in
the tail。New appends after frozen horizon may leave an extra old value，which is safe and removed by a later task；they never
cause premature removal。

Range selection is a pure first stage：

```java
KafkaCompactionPlanner.Candidate select(KafkaCompactionPlanner.Snapshot snapshot);

record Snapshot(
    KafkaVirtualSegmentState virtualSegments,
    KafkaCompactionPlanner.Policy policy,
    Optional<MandatoryCoverage> mandatoryCoverage,
    long lastStableOffset,
    long highWatermark,
    long nowMillis) {}

record Candidate(
    OffsetRange outputCoverage,
    OffsetRange decisionHorizon,
    int selectedSegmentCount,
    Policy policy,
    Optional<MandatoryCoverage> previousMandatoryCoverage,
    long evaluatedAtMillis) {}
```

The current policy must equal the final section-4 config-history entry。`firstDirty` is current logStart when no mandatory
coverage exists，otherwise the exact mandatory end。The planner walks the consecutive closed descriptors intersecting
that offset and stops before the ACTIVE descriptor，before a descriptor ending beyond LSO，or at the first descriptor whose
`largestTimestamp > now - min.compaction.lag.ms`（strict stock predicate；close time is fallback when there is no record
timestamp）。The output therefore ends only at a closed segment boundary at or below LSO。`max.compaction.lag.ms` is frozen
for scheduler priority but does not override the minimum-lag uncleanable boundary。The decision horizon is
`[firstDirty, stableEnd)`；later transaction strategy must exclude unsafe open/aborted facts。

Current implementation（2026-07-27）：`KafkaCompactionPlanner` implements this candidate stage and validates the immutable
mandatory coverage tuple (start/end、activation epoch、generation/policy SHA)。Tests cover LSO boundary、strict min-lag
equality/first-young stop、mandatory-end resume、active exclusion、delete-only no-op and config/coverage mismatch。
Protocol-neutral `ExactSourceSet`/`ExactSourceSetVerifier` now canonicalize a gap-free source list and SHA，then require every
batch in both passes to match the frozen range、generation/commit version、target identity、payload/schema/projection and
per-source count/byte accounting。`KafkaCompactionTwoPassExecutor` additionally requires the output source set to be the
exact prefix of the decision source set，so re-resolution to byte-equivalent alternate targets fails closed。
`ExactSourceSetCodecV1` now provides a strict bounded `EXS1` image containing every canonical physical target and accounting
field。`KafkaCompactionPlan`/`KafkaCompactionPlanCodecV1` link the exact output `MaterializationTask` to the full decision
horizon and freeze binding version、LSO/HW、config/mandatory-coverage facts、transaction/marker snapshot、resource caps and
all strategy/key/rewrite/message-format identities in a deterministic `kcp1-*` id。Round trip is byte-stable；digest、
target、enum、length、trailing-byte or task-link drift fails closed，and both durable images are capped at 64 KiB before any
metadata write。`KafkaCompactionPlanRecordMapper` wraps at most 60 KiB of KCP1 in a SHA-verified
`KafkaCompactionPlanRecord`；`KafkaCompactionPlanMetadataStore` persists it under the partition keyspace with immutable
create/idempotent reread/exact-version delete semantics，and rejects a same-ID byte conflict。

`DefaultCommittedSourceSetResolver` now resolves the decision range from the authoritative COMMITTED generation index：

```java
CompletableFuture<CommittedSourceSetResolution> resolve(
    StreamId streamId,
    OffsetRange exactCoverage);

CompletableFuture<Void> revalidate(
    CommittedSourceSetResolution expected);

CompletableFuture<KafkaCompactionSourceResolver.ResolvedSources> resolve(
    StreamId streamId,
    KafkaCompactionPlanner.Candidate candidate,
    MaterializationPolicy outputPolicy);
```

The generic resolver scans at most 4096 candidate edges in pages of at most 1000，maps only COMMITTED candidates below the
current head and searches a bounded 4096-state DAG for one deterministic exact path。Edges must start/end on the requested
boundaries，remain gap-free，preserve cumulative-byte continuity and have non-decreasing commit versions；a trim-straddling
source is never clipped。For equivalent complete paths the stable order is fewer physical sources，then higher generation、
farther edge end and canonical UTF-8 index key。

The paginated scan is deliberately not called a snapshot transaction。After selection，every source is reread by
`(stream, COMMITTED, offsetEnd, generation)` and compared byte-for-byte with its index version/SHA/physical target/accounting。
The resolver reloads stream and registration authority，requires ACTIVE/SEALED object-materialization profile、matching
projection and retained bounds，and allows only append-only head/hint progress that cannot change the frozen range。Source
removal、profile/policy/projection drift or trim entering coverage fails closed。`KafkaCompactionSourceResolver` additionally
requires KAFKA_RECORD_BATCH sources，derives the output set as an exact prefix ending at `outputCoverage.endOffset` and creates
the deterministic `TOPIC_KEY_COMPACTION` task。Its mutation guard revalidates this source/head proof before the caller's
binding/leader guard；`MaterializationTaskStore` then performs its own exact per-generation checks immediately before task
mutation。A range-only `Candidate` is therefore still not publication authorization。

### 10.4 Two-pass algorithm

Pass 1 over decision horizon：

- strict decode every Kafka batch；
- emit one logical record per Kafka record with absolute offset；
- external-sort/spill `KCK2 key -> greatest eligible offset/facts` under shared budgets；
- null-key/control identities are unique by offset；
- collect transaction/control horizons and tombstone timestamps；
- source checksum/count must match plan。

Pass 2 over output coverage：

- decode again and compare each record to retained fact；
- apply Kafka tombstone/transaction/control eligibility；
- rewrite survivor to valid Kafka batch，initially one normal record per batch；
- write ordered sparse NTC2 rows；
- verify output/source counts、offset order、Kafka CRC and full Parquet format；
- publish F4 TOPIC_COMPACTED generation；
- activate mandatory coverage separately。

Spill files private、bounded、checksummed and deleted on terminal path。Worker restart uses durable task identity，not partial
spill data。

The current bounded production composition has these executable boundaries：

```java
KafkaCompactionPassOneCollector(
    Snapshot snapshot,
    StagingFileManager stagingFiles);
void accept(DecodedCompactionRecord record);
Facts finish();

KafkaCompactionTwoPassExecutor(
    KafkaTopicCompactionCodecV1 codec,
    KafkaCompactionStrategyV1 strategy,
    KafkaCompactionRowMapper rowMapper,
    Limits limits,
    StagingFileManager stagingFiles);

Prepared KafkaCompactionTwoPassExecutor.prepare(
    Snapshot snapshot,
    ExactSourceSet decisionHorizonSources,
    Iterable<ReadBatch> decisionHorizonBatches);

Result Prepared.rewrite(
    ExactSourceSet outputCoverageSources,
    Iterable<ReadBatch> outputCoverageBatches,
    boolean allowUncompressedFallback);

KafkaTopicCompactedObjectWriteRequest KafkaCompactionWriteRequestFactory.create(
    KafkaCompactionWriteRequestFactory.Input frozenTaskInput,
    KafkaCompactionTwoPassExecutor.Result verifiedResult);
```

`Snapshot` freezes output/decision ranges、transaction-state end、planning time、delete retention、complete aborted/open
transaction ranges、one decision for every scanned control marker and record/key/in-memory budgets。Pass 1 requires a dense
logical-offset scan，excludes ABORTED and OPEN data from the winner index，fails if OPEN crosses output coverage，and
produces domain-separated SHA-256 digests for the full horizon and output prefix。This matches stock
`Cleaner.buildOffsetMap` on the important rule that an aborted newer key cannot shadow an older committed value。Marker
decisions remain an explicit upstream transaction-proof input；the collector does not guess them from a local key scan。

Pass 2 must replay the output prefix densely and reproduce the exact pass-1 output digest before any `Result` is returned。
For every retained decision，the executor uses the frozen effective delete horizon、rewrites one valid batch and maps it
through `KafkaCompactionRowMapper` to one ordered NTC2 row。Source/output batch and byte limits fail the call without a
publishable result。The result carries its verified output/decision ranges、exact decision/output source-set SHA-256、
source/output batch counts and full/output fact digests。`KafkaCompactionWriteRequestFactory` rejects a task/result coverage
mismatch and derives the closed NTC2 request metadata from those verified facts：output source-set SHA、record/batch/logical
byte counts、`kafka-log-cleaner-v1/1`、`KCK2`、`KAFKA_RECORD_REWRITE_V1` and the codec message-format digest。The caller only
supplies task-owned cluster/stream/attempt/policy/cumulative-size/Parquet-writer facts；it cannot substitute row accounting
or compatibility identities。

`KafkaCompactionWinnerIndex` is now the production pass-one key index。It owns a `TreeMap<KCK2,long>` whose accounting is
`encodedKey.length + 64` per distinct resident key。Before adding a new key that would cross
`Snapshot.maxInMemoryKeyBytes`，it flushes the current map；one single entry may temporarily exceed that budget by at most
`maxKeyBytes + 64` so progress does not depend on the budget exceeding the largest admitted key。A resident duplicate only
updates its greatest eligible offset。The one-argument collector and four-argument executor constructors remain
strict-budget in-memory semantic-test paths；production construction supplies the shared `StagingFileManager` explicitly。

Every flushed run is an owner-only `PrivateStagingSpillFile` charged to the process-global staging-byte permit：

```text
KCSR run V1
  magic:int32             = 0x4b435352
  version:int32           = 1
  repeated:
    encodedKeyLength:int32
    encodedKck2Key:bytes  # first tag must be KEYED_TAG
    greatestOffset:int64  # inside frozen decisionHorizon
  end:int32               = -1
  entryCount:int64
```

Run keys are strictly increasing under unsigned lexicographic byte order and therefore unique inside one run。
`PrivateStagingSpillFile.seal()` freezes file identity、length and whole-file SHA-256；every merge opens
`openVerifiedInputStream()` and must consume through EOF，so a same-inode/same-length byte mutation still fails checksum
verification。The merge queue holds at most `DEFAULT_MERGE_FAN_IN=16` readers，selects the greatest offset for equal keys and
recursively emits another checked run when the fan-in bound is reached。Key length/tag、header/version、strict order、
offset range、entry count、trailing bytes and checksum all fail closed。

The final merge does **not** retain another key map。It sets one `BitSet` position only when the greatest eligible offset is
inside output coverage；the index is `absoluteOffset - outputCoverage.startOffset` and the bitmap is bounded by the frozen
output record count。`Facts.contextFor` consequently passes `latestForKey:boolean` rather than materializing the greatest
offset again。A keyed output record whose bit is false is superseded；unkeyed/control and ABORTED/OPEN validation contexts
carry `true` because they are not winner-index lookups。`Facts.spillRunCount` and `peakInMemoryKeyBytes` expose bounded
execution evidence without becoming durable decision inputs。

Success、parse/checksum failure、upstream decode failure、cancellation and explicit close all close every run，delete its
private file and release the shared staging permit。Spill bytes are deliberately not durable state：after a process crash the
orphan-grace scanner may remove them，while task recovery reloads KCP1 and deterministically replays the same exact
decision/output sources。`KafkaCompactionWinnerIndexTest` proves spill/no-spill winner equivalence including a newer
decision-tail key，two independent restart recomputations、same-length sealed-run corruption rejection and cancellation
cleanup。

`KafkaCompactionStreamingExecutor` is now the production composition of those exact inputs。Its decision stage subscribes
to the full-horizon cold stream one batch at a time，runs `ExactSourceSetVerifier` before decode and finishes the spilling
collector。Its output stage independently subscribes to the output-prefix stream，reproduces the pass-one output fact SHA，
applies strategy/rewrite/row mapping and writes only survivors to an execution-local KCRS spool。Both stages enforce
`Limits.maxSourceBatches`；the spool enforces `maxOutputBatches/maxOutputBytes`。Cancellation atomically cancels the active
stage future、closes both exact streams and closes the collector/spool，including a decision-pass cut after KCSR has already
reserved staging bytes。

The extra spool is required because the closed NTC2 request/footer needs exact survivor rows、records and logical bytes
before the Parquet writer starts，while KCP1 permits exactly two authoritative source passes and payloads cannot be retained
in heap。KCRS V1 is not durable task state：

```text
KCRS row spool V1
  magic:int32                       = 0x4b435253
  version:int32                     = 1
  repeated:
    absoluteOffset:int64            strictly increasing, inside outputCoverage
    recordCount:int32
    dispositionWireId:int32
    encodedKck2Length:int32
    encodedKck2:bytes
    exactPayloadLength:int32
    exactKafkaBatch:bytes
    payloadCrc32c:int32
    sourceBatchBaseOffset:int64
    sourceRecordIndex:int32
    sourceBatchSha256:bytes[32]
    hasEventTime:boolean
    eventTimeMillis:int64           iff hasEventTime
  terminalOffset:int64              = -1
  outputRows:int32
  outputRecords:int64
  logicalBytes:int64
```

The file is an owner-only `PrivateStagingSpillFile`，therefore seal freezes inode/length/whole-file SHA-256 and replay must
consume verified EOF。The single subscriber receives rows only under positive demand；header/version、lengths、offset order、
coverage、row constructors、running/terminal accounting、trailing bytes and whole-file SHA all fail closed。Verified EOF、
cancel、explicit close、executor rejection and corruption all close/delete the file and release the shared staging permit。
Empty survivor output is valid and is encoded by the terminal tuple `0/0/0` without fabricating a row。

`KafkaCompactionWriteRequestFactory.create(input, StreamingResult)` derives immutable NTC2 metadata only after both passes
finish。`KafkaCompactionParquetPublisher.prepare` then consumes KCRS exactly once through
`KafkaTopicCompactedObjectWriter` and returns an owned staged `PreparedObject` plus decision/output source SHA、fact SHA and
spill metrics。The prepared object is not yet a committed generation：caller close owns its staged file until later upload、
strict verification and publication. `KafkaCompactionStreamingExecutorTest` compares its content SHA with the dense
reference executor，uploads the staged file to `LocalFileObjectStore` and passes the strict ranged NTC2 verifier；it also
proves empty-survivor NTC2 is strictly readable and cancellation during pass one releases an already-created KCSR run。
`KafkaCompactionRowSpoolTest` covers one-row demand、the empty terminal tuple、same-length corruption detected only at
verified EOF、pre-subscribe close and invalid replay-executor cleanup。

The generic F4 publication boundary now has an explicit Kafka NTC2 policy identity
`nereus-kafka-topic-compacted-v2` / `NEREUS_TOPIC_COMPACTED_KAFKA_PARQUET_V2`。The policy snapshot codec accepts NTC1
and NTC2 as distinct closed `TOPIC_COMPACTED` formats，and the Kafka resolver rejects an NTC1 task before exact-source
execution。`MaterializationOutput`、its durable task output and `GenerationIndexRecord` allow `entryCount == 0` only for
the sparse view with `outputRecordCount == 0 && logicalBytes == 0`；`COMMITTED` remains dense/non-empty。Generation
publication additionally requires the output physical format to equal the frozen task policy，so an NTC2 object cannot be
published under an NTC1 policy digest。`GenerationIndexPublicationTest` proves a fully superseded NTC2 output reaches a
zero-row `COMMITTED` generation without weakening COMMITTED-view accounting。

`KafkaCompactionPublicationCoordinator` now owns the next production boundary。It claims/heartbeats the exact durable task，
uploads the sealed NTC2 with per-attempt task/binding/config authority revalidation，reconciles uncertain PUT by exact HEAD，
then performs full-file SHA/CRC/closed-schema/row verification before freezing `OUTPUT_READY`。The generic
`GenerationCommitter` is invoked only after that durable output exists；its returned COMMITTED index identity is encoded
into a canonical、gap-free `KafkaCompactionGenerationSet` digest。Only the final exact binding CAS makes the set mandatory。
Initial、same-policy extension and verified replacement use explicit transition modes and increment `activationEpoch`
exactly once。PUT response loss and coverage-CAS response loss reload exact state；a concurrent coverage/config/trim
change after Generation commit leaves the generation non-mandatory and fails closed。The NTC2-specific F4 verifier also
maps physical `KAFKA_RECORD_BATCH_V1` back to the durable `KAFKA_RECORD_BATCH` payload identity without relabelling the
read target。

After the output has reached durable `OUTPUT_READY`，`recoverPublication` is the only Kafka-specific restart re-entry：
it accepts only `OUTPUT_READY/PUBLISHING/PUBLISHED` tasks whose frozen task/output still match the recovered KCP1 and exact
NTC2 format。It reloads the task/output and ACTIVE binding，revalidates the binding window plus caller authority，then
re-enters the idempotent generic Generation state machine without a local staging file。If the binding already contains the
canonical desired generation-set digest/range/policy and the activation epoch derived from the plan's previous mandatory
coverage，recovery converges without a second coverage mutation；otherwise the exact frozen activation basis is checked
again immediately before the bounded response-loss-safe CAS。A changed task/output、binding window、coverage basis or
authority fails closed，and a committed but unactivated generation remains non-mandatory。

`KafkaCompactionPublicationCoordinatorTest` executes a real staged Parquet NTC2 upload/read verifier and the durable task
claim/output transition，injects both response-loss cuts，accepts a same-owner heartbeat race and proves an unrelated
coverage activation cannot be overwritten after Generation commit。It now also interrupts coverage activation after the
durable output/Generation boundary，discards the staging context，then proves a fresh recovery call activates the exact
coverage；a repeated recovery recognizes an already activated root without advancing its epoch。This is deterministic
metadata/object-boundary restart evidence，not the still-required real-provider process-restart gate。The binding-rooted
read-view router described in section 11.3、exact activated-generation-set discovery and generation-constrained Object-WAL
runtime reads are implemented。`KafkaCompactionPartitionPass` now owns the full single-partition composition；
`KafkaCompactionRuntime` owns bounded partition enumeration、process-current leader revalidation and cross-partition
scheduling。The remaining boundary is the production fork registration/concrete partition-lock/KRaft authority capture
provider and the real-provider/process-restart gate，not generation discovery、durable-output re-entry、per-partition
lifecycle routing or process-level dispatch。

`KafkaCompactionPlanCoordinator` now makes KCP1/task publication and worker recovery executable without pretending the two
Oxia roots are atomic。It writes the immutable KCP1 child first，then asks `MaterializationTaskStore.create` to revalidate
authority and reread the exact task-addressed KCP1 immediately before task admission。A crash before task creation can leave
only a harmless plan orphan；a visible task cannot be created after its plan disappears or changes。Restart recovery resolves
the child directly by `materializationTaskId` and cross-validates every task identity/source/policy fact before returning the
frozen decision horizon。Exact terminal task-first/KCP1-second retirement is now implemented as described in `04`。
`KafkaCompactionPlanMetadataStore.scanCompactionPlans` now adds partition-scoped opaque continuation and a strict
`[1,1000]` page bound。`KafkaCompactionPlanOrphanScanner` freezes one scan time，caps both page and total plans，skips the
grace window/live tasks，and deletes an old plan only after stable no-admission authority、exact plan reload and a second
task-absence proof。An uncertain exact-version delete reloads the same task-addressed key；only exact absence is accepted
as applied。

`KafkaCompactionBatchSource.open(recoveredPlan)` now turns that recovered KCP1 into two independent cold
`ExactSourceSetBatchPublisher` instances：one for the full decision horizon and one for the output prefix。Each stream opens
the frozen generations sequentially through `ExactSourceRangeReader`，holds the existing object pin for the active source，
propagates one downstream request at a time and owns cancel/close。Every emitted batch must match range、generation、
commitVersion、target bytes/identity、payload/schema/projection and per-source final accounting；no generation resolver or
fallback is invoked between passes。The streams accept ranged Kafka batches，not the old one-entry/one-record assumption。
Completion is signalled after the last source summary even when the last batch consumes the exact remaining demand；it never
requires a protocol-invalid extra request。The durable streams、checksummed winner spill、KCRS replay、streaming NTC2
writer、upload/generation publication、coverage activation and exact terminal retirement now form executable product
components。`KafkaCompactionPartitionPass` composes them into one recoverable per-partition execution：

1. capture one exact ACTIVE binding、planner image、output policy、pass-one transaction/marker/resource facts、writer
   settings and a stable authority guard；
2. scan KCP1 children with explicit page/total bounds，resume the unique task-rooted plan before admitting fresh work and
   leave task-absent plan-only roots to the orphan scanner；
3. route every durable task lifecycle explicitly，including retry deferral、live-claim observation、clock-skew-safe expired
   claim requeue、durable-output publication re-entry and terminal retirement；
4. for fresh work only，run planner → exact source resolution → plan-first KCP1/task convergence，then claim with a secure
   process-scoped identity；
5. heartbeat while KCSR/KCRS/NTC2 preparation runs，perform one final renewal before publication owns the claim，and close
   exact streams/staging on synchronous、asynchronous、cancellation and heartbeat failure cuts；
6. persist a typed `TaskFailureClass` plus retry deadline by exact task-version CAS；`SOURCE_CHANGED/SOURCE_RETIRED/CLOSED`
   cancel，resource/metadata/object failures retry within the attempt bound，and corruption/unsupported/invariant failures
   become terminal；
7. reconstruct INITIAL/EXTEND/REPLACE and the exact previous generation set from KCP1's frozen previous mandatory coverage，
   never from a newer mutable binding snapshot；
8. after publication，reload exact PUBLISHED task/KCP1 roots and retire task first、plan second under the same authority。

Concurrent `runOnce` callers receive cancellation-isolated views of one in-flight pass。The deterministic integration test
uses actual Parquet NTC2 write/read verification、local-file object storage、in-memory Generation/binding metadata and exact
source streams to prove `PLANNED → CLAIMED → OUTPUT_READY/PUBLISHING/PUBLISHED → coverage CAS → dual-root retirement`。

`KafkaCompactionScheduler` is the process-level non-overlap owner for invoking partition passes。It schedules an
immediate startup pass and fixed delay only after completion，keeps at most one active and one coalesced pending pass，and
freezes all pending reasons with the highest operational priority。A trigger received during the active pass completes only
after the next pass consumes it；caller cancellation cannot cancel shared work。Close cancels the scheduler-owned deadline
and active source but never closes the borrowed scheduler or callback executor。The `PassExecutor` remains the explicit seam
where runtime wiring must enumerate currently owned partitions、construct their concrete capture provider and invoke
`KafkaCompactionPartitionPass`。`KafkaCompactionRuntime` now implements that product-side callback：the fork supplies one
bounded immutable `OwnedPartition` snapshot containing canonical identity、observed leader epoch、INTERNAL/USER work class
and the partition-lock-backed `CaptureProvider`。The runtime rejects duplicate durable partition IDs and over-limit
snapshots，sorts INTERNAL before USER，rechecks each item against `KafkaPartitionStorageManager.current` immediately before
launch，and skips missing、changed-epoch or non-writable leaders。It runs at most the configured number of partitions in
parallel，attempts every accepted item even when another fails，and reports failures in stable partition order。Its
non-cancellable aggregate lets scheduler close stop new deadlines while waiting for already accepted partition work；
borrowed scheduler/callback executors are never closed。

### 10.5 Record rewrite V1

For a normal survivor：

- preserve absolute offset、timestamp type/value、key/value/headers；
- preserve transactional/control flags as applicable；
- preserve producerId/epoch；
- set base/last sequence to original sequence for that record；
- set one-record offset delta 0；
- use activated Kafka magic and original compression codec policy；initial implementation may emit uncompressed only if
  Kafka baseline cleaner oracle proves equivalence and config explicitly allows；
- recompute Kafka batch CRC and NTC2 row CRC；
- validate decode round trip equals logical source record。

Ordinary COMMITTED recovery never consumes rewritten NTC2，so producer sequence truth remains source/checkpoint-derived。

`KafkaTopicCompactionCodecV1.rewrite` first SHA-verifies and re-decodes the opaque source batch，then value-compares all
selected facts before creating one magic-v2 batch at the original absolute offset。For normal records it preserves timestamp
type/value、key/value/header order and bytes、partition leader epoch、transactional flag、producer id/epoch and the selected
record sequence；for commit/abort control records it round-trips the exact `EndTransactionMarker` meaning。Original
compression is preserved，with uncompressed retry permitted only when the frozen context explicitly enables it。The output
is decoded again and compared before its CRC32C is published。The codec also preserves an existing magic-v2 delete horizon；
for a first retained tombstone/control pass it writes the plan-frozen `now + delete.retention.ms` horizon into the rewritten
batch。Current tests cover empty-key/null-key/tombstone separation、GZIP value/header rewrite、transactional sequence、
abort marker/horizon preservation and range/SHA/message-format drift rejection。`KafkaCompactionRowMapper` then copies
the exact bytes/CRC/source identity to NTC2 without reinterpretation；the NTC2 row contract requires a non-empty fetchable
payload for tombstones as well as all other survivors。

### 10.6 Transaction/control rules

`KafkaCompactionStrategyV1` ports/encapsulates the selected Kafka `LogCleaner` semantics，not a key-only shortcut：

- null-key data is retained by unique offset；
- control records use unique semantic identity and are retained until transaction-marker deletion is safe；
- a transactional data survivor retains transactional flag/producer facts；
- aborted data and markers follow stock cleaner visibility/retention behavior；
- tombstone is retained through `delete.retention.ms` and only dropped with the required full-scan horizon proof；
- open transactions crossing output coverage prevent unsafe cleaning or shrink the eligible end；
- unknown magic/control type/producer invariant aborts task，never drops record。

The implemented decision core is deliberately independent from I/O：

```java
Decision decide(DecodedCompactionRecord record, RecordContext context);

record RecordContext(
    boolean latestForKey,
    TransactionStatus transactionStatus,
    MarkerStatus markerStatus,
    OptionalLong deleteHorizonMillis,
    boolean fullScanHorizonProven,
    boolean deleteHorizonPreexisting,
    long nowMillis) {}

enum TransactionStatus {
    NON_TRANSACTIONAL, COMMITTED, ABORTED, OPEN, DECIDED
}

enum MarkerStatus {
    NOT_CONTROL, RETAIN_REQUIRED, DELETE_ELIGIBLE
}
```

Pass 1 must provide these facts from the **entire frozen decision horizon**；callers cannot infer them while writing pass 2。
For keyed data，only the offset marked by the final winner bitmap receives `latestForKey=true`；an older occurrence is
superseded。
ABORTED transactional data is dropped and OPEN fails closed。A non-transactional record cannot carry transactional facts，
and a transactional data record cannot carry `NON_TRANSACTIONAL` or marker-only `DECIDED` facts。Control records require
`DECIDED` plus an exact own-offset identity and one explicit marker decision。

`horizonPassed` is true only when pass 1 supplied `fullScanHorizonProven=true`、a delete horizon and
`deleteHorizonPreexisting=true`，and `nowMillis >= deleteHorizonMillis`。A first cleaning pass that assigns
`now + delete.retention.ms` must retain the record even when retention is zero；only a later scan observes that horizon as
preexisting and may drop it at equality。Therefore an absent/new horizon、partial scan or `now < horizon` retains the
tombstone/marker；equality is the first delete-eligible instant for a preexisting horizon。`RETAIN_REQUIRED` markers never
use the time condition。

F9 deliberately represents a null-key data record as a unique offset identity and retains it；this is the compatibility
contract required by this design rather than silently discarding an input that Nereus has already durably accepted。Empty
keys remain normal keyed values and never collide with null-key/control identities。Changing this rule requires a strategy
version bump and migration/differential evidence。

Current tests prove latest/older keyed decisions、unique null-key retention、committed/aborted/open transaction handling、
tombstone and marker equality boundaries、first-pass horizon retention、missing full-scan proof fail-safe retention and
fact-pair rejection。`KafkaCompactionPassOneCollectorTest` now supplies/proves these facts over a dense frozen horizon and
re-proves the output prefix；`KafkaCompactionTwoPassExecutorTest` composes real Kafka decode、strategy、rewrite and NTC2
mapping；`KafkaCompactionWinnerIndexTest` covers the production sorted spill and restart recomputation。The stock-cleaner
differential oracle is still required before activation。

Mandatory differential tests run the same bounded log/config through stock Kafka cleaner and F9 engine，then compare visible
logical records for READ_UNCOMMITTED and READ_COMMITTED plus transaction metadata。Any deliberate difference requires a
documented compatibility decision and version bump。

## 11. Irreversible compaction coverage

### 11.1 Why a root is required

Once Kafka cleaner removes an older record，later changing cleanup policy or losing a compacted object must not make it visible
again。Nereus still has lossless COMMITTED source for recovery，so a naive fallback would resurrect data。

Binding therefore contains a correctness field `KafkaCompactionCoverageRecord`：

```text
coverageVersion:int=1
startOffset:long
endOffset:long                       mandatory half-open prefix/interval
activationEpoch:long
generationSetSha256:32 bytes
policySha256:32 bytes
activatedAtMillis:long
```

Empty coverage uses `start=end=0, activationEpoch=0`。For an active partition，coverage is contiguous from the logStart that
was current at first activation；trim may move start forward。`endOffset` never decreases。Replacement can change generation
digest at same/end-greater coverage with activation epoch +1。

### 11.2 Activation protocol

1. F4 NTC2 generation(s) reach COMMITTED and full verification；not yet client-mandatory；
2. acquire protections for exact generation set；
3. reload binding、KRaft config、stream head and source coverage；
4. require new coverage starts at current mandatory end or is verified replacement of already mandatory range；
5. CAS binding root to new coverage/digest/activation epoch；**this is logical compaction linearization**；
6. readers reloading root must use TOPIC_COMPACTED for mandatory coverage；
7. release superseded set only after same-view replacement/pins/grace。

Response loss reloads binding。Generation committed without root CAS may be retried or retired and does not change client
visibility。

The implemented CAS accepts only one of three shapes：

- `INITIAL`：old coverage is EMPTY and the new set begins at current authoritative logStart；
- `EXTEND`：old generation-set digest is supplied and re-proved，start/policy stay fixed and end advances；
- `REPLACE`：old digest is supplied and re-proved，new set covers every untrimmed mandatory offset and epoch advances。

The binding transition itself rechecks ACTIVE lifecycle、stable end、trimmed-gap safety、policy/generation SHA length and
monotonic timestamp/epoch，so a caller cannot bypass these rules with a hand-built binding copy。

### 11.3 Read rules

```text
offset < logStart                        -> OFFSET_OUT_OF_RANGE
logStart <= offset < mandatoryEnd       -> TOPIC_COMPACTED only
offset >= mandatoryEnd                  -> COMMITTED tail
```

Within mandatory range：reader may fall back to another healthy **NTC2** generation of same activated coverage，never to
COMMITTED/NCP2/WAL。If no valid compacted source，fail storage read and offline/repair internal coordinator as applicable。

Sparse result uses `sourceCoverageEndOffset` to cross holes。At mandatory end assembler switches view in a new resolver
candidate。No single physical candidate crosses view。

Changing cleanup policy `compact -> delete` freezes existing mandatory coverage and uses COMMITTED only for new tail；old
records do not reappear。Re-enabling compaction resumes/extends or replaces NTC2 coverage。

Current implementation（2026-07-27）：`KafkaCompactedFetchPlanner` validates the exact ACTIVE partition/stream binding and
freezes the activation epoch、generation-set SHA and policy SHA into an immutable two-segment plan。It treats the binding
coverage as correctness state independently of the current cleanup policy。`KafkaCompactedFetchReader` reloads that root for
every Fetch，issues only `TOPIC_COMPACTED` calls below mandatory end，advances sparse/empty results by
`sourceCoverageEndOffset` and then issues a new `COMMITTED` call at the exact boundary with the remaining record/byte/deadline
budget。All mandatory failures propagate directly；there is no adapter path that retries the same offset through COMMITTED。
The production runtime now passes its borrowed partition metadata store into every opened storage instance；the legacy
constructor remains committed-only for isolated compatibility tests。`KafkaActivatedGenerationSetResolver` scans a bounded
set of TOPIC_COMPACTED indexes，requires the unique gap-free path whose canonical identity digest matches the binding，and
builds a `GenerationReadConstraint` containing every range/generation/publication/index key/version/SHA。The core resolver
filters to those exact identities before decode/pin，so a newer unactivated unsupported or corrupt generation cannot mask the
activated set。The Object-WAL runtime registers the NTC2 reader and constrained semantic reader；a runtime without those
capabilities fails closed after coverage activation rather than exposing WAL/NCP2。Real-provider restart evidence remains
part of the remaining M5 gate。

## 12. Compaction and lossless materialization coexistence

Two independent views：

| View | Format | Purpose | Recovery eligible |
| --- | --- | --- | --- |
| `COMMITTED` | WAL/BK/NCP2 | exact source bytes | yes |
| `TOPIC_COMPACTED` | NTC2 | Kafka client cleaned visibility | no |

F4 physical GC may replace primary WAL with NCP2 for COMMITTED recovery/read；it cannot claim NTC2 as lossless replacement。
Conversely NTC2 activation/retirement does not change stream head、LEO、HW、producer checkpoint or logical trim。

## 13. Planner scheduling and config changes

`KafkaCompactionPolicyProvider` reads KRaft config snapshot：cleanup policy、min cleanable ratio、min/max compaction lag、
delete retention、segment settings and message format。Policy digest includes effective defaults and metadata offset。

Scheduling：

- only closed segments；
- exclude ranges beyond frozen stable end；
- bound task sources/records/bytes/spill；
- prioritize internal topics but apply quotas；
- one active compaction-activation task per partition root；generic F4 workers may execute tasks concurrently across partitions；
- policy change invalidates unclaimed task or prevents final activation after revalidation；
- compact disable stops new tasks but preserves mandatory coverage。

Current scheduler-owner implementation freezes these triggers as `STARTUP`、`PERIODIC`、`DIRTY_BYTES`、`ADMIN`、
`MAX_LAG`、`POLICY_CHANGE` and `LEADERSHIP_CHANGE`，with the last reason highest priority。It bounds resident work to one
active and one coalesced pending pass；the pass callback still owns partition enumeration、internal-topic quota and
per-partition serialization described above。

## 14. Failure/repair cases

| Failure | Result |
| --- | --- |
| producer checkpoint upload fails | append continues；replay grows；retention pauses if barrier needed |
| checkpoint ref corrupt | try older ref；full replay if trim=0；otherwise offline |
| process dies after stable append before producer update | reopen/replay committed batch |
| txn index update fails | known-committed fence/reopen |
| compaction task crashes during spill/upload | no visibility；task recovery/GC |
| NTC2 generation commits before binding CAS | not mandatory；retry/retire |
| binding CAS applies but response lost | reload exact activation；serve NTC2 |
| mandatory NTC2 object missing | same-view fallback or fail closed；no source resurrection |
| policy disables compact during task | activation revalidation fails；old mandatory coverage remains |
| trim response lost | reload stream trim；update local state idempotently |
| retention races new append/config | revalidation/recompute；never decrease offsets |
| group offset lags behind trim | expected Kafka behavior；consumer receives out-of-range/reset policy |

## 15. Planned classes/files

| Owner | Class |
| --- | --- |
| Kafka fork | `NereusProducerStateManager`、`NereusTransactionIndex`、`NereusTimeIndex`、`NereusLeaderEpochCache` |
| adapter checkpoint | producer/txn/epoch/segment/time/byte section codecs V1 + full composition implemented |
| adapter retention | `KafkaRetentionCoordinator`、`KafkaDeleteRecordsCoordinator`、`KafkaRetentionPlanner`、`KafkaRetentionCheckpointGate/Services`、`KafkaTrimBarrier`、`KafkaRetentionDurableTrimListener` partial implementation |
| adapter compaction | codec/strategy/rewrite/policy/planner/coverage/fetch + activated-generation resolver + scheduler/orphan scanner + single-partition pass + bounded owned-partition runtime bridge implemented；production fork capture wiring pending |
| materialization | ranged decoder SPI、V2 two-pass engine/publisher/verifier |
| metadata | binding compaction coverage nested record/codec/transition validators + partition-scoped KCP1 scan continuation |
| object store | NTC2 writer/reader/goldens from document 02 |

## 16. Test plan

### 16.1 Producer/idempotence

- epochs/sequences/wrap/duplicate last five batches；
- stable append response loss then exact producer retry；
- snapshot/replay equivalence for random batch traces；
- producer expiration and trim checkpoint；
- post-commit derived-state failure fence/reopen。

### 16.2 Transactions/coordinators

- open/commit/abort across checkpoint/restart/leader takeover；
- LSO/HW and read committed/uncommitted；
- marker/data span virtual segments and compaction coverage；
- `__consumer_offsets` group commit/rebalance/restart/compaction；
- `__transaction_state` coordinator failover and ongoing transaction；
- corrupt mandatory internal-topic NTC2 fails election without COMMITTED fallback。

### 16.3 Retention/DeleteRecords

- time/size/both/disabled policies against stock virtual-segment oracle；
- group lag does not block trim；
- checkpoint barrier failure/response loss；
- DeleteRecords at batch start/middle/end/HW；
- compact+delete interaction、restart and physical-GC lag。

### 16.4 Compaction

- keyed overwrite、empty key、null key、tombstone windows；
- all compression/message formats；headers/timestamps；
- idempotent/transactional/control/aborted/open transaction traces；
- decision horizon with newer tail keys；
- spill boundaries/restart/determinism；
- mandatory coverage extend/replace/policy disable；
- no-resurrection property under every same-view corruption/fallback cut；
- differential oracle against stock Kafka `LogCleaner`。

F9-M4 exits only after native producer/transaction/internal-topic recovery gates；F9-M5 exits only after retention、
DeleteRecords、compaction and no-resurrection gates。
