# 05 — Producer State, Transactions, Compaction and Retention

> 状态：F9-M4 all seven NKC1 canonical sections + strict V1 codecs/full composition + exact idempotent/transaction/control append encoding implemented；Kafka-fork stock producer/transaction import/replay、checkpoint hydration、HW/LSO publication、READ_COMMITTED/aborted-index、transactional executor handoff and internal-topic ready-ordering deterministic slices implemented locally；F9-M5 virtual segment/config history/derived index、checkpoint-before-DeleteRecords、periodic retention runtime and compaction fork authority/marker capture implemented；native DeleteRecords + rooted NKC1 + durable trim + forced-restart/current-trim recovery process slice passes；completed group/transaction coordinator live migration、Object-WAL bidirectional OPEN transaction COMMIT/ABORT migration、all-five-profile before/after-provider abort-marker process cuts、deterministic mandatory NTC2 admission and real Object-WAL NTC2 delete/corrupt + exact repair/re-election pass；stock `LogCleaner` differential now covers stable-prefix keyed/null/tombstone/transaction/control/idempotent semantics，while non-Object NTC2 profile expansion、full compression/OPEN-boundary oracle and real compaction restart remain in progress
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
    KafkaProducerTransactionState exportCanonical(long expectedMapEndOffset);
    void restoreCanonical(KafkaProducerTransactionState state);
    void resetForRecovery(long logStartOffset);
    void replayBatch(RecordBatch batch);
    KafkaProducerTransactionState freezeCanonical(long stableEndOffset);

    @Override Optional<File> takeSnapshot(boolean sync); // empty；no truth-bearing file
}
```

The selected Kafka baseline needs one narrow inert stock-package constructor seam because `ProducerAppendInfo.appendDataBatch`
cannot represent a marker-updated `lastTimestamp` independently from the retained data-batch window：

```java
// org.apache.kafka.storage.internals.log.ProducerStateEntry
public static ProducerStateEntry fromBatchMetadata(
    long producerId,
    short producerEpoch,
    int coordinatorEpoch,
    long lastTimestamp,
    OptionalLong currentTxnFirstOffset,
    List<BatchMetadata> batchesOldestToNewest);
```

The factory requires at most `NUM_BATCHES_TO_RETAIN` non-null entries and copies them in supplied order without calling
`addBatch`，so it never overwrites the separately supplied `lastTimestamp`。It is storage-neutral and unused by the stock
path。`NereusProducerStateManager.restoreCanonical` converts each canonical batch directly to stock `BatchMetadata` and loads
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
fork can import it。Isolated fork commit `ec7f0db991` implements `NereusProducerStateManager` import/export and exact
COMMITTED-batch replay。Stock `ProducerStateEntry.fromBatchMetadata` reconstructs the retained five-batch deque without
calling the scalar-mutating add path，so a later marker-updated `lastTimestamp` remains independent。Import is accepted only
into a fresh manager and immediately re-exported for exact canonical equality；ordinary local snapshot IO remains disabled。
Deterministic tests cover sequence wrap、five-batch retention、marker timestamp preservation、checkpoint encode/decode and
replay equality。该 commit 现已作为 published F9 branch 的第二十个 commit 包含在
`nereusstream/kafka:nereus/future9-native-kafka-storage@50b46aab2d`。

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
canonical payload and requires strictly increasing marker offsets，matching stock `TransactionIndex.append`。Fork
`NereusTransactionIndex` now owns an ordered in-memory `AbortedTxn` list，implements append/import/truncate/filter/all/reset
without a truth-bearing local file，and is populated both by NKC1 import and exact committed-tail replay。

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

Current fork implementation first decodes the complete seven-section `KafkaCanonicalCheckpointStateCodecV1` image。
It restores producer/open/aborted state、leader-epoch ranges、virtual logical bytes and timestamp maxima before replaying
the bounded committed tail。`NereusUnifiedLog.installRecoveredState` installs the same exact producer manager and repopulates
the stock leader-epoch cache before publishing storage。On append，stock validation and verification run first；after the
durable callback returns，the shell advances stock HW to the exact durable end、derives LSO and calls
`publishDerivedOffsets`。Open transactions therefore hold LSO at their first offset，abort markers advance it only after
stock state/index completion，and READ_COMMITTED emits aborted metadata only for the actual returned byte page。Focused
fork tests cover non-zero checkpoint + tail hydration、overlapping open/abort restore、verification guard、two aborted
transactions with a bounded page and no truth-bearing snapshot/transaction-index files。

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

### 5.1 Mandatory compacted-read admission probe

Product `b6b02f4` implements the storage-side half of this contract。The public boundary is：

```java
CompletableFuture<Void> KafkaPartitionStorage.probeMandatoryCompactedRead(Duration timeout);
```

The interface default returns a failed future with `UNSUPPORTED_READ_SEMANTICS`。It is intentionally not a successful
no-op：a fork compiled against the new API but running an implementation without binding/generation authority must block
coordinator election。

`DefaultKafkaPartitionStorage` captures the same immutable `KafkaStableSnapshot` used by Fetch and delegates to
`KafkaCompactedFetchReader.probeMandatoryCompactedRead(snapshot, timeout)`。The reader executes this exact algorithm：

1. require a positive millisecond-representable timeout and a configured `KafkaPartitionMetadataStore`；
2. linearizably load the exact durable partition binding；missing binding is `STREAM_NOT_FOUND`；
3. compute `authoritativeLogStart = max(snapshot.logStartOffset, binding.observedLogStartOffset)`；
4. call `KafkaCompactedFetchPlanner.plan` with `maxOffsetExclusive = coverage.endOffset`，so an active untrimmed range
   must produce exactly one `TOPIC_COMPACTED` segment and can never contain a COMMITTED tail；
5. resolve the binding-rooted `generationSetSha256` through `KafkaActivatedGenerationAuthority` to one exact,
   gap-free `GenerationReadConstraint`；
6. for every constraint identity whose exclusive end is above `authoritativeLogStart`，issue one
   `ReadRequest(start=max(logStart,generation.start), view=TOPIC_COMPACTED,
   boundary=CONTAINING_ENTRY, firstEntry=ALLOW_FIRST_ENTRY_OVERFLOW,
   maxRecords=1, maxBytes=1, isolation=COMMITTED, remainingDeadline)`；
7. pass the exact `GenerationReadConstraint` to `ConstrainedSemanticStreamReader.read`；the resolver may try another
   physical candidate only inside that same activated generation/view；
8. require exact stream/requested offset/view and strictly advancing
   `sourceCoverageEndOffset <= constraint.coverage.endOffset`；
9. complete only when `CompletableFuture.allOf` has validated every untrimmed activated generation。

The one-record/one-byte limits make this an availability probe rather than a duplicate coordinator replay；
`ALLOW_FIRST_ENTRY_OVERFLOW` permits a single Kafka RecordBatch without weakening the view constraint。A sparse generation
with no surviving row is still valid only when its source-coverage result advances across the probed offset。Coverage wholly
below the current authoritative log start needs no physical probe；coverage version 0 also succeeds after binding identity/
lifecycle validation because no irreversible compacted prefix exists yet。

Failure semantics are deliberately direct：

| Failure | Result before coordinator election |
| --- | --- |
| binding missing、coverage outside snapshot、digest/constraint mismatch | fail open with original metadata/invariant code |
| constrained reader unavailable | `UNSUPPORTED_READ_SEMANTICS` |
| any untrimmed activated generation has no healthy same-view candidate | propagate `OBJECT_NOT_FOUND`/corruption/read-resolution failure |
| deadline expires | `TIMEOUT` |
| probe returns another stream/view/offset or no coverage progress | `METADATA_INVARIANT_VIOLATION` |
| all probes succeed | allow fork installation and eventual ready callback |

There is no branch from this method to `StreamStorage.read(... COMMITTED ...)`。The only ordinary `read` call reused here is
the exact constrained `TOPIC_COMPACTED` overload；therefore lossless source bytes cannot resurrect tombstoned coordinator
state even when they remain physically readable。

Before each mandatory probe, `KafkaCompactedFetchReader` calls
`KafkaActivatedGenerationAuthority.repairIfQuarantined(partition, streamId, binding, timeout)`。This is not a
full-source rebuild path：it runs only when the historical binding digest uniquely resolves after substituting durable
`prior-index-version`/`prior-index-sha256` facts for quarantined wrappers。The resolver verifies the restored physical
object against the retained root with HEAD length/CRC32C/ETag plus a full expected-CRC read and content SHA-256，then CAS
reactivates the exact root and generation。Finally it recomputes the generation-set digest from the new COMMITTED wrapper
versions/SHAs and activates the same coverage with `REPLACE`，yielding a larger activation epoch。The probe reloads that
returned binding and still reads only the new exact `TOPIC_COMPACTED` constraint；a mismatched repair remains unavailable。

Fork `89b66ab03b` implements the admission half inside `NereusListOffsetsLifecycle`，between manager recovery and exact
storage/lookup installation。Group、transaction and share coordinator topics wait；user topics bypass the probe。A failed
probe cancels the prepared leader epoch、removes the slot and resigns the recovered product storage before completing
`openLeader` exceptionally。`NereusTopicDeltaLifecycle` already calls `onLeaderReady` only after that future succeeds, so
`BrokerMetadataPublisher` cannot invoke coordinator `onElection` on the failure path。

Deterministic evidence is split at the ownership boundary：

- `KafkaInternalTopicNoResurrectionTest.scenarioKfTxn016` injects
  `OBJECT_NOT_FOUND` and asserts the only issued request uses `TOPIC_COMPACTED`；
- `unavailableLaterActivatedGenerationAlsoFailsTheProbe` uses two activated generation identities，makes the first
  readable and the later one unavailable，and proves readiness checks the whole untrimmed set rather than only its first
  object；
- healthy sparse and fully trimmed coverage tests lock success/no-I/O behavior；
- fork `NereusListOffsetsLifecycleTest` first reproduced the bug（old code completed open while probe was pending），then
  proves success waits before installation and failure performs cancel + resign with zero installed partitions；
- existing `NereusTopicDeltaLifecycleTest` and `BrokerMetadataPublisherTest` compose that open future with the ready/
  election boundary。

The full deterministic `f9CompactionPropertyTest` and source-locked `phase9M3KafkaForkBridgeCheck` pass against fork
`768924da60`。Product `0ae8ca9` adds
`f9MandatoryInternalTopicNtc2ProcessIntegrationTest.mandatoryInternalTopicNtc2FailureBlocksElectionUntilPhysicalRepair`：
one real `__consumer_offsets-0` is compacted on broker 1，its activated physical NTC2 keys are decoded from the ObjectStore
mapping and snapshotted with bytes/user metadata/content type/provider CRC。With broker 2 live, the gate:

1. deletes every activated NTC2 object，moves the group partition to broker 2 and proves group lookup is unavailable；
2. restores the exact snapshots，moves back to broker 1 and reloads committed offset `1`；
3. flips one middle byte under each original key while preserving metadata，moves to broker 2 and again proves unavailable；
4. restores the exact snapshots a second time，moves back and reloads committed offset `1` again。

Both brokers stay alive throughout。Each failed open quarantines the exact root/index before coordinator election；each
repair is followed by an ordinary reassignment rather than a test-only election callback。Fresh execution passes 73/73
outer tasks in 1m39s after the fork release build passes 166/166 actionable tasks。KF-TXN-016 is therefore
`PASSED_CURRENT_SOURCE` for Object-WAL P/C/K；Object async/BookKeeper profile expansion and the F9 final aggregate remain
separate gates。

Local fork commit `032974067c` adds deterministic ordering evidence at the stock seams：both group and transaction
coordinator elections remain absent until `AsyncTopicDeltaLifecycle` reports the exact leader ready，and the
`__transaction_state` ready callback itself waits for installation of the exact recovered storage instance。This closes
the request/lifecycle unit-test gap。The product `NereusKafkaNativeProcessIntegrationTest` now adds a real single-node
graceful restart slice：the first JVM commits transactional data plus its COMMIT marker、joins/rebalances a real consumer
group and commits offset 2；the second JVM concurrently recovers both internal topics，loads that committed offset，
reinitializes the same transactional ID、commits the next transaction and resumes the group at the next visible data offset。
The same gate then leaves transaction data stable without an end marker、forcibly kills the broker and proves a fresh JVM
reinitializing that transactional ID writes the missing ABORT before its next committed transaction；read-committed and the
real group both skip the aborted data。This is partial KF-TXN-007/014 evidence only；the exact rows still require their locked
BookKeeper/provider matrix，multi-broker coordinator takeover and mandatory NTC2 unavailability process gates；the native
DeleteRecords checkpoint/virtual-segment trim/restart subset is covered in section 9.4.1。

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
corruption/invariant cases and 200 deterministic randomized round trips are in the partial M4 gate。Kafka-fork
`NereusCanonicalLogState` now owns the live realization：only stable append finalizes size/time/index facts，an in-flight
roll is reusable until that append becomes stable，SIZE/TIME/RELATIVE/INDEX_FULL/CONFIG/MANUAL reasons are explicit，
deterministic roll jitter is frozen，and exact KRaft metadata offsets enter config history through
`DynamicConfigPublisher -> TopicConfigHandler -> LogManager`。Recovery hydrates the canonical checkpoint and then replays
the committed tail before rebuilding every cache-only `NereusLogSegment` shell；the active shell is never durable truth。

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
implemented。Kafka-fork `NereusCanonicalLogState` now constructs sparse time/logical samples from exact stable batches，
preserves exact in-memory batch positions，restores them from checkpoint plus bounded committed-tail replay，and projects
real `LogOffsetMetadata` through `NereusUnifiedLog.maybeConvertToOffsetMetadata`。Timestamp ListOffsets uses the time
index only as a scan candidate and verifies the first qualifying timestamp against exact COMMITTED payload under the hard
read bound。Fresh-process provider restart/takeover and differential stock evidence remain pending。

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
segment protection and config-history mismatch。Fork `bd9963c980` adds the stock differential half as
`KafkaRetentionOracleTest`。The test creates a real `UnifiedLog` with legal `segment.bytes=1 MiB`，fixed
`MockTime(now=10000)` and eight single-record batches at timestamps
`[1000,1000,2000,2000,9000,9000,10000,10000]`；explicit `UnifiedLog.roll()` after offsets `1/3/5` fixes closed segment
boundaries at `2/4/6` and leaves `[6,8)` active。For each case it converts the live stock segment facts into the exact
`KafkaVirtualSegmentState` consumed by the product planner，then compares both selected segment count and final logStart
against a real `UnifiedLog.deleteOldSegments()` call：

| Case | Policy/HW | Expected logStart |
| --- | --- | --- |
| time | `retention.ms=5000`, HW `8` | `4` |
| size | retain four record-batch bytes, HW `8` | `4` |
| combined | time + size on the same image | `4` |
| HW cap | `retention.ms=0`, HW `3` | `2` |
| strict equality | `retention.ms=9000`, HW `8` | `0` |
| compact-only | delete disabled | `0` |

The dedicated product gate `phase9M5KafkaRetentionOracleCheck` publishes the exact `0.1.0-f9-dev` artifacts，checks the
current fork source lock，runs fork Spotless/Checkstyle and executes only this oracle。KF-RET-001/002/003 are
therefore `PASSED_CURRENT_SOURCE`；retention remains separate from the compaction oracle below。

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
`KafkaRetentionCheckpointGate` now queries `KafkaCheckpointFailureQuarantine` before object verification。A rooted reference
already quarantined for the exact partition incarnation/reference digest is skipped without object I/O；a newly classified
not-found/checksum/unsupported-format/invariant failure must await immutable Oxia quarantine create or response-loss
reconciliation before the next older reference is verified。Any quarantine metadata read/write/collision failure fails the
retention attempt closed and therefore prevents trim。The durable record is a first-failure audit only；it does not remove the
reference from the binding root or authorize physical deletion。
`KafkaRetentionDurableTrimListener` publishes monotonic observed logStart through binding CAS before invoking the exact
local leader updater，recovers applied-but-response-lost CAS by reload and refuses changed leader terms。Product commits
`3eb6b63` and `57dcf35` now add `DefaultKafkaPartitionMaintenance`、`KafkaPartitionMaintenanceRuntime` and
`CompositeKafkaRuntimeBackgroundService`。Fork commit `feabf6c686` enumerates the exact process-owned writable
partitions，captures each snapshot under the stock partition lock，uses the same `NereusUnifiedLog` for local log-start
publication，and starts a bounded non-overlapping periodic retention pass only after runtime readiness。The complete
`f9RetentionTest` passes。Real provider trim、fresh-process restart/takeover and multi-broker race evidence remain
pending，so this is still deterministic partial M5 evidence rather than an end-to-end retention claim。The focused
`phase9M6CheckpointQuarantineCheck --rerun-tasks` composes immutable metadata contracts、real Oxia close/reconnect、
recovery/retention ordering and production Object-WAL runtime wiring；it does not replace the pending real provider trim and
fresh-process takeover gates。

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
Kafka-fork commit `4c060aec89` now keeps the stock `Partition.deleteRecordsOnLeader` validation and `-1 -> HW`
normalization，captures the product snapshot under the partition lock，waits on the storage worker and advances the same
`NereusUnifiedLog` only after durable trim。Focused `PartitionTest` and `NereusUnifiedLogFactoryTest` cover normalized
HW、exact mid-batch target and local log-start publication。The process slice below now supplies one exact
`OBJECT_WAL_SYNC_OBJECT` start-boundary/Fetch-wake-up/forced-restart path；the following provider-applied response-loss
slice and its companion profile matrix close the same cut across all five profiles。Section 9.4.3 supplies the independent
start/middle/end/HW mapping gate and makes KF-RET-006 `PASSED_CURRENT_SOURCE`；the row is not a claim that every boundary
was repeated under all five profiles。

#### 9.4.1 Forced-restart checkpoint/trim recovery

`:nereus-kafka-adapter:f9CheckpointTrimRecoveryProcessIntegrationTest` runs the exact native sequence：

```text
format KRaft + activate Nereus OBJECT_WAL_SYNC_OBJECT
  -> create RF1 topic with segment.bytes=1048576 and cleanup.policy=delete
  -> Produce six independent ~600 KiB RecordBatches at offsets 0..5
  -> kafka-delete-records.sh requests offset 3
  -> require rooted NKC1(checkpointLogStart=0, checkpointOffset=6)
  -> require stream trim=3/end=6 and Kafka earliest/latest=3/6
  -> SIGKILL broker JVM
  -> fresh release JVM opens the same partition
  -> hydrate checkpoint under captured [0,6]
  -> prune canonical state under current durable [3,6]
  -> Fetch offset 3, Produce offset 6, ListOffsets=3/7
```

恢复实现必须遵守“先按历史 checkpoint window 验证，后按当前 trim 裁剪”的顺序。传入
`KafkaCheckpointHeader` 的 `logStartOffset=0/stableEndOffset=6` 解码 section 4/6/7，可合法包含 base
`1/2`；随后 `NereusCanonicalLogState.advanceLogStart(3)` 删除 end `<=3` 的 virtual segments、
`exactBatchPositions.headMap(3,false)` 和过期 config history。若直接以 current `logStart=3` 解码，合法的
pre-trim bases 会被错误报告为“logical-byte segment bases not bounded”，属于恢复实现缺陷，不得降级为 full
replay：trim 已大于 0 时 full replay 缺失历史 producer/transaction state，必须 fail closed。

`KafkaPartitionStorage.publishDurableLogStart(3)` 先把 product `KafkaStableSnapshot` 原子推进到 durable trim；
fork 仅在 exact storage + leader epoch 仍 writable 时消费该结果，并更新 canonical/virtual/stock logStart。
重启 open 完成后，`DefaultKafkaPartitionOpener` 再把 binding observation CAS 到 current `3/6`。Evidence 读取
binding 时只要求 `observedStableEndOffset <= durableEnd` 且不低于 checkpoint offset；append 后它可以暂时停留在
`6`，不能用 advisory value 否定 authoritative stream end `7`。

大记录还固定了 page-boundary contract：若前一页已返回至少一个 batch，下一次 strict read 因剩余 byte budget
小于首 entry 返回 `READ_LIMIT_TOO_SMALL`，`ParquetV2CompactedTargetReader`、`ReadTargetDispatcher` 与
`KafkaCompactedFetchReader` 必须停止当前页并返回已累积 batches；只有 accumulator 为空时才把错误传给调用方。

#### 9.4.2 Provider-applied trim response loss and fresh-process idempotence

`:nereus-kafka-adapter:f9TrimResponseLossProcessIntegrationTest` 复用相同 Object-WAL/RF1 数据形状，但把故障精确
放在 provider durable mutation 之后、`KafkaTrimBarrier` 观察 completion 之前：

```text
arm TrimCompletionLossAgent(target=3)
  -> intercept DefaultStreamStorage.trim(streamId, 3, options) once
  -> let the original provider future complete successfully
  -> persist applied marker and return an incomplete caller future
  -> observe rooted NKC1 + stream trim/end=3/6
  -> require binding observedLogStart/end=0/6 and DeleteRecords process still alive
  -> SIGKILL broker
  -> fresh broker reloads durable trim and publishes local/binding logStart=3
  -> retry native DeleteRecords(target=3) => lowWatermark=3
  -> require zero new stream-head CAS, binding CAS or checkpoint object
  -> Produce/Fetch offset 6; ListOffsets=3/7
```

Agent capture 只允许在 test 创建 `arm` marker 后发生，避免 startup/recovery trim 消耗 one-shot fault。Advice
替换的是返回给 caller 的 `CompletableFuture<Void>`，不是 provider future；provider exception 与 marker-write
failure 仍必须完成 caller exceptionally，只有真实 provider success 才制造 lost completion。`captured`、
`applied`、CLI liveness 和外部 Oxia snapshot 共同定义故障边界；CLI 文本不是边界，因为 stock tool 会在分区
future 完成前打印结果表头。

Fresh-process no-op 不能只看 low watermark。测试在重试前后读取同一
`StableStreamHeadSnapshot` 和 binding，并要求以下 identity 全等：

```text
stream:
  trimOffset, committedEndOffset, cumulativeSize,
  commitVersion, lastCommitId, metadataVersion, durableHeadSha256
binding:
  metadataVersion, observedLogStartOffset, observedStableEndOffset,
  ordered checkpointReferences
object store:
  exact NKC1 checkpoint object-key set
```

其中 stream `metadataVersion` 不变证明没有重复 trim CAS；binding metadata version 不变证明 already-deleted
路径没有重复 publication；checkpoint refs 和 NKC1 key set 不变证明没有重复 checkpoint PUT。只比较 checkpoint
prefix，避免 Object-async/BookKeeper-async 的正常 NCP2 materialization 或 ledger GC 被误判为 trim side effect。
测试把 session TTL/renew interval 固定为 `90s/30s`，确保上述短窗口内的 metadata version 不被正常 authority
renewal 混入。

`:nereus-kafka-adapter:f9TrimProfileMatrixProcessIntegrationTest` 用同一 `runTrimResponseLossProfile` helper 对
`OBJECT_WAL_ASYNC_OBJECT`、`BOOKKEEPER_WAL_ONLY`、`BOOKKEEPER_WAL_ASYNC_OBJECT`、
`BOOKKEEPER_WAL_SYNC_OBJECT` 重复以上完整序列；加上前置 `OBJECT_WAL_SYNC_OBJECT` task，五档代码路径均被真实
release JVM 覆盖。三种 BookKeeper profile 共用真实 stock ZooKeeper/two-bookie service，但使用互不相交的
namespace reservation、protocol activation、bucket 和 Kafka/Nereus directories。Fresh matrix 以 75/75 tasks、
3m23s 通过，并由 M6 general/BookKeeper process aggregates 聚合。KF-RET-005/010 的 five-profile
response-loss/checkpoint-barrier process slice 已闭合；the boundary and retention differential are closed by the next
gate，while broader chaos remains open。

#### 9.4.3 Exact start/middle/end/HW process mapping

`:nereus-kafka-adapter:f9DeleteRecordsBoundaryProcessIntegrationTest` uses the release distribution at locked fork
`bd9963c980` and a real four-shard Oxia + LocalStack Object-WAL runtime。The fixture creates one RF1 delete-policy topic，
then opens three independent producers；each producer sends three records with `batch.size=1 MiB`、`linger.ms=5000`、
compression disabled and an explicit flush/close，yielding ordered offsets `0..8` in three request batches。The same live
broker processes four stock `kafka-delete-records.sh` requests sequentially：

```text
requested target      3       4       6       HIGH_WATERMARK(-1)
semantic boundary     start   middle  end / next-start   current HW
expected lowWatermark 3       4       6       9
expected latest       9       9       9       9
expected first Fetch  3       4       6       none
```

Each CLI result must return the exact expected RF1 low watermark；Admin earliest/latest must equal
`expectedLowWatermark/9` after every mutation。For targets below `9`，a consumer seeks to the new start and must receive
both the exact offset and the value originally produced at that offset，proving the containing-entry filter does not leak
trimmed records or round the logical start。After target `3`，the test also reloads Oxia and requires a rooted NKC1 with
`checkpointLogStart=0`、`checkpointOffset=9` and durable trim/end `3/9` before proceeding。The `-1` request is passed
through the stock CLI and only normalized inside the fork under the partition lock。

The Gradle task is a first-class dependency of `phase9M6KafkaProcessCheck`，runs after the forced-restart trim gate and
before response-loss injection，uses its own evidence directory
`nereus-kafka-adapter/build/f9-kafka-delete-records-boundary-evidence/` and is explicitly classified as Docker-backed so
service availability is checked before execution。A forced fresh run passes 64/64 actionable tasks in 38s。Together with
the deterministic range/policy/config-race tests and 9.4.1/9.4.2 recovery/idempotence gates，this closes the current-source
KF-RET-006 contract without marking F9-M5 final。

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
rewrite token。The adapter compiles against `kafka-clients` with `compileOnly` and adds the locked 3.9 client only to its
test classpath；a Kafka broker/fork therefore supplies its own matching Kafka runtime and does not receive a transitive 3.9
client or LZ4 capability。Kafka server classes remain absent。

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
scheduling。`KafkaCompactionProductionRuntimeFactory` now supplies the production Object-WAL composition behind both：
direct-stream COMMITTED source resolution、recovered-plan stream-bound exact readers、shared KCSR/KCRS/NTC2 staging、
strict object verification、activation/partition-authority-fenced Generation commit、coverage activation、terminal
retirement and the process scheduler all use the same Oxia/Object-WAL/read-pin/provider graph。Product commit `e18bf36`
and Kafka fork commit `58342d9dca` close the former production-fork boundary：the product validates one exact
ACTIVE binding/source and constructs the mutation guard；the fork enumerates current leaders，captures canonical state under
the partition lock，then scans the selected decision horizon with stock `CleanedTransactionMetadata` and revalidates the
producer/transaction image。The remaining boundary is the real-provider/process-restart and full stock-cleaner
differential gate，not registration、authority capture、generation discovery、durable-output re-entry、per-partition
lifecycle routing、process-level dispatch or product provider composition。

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

The activated Object-WAL creator accepts an optional `NereusKafkaCompactionContext` containing one validated
`NereusKafkaCompactionRuntimeConfiguration` and the fork-owned `OwnedPartitionSource`。The configuration rejects a relative
staging directory、a pass bound below concurrency、a staging budget below one upload chunk、non-COMMITTED source reads and
all individual hard-limit violations before provider work。`NereusKafkaObjectWalRuntimeFactory` creates one private
`StagingFileManager` under that resource ledger，then defers `KafkaCompactionProductionRuntimeFactory.create` until the one
process `KafkaPartitionStorageManager` exists。This prevents a compaction scheduler from observing another manager instance。
`DefaultNereusKafkaRuntime.start` starts the resulting service after activation but before READY；drain closes it and waits
accepted work before shutting the manager，and only then may provider/staging resources close。
The Kafka runtime instance ID is hashed through the shared canonical SHA-256/base32 helper before it becomes any durable
`processRunId`，including materialization worker claims and physical-object reader leases，so the fork's UUID-shaped process
identity never reaches base32-only metadata records；all subsystems in one runtime derive the same canonical identity。

`KafkaCompactionBatchSource.ReaderFactory` is intentionally keyed by the recovered KCP1 `streamId`。One process-global
composition therefore cannot accidentally capture a stream from broker startup；each pass constructs
`DefaultExactSourceRangeReader(cluster, recoveredPlan.streamId, ...)` from the durable plan before opening either cold
stream。The existing fixed-reader constructor is retained only for narrow tests/compatibility。

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
for a first retained tombstone，or a `DELETE_ELIGIBLE` empty transaction marker，it writes the plan-frozen
`now + delete.retention.ms` horizon into the rewritten batch；`RETAIN_REQUIRED` control markers preserve an absent
horizon。Current tests cover empty-key/null-key/tombstone separation、GZIP value/header rewrite、transactional sequence、
abort marker/horizon preservation and range/SHA/message-format drift rejection。`KafkaCompactionRowMapper` then copies
the exact bytes/CRC/source identity to NTC2 without reinterpretation；the NTC2 row contract requires a non-empty fetchable
payload for tombstones as well as all other survivors。

### 10.6 Transaction/control rules

`KafkaCompactionStrategyV1` ports/encapsulates the selected Kafka `LogCleaner` semantics，not a key-only shortcut：

- null-key data is dropped，matching stock `LogCleaner.shouldRetainRecord` invalid-message behavior；
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

F9 still represents a decoded null-key record with a unique tagged offset identity so decode、fact hashing、audit and old
NTC2 rows remain unambiguous，but the current strategy returns `DROP_UNKEYED` before rewrite，matching stock cleaner。
Empty keys remain normal keyed values and never collide with null-key/control identities。`RETAIN_UNKEYED` remains a
durable NTC2 read disposition for backward compatibility；new stock-compatible plans do not emit it。

Current tests prove latest/older keyed decisions、stock-compatible null-key drop、committed/aborted/open transaction handling、
tombstone and marker equality boundaries、first-pass horizon retention、missing full-scan proof fail-safe retention and
fact-pair rejection。`KafkaCompactionPassOneCollectorTest` now supplies/proves these facts over a dense frozen horizon and
re-proves the output prefix；`KafkaCompactionTwoPassExecutorTest` composes real Kafka decode、strategy、rewrite and NTC2
mapping；`KafkaCompactionWinnerIndexTest` covers the production sorted spill and restart recomputation。
`phase9M5KafkaCompactionOracleCheck` now publishes current product bytes and compares four real stock-cleaner traces，
including exact producer/sequence/control metadata。OPEN crossing remains a product fail-closed test because Kafka does
not clean beyond LSO；it is not claimed as stock stable-prefix differential evidence。

### 10.6.1 Current stock `LogCleaner` differential

Fork `c4a0a2d1fa` adds `KafkaCompactionOracleTest` and `KafkaCompactionOracleSupport` beside the stock log classes。
Every scenario appends the exact same `MemoryRecords` bytes to a legal stock `UnifiedLog` through
`appendAsFollower` and wraps those bytes as Nereus `ReadBatch`/`ExactSourceSet` inputs。The stock side calls
`Cleaner.buildOffsetMap` over the frozen stable horizon and `Cleaner.cleanSegments` over only the explicitly rolled
output coverage；the Nereus side calls `KafkaCompactionTwoPassExecutor.prepare(...).rewrite(...)` with the same output and
decision ranges。The comparison sorts by logical offset and requires equality of：

- key/value bytes、timestamp、compression and delete horizon；
- transactional/control flags、producer id/epoch、record sequence and partition leader epoch；
- survivor offsets，including sparse records rewritten from one idempotent source batch。

The four deterministic traces are：

1. keyed overwrite、null-key invalid-message drop、GZIP/NONE、first-pass tombstone horizon and a newer tail winner；
2. committed data + commit marker、aborted data + abort marker and a newer non-transactional tail winner；
3. preexisting equal-time tombstone and empty-abort-marker horizons，both deleted at `now == horizon`；
4. a two-record idempotent GZIP batch whose first record is superseded，requiring the second record to retain exact
   producer id/epoch and sequence `101`。

The first executions exposed two real product mismatches。Product `666bab1` changes `UNKEYED` from
`RETAIN_UNKEYED` to `DROP_UNKEYED`；the KCK2 null-key tag and NTC2 disposition remain readable compatibility values but
are not emitted by new plans。The same commit changes `effectiveDeleteHorizon` so only tombstones and
`DELETE_ELIGIBLE` markers receive a new `now + delete.retention.ms` value；`RETAIN_REQUIRED` markers preserve an absent
horizon。Product `08fe686` adds `phase9M5KafkaCompactionOracleCheck` to `phase9M5CompactionCoreCheck` and locks fork
`1e3783458b` at 52 commits/126 files。Fork commit `bf8a2946e5` marks only isolated Nereus development modules changing
with zero cache，so a gate cannot silently reuse an older fixed `0.1.0-f9-dev` artifact；the later head additionally owns
the transaction-marker retry fix described in section 15.3。

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
| Kafka fork | `NereusProducerStateManager`、`NereusTransactionIndex`、`NereusCanonicalLogState`、`NereusLogSegment` implemented locally；dedicated `NereusTimeIndex`/`NereusLeaderEpochCache` subclasses are unnecessary unless a later stock caller cannot consume the canonical facade |
| adapter checkpoint | producer/txn/epoch/segment/time/byte section codecs V1 + full composition implemented |
| adapter retention | planner/checkpoint/barrier/DeleteRecords/durable listener + per-partition maintenance + bounded periodic owned-partition runtime implemented；native forced-restart checkpoint/trim and all-five-profile provider-applied response-loss/no-repeat process gates pass，remaining boundaries/stock-oracle matrix pending |
| adapter compaction | codec/strategy/rewrite/policy/planner/coverage/fetch + activated-generation resolver + scheduler/orphan scanner + single-partition pass + bounded owned-partition runtime bridge + projection-free Kafka stream registration/ACTIVE-readiness generation guard + activated Object-WAL production composition + fork registration/concrete partition-lock/KRaft/local-log capture + stock marker pre-scan implemented；real-provider restart/takeover and full cleaner differential gate pending |
| materialization | ranged decoder SPI、V2 two-pass engine/publisher/verifier + explicit projection-required/direct-stream authority modes and caller authority final-CAS fence |
| metadata | binding compaction coverage nested record/codec/transition validators + partition-scoped KCP1 scan continuation |
| object store | NTC2 writer/reader/goldens from document 02 |

### 15.1 Current completed-coordinator migration evidence

Product `7c25d2e` adds
`:nereus-kafka-adapter:f9CoordinatorMigrationProcessIntegrationTest` against the unchanged published Kafka fork
`1cbe8b65a8`。It uses two real release JVMs、one KRaft controller quorum、four-shard Oxia and LocalStack Object-WAL：

```text
node 1:
  user partition [1]
  Produce offset 0
  transaction(data=1, COMMIT=2)
  group rebalance + commit user offset 2

start node 2:
  assert user/__consumer_offsets-0/__transaction_state-0 are all leader/replicas/ISR [1]
  one Admin reassignment moves all three to [2]
  await all three exact [2] states and no ongoing reassignment
  keep node 1 alive

node 2:
  group lookup == 2
  same transactional ID -> data=3, COMMIT=4
  READ_COMMITTED sees offsets 1 and 3
  same group resumes at visible offset 3 and commits 4
  final user earliest/latest = 0/5
```

The gate proves the exact shared-storage handoff that coordinator election depends on：the new broker cannot successfully
answer the group or transaction client until `NereusTopicDeltaLifecycle` has opened and published the recovered internal
partition storage，after which stock coordinator replay consumes those bytes。It also proves old-owner resignation does not
delete either shared binding while the old broker stays live。Fresh execution passes 73/73 actionable tasks in 1m07s；the
baseline cold-restart and ordinary data-handoff regressions pass together at 74/74 in 1m50s。

This is intentionally a completed-state cut；the next gate covers OPEN state。Abort-resolution failure cuts、
BookKeeper/profile migration and upstream coordinator suites remain required；the deterministic and real Object-WAL
mandatory-NTC2 gates are covered separately in section 5.1。

### 15.2 Current ongoing-transaction migration evidence

Product `efe782d` registers `f9OngoingTransactionMigrationProcessIntegrationTest` as a direct
`phase9M6KafkaProcessCheck` dependency。It deliberately moves the data and coordinator truth together so no successful
EndTxn can be explained by a coordinator that never changed owner：

```text
node 1:
  begin txn A -> data=0, LSO=0
  reassign user + __transaction_state-0 [1] -> [2]
  commit txn A through recovered coordinator -> COMMIT=1, LSO=2
  same txn ID A -> data=2, COMMIT=3, LSO=4

node 2:
  begin txn B -> data=4, LSO=4
  reassign user + __transaction_state-0 [2] -> [1]
  abort txn B through recovered coordinator -> ABORT=5, LSO=6

node 1:
  same txn ID B -> data=6, COMMIT=7
  READ_COMMITTED seek(4) -> offset 6
  earliest/latest/read-committed-end = 0/8/8
```

For each direction the Admin request contains both partitions，and completion requires exact singleton
leader/replicas/ISR、empty reassignment and both release JVMs alive。The original `KafkaProducer` object performs
`commitTransaction()` or `abortTransaction()` after handoff，which exercises stock coordinator discovery/retry rather than
constructing a new producer before resolving the OPEN transaction。The ABORT assertion uses eventual LSO convergence：
EndTxn completion may precede the asynchronous WriteTxnMarkers append，so the harness keeps checking broker liveness and
captures both configs/logs if offset 6 does not arrive under `CLIENT_TIMEOUT`。Fresh execution passes 64/64 tasks in 47s
with a 32.753s JUnit case；the adjacent completed coordinator and ordinary handoff regressions pass 74/74 tasks in 1m30s。
This is Object-WAL P/K evidence for KF-TXN-007/012/014，not injected marker response-loss、broker death during resolution、
BookKeeper/profile or mandatory-NTC2 evidence。

### 15.3 Current transaction-resolution process-cut evidence

Product `04e661e` registers `f9TransactionResolutionCutProcessIntegrationTest` as a direct
`phase9M6KafkaProcessCheck` dependency。The task builds a test-only fat Java agent from the isolated
`f9TransactionResolutionFaultAgent` source set；the agent is passed only to broker 2 with `-javaagent` and is absent from
all production artifacts。Its exact seam is
`DefaultKafkaPartitionStorage.append(MemoryRecords, KafkaAppendContext)`：

- advice is inert until the fixture creates the one-shot `arm` file；
- it filters the append through `KafkaAppendContext.tags().get("topic")`，so unrelated internal-topic and recovery
  appends cannot consume the cut；
- `before-provider` skips the real provider invocation and replaces the return value with an incomplete
  `CompletableFuture`；
- `after-provider` lets the real future complete successfully，persists an `applied` marker and replaces only the caller
  completion with an incomplete future；
- a create-once `captured` marker guarantees only one append is cut；separate failure advice records the first synchronous
  or executor exception if the configured boundary is never reached。

The real-process fixture keeps `__transaction_state-0` on broker 1 and moves only the OPEN transaction's user partition
from broker 1 to broker 2。Before arming，it requires broker 2 to serve the migrated record through READ_UNCOMMITTED while
READ_COMMITTED end/LSO remains `0`。It then calls the original producer's `abortTransaction()` and concurrently waits for
the selected boundary，kills broker 2 and restarts broker 2 against the same KRaft/Oxia/S3 directories and binding：

```text
before-provider:
  data=0 durable, abort marker not invoked
  kill broker 2 -> coordinator retains pending marker -> recovered broker receives retry

after-provider:
  data=0 and abort marker durable, caller completion absent
  kill broker 2 -> recovery rebuilds transaction index/LSO from durable bytes

both:
  recovered READ_COMMITTED end >= 2 and <= 3
  aborted data at offset 0 is invisible
  same transactional.id commits next data at recovered end
  final committed marker advances end by exactly 2
```

The first before-provider execution exposed a stock coordinator liveness bug rather than a product-storage bug。During
broker 2 restart，KRaft still contained the partition but reported `leader=-1`；
`TransactionMarkerChannelManager.addTxnMarkersToBrokerQueue` treated an empty leader endpoint as a deleted partition，
removed it from the pending transaction and completed ABORT without ever writing the data-partition marker。Fork
`1e3783458b` changes that branch to：

```scala
metadataCache.getPartitionLeaderEndpoint(topicPartition, listenerName)
  .orElse(if (metadataCache.contains(topicPartition)) Some(Node.noNode) else None)
```

`Node.noNode` routes the marker into the existing unknown-broker queue；a later metadata update drains it to the recovered
leader。Only a partition absent from metadata is skipped。The new
`TransactionMarkerChannelManagerTest.shouldSaveForLaterWhenExistingPartitionHasNoLeader` passes with the focused fork
suite。The product process gate then passes both cuts，and the final forced `--rerun-tasks` execution runs 66/66 tasks in
1m30s。This is Object-WAL P/C evidence for KF-TXN-008 and an injected-resolution slice for KF-TXN-007/014。

Product `2d7091d` closes the profile part of that requirement without changing the fault seam。The original task remains
the fast `OBJECT_WAL_SYNC_OBJECT` gate；new
`f9TransactionResolutionProfileMatrixProcessIntegrationTest` repeats both cuts for
`OBJECT_WAL_ASYNC_OBJECT`、`BOOKKEEPER_WAL_ONLY`、`BOOKKEEPER_WAL_ASYNC_OBJECT` and
`BOOKKEEPER_WAL_SYNC_OBJECT`。Together they execute ten release-process scenarios。Every BookKeeper cut derives a distinct
ledger-id authority seed from `(profileSeed * 2) + cutOffset`，so before/after-provider fixtures never reuse or revoke each
other's namespace。WAL-only requires zero objects；the other four profiles await a positive materialized-object count。

The first real matrix run exposed a production capacity bug in `BookKeeperLedgerHandleCache`。With a two-handle bound，
two completed/released ledger handles remained cached until their one-hour idle deadline；opening a third recovery ledger
returned retriable `BACKPRESSURE_REJECTED` even though neither old handle had an active lease。The cache now evicts the
least-recently-used released entry until one handle/byte slot is available，but still rejects when every cached entry is
referenced。Focused tests lock both branches。The complete profile matrix then passes 66/66 tasks in 6m28s；a second fresh
invocation combines the complete BookKeeper test suite、the Object-sync process gate and 146/146 manifest validation and
passes 78/78 tasks in 1m40s。KF-TXN-008 therefore has current-source P/C/K evidence across all five profiles；the final M4
aggregate remains a separate requirement。

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

Current executable subset：the first two bullets now include completed-state and live OPEN COMMIT/ABORT migration across two
release brokers plus before/after-provider abort-marker process cuts across all five storage profiles；Object-WAL mandatory
internal-topic compaction/no-resurrection is also covered。Non-Object mandatory-NTC2 expansion and the final aggregate
remain open。

### 16.3 Retention/DeleteRecords

- time/size/both/disabled policies against stock virtual-segment oracle；
- group lag does not block trim；
- checkpoint barrier failure/response loss；
- exact quarantine restart lookup、redacted first-failure audit、write-before-fallback and quarantine-store fail-closed；
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
