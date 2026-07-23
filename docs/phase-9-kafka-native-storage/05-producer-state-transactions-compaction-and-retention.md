# 05 — Producer State, Transactions, Compaction and Retention

> 状态：F9-M4 producer/open/aborted canonical state + strict V1 codec partial slice implemented；Kafka import/replay and request semantics remain in progress；F9-M5 designed target
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
timestamps use Kafka sentinel allowlist。No `VerificationStateEntry` is persisted；transaction coordinator verification is
re-established by stock protocol。

`mapEndOffset == NKC1.checkpointOffset`。Import into a fresh manager only；merging with existing state is forbidden。

Current implementation（2026-07-24）：product side now owns the Kafka-artifact-neutral
`KafkaProducerTransactionState`、`KafkaProducerTransactionStateCodecV1` and deterministic
`f9ProducerStatePropertyTest` gate。The model preserves at most five batches in oldest-to-newest order，uses Kafka
sequence wrap arithmetic，requires producer IDs to be strictly sorted and requires `mapEndOffset` to equal the outer
checkpoint offset。The codec is strict big-endian and rejects count/length/flag/version/trailing-byte ambiguity before a
fork can import it。It is not yet evidence that stock `ProducerStateManager` import or replay is complete。

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
queries and checkpoint explicitly marks it as carried-forward。Tail replay adds/extends entries。`endOffsetForEpoch` derives
end from next start or stable end。No local leader-epoch checkpoint file is loaded as truth。

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
`logicalBytes == lastCumulative-firstCumulative`。roll wire IDs：initial、size、time、relative-offset-overflow、config、
manual/test；unknown fail closed。

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

NKC1 section 4 contains descriptor count + descriptors above，followed by config-history entries sorted KRaft metadata
offset：effective roll/retention/compaction fields and SHA-256 digest。No arbitrary Properties map。

## 8. Time and logical-byte indexes

### 8.1 Time index section

Per segment，store sparse entries matching stock index interval：

```text
segmentBaseOffset:i64
entryCount:u32
entries sorted timestamp then offset:
  timestamp:i64
  offset:i64
```

Timestamp lookup chooses floor/candidate then scans exact COMMITTED batches to return first record timestamp `>= target`。
Index never returns final answer without payload verification。

### 8.2 Logical-byte position section

Used for size retention and Kafka `LogOffsetMetadata`：

```text
segmentBaseOffset:i64
segmentLogicalBytes:i64
sampleCount:u32
sample every configured index interval:
  entryStartOffset:i64
  cumulativeLogicalBytes:i64
```

Missing sample falls back to message-offset-only metadata or bounded scan。It never controls data visibility。

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
    void decode(ReadBatch rangedBatch, DecodedRecordConsumer consumer);
    CompactedObjectRow rewrite(DecodedCompactionRecord survivor, RewriteContext context);
}
```

Kafka-aware implementation in `nereus-kafka-adapter`：

```text
KafkaTopicCompactionCodecV1
KafkaCompactionStrategyV1
KafkaRecordRewriteCodecV1
KafkaCompactionPolicyProvider
KafkaCompactionPlanner
KafkaCompactionCoverageCoordinator
KafkaCompactedFetchPlanner
```

`DecodedCompactionRecord` is protocol-neutral and contains offset、tagged key、tombstone flag、event timestamp、source
batch identity/index and opaque rewrite token。Kafka types remain module-local。

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

### 10.6 Transaction/control rules

`KafkaCompactionStrategyV1` must port/encapsulate the selected Kafka `LogCleaner` semantics，not invent a key-only shortcut：

- null-key data is retained by unique offset；
- control records use unique semantic identity and are retained until transaction-marker deletion is safe；
- a transactional data survivor retains transactional flag/producer facts；
- aborted data and markers follow stock cleaner visibility/retention behavior；
- tombstone is retained through `delete.retention.ms` and only dropped with the required full-scan horizon proof；
- open transactions crossing output coverage prevent unsafe cleaning or shrink the eligible end；
- unknown magic/control type/producer invariant aborts task，never drops record。

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
| adapter checkpoint | producer/txn/epoch/segment/time/byte section codecs V1 |
| adapter retention | `KafkaRetentionCoordinator`、`KafkaRetentionPlanner`、`KafkaTrimBarrier` |
| adapter compaction | codec/strategy/rewrite/policy/planner/coverage/fetch classes listed above |
| materialization | ranged decoder SPI、V2 two-pass engine/publisher/verifier |
| metadata | binding compaction coverage nested record/codec/transition validators |
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
