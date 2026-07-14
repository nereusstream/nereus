# Task State Machines, Recovery, Async Materialization, and Checkpoints

## 1. Runtime Components

The new module owns the following runtime graph：

```text
MaterializationService
  |- RegisteredMaterializationStreamScanner
  |- MaterializationPlanner
  |- MaterializationTaskStore (GenerationMetadataStore facade)
  |- MaterializationWorkerPool
  |    |- SourceProtectionManager
  |    |- ExactSourceRangeReader
  |    `- CompactedObjectWriter
  |- GenerationCommitter
  |- TaskRecoveryScanner
  |- MaterializationCheckpointReconciler
  |- RecoveryCheckpointCoordinator
  |- SourceRetirementCoordinator
  |- PhysicalObjectRootScanner
  |- PhysicalObjectGarbageCollector
  `- PhysicalRootTombstoneRetirementCoordinator
```

`MaterializationService` is process-shared and multi-stream. It has no broker/topic ownership assumption. Duplicate
planners/workers on multiple brokers are expected；deterministic task ids and Oxia CAS converge them.

Target construction：

```java
public final class MaterializationRuntime implements AutoCloseable {
    public MaterializationRuntime(
            String cluster,
            String processRunId,
            StreamStorage streamStorage,
            OxiaMetadataStore l0MetadataStore,
            SourceRetirementMetadataStore sourceRetirementStore,
            ObjectAuditRetirementStore objectAuditRetirementStore,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore objectMetadataStore,
            ObjectStore objectStore,
            ReadTargetReaderRegistry readers,
            GenerationProtocolActivationGuard activationGuard,
            MaterializationConfig config,
            PhysicalGcConfig physicalGcConfig,
            ScheduledExecutorService scheduler,
            ExecutorService workerExecutor,
            Executor callbackExecutor,
            Clock clock,
            List<GcReferenceDomain> referenceDomains,
            MaterializationMetricsObserver observer) { }
}
```

Every dependency is non-null and borrowed by `MaterializationRuntime`；it closes its admitted internal work but does
not close stores or shut down injected executors. `DefaultNereusRuntimeProvider` is their sole owner and closes them
after this runtime drains, in the order fixed by document 06. Unit tests use deterministic clocks/executors；
production never uses common fork-join pools.

`GenerationProtocolActivationGuard` is a protocol-neutral core interface. The managed-ledger implementation proves
cluster readiness and the exact projection marker through product metadata；materialization receives only a typed
`GenerationActivationProof` and never imports or decodes Pulsar records.

## 2. Configuration and Limits

```java
public record MaterializationConfig(
        MaterializationPolicy committedPolicy,
        int registryScanPageSize,
        Duration registryScanInterval,
        int plannerPageSize,
        int taskScanPageSize,
        int maxTasksPerPlan,
        int maxConcurrentWorkers,
        int maxConcurrentWorkersPerStream,
        int sourceReadPageRecords,
        long sourceReadPageBytes,
        Path stagingDirectory,
        long maxStagingBytes,
        int uploadChunkBytes,
        Duration workerClaimDuration,
        Duration workerClaimRenewInterval,
        Duration operationTimeout,
        Duration closeTimeout,
        Duration retryMinBackoff,
        Duration retryMaxBackoff,
        int maxTaskAttempts,
        long lagThrottleRecords,
        long lagRejectRecords,
        long lagThrottleBytes,
        long lagRejectBytes,
        Duration lagRejectAge,
        Duration sourceRetirementGrace,
        Duration appendReplayGrace,
        Duration metadataAuditGrace,
        int recoveryCheckpointMaxEntries,
        long recoveryCheckpointMaxBytes) { }
```

Validation：

- committed policy is exactly `COMMITTED + LOSSLESS_REWRITE + NEREUS_COMPACTED_PARQUET_V1` with no topic-compaction
  spec and satisfies document 02 hard limits；
- registry/task/planner page sizes and concurrency are positive and within document 03 hard limits；registry scan
  interval is positive；
- source read pages are positive, records are at most 65,536 and bytes are in `[64 KiB, 64 MiB]`；the byte limit also
  fits the shared core read-buffer reservation and one worker may retain at most one delivered page at a time；
- staging directory is absolute/owner-only，staging bytes cover at least
  `max(committedPolicy.targetObjectBytes(), recoveryCheckpointMaxBytes)` and upload chunks are in
  `[64 KiB, 8 MiB]`；the global staging semaphore is acquired before writing and released exactly once on cleanup；
- renew interval is at most one third of claim duration；
- operation timeout plus maximum clock skew is less than claim duration；
- retry backoff is positive and bounded；
- throttle thresholds are strictly below corresponding reject thresholds；
- source-retirement、append-replay and metadata-audit grace are positive；metadata audit grace is not shorter than
  source-retirement grace；
- checkpoint entry/byte limits do not exceed NRC1 hard limits；
- close timeout is positive。

Configuration changes do not alter an existing task. Policy/format inputs are copied into its immutable identity.

## 3. Task Lifecycle

### 3.1 Closed state set

```java
public enum TaskLifecycle {
    PLANNED(1),
    CLAIMED(2),
    OUTPUT_READY(3),
    PUBLISHING(4),
    PUBLISHED(5),
    RETRY_WAIT(6),
    CANCELLED(7),
    TERMINAL_FAILED(8)
}
```

Transitions：

```text
PLANNED      -> CLAIMED
RETRY_WAIT   -> CLAIMED               after retryNotBefore
CLAIMED      -> CLAIMED               same claim heartbeat only
CLAIMED      -> OUTPUT_READY
CLAIMED      -> RETRY_WAIT
CLAIMED      -> CANCELLED
CLAIMED      -> TERMINAL_FAILED
OUTPUT_READY -> PUBLISHING
OUTPUT_READY -> RETRY_WAIT             only before any possibly committed publication
PUBLISHING   -> PUBLISHED
PUBLISHING   -> OUTPUT_READY            only after proving index absent/ABORTED
PUBLISHING   -> RETRY_WAIT              same proof required

PUBLISHED terminal for execution; retained for audit/checkpoint
CANCELLED terminal
TERMINAL_FAILED terminal
```

`attempt` increments only on `PLANNED/RETRY_WAIT -> CLAIMED`. Terminal fields are immutable after the terminal CAS.

### 3.2 Worker claim

```java
public record WorkerClaimRecord(
        String claimId,
        String processRunId,
        long attempt,
        long claimedAtMillis,
        long expiresAtMillis) { }
```

Claim id is a random 128-bit lowercase base32 value and becomes the output-attempt id. A claimant：

1. loads task and checks `PLANNED` or eligible `RETRY_WAIT`；
2. CASes the task to `CLAIMED` with incremented attempt and bounded expiry；
3. creates every source `MATERIALIZATION_SOURCE` protection；
4. revalidates each physical root after protection creation；
5. only then starts source IO；
6. heartbeats by same-key task CAS before the renewal deadline。

A stale claim cannot write output because `claimId + attempt + task metadataVersion` are required at the output CAS.
After expiry, another worker may reclaim. The old worker must treat heartbeat/output CAS conflict as cancellation and
release local buffers/leases；its uploaded immutable object is harmless until referenced.

Clock-based expiry assumes the configured maximum broker clock skew. Reclaim waits until
`expiresAt + maxClockSkew`; this safety margin is shared with reader leases.

### 3.3 Failure classes

```java
public enum TaskFailureClass {
    NONE(0),
    RETRYABLE_METADATA(1),
    RETRYABLE_OBJECT_STORE(2),
    RETRYABLE_RESOURCE_LIMIT(3),
    SOURCE_CHANGED(4),
    SOURCE_RETIRED(5),
    UNSUPPORTED_MAPPING(6),
    OUTPUT_INVARIANT(7),
    CORRUPT_SOURCE(8),
    CLOSED(9)
}
```

- classes 1-3 enter `RETRY_WAIT` with capped exponential jitter；
- 4-5 cancel the task and ask the planner for a fresh source set；
- 6-8 are terminal and emit an operator-visible reason；
- 9 is retryable after process restart unless shutdown explicitly cancelled the task before claim；
- message/cause classes are diagnostic only and do not control this decision。

## 4. Planner Algorithm

### 4.1 Registered-stream discovery

`RegisteredMaterializationStreamScanner` is the process-wide liveness entry point. It keeps one in-memory continuation
per shard, visits shards `00..63` round-robin and invokes
`scanStreamRegistrations(cluster, shard, continuation, registryScanPageSize)`. End-of-shard clears that continuation；
a completed pass starts again at the prefix, and restart starts every shard at the prefix. Watches only schedule an
earlier pass.

For each decoded registration `R` it performs, under one operation deadline：

```text
validate registry key/shard/record identity
load authoritative L0 StreamMetadata
decode R.projectionRef
requireReady(GENERATION_PUBLISH,
    LiveProjectionSubject(streamId, projectionRef, projectionIdentitySha256), false)
require stream is live, profile matches R and generation operation is admitted
scan the exact per-stream task prefix with bounded pagination
reconcile non-terminal tasks/index-without-task-finalization
load head + recovery root + materialization checkpoint
plan any authoritative committed coverage not represented by a valid task/index
revalidate activation proof immediately before every task/checkpoint mutation
```

`lastHintCommitVersion` may prioritize a stream but never skips head、task or index reads. A stale/deleted projection
cannot be made live by the registration；it is skipped and cleanup is delegated to the domain-validated path in
document 05. A missing/mismatched registration blocks broker activation/async admission as described in document 06.
Therefore a committed head is discoverable even when the creating broker died before task creation and no broker owns
or has loaded the topic. Duplicate scanners are harmless because planning ids and task transitions use deterministic
keys/CAS.

`TaskRecoveryScanner` is not a second global enumeration mechanism. It consumes the validated stream discovered above
and pages `scanTasks(cluster, streamId, continuation, taskScanPageSize)` until completion. A task watch may enqueue the
same stream for lower latency but never substitutes for the registry pass.

### 4.2 Candidate discovery

For `(stream, policy)`：

```text
load head/trim and advisory materialization checkpoint
scan requested view index from max(trimOffset, checkpoint hint)
hydrate all healthy visible/admitted whole-index entries within the bounded scan
build a DAG whose vertices are exact index boundaries and whose edges are complete
  non-split source ranges; adjacent edges must also have equal cumulative-size
  boundary and compatible payload/projection/schema facts; generation zero guarantees
  the baseline path
choose one canonical gap-free/non-overlapping path, ranked by:
  furthest end within range/record/byte bounds;
  then fewest source edges;
  then lexicographic edge sequence of generation descending,
       edge end descending, indexKey unsigned-UTF-8 ascending
form canonical contiguous runs from that path with identical:
  payload format
  projection identity
  logical format
  schema compatibility
  read view
stop at policy max source ranges/range records/target bytes
classify a run eligible only when:
  any selected source is generation zero or has a different policy/physical format; OR
  at least policy.minMergeSourceRanges current-policy higher-generation sources can be
    replaced by one output within every range/byte bound
never split/clip an index source and never create a task whose sole source is one
  healthy current-policy higher generation
```

The checkpoint is only a starting hint. The planner verifies actual index coverage and searches backward when a gap or
missing task is found. A checkpoint ahead of truth is repaired and cannot skip work.

Before task create the planner also scans committed indexes for the deterministic source-set/policy/coverage identity.
An already committed result repairs task/checkpoint state；it does not schedule a rewrite. The eligibility rule makes
a stable policy converge to a fixed set of bounded objects instead of generating an unbounded sequence of semantically
identical generations. A policy digest change may intentionally rewrite once, after which the same fixed-point rule
applies.

The planner path need not equal the per-offset read resolver's highest-generation choice：all admitted `COMMITTED`
sources are lossless and byte-identical for overlapping offsets. Requiring whole edges preserves exact index identity、
logical bytes and cumulative boundaries without inventing per-record metadata. If no complete path reaches a safe
task boundary, planning stops conservatively and reports the first gap；it never clips a larger source around an
overlap. Candidate/edge counts remain under the resolver/task hard limits, so the dynamic-programming path table is
bounded.

Fully trimmed ranges are not materialized. A source entry straddling trim remains whole in V1；planner does not invent
a partial generation-zero target. Later higher-generation compaction may align future ranges, but ordinary read still
starts at current trim.

### 4.3 Canonical task create

The planner calculates source/policy digests and deterministic task id from document 02, then `putIfAbsent`：

- absent：create `PLANNED`；
- present byte-for-byte same immutable identity：reuse current lifecycle；
- same key with different immutable fields：metadata invariant failure；
- terminal cancelled because source changed：the new source set produces a different id；
- already published：advance/reconcile checkpoint only。

`taskSequence` equals the final source `lastCommitVersion`; it is a deterministic scheduling key, not a uniqueness or
visibility field.

Task create is allowed before source protections exist. A worker that cannot acquire a protection because GC marked a
source cancels/replans. GC also scans task records as defense-in-depth but correctness relies on explicit protections
and the root handshake, not on catching an instantaneous task-create scan.

### 4.4 Overlap policy

At most one local active task per `(stream, view, policy, overlapping range)` is scheduled, but correctness does not
depend on this local rule. Concurrent brokers may publish overlapping lossless generations. Unique generation numbers
give a deterministic winner；the retirement planner later removes fully dominated targets.

For `TOPIC_COMPACTED`, a task source coverage is immutable and append-only offsets inside that range cannot change.
Strategy id/version is part of task identity, so a later policy is a new generation rather than an in-place semantic
mutation.

## 5. Worker Algorithm

### 5.1 Exact source reader

The worker must not call ordinary `StreamStorage.read` and accidentally consume a newer target than the task snapshot.
It uses：

```java
public interface ExactSourceRangeReader {
    CompletableFuture<ExactSourceRead> read(
            SourceGeneration expected,
            ReadOptions options);
}

public interface ExactSourceRead extends AutoCloseable {
    SourceGeneration source();
    Flow.Publisher<ReadBatch> batches();
    CompletableFuture<ExactSourceReadSummary> completion();
    @Override void close();
}

public record ExactSourceReadSummary(
        OffsetRange coverage,
        int recordCount,
        int entryCount,
        long logicalBytes,
        Checksum orderedPayloadSha256) { }
```

The implementation acquires a durable lease for `expected.readTarget`, re-reads the exact index key/version/identity，
and dispatches that exact target. If it changed/disappeared, the read fails `SOURCE_CHANGED`; it never silently
substitutes another generation while continuing to claim the old source digest.

The publisher is cold and single-subscription. It pages the selected target using
`sourceReadPageRecords/sourceReadPageBytes`, emits immutable `ReadBatch` values only under demand and holds the exact
object lease until terminal completion or `close`. At most one emitted page may be outstanding；cancel/decoder error
closes the current reader future and releases the lease exactly once. `completion` succeeds only after dense ordered
coverage and all counts/bytes/digest match the source snapshot. The worker consumes sources sequentially, so task
memory is bounded independently of its 1 GiB object limit.

### 5.2 Lossless rewrite

```text
for each canonical source:
  acquire exact source lease and revalidate protection/root/index
  read within task deadline/resource budget
  verify offset/count/payload/projection/checksums
  append rows in strictly increasing offset order

verify final dense coverage and logical/cumulative totals
stream into bounded private staging file under the global byte permit
finalize/seal Parquet footer and calculate content SHA-256 + storage CRC32C
derive generation-neutral key/object id from content SHA-256 + durable worker claim/outputAttemptId
reread exact CLAIMED task/claim/version and optional physical root;
  require the root absent or the same ACTIVE identity
put if absent through ReplayableObjectUpload without whole-object heap aggregation;
  its PutObjectAttemptGuard repeats that task/claim/root proof before every provider transmission
HEAD and strict-read the footer + sampled/all payload according to gate
create/register physical root if absent
create MATERIALIZATION_OUTPUT protection and revalidate root
CAS CLAIMED -> OUTPUT_READY with exact output
release read leases; keep source/output durable protections
close/delete staging file and release its byte permit on every completion/cancel path
```

The production publish gate performs a complete logical scan of the new object's offset/CRC columns. Tests and final
acceptance perform byte-for-byte full comparison. Sampling alone cannot publish.

For `PULSAR_ENTRY_V1`, every source `ReadBatch` must contain one exact Entry per offset. Any normalization, missing
batch boundary or Entry parse/re-encode attempt is `OUTPUT_INVARIANT`.

### 5.3 Topic-compaction worker

The worker obtains a registered `TopicCompactionDecoder` for the task policy. It performs a deterministic two-pass
algorithm bounded by task range/resource limits：

1. pass one builds latest-offset-by-key according to strategy, spilling sorted immutable runs when memory budget is
   reached；
2. pass two emits surviving VALUE/TOMBSTONE rows in offset order；
3. it validates dense source coverage and sparse output count；
4. output uses the separate NTC1 format/keyspace；
5. no ordinary generation/index API accepts the result。

This primitive is tested in F4 but is not scheduled from Pulsar compaction policy until F8.

## 6. Publish Recovery

### 6.1 `OUTPUT_READY -> PUBLISHING`

The first CAS freezes：

- publication id；
- exact output identity；
- source/policy digests；
- publishing start time。

The task initially enters `PUBLISHING(publicationId, allocatedGeneration=empty)`. The owner then allocates G and
CASes the exact pair into the same task before creating an index. If the allocation response is lost and another
stream/view allocation interleaves, a retry may burn a second G for the same publication id；only the G that wins the
task CAS can ever be used by `createPrepared`. Once the task contains G, recovery never calls the allocator again and
never allocates a replacement until the exact index outcome is proved absent/`ABORTED`.

The candidate CAS generates one random 128-bit lowercase-base32 publication id. Response loss does not regenerate it：
recovery reloads the task and uses the stored value. A later, proved-aborted transition back to `OUTPUT_READY` clears
both publication id and generation；only the next distinct publish attempt then generates a new id.

### 6.2 Reconcile algorithm

```text
load task T
if T is OUTPUT_READY:
  CAS -> PUBLISHING(new stable publication id, no G)
if T is PUBLISHING with no G:
  allocate; CAS one returned G into T; every losing/unattached G is a permanent gap

load exact index key K(view, end, G)
if K absent:
  require no committed result for publication id
  create PREPARED and continue normal publish
if K PREPARED same identity:
  verify all preconditions; COMMIT or ABORT
if K COMMITTED same identity:
  CAS-transfer visible protection owner task -> exact K; CAS task -> PUBLISHED
if K ABORTED same identity:
  prove no committed K, conditionally remove task-owned visible protection,
  CAS task -> OUTPUT_READY/RETRY_WAIT clearing publication/G;
  allocate a new publication id/G only afterward
if K QUARANTINED/DRAINING/RETIRED same identity:
  task is historically PUBLISHED; record terminal publication health separately
else:
  fail metadata invariant
```

CAS conflict or response loss at any step reloads durable state. `MAY_HAVE_COMMITTED` is resolved from the exact index
record, never inferred from an exception type.

### 6.3 Protection release

- source task protections remain until task is proven `PUBLISHED` and no recovery/checkpoint builder is reading the
  task snapshot；
- output task protection remains until committed visible-generation protection is re-proved；
- a task-owned visible-generation protection is CAS-rebound to the exact committed index before task publication；
- PREPARED/ABORTED output retains task protection during retry/orphan grace；
- terminal failed/cancelled tasks remove protections only after proving no PREPARED/COMMITTED index references the
  output。

## 7. Advisory Materialization Checkpoint

### 7.1 Reconciliation

The reconciler scans from the current checkpoint and advances `contiguousCoveredOffset` while every offset is covered
by a healthy `COMMITTED` generation satisfying the policy. It stores the highest source commit version observed.

Rules：

- gaps、PREPARED、QUARANTINED or wrong policy stop advancement；
- an overlapping generation counts only for its declared coverage；
- task PUBLISHED without committed index does not count；
- committed index without task PUBLISHED does count and repairs the task asynchronously；
- checkpoint loss/corruption causes a bounded rebuild from trim/recovery root；
- primary WAL/source retention never uses the checkpoint alone。

Lag metrics are `head - verified checkpoint` plus exact unmaterialized source bytes/age; they are not calculated from
the number of task records.

### 7.2 Terminal workflow-metadata retirement

Task/checkpoint/stats deletion never changes read or append truth. The per-stream scanner may conditionally delete a
task only after `updatedAtMillis + metadataAuditGrace < now` and one of these complete proofs：

- `PUBLISHED`：the exact index is `COMMITTED`, its visible protection is owned by that index, the advisory checkpoint
  covers the task range/sequence and no task-owned source/output protection remains；
- `CANCELLED/TERMINAL_FAILED`：no `PREPARED/COMMITTED` index exists for its publication/output and every task-owned
  protection is absent after owner/root revalidation。

It reloads task/index/checkpoint/root immediately before `deleteTask(expectedVersion)`. Conflict or response loss
restarts from exact reads. A live stream keeps its current policy checkpoint. Stale `RangeRetentionStatsRecord`s are
conditionally removed when their whole range is below a stable completed trim or their source index identity/version
is gone；their absence only makes planning conservative. Sequence、current checkpoint and recovery-root removal is
reserved for the deleted-stream final proof in document 05. There is no time-only deletion of a non-terminal task.

A live stream may conditionally retire an old `(policyId, policyVersion)` checkpoint after metadata audit grace only
when the current projection policy is a different exact version, every task for the old version is terminal/retired,
and an index/task-prefix rescan plus projection revalidation is unchanged. This never deletes the current-policy
checkpoint and never uses old progress to seed the new version.

## 8. Recovery Checkpoint Protocol

### 8.1 Why it is required

Current `StreamHeadRecord.lastCommitId` anchors a chain to genesis. Those commit records contain generation-zero
targets and are the repair/replay source. Deleting source bytes/index keys without replacing that role would make
append replay and missing-index repair depend on already deleted data.

F4 therefore changes the valid append proof from：

```text
head -> live commit nodes -> genesis
```

to：

```text
head -> bounded live tail -> RecoveryCheckpointRoot -> immutable NRC1 prefix
```

The head CAS remains the append linearization point. The checkpoint is a compaction of recovery evidence, not a new
append commit.

### 8.2 Eligible checkpoint prefix

A prefix ending at commit `C` is eligible only when：

- `C` is currently reachable from head or the previous active checkpoint bridge；
- every commit/range is fully reconstructed from exact current records；
- every untrimmed offset is covered by one or more healthy committed lossless generations；
- those target objects have `RECOVERY_CHECKPOINT_TARGET` protections；
- ranges wholly below completed L0 trim need no readable target but the checkpoint records their scalar chain/audit
  identity according to append replay grace；
- no exact in-process append recovery handle/task needs a source record being removed；
- source commits are older than `appendReplayGrace`；
- all NRC1 count/size/root bounds hold。

The coordinator never checkpoints a partial commit. `coveredEndOffset` is a commit boundary.

### 8.3 Build and publish

```text
read root R1 + head H
requireReady(RECOVERY_CHECKPOINT, exact live projection, activateIfAbsent=true)
walk and validate live commit prefix/tail bridge
read/lease committed generation records
generate checkpointAttemptId, canonicalize exact generic commit envelopes,
deduplicate exact COMMITTED generation-index codec bytes,
and build canonical NRC1 publication table/commit entries containing both
put-if-absent content-verified attempt-addressed checkpoint object;
  before every provider transmission, the guard reloads exact R1, attempt/set digest and optional root,
  and accepts only unchanged R1 plus absent root or the same ACTIVE identity
HEAD + full strict decode/checksum validation
create bounded RECOVERY_CHECKPOINT_PENDING protection on the checkpoint object,
  owned by exact R1 version + next sequence/object/set digest
reread head and root R2; require R2.version == R1.version and prefix remains reachable
revalidate activation proof, pending protection/root and its skew-safe expiry
construct new root Rnext (append or merge refs, monotonic coverage)
CAS recovery root R1 -> Rnext                 CHECKPOINT PUBLICATION
reread Rnext and checkpoint bytes
create permanent CHECKPOINT_OBJECT and CHECKPOINT_TARGET protections owned by exact Rnext
revalidate every target root/index and Rnext; remove pending protection
only then schedule old live commit/index/source retirement
```

CAS loss leaves a bounded pending protection and orphan checkpoint object that recovery can reuse if identical or
reclaim after expiry. If the root CAS succeeded but its response/permanent-protection writes were lost, root-domain
revalidation repairs permanent protections before removing pending. None of these workflow records changes replay
truth before the root CAS.

### 8.4 Live-tail bridge

After checkpoint publication：

- if `head.lastCommitId == root.lastCommitId`, the complete committed history is in the checkpoint and the live tail
  may be empty；
- otherwise walking back from head must reach a live commit whose `previousCommitId == root.lastCommitId` and whose
  offset/commitVersion/cumulative predecessor scalars equal root covered end；
- new append continues storing `previousCommitId = head.lastCommitId` even if that id is checkpoint-resident；
- chain readers detect the bridge from the recovery root rather than requiring the predecessor key to exist。

### 8.5 Replay and index repair

`searchAppendReplay` searches the bounded live tail first and then NRC1 directories by commit id/version. It decodes
the canonical commit envelope and runs the same request-vs-record validation as the live generic record. A checkpoint
entry returns the same stable offset/cumulative/schema/projection result；the historical primary target remains audit/
result identity only, while its current committed publication-table target is used for read repair.

`repairDerivedStreamIndexes`：

- below current L0 trim：returns a trimmed result and creates no index；
- inside checkpoint coverage：decodes the publication table's exact canonical committed-generation record,
  revalidates its physical root/protection/activation and `putIfAbsent` recreates or validates the identical index；
  it never regenerates a deleted generation-zero target；
- in live tail：uses the existing commit record path；
- a root change during either path restarts the proof。

### 8.6 Merge and retirement

At 32 root references or configured object threshold, a merge task reads/pins current checkpoint objects and writes a
new NRC1 object with identical canonical entries. Root CAS atomically replaces the reference list. Old checkpoint
objects are GC candidates only after all replay readers release leases and the newer root is revalidated.

Live commit keys below the new root, their generic committed markers and generation-zero indexes become retirement
candidates. Conditional metadata deletion and physical source deletion follow document 05；root publication alone
does not perform those side effects.

## 9. Object-WAL Async Profile

### 9.1 Supported matrix after F4-M5

| Profile | Durability accepted | Success boundary | Secondary work |
| --- | --- | --- | --- |
| `OBJECT_WAL_SYNC_OBJECT` | `WAL_DURABLE_AND_INDEX_COMMITTED` | protected head + protected generation-zero index | higher generation async |
| `OBJECT_WAL_ASYNC_OBJECT` | `WAL_DURABLE` | primary Object WAL + protected commit intent + head | gen-0 repair + higher generation async |
| `OBJECT_WAL_ASYNC_OBJECT` | `WAL_DURABLE_AND_INDEX_COMMITTED` | protected head + protected generation-zero index | higher generation async |
| all BookKeeper profiles | none until BK adapter gate | rejected before IO | F4 machinery remains reusable |

`OBJECT_WAL_ASYNC_OBJECT` still uploads its primary Object WAL to object storage before success. It removes secondary
index/read-optimized publication from the ack path；it is not a local-WAL latency claim. The full
BookKeeper-WAL-then-async-object profile requires the separate BookKeeper adapter.

### 9.2 Append state machine

```text
ACCEPT
 -> PROFILE/SESSION VALID
 -> OBJECT WAL PREPARED + UPLOADED
 -> COMMIT INTENT STORED/REUSED
 -> PHYSICAL ROOT ACTIVE + REACHABLE_APPEND PROTECTION ACQUIRED
 -> STREAM HEAD CAS/REPLAY PROVED
 -> if WAL_DURABLE: return exact AppendResult
 -> repair/materialize generation zero
 -> VISIBLE_GENERATION PROTECTION ACQUIRED FOR EXACT INDEX
 -> if INDEX_COMMITTED: return exact AppendResult
 -> planner discovers higher-generation work
```

The concrete two-stage interfaces、identities and crash cuts are document 03 §10. `AppendCoordinator` no longer calls
one combined metadata `commitStableAppend`; it invokes prepare, `GenerationZeroPhysicalReferencePublisher`, then the
protected head commit. `GenerationZeroIndexMaterializer` returns `MaterializedGenerationZero`, so strict success can
prove the exact index-owned protection instead of assuming that a successful index put is physically durable.
Append recovery runs the same idempotent sequence and never acknowledges from an unprotected replay result.

Admission proves the exact stream registration and current projection activation before primary IO；a stream cannot
enter the async profile with only an in-memory enqueue path. The ack does not wait for task creation. A crash after
head commit but before enqueue is repaired when the durable registry scan reloads the authoritative head and scans the
anchor-reachable commit history/index gap. Therefore task loss、watch loss、topic unload and broker failover cannot lose
acknowledged data or permanently hide its materialization work.

### 9.3 Read after `WAL_DURABLE`

When ordinary resolve finds no index at a committed offset：

1. read head/recovery root and prove the offset committed；
2. run the existing bounded `repairDerivedStreamIndexes` against live tail/checkpoint；
3. re-resolve the new generation-zero/higher entry；
4. finish within the caller read deadline or return a retriable metadata/materialization error；
5. never return empty/end-of-stream for an index gap below committed end。

F4 configuration reserves separate repair concurrency so a materialization backlog cannot starve read-after-write.

### 9.4 Lag/backpressure

Signals：

```text
nereus_materialization_lag_offsets{profile,view}
nereus_materialization_lag_bytes{profile,view}
nereus_materialization_oldest_lag_seconds{profile,view}
nereus_materialization_registry_scan_total{outcome,shard}
nereus_materialization_registry_scan_age_seconds
nereus_materialization_registration_mismatch_total{reason}
nereus_materialization_task_count{state,failure_class}
nereus_materialization_publish_total{outcome}
nereus_materialization_primary_fallback_total{target_type}
nereus_materialization_append_throttle_total{reason}
nereus_materialization_append_reject_total{reason}
```

At throttle threshold, append admission applies bounded delay before primary IO. At reject threshold/age, new async
appends fail retriably before primary IO while reads/repair/workers continue. Existing acknowledged primary ranges are
never discarded to reduce lag.

## 10. Worker/Runtime Close

Close order：

```text
stop registry/planner/task scans and reject new claims
stop append-side task hints
allow admitted publication CAS/recovery to resolve until deadline
stop claim heartbeats; release local source/read leases
leave durable tasks/protections for another process
close GC/checkpoint coordinators and complete the runtime close future
outer provider then shuts worker/scheduler/callback executors
outer provider closes metadata/object dependencies only after every F4 future has drained
```

Close never CASes a possibly committed task back to retry without first reconciling its index. Executor rejection is
handled like an async failure and completes every admitted future exactly once.

## 11. Failure Matrix

| Failure | Durable state | Recovery/outcome |
| --- | --- | --- |
| duplicate planners create same task | one deterministic key | compare identical and share task |
| registration create/refresh response lost | one versioned shard key | reload exact identity；never create a second route |
| topic unload/restart with no task | registration + committed head | shard scan recreates deterministic task from head/index gap |
| registration stale/missing/mismatched | never stream truth | skip mutation；broker activation/async admission fails closed |
| worker dies after claim | expired `CLAIMED` | new claim after skew-safe expiry |
| source GC marks during protection acquisition | root epoch/state changes | protection post-check fails；cancel/replan |
| source changes after protection | immutable bytes + protection | task remains valid；publish admission may replan for efficiency |
| worker uploads before output CAS | unreferenced content-verified attempt object | attach on same-claim retry or orphan GC |
| worker loses claim before a provider retry | exact guard reload fails | no retry bytes sent；new claim uses a fresh output key |
| output CAS succeeds, response lost | `OUTPUT_READY` | reload exact output |
| generation allocated, process dies | counter gap/task may record G | gap safe；reconcile task before new allocation |
| PREPARED index, worker dies | invisible PREPARED | recovery commits or aborts |
| COMMITTED index, task still PUBLISHING | visible index | repair task/checkpoint from index |
| terminal task delete races checkpoint/index change | expected-version/final revalidation loses | retain and retry；visibility unchanged |
| checkpoint upload, root CAS loses | old root authoritative | reuse identical bytes or orphan GC |
| checkpoint owner/root changes before provider retry | guarded transmission is rejected | rebuild with a fresh checkpoint attempt id |
| root CAS succeeds before commit-key retirement | new checkpoint authoritative | idempotent retirement resumes |
| commit key deleted while old replay scan runs | old scan fails final root version check | restart against checkpoint |
| append intent stored before physical protection | head unchanged | retry root/protection handshake；no acknowledgement |
| protected head commits before gen-0 index | head/commit + `REACHABLE_APPEND` authoritative | async ack legal；read repair or retriable timeout, never false EOS |
| gen-0 index exists before index protection | reachable protection remains | repair `VISIBLE_GENERATION` before strict success or commit retirement |
| object store unavailable | tasks retry；primary Object WAL reads may also fail | backpressure, no metadata visibility change |
| shutdown during publish response loss | task/index may have committed | next process exact reconciliation |

## 12. Deterministic Test Contract

Tests inject failure immediately before and after every task/index/root CAS and every object PUT/HEAD/read. Required
models include：

- two planners and four workers on the same/overlapping ranges；
- 64-shard fairness、pagination restart、stale hint/watch loss and duplicate registry scanners；
- unloaded stream with committed head but no task is rediscovered after a fresh process starts；
- claim expiry/heartbeat/clock-skew boundary and stale worker output CAS；
- exact source reader refusing silent generation substitution；
- all publication response-loss outcomes；
- task/checkpoint corruption and rebuild from index；
- terminal task/stale stats conditional retirement, response loss and no deletion before audit grace；
- recovery checkpoint root changes during append、replay、repair and merge；
- deletion of every old live commit key only after checkpoint publication；
- async ack crash before task/gen-0 index plus restart read repair；
- every prepare/root/protection/head/index-protection cut for sync and async durability, including response loss；
- lag throttle/reject before IO and recovery workers continuing under backpressure；
- close/executor rejection with no hung or duplicate completion。
