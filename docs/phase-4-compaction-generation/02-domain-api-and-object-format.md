# Domain API and Object Formats

## 1. Design Rules

Java below is the normative Phase 4 target surface. The F4 API/metadata/object-store foundations、core
physical-reference/reader-pin/activation-proof values and the F4-M3 compacted-Parquet writer/strict-reader、policy、
whole-index planner、task-store、task-recovery and registered-stream-scanner checkpoints identified in document 07
are implemented. The exact-source reader/lossless-row/claim-to-output-ready worker、task-protection crash-cut
reconciliation、advisory checkpoint reconciler、bounded M3 service lifecycle and Pulsar Entry/NCP1 exact-byte round
trip are also implemented. The protocol-neutral topic-compaction decoder/strategy SPI、exact frozen-identity
registry、COMMITTED-source planner bootstrap、collision-free tagged-v1 key namespace、checksum-verified sorted-spill
two-pass engine、NTC1 worker/publication path and proof-driven terminal workflow-metadata retirement are implemented
and covered by focused tests. The M3 ordinary/final gates passed on 2026-07-15. F4-M4 is complete/final-gated：the NRC1
domain values、private-staging streaming writer、strict header/footer/directory/range reader、attempt/key identity and
authoritative F4 metadata-record verifier described in section 9 are implemented and focused-tested. The document 03
§10 generation-zero protocol is also implemented for ordinary and recovery append paths：exact prepared intent、
physical root/protection proof、protected head CAS、exact index identity and strict visible protection are now the
production sequence. Recovery-root publication、checkpoint append replay and checkpoint-derived index repair are now
implemented checkpoints. Exact source/object-audit retirement metadata adapters、bounded/reconstructable GC
config/candidate/plan values、the exact bounded reference-domain registry、recoverable
`ACTIVE -> MARKED -> DELETING` root fencing、the 256-shard physical-root scanner、query-bound domain revalidation and
affected-stream generation/append-recovery/materialization domains are also implemented. The bounded canonical
reference-snapshot builder and activation-gated global scope now live in core, and affected plus ownerless
projection-generation/cursor-snapshot domains are implemented over exact F2/F3 authority wrappers. Every domain reference must
match one exact planned metadata removal；active task/live-tail and non-DRAINING higher-generation references veto.
The collector stops at durable delete intent and deliberately exposes no metadata/protection/object delete call；
typed source destructive recovery and the separate DELETED-root/Phase 1 audit-retirement coordinator are now
implemented. Checkpoints X–AF additionally deliver durable registration/backfill/activation、the protected async
Object-WAL boundary and the coupled production generation-aware read/replay/source-repair/materialization runtime.
Checkpoints AG–AI additionally deliver exact logical-retention planning/service semantics、the shared bounded execution
lane、production managed-ledger/facade installation and exact Pulsar retention/backlog policy plus loaded/unloaded
admin admission. Checkpoints AJ–AN additionally implement cursor-snapshot discovery/execution、the current-writer
known-prefix inventory and exact missing-root registration boundary、registration-last retirement and the strict
metadata-first lifecycle. Checkpoint AO maps the exact bounded broker physical-GC configuration into that runtime；
checkpoint AP implements the configured-scope object-store PUT/HEAD/LIST/exact-DELETE capability proof；checkpoints
AQ–BC add atomic activation、runtime routing、restart/scale/failure cuts and deletion-active readiness rollover. The
retry-disabled real two-broker gate proves exact source deletion、compacted reads、MessageId stability、unload/
failover/restart and BookKeeper coexistence. Package、class and method names
are normative unless a review replaces them together with
every caller/test listed in document 07.

The domain model obeys these rules：

- half-open ranges are always `[startOffset, endOffset)`；
- `generation == 0` is append-derived；F4 allocation starts at 1；
- generation order exists only inside one `(streamId, ReadView)`；
- `COMMITTED` output is dense and byte preserving；
- `TOPIC_COMPACTED` output may be sparse but carries explicit dense source coverage；
- task identity、publication identity、object identity and reader lease identity are distinct values；
- `metadataVersion` is hydrated from Oxia and encoded as zero；
- every future/async method fails through its future and does not synchronously leak ordinary IO failures；
- close rejects new work and drains/cancels admitted work within a configured deadline。

## 2. Planned Source Layout

```text
nereus-api/src/main/java/com/nereusstream/api/
  ReadView.java
  GenerationId.java
  PublicationId.java
  ObjectKeyHash.java

nereus-core/src/main/java/com/nereusstream/core/read/
  StreamViewReader.java
  ViewReadResult.java
  ReadTargetReader.java
  ReadTargetReaderKey.java
  ReadTargetReaderRegistry.java
  GenerationReadResolver.java
  GenerationReadCandidate.java
  GenerationIndexRepairer.java
  GenerationIndexRepairResult.java
  GenerationIndexRepairSource.java
  MetadataGenerationIndexRepairer.java
  PinnedResolvedRange.java
  ParquetCompactedTargetReader.java

nereus-core/src/main/java/com/nereusstream/core/capability/
  GenerationProtocolActivationGuard.java
  GenerationOperation.java
  GenerationActivationSubject.java
  LiveProjectionSubject.java
  DomainValidatedDeletionSubject.java
  GenerationActivationProof.java

nereus-core/src/main/java/com/nereusstream/core/recovery/
  CheckpointDerivedIndexRepairer.java
  RecoveryCheckpointProtectionIdentities.java

nereus-object-store/src/main/java/com/nereusstream/objectstore/
  PutObjectAttemptGuard.java
  ObjectPutRetryPolicy.java
  ObjectStoreDeleteCapabilityProbe.java
  ObjectStoreDeleteCapabilityRequest.java
  ObjectStoreDeleteCapabilityProof.java
  DefaultObjectStoreDeleteCapabilityProbe.java

nereus-object-store/src/main/java/com/nereusstream/objectstore/staging/
  ManagedStagingFile.java
  StagedObjectFile.java
  PrivateStagedObjectFile.java
  PrivateStagingSpillFile.java
  StagingFileManager.java

nereus-object-store/src/main/java/com/nereusstream/objectstore/compacted/
  CompactedObjectFormatV1.java
  CompactedObjectFormatException.java
  CompactedObjectMetadata.java
  CompactedObjectRow.java
  CompactedObjectWriteRequest.java
  CompactedObjectWriteResult.java
  CompactedObjectWriter.java
  TopicCompactionFormatSpec.java
  CompactedObjectReadRequest.java
  CompactedObjectReadResult.java
  CompactedObjectReader.java
  ParquetCompactedObjectWriter.java
  ParquetCompactedObjectReader.java
  TopicCompactedObjectReader.java
  TopicCompactionKeyEncodingV1.java
  CompactedObjectVerificationRequest.java
  CompactedObjectVerifier.java

nereus-object-store/src/main/java/com/nereusstream/objectstore/checkpoint/
  RecoveryCheckpointCodecV1.java
  DefaultRecoveryCheckpointCodecV1.java
  RecoveryCheckpointFormatV1.java
  RecoveryCheckpointFormatException.java
  RecoveryCheckpointObject.java
  RecoveryCheckpointEntry.java
  RecoveryCheckpointPublication.java
  RecoveryCheckpointWriteRequest.java
  RecoveryCheckpointWriteResult.java
  RecoveryCheckpointDirectory.java
  RecoveryCheckpointVerifier.java

nereus-materialization/src/main/java/com/nereusstream/materialization/recovery/
  MetadataRecoveryCheckpointVerifier.java

nereus-materialization/src/main/java/com/nereusstream/materialization/
  MaterializationPlanner.java
  DefaultMaterializationPlanner.java
  MaterializationPolicyFactory.java
  MaterializationTaskStore.java
  MaterializationTaskRecovery.java
  TaskRecoveryScanner.java
  RegisteredMaterializationStreamScanner.java
  MaterializationCheckpointReconciler.java
  DefaultMaterializationCheckpointReconciler.java
  MaterializationTaskDispatcher.java
  DefaultMaterializationTaskDispatcher.java
  MaterializationWorker.java
  GenerationCommitter.java
  MaterializationService.java
  DefaultMaterializationService.java
  MaterializationConfig.java
  MaterializationMetricsObserver.java
  MaterializationPolicy.java
  MaterializationTask.java
  MaterializationOutput.java
  SourceGeneration.java
  TopicCompactionDecoder.java
  CompactionRecord.java
  CompactionDisposition.java
  TopicCompactionStrategy.java
  TopicCompactionRegistry.java
  TopicCompactionEngine.java
  DefaultTopicCompactionEngine.java
  TopicCompactionPlan.java
  ExactSourceBatchPublisher.java
  TopicCompactionRowPublisher.java
  TerminalWorkflowMetadataRetirer.java
  DefaultTerminalWorkflowMetadataRetirer.java
  TerminalWorkflowMetadataRetirementResult.java
  CompactedMaterializationFormatVerifier.java
```

The new module also contains task/recovery/GC implementation packages described in documents 04 and 05.

## 3. Stable API Values

### 3.1 Read view

```java
package com.nereusstream.api;

public enum ReadView {
    COMMITTED(1),
    TOPIC_COMPACTED(2);

    private final int wireId;

    ReadView(int wireId) { this.wireId = wireId; }
    public int wireId() { return wireId; }
    public static ReadView fromWireId(int wireId) { /* strict closed mapping */ }
}
```

Wire ids are durable registry values, not ordinals. Unknown values fail decode. `StreamStorage.read(...)` and
`resolve(...)` remain compatibility methods fixed to `COMMITTED`; Phase 4 does not add a view argument to those
existing signatures.

The storage-level semantic primitive is exposed separately：

```java
public interface StreamViewReader extends AutoCloseable {
    CompletableFuture<ViewReadResult> read(
            StreamId streamId,
            long startOffset,
            ReadView view,
            ReadOptions options);
}

public record ViewReadResult(
        ReadView view,
        ReadResult result,
        long sourceCoverageEndOffset) { }
```

`StreamViewReader` is initially an internal runtime service. F8 may expose `TOPIC_COMPACTED` through Pulsar only after
its compatibility gate；ordinary F2/F3 calls never accidentally inherit a caller-controlled view.
`sourceCoverageEndOffset` is the exclusive dense source-coverage cursor consumed by the call. For `COMMITTED` it is
equal to `result.nextOffset()`；for a sparse `TOPIC_COMPACTED` result it may be greater than the last emitted row but
may never exceed the selected generation's declared coverage end. Construction requires matching stream/request
identity、a non-decreasing result cursor and `sourceCoverageEndOffset >= result.nextOffset()`；empty sparse output
advances only through explicitly proven source coverage and is never interpreted as ordinary end-of-stream.

The protocol-neutral activation boundary is：

```java
public enum GenerationOperation {
    GENERATION_PUBLISH,
    RECOVERY_CHECKPOINT,
    LOGICAL_TRIM,
    PHYSICAL_DELETE,
    TOPIC_COMPACTED_PUBLISH
}

public sealed interface GenerationActivationSubject
        permits LiveProjectionSubject, DomainValidatedDeletionSubject { }

public record LiveProjectionSubject(
        StreamId streamId,
        ProjectionRef projectionRef,
        Checksum projectionIdentitySha256) implements GenerationActivationSubject { }

public record DomainValidatedDeletionSubject(
        GcReferenceQuery referenceQuery,
        Checksum projectionDomainSnapshotSha256) implements GenerationActivationSubject { }

public record GenerationActivationProof(
        GenerationOperation operation,
        GenerationActivationSubject subject,
        Checksum subjectSha256,
        long subjectValidationVersion,
        long clusterActivationMetadataVersion,
        long brokerCapabilityReadinessEpoch,
        Checksum referenceDomainSetSha256,
        boolean publicationEnabled,
        boolean deletionEnabled,
        long provedAtMillis) { }

public interface GenerationProtocolActivationGuard {
    CompletableFuture<GenerationActivationProof> requireReady(
            GenerationOperation operation,
            GenerationActivationSubject subject,
            boolean activateLiveProjectionIfAbsent);

    CompletableFuture<Void> revalidate(GenerationActivationProof proof);
}
```

Only a `LiveProjectionSubject` may request first activation. `DomainValidatedDeletionSubject` is legal only for
`PHYSICAL_DELETE` after the projection reference domain has produced the exact snapshot for that query. The guard checks the
operation-specific publication/deletion bit. For a live subject, the product adapter resolves the opaque
`ProjectionRef` to the current authoritative projection, verifies its complete immutable identity digest and captures
that record's Oxia version as `subjectValidationVersion`；the caller never supplies a possibly stale mutable version.
For a domain-validated deletion, `subjectValidationVersion == 0` because a multi-authority snapshot has no truthful
single metadata version；the guard recomputes the exact projection-domain digest, while GC separately calls
`stillMatches` on every captured domain snapshot/authority token. `revalidate` reloads the subject carried by the
proof and is mandatory immediately before every index/root/trim CAS.
The proof is ephemeral and never becomes visibility or deletion truth.

Construction is strict：`LiveProjectionSubject` requires a valid ref and SHA-256 identity；the product guard must
resolve that opaque ref and prove its decoded stream equals `streamId` before issuing a proof.
`DomainValidatedDeletionSubject.referenceQuery` is the exact core query defined in
document 05, including physical object、query kind、candidate evidence and canonical affected-stream set；its digest
must be the complete `projection-generation-v1` snapshot for that same query identity. Referenced/snapshot candidates
require a non-empty stream set；only `OWNERLESS_ORPHAN_CANDIDATE` may carry an empty set, and its domain proof must
establish cluster binding/projection absence rather than treating emptiness as permission.
`GenerationActivationProof.subjectSha256` hashes the canonical subject fields and `subjectValidationVersion` is the
current projection version for a live subject or exactly zero for a deletion subject. Unknown subject/operation
combinations fail before any metadata mutation.
`GENERATION_PUBLISH`、`RECOVERY_CHECKPOINT`、`TOPIC_COMPACTED_PUBLISH` and `LOGICAL_TRIM` consume ACTIVE publication
authority and a live projection subject；`LOGICAL_TRIM` deliberately does not require either physical-deletion bit.
Only `PHYSICAL_DELETE` consumes both deletion bits、their same-epoch backfill/capability proof and a
`DomainValidatedDeletionSubject`.

The implemented V1 proof-value digests are domain-tagged SHA-256 over big-endian、length-delimited UTF-8 fields；
optional fields carry an explicit presence byte and checksums include both algorithm name and canonical value.
`LiveProjectionSubject` hashes stream id、projection-ref type/value and projection identity；the deletion subject
hashes the complete query identity and projection-domain snapshot identity. These encodings are input to ephemeral
proof comparison only and do not replace the authoritative metadata versions reloaded by `revalidate`.

### 3.2 Strong identities

```java
public record GenerationId(long value) {
    public GenerationId {
        if (value < 0) throw new IllegalArgumentException("generation must be non-negative");
    }
}

public record PublicationId(String value) {
    // 128-bit-or-stronger lowercase base32; stable for one logical task publication
}

public record ObjectKeyHash(String value) {
    // base32lower-no-pad(SHA-256(UTF-8 ObjectKey.value))
}
```

`ObjectKeyHash` is always recomputed and compared with the stored key on decode. A hash collision or mismatched key is
a non-retriable metadata invariant failure；the system never treats two keys as the same physical object.

## 4. Exact Reader Dispatch

The Phase 1.5 registry keyed only by `ReadTargetType` cannot distinguish Object WAL from compacted Parquet. F4 replaces
the reader half of `PrimaryWalRegistry` with：

```java
public record ReadTargetReaderKey(
        ReadTargetType targetType,
        int targetVersion,
        Optional<ObjectType> objectType,
        Optional<String> physicalFormat) {

    public static ReadTargetReaderKey from(ReadTarget target) { /* closed mapping */ }
}

public interface ReadTargetReader extends AutoCloseable {
    ReadTargetReaderKey key();

    long reservationBytes(ResolvedRange range);

    CompletableFuture<WalReadResult> readWithStats(
            StreamId streamId,
            long startOffset,
            List<ResolvedRange> ranges,
            ReadOptions options);
}

public final class ReadTargetReaderRegistry {
    public ReadTargetReader require(ReadTarget target);
}

public record GenerationReadCandidate(
        ReadView view,
        ResolvedRange resolvedRange,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexRecordSha256,
        boolean generationZero,
        Optional<PublicationId> publicationId) { }

public final class PinnedResolvedRange implements AutoCloseable {
    private final GenerationReadCandidate candidate;
    private final ObjectReadLease objectLease;

    public GenerationReadCandidate candidate();
    public ResolvedRange resolvedRange();
    @Override public void close();              // idempotent; releases exactly this lease
}
```

`PrimaryWalReader` becomes a compatibility subtype or adapter of `ReadTargetReader`; appenders remain keyed by primary
target type. Initial exact keys are：

| Target | Reader key |
| --- | --- |
| Object WAL v1 | `(OBJECT_SLICE, 1, MULTI_STREAM_WAL_OBJECT, WAL_OBJECT_V1)` |
| F4 committed Parquet | `(OBJECT_SLICE, 1, STREAM_COMPACTED_OBJECT, NEREUS_COMPACTED_PARQUET_V1)` |
| Future BookKeeper | `(BOOKKEEPER_ENTRY_RANGE, 1, empty, empty)` |

Duplicate exact keys fail runtime construction. An unknown physical format fails `UNSUPPORTED_READ_TARGET`; it is not
sent to the Object WAL reader. `ReadTargetDispatcher` groups only adjacent ranges with the same exact key.

`GenerationReadCandidate` is constructed only after the index decoder has proved its lifecycle/view. Its index key
is canonical、version is non-negative and `indexRecordSha256` is the SHA-256 of the exact durable value bytes.
Generation zero requires an empty publication id；higher generations require one matching the F4 index. A
`PinnedResolvedRange` is returned only after the root/lease/index/head revalidation in document 03. It owns the lease
until the read future's terminal cleanup and cannot be copied into an unpinned `ResolvedRange` for later IO.

## 5. Generation and Task Domain

### 5.1 Source identity

```java
public record SourceGeneration(
        ReadView view,
        OffsetRange range,
        long generation,
        long commitVersion,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexRecordSha256,
        ReadTarget readTarget,
        Checksum targetIdentitySha256,
        Optional<Checksum> materializationPolicySha256,
        PayloadFormat payloadFormat,
        Optional<ProjectionRef> projectionRef,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd) {
}
```

Canonical source order is `(range.startOffset, range.endOffset, generation, indexKey unsigned UTF-8)`. The task
`sourceSetSha256` is SHA-256 over the F4 canonical binary encoding of the ordered list；JSON or Java serialization is
never hashed.

The source wrapper records the exact durable index value digest in addition to its Oxia version. Its cumulative
interval is non-negative and `cumulativeSizeAtEnd - cumulativeSizeAtStart == logicalBytes`；schema refs use the
existing canonical ordering. The planner re-reads key/version/digest before task create, the worker repeats it before
IO and the committer repeats it before publication.
`range` is always the complete decoded index range；V1 tasks never clip or split one index while retaining its identity.
Generation zero has an empty materialization-policy digest；every F4 source requires the exact `policySha256` from its
index. This field is part of `sourceSetSha256` and is the fixed-point input used by the planner.

For a legacy/generic generation-zero index, `indexMetadataVersion` is the current Oxia version and
`targetIdentitySha256` is calculated from the canonical existing `ReadTargetCodec` bytes plus range/payload/projection
identity. F2 generation-zero records written before F4 legitimately have an empty `projectionRef`. For an activated
managed-ledger stream, the matching `MaterializationStreamRegistrationRecord` supplies the effective projection ref：
every source is either legacy-empty or exactly equal to that ref, and any different non-empty ref fails the task.
Every higher-generation record stores the effective ref, so compaction never fabricates or changes projection
identity. For an F4 record, the stored identity must match the recomputation.

### 5.2 Policy

```java
public record MaterializationPolicy(
        String policyId,
        long policyVersion,
        ReadView view,
        TaskKind taskKind,
        String targetPhysicalFormat,
        int minMergeSourceRanges,
        int maxSourceRanges,
        long maxRangeRecords,
        long targetObjectBytes,
        int targetRowGroupRecords,
        String compression,
        Optional<TopicCompactionSpec> topicCompaction) {
}

public record TopicCompactionSpec(
        String strategyId,
        long strategyVersion,
        String keyCodecId) { }

public enum TaskKind {
    LOSSLESS_REWRITE(1),
    TOPIC_KEY_COMPACTION(2);

    private final int wireId;
    TaskKind(int wireId) { this.wireId = wireId; }
    public int wireId() { return wireId; }
    public ReadView sourceView() { return ReadView.COMMITTED; }
    public static TaskKind fromWireId(int id) { /* strict closed mapping */ }
}
```

`LOSSLESS_REWRITE` requires `view=COMMITTED`, `targetPhysicalFormat=NEREUS_COMPACTED_PARQUET_V1` and empty
`topicCompaction`. `TOPIC_KEY_COMPACTION` requires `view=TOPIC_COMPACTED`,
`targetPhysicalFormat=NEREUS_TOPIC_COMPACTED_PARQUET_V1` and a registered non-empty strategy id/version/key codec.
Compression is exactly `ZSTD` or `UNCOMPRESSED` in V1. Any other combination fails policy construction before a task
key is calculated.

`MaterializationTask.view` and `MaterializationPolicy.view` are target views. `TaskKind.sourceView()` is the durable
V1 source-view rule and returns `COMMITTED` for both task kinds. Consequently a topic task freezes only exact
lossless COMMITTED indexes in `SourceGeneration`; an NTC1 target can prove the current-policy fixed point but is
never fed back into the dense exact-source reader. The planner scans source and target namespaces separately when
the views differ. Generation counters remain view-scoped, so equal numeric generations in COMMITTED and
TOPIC_COMPACTED identify unrelated indexes.

`minMergeSourceRanges` is at least 2 and at most `maxSourceRanges`. It applies only when every selected source is
already a healthy higher generation written with the exact current policy/format. A run containing generation zero
or a mismatched older policy remains eligible with one source；a single healthy current-policy higher generation is
already satisfied and must never be rewritten solely to obtain a larger generation number.

F4 hard limits, checked before task creation：

```text
maxSourceRanges          <= 128
minMergeSourceRanges     in [2, maxSourceRanges]
maxRangeRecords          <= 1_048_576
targetObjectBytes        <= 1 GiB
targetRowGroupRecords    in [1, 65_536]
encoded task root        <= 64 KiB
```

Configuration may choose smaller values but cannot raise a durable hard limit without a format/capability review.

The admitted corner is executable, not only a configuration inequality. A single planned task may contain exactly
128 contiguous source ranges and exactly 1,048,576 coverage records (8,192 records per source in the scale fixture).
Before the Oxia create, `MaterializationTaskStore` encodes the complete envelope and rejects values larger than
64 KiB；it then reloads every exact source candidate by `(stream, view, endOffset, generation)` and verifies the full
durable source identity before creating the task. `MaterializationTaskRecord` schema V2 dictionaries repeated source
facts and target descriptors and stores SHA-256 fields as exact 32-byte values, so this maximum admitted task fits the
same limit. The durable round-trip must reproduce all 128 source ranges and the one-task coverage exactly；splitting,
truncating or silently dropping a source is not an accepted fallback. V1 task roots remain readable, but new task
roots use schema/min-reader `2 / 2` and require broker generation lookup capability version `2`.

`policyDigestSha256` is computed from every field above. A policy version change creates a new task identity；it does
not mutate an existing task's meaning.

The implemented lossless factory uses the stable id `nereus-committed-default`. It derives a positive low-63-bit
`policyVersion` from SHA-256 over the canonical operator-controlled limits、row-group size and compression；the full
policy digest still covers every policy field. A durable task embeds the complete immutable policy snapshot in
addition to `(policyId, policyVersion, policyDigestSha256)`. Recovery reconstructs the task from that snapshot and
never consults current process configuration, so changing operator defaults cannot reinterpret or strand old
`CLAIMED`/`OUTPUT_READY`/`PUBLISHING` tasks.

### 5.3 Task and output

```java
public record MaterializationTask(
        String taskId,
        StreamId streamId,
        ReadView view,
        TaskKind taskKind,
        OffsetRange coverage,
        List<SourceGeneration> sources,
        Checksum sourceSetSha256,
        MaterializationPolicy policy,
        Checksum policyDigestSha256) {
}

public record MaterializationOutput(
        String taskId,
        StreamId streamId,
        ReadView view,
        OffsetRange coverage,
        String outputAttemptId,
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectKeyHash objectKeyHash,
        long objectLength,
        Checksum storageCrc32c,
        Checksum contentSha256,
        String etag,
        String physicalFormat,
        String logicalFormat,
        ReadTarget readTarget,
        Checksum targetIdentitySha256,
        EntryIndexRef entryIndexRef,
        int sourceRecordCount,
        int outputRecordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        Checksum sourceSetSha256,
        Optional<ProjectionRef> projectionRef) {
}

public interface MaterializationOutputVerifier {
    CompletableFuture<Void> verify(
            MaterializationTask task,
            MaterializationOutput output,
            Duration timeout);
}

public interface MaterializationFormatVerifier {
    CompletableFuture<Void> verify(
            MaterializationTask task,
            MaterializationOutput output,
            Duration timeout);
}
```

`taskId` is：

```text
"mat1-" + base32lower-no-pad(
  SHA-256(streamId || viewId || taskKindId || coverage ||
          sourceSetSha256 || policyDigestSha256))
```

`outputAttemptId` is the successful worker claim's random 128-bit `claimId` and is frozen by the `OUTPUT_READY` CAS.
The output key contains both this id and exact content SHA-256；generation is deliberately absent from
`MaterializationOutput` and file identity. One surviving verified output may be published under a newly allocated
generation after a lost/aborted publish attempt without rewriting bytes. If that physical root reaches `DELETED`, a
later worker uses a new claim/output-attempt id and therefore a new key；it never resurrects the deleted identity.
The verifier signatures deliberately include the exact task：`policyDigestSha256` is not duplicated into
`MaterializationOutput`, so an output-only verifier could prove byte identity but could not prove that the file's
policy metadata belongs to the publishing task. HEAD and full-format verification consume one shared monotonic
deadline.

### 5.4 Service interfaces

```java
public interface MaterializationPlanner {
    CompletableFuture<List<MaterializationTask>> plan(
            StreamId streamId,
            OffsetRange requestedRange,
            MaterializationPolicy policy,
            int maxTasks);
}

public interface MaterializationWorker {
    CompletableFuture<MaterializationOutput> execute(MaterializationTask task);
}

public interface GenerationCommitter {
    CompletableFuture<GenerationCommitResult> publish(
            MaterializationTask task,
            MaterializationOutput output);
}

public record GenerationCommitResult(
        StreamId streamId,
        ReadView view,
        OffsetRange coverage,
        GenerationId generation,
        PublicationId publicationId,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexRecordSha256,
        boolean committedByThisCall) { }
```

Planner output is advisory until `createTask` persists it. Worker success means immutable output was uploaded and
strictly verified. Only `GenerationCommitter` can transition a generation index to `COMMITTED`.
`GenerationCommitResult` is returned only after re-reading the exact `COMMITTED` index and index-owned physical
protection. `committedByThisCall` is diagnostic；both values of that flag have identical success semantics.

## 6. Object Keys and Identities

All path components use the existing canonical `KeyComponentCodec`/base32 helpers；examples are readable forms.

### 6.1 Lossless compacted object

```text
  {clusterComponent}/compacted/v1/committed/
  {streamIdComponent}/{offsetStart019}-{offsetEnd019}/
  {contentSha256}-{outputAttemptId}.parquet
```

### 6.2 Topic-compacted object

```text
  {clusterComponent}/compacted/v1/topic-compacted/
  {streamIdComponent}/{offsetStart019}-{offsetEnd019}/
  {contentSha256}-{outputAttemptId}.parquet
```

### 6.3 Recovery checkpoint

```text
{clusterComponent}/recovery-checkpoints/v1/
  {streamIdComponent}/{checkpointSequence019}-{contentSha256}-{checkpointAttemptId}.nrc
```

`ObjectId` is `co1-` or `rc1-` plus base32 SHA-256 of the canonical object key. Object ID and key hash are checked on
every metadata decode. F4 writers always use guarded `putObject(... ifAbsent=true)` and compare HEAD length/checksum/metadata on
an already-existing result. A deleted key is never reused. Publication/task retry chooses the identical key only
while its physical root is not `DELETING/DELETED`；a new worker output after deletion uses a fresh output-attempt id
even if content bytes hash the same.

### 6.4 Streaming compacted-writer surface

The object-store module accepts protocol-neutral rows and immutable format facts；it never imports materialization or
Pulsar classes：

```java
public record CompactedObjectRow(
        long streamOffset,
        ByteBuffer exactPayload,
        int payloadCrc32c,
        OptionalLong publishTimeMillis,
        OptionalLong eventTimeMillis,
        Optional<ByteBuffer> messageKey,
        Optional<ByteBuffer> orderingKey,
        Optional<String> schemaIdentity,
        OptionalLong producerId,
        OptionalLong producerSequenceId,
        OptionalInt batchMessageCount,
        OptionalInt sparseDisposition,
        Optional<ByteBuffer> compactionKey) { }

public record CompactedObjectWriteRequest(
        String cluster,
        ReadView view,
        StreamId streamId,
        OffsetRange sourceCoverage,
        String outputAttemptId,
        Checksum sourceSetSha256,
        Checksum policySha256,
        PayloadFormat payloadFormat,
        String logicalFormat,
        Optional<Checksum> projectionIdentitySha256,
        int sourceRecordCount,
        int expectedOutputRecordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        int targetRowGroupRecords,
        String compression,
        String writerBuild,
        Optional<TopicCompactionFormatSpec> topicCompaction) { }

public record TopicCompactionFormatSpec(
        String strategyId,
        long strategyVersion,
        String keyCodecId) { }

public record CompactedObjectWriteResult(
        StagedObjectFile stagingFile,
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectKeyHash objectKeyHash,
        long objectLength,
        Checksum storageCrc32c,
        Checksum contentSha256,
        String physicalFormat,
        EntryIndexRef entryIndexRef,
        int outputRecordCount) implements AutoCloseable {
    @Override public void close();
}

public interface CompactedObjectWriter {
    CompletableFuture<CompactedObjectWriteResult> write(
            CompactedObjectWriteRequest request,
            Flow.Publisher<CompactedObjectRow> rows);
}

public record CompactedObjectVerificationRequest(
        StreamId streamId,
        ReadView view,
        OffsetRange sourceCoverage,
        ObjectSliceReadTarget target,
        PayloadFormat payloadFormat,
        Checksum storageCrc32c,
        Checksum contentSha256,
        Duration timeout) { }

public final class CompactedObjectVerifier {
    CompletableFuture<CompactedObjectMetadata> verify(
            CompactedObjectVerificationRequest request);

    CompletableFuture<Void> verifyExact(
            CompactedObjectVerificationRequest request,
            CompactedObjectWriteRequest expected);
}

public interface StagedObjectFile extends ReplayableObjectUpload {
    long sealedLength();
    Checksum storageCrc32c();
    Checksum contentSha256();
    @Override void close();
}
```

The row publisher is single-subscription、ordered and backpressure-aware. Each emitted buffer is read-only and remains
valid only until the writer requests the next row；the writer copies it into the staging stream before advancing
demand. Dense committed rows require every offset exactly once and empty sparse-only fields. Topic-compacted rows
require increasing offsets inside dense source coverage and the disposition/key rules in section 8. Every optional
buffer is capped before copying, and total counts/bytes are checked with exact arithmetic.

The writer owns no upload. It seals a private staging file, calculates the complete-file SHA/key/id and returns a
close-owned result exposing that file as `ReplayableObjectUpload`. The worker performs put-if-absent、HEAD and format
verification, builds `MaterializationOutput`, then closes the result on every path. `write` failure closes/deletes its
partial file itself. The result's `outputRecordCount` must equal the request expectation；its key components and
footer reference are recomputed on construction.

`TopicCompactionFormatSpec` is the protocol-neutral object-format copy of the materialization-layer
`TopicCompactionSpec`. This explicit field is required because an NTC1 file must persist strategy id/version and key
codec in its immutable footer metadata, while `nereus-object-store` must not import `nereus-materialization`.
The worker must copy all three values exactly；neither layer may infer defaults during write or recovery.

The verifier streams the complete immutable object in exact ranges no larger than 8 MiB, recomputes whole-file
CRC32C and SHA-256, compares the SHA with the canonical key filename, then scans the strict reader in bounded batches
under the same monotonic deadline. It never aggregates the complete object or all rows in memory. `verifyExact`
additionally compares every persisted request fact, including policy/source digests、attempt、counts、codec profile
and NTC1 strategy. The materialization bridge receives both `MaterializationTask` and `MaterializationOutput` so the
file policy digest cannot be validated against output identity alone.

## 7. `NEREUS_COMPACTED_PARQUET_V1`

### 7.1 Required Parquet profile

The first implementation uses Apache Parquet with these fixed choices：

| Property | V1 value |
| --- | --- |
| Parquet format | standard Parquet file, `PAR1` header/footer |
| Nereus physical format | `NEREUS_COMPACTED_PARQUET_V1` |
| row order | strictly increasing unsigned/non-negative stream offset |
| one committed row | exactly one source offset and exact source payload |
| compression | `ZSTD` by default；`UNCOMPRESSED` allowed；codec stored in policy/file metadata |
| ZSTD level | `3` when ZSTD is selected |
| dictionary | disabled for every V1 column |
| Parquet writer version | `PARQUET_2_0` |
| data page size | `1 MiB` |
| bloom filters | disabled for every V1 column |
| page checksum | enabled；payload CRC remains mandatory |
| encryption | none in V1；encrypted object support requires a format minor/reader gate |
| writer time zone | UTC；timestamps are epoch millis, no local-time logical type |

Writer-library build/version and every option that can alter bytes are stored in file key-value metadata. Different
writer bytes are allowed because the exact content hash is part of the object key；semantic validation, not deterministic byte output,
is the publish requirement.

### 7.2 Dense committed schema

```text
message nereus_committed_generation_v1 {
  required int64  stream_offset;
  required binary payload;
  required int32  payload_crc32c;

  optional int64  publish_time_millis;
  optional int64  event_time_millis;
  optional binary message_key;
  optional binary ordering_key;
  optional binary schema_identity;
  optional int64  producer_id;
  optional int64  producer_sequence_id;
  optional int32  batch_message_count;
}
```

Required file key-value metadata：

```text
nereus.format                         = NCP1
nereus.format.version                 = 1
nereus.read.view                      = COMMITTED
nereus.stream.id                      = exact StreamId.value
nereus.offset.start                   = decimal long
nereus.offset.end                     = decimal long
nereus.source.set.sha256              = 64 lowercase hex
nereus.policy.sha256                  = 64 lowercase hex
nereus.output.attempt.id              = exact worker claim id
nereus.payload.format                 = enum name
nereus.logical.format                 = source logical mapping
nereus.projection.identity.sha256     = 64 hex or empty
nereus.source.record.count            = decimal int
nereus.output.record.count            = same decimal int
nereus.entry.count                    = decimal int
nereus.logical.bytes                  = decimal long
nereus.cumulative.size.at.end         = decimal long
nereus.writer                         = product build id
nereus.parquet.library.version         = pinned dependency version
nereus.parquet.writer.version          = PARQUET_2_0
nereus.parquet.compression              = ZSTD or UNCOMPRESSED
nereus.parquet.zstd.level               = 3 for ZSTD；required empty string for UNCOMPRESSED
nereus.parquet.data.page.bytes          = 1048576
nereus.parquet.dictionary.enabled       = false
nereus.parquet.bloom.filter.enabled     = false
nereus.parquet.page.checksum.enabled    = true
nereus.parquet.row.group.records        = policy targetRowGroupRecords
```

The reader recomputes range/stream components from the key, compares `nereus.output.attempt.id` with the key suffix
and output/index identity, and compares the complete-file SHA-256 with the key hash before publication admission.

For `COMMITTED`：

```text
outputRecordCount == sourceRecordCount == offsetEnd - offsetStart
first stream_offset == offsetStart
last stream_offset  == offsetEnd - 1
every adjacent stream_offset differs by exactly 1
```

`payload` is the observable authority. Auxiliary columns may accelerate future filtering/table work but are never
used to reconstruct bytes. If present, a mapping-specific verifier must prove they match `payload`; otherwise publish
fails. Missing auxiliary fields do not lose metadata because exact opaque payload preserves it.

### 7.3 Pulsar exact-byte rule

For source projection mapping `PULSAR_ENTRY_V1`：

- `payload` is one complete ManagedLedger Entry byte sequence, starting at the same metadata framing bytes returned
  by generation 0；
- `stream_offset` is the Entry offset and later projects to the same virtual ledger `Position.entryId`；
- a batched Entry remains one row；`batch_message_count` is auxiliary and batch indexes remain encoded in payload；
- worker and reader compare length + CRC32C for every Entry；the integration gate additionally compares all bytes；
- compression is container-level only；decrypt/re-encrypt、metadata parse/re-serialize and checksum regeneration are
  forbidden；
- entries with a mapping the verifier cannot prove are not publishable to `COMMITTED`。

The selected generation/index keeps the effective projection ref as an admission and compatibility identity, but
`ParquetCompactedTargetReader` does not copy that generation-level value into each returned `ReadBatch`.
`PULSAR_ENTRY_V1` therefore preserves the locked empty per-entry schema/projection-ref surface while the resolver and
file metadata still prove the exact virtual-ledger projection. `PulsarEntryOpaqueRoundTripTest` covers unbatched and
ZSTD-compressed batched entries with properties、ordering keys and a serialized middle-batch MessageId.

Read accounting must not compare decompressed logical bytes with compressed object length as if they shared one byte
domain. `WalSliceReadStats` therefore carries resolved target payload/index lengths for identity validation and
separate measured payload/index downloads for metrics. `ParquetCompactedTargetReader` copies
`CompactedObjectReadResult.physicalBytesRead/footerBytesRead` into those measured fields；returned logical bytes may
legitimately exceed physical IO. `ioDeltaBytes` is signed, while `amplificationBytes` and
`compressionSavingsBytes` expose its two non-negative parts. `readsCompressibleLogicalPayloadLargerThanPhysicalParquetIo`
locks the case where two returned 256 KiB rows are larger than the ZSTD Parquet IO and still decode byte-for-byte。

### 7.4 Footer/index reference

Every row group must cover one dense subrange and expose Parquet min/max statistics for `stream_offset`. The writer
records the exact footer byte range in：

```java
new EntryIndexRef(
    EntryIndexLocation.OBJECT_FOOTER,
    Optional.of(objectId),
    Optional.of(objectKey),
    Optional.empty(),
    footerOffset,
    footerLength,
    footerCrc32c);
```

The compacted reader：

1. range-reads and CRC-checks the footer；
2. validates all required file metadata；
3. selects only row groups whose declared range intersects the requested offset；
4. range-reads pages under the read budget；
5. verifies strictly increasing offsets and per-payload CRC；
6. returns the exact `ReadBatch` mapping expected by the existing projection。

Footer length is capped at 16 MiB and row-group count at 65,536. Exceeding either limit fails the worker before
upload/publication.

## 8. `NEREUS_TOPIC_COMPACTED_PARQUET_V1`

This is a storage primitive, not Pulsar behavior. Schema：

```text
message nereus_topic_compacted_v1 {
  required int64  stream_offset;
  required int32  disposition;        // 1=VALUE, 2=TOMBSTONE
  required binary compaction_key;
  optional binary payload;            // exact source Entry/record for VALUE
  optional int32  payload_crc32c;
  optional int64  publish_time_millis;
  optional int64  event_time_millis;
}
```

Required metadata uses the 7.2 key set, replacing format/view/output-count with the sparse values below, plus the
strategy fields：

```text
nereus.format                         = NTC1
nereus.read.view                      = TOPIC_COMPACTED
nereus.source.coverage.start/end      = dense source range
nereus.output.record.count            = sparse emitted row count
nereus.compaction.strategy            = registered strategy id
nereus.compaction.strategy.version    = decimal long
nereus.compaction.key.codec           = registered decoder id
nereus.compaction.key.encoding        = TAGGED_V1
```

Rows are strictly increasing but need not be adjacent. Missing offsets mean superseded records, not unavailable
storage. A tombstone row has no payload；its retention/removal policy is part of the strategy version. The generation
record stores dense source coverage separately from sparse output count, so this format cannot hydrate the ordinary
`OffsetIndexEntry/ResolvedRange` type.

`compaction_key` is a collision-free storage namespace, not the raw decoder result：

```text
keyed record     = 0x00 || non-empty decoder key
unkeyed record   = 0x01 || stream_offset as signed int64 big-endian
```

`TopicCompactionKeyEncodingV1` is the sole V1 encoder/decoder. The writer and strict reader reject unknown tags、an
empty keyed body、a malformed/negative unkeyed offset or an unkeyed value whose embedded offset differs from the row
offset. This prevents arbitrary decoder keys from colliding with the retain-exact representation.

Protocol payload parsing is supplied through a neutral SPI：

```java
public interface TopicCompactionDecoder {
    String id();
    Optional<CompactionRecord> decode(long offset, ByteBuffer exactPayload);
}

public enum CompactionDisposition {
    VALUE(1), TOMBSTONE(2)
}

public record CompactionRecord(
        long streamOffset,
        ByteBuffer compactionKey,
        CompactionDisposition disposition,
        OptionalLong publishTimeMillis,
        OptionalLong eventTimeMillis) { }

public interface TopicCompactionStrategy {
    String id();
    long version();
    boolean retainTombstone(CompactionRecord tombstone, long planningTimeMillis);
}

public final class TopicCompactionRegistry {
    public TopicCompactionRegistry(
            List<? extends TopicCompactionDecoder> decoders,
            List<? extends TopicCompactionStrategy> strategies);
    public Binding resolve(TopicCompactionSpec spec);
}
```

F4 core has no dependency on Pulsar classes. An encrypted/unsupported payload is either retained according to the
registered strategy or makes the task terminally unsupported；it is never silently dropped.
Decoder output must repeat the supplied offset, return a non-empty read-only key within the configured key-byte cap
and never return payload bytes；pass two re-reads the exact source for surviving VALUE rows. Empty decoder output means
“unkeyed and retain the exact record”, not silent deletion. Strategy id/version are
the exact task-policy values, and `planningTimeMillis` is captured once so tombstone decisions are deterministic.
`TopicCompactionRegistry` rejects duplicate decoder ids and duplicate `(strategyId, strategyVersion)` pairs, resolves
only the exact identities frozen in `TopicCompactionSpec`, and re-reads implementation identities at resolution so a
mutable implementation cannot silently change durable task semantics. `CompactionRecord` defensively copies a
non-empty key into a read-only buffer, while `CompactionDisposition` uses explicit durable wire ids rather than enum
ordinals. The implemented engine owns the configured key-byte cap, supplied-offset equality check, unkeyed
retain-exact representation and two-pass payload reread；those checks are not delegated to the registry.

The execution surface is：

```java
public interface TopicCompactionEngine {
    CompletableFuture<TopicCompactionPlan> prepare(
            MaterializationTask task,
            ExactSourceRangeReader sourceReader,
            ReadOptions readOptions,
            TopicCompactionRegistry.Binding binding,
            long planningTimeMillis);
}

public interface TopicCompactionPlan
        extends Flow.Publisher<CompactedObjectRow>, AutoCloseable {
    int outputRecordCount();
    @Override void close();
}
```

`DefaultTopicCompactionEngine` requires a TOPIC task whose sources use `COMMITTED`, re-resolves the exact frozen
decoder/strategy identities, and captures the durable task `createdAtMillis` as the one tombstone planning time. Pass
one validates dense one-record-per-offset source batches, updates latest-offset-by-key state, marks unkeyed records
directly in a bounded survivor `BitSet`, and spills immutable key-sorted runs when its key-memory budget is reached.
The budget is `[64 KiB, 256 MiB]`, decoded keys are `[1, 1 MiB - 1]` bytes, merge fan-in is `[2, 64]`, and task
coverage remains capped at 1,048,576 records. Spill runs share `StagingFileManager`'s global byte budget, are sealed
with file identity plus SHA-256, and are checksum-verified during bounded k-way merge.

Pass one freezes the survivor bitmap、sparse count and a canonical decoder-fact digest, then closes every spill file.
Pass two reopens the same exact COMMITTED sources, repeats offset/decoder/strategy facts, compares the digest and
count, and emits only survivors in source-offset order. VALUE rows carry the original exact payload；TOMBSTONE rows
carry no payload. Any decoder mutation、source substitution、count/digest drift、spill corruption or cancellation
fails the writer before upload/publication.

## 9. Recovery Checkpoint Object V1

The recovery checkpoint replaces a prefix of live Oxia commit nodes as append-replay/index-repair evidence. It is
not a data generation and never serves `StreamStorage.read` directly.

> Implementation status (2026-07-15)：the object protocol and bounded publication-table scan in this section are
> implemented and the guarded coordinator/root-CAS path consumes them. Checkpoint E additionally implements bounded
> offset-to-entry lookup for append replay；checkpoint F consumes the same entry/publication facts for exact committed
> index restoration. This is not an M4 completion claim；runtime composition and retirement/GC paths in documents
> 03–05 are still required.

### 9.1 Streaming codec surface

```java
public record RecoveryCheckpointWriteRequest(
        String cluster,
        StreamId streamId,
        long checkpointSequence,
        String checkpointAttemptId,
        OffsetRange coverage,
        long firstCommitVersion,
        long lastCommitVersion,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        String firstCommitId,
        String lastCommitId,
        String sourceHeadCommitId,
        long sourceHeadCommitVersion,
        Checksum projectionIdentitySha256,
        int expectedEntryCount,
        int expectedPublicationCount) { }

public record RecoveryCheckpointPublication(
        long generation,
        PublicationId publicationId,
        OffsetRange coverage,
        ByteBuffer canonicalGenerationIndexRecord,
        Checksum generationIndexRecordSha256) { }

public record RecoveryCheckpointEntry(
        long commitVersion,
        OffsetRange range,
        long cumulativeSizeAtEnd,
        String commitId,
        String previousCommitId,
        ByteBuffer canonicalCommitRecord,
        Checksum canonicalCommitRecordSha256,
        List<Integer> coveringPublicationIndexes) { }

public record RecoveryCheckpointDirectory(
        long publicationDirectoryOffset,
        long publicationDirectoryLength,
        long commitDirectoryOffset,
        long commitDirectoryLength,
        int commitDirectoryStride) { }

public record RecoveryCheckpointWriteResult(
        StagedObjectFile stagingFile,
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectKeyHash objectKeyHash,
        long objectLength,
        Checksum storageCrc32c,
        Checksum bodySha256,
        Checksum contentSha256,
        RecoveryCheckpointDirectory directory) implements AutoCloseable {
    @Override public void close();
}

public record RecoveryCheckpointObject(
        RecoveryCheckpointWriteRequest header,
        ObjectId objectId,
        ObjectKey objectKey,
        long objectLength,
        Checksum bodySha256,
        Checksum contentSha256,
        RecoveryCheckpointDirectory directory) { }

public record RecoveryCheckpointPublicationPage(
        List<RecoveryCheckpointPublication> values,
        OptionalInt continuation) { }

public interface RecoveryCheckpointCodecV1 {
    CompletableFuture<RecoveryCheckpointWriteResult> write(
            RecoveryCheckpointWriteRequest request,
            Flow.Publisher<RecoveryCheckpointPublication> publications,
            Flow.Publisher<RecoveryCheckpointEntry> entries);

    CompletableFuture<RecoveryCheckpointObject> openAndVerify(
            ObjectKey key, long expectedLength, Checksum expectedContentSha256,
            Duration timeout);

    CompletableFuture<Optional<RecoveryCheckpointPublication>> findPublication(
            RecoveryCheckpointObject object, long generation, PublicationId publicationId,
            Duration timeout);

    CompletableFuture<RecoveryCheckpointPublicationPage> scanPublications(
            RecoveryCheckpointObject object, OptionalInt continuation, int limit,
            Duration timeout);

    CompletableFuture<Optional<RecoveryCheckpointEntry>> findCommitCoveringOffset(
            RecoveryCheckpointObject object, long offset, Duration timeout);

    CompletableFuture<Optional<RecoveryCheckpointEntry>> findCommit(
            RecoveryCheckpointObject object, long commitVersion, String commitId,
            Duration timeout);
}
```

Checkpoint F adds the following exact metadata/consumer surface：

```java
public final class GenerationIndexDigests {
    public static Checksum canonicalRecordSha256(GenerationIndexRecord record);
    public static Checksum durableValueSha256(GenerationIndexRecord record);
}

public interface GenerationMetadataStore {
    CompletableFuture<VersionedGenerationIndex> restoreCommittedFromCheckpoint(
            String cluster,
            GenerationIndexRecord record,
            Checksum canonicalRecordSha256);
}

public enum GenerationIndexRepairSource {
    TRIMMED,
    LIVE_COMMIT,
    RECOVERY_CHECKPOINT
}

public record GenerationIndexRepairResult(
        StreamId streamId,
        long targetOffset,
        GenerationIndexRepairSource source,
        int scannedRecords,
        Optional<VersionedGenerationIndex> restoredIndex) { }

public interface GenerationIndexRepairer {
    CompletableFuture<GenerationIndexRepairResult> repair(
            StreamId streamId, long targetOffset, Duration timeout);
}
```

`canonicalRecordSha256` is over `GenerationIndexRecordCodecV1` bytes embedded in NRC1；`durableValueSha256` is over
the metadata envelope written to Oxia with encoded `metadataVersion == 0`. They are distinct protocol facts.
`restoreCommittedFromCheckpoint` accepts only a COMMITTED-view unhydrated record whose raw digest matches. It performs
one final-key put-if-absent and, on conflict/uncertain response, reloads the exact key and accepts only the identical
hydrated record plus durable-envelope digest. It never creates `PREPARED` and never advances a lifecycle.

`CheckpointDerivedIndexRepairer` first proves trim/head and a root-stable bounded live tail. For checkpoint coverage it
pins the selected NRC1 object, reads exactly the publication indexes named by the covering commit entry, chooses the
highest candidate with an exact ACTIVE physical root, establishes the current-root-owned target protection, then
revalidates activation/root/trim/physical identity/protection around the metadata restore. Root change restarts the
whole proof；trim returns `TRIMMED` without a write. `GenerationReadResolver` consumes only the terminal result and
performs a fresh authority scan and ordinary read pin before returning a range.

The production implementation is `DefaultRecoveryCheckpointCodecV1(ObjectStore, StagingFileManager, Executor,
RecoveryCheckpointVerifier)`. `RecoveryCheckpointVerifier` keeps `nereus-object-store` protocol-neutral while the
materialization implementation decodes embedded records through `GenerationIndexRecordCodecV1` and
`MetadataRecordCodecFactory`/`StreamCommitTargetRecord`. It rejects non-canonical bytes、non-zero embedded
`metadataVersion`、non-COMMITTED generation evidence and any duplicated identity/range/version drift.

Both publishers are cold、single-subscription、strictly ordered and bounded to one outstanding item. The codec writes
publications first, then commits, while incrementally building spillable directory runs in the same owner-only
staging area；it never retains all million entries or publications in heap. Input buffers are read-only and capped at
64 KiB. Counts, range/version/cumulative continuity and every digest are verified while streaming. Write failure owns
partial-file cleanup；success transfers one close obligation with the result. `openAndVerify` range-reads footer and
directories, validates body/complete-object digests and returns only bounded header/directory state. Lookup reads and
strictly decodes one bounded record; absence means exact directory miss, never a truncated scan.

### 9.2 Binary layout

All integers are big-endian. Strings are strict UTF-8 prefixed by unsigned 32-bit length. Counts/lengths are checked
before allocation.

The request contains `cluster`, and V1 therefore encodes `cluster` in the header. Omitting it would make
`openAndVerify` unable to reconstruct and validate the canonical object key from decoded bytes；the code-level layout
below is the authoritative correction to the earlier pre-implementation sketch.

```text
Header
  4 bytes  magic "NRC1"
  u16      majorVersion = 1
  u16      flags = 0
  string   cluster
  string   streamId
  i64      checkpointSequence >= 1
  string   checkpointAttemptId             // random 128-bit lowercase base32
  i64      coveredStartOffset
  i64      coveredEndOffset
  i64      firstCommitVersion
  i64      lastCommitVersion
  i64      cumulativeSizeAtStart
  i64      cumulativeSizeAtEnd
  string   firstCommitId
  string   lastCommitId
  string   sourceHeadCommitId
  i64      sourceHeadCommitVersion
  32 bytes projectionIdentitySha256
  i32      entryCount
  i32      publicationCount
  i32      headerCrc32c

Publication[publicationCount]
  i64      generation
  string   publicationId
  i64      coverageStart
  i64      coverageEnd
  i32      generationIndexRecordLength
  bytes    canonical GenerationIndexRecordCodecV1 bytes
           (COMMITTED lifecycle, encoded metadataVersion=0)
  32 bytes generationIndexRecordSha256
  i32      publicationCrc32c

CommitEntry[entryCount]
  i64      commitVersion
  i64      offsetStart
  i64      offsetEnd
  i64      cumulativeSizeAtEnd
  string   commitId
  string   previousCommitId
  i32      canonicalCommitRecordLength
  bytes    canonical generic StreamCommitTargetRecord envelope
           (L0TargetMetadataCodecs, encoded metadataVersion=0)
  32 bytes canonicalCommitRecordSha256
  i32      coveringPublicationCount
  repeated i32 publicationTableIndex
  i32      entryCrc32c

PublicationDirectory
  i32      publicationDirectoryCount == publicationCount
  repeated { i32 publicationTableIndex; i64 fileOffset; }

CommitDirectory
  i32      stride = 256
  i32      directoryCount
  repeated { i64 commitVersion; i64 offsetStart; i64 fileOffset; }

Footer
  4 bytes  magic "NRF1"
  i64      publicationDirectoryOffset
  i64      publicationDirectoryLength
  i64      commitDirectoryOffset
  i64      commitDirectoryLength
  32 bytes bodySha256 of every preceding byte
  i32      footerCrc32c
```

`bodySha256` protects the checkpoint body/directories before the footer. The `contentSha256` used in the object key、
physical root and recovery-root reference is a second SHA-256 over the complete final object including `NRF1` and its
CRC. The two fields are never substituted for one another and both are verified after upload.

Publication rows are canonical-deduplicated by `(generation, publicationId, generationIndexRecordSha256)` and sorted
by generation. Embedded record bytes must decode through the F4 codec registry, belong to the same stream/
`COMMITTED` view, be lifecycle `COMMITTED`, cover their declared range and reproduce the table identity. This embeds
the exact target/key/checksum/count/projection/schema facts required to recreate a missing index；a hash alone is not
repair evidence.

Each commit envelope decodes to a generic generation-zero `StreamCommitTargetRecord` whose duplicated
commit/version/range/size/predecessor fields exactly equal the entry header. Existing legacy Object-WAL commits are
losslessly canonicalized through the Phase 1.5 target codec before writing NRC1；commit id、writer/fence identity、
target、payload、counts、schema refs、projection ref、event times and prepared time all remain available for exact
append replay validation. Unknown envelope/target versions fail closed.

Entries are contiguous in commit version、offset and cumulative size. The first entry bridges the previous active
checkpoint (or genesis)；the final entry exactly matches the root's covered end. The implemented builder contract
anchors checkpoint coverage at the authoritative logical-trim offset (genesis is trim offset zero), so every encoded
entry is still legal repair evidence and carries 1–8 valid publication-table references whose union covers its exact
range. V1 does not infer "already trimmed" from process-local state and does not encode an empty-reference shortcut.

### 9.3 Limits

```text
checkpoint object bytes       <= 1 GiB
commit entries                <= 1_000_000
publication refs per entry    <= 8
publication records           <= 1_000_000 and object-byte cap
embedded generation record    <= 64 KiB
embedded canonical commit     <= 64 KiB
combined directories          <= 32 MiB
string bytes per field        <= 16 KiB
directory stride              == 256 in V1
```

Readers range-read `NRF1` and both directories first. Commit lookup uses the sparse commit directory, then reads one
commit entry；its publication indexes are resolved through the exact publication directory without loading the full
table/object. Directory indexes/offsets must be sorted, unique, in bounds and point to records whose decoded identity
matches the directory entry.

The implemented writer requests exactly one publisher item at a time and retains only the current bounded record.
Publication and sparse-commit directory runs plus fixed-width publication coverage facts use owner-only spill files
under the shared staging budget；entry validation reuses one identity-checked random-read handle rather than opening a
file per reference. `openAndVerify` uses one monotonic deadline, hashes the object in at most 1 MiB ranges, and checks
HEAD length、header/footer CRC32C、directory bounds、body SHA-256、complete-object SHA-256 and reproduced key identity.
Exact lookup then validates the selected record CRC、embedded digest、metadata semantics and referenced coverage.
`scanPublications` uses an integer table-index continuation, caps one page at 1,000 rows, and strictly decodes each
row；it exists so post-root-CAS restart reconciliation can rebuild target protections without retaining the full
publication table in heap.

`findCommitCoveringOffset` binary-searches the sparse directory's canonical offset-start column, reads at most one
stride-256 commit block, strictly decodes entries in that block and verifies the selected entry's publication
coverage. It returns the unique half-open entry containing the offset；a covered checkpoint offset with no entry is a
corrupt/invariant condition at the consumer, not a negative append-replay proof.

Larger history is split across checkpoint objects referenced by a bounded root chain. The active Oxia root stores at
most 32 checkpoint references；before the 33rd, a merge checkpoint replaces older checkpoint objects. Root publication
and recovery are specified in documents 03 and 04.

`checkpointAttemptId` is generated once per build attempt and copied into bytes、key、root reference and bounded
pending-protection identity. It keeps retry/orphan keys distinct after an older physical root reaches `DELETED`；it
has no ordering or visibility meaning.

## 10. ObjectStore Surface Required by F4

```java
public interface ObjectStore extends AutoCloseable {
    // existing ByteBuffer put/read/head remains source-compatible

    CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ReplayableObjectUpload source,
            PutObjectOptions options);

    CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ReplayableObjectUpload source,
            PutObjectOptions options,
            PutObjectAttemptGuard attemptGuard);

    CompletableFuture<ListObjectsResult> listObjects(
            ObjectKeyPrefix prefix,
            Optional<String> continuationToken,
            ListObjectsOptions options);

    CompletableFuture<DeleteObjectResult> deleteObject(
            ObjectKey key,
            DeleteObjectOptions options);
}

@FunctionalInterface
public interface PutObjectAttemptGuard {
    CompletableFuture<Void> authorize(ObjectKey key, int providerAttemptNumber);
}

public record ObjectPutRetryPolicy(
        int maxAttempts,
        Duration minBackoff,
        Duration maxBackoff) { }

public interface ReplayableObjectUpload extends AutoCloseable {
    long contentLength();
    Flow.Publisher<ByteBuffer> openPublisher();
    @Override void close();
}

public interface StagedObjectFile extends ReplayableObjectUpload {
    long sealedLength();
    Checksum storageCrc32c();
    Checksum contentSha256();
}

public final class StagingFileManager implements AutoCloseable {
    public StagingFileManager(
            Path absoluteOwnerOnlyDirectory,
            long maxStagingBytes,
            int uploadChunkBytes,
            Duration orphanGrace,
            Executor objectIoExecutor);
    public PrivateStagedObjectFile create(String canonicalPurpose);
    public PrivateStagingSpillFile createSpill(String canonicalPurpose);
    public int cleanupOrphans();
    public long reservedBytes();
}

public final class PrivateStagedObjectFile implements StagedObjectFile {
    public OutputStream outputStream();       // claim once
    public PrivateStagedObjectFile seal();    // force + immutable identity/checksums
}

public final class PrivateStagingSpillFile implements AutoCloseable {
    public OutputStream outputStream();
    public PrivateStagingSpillFile seal();
    public long sealedLength();
    public Checksum contentSha256();
    public InputStream openVerifiedInputStream();
    @Override public void close();
}

public record ListObjectsOptions(int maxKeys, Duration timeout) { }

public record ListedObject(
        ObjectKey key,
        long objectLength,
        Optional<String> etag,
        Optional<Instant> lastModified) { }

public record DeleteObjectOptions(
        long expectedLength,
        Checksum expectedStorageChecksum,
        Optional<String> expectedEtag,
        Duration timeout) { }

public record DeleteObjectResult(ObjectKey key, Status status) {
    public enum Status { DELETED, ALREADY_ABSENT }
}
```

The existing `ByteBuffer` overload delegates to a buffer-backed upload source. F4 compacted/checkpoint writers use a
private, bounded staging file and expose a fresh backpressure-aware publisher for each provider upload attempt. Each
publisher emits read-only chunks no larger than the configured hard-capped upload chunk, respects demand/cancel,
uses the injected object-IO executor and produces exactly `contentLength` bytes. S3 maps it to an async request body；
the local fixture consumes the same publisher. No provider may aggregate the whole object with `toByteArray`.
`openPublisher` may be called again only for an explicit provider retry after the prior publisher terminates；the
source remains open until the returned `putObject` future completes, and the caller closes it exactly once afterward.

The production S3 future completes only after both the SDK response succeeds and exactly the declared
`contentLength` bytes have been delivered to the SDK subscriber. It does not require the upstream publisher's
`onComplete` after the last byte because an SDK may cancel a known-length publisher once it has consumed that byte.
The adapter then synchronously rechecks byte count and CRC32C before exposing success. A short/long/error body fails
closed；body failure cancels the SDK request, while caller cancellation or deadline cancellation cancels the active
SDK request. An early SDK response therefore cannot acknowledge an incompletely consumed body.

The guarded overload is mandatory for Object WAL, compacted output, recovery checkpoint and cursor-snapshot writes
once F4 publication hooks are enabled. `providerAttemptNumber` starts at one and increases exactly once per actual
provider transmission. Immediately before each transmission, including a retry, the adapter awaits `authorize` under
the one overall put deadline. The caller's closure reloads the exact durable session/task-claim/checkpoint/cursor
owner and physical root; only an unchanged owner plus absent root or the same `ACTIVE` root succeeds. Guard failure
sends no bytes and is not retryable under that key.

`S3CompatibleObjectStore` disables opaque SDK PUT retries for guarded calls and owns the bounded retry loop (or uses
an execution interceptor that invokes the guard before every wire transmission), so no hidden retry bypasses the
callback. The local fixture follows the identical attempt sequence. Existing unguarded overloads remain
source-compatible for pre-F4 callers, but a source audit rejects their use from F4 writers and from
`DefaultWalObjectWriter` after the F4 physical-reference hook is installed. Tombstone retirement cross-validation
requires its grace to exceed this complete put deadline plus clock skew.

Checkpoint BB makes the Object-WAL clause executable. `AppendCoordinator` converts the prepared `WalWriteResult`
into the whole-object `PhysicalObjectIdentity` and passes one guard through `ObjectWalAppenderAdapter` and
`DefaultWalObjectWriter` to the replayable object-store overload. The authorization sequence is exactly：

```text
revalidate captured append session S against the current ACTIVE stream head
get physical root by ObjectKeyHash
  allow absent, or require exact immutable identity and lifecycle ACTIVE
revalidate S again
return to the provider, which may now open/transmit this attempt's publisher
```

The read-only session proof requires a non-expired current lease and exact writer、epoch and fencing token；
`current.leaseVersion >= captured.leaseVersion` permits a same-owner renewal but never extends the captured request
or accepts a replaced owner. An expired session fails the first attempt before bytes；MARKED/DELETING/DELETED roots
fence an initial or retry transmission under that key. A later acquired epoch prepares a new attempt-addressed WAL
key. `ObjectWalGuardedUploadTest` verifies one successful first transmission followed by a DELETED-root retry fence,
and separately zero stale transmissions after root-tombstone retirement followed by one fresh-key success.

`ObjectStoreConfiguration` gains the non-null `ObjectPutRetryPolicy`. `maxAttempts` counts the initial provider
transmission and is in `[1, 10]`；backoffs are positive, millisecond-representable and
`minBackoff <= maxBackoff <= requestTimeout`. Attempt `n > 1` uses injected full jitter in
`[0, min(maxBackoff, minBackoff * 2^(n-2))]`, with overflow saturation. Only the existing typed retryable transport/
5xx/throttle classifications retry；precondition/conflict/checksum/guard/closed failures do not. Guard + backoff +
transmission all share one absolute deadline from `PutObjectOptions.timeout`. A sampled backoff starts only when its
deadline is strictly earlier than that absolute deadline；each following request receives
`min(ObjectStoreConfiguration.requestTimeout, remainingOverallTime)` and requires a positive millisecond duration.
Production jitter is random；tests inject a deterministic value source.

`PrivateStagedObjectFile` and `PrivateStagingSpillFile` are allocated by the process-shared `StagingFileManager`
under a configured owner-only
directory, opens a new regular file with
no-follow/exclusive-create semantics, streams Parquet/NRC1 bytes while calculating CRC32C/SHA-256 and is sealed before
`openPublisher`. Seal records file identity and exact length；publisher open/re-read mismatch fails before upload.
Closing deletes the staging file best-effort, and startup cleanup removes only valid product-prefixed files older than
the orphan staging grace. The manager's global byte semaphore bounds sealed + in-progress staging bytes across
compacted/checkpoint writers and topic-compaction spill runs. A sealed spill exposes one active verified input stream；
EOF validates its exact length and SHA-256, and close deletes it and releases exactly its reserved bytes. Staging files are never
durable recovery truth；a process crash simply causes task replay and object-store orphan reconciliation.

List is bounded to at most 1,000 logical keys per call and the continuation token is opaque. Because canonical S3
keys use base64url, an arbitrary UTF-8 logical prefix is represented by one exact physical prefix when its byte
length is divisible by three, 16 disjoint prefixes for remainder one, and four for remainder two. The adapter walks
those disjoint prefixes under one `nls1` Nereus continuation token；it never treats one broad base64 prefix plus
post-filtering as a complete page. A provider returning more keys、wrong prefix/order, a non-canonical key or a
repeated non-terminal token fails the scan. Objects are strictly ordered within each returned logical page；callers
must use the opaque token rather than infer cross-page order from base64 physical keys.

Checkpoint AW makes that last sentence executable in the production inventory scanner. A returned page must carry
the exact requested logical family prefix；when it is non-terminal, its opaque token must differ from the token
supplied for that call. The scanner does not compare the first logical key of the new page with the previous page's
last key. Focused tests feed pages in descending cross-page logical order and separately repeat an opaque token；the
former completes, while the latter fails closed before another continuation is followed.

Delete is admitted only while the physical root is `DELETING`. The implementation performs exact HEAD identity
validation before DELETE. S3 `If-Match` is used where supported；for compatible stores without conditional DELETE，
only a raw HTTP 405 or 501 response to that conditional DELETE permits one retry without `If-Match`. The exact HEAD、
conditional attempt and compatibility fallback share the original absolute deadline, and cancellation follows the
currently active request. Every other conditional failure remains closed. For that narrow fallback, immutability +
no-key-reuse + the durable root fence close the race. `ALREADY_ABSENT` is idempotent success only when the root already
records the exact expected immutable identity；absence during ordinary reads remains corruption.

### 10.1 Configured-scope destructive capability proof

Checkpoint AP implements this production API in `nereus-object-store`：

```java
public interface ObjectStoreDeleteCapabilityProbe {
    String expectedCapabilitySha256();

    CompletableFuture<ObjectStoreDeleteCapabilityProof> probe(
            ObjectStoreDeleteCapabilityRequest request);
}

public record ObjectStoreDeleteCapabilityRequest(
        String runId,
        Duration timeout) { }

public record ObjectStoreDeleteCapabilityProof(
        int protocolVersion,
        String capabilitySha256,
        String probeObjectKeySha256,
        long completedAtMillis) { }
```

`runId` is 26–128 lowercase unpadded base32 characters and therefore carries at least 128 bits of caller-generated
entropy. The one overall timeout covers every primary operation and response-loss recheck；the internal deadline uses
checked nanosecond addition, passes only positive millisecond durations to providers and also bounds exposed futures.
The probe key is exactly `__nereus_capability__/delete-v1/<runId>/probe`. Payload bytes bind protocol name、expected
capability digest and run id, and are uploaded with `ifAbsent=true` plus CRC32C metadata.

The V1 success sequence is ordered and indivisible：PUT (or exact HEAD convergence after a lost response)、exact HEAD、
complete single-page prefix LIST、exact identity DELETE with the observed ETag、HEAD absence convergence、a second
idempotent DELETE returning `ALREADY_ABSENT`, then complete LIST absence. Exact HEAD requires key、length、CRC32C and
non-empty ETag；present LIST additionally requires the same key/length, a last-modified value, no continuation token
and no conflicting ETag. A response-loss DELETE is success only after exact HEAD absence. Discovery/listing is being
tested here as a provider capability；it still never authorizes deletion of a Nereus data object.

The deterministic capability SHA-256 uses length-delimited UTF-8 fields：protocol、provider class、normalized endpoint、
region、bucket、logical prefix、path-style flag and six frozen semantic labels. Request timeout、pool size and
credential references are intentionally excluded：they are runtime tuning/secret inputs, not scope identity. On any
failure, cleanup first requires the same exact HEAD identity and deletes only that canary；a foreign/mismatching key is
retained and the probe fails closed. The returned key hash supports redacted audit correlation. Only
`capabilitySha256` may later be persisted in cluster activation metadata, and checkpoint AP itself performs no such
CAS and grants no physical-delete authority.

## 11. Object-format Test Contract

Required tests before any publish gate：

- golden Parquet file metadata/schema and strict reader rejection of missing/unknown required values；
- random dense range round trip for every supported payload format；
- byte-for-byte Pulsar Entry comparison including compression、batching、properties and middle-batch MessageId；
- corrupt footer、row-group stats、payload CRC、offset order、count and trailing/mismatched object metadata；
- sparse topic-compaction coverage/tombstone strategy tests with no ordinary-reader cross-over；
- tagged keyed/unkeyed namespace non-collision、malformed-tag and embedded-offset rejection；
- topic pass-one forced spill/latest-key selection、unkeyed retain-exact、tombstone retention/removal、pass-two exact
  replay and decoder-fact drift rejection；
- recovery checkpoint golden bytes、truncation at every byte、CRC/SHA failure、directory lookup and prefix merge；
- object-key content-hash/output-attempt addressing、already-exists exact comparison and deleted-key no-reuse；
- LocalStack range/footer/list/delete identity behavior and local-file traversal/symlink safety。
- streaming upload backpressure/cancel/retry、no whole-object aggregation、staging byte semaphore and crash cleanup。
