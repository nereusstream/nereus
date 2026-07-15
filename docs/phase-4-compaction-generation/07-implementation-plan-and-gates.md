# Phase 4 Implementation Plan and Gates

## 1. Current Status

F4-M0 is complete against Nereus `e330969cd5c2c11cd38d0bd7f687185171ae91e2` and local Pulsar
`c2f7c22fdc562022b992a5c7aecb5fd5c02d318d`. F4-M1、F4-M2 and F4-M3 completed their ordinary and Docker-backed
final gates on 2026-07-15；the following foundation parts are implemented and covered by focused and real-service tests：

- F4 API identities、materialization module boundary、Oxia keyspace/records/codecs/store adapters and conditional
  delete surface；
- replayable private staging、guarded/retried S3 PUT、SDK-response plus exact-byte upload completion、exact
  logical-prefix LIST、HEAD-before-DELETE、narrow HTTP 405/501 conditional-delete fallback and the safe local
  object-store fixture；
- strict physical-object/reference-domain proof values、the protocol-neutral generation activation proof surface and
  the durable create/revalidate/release reader-pin handshake backed by one process/object lease；
- durable protection acquire/revalidate/same-key owner-transfer/owner-authorized-release handshakes, including
  response-loss cleanup ownership and fail-safe deletion-veto behavior；
- deterministic fake physical metadata storage used by the reader/reference handshake tests；
- partition-aware in-memory Oxia backend plus generation、64-shard stream-registration、256-shard physical-root and
  exact conditional-delete store contracts；
- production/fake shared physical-root lifecycle/epoch validation, record contradiction tests and response-loss-safe
  generation-index/task create recovery with immutable identity checks；
- `GenerationMetadataTransitions` guards closed index/task lifecycles、the publication-before-allocation task shape、
  monotonic checkpoint/registration/recovery progress and immutable retention boundaries；
- 43 frozen F4 metadata envelope vectors cover every lifecycle/optional branch；focused real Oxia covers slash-aware
  fixed-depth ranges、restart、CAS、pagination and conditional metadata delete；
- M1 contract/document/module/source-lock/guarded-PUT audits and the `phase4M1Check`/`phase4M1FinalCheck` tasks exist。

`phase4M1Check`、`phase4M1FinalCheck --rerun-tasks`、`phase4M2Check`、
`phase4M2FinalCheck --rerun-tasks`、`phase4M3Check` and `phase4M3FinalCheck --rerun-tasks` passed on 2026-07-15. M2 adds authoritative committed-generation resolve/read,
restart-safe publication, pin-owned fallback/quarantine and a real Oxia/LocalStack independent-runtime fixture.
F4-M1、F4-M2 and F4-M3 are complete/final-gated. F4-M3 has nine implementation checkpoints：the real compacted
format/read path, the deterministic policy/planner/task-store/recovery/registered-stream-scan path, and the
exact-source reader plus claim-to-output-ready worker path, followed by task-protection owner crash-cut reconciliation
across recovery/publication, then monotonic advisory checkpoint reconstruction plus bounded dispatcher/service
lifecycle. A sixth checkpoint proves Pulsar Entry/NCP1 exact-byte and middle-batch MessageId round trip while keeping
the generation projection ref out of per-entry metadata. A seventh checkpoint implements the protocol-neutral
topic-compaction decoder/strategy SPI, closed disposition wire ids, immutable compaction-key facts and an exact
frozen-identity registry. An eighth checkpoint implements proof-driven terminal task/protection/stats/old-checkpoint
retirement with exact reload and response-loss convergence. A ninth checkpoint implements COMMITTED-source topic
bootstrap、tagged-v1 keyed/unkeyed encoding、shared-budget checksum-verified sorted spills、bounded two-pass exact
replay and the NTC1 worker/view-isolated publication path. M3's deterministic and real-service gates additionally
prove two-worker claim convergence、restart/output reuse、lost `OUTPUT_READY` response recovery、full output bytes and
all-64-shard pagination without watch/process-local hints. F4-M3 is complete/final-gated. This is not a Phase 4
completion claim. F4-M4 implementation checkpoint A now provides the NRC1 domain/format、spill-backed one-at-a-time
writer、strict bounded open/lookup path、attempt/key identity and authoritative metadata verifier. Its focused object-
store/materialization tests and `phase4M4CheckpointCheck --rerun-tasks` passed on 2026-07-15 (114 tasks) and are the
object-protocol evidence boundary. Checkpoint B now provides exact intent preparation、ACTIVE root and permanent
commit-owned protection before head CAS、exact generation-zero materialization and permanent index-owned protection
before strict success, with the same sequence used by recovery and wired into production shared-Oxia assembly.
Metadata/core/pulsar unit tests and affected real-service source-set compilation pass；a Docker-backed checkpoint-B
execution gate has not yet run. Full recovery-root/
anchor-aware retirement/GC and async/Pulsar execution paths still arrive in the remainder of F4-M4–M6. Checkpoint C's
current foundation adds the exact paged append-recovery tail in production/fake metadata stores, the root-stable
`AnchorAwareCommitWalker`, the bounded whole-commit `RecoveryCheckpointBuilder`, and root-owned pending/permanent
protection handshakes. Metadata contracts and focused walker/builder tests pass；guarded upload、full verification、
root CAS/reconciliation、retirement and GC were not implemented by that foundation. Checkpoint D now adds guarded
if-absent NRC1 upload、exact HEAD/full verification、pending protection、root CAS with exact response-loss reload, and
bounded restart reconciliation of current-root object/target protections from the NRC1 publication table. Focused
tests use the real in-memory Oxia adapter plus fake physical/local object storage to prove lost root-CAS response and
process death after CAS but before permanent protection. Checkpoint E now adds sparse offset-to-entry lookup、the
root-double-read/pinned checkpoint append replay adapter、shared live/NRC1 request validation and the explicit
`AppendCoordinator`/`DefaultStreamStorage` injection seam. Checkpoint F now adds exact committed-index restore、
raw/envelope digest separation、root-stable pinned NRC1 repair、highest healthy target selection、current-root-owned
target protection and the `GenerationReadResolver` terminal repair seam. Runtime composition、retirement and GC
remain. Checkpoint G now adds exact read-before-delete generation-zero source and Phase 1 object-audit metadata
adapters、committed-marker capture、legacy/generic encoding separation and a borrowed get/delete-only shared-Oxia
bridge. Checkpoint H adds strict GC configuration、ACTIVE-root candidates and bounded canonical plans whose complete
domain/protection/metadata-key facts can be recomputed after restart. Checkpoint I adds canonical exact-domain
aggregation、mandatory metadata-fact reload、recoverable ACTIVE/MARKED/DELETING fencing and full 256-shard metadata
root enumeration. It stops at durable delete intent；no source/protection/audit/object delete path is enabled by
checkpoints G–I. Checkpoint J adds query-bound stateless revalidation、exact reference-to-removal binding and the
affected-stream generation、append-recovery and materialization reference domains. Projection/cursor/future-sentinel
domains、ownerless global absence proof、retirement/destructive coordination and runtime composition remain pending.

`phase4M4ProtectedAppendCheck` passed on 2026-07-15, including the inherited M1–M3/NRC1 chain、all affected Nereus
checks/source-set compilation and the locked local Pulsar M4 check. This is checkpoint-B evidence, not a claim that
the Docker-backed M4 final scenarios or any delete path are available.

A later milestone is complete only when：

1. every named production artifact exists or a reviewed equivalent is documented；
2. record/object golden bytes and focused deterministic tests pass；
3. fake and production adapters share the same invariant contract；
4. the milestone Gradle gate exists and is green；
5. required real Oxia/LocalStack/Pulsar evidence is green；
6. both Nereus and local Pulsar worktrees are clean at the recorded source locks；
7. authoritative docs are updated from planned to exact implemented behavior in the same commit。

## 2. F4-M0 Design Gate

### 2.1 Evidence completed

- audited current `ReadTarget`、resolver、dispatcher/registry、offset-index and stable append split；
- audited `StreamHeadRecord`/commit chain and proved that source deletion needs a recovery checkpoint；
- audited current ObjectStore and low-level Oxia client absence of list/delete；
- audited F3 owner-session、pending protection/trim、cursor snapshot inventory and no-op trim handoff；
- audited local Pulsar capability、feature resolver/validator、loaded/unloaded admin and backlog eviction paths；
- froze separate `COMMITTED`/`TOPIC_COMPACTED` views and exact opaque Entry preservation；
- froze single-key generation publication, durable reader/protection handshake and physical root lifecycle；
- froze protected two-stage generation-zero prepare/head ordering and sync/async acknowledgement cuts；
- froze whole-index planner tiling plus current-policy fixed-point termination；
- froze 256-shard physical-root enumeration so MARKED/DELETING recovery never depends on object listing；
- froze 64-shard registered-stream discovery as a liveness hint, with projection/head/index remaining truth；
- froze the new module/dependency boundary and M1-M6 delivery order。

### 2.2 Gate conclusion

The protocol permits F5/F6/F8 to extend references/views without remapping offsets or making projection/task/GC a new
correctness owner. No implementation-blocking open decision remains. Implementation must not weaken the design to
avoid recovery checkpoint or durable read pins.

## 3. F4-M1 — Metadata, Object Lifecycle, and IO Primitives

### 3.1 Production artifacts

`nereus-api`：

```text
ReadView.java
GenerationId.java
PublicationId.java
ObjectKeyHash.java
```

`nereus-metadata-oxia`：

```text
F4Keyspace.java
F4ScanKind.java
F4ScanToken.java
GenerationIndexIdentity.java
GenerationZeroIndexEncoding.java
ObjectProtectionIdentity.java
GenerationMetadataStore.java
PhysicalObjectMetadataStore.java
VersionedGenerationSequence.java
AllocatedGeneration.java
VersionedGenerationCandidate.java
VersionedGenerationZeroIndex.java
VersionedGenerationIndex.java
VersionedMaterializationTask.java
VersionedMaterializationStreamRegistration.java
VersionedMaterializationCheckpoint.java
VersionedRangeRetentionStats.java
VersionedRecoveryCheckpointRoot.java
VersionedPhysicalObjectRoot.java
VersionedReaderLease.java
VersionedObjectProtection.java
GenerationScanPage.java
TaskScanPage.java
MaterializationCheckpointScanPage.java
StreamRegistrationScanPage.java
RangeRetentionStatsScanPage.java
PhysicalObjectRootScanPage.java
ReaderLeaseScanPage.java
ObjectProtectionScanPage.java
OxiaJavaGenerationMetadataStore.java
OxiaJavaPhysicalObjectMetadataStore.java
GenerationMetadataTransitions.java
PhysicalObjectRootTransitions.java

records/
  GenerationSequenceRecord.java
  GenerationIndexRecord.java
  GenerationLifecycle.java
  MaterializationStreamRegistrationRecord.java
  MaterializationPolicyRecord.java
  MaterializationTaskRecord.java
  TaskLifecycle.java
  SourceGenerationRecord.java
  WorkerClaimRecord.java
  MaterializationOutputRecord.java
  MaterializationCheckpointRecord.java
  RangeRetentionStatsRecord.java
  RecoveryCheckpointRootRecord.java
  RecoveryCheckpointReferenceRecord.java
  PhysicalObjectRootRecord.java
  PhysicalObjectLifecycle.java
  ObjectReaderLeaseRecord.java
  ObjectProtectionRecord.java
  ObjectProtectionType.java

codec/
  F4Binary.java
  F4MetadataCodecs.java
  GenerationSequenceRecordCodecV1.java
  GenerationIndexRecordCodecV1.java
  MaterializationStreamRegistrationRecordCodecV1.java
  MaterializationTaskRecordCodecV1.java
  MaterializationCheckpointRecordCodecV1.java
  RangeRetentionStatsRecordCodecV1.java
  RecoveryCheckpointRootRecordCodecV1.java
  PhysicalObjectRootRecordCodecV1.java
  ObjectReaderLeaseRecordCodecV1.java
  ObjectProtectionRecordCodecV1.java
```

`PartitionedOxiaClient.Backend`、production backend and test backend add only conditional `deleteIfVersion`.

`nereus-object-store`：

```text
ObjectKeyPrefix.java
ReplayableObjectUpload.java
ByteBufferObjectUpload.java
PutObjectAttemptGuard.java
ObjectPutRetryPolicy.java
staging/StagedObjectFile.java
staging/PrivateStagedObjectFile.java
staging/StagingFileManager.java
ListObjectsOptions.java
ListObjectsResult.java
ListedObject.java
DeleteObjectOptions.java
DeleteObjectResult.java

ObjectStore.java                              add bounded list/delete
ObjectStoreConfiguration.java                 add bounded guarded-PUT retry policy
S3CompatibleObjectStore.java                 streaming PUT + exact HEAD/list/delete
S3ObjectErrorMapper.java                     LIST/DELETE mapping
testing/LocalFileObjectStore.java            safe list/delete fixture
```

`nereus-core` physical-reference primitives：

```text
physical/PhysicalObjectIdentity.java
physical/ObjectReadPinManager.java
physical/DefaultObjectReadPinManager.java
physical/ObjectReadLease.java
physical/ObjectProtectionManager.java
physical/DefaultObjectProtectionManager.java
physical/ObjectProtection.java
physical/ObjectProtectionOwner.java
physical/ObjectProtectionRequest.java
physical/GcReferenceDomain.java
physical/GcReferenceQuery.java
physical/GcReferenceQueryKind.java
physical/GcReferenceSnapshot.java
physical/GcAuthorityToken.java
physical/GcReference.java
capability/GenerationProtocolActivationGuard.java
capability/GenerationOperation.java
capability/GenerationActivationSubject.java
capability/LiveProjectionSubject.java
capability/DomainValidatedDeletionSubject.java
capability/GenerationActivationProof.java
```

`nereus-materialization` module scaffold adds `build.gradle.kts` now, then M2/M3 add implementation classes. Add the
module to `settings.gradle.kts` and `nereus-bom`；dependency audit enforces
`materialization -> core` and forbids `core -> materialization` or Pulsar imports.

`ObjectProtectionIdentity` lives with the Oxia store/key identity rather than in `nereus-core` because
`PhysicalObjectMetadataStore` uses it directly and metadata must not depend on core. Core protection managers reuse
that protocol-neutral metadata identity; this preserves the documented acyclic dependency direction.

### 3.2 Focused tests

```text
nereus-api/.../F4ApiValueTest
nereus-metadata-oxia/.../F4KeyspaceTest
nereus-metadata-oxia/.../F4ScanTokenTest
nereus-metadata-oxia/.../F4RecordValidationTest
nereus-metadata-oxia/.../F4MetadataCodecGoldenTest
nereus-metadata-oxia/.../GenerationMetadataTransitionsTest
nereus-metadata-oxia/.../GenerationMetadataStoreContractTest
nereus-metadata-oxia/.../MaterializationStreamRegistryContractTest
nereus-metadata-oxia/.../PhysicalObjectMetadataStoreContractTest
nereus-metadata-oxia/.../ConditionalDeleteContractTest
nereus-metadata-oxia/.../F4MetadataStoreOxiaIntegrationTest
nereus-object-store/.../ObjectStoreListDeleteContractTest
nereus-object-store/.../ReplayableObjectUploadContractTest
nereus-object-store/.../GuardedPutObjectAttemptContractTest
nereus-object-store/.../ObjectPutRetryPolicyTest
nereus-object-store/.../S3ObjectListDeleteErrorMapperTest
nereus-object-store/.../testing/LocalFileObjectStoreGcSafetyTest
nereus-core/.../ObjectReadPinManagerTest
nereus-core/.../ObjectProtectionManagerTest
nereus-core/.../ObjectReferenceHandshakeModelTest
nereus-core/.../ObjectProtectionOwnerTransferTest
nereus-core/.../GcReferenceValueTest
```

Golden tests include every lifecycle/optional branch and frozen hex. Registry tests freeze SHA-256 low-six-bit shard
vectors、all 64 partition keys、strict continuation bounds and response-loss idempotence. Store contract tests run
unchanged against fake and real adapters；physical-root tests additionally freeze all 256 first-hash-byte shards、
root-only pagination and same-object root/lease/protection partition equality.

### 3.3 Gates

```text
./gradlew phase4M1Check
./gradlew phase4M1FinalCheck --rerun-tasks
```

Ordinary gate compiles/tests M1 modules、both integration source sets and contract/dependency/doc/source-lock/guarded-
PUT audits. Final gate adds focused real Oxia slash-aware fixed-depth range/CAS/pagination/conditional-delete coverage
and pinned LocalStack SDK-response/exact-byte upload、list、HEAD identity and conditional-delete compatibility tests.
Both passed on 2026-07-15. M1 does not publish a higher generation or delete a product object.

## 4. F4-M2 — Generation Publication and Committed Read

> 当前实现检查点（2026-07-15）：`StreamViewReader`/`ViewReadResult`、exact
> `ReadTargetReaderKey` registry/dispatch、`GenerationAllocator`、strict `GenerationIndexValidator`、old/new
> generation exact lookup、physical identity resolver、durable pin-owned `GenerationReadResolver` 及其
> candidate-limit/view-isolation/stale-selection tests 已落地。`ReadCoordinator` 已持有 pin 贯穿 exact-reader IO
> 和 terminal cleanup，并以 per-operation candidate exclusion 实现 same-view fallback；永久 missing/checksum
> corruption 会 quarantine exact root、选中的 higher index 与 bounded scan 发现的所有同 stream/view、同
> object-key `COMMITTED` index。瞬态 IO failure 默认在同 candidate 上 fresh resolve/read 两次，达到可配
> 阈值后才 same-view fallback，且不写健康状态。严格
> policy/source/task/output 值、canonical identity/digest、secure publication id、durable mapper、HEAD/full-format
> verifier seam 与 monotonic operation deadline 也已落地。restart-safe `DefaultGenerationCommitter` 现已执行
> publication-id/generation task freeze、deterministic PREPARED create、发布前全重验证、唯一
> `PREPARED -> COMMITTED` visibility CAS、visible-protection owner transfer 和 task PUBLISHED convergence；并发、
> response-loss、已发布重进与 exactly-proven ABORTED 重分配均有测试。
> `GenerationPublicationReconciler` 已提供 recovered task/output pair 的幂等重进入口。M2 ordinary/final
> aggregate gates 已通过；real Oxia/LocalStack fixture 验证两个独立 runtime 的并发发布收敛、COMMITTED
> response loss 后重启恢复，以及 higher object 丢失后的 exact pin release、root/index quarantine 与同 view
> generation-zero fallback。该 fixture 同时暴露并修复 inline `EntryIndexRef` durable round-trip 的内容相等性。
> F4-M2 is complete/final-gated。M3 已接入真实 compacted format、worker-owned source/output protections、
> 跨 key crash-cut 恢复、advisory checkpoint reconciliation 与 bounded service lifecycle checkpoints；Pulsar
> opaque-entry round trip、topic-compaction neutral SPI/registry 与 terminal workflow-metadata retirement 已完成；
> COMMITTED-source topic bootstrap、tagged-v1 NTC1 key、sorted-spill two-pass engine、worker 与 isolated
> publication tests 也已完成；M3 ordinary/final gates 已通过；M4 仍需补齐
> recovery-root/anchor-aware source reachability，physical delete 继续禁用。

### 4.1 Production artifacts

`nereus-core`：

```text
read/StreamViewReader.java
read/ViewReadResult.java
read/ReadTargetReader.java
read/ReadTargetReaderKey.java
read/ReadTargetReaderRegistry.java
read/GenerationReadResolver.java
read/GenerationReadCandidate.java
read/PinnedResolvedRange.java
read/ReadTargetDispatcher.java                   exact-key grouping
read/ReadCoordinator.java                        pin lifetime/fallback
read/OffsetIndexCache.java                       decode hint only; never suppress F4 scan
wal/PrimaryWalReader.java                        compatibility adapter
wal/PrimaryWalRegistry.java                      appender-only or bridged reader registry
```

`nereus-metadata-oxia`：

```text
GenerationAllocator.java
GenerationIndexValidator.java
OxiaMetadataStore.scanOffsetIndex                hydrate F4 COMMITTED records
OxiaJavaClientMetadataStore                      old/new decode and exact lookup
testing/FakeOxiaMetadataStore                    identical behavior
```

`nereus-materialization`：

```text
GenerationCommitter.java
DefaultGenerationCommitter.java
GenerationCommitResult.java
GenerationPublicationReconciler.java
```

No compacted Parquet production reader is required until M3；M2 tests use exact test readers, including one that
checks LocalStack object bytes, and prove the publication/resolver contract independently of the future compacted
file format.

### 4.2 Focused tests

```text
GenerationAllocatorConcurrencyTest
GenerationIndexPublicationTest
GenerationPublicationFailureInjectionTest
GenerationPublicationPropertyTest
GenerationPublicationReconcilerTest
MaterializationDomainTest
GenerationIndexCompatibilityTest
GenerationReadResolverTest
MetadataGenerationReadFailureHandlerTest
ReadTargetReaderRegistryTest
ReadTargetDispatcherMixedFormatTest
PinnedReadCoordinatorTest
GenerationPublicationOxiaS3IntegrationTest             real-service final fixture
```

Required assertions：unique generations with valid gaps；allocation-response loss plus interleaving can burn only
unattached gaps and exactly one task-attached G may create an index；PREPARED invisible；one CAS publication point；task/checkpoint
never visible；highest covering generation；same-view fallback only；exact index/root post-pin revalidation；unknown
format fails closed；stale cache/watch loss cannot hide a higher generation or a fresh fallback；old generation-zero
goldens still resolve.

### 4.3 Gates

```text
./gradlew phase4M2Check
./gradlew phase4M2FinalCheck --rerun-tasks
```

Final adds independent-runtime real Oxia allocation/publication/response-loss and LocalStack pin/fallback fixtures. Physical
delete stays disabled and all source objects remain protected. Both gates passed on 2026-07-15；the final gate was
also run with `--rerun-tasks` so the real-service evidence was not satisfied from Gradle cache.

## 5. F4-M3 — Object Format, Planner, and Worker

> Implementation checkpoints (2026-07-15)：the pinned Apache Parquet/Hadoop/ZSTD dependencies、frozen NCP1/NTC1
> schemas/metadata parser、streaming private-staging writer、content-addressed identity/footer reference、bounded
> exact-range strict reader、NTC1-only facade、whole-file verifier、task-aware materialization verifier and core NCP1
> adapter are implemented. The seven object-format suites plus `ParquetCompactedTargetReaderTest` and
> `CompactedMaterializationFormatVerifierTest` prove standard `PAR1` bytes、random dense/ZSTD+uncompressed files、
> sparse NTC1、backpressure/cancellation cleanup、footer/page corruption classification、whole-file CRC/SHA/key
> identity、cross-stream rejection and task-policy binding. The next checkpoint implements deterministic policy
> versioning、the bounded whole-index DAG planner/fixed-point rule、exact-source task-create revalidation、complete
> durable policy snapshots、different-clock duplicate-create convergence、claim-expiry/publication recovery、per-stream
> task pagination and all-64-shard registered-stream discovery with activation revalidation. A third checkpoint adds
> the stream-scoped exact-source reader, durable pin/revalidation, lossless single-source row stream, deterministic
> claim/heartbeat worker, guarded upload, strict full-output verification, source/output task protections and typed
> durable failure transitions through `OUTPUT_READY`. A fourth checkpoint adds same-logical-owner monotonic
> `acquireOrTransfer`, exact task-protection reconstruction, recovery/publication integration, duplicate expired-claim
> CAS convergence and `PUBLISHED` repair. A fifth checkpoint adds strict `MaterializationConfig` validation、bounded
> full-proof advisory checkpoint reconciliation with exact CAS-response-loss reload、global/per-stream fair task
> dispatch and the non-overlapping/coalesced/deadline-close `MaterializationService` loop. A sixth checkpoint adds
> the managed-ledger cross-layer `PulsarEntryOpaqueRoundTripTest` and keeps the generation-level projection identity
> out of the original logical `ReadBatch` surface. A seventh checkpoint adds `TopicCompactionDecoder`,
> `CompactionRecord`, closed `CompactionDisposition` wire ids, versioned `TopicCompactionStrategy` and
> `TopicCompactionRegistry`; its focused test proves exact frozen-id resolution, duplicate/mutable identity rejection
> and immutable read-only key ownership. An eighth checkpoint adds `DefaultTerminalWorkflowMetadataRetirer`, bounded
> checkpoint scanning and exact candidate-by-key lookup. It prevents PUBLISHED recovery from recreating temporary
> protections, proves the exact index/checkpoint/root/visible owner, retires failed tasks only without a publication,
> removes stale stats/old checkpoints, and converges lost protection/task delete responses. A ninth checkpoint adds
> the COMMITTED-source/TOPIC-target planner split、`TAGGED_V1` keyed/unkeyed namespace、shared-budget SHA-verified
> sorted spills、bounded survivor bitmap、two-pass exact replay and NTC1 worker/publication integration. Focused tests
> force spills, prove latest-key/unkeyed/tombstone behavior, fail on decoder drift, write/verify real Parquet NTC1 and
> publish only to the TOPIC_COMPACTED namespace. The ordinary and real-service final gates below passed on
> 2026-07-15, so F4-M3 is complete/final-gated；production activation stays disabled until M4–M6 complete.

### 5.1 Production artifacts

`nereus-object-store`：

```text
compacted/CompactedObjectFormatV1.java
compacted/CompactedObjectRow.java
compacted/CompactedObjectWriteRequest.java
compacted/CompactedObjectWriteResult.java
compacted/CompactedObjectWriter.java
compacted/CompactedObjectReader.java
compacted/ParquetCompactedObjectWriter.java
compacted/ParquetCompactedObjectReader.java
compacted/TopicCompactedObjectReader.java
compacted/TopicCompactionKeyEncodingV1.java
compacted/CompactedObjectVerificationRequest.java
compacted/CompactedObjectVerifier.java
staging/ManagedStagingFile.java
staging/PrivateStagingSpillFile.java
```

Add pinned Parquet/ZSTD dependencies and dependency constraints. The format does not import Pulsar.

`nereus-materialization`：

```text
MaterializationPolicy.java
MaterializationPolicyFactory.java
TopicCompactionSpec.java
MaterializationTask.java
MaterializationOutput.java
SourceGeneration.java
MaterializationPlanner.java
DefaultMaterializationPlanner.java
MaterializationWorker.java
DefaultMaterializationWorker.java
ExactSourceRangeReader.java
DefaultExactSourceRangeReader.java
ExactSourceRead.java
ExactSourceReadSummary.java
MaterializationTaskStore.java
MaterializationTaskRecovery.java
MaterializationTaskProtectionReconciler.java
DefaultMaterializationTaskProtectionReconciler.java
MaterializationTaskProtections.java
RegisteredMaterializationStreamScanner.java
MaterializationCheckpointReconciler.java
DefaultMaterializationCheckpointReconciler.java
MaterializationTaskDispatcher.java
DefaultMaterializationTaskDispatcher.java
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
MaterializationService.java
DefaultMaterializationService.java
MaterializationConfig.java
MaterializationMetricsObserver.java
CompactedMaterializationFormatVerifier.java
```

`nereus-core` registers `ParquetCompactedTargetReader` through the exact NCP1 reader key at product composition.
The internal reader call carries the selected `StreamId` in addition to `ResolvedRange` so a self-consistent object
from another stream cannot pass merely because the old generic resolved-range shape omitted stream identity.

### 5.2 Focused tests

```text
CompactedParquetGoldenTest
CompactedParquetStrictReaderTest
CompactedParquetDensePropertyTest
CompactedParquetCorruptionTest
CompactedObjectStreamingUploadTest
CompactedObjectStagingLimitTest
PulsarEntryOpaqueRoundTripTest                 in managed-ledger integration layer
TopicCompactedSparseFormatTest
TopicCompactionKeyEncodingV1Test
TopicCompactionRegistryTest
TopicCompactionEngineTest
ParquetCompactedTargetReaderTest
CompactedMaterializationFormatVerifierTest
MaterializationTaskIdentityTest
MaterializationPolicyFactoryTest
MaterializationOutputAttemptIdentityTest
MaterializationPlannerTest
MaterializationPlannerFixedPointTest
MaterializationPlannerOverlapTilingTest
MaterializationTaskStoreTest
MaterializationWorkerTest
MaterializationWorkerFailureInjectionTest
MaterializationWorkerClaimModelTest
ExactSourceRangeReaderTest
MaterializationTaskRecoveryTest
MaterializationTaskProtectionReconcilerTest
RegisteredMaterializationStreamScannerTest
MaterializationCheckpointReconcilerTest
DefaultMaterializationTaskDispatcherTest
TerminalWorkflowMetadataRetirementTest
MaterializationServiceCloseTest
MaterializationConfigTest
```

Pulsar Entry round trip includes unbatched、compressed、batched、properties、ordering key and exact middle-batch
MessageId bytes. Worker tests reject parse/re-encode even when decoded messages are semantically equal.
Planner fixed-point tests run repeated full registry passes：generation-zero/old-policy sources produce bounded work,
one healthy current-policy higher generation produces none, eligible multi-source merge produces exactly one new task,
and the post-publication pass produces no semantically identical higher generation.
Overlap-tiling tests permute nested/crossing concurrent generations and prove the bounded DAG chooses the same
gap-free whole-index path without clipping、duplicating offsets or depending on scan return order.

### 5.3 Gates

```text
./gradlew phase4M3Check
./gradlew phase4M3FinalCheck --rerun-tasks
```

Final runs real Oxia task claims and LocalStack full upload/footer/range read, two independent workers, every
upload/output/publish crash cut, all-shard registry pagination/watch loss, fresh-process recovery of an unloaded
stream whose committed head has no task, and full output byte comparison. It may publish higher generations only
with the test activation guard；production rollout remains disabled by default.

Both gates passed on 2026-07-15；the final gate was run with `--rerun-tasks`. `MaterializationWorkerOxiaS3IntegrationTest`
proves two independent worker runtimes converge on one durable attempt/output、a fresh process reuses the exact
published Parquet bytes, and a lost `OUTPUT_READY` CAS response reloads the frozen output. Deterministic failure/model
tests cover the remaining crash cuts, and `RegisteredMaterializationStreamScannerTest` forces page-size-one scans over
two registrations in every shard, repeats with a fresh scanner, and separately converges an unowned committed head
with no durable task. F4-M3 is complete/final-gated；M4 is the next implementation milestone.

## 6. F4-M4 — Recovery Checkpoint, Retirement, and GC

> Current status：in progress. Checkpoint A (NRC1 object protocol)、checkpoint B (protected generation-zero
> append/recovery)、checkpoint C (anchor-aware tail/planning foundation), and checkpoint D (guarded root publication
> plus restart protection reconciliation), checkpoint E (checkpoint-aware append replay adapter), and checkpoint F
> (checkpoint-derived committed-index repair), checkpoint G (exact retirement metadata adapters), checkpoint H
> (bounded/reconstructable GC config/candidate/plan values), and checkpoint I (exact domain aggregation、recoverable
> root MARK/DRAIN/DELETING fence and 256-shard scanner), and checkpoint J (query-bound revalidation、exact
> reference/removal binding and affected-stream generation/append-recovery/materialization domains) are implemented；
> runtime composition、projection/cursor/future-sentinel and ownerless-global domains、retirement/delete coordinators
> and physical/cursor GC completion remain before F4-M4 can be called complete or final-gated.

### 6.1 Production artifacts

`nereus-object-store`：

```text
checkpoint/RecoveryCheckpointCodecV1.java
checkpoint/DefaultRecoveryCheckpointCodecV1.java
checkpoint/RecoveryCheckpointFormatV1.java
checkpoint/RecoveryCheckpointFormatException.java
checkpoint/RecoveryCheckpointObject.java
checkpoint/RecoveryCheckpointEntry.java
checkpoint/RecoveryCheckpointPublication.java
checkpoint/RecoveryCheckpointPublicationPage.java        implemented checkpoint D
checkpoint/RecoveryCheckpointWriteRequest.java
checkpoint/RecoveryCheckpointWriteResult.java
checkpoint/RecoveryCheckpointDirectory.java
checkpoint/RecoveryCheckpointVerifier.java
checkpoint/RecoveryCheckpointBinary.java                 package-private strict codec
checkpoint/RecoveryCheckpointValidation.java             package-private value guard
```

`nereus-core` / `nereus-metadata-oxia`：

```text
append/StableAppendCommitter.java                 split prepare/protected commit
append/GenerationZeroIndexMaterializer.java       return exact versioned index
append/GenerationZeroPhysicalReferencePublisher.java
append/DefaultGenerationZeroPhysicalReferencePublisher.java
append/GenerationZeroProtectionIdentities.java
append/ProtectedStableAppend.java
append/ProtectedGenerationZero.java
append/AppendCoordinator.java                     protected sync path
DefaultStreamStorage.java                        required publisher injection
PreparedStableAppend.java
MaterializedGenerationZero.java
OxiaMetadataStore                                prepare/commit prepared append
OxiaJavaClientMetadataStore                      exact two-stage stable append
AppendRecoveryAnchor.java                        implemented checkpoint C foundation
AppendRecoveryCommit.java                        implemented checkpoint C foundation
AppendRecoveryTailCursor.java                    implemented checkpoint C foundation
AppendRecoveryTailPage.java                      implemented checkpoint C foundation
RecoveryCheckpointRootDigests.java               implemented checkpoint C foundation
recovery/AnchorAwareCommitWalker.java             implemented checkpoint C foundation
recovery/AppendRecoverySearcher.java              implemented checkpoint E seam
recovery/AppendReplayResolution.java               implemented checkpoint E terminal proof
recovery/CheckpointAppendReplayReader.java         implemented checkpoint E
recovery/MetadataAppendRecoverySearcher.java       implemented checkpoint E compatibility adapter
read/GenerationIndexRepairer.java                  implemented checkpoint F seam
read/GenerationIndexRepairResult.java              implemented checkpoint F terminal result
read/GenerationIndexRepairSource.java              implemented checkpoint F evidence label
read/MetadataGenerationIndexRepairer.java          implemented checkpoint F compatibility adapter
recovery/CheckpointDerivedIndexRepairer.java       implemented checkpoint F
recovery/RecoveryCheckpointProtectionIdentities.java implemented checkpoint F shared identity
GenerationIndexDigests.java                        implemented checkpoint F raw/envelope roles
GenerationMetadataStore.restoreCommittedFromCheckpoint implemented checkpoint F exact create/reload
retirement/RetirementMetadataClient.java                 implemented checkpoint G
retirement/RetirementMetadataKey.java                    implemented checkpoint G
retirement/RetirementMetadataValue.java                  implemented checkpoint G
retirement/SourceRetirementMetadataStore.java            implemented checkpoint G
retirement/OxiaJavaSourceRetirementMetadataStore.java    implemented checkpoint G
retirement/GenerationZeroMarkerIdentity.java             implemented checkpoint G
retirement/LegacyCommittedSliceIdentity.java             implemented checkpoint G
retirement/GenericCommittedAppendIdentity.java           implemented checkpoint G
retirement/VersionedGenerationZeroMarker.java             implemented checkpoint G
retirement/ObjectAuditRetirementStore.java               implemented checkpoint G
retirement/OxiaJavaObjectAuditRetirementStore.java       implemented checkpoint G
retirement/VersionedObjectManifestAudit.java             implemented checkpoint G
retirement/VersionedObjectReferencesAudit.java           implemented checkpoint G
OxiaJavaClientMetadataStore                    bridge live tail to root/NRC1
testing/FakeOxiaMetadataStore                  exact parity
```

`nereus-materialization`：

```text
recovery/RecoveryCheckpointCoordinator.java               implemented checkpoint D
recovery/RecoveryCheckpointBuilder.java           implemented checkpoint C foundation
recovery/RecoveryCheckpointMerger.java
recovery/RecoveryCheckpointProtectionManager.java implemented checkpoint C foundation
recovery/RecoveryCheckpointRootReconciler.java            implemented checkpoint D
recovery/MetadataRecoveryCheckpointVerifier.java
gc/GcCandidate.java                                      implemented checkpoint H
gc/GcCandidateRootState.java                             implemented checkpoint H
gc/GcPlan.java                                           implemented checkpoint H
gc/GcPlannedProtectionRemoval.java                       implemented checkpoint H
gc/GcPlannedMetadataRemoval.java                         implemented checkpoint H
gc/GcIdGenerator.java                                    implemented checkpoint H
gc/SecureGcIdGenerator.java                              implemented checkpoint H
gc/GcReferenceDomainVersion.java                         implemented checkpoint I
gc/GcReferenceCollectionStatus.java                      implemented checkpoint I
gc/GcReferenceCollection.java                            implemented checkpoint I
gc/GcReferenceDomainRegistry.java                        implemented checkpoint I
gc/GcPlanMetadataRevalidator.java                        implemented checkpoint I
gc/GcReferenceSnapshotAccumulator.java                   implemented checkpoint J
gc/GenerationReferenceDomain.java                        implemented checkpoint J affected-stream domain
gc/AppendRecoveryReferenceDomain.java                    implemented checkpoint J affected-stream domain
gc/MaterializationReferenceDomain.java                   implemented checkpoint J affected-stream domain
gc/FutureCatalogSentinelDomain.java
gc/SourceRetirementCoordinator.java
gc/StreamRegistrationRetirementCoordinator.java
gc/PhysicalGcMarkStatus.java                              implemented checkpoint I
gc/PhysicalGcMarkResult.java                              implemented checkpoint I
gc/PhysicalGcAdvanceStatus.java                           implemented checkpoint I
gc/PhysicalGcAdvanceResult.java                           implemented checkpoint I
gc/PhysicalObjectGarbageCollector.java                    implemented checkpoint I fence only
gc/PhysicalObjectRootVisitor.java                         implemented checkpoint I
gc/PhysicalObjectRootScanResult.java                      implemented checkpoint I
gc/PhysicalObjectRootScanner.java                         implemented checkpoint I
gc/PhysicalRootTombstoneRetirementCoordinator.java
gc/TombstoneRetirementResult.java
gc/TombstoneRetirementStatus.java
gc/PhysicalRootBackfillCoordinator.java
gc/PhysicalRootBackfillRequest.java
gc/PhysicalRootBackfillReport.java
gc/PhysicalRootBackfillFailure.java
gc/PhysicalRootBackfillStage.java
gc/ObjectInventoryScanner.java
gc/PhysicalGcConfig.java                                 implemented checkpoint H
gc/GcMetricsObserver.java
```

`nereus-managed-ledger`：

```text
retention/CursorSnapshotReferenceDomain.java
retention/CursorSnapshotGcScanner.java
retention/ProjectionGenerationReferenceDomain.java
cursor/DefaultCursorSnapshotStore.java          pin/protection integration
cursor/CursorSnapshotInventory.java             unchanged authority semantics
```

### 6.2 Focused tests

```text
RecoveryCheckpointGoldenTest
RecoveryCheckpointAttemptIdentityTest
RecoveryCheckpointStrictDecodeTest
RecoveryCheckpointStreamingTest
RecoveryCheckpointSparseDirectoryTest
RecoveryCheckpointDomainValidationTest
MetadataRecoveryCheckpointVerifierTest
RecoveryCheckpointCoordinatorTest
RecoveryCheckpointBuilderTest
RecoveryCheckpointMergeTest
AnchorAwareCommitWalkerTest
CheckpointAppendReplayTest                         implemented checkpoint E
CheckpointDerivedIndexRepairTest                    implemented checkpoint F
MetadataGenerationIndexRepairerTest                 implemented checkpoint F
SourceRetirementMetadataStoreContractTest                  implemented checkpoint G
ObjectAuditRetirementStoreContractTest                     implemented checkpoint G
RecoveryCheckpointFailureInjectionTest
PreparedStableAppendContractTest
GenerationZeroPhysicalReferencePublisherTest
ProtectedStableAppendFailureInjectionTest
GenerationZeroVisibleProtectionRepairTest
GcReferenceDomainRegistryTest                              implemented checkpoint I
GenerationReferenceDomainTest                             implemented checkpoint J
AppendRecoveryReferenceDomainTest                         implemented checkpoint J
MaterializationReferenceDomainTest                        implemented checkpoint J
PhysicalObjectGarbageCollectorTest                         implemented checkpoint I fence/lost-response tests
PhysicalObjectGarbageCollectorModelTest
PhysicalObjectGarbageCollectorFailureInjectionTest
PhysicalGcConfigTest                                      implemented checkpoint H
GcPlanTest                                                implemented checkpoint H
PhysicalObjectRootScannerTest                              implemented checkpoint I
PhysicalRootTombstoneRetirementTest
LatePutAfterTombstoneTest
PhysicalRootBackfillCoordinatorTest
MultiStreamWalRetirementTest
GenerationRetirementFallbackTest
CursorSnapshotGcScannerTest
CursorSnapshotGcRaceTest
ProjectionGenerationReferenceDomainTest
ObjectInventoryScannerTest
StreamRegistrationRetirementCoordinatorTest
FutureCatalogSentinelTest
```

The GC model enumerates all reader/protection/mark/delete orderings for a small state space. Failure tests kill after
every conditional metadata delete and object delete response. Cursor snapshot tests cover ACTIVE plus both pending F3
retention lifecycles and owner/root changes during the drain wait. Tombstone tests cover both exact HEAD-absence
windows, every L0 audit/root conditional-delete response-loss cut and a guarded provider retry racing final root
retirement.

### 6.3 Gates

```text
./gradlew phase4M4CheckpointCheck
./gradlew phase4M4ProtectedAppendCheck
./gradlew phase4M4RecoveryRootCheck
./gradlew phase4M4CheckpointReplayCheck
./gradlew phase4M4CheckpointIndexRepairCheck
./gradlew phase4M4RetirementMetadataCheck
./gradlew phase4M4GcPlanCheck
./gradlew phase4M4RootFenceCheck
./gradlew phase4M4ReferenceDomainsCheck
./gradlew phase4M4Check
./gradlew phase4M4FinalCheck --rerun-tasks
```

`phase4M4CheckpointCheck` is deliberately narrower than the milestone gates：it verifies the implemented NRC1
streaming/strict-read/canonical-metadata boundary and all earlier M3 prerequisites. It must not be cited as evidence
that recovery-root publication、source retirement or any physical deletion path is enabled.

`phase4M4ProtectedAppendCheck` extends that checkpoint chain with the exact two-stage metadata surface、the one raw
core caller rule、ordinary/recovery ordering、production shared-Oxia composition、unit checks and compilation of every
affected real-service source set. It is checkpoint-B evidence only；it does not run or imply the later recovery-root、
retirement、GC or M4 final-service scenarios.

`phase4M4RecoveryRootCheck` extends checkpoint B with anchor-aware tail/planning contracts, bounded NRC1 publication
enumeration, guarded upload/root-CAS sequencing, digest-role separation and restart-safe permanent-protection repair.
It includes focused lost-response/process-death tests and the affected module checks. It is checkpoint-D evidence；it
does not imply that checkpoint-aware append replay/index repair、source retirement、physical/cursor GC or the
Docker-backed M4 final gate is available.

`phase4M4CheckpointReplayCheck` extends checkpoint D with sparse offset lookup、shared live/NRC1 append-record
validation、root-change restart、durable read-pin lifetime and the terminal replay integration seam. It is checkpoint-E
evidence only；the default runtime does not select the checkpoint reader until the later F4 composition checkpoint,
and index repair、retirement、GC and delete enablement remain unavailable.

`phase4M4CheckpointIndexRepairCheck` extends checkpoint E with the exact committed-index restore contract、raw-record
versus durable-envelope digest separation、root-stable NRC1 pin/lookup、highest healthy ACTIVE target selection、
root-owned target protection、activation/trim/root revalidation and resolver fresh rescan. It covers index retirement,
root-at-pin change, trim no-write and lease cleanup. It is checkpoint-F evidence only；the default runtime composition
still selects the live-only repair adapter, and source/index retirement、physical/cursor GC and all delete enablement
remain unavailable.

`phase4M4RetirementMetadataCheck` extends checkpoint F with the dedicated read-before-delete source/object-audit
adapters. It proves legacy/generic generation-zero index、marker and commit-node separation；canonical key/partition、
encoded version、captured Oxia version and exact stored-envelope SHA checks；hydrated object audits；
references-before-manifest ordering；and fail-closed missing/response-loss behavior. It deliberately provides no
source-retirement plan、root MARK/DRAIN/DELETING transition、domain proof、object-store delete or runtime composition,
so it is checkpoint-G evidence only.

`phase4M4GcPlanCheck` extends checkpoint G with exact-millisecond/cross-lifetime configuration validation、safe
deadline overflow、ACTIVE-root candidate construction、secure non-authorizing ids and canonical complete-domain plan
digests. It proves configured bounds cannot be implemented by truncation, unrelated-object protections cannot enter a
MARK digest, protection owner/value/version/envelope drift changes that digest, and a response-loss reload must match
exact root object/attempt/digest/version/epoch. Restart recovery uses the current MARKED version and does not invent a
previous or adjacent Oxia version. `GcPlan` has no
durable codec and this gate invokes no root CAS or delete primitive；it is checkpoint-H evidence only.

`phase4M4RootFenceCheck` extends checkpoint H with the canonical unique domain registry、explicit
veto/incomplete/limit blockers、shared-deadline collection/revalidation、mandatory typed metadata reload and exact
root CAS recovery. It proves premature/disabled/dry-run paths do not mutate；MARK binds domain/protection/metadata
facts；drain waits through grace and paged reader leases；protection owner/version、metadata version/envelope or domain
authority drift unmarks before any destructive side effect；lost MARK/unmark/DELETING responses and a fresh MARKED
recovery candidate converge only by exact root reload. The scanner test forces page size one across all 256 shards and
checks exact lifecycle counts、visitor-failure re-entry and borrowed-resource close semantics. A contract-surface
check rejects any source/audit/protection/object delete dependency in the collector. This is checkpoint-I evidence
only：the terminal result is durable `DELETING` intent, concrete domains/runtime composition and every destructive
side effect remain absent.

`phase4M4ReferenceDomainsCheck` extends checkpoint I with query-bound `stillMatches` re-scans and an exact invariant
that every retained domain reference is paired with the same planned metadata removal key、Oxia version and durable
value SHA. It proves both-view generation scans、DRAINING-only higher-generation eligibility、optional recovery-root
plus complete live-tail authorities、active materialization-task vetoes、bounded fail-closed accumulation and
ownerless fail-closed behavior without treating stream registrations as truth. This is checkpoint-J evidence only：
projection/cursor/future-sentinel and ownerless-global domains、source retirement、physical deletion and runtime
composition remain absent.

Final gate uses real Oxia + LocalStack across two independent runtimes. It proves old commit/index/source deletion is
impossible before root checkpoint; after deletion, append replay/index repair/read use the checkpoint/higher target.
It also proves live-reference root/protection backfill, all-shard lifecycle recovery with empty object listing, 10,000
cursor roots, snapshot listing, process-death lease expiry and multi-stream object veto. It additionally proves that
both existing sync append and replay recovery establish `REACHABLE_APPEND` before head visibility and
`VISIBLE_GENERATION` before strict success, across every response-loss cut. Ten thousand deleted roots are reduced to
bounded audit metadata only after the long grace, while a stale first/retried PUT cannot resurrect a retired key.

## 7. F4-M5 — Async Profile, Retention, and Pulsar Integration

### 7.1 Nereus production artifacts

```text
nereus-core/.../profile/Phase4StorageProfileResolver.java
nereus-core/.../append/AsyncObjectWalAppendCoordinator.java
nereus-core/.../recovery/GenerationZeroRepairScanner.java
nereus-core/.../read/ReadAfterStableCommitRepair.java
nereus-core/.../backpressure/MaterializationLagGate.java

nereus-metadata-oxia/.../ManagedLedgerGenerationProtocol.java
nereus-metadata-oxia/.../ManagedLedgerProtocolProperties.java
nereus-metadata-oxia/.../GenerationProtocolActivationRecord.java
nereus-metadata-oxia/.../ReferenceDomainVersionRecord.java
nereus-metadata-oxia/.../GenerationBackfillProofRecord.java
nereus-metadata-oxia/.../VersionedGenerationProtocolActivation.java
nereus-metadata-oxia/.../GenerationProtocolActivationStore.java
nereus-metadata-oxia/.../OxiaJavaGenerationProtocolActivationStore.java
nereus-metadata-oxia/.../codec/GenerationProtocolActivationRecordCodecV1.java
nereus-metadata-oxia/.../ManagedLedgerProjectionMetadataStore.java
nereus-metadata-oxia/.../ProjectionMetadataStoreCore.java
nereus-metadata-oxia/.../records/TopicProjectionRecord.java

nereus-managed-ledger/.../retention/RetentionPolicySnapshot.java
nereus-managed-ledger/.../retention/NereusRetentionConfig.java
nereus-managed-ledger/.../retention/RetentionStatsToken.java
nereus-managed-ledger/.../retention/RetentionCandidate.java
nereus-managed-ledger/.../retention/RetentionCandidatePlanner.java
nereus-managed-ledger/.../retention/NereusManagedLedgerRetentionService.java
nereus-managed-ledger/.../generation/ManagedLedgerGenerationProjectionRefV1.java
nereus-managed-ledger/.../generation/ManagedLedgerMaterializationRegistrationCoordinator.java
nereus-managed-ledger/.../generation/ManagedLedgerGenerationProtocolActivationGuard.java
nereus-managed-ledger/.../NereusManagedLedger.java
nereus-managed-ledger/.../NereusManagedLedgerRuntime.java

nereus-pulsar-adapter/.../NereusRuntimeConfiguration.java
nereus-pulsar-adapter/.../DefaultNereusRuntimeProvider.java
```

### 7.2 Local Pulsar fork artifacts

```text
pulsar-broker/.../nereus/NereusGenerationProtocolCapability.java
pulsar-broker/.../nereus/NereusBrokerCapabilityCoordinator.java
pulsar-broker/.../nereus/NereusGenerationRegistrationBackfill.java
pulsar-broker/.../nereus/NereusStorageBindingCapability.java
pulsar-broker/.../nereus/NereusResolvedTopicFeatures.java
pulsar-broker/.../nereus/NereusTopicFeatureResolver.java
pulsar-broker/.../nereus/NereusTopicFeatureValidator.java
pulsar-broker/.../nereus/NereusManagedLedgerStorage.java
pulsar-broker/.../nereus/NereusBrokerStorageConfiguration.java
pulsar-broker/.../service/persistent/PersistentTopic.java
pulsar-broker/.../admin/impl/PersistentTopicsBase.java
pulsar-broker-common/.../ServiceConfiguration.java
```

### 7.3 Focused tests

Nereus：

```text
Phase4StorageProfileResolverTest
AsyncObjectWalAppendCoordinatorTest
AsyncAppendPhysicalProtectionTest
AsyncReadAfterCommitRepairTest
GenerationZeroRepairScannerTest
MaterializationLagGateTest
ManagedLedgerGenerationProtocolTest
ManagedLedgerGenerationProjectionRefV1GoldenTest
ManagedLedgerMaterializationRegistrationCoordinatorTest
GenerationActivationCompatibilityTest
RetentionCandidatePlannerTest
NereusRetentionConfigTest
NereusManagedLedgerRetentionTest
NereusManagedLedgerGenerationReadTest
NereusRuntimeF4CompositionTest
NereusRuntimeF4ConfigurationCrossValidationTest
```

Pulsar fork：

```text
NereusGenerationProtocolCapabilityTest
NereusBrokerCapabilityCoordinatorGenerationTest
NereusGenerationRegistrationBackfillTest
NereusTopicFeatureResolverF4Test
NereusTopicFeatureValidatorF4Test
NereusAdminOperationF4Test
NereusManagedLedgerStorageF4Test
NereusRetentionPolicyUpdateTest
NereusBacklogEvictionTest
NereusTrimTopicRouteTest
NereusGenerationProtocolBrokerTest
```

### 7.4 Gates

```text
./gradlew phase4M5Check
./gradlew phase4M5FinalCheck --rerun-tasks
```

Ordinary gate builds/publishes the F4 development artifacts and runs the exact clean local Pulsar source lock plus
focused fork tests/spotless. Final gate runs real two-broker Pulsar with real Oxia、pinned LocalStack and stock
BookKeeper coexistence. It proves projection-ref resolution、registration-before-marker crash cuts、async ack/read
repair、cold-topic backfill、policy trim、backlog eviction、capability rollout and source retirement without claiming a
Nereus BookKeeper profile.

## 8. F4-M6 — Final Acceptance

### 8.1 Required scenarios

The final gate must cover all of the following as named deterministic or real-service test methods：

1. exact ordinary Entry bytes and MessageId across generation-zero -> higher generation switch；
2. exact compressed/batched middle-batch MessageId and properties through fallback；
3. `PREPARED` index never selected at every crash cut；
4. concurrent publication and lost allocation responses produce unique task-attached generations, harmless gaps and
   a deterministic highest winner；
5. checksum/missing higher object quarantines and falls back only inside `COMMITTED`；
6. `TOPIC_COMPACTED` record is invisible to every ordinary resolver/facade path；
7. resolve/pin/read races GC mark before/after lease write and root revalidation；
8. reader process death protects until skew-safe lease expiry；
9. source task protection races GC mark without early delete；
10. task claim expiry/stale worker/output/publish response loss converges；
11. advisory checkpoint loss/ahead-of-truth cannot skip tasks；
12. recovery checkpoint replaces a live commit prefix and append continues from its bridge；
13. replay/read-index repair works after old commit/gen-0 index/source deletion；
14. checkpoint/root CAS loss and merge leave only safe orphans；
15. multi-stream WAL object remains while any stream slice is live；
16. higher replacement quarantine during source retirement vetoes delete；
17. cursor `PROTECTION_PENDING` at inventory/mark/drain vetoes delete；
18. cursor `TRIM_PENDING` at inventory/mark/drain vetoes delete；
19. owner takeover/root version change invalidates snapshot GC；
20. live/current、old、CAS-lost and deleted-cursor snapshots classify/delete correctly at 10,000 roots；
21. logical trim completes before physical GC and no admin promise waits for object deletion；
22. retention size/time OR formula, zero/infinite and stale publish-time stats；
23. size/precise-time backlog eviction persists cursor then permits safe trim；
24. non-precise ledger time eviction remains rejected；
25. Object-WAL async success before gen-0 index, crash/restart and bounded read repair；
26. materialization lag throttle/reject happens before new primary IO while recovery continues；
27. generation capability two-snapshot barrier and monotonic topic marker；
28. old F3 decoder fails an activated topic before returning a ledger；
29. publication enabled/deletion disabled rollout stage preserves all sources；
30. loaded/unloaded/partitioned admin matrices match；
31. compaction/offload/truncate/read-compacted remain rejected；
32. unload、broker failover and full process restart preserve projection/cursor/generation behavior；
33. same-name topic recreate cannot alias old generation/task/snapshot/object；
34. future catalog sentinel blocks GC when a domain capability is unknown；
35. object listing loss/staleness changes discovery only, never deletion correctness；
36. close/executor rejection completes all callbacks once and leaves recoverable durable work；
37. stock BookKeeper topic remains functional beside Nereus F4 topics；
38. candidate/task/root/lease/protection/domain-snapshot hard limits fail closed with no truncation；
39. no generation/task/object identity leaks into Pulsar Position/MessageId；
40. live projection marker/deleted-incarnation proofs gate single- and multi-stream source/orphan deletion；
41. streaming upload stays within staging/heap bounds and cleans cancellation/crash leftovers；
42. Phase 1/1.5/2/3 final gates remain green；
43. every registry shard/page is scanned fairly and stale hint/watch loss cannot skip authoritative head/task work；
44. an unloaded stream with committed head but no task is rediscovered after all original brokers/process state die；
45. projection ref resolves the current same-incarnation record after mutable property CAS, while missing/mismatched
    registration and marker ordering fail closed at every response-loss cut；
46. deleted-stream registration remains until task/index/recovery/domain drain, then conditional retirement survives
    response loss without affecting a same-name new incarnation；
47. all 256 physical-root shards recover MARKED/DELETING work after process loss, including object-delete success with
    lost response and an empty object-store listing；
48. terminal task/index/stats/checkpoint/sequence/recovery metadata retires only after exact grace/reference proofs,
    and deleted-stream registration is the final conditional key removed；
49. cold unloaded-topic registration backfill covers existing partitions under one stable broker-readiness epoch,
    while concurrent creates self-register and any failure/old-broker join blocks activation；
50. live generation-zero/cursor references receive physical roots/protections before deletion activation, while
    listing omission cannot falsely complete the coverage bit；
51. after the backfill epoch, sync/async append prepare cannot make a head reachable before `REACHABLE_APPEND`, strict
    success cannot precede generation-zero `VISIBLE_GENERATION`, and an abandoned protected intent is reclaimed only
    by the full orphan/head/recovery-domain proof；
52. DELETED-root retirement requires two separated exact HEAD-absence observations, unchanged owner/domain proofs and
    versioned reference/manifest cleanup；a first or retried PUT at every cut is rejected or uses a fresh key, and
    externally reappearing bytes return to the ownerless-orphan proof path。

### 8.2 Scale fixtures

At minimum：

```text
10,000 durable cursor roots for one stream
4,096 generation candidates at admitted edge; 4,097 exact failure
1,000 paginated physical protections/readers
1,001 physical roots in one shard plus at least one root in every other physical shard
10,000 DELETED roots with Phase 1 manifest/reference audit records aged through bounded tombstone retirement
128 source ranges and 1,048,576 offsets in one task
1,000,000 recovery checkpoint commit entries using generated streaming fixture
32 active checkpoint refs followed by merge
16,448 registered streams arranged as 257 entries in each of 64 shards
two brokers + at least two worker runtimes contending on the same streams
```

The million-entry checkpoint test may use generated streaming bytes rather than holding all entries in heap；it must
exercise writer/reader/directory bounds. The registry fixture uses deterministic valid `StreamId` inputs selected by
the frozen shard function；it traverses at least two pages per shard and then restarts with empty continuations.

### 8.3 Gates

```text
./gradlew phase4M6Check
./gradlew phase4M6FinalCheck --rerun-tasks
./gradlew phase4Check
./gradlew phase4FinalCheck --rerun-tasks
```

`phase4FinalCheck` composes every M1-M6 final gate plus `phase3FinalCheck`. It is the only Phase 4 completion claim.

## 9. Gradle / Build Plan

Root changes during implementation：

- `settings.gradle.kts` includes `nereus-materialization` and treats `phase4*`/affected modules as requiring the local
  Pulsar source composite where broker/facade tests need it；
- `nereus-bom` constrains the new artifact；
- through M1 the Phase 4 gate deliberately reuses the fork's frozen `0.1.0-f2-dev` source-composite coordinate so the
  already-final-gated F3 broker tests remain reproducible. F4-M5 changes Nereus and the local Pulsar version catalog
  atomically to the explicit F4 development coordinate before any broker-side F4 code is compiled；neither coordinate
  is treated as a published Pulsar snapshot；
- `gradle/libs.versions.toml` pins Parquet/ZSTD dependencies；
- each integration source set has its own task and Docker requirement；ordinary unit gates stay usable without Docker；
- root M1-M6 tasks depend only on the code/tests available by that milestone；
- final gates use `--rerun-tasks` evidence and do not pass from stale test outputs。

Implemented M1 scripts：

```text
scripts/check-phase4-contract-surface.sh
scripts/check-phase4-documentation.sh
scripts/check-phase4-module-boundaries.sh
scripts/check-phase4-pulsar-source-lock.sh
scripts/check-phase4-guarded-object-put.sh
```

Later-milestone scripts：

```text
scripts/check-phase4-no-direct-trim.sh
scripts/check-phase4-view-isolation.sh
```

Audits fail on missing planned production/test artifacts once their milestone status becomes complete, stale source
locks, broken Markdown links, direct F4 `StreamStorage.trim`, Pulsar imports in protocol-neutral modules and ordinary
resolver references to the topic-compacted namespace. The guarded-PUT audit also rejects unguarded Object WAL,
compacted, checkpoint or cursor-snapshot writes once their F4 publication hook is active.

## 10. Requirement-to-Evidence Matrix

| Requirement | Primary evidence when implemented |
| --- | --- |
| publication only switches physical target | generation publication/failure tests + head unchanged assertion |
| exact Pulsar bytes/MessageIds | Parquet opaque round trip + real two-broker M6 method |
| view separation | keyspace/codec/resolver isolation tests and source audit |
| task not visibility owner | PREPARED/task/index model tests |
| restart-safe stream work discovery | cold-topic coverage gate + registry shard/page/watch-loss tests + unloaded no-task fresh-process gate |
| registry not correctness owner | stale/mismatched hint negative tests + authoritative projection/head reload |
| registry cardinality retirement | full deleted-stream drain proof + same-name recreation/conditional-delete tests |
| unique deterministic generation order | allocator concurrency + resolver property tests |
| safe fallback | quarantine/fallback tests with retained lower target |
| durable reader fence | handshake model + real process-death lease gate |
| task/reference fence | protection model + mark race gate |
| source deletion needs recovery checkpoint | GC negative gate before root + replay positive gate after deletion |
| bounded Oxia/history cardinality | checkpoint/pruning + 10,000-root tombstone-retirement scale fixtures |
| F3 pending lifecycle honored | cursor snapshot/data GC race methods |
| policy trim uses owner coordinator | no-direct-trim audit + ownership-cut tests |
| cursor snapshot inventory not delete authority | final version/domain revalidation tests |
| async ack/read repair | async crash/restart real gate |
| lag cannot discard acknowledged primary | backpressure/recovery tests |
| rollout/downgrade fail closed | capability/marker/old-decoder broker tests |
| future domains extend safely | sentinel/domain registry test |
| object list is audit only | stale/empty list model tests |
| GC lifecycle restart discovery | authoritative 256-shard root scan + empty-list DELETING recovery |
| bounded DELETED-root audit without resurrection | tombstone-retirement scale/race tests + guarded-PUT source audit |
| BookKeeper non-claim/coexistence | profile rejection + stock BK real topic |

## 11. Documentation Synchronization

Every implementation milestone updates：

- this README/milestone status and exact source locks；
- the affected numbered code-level documents from planned surface to actual surface；
- `docs/design/nereus-future4-compaction-generation.md`；
- `docs/design/nereus-commit-protocol.md` for publication/checkpoint/GC facts；
- `docs/design/nereus-storage-object-format.md` for implemented object/golden format；
- `docs/design/nereus-overall-architecture.md` module/status/open decisions；
- `docs/design/nereus-futures.md` and design index delivery status；
- `docs/design/nereus-terminology.md` for new stable terms；
- `docs/automq-like-stream-storage/README.md` when async profile state changes；
- Phase 3 F4 handoff text only when the consumed interface/behavior actually changes。

Designed examples must not be rewritten as implemented until the corresponding executable gate passes. Historical
source hashes remain labeled historical rather than silently replaced.

## 12. Mandatory Review Stops

Stop implementation and reopen the narrow design gate if any of these becomes necessary：

- change `streamId + offset` or Pulsar virtual ledger/entry/batch mapping；
- reconstruct rather than preserve `PULSAR_ENTRY_V1` bytes in `COMMITTED`；
- publish from task/checkpoint/object existence instead of index CAS；
- require multi-key Oxia atomicity；
- delete source before a recovery checkpoint replaces commit/index repair evidence；
- rely on JVM reader counts or object listing for deletion safety；
- send an F4 object PUT/retry without owner/root guard, reuse a deleted object key, or retire a DELETED root without
  the durable dual absence/authority proof；
- bypass current F3 owner/pending protocol or make GC claim cursor ownership；
- expose `TOPIC_COMPACTED` to ordinary ManagedLedger reads；
- enable a BookKeeper profile without a real writer/reader/retention adapter；
- admit Pulsar compaction/read-compacted/offload/truncate in F4；
- add a future reference domain without capability/GC registration。

Tuning worker counts、file sizes within hard bounds、scan intervals or metrics names does not require a protocol review
but still requires config/tests/docs synchronization.

## 13. Phase 4 Exit Gate

Phase 4 may be labeled `Implemented / final-gated` only after：

- M1-M6 gates and aggregate final gate are green；
- Nereus and Pulsar exact worktrees/commits are recorded and clean；
- every scenario in section 8 maps to an executable test method；
- production and test artifact audits find no missing planned surface；
- no stale “Designed” claim remains for implemented F4 behavior and no future/BookKeeper behavior is overclaimed；
- all prior phase final gates pass in the aggregate run；
- physical delete has been proved on real Oxia/S3 with reader/cursor/task/reference races, not only mocked。
