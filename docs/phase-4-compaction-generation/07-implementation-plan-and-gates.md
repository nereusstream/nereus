# Phase 4 Implementation Plan and Gates

## 1. Current Status

F4-M0 is complete against Nereus `e330969cd5c2c11cd38d0bd7f687185171ae91e2` and local Pulsar
`148d18a404aee6eb0208a8a1f7e2c0eabc89a2a1`. F4-M1、F4-M2 and F4-M3 completed their ordinary and Docker-backed
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
- 49 frozen F4 metadata envelope vectors cover every lifecycle/optional branch、retirement-journal and activation record；focused real Oxia covers slash-aware
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
domain/protection/metadata-key facts can be recomputed before destructive retirement. Checkpoint I adds canonical exact-domain
aggregation、mandatory metadata-fact reload、recoverable ACTIVE/MARKED/DELETING fencing and full 256-shard metadata
root enumeration. It stops at durable delete intent；no source/protection/audit/object delete path is enabled by
checkpoints G–I. Checkpoint J adds query-bound stateless revalidation、exact reference-to-removal binding and the
affected-stream generation、append-recovery and materialization reference domains. Checkpoint K adds the core shared
bounded snapshot builder、composed managed-ledger generation-marker/exact-stream-authority APIs, and affected-stream
projection-generation plus cursor-snapshot domains. Future-sentinel domains、ownerless global absence proof、
retirement planning and runtime composition remain pending. Checkpoint L's protocol foundation adds
the reference-set-v2 compact domain proof and validated retirement manifest/protection/removal records/codecs. Its
durable-store slice now adds fixed-depth object/attempt keyspace、production/fake create/scan parity and a
manifest-last seal/load service. Mandatory collector injection now seals/authenticates it before MARK and reloads it
at drain admission and the final delete-intent fence. Checkpoint M adds the root-authenticated DELETING recovery、
typed dispatch seam、journaled protection removal、exact object delete and DELETED convergence. Checkpoint N adds
strict generation-index restart routing、exact generation-zero conditional deletion、higher-generation
`DRAINING -> RETIRED` recovery and root+journal reauthentication at every destructive batch/final object fence.
Checkpoint O adds exact-key legacy/generic marker/commit inverse routing、response-loss-safe typed handlers and an
NRC1/source/index/marker-bound generation-zero plan builder plus exact-key revalidator. Checkpoint P adds strict
canonical publication resolution、current exact COMMITTED index binding、another-object ACTIVE physical-root proof and
final exact index/root reload. Checkpoint Q adds a shared replacement verifier、COMMITTED-view whole-source NRC1 tiling
and response-loss-safe higher-generation `COMMITTED/QUARANTINED -> DRAINING`, with DRAINING plan-time reproof.
Checkpoint R adds exact completed-trim eligibility for generation-zero and either higher view、current strictly newer
TOPIC_COMPACTED/ACTIVE same-view replacement proof, and a pre-read source-retirement grace fence；all three higher
eligibility branches are repeated at DRAINING plan time. Checkpoint S adds the durable cluster generation-activation record/codec/exact-key store、
read-only lookup、PREPARED bootstrap、monotonic CAS guard and frozen lifecycle/capability vectors. It is authority
foundation only. Checkpoint T adds the future sentinel、activation/backfill/domain-set-gated 64-shard global scope and
ownerless variants for all five concrete domains. Checkpoint U adds the persisted dual-absence DELETED-root pass、
fresh ownerless-domain/handle revalidation、late exact-byte cleanup、Phase 1 references-before-manifest retirement and
root-last response-loss-safe CAS. Checkpoint V adds guarded cursor-snapshot upload、current-root pending protection、
cursor-CAS-then-permanent completion、response-loss repair on hydrate/read、durable reader lease and the shared
physical/protection/read-pin production wiring for this path. Checkpoint W adds strict NPR1 projection resolution、
the all-64-shard physical-root/cursor-root live-reference backfill、exact commit/index/cursor owner protections、
stable final revalidation and response-loss-safe dual activation proofs. Broker registration backfill proof/barrier、
cursor snapshot candidate/deletion scanning、object inventory、registration retirement and the remaining
materialization/GC runtime composition remain pending. Checkpoint X starts M5 by adding the exact durable
registration create/refresh/final-revalidation coordinator、topic-open return barrier and shared generation-store
production ownership. Checkpoint Y adds the locked Pulsar fork's reserved generation lookup property、exact
binding/cursor/generation two-stable-snapshot barrier、deterministic broker-incarnation readiness epoch/full digest
and registry-notification invalidation. Checkpoint Z adds the exact unloaded-projection candidate、canonical
one-namespace-at-a-time cold-topic traversal、bounded concurrency/deadline、deterministic full report and final
binding/readiness revalidation. Checkpoint AA adds the product-neutral exact-readiness provider/completion value、
shared activation-store runtime ownership、zero-failure admission、response-loss-safe durable
`streamRegistrationBackfill` CAS、same-epoch coverage immutability and newer-epoch dependent-proof invalidation.
Checkpoint AB adds the product-owned generation activation guard、frozen six-domain digest、strict
projection/L0/registration capture、response-loss-safe monotonic topic marker、proof revalidation、exact
projection-domain delete admission and the disabled-by-default Pulsar first-activation switch.
Checkpoint AC adds the product-owned publication coordinator、proof-gated
`PREPARED -> ACTIVE(publication=true, deletion=false)` CAS、bounded conflict/lost-response recovery、final readiness
revalidation and broker sequencing after a zero-failure durable backfill proof. The task/index/checkpoint/trim/delete
mutation call sites remain pending. Checkpoint AD adds the opt-in Phase 4 Object-WAL profile matrix、protected-head
`WAL_DURABLE` acknowledgement boundary、independently bounded generation-zero repair、root-stable restart scanner and
protected read-after-commit live repair. Checkpoint AE adds exact F2 sync/async profile round-trip、per-stream-lane
pre-IO activation/marker proof and final revalidation、authoritative records/bytes/age lag measurement and
throttle/reject semantics. Checkpoint AF atomically installs the Phase 4 resolver、pre-IO guard、
generation-aware read/failure handler、NRC1 replay/index repair、generation-zero repair-before-plan and owned
materialization service in `DefaultNereusRuntimeProvider`；the local Pulsar fork maps exact sync/async first-create
profile and the complete bounded materialization configuration. Legacy constructors remain Phase 1.5-only, sync is
still the default, and async remains activation-proof gated.

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
physical/GcReferenceDomainConfig.java             implemented checkpoint K
physical/GcReferenceSnapshotBuilder.java          implemented checkpoint K
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

> Current status：in progress. Checkpoints A–R implement NRC1/protected append/recovery、checkpoint replay/index repair、
> retirement metadata、bounded GC plan/root/journal fencing、root-authenticated destructive recovery、typed source
> retirement and all view/trim eligibility paths. Checkpoint S implements durable generation activation metadata；
> checkpoint T implements the future sentinel、activation-gated full registration scope and affected/ownerless variants
> of all five reference domains. Checkpoint U implements persisted dual-absence DELETED-root/Phase 1 audit retirement.
> Checkpoint V implements the guarded/protected/pinned cursor-snapshot new-write frontier and its production
> physical/protection/read-pin wiring. Checkpoint W implements strict NPR1 projection authority and the stable
> physical-root/cursor-root live-reference backfill plus dual activation proofs. Broker registration proof/barrier、
> cursor snapshot candidate/deletion scanning、object inventory、registration retirement、remaining materialization/GC runtime
> composition、real-service destructive scenarios and the final M4 gate remain before F4-M4 can be called complete.
> Checkpoint X separately starts M5's rollout frontier：new/create/open/recreate topics cannot return before exact
> durable registration. Checkpoint Y publishes/verifies the generation broker capability and stable readiness
> identity. Checkpoint Z implements the cold-topic traversal/report, but no durable coverage proof、marker or product
> activation permit is produced yet.

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
append/GenerationZeroProtectionIdentities.java            public backfill overloads checkpoint W
append/ProtectedStableAppend.java
append/ProtectedGenerationZero.java
append/AppendCoordinator.java                     protected sync path
DefaultStreamStorage.java                        required publisher injection
PreparedStableAppend.java
MaterializedGenerationZero.java
physical/GcReferenceDomainConfig.java                 implemented checkpoint K shared bounds
physical/GcReferenceSnapshotBuilder.java              implemented checkpoint K canonical fail-closed builder
physical/GcGlobalReferenceScope.java                  implemented checkpoint T protocol-neutral ownerless scope
physical/GcGlobalReferenceScopeSnapshot.java          implemented checkpoint T canonical activation/registration facts
capability/GenerationProjectionAuthorityReader.java   implemented checkpoint W protocol-neutral F2 authority seam
capability/GenerationProjectionAuthoritySnapshot.java implemented checkpoint W exact live/non-live authority capture
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
retirement/SourceRetirementMetadataStore.java            implemented G, exact-key restart API extended O
retirement/OxiaJavaSourceRetirementMetadataStore.java    implemented G, strict inverse extended O
retirement/GenerationZeroMarkerIdentity.java             implemented checkpoint G
retirement/LegacyCommittedSliceIdentity.java             implemented checkpoint G
retirement/GenericCommittedAppendIdentity.java           implemented checkpoint G
retirement/VersionedGenerationZeroMarker.java             implemented G, target identity extended O
retirement/VersionedGenerationZeroCommit.java             implemented checkpoint O
retirement/ObjectAuditRetirementStore.java               implemented checkpoint G
retirement/OxiaJavaObjectAuditRetirementStore.java       implemented checkpoint G
retirement/VersionedObjectManifestAudit.java             implemented checkpoint G
retirement/VersionedObjectReferencesAudit.java           implemented checkpoint G
records/GcDomainSnapshotProofRecord.java                 implemented checkpoint L foundation
records/GcRetirementManifestRecord.java                  implemented checkpoint L foundation
records/GcRetirementProtectionRecord.java                implemented checkpoint L foundation
records/GcRetirementRemovalRecord.java                   implemented checkpoint L foundation
codec/GcRetirementManifestRecordCodecV1.java             implemented checkpoint L foundation
codec/GcRetirementProtectionRecordCodecV1.java           implemented checkpoint L foundation
codec/GcRetirementRemovalRecordCodecV1.java              implemented checkpoint L foundation
VersionedGcRetirementManifest.java                       implemented checkpoint L durable-store slice
VersionedGcRetirementProtection.java                     implemented checkpoint L durable-store slice
VersionedGcRetirementRemoval.java                        implemented checkpoint L durable-store slice
GcRetirementProtectionScanPage.java                      implemented checkpoint L durable-store slice
GcRetirementRemovalScanPage.java                         implemented checkpoint L durable-store slice
F4Keyspace.gcRetirement*                                 implemented checkpoint L durable-store slice
F4Keyspace.parseGenerationIndexKey                       implemented checkpoint N strict restart router
GenerationCandidateKeyIdentity.java                     implemented checkpoint N strict restart router
PhysicalObjectMetadataStore retirement-journal API       implemented checkpoint L durable-store slice
OxiaJavaPhysicalObjectMetadataStore journal methods      implemented checkpoint L durable-store slice
ManagedLedgerProtocolProperties.java                     implemented checkpoint K composed marker validator
ManagedLedgerGenerationProtocol.java                     implemented checkpoint K monotonic marker foundation
ManagedLedgerStreamProjection.java                       implemented checkpoint K exact stream authority view
VersionedTopicProjection.java                            implemented checkpoint K key/version/envelope wrapper
VersionedVirtualLedgerProjection.java                    implemented checkpoint K key/version/envelope wrapper
CursorMetadataDigests.java                               implemented checkpoint K exact F3 envelope digests
ManagedLedgerProjectionMetadataStore.getProjectionByStream implemented checkpoint K
ManagedLedgerProjectionMetadataStore.activateGenerationProtocol implemented checkpoint K CAS foundation
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
gc/GcDomainSnapshotProof.java                            implemented checkpoint L foundation
gc/GcRetirementJournal.java                              implemented checkpoint L durable-store slice
gc/GcRetirementJournalSnapshot.java                      implemented checkpoint L durable-store slice
gc/DefaultGcRetirementJournal.java                       implemented checkpoint L durable-store slice
gc/GcMetadataRetirementContext.java                      implemented checkpoint M dispatch foundation
gc/GcMetadataRetirementHandler.java                      implemented checkpoint M dispatch foundation
gc/GcMetadataRetirementOutcome.java                      implemented checkpoint M dispatch foundation
gc/GcMetadataRetirementRegistry.java                     implemented checkpoint M dispatch foundation
gc/GenerationZeroIndexRetirementHandler.java             implemented checkpoint N
gc/HigherGenerationIndexRetirementHandler.java           implemented checkpoint N
gc/GenerationZeroMarkerRetirementHandler.java            implemented checkpoint O
gc/GenerationZeroCommitRetirementHandler.java            implemented checkpoint O
gc/RecoveryReplacementVerifier.java                      implemented checkpoint Q shared NRC1/current-root proof
gc/CompletedTrimRetirementVerifier.java                  implemented checkpoint R exact L0/source/root proof
gc/HigherGenerationRecoveryCoverageVerifier.java         implemented checkpoint Q whole-range proof
gc/TopicCompactedReplacementVerifier.java                implemented checkpoint R same-view proof
gc/HigherGenerationRetirementEligibilityVerifier.java    implemented checkpoint R proof selector
gc/HigherGenerationPreDrainCoordinator.java              extended checkpoint R grace/view/trim fence
gc/HigherGenerationPreDrainResult.java                   extended checkpoint R
gc/HigherGenerationPreDrainStatus.java                   extended checkpoint R
gc/SourceRetirementPlanBuilder.java                      extended checkpoint R trim/view DRAINING reproof
gc/GenerationRetirementOperations.java                   implemented N, source seams extended O
gc/GcIdGenerator.java                                    implemented checkpoint H
gc/SecureGcIdGenerator.java                              implemented checkpoint H
gc/GcReferenceDomainVersion.java                         implemented checkpoint I
gc/GcReferenceCollectionStatus.java                      implemented checkpoint I
gc/GcReferenceCollection.java                            implemented checkpoint I
gc/GcReferenceDomainRegistry.java                        implemented checkpoint I
gc/GcPlanMetadataRevalidator.java                        implemented checkpoint I
gc/GenerationReferenceDomain.java                        implemented J affected, extended T ownerless
gc/AppendRecoveryReferenceDomain.java                    implemented J affected, extended T ownerless
gc/MaterializationReferenceDomain.java                   implemented J affected, extended T ownerless
gc/GenerationProtocolDomainSets.java                     implemented checkpoint T exact installed set
gc/RegisteredStreamGcGlobalReferenceScope.java           implemented checkpoint T 64-shard authority
gc/FutureCatalogSentinelDomain.java                      implemented checkpoint T
gc/SourceRetirementCoordinator.java                      implemented checkpoint M recovery foundation
gc/PhysicalGcDeletionResult.java                         implemented checkpoint M recovery foundation
gc/PhysicalGcDeletionStatus.java                         implemented checkpoint M recovery foundation
gc/StreamRegistrationRetirementCoordinator.java
gc/PhysicalGcMarkStatus.java                              implemented checkpoint I
gc/PhysicalGcMarkResult.java                              implemented checkpoint I
gc/PhysicalGcAdvanceStatus.java                           implemented checkpoint I
gc/PhysicalGcAdvanceResult.java                           implemented checkpoint I
gc/PhysicalObjectGarbageCollector.java                    implemented checkpoint I fence, extended L journal auth
gc/PhysicalObjectRootVisitor.java                         implemented checkpoint I
gc/PhysicalObjectRootScanResult.java                      implemented checkpoint I
gc/PhysicalObjectRootScanner.java                         implemented checkpoint I
gc/PhysicalRootTombstoneRetirementCoordinator.java         implemented checkpoint U boundary
gc/DefaultPhysicalRootTombstoneRetirementCoordinator.java  implemented checkpoint U
gc/TombstoneRetirementResult.java                          implemented checkpoint U
gc/TombstoneRetirementStatus.java                          implemented checkpoint U
gc/TombstoneRetirementDigests.java                         implemented checkpoint U
gc/PhysicalRootBackfillCoordinator.java                  implemented checkpoint W contract
gc/DefaultPhysicalRootBackfillCoordinator.java           implemented checkpoint W all-shard backfill/proof CAS
gc/PhysicalRootBackfillDigest.java                        implemented checkpoint W hierarchical coverage digest
gc/PhysicalRootBackfillRequest.java                       implemented checkpoint W bounded request
gc/PhysicalRootBackfillReport.java                        implemented checkpoint W full counters/bounded failures
gc/PhysicalRootBackfillFailure.java                       implemented checkpoint W redacted failure
gc/PhysicalRootBackfillStage.java                         implemented checkpoint W stable machine stage
gc/ObjectInventoryScanner.java
gc/PhysicalGcConfig.java                                 implemented checkpoint H
gc/GcMetricsObserver.java
```

`nereus-managed-ledger`（直接依赖 core/metadata，不依赖 materialization）：

```text
retention/CursorSnapshotReferenceDomain.java             implemented K affected, extended T ownerless
retention/CursorSnapshotGcScanner.java
retention/ProjectionGenerationReferenceDomain.java       implemented K affected, extended T ownerless
cursor/CursorSnapshotWriteAuthority.java                  implemented checkpoint V
cursor/CursorSnapshotPublication.java                     implemented checkpoint V
cursor/CursorSnapshotStore.java                           extended checkpoint V two-stage contract
cursor/DefaultCursorSnapshotStore.java                    implemented checkpoint V guarded/pending/permanent/pinned IO
cursor/CursorSnapshotInventory.java             unchanged authority semantics
generation/ManagedLedgerGenerationProjectionRefV1.java   implemented checkpoint W strict NPR1 + golden identity
generation/ManagedLedgerGenerationProjectionAuthorityReader.java implemented checkpoint W binding/topic capture
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
GcReferenceSnapshotBuilderTest                             implemented K, extended T explicit incomplete/global scope
RegisteredStreamGcGlobalReferenceScopeTest                 implemented checkpoint T
GenerationReferenceDomainTest                             implemented J, extended T ownerless
AppendRecoveryReferenceDomainTest                         implemented J, extended T ownerless
MaterializationReferenceDomainTest                        implemented J, extended T ownerless
ManagedLedgerGenerationProtocolTest                       implemented checkpoint K marker/CAS authority
PhysicalObjectGarbageCollectorTest                         checkpoint I fence, extended L PREPARE/final-load tests
PhysicalObjectGarbageCollectorModelTest
PhysicalObjectGarbageCollectorFailureInjectionTest
PhysicalGcConfigTest                                      implemented checkpoint H
GcPlanTest                                                implemented checkpoint H
F4MetadataCodecGoldenTest                                extended checkpoint S activation foundation (49 vectors)
F4RecordValidationTest                                   extended checkpoint S activation contradictions
GenerationProtocolActivationStoreContractTest            implemented checkpoint S exact-key/CAS contract
GcRetirementJournalMetadataStoreContractTest             implemented checkpoint L durable-store parity
DefaultGcRetirementJournalTest                           implemented checkpoint L seal/reload/crash cuts
SourceRetirementCoordinatorTest                          implemented checkpoint M initial recovery cuts
GenerationIndexRetirementHandlerTest                     implemented checkpoint N response-loss cuts
GenerationZeroSourceRetirementHandlerTest                implemented checkpoint O response-loss cuts
SourceRetirementPlanBuilderTest                          extended checkpoint R completed-trim proof/reproof
HigherGenerationPreDrainCoordinatorTest                  extended checkpoint R view/trim/grace/race cuts
PhysicalObjectRootScannerTest                              implemented checkpoint I
PhysicalRootTombstoneRetirementTest                        implemented checkpoint U
LatePutAfterTombstoneTest                                  implemented checkpoint U
CursorSnapshotStoreTest                                   extended checkpoint V protection/read-pin/response-loss
CursorStorageOxiaS3IntegrationTest                        migrated checkpoint V real Oxia/S3 protocol
DefaultCursorSnapshotStoreLocalStackIntegrationTest       migrated checkpoint V real S3 restart/collision protocol
NereusManagedLedgerRuntimeTest                            extended checkpoint V F4 resource ownership
ManagedLedgerGenerationProjectionRefV1Test                implemented checkpoint W NPR1/authority drift
PhysicalRootBackfillCoordinatorTest                       implemented checkpoint W data/cursor/response-loss paths
MultiStreamWalRetirementTest
GenerationRetirementFallbackTest
CursorSnapshotGcScannerTest
CursorSnapshotGcRaceTest
CursorSnapshotReferenceDomainTest                         implemented K, extended T ownerless
ProjectionGenerationReferenceDomainTest                   implemented K, extended T ownerless
ObjectInventoryScannerTest
StreamRegistrationRetirementCoordinatorTest
FutureCatalogSentinelTest                                 implemented checkpoint T
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
./gradlew phase4M4ManagedLedgerDomainsCheck
./gradlew phase4M4RetirementJournalCheck
./gradlew phase4M4DestructiveRecoveryCheck
./gradlew phase4M4GenerationRetirementCheck
./gradlew phase4M4ActivationMetadataCheck
./gradlew phase4M4GlobalDomainsCheck
./gradlew phase4M4TombstoneRetirementCheck
./gradlew phase4M4CursorProtectionCheck
./gradlew phase4M4PhysicalRootBackfillCheck
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
ownerless fail-closed behavior without treating stream registrations as truth. This is checkpoint-J evidence only；
at that boundary projection/cursor/future-sentinel and ownerless-global domains、source retirement、physical deletion
and runtime composition remained absent.

`phase4M4ManagedLedgerDomainsCheck` extends checkpoint J with the core shared bounded/canonical builder、composed F2
cursor/generation marker rules、monotonic generation-marker CAS and exact per-stream binding/current-topic authority
lookup. It proves stored-envelope/Oxia-version binding、same-incarnation marker/DELETED/DELETING decisions、strictly
newer incarnation unaddressability、topic-published-before-binding-repair fail-closed behavior、complete paged F3
retention/cursor scans、live snapshot-root veto、pending-retention and projection drift、configured overflow and
ownerless fail-closed behavior. The module-boundary gate rejects a managed-ledger -> materialization dependency. This
is checkpoint-K evidence only：the M5 activation record/registration barrier, future sentinel/ownerless-global proof、
source retirement、physical deletion and runtime composition remain absent.

`phase4M4RetirementJournalCheck` extends checkpoint K with the protocol foundation and durable journal path. Module
tests prove codec/factory round
trips、record contradictions、full-snapshot/compact-proof digest equality、fixed-depth keyspace and production/fake
store parity、page-size-one restart reload、manifest-last sealing, create-response-loss convergence and no manifest
when an entry is missing. Collector tests prove exact PREPARE completes before activation/root CAS, every PREPARE
failure leaves the root ACTIVE, both drain admission and final intent fence reload the same root-authenticated
snapshot, and a missing final snapshot leaves the root MARKED. This is checkpoint-L ordinary evidence only：it stops
at DELETING intent and does not enable source/protection/metadata/object deletion or replace the M4 final gate.

`phase4M4DestructiveRecoveryCheck` extends checkpoint L with checkpoint M's first root-authenticated recovery slice.
It verifies that a matching sealed journal is mandatory before destructive work, generic removal types dispatch only
through an explicitly registered type-owned handler, an exact immutable object is HEADed/deleted under the shared
deadline, and the exact attempt alone can CAS `DELETING -> DELETED`. Focused tests cover ordinary deletion、an already
absent object under matching authority、same-record `DELETED` restart, and missing-journal failure before object access.
At checkpoint M the gate did not claim production source-metadata handlers, protection/metadata response-loss
coverage、runtime composition、future-sentinel/ownerless-global domains、cursor completion or final M4 deletion
enablement.

`phase4M4GenerationRetirementCheck` extends checkpoint M through checkpoint R. API/metadata tests freeze canonical
inverse key decoding、encoded stream ids、both view namespaces、fixed depth/current-cluster round trips, plus exact-key
legacy/generic marker/commit value-to-key reconstruction. Handler tests prove exact generation-zero index/marker/
commit deletion、delete-response-loss absence classification、DRAINING-only higher retirement、attempt+reference-set-
bound deterministic RETIRED recovery、CAS-response-loss convergence and drift rejection. Source-plan tests require an
unchanged recovery root, one covering NRC1 entry, identical canonical commit SHA/source/index/marker facts and exact
candidate-bound revalidator output；root drift、a different NRC1 canonical commit and an existing but unbound extra
source removal fail closed. Plan tests require reference/removal
type equality. Coordinator tests force `maxConcurrentDeletes=1`, remove the journal before batch two or the physical
fence, and prove no later handler/object call occurs. Checkpoint P extends the same ordinary gate with strict one-row
NRC1 publication resolution、canonical raw/durable digest separation、current exact COMMITTED index binding、another-
object ACTIVE root/slice proof and final exact index/root reload. Tests reject a QUARANTINED index、MARKED root and
either wrapper changing during freeze. Checkpoint Q further requires a whole-source NRC1 offset/commit/cumulative/
count/schema tiling, a strictly newer current COMMITTED/ACTIVE replacement for every entry, and exact replacement/
root/source reload before candidate-root-fenced `COMMITTED/QUARANTINED -> DRAINING`. Tests cover both source
lifecycles、already-DRAINING reproof、dry-run zero reads、incomplete tiling、replacement-root drift、candidate-root final
fence and CAS response loss；the source planner repeats the proof before freezing a DRAINING removal.
Checkpoint R adds the exact completed-trim alternative for generation-zero and either higher view, including
source/full-stream-snapshot/recovery-root rereads. For untrimmed TOPIC_COMPACTED sources it requires a strictly newer
current same-view index that covers offset/commit/cumulative bounds with matching payload/projection, plus another-
object `ACTIVE/TOPIC_COMPACTED` root and final index/root/source reload. Tests cover successful same-view pre-drain and
DRAINING plan reproof、MARKED replacement veto、trim success/drift and a grace result before any metadata/root read.
At checkpoint R, future/global domains、runtime composition、cursor/root/audit retirement and the final M4 gate
remained pending；checkpoints S/T close the activation/global-domain portion and checkpoint U closes DELETED-root audit
retirement only.

`phase4M4ActivationMetadataCheck` extends the ordinary checkpoint chain with checkpoint S's single-key cluster
authority foundation. It audits the exact key/partition、record fields and contradictions、explicit codec registration、
monotonic transition guard、production Oxia create/CAS/reload behavior and three frozen golden vectors. The focused
contract test proves two runtime adapters converge on one PREPARED value, then advance publication and deletion facts
without accepting stale versions or domain/capability regression. This gate does not execute any backfill、install the
future sentinel、prove ownerless global absence or enable production capability bits.

`phase4M4GlobalDomainsCheck` extends checkpoint S with checkpoint T. It audits the core global-scope contract、
read-only activation lookup、exact deletion-ready/domain-set admission、all 64 registration shards、pagination
progress、post-scan activation reload、authority overflow and the future sentinel. Focused tests prove activation
absence never creates authority、unknown installed domains and publication-only rollout veto、page-size-one global
enumeration is canonical, activation drift invalidates revalidation, and generation/append/task/projection/cursor
domains all repeat their exact per-stream scans for ownerless queries. This gate does not execute the backfill/broker
barrier or install production runtime deletion.

`phase4M4TombstoneRetirementCheck` extends checkpoint T with checkpoint U. It audits the exact DELETED input、
disabled/dry-run no-mutation paths、strict audit/orphan-plus-skew boundaries、persisted first-absence proof、reader and
protection handle veto、owner/reference versus generic domain veto classification、authority-digest window restart、
exact late-object delete/retry handling、mismatched-byte quarantine、Phase 1 references-before-manifest conditional
retirement and root-last version+durable-SHA delete. Focused tests force audit/root/object delete response loss and an
owner appearing before the late-PUT provider delete. This gate still does not run physical-root backfill、cursor
snapshot GC、object inventory、registration retirement、production runtime composition or the real-service M4 final
scenarios. The ordinary gate passed on 2026-07-16.

`phase4M4CursorProtectionCheck` extends checkpoint U with checkpoint V. It audits the exact captured ACTIVE cursor
authority、guarded replayable PUT before every provider attempt、ACTIVE physical-root plus bounded pending proof、
`prepareWrite -> cursor CAS -> completeWrite` visibility ordering、permanent-before-pending-release and
lease-before-range-IO. Focused tests prove owner/root drift blocks upload、cursor-CAS response loss is repaired from
the live snapshot reference、read/decode failures still release the durable lease、collision never overwrites bytes
and borrowed stores/managers are not closed by the snapshot store. It also compiles the migrated real-S3 and real
Oxia/S3 source sets and verifies production provider/runtime ownership. This gate does not implement legacy
physical-root/cursor inventory backfill、`CursorSnapshotGcScanner` candidate/deletion logic、broker activation barrier、
object inventory、registration retirement or production deletion. The ordinary gate plus
`cursorS3IntegrationTest` and `cursorM2IntegrationTest --rerun-tasks` passed on 2026-07-16.

`phase4M4PhysicalRootBackfillCheck` extends checkpoint V with checkpoint W. It audits strict NPR1 projection
identity、the protocol-neutral live/non-live projection authority capture、same-epoch completed registration-proof
admission、all 64 registry shards、bounded deterministic stream concurrency、recovery-root-anchored commit traversal、
complete generation-zero index traversal、F3 retention/cursor inventory、HEAD-before-root and exact owner-bound
`REACHABLE_APPEND`/`VISIBLE_GENERATION`/`CURSOR_SNAPSHOT_ROOT` handshakes. Final admission reloads registration、L0
snapshot、projection、recovery root/head and cursor inventory before publishing the two coverage proofs. Focused tests
cover an empty stable registry、activation-CAS response loss、one Object-WAL shared by a reachable commit and visible
index、a canonical NCS1 cursor snapshot with ETag, and fail-closed registration preconditions. Listing is forbidden as
coverage evidence. This ordinary checkpoint does not implement the broker-side cold-topic registration backfill or
capability barrier, enable delete bits, schedule `CursorSnapshotGcScanner`, enumerate orphan object inventory, retire
registrations, or run the real-service destructive final scenarios. The ordinary gate passed with
`--rerun-tasks` on 2026-07-16.

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
nereus-core/.../profile/Phase4StorageProfileResolver.java             implemented checkpoint AD
nereus-core/.../append/AsyncObjectWalAppendCoordinator.java          implemented checkpoint AD
nereus-core/.../append/AppendAdmissionGuard.java                      implemented checkpoint AE
nereus-core/.../append/AppendAdmissionRequest.java                    implemented checkpoint AE
nereus-core/.../recovery/GenerationZeroRepairScanner.java            implemented checkpoint AD
nereus-core/.../read/ReadAfterStableCommitRepair.java                implemented checkpoint AD
nereus-core/.../backpressure/MaterializationLagSnapshot.java          implemented checkpoint AE
nereus-core/.../backpressure/MaterializationLagSnapshotReader.java    implemented checkpoint AE
nereus-core/.../backpressure/MaterializationLagThresholds.java        implemented checkpoint AE
nereus-core/.../backpressure/MaterializationLagGate.java              implemented checkpoint AE
nereus-core/.../read/Phase4ReadComponents.java                        implemented checkpoint AF atomic read seam
nereus-core/.../DefaultStreamStorage.java                              extended checkpoint AF coupled append/read composition
nereus-materialization/.../DefaultMaterializationLagSnapshotReader.java implemented checkpoint AE
nereus-materialization/.../MaterializationSourceRepairer.java         implemented checkpoint AF
nereus-materialization/.../RegisteredMaterializationStreamScanner.java extended checkpoint AF repair-before-plan

nereus-metadata-oxia/.../ManagedLedgerGenerationProtocol.java       protocol/CAS foundation implemented K
nereus-metadata-oxia/.../ManagedLedgerProtocolProperties.java       composed property validator implemented K
nereus-metadata-oxia/.../GenerationProtocolActivationLifecycle.java       implemented checkpoint S
nereus-metadata-oxia/.../GenerationProtocolActivationRecord.java          implemented checkpoint S
nereus-metadata-oxia/.../ReferenceDomainVersionRecord.java                 implemented checkpoint S
nereus-metadata-oxia/.../GenerationBackfillProofRecord.java                implemented checkpoint S
nereus-metadata-oxia/.../VersionedGenerationProtocolActivation.java        implemented checkpoint S
nereus-metadata-oxia/.../GenerationProtocolActivationStore.java            implemented checkpoint S foundation
nereus-metadata-oxia/.../GenerationProtocolActivationTransitions.java      implemented checkpoint S
nereus-metadata-oxia/.../OxiaJavaGenerationProtocolActivationStore.java    implemented checkpoint S production
nereus-metadata-oxia/.../codec/GenerationProtocolActivationRecordCodecV1.java implemented checkpoint S
nereus-metadata-oxia/.../ManagedLedgerProjectionMetadataStore.java   exact lookup/marker CAS implemented K
nereus-metadata-oxia/.../ProjectionMetadataStoreCore.java            exact lookup/marker CAS implemented K
nereus-metadata-oxia/.../records/TopicProjectionRecord.java          composed marker validation implemented K

nereus-managed-ledger/.../retention/RetentionPolicySnapshot.java
nereus-managed-ledger/.../retention/NereusRetentionConfig.java
nereus-managed-ledger/.../retention/RetentionStatsToken.java
nereus-managed-ledger/.../retention/RetentionCandidate.java
nereus-managed-ledger/.../retention/RetentionCandidatePlanner.java
nereus-managed-ledger/.../retention/NereusManagedLedgerRetentionService.java
nereus-managed-ledger/.../generation/ManagedLedgerGenerationProjectionRefV1.java checkpoint W strict codec
nereus-managed-ledger/.../generation/ManagedLedgerGenerationProjectionAuthorityReader.java checkpoint W reader
nereus-managed-ledger/.../generation/ManagedLedgerMaterializationRegistrationCoordinator.java checkpoint X broker-safe contract
nereus-managed-ledger/.../generation/DefaultManagedLedgerMaterializationRegistrationCoordinator.java checkpoint X exact registration
nereus-managed-ledger/.../generation/ManagedLedgerMaterializationRegistrationCandidate.java checkpoint Z exact unloaded projection fact
nereus-managed-ledger/.../generation/ManagedLedgerGenerationRegistrationBackfillProofCoordinator.java checkpoint AA product boundary
nereus-managed-ledger/.../generation/DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator.java checkpoint AA proof CAS
nereus-managed-ledger/.../generation/ManagedLedgerGenerationProtocolActivationGuard.java checkpoint AB exact admission/proof
nereus-managed-ledger/.../generation/ManagedLedgerAsyncAppendAdmissionGuard.java checkpoint AE pre-IO proof/lag admission
nereus-managed-ledger/.../generation/ManagedLedgerGenerationProtocolActivationCoordinator.java checkpoint AC product boundary
nereus-managed-ledger/.../generation/DefaultManagedLedgerGenerationProtocolActivationCoordinator.java checkpoint AC ACTIVE CAS
nereus-managed-ledger/.../NereusManagedLedger.java
nereus-managed-ledger/.../NereusManagedLedgerOpenCoordinator.java     checkpoint X return-before-registration, extended Z inspect/ensure
nereus-managed-ledger/.../NereusManagedLedgerRuntime.java             checkpoints X/AA–AC plus AF materialization lifecycle ownership

nereus-pulsar-adapter/.../NereusRuntimeConfiguration.java              extended checkpoint AF MaterializationConfig/cross-validation
nereus-pulsar-adapter/.../NereusRuntimeContext.java                   checkpoints AA/AB readiness provider and first-activation switch
nereus-pulsar-adapter/.../NereusGenerationProtocolReferenceDomains.java checkpoint AA exact six-domain set
nereus-pulsar-adapter/.../Phase4ObjectWalRuntime.java                  implemented checkpoint AF owned production composition
nereus-pulsar-adapter/.../DefaultNereusRuntimeProvider.java           checkpoints X/AA–AC plus AF resolver/read/repair/service wiring
```

### 7.2 Local Pulsar fork artifacts

```text
pulsar-broker/.../nereus/NereusGenerationProtocolCapability.java       implemented checkpoint Y
pulsar-broker/.../nereus/NereusGenerationCapabilityReadiness.java     implemented checkpoint Y, extended AA core handoff
pulsar-broker/.../nereus/NereusBrokerCapabilityCoordinator.java       extended checkpoints Y/AA
pulsar-broker/.../nereus/NereusCursorProtocolCapability.java          extended checkpoint Y reserved chain
pulsar-broker/.../nereus/NereusGenerationRegistrationBackfill.java    implemented checkpoint Z contract
pulsar-broker/.../nereus/DefaultNereusGenerationRegistrationBackfill.java implemented checkpoint Z traversal/report, AA proof handoff
pulsar-broker/.../nereus/GenerationRegistrationBackfillRequest.java   implemented checkpoint Z bounded request
pulsar-broker/.../nereus/GenerationRegistrationBackfillReport.java    implemented checkpoint Z counters/digest/failures
pulsar-broker/.../nereus/GenerationRegistrationBackfillStage.java     implemented checkpoint Z stable stages
pulsar-broker/.../nereus/BackfillFailure.java                          implemented checkpoint Z redacted failure
pulsar-broker/.../nereus/NereusStorageBindingCapability.java
pulsar-broker/.../nereus/NereusResolvedTopicFeatures.java
pulsar-broker/.../nereus/NereusTopicFeatureResolver.java
pulsar-broker/.../nereus/NereusTopicFeatureValidator.java
pulsar-broker/.../nereus/NereusManagedLedgerStorage.java               extended checkpoints AA–AC proof, switch and proof-to-ACTIVE sequencing
pulsar-broker/.../nereus/NereusBrokerStorageConfiguration.java         extended checkpoint AF exact profile/materialization mapping
pulsar-broker/.../service/persistent/PersistentTopic.java
pulsar-broker/.../admin/impl/PersistentTopicsBase.java
pulsar-broker-common/.../ServiceConfiguration.java                      extended checkpoint AF profile/materialization defaults
conf/broker.conf                                                        extended checkpoint AF operator reference
```

### 7.3 Focused tests

Nereus：

```text
Phase4StorageProfileResolverTest                                    implemented checkpoint AD
AsyncObjectWalAppendCoordinatorTest                                 implemented checkpoint AD
AsyncAppendPhysicalProtectionTest                                   implemented checkpoint AD
AsyncReadAfterCommitRepairTest                                      implemented checkpoint AD
GenerationZeroRepairScannerTest                                     implemented checkpoint AD
MaterializationLagGateTest                                          implemented checkpoint AE
DefaultMaterializationLagSnapshotReaderTest                         implemented checkpoint AE
ManagedLedgerAsyncAppendAdmissionGuardTest                          implemented checkpoint AE
RegisteredMaterializationStreamScannerTest                          extended checkpoint AF repair-before-plan ordering
Phase4ObjectWalRuntimeTest                                          implemented checkpoint AF owned composition/lifecycle
ManagedLedgerGenerationProtocolTest                         implemented K protocol/CAS foundation
ManagedLedgerGenerationProjectionRefV1GoldenTest
ManagedLedgerMaterializationRegistrationCoordinatorTest    implemented checkpoint X identity/hint/response-loss/drift
ManagedLedgerGenerationRegistrationBackfillProofCoordinatorTest implemented checkpoint AA admission/CAS/epoch/loss
ManagedLedgerGenerationProtocolActivationGuardTest         implemented checkpoint AB marker/proof/delete admission
ManagedLedgerGenerationProtocolActivationCoordinatorTest   implemented checkpoint AC ACTIVE CAS/conflict/loss/drift
ManagedLedgerGenerationProjectionRefV1Test                 extended checkpoint Z exact candidate identity
NereusManagedLedgerOpenCoordinatorTest                     extended checkpoint X return ordering and Z unloaded capture
NereusManagedLedgerRuntimeTest                             extended checkpoints X/AA–AC ownership/close order
ProjectionIdentityTest                                     implemented checkpoint X canonical encoder
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
NereusGenerationProtocolCapabilityTest                    implemented checkpoint Y, extended AA core readiness
NereusCursorProtocolCapabilityTest                        extended checkpoint Y
NereusStorageBindingCapabilityTest                        extended checkpoint Y
NereusGenerationRegistrationBackfillTest                  implemented checkpoint Z order/concurrency/digest/drift, extended AA proof admission
NereusBrokerStorageConfigurationTest                      extended checkpoint AF exact profile/config/default/rejection
NereusManagedLedgerStorageGenerationActivationTest        implemented checkpoint AC success/failure/disabled sequencing
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
./gradlew phase4M5RegistrationFrontierCheck
./gradlew phase4M5GenerationCapabilityCheck
./gradlew phase4M5RegistrationBackfillCheck
./gradlew phase4M5RegistrationProofCheck
./gradlew phase4M5ActivationGuardCheck
./gradlew phase4M5PublicationActivationCheck
./gradlew phase4M5AsyncObjectWalCheck
./gradlew phase4M5Check
./gradlew phase4M5FinalCheck --rerun-tasks
```

Ordinary gate builds/publishes the F4 development artifacts and runs the exact clean local Pulsar source lock plus
focused fork tests/spotless. Final gate runs real two-broker Pulsar with real Oxia、pinned LocalStack and stock
BookKeeper coexistence. It proves projection-ref resolution、registration-before-marker crash cuts、async ack/read
repair、cold-topic backfill、policy trim、backlog eviction、capability rollout and source retirement without claiming a
Nereus BookKeeper profile.

`phase4M5RegistrationFrontierCheck` is the checkpoint-X precursor to those full M5 gates. It verifies the shared
projection-ref encoder、exact projection/L0 registration capture、idempotent create/monotonic hint refresh、
response-loss reload、final authority revalidation、open-return ordering and production shared-store ownership. It
does not modify the local Pulsar fork, advertise generation capability, activate a topic marker, enumerate cold
topics, or publish a cluster backfill proof. The ordinary gate passed with `--rerun-tasks` on 2026-07-16.

`phase4M5GenerationCapabilityCheck` is the checkpoint-Y precursor. It consumes the exact clean local fork
`master@148d18a404aee6eb0208a8a1f7e2c0eabc89a2a1`, audits the capability/readiness/invalidation surface, publishes
the existing Nereus F2 development composite, and runs broker spotless、checkstyle plus focused generation/cursor/
binding suites. The readiness identity is domain-separated SHA-256 over sorted persistent broker registry keys、
advertised broker ids、start timestamps and sorted required protocol pairs；the frozen two-broker fixture yields
epoch `4351585672493013605` and digest
`bc63f01d0aa01a65c7205625a2714f0246d8ba7e7b88b8a653137abbc719cc0d`. Registry notifications invalidate the
cache and a notification between equal snapshots fails closed. This gate does not enumerate cold topics、CAS the
registration backfill proof、activate a topic marker or enable publication/deletion. It passed with `--rerun-tasks`
on 2026-07-16.

`phase4M5RegistrationBackfillCheck` is the checkpoint-Z precursor. It consumes the current exact clean local fork
`master@148d18a404aee6eb0208a8a1f7e2c0eabc89a2a1`, publishes the Nereus development composite, audits the product
candidate and broker traversal/config/lifecycle surfaces, and runs managed-ledger plus Pulsar spotless、checkstyle and
focused suites. The traversal never loads/owns a topic；it sorts tenants/namespaces/topics, processes one namespace at
a time, batches topic work under the configured concurrency, captures strict NPR1 projection identity, performs
idempotent registration, re-reads binding and requires identical start/end readiness. The report hashes the full
canonical traversal, retains only the first 100 redacted failures and freezes coverage SHA
`2f234d6b9baa3a760460090850d22734f94cd72d51fd0f27706fda272fc01d7c`. This gate does not CAS the durable
`streamRegistrationBackfill` proof, activate a marker or enable publication/deletion. It passed with `--rerun-tasks`
on 2026-07-16.

`phase4M5RegistrationProofCheck` is the checkpoint-AA precursor. It consumes the current exact clean local fork,
publishes the Nereus development composite, inherits checkpoint Z's broker spotless/checkstyle/focused tests and adds
the exact proof contract-surface audit plus core/managed-ledger/adapter checks. The broker converts the complete
readiness identity without truncating the full digest, submits only a zero-failure report after final readiness
revalidation, and never owns the activation store. The product reacquires exact readiness, performs a bounded
response-loss-safe CAS, enforces same-epoch coverage immutability, resets dependent proof epochs on readiness advance
and rejects standalone advance after deletion enablement. The gate covers lost CAS responses and readiness
invalidation after CAS. It does not activate a topic marker or enable publication/deletion. It passed on 2026-07-16.

`phase4M5ActivationGuardCheck` is the checkpoint-AB precursor. It consumes the exact clean local fork
`master@148d18a404aee6eb0208a8a1f7e2c0eabc89a2a1`, publishes the current development composite and runs
core/managed-ledger/adapter checks plus broker-common/broker spotless、checkstyle and
`NereusBrokerStorageConfigurationTest`. The managed-ledger guard requires one exact ACTIVE cluster record carrying
the current readiness epoch、complete stream-registration proof and the canonical six-domain set whose frozen digest
is `5b29cf6df71cce198d01299f5bd740f0f123c601e12f04d8251d336a6a2a8c4d`. Live proofs bind strict NPR1
projection、topic metadata version、L0 identity/profile and registration；first marker activation is disabled unless
the explicit broker switch is true and response loss converges only from exact reload. Physical delete additionally
requires the current delete bits/backfills/object-store capability and the exact non-vetoed
`projection-generation-v1` snapshot. Revalidation fails closed on readiness、activation、topic or domain drift. This
gate does not advance the cluster record to ACTIVE and does not yet install the guard at mutation call sites, so
async publication、logical trim and physical deletion remain unavailable. It passed on 2026-07-16.

`phase4M5PublicationActivationCheck` is the checkpoint-AC precursor. It consumes the exact clean local fork
`master@148d18a404aee6eb0208a8a1f7e2c0eabc89a2a1`, publishes the current development composite and runs
core/managed-ledger/adapter checks plus broker spotless、checkstyle、
`NereusGenerationRegistrationBackfillTest` and
`NereusManagedLedgerStorageGenerationActivationTest`. The product coordinator makes no first write while the switch
is false, then requires current exact broker readiness、a same-epoch completed stream-registration proof and the
canonical six-domain set before its only transition：
`PREPARED -> ACTIVE(publication=true, physicalDelete=false, cursorSnapshotDelete=false)`. Condition conflicts retry
from the reloaded PREPARED value；concurrent success or a lost CAS response converges only from a valid durable ACTIVE
record. Cached readiness and ACTIVE authority are revalidated after CAS. Broker backfill completion waits for this
coordinator only when the report is zero-failure and the explicit switch is true；disabled/failure reports never call
it, and activation failure fails the returned promise. This gate does not set a topic marker or admit any
task/index/checkpoint/trim/delete mutation. It passed on 2026-07-16.

`phase4M5AsyncObjectWalCheck` is the checkpoint-AD precursor. It inherits the complete checkpoint-AC rollout chain,
then audits and tests the core-only opt-in Phase 4 Object-WAL execution boundary. The gate proves that legacy
constructors still install `Phase15StorageProfileResolver`、BookKeeper profiles remain rejected before IO、
`WAL_DURABLE` returns only after protected stable head but before generation-zero completion、strict durability waits
for exact `VISIBLE_GENERATION` protection、secondary failure cannot revoke an acknowledgement、restart/live read
repair materializes and protects the exact reachable commit、trim wins without creating a new index, and
NRC1-covered offsets are not regenerated as generation zero. It does not install the resolver in the production
provider, create the first topic marker, apply lag admission, or claim async Pulsar policy support.

Checkpoint AE closes the next product-neutral cut without claiming production rollout. It adds exact sync/async
Object-WAL profile persistence across F2 create/open/append, inserts activation/first-marker proof plus final
revalidation and authoritative lag admission into the per-stream append lane before writer preparation, and derives
lag from rebuilt current-policy COMMITTED coverage rather than tasks/checkpoint counters. Focused tests cover blocked
pre-I/O admission、one-delay remeasurement、records/bytes/age rejection、disabled thresholds、projection/profile
contradictions、proof revalidation、exact live-tail bytes/age and ahead-checkpoint rejection. On 2026-07-16 the full
`:nereus-core:test :nereus-metadata-oxia:test :nereus-managed-ledger:test :nereus-materialization:test` regression
passed.

Checkpoint AF completes the production composition cut. `Phase4ObjectWalRuntime` owns the shared read-target
registry、checkpoint replay/index repair、generation-zero source repair、lag reader and background materialization
service；`DefaultNereusRuntimeProvider` installs it together with the async admission guard and passes the exact read
components into `DefaultStreamStorage`. The runtime starts before broker storage is returned and closes before
generation/Oxia/ObjectStore resources. The locked Pulsar mapper persists only exact sync/async Object-WAL profile
names, constructs the complete validated `MaterializationConfig`, appends processRunId to the staging base and keeps
sync as the default. Focused Nereus module tests and `NereusBrokerStorageConfigurationTest` passed on 2026-07-16.
Retention/admin and destructive GC composition remain outside this checkpoint.

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
