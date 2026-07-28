# 07 — Implementation Plan and Gates

> 状态：F9-M1/M2/M3 implementation slices complete；F9-M4 all seven canonical states/strict V1 codecs/full composition plus local Kafka-fork producer/transaction import/replay and isolation shell slices implemented，including single-node real user/group/transaction restart and interrupted-transaction recovery；M5 deterministic retention/compaction slices implemented；M6 Object sync、Object async and all three BookKeeper real release/fresh-JVM gates pass；Kafka NCP2 direct-stream materialization runtime/profile composition、real five-profile provider evidence、Kafka-fork five-profile mapping、BookKeeper async/sync fresh-process gates、provider-level applied-delete response-loss、release-process physical-deletion/fresh-JVM NCP2 fallback and real-Oxia two-runtime Object-WAL live leader takeover are implemented；release-process response-loss restart、two Kafka-process/multi-controller takeover、remaining M4 internal-topic cuts and inherited final gates remain open
> 2026-07-28 状态增量：two-release-process Object-WAL/KRaft singleton takeover 已实现并进入 M6 process aggregate；仍 open 的 takeover 边界是 already-dispatched old append、BookKeeper profiles、coordinator/internal topics、multi-controller 与 chaos
> Sequence：F9-M0 → M1 → M2 → M3 → {M4,M5} → M6 → M7
> Rule：one milestone commit series + ordinary gate + fresh final gate + mandatory review stop

## 1. Repository boundary

### 1.1 Nereus product repository

This repository owns：

- protocol-neutral ranged append/read/session/format evolution；
- `nereus-kafka-adapter` and its public packaging；
- Oxia binding/activation/checkpoint records/codecs/stores；
- Kafka batch codec/partition storage/recovery/retention/compaction services；
- NCP2/NTC2/NKC1 object formats；
- real provider integration tests and root aggregate gates。

### 1.2 Kafka organization fork

Kafka source-tree changes belong in `github.com/nereusstream/kafka`，branch
`nereus/future9-native-kafka-storage`。The fork should start from an exact Apache Kafka upstream commit；the local AutoMQ checkout
is a reference/source-audit input，not automatically the production fork base。Before F9-M3：

```text
pin exact Apache Kafka upstream commit/version
record Nereus fork commit/blob hashes
diff relevant APIs against AutoMQ 1c648d...
refresh document 03 method map
compile signature probe
```

If the project later deliberately bases on AutoMQ rather than Apache Kafka，that is a separate ADR/source-lock update and must
remove or disable conflicting `Elastic*` execution paths（notably `elasticstream.enable`）。

### 1.3 Development dependency flow

```text
Nereus build/publish dev artifacts
  -> local isolated Maven repository
  -> Kafka fork build consumes exact dev version
  -> Kafka fork focused tests/package
  -> Nereus aggregate gate invokes fork checkout + real service fixture
```

The product repository stays buildable without Kafka checkout except `phase9*` tasks and `nereus-kafka-adapter` fork-surface
verification。Configuration mirrors existing `pulsarCheckout` pattern：Gradle property `kafkaCheckout` or env
`NEREUS_KAFKA_CHECKOUT`，with exact settings/source-lock validation。

## 2. Planned module changes

### 2.1 New module

At F9-M2 add `nereus-kafka-adapter` to `settings.gradle.kts` and BOM。Dependencies：

```text
api(project(:nereus-api)) only where callers need public values
implementation(project(:nereus-core))
implementation(project(:nereus-metadata-oxia))
implementation(project(:nereus-object-store))
implementation(project(:nereus-materialization))
implementation(selected Kafka server/storage/client artifacts)
testImplementation(shared real-service fixtures)
```

Kafka artifacts never leak into `nereus-api`、`nereus-core` or `nereus-metadata-oxia` public signatures。

### 2.2 Existing modules

| Module | Planned ownership |
| --- | --- |
| `nereus-api` | append precondition、read request/result、authority request/result、errors |
| `nereus-core` | new overload implementations、range/result validation、authority/session propagation |
| `nereus-metadata-oxia` | StreamHead V2 dual codec、Kafka records/keyspace/stores/transitions/activation |
| `nereus-object-store` | WAL ranged read + NCP2/NTC2/NKC1 strict IO/verification |
| `nereus-materialization` | ranged lossless publisher、exploding compaction SPI/V2 engine、generation refs |
| `nereus-bookkeeper` | containing-entry/first-overflow reader parity，no Kafka type |
| `nereus-managed-ledger` / Pulsar adapter | non-regression only；default exact semantics unchanged |
| `nereus-kop-adapter` | no dependency/change unless shared neutral gate finds regression |

## 3. Package/file target map

### 3.1 `nereus-api`

```text
AppendPrecondition.java
ReadBoundaryMode.java
FirstEntryPolicy.java
ReadRequest.java
SemanticReadResult.java
AppendAuthority.java
AppendSessionRequest.java
AcquiredAppendSession.java
AppendBatch.java                         format-specific validation change
StreamStorage.java                       binary-safe overloads
ErrorCode.java                           append-only constants
```

### 3.2 Core/primary readers

```text
nereus-core/.../DefaultStreamStorage.java
nereus-core/.../append/AppendCoordinator.java
nereus-core/.../append/AppendResultValidator.java
nereus-core/.../append/AppendSessionManager.java
nereus-core/.../read/ReadCoordinator.java
nereus-core/.../read/ReadTargetDispatcher.java
nereus-core/.../read/ReadRequestValidator.java
nereus-core/.../read/StreamViewReader.java       deprecated bridge

nereus-object-store/.../wal/DefaultWalObjectReader.java
nereus-object-store/.../wal/DefaultWalObjectWriter.java
nereus-bookkeeper/... reader/adapter equivalents
```

### 3.3 V2 formats

```text
nereus-object-store/.../compacted/CompactedObjectFormatV2.java
RangedCompactedObjectRow.java
RangedCompactedObjectMetadata.java
ParquetRangedCompactedObjectWriter.java
ParquetRangedCompactedObjectReader.java
RangedCompactedObjectVerifier.java
KafkaTopicCompactedFormatSpecV2.java
ParquetKafkaTopicCompactedWriter.java
ParquetKafkaTopicCompactedReader.java
RangedCompactedObjectVerifier.java             shared exact NCP2/NTC2 verifier

nereus-object-store/.../kafka/checkpoint/KafkaCheckpointFormatV1.java
KafkaCheckpointWriter.java
KafkaCheckpointReader.java
KafkaCheckpointVerifier.java
```

V1 classes remain closed；do not add V2 optional branches to their schema validators。

### 3.4 Metadata

```text
KafkaPartitionKeyspace.java
KafkaPartitionMetadataStore.java
OxiaJavaKafkaPartitionMetadataStore.java
KafkaPartitionMetadataTransitions.java
KafkaStorageActivationMetadataStore.java
OxiaJavaKafkaStorageActivationMetadataStore.java
records/Kafka*.java                     exact records in documents 04/06
codec/Kafka*CodecV1.java
codec/StreamHeadRecordCodecV2.java
```

### 3.5 Kafka adapter

```text
config/NereusKafkaStorageConfig.java
config/NereusKafkaConfigValidator.java
runtime/NereusKafkaRuntime*.java
runtime/KafkaStorageAdmission.java
runtime/KafkaStorageHealth.java
partition/KafkaPartition*.java
codec/KafkaRecordBatchCodec.java
codec/KafkaAppendBatchEncoder.java
codec/KafkaFetchAssembler.java
metadata/KafkaPartitionLifecycleCoordinator.java
checkpoint/KafkaCheckpoint*.java + section codecs
recovery/KafkaPartitionRecoveryCoordinator.java
recovery/DefaultKafkaPartitionRecoveryLauncher.java
recovery/DefaultKafkaRecoveryBatchSource.java
recovery/KafkaRecoveryBatchPage.java
recovery/KafkaRecoveryState.java
recovery/KafkaRecoveryStateFactory.java
retention/KafkaRetention*.java
compaction/KafkaCompaction*.java
admin/NereusKafkaStorageAdmin.java
metrics/NereusKafkaStorageMetrics.java
```

### 3.6 Kafka fork

New classes and modified stock files are exactly those listed in document 03 §2/§3。Fork tests live beside Kafka core/storage
tests and use paired Nereus marker checker。

## 4. F9-M0 — Design/source gate

### Deliverables

- Future 9 roadmap/index/architecture references；
- documents 01–08 complete；
- AutoMQ reference commit/version/blob lock；
- current Nereus gap evidence；
- F5/F9 separation；
- API/format/metadata/fork/runtime/scenario traceability。

### Gate

```text
documentation links resolve
git diff --check
all status claims are Designed
no production source/build implementation change
review every MUST invariant against scenario matrix
```

M0 does not require a Kafka org fork yet。M3 entry does。

## 5. F9-M1 — Ranged-entry foundation

### Implementation slices

1. public value objects/default overloads/error append；
2. AppendBatch Kafka/ranged validation and exact result precondition；
3. StreamHead V2 codec scaffolding only as needed for authority compatibility tests；
4. Object WAL + BookKeeper containing-entry/overflow parity；
5. semantic view public surface；
6. NCP2 writer/reader/verifier/goldens；
7. NTC2 physical format primitive/goldens（no Kafka compaction engine yet）；
8. exact format registry/capability admission。

### Planned test source sets/tasks

```text
:nereus-api:test
:nereus-core:rangedEntryTest
:nereus-object-store:rangedFormatTest
:nereus-object-store:rangedFormatS3IntegrationTest
:nereus-bookkeeper:rangedBookKeeperIntegrationTest
phase9M1Check
phase9M1FinalCheck --rerun-tasks
```

`phase9M1FinalCheck` also depends on current Phase 1/1.5/F2/F3/F4/F1-BK public/format/reader gates。Docker tests use existing
shared gate service。

### 5.1 Current implementation evidence（2026-07-23）

- `phase9SourceLockCheck` locks AutoMQ `1c648d...` / `3.9.0-SNAPSHOT` and 29 current Nereus ranged/head/session/planner/provider-composition source blobs；
- `phase9M1Check --rerun-tasks` passes API、conditional append/result、semantic read、Object WAL、BookKeeper、
  exact reader registry/capability and materialization tests；
- `:nereus-object-store:rangedFormatS3IntegrationTest --rerun-tasks` passes NCP2 and NTC2 upload/read/full verification
  against pinned LocalStack `4.14.0`；
- frozen object SHA-256：NCP2 `671ac184f5b1fbf898329cd868f88d53a569e229cfeb451ebdb4c618b5591532`，
  NTC2 `367da6663bb4e8d6e83e942277b3a250b86ec13f4f4a5863235aed32157bd2e8`；
- frozen capability digest：`3c99feb81221497e1e1e7401766ecad898ace0cce2a68312c91bbec25b09bace`；
- `phase9M1FinalCheck --rerun-tasks` currently stops in inherited `:nereus-managed-ledger:compileJava` because the local
  Pulsar checkout is `5ffc2caa...` while Nereus locks `2f9c1eb...`；this is recorded as environment/source-lock drift，
  not counted as a passed milestone final gate。

### Exit

- every primary reader passes same range contract；
- V1 goldens unchanged/dual readable；
- old default APIs exact；
- old capability excluded from V2 writes；
- no Kafka module/fork path is required to pass M1。

## 6. F9-M2 — Binding, authority and checkpoint

### Implementation slices

1. add module skeleton/config-free domain values；
2. Kafka keyspace/records/explicit codecs/fake store；
3. real Oxia store + all-shard scans；
4. authority-bound session/head V2 dual-read/CAS；
5. deterministic binding create/delete/scanner；
6. NKC1 container + physical protections/pins；
7. checkpoint publication/fallback；
8. recovery state machine tested with synthetic Kafka batches/state codec（no broker fork）。

### Tasks

```text
:nereus-metadata-oxia:f9MetadataTest
:nereus-metadata-oxia:f9OxiaIntegrationTest
:nereus-object-store:kafkaCheckpointTest
:nereus-object-store:kafkaCheckpointS3IntegrationTest
:nereus-kafka-adapter:f9M2Test
:nereus-kafka-adapter:f9M2IntegrationTest
phase9M2Check
phase9M2FinalCheck --rerun-tasks
```

### Mandatory review stop

Review durable bytes/keys/wire IDs、head V1→V2 migration、authority comparison、no-cross-shard cuts、NKC1 bounds and
checkpoint-before-trim proof。No Kafka fork coding starts until accepted。

### 6.1 Current implementation evidence（2026-07-23）

- slices 1–8 are implemented：module/domain skeleton、canonical keys、25-field root + registry explicit V1 codecs、
  fake/real Oxia stores、authority-bound head V2、deterministic binding lifecycle/all-shard scan、NKC1 protected
  publication/fallback and fresh-state exact committed replay；
- frozen Phase 1 V1 codec goldens remain unchanged；binding/registry envelope SHA-256 values are
  `c196685df742d8ff9528bfa5eb4fa7e3c7a9ec8b7077818a19d100a4050ba578` and
  `8919c79ce1e19e4128ef905b78d18e45ec49d1df4a2f2a582e2e183f249a3b55`；
- focused metadata/Oxia/adapter tests cover key round trips、unknown wire values、single-key CAS races、stream-create
  response loss、idempotent delete、same-name/new-topic isolation、all 64 registry shards、checkpoint PUT reconciliation、
  physical protection/pins、newest-to-older fallback、trim fail-closed and pre/post-publication head-fenced synthetic batch replay；
- frozen NKC1 full-object SHA-256 is
  `c6d8848d7e946917e649b0fb0679f390ce76c8660a88bf447c797581285ce91c`；
- `phase9M2Check --rerun-tasks` and the three direct real-service/integration tasks pass on current source；
  `phase9M2FinalCheck --rerun-tasks` is blocked by the inherited Pulsar checkout HEAD mismatch
  (`required 2f9c1eb...`，`local 5ffc2caa...`) rather than an F9-M2 test failure。

## 7. F9-M3 — Native Produce/Fetch

### Entry

- M1/M2 final gates fresh-pass；
- exact `nereusstream/kafka` fork source lock committed；
- local fork clean；relevant method signatures match document 03；
- development artifacts isolated by exact version/repository。

### Implementation slices

1. fork config/runtime factory/log-manager wiring disabled fallback；
2. `NereusUnifiedLog`/`LocalLog`/segment/records and batch codec；
3. single-partition stable Produce with acks 0/1/-1；
4. bounded append executor/buffer ownership/error mapping；
5. committed Fetch/limits/async operation；
6. LEO/HW basic state and ListOffsets earliest/latest/timestamp；
7. leader open/resign/takeover/restart；
8. all activated Nereus profiles；
9. multi-partition request and stock-disabled compatibility。

### Tasks

```text
Kafka fork: nereusF9SourceLockTest
Kafka fork: nereusF9LogTest
Kafka fork: nereusF9ProduceFetchTest
Kafka fork: nereusF9DisabledCompatibilityTest
Nereus: :nereus-kafka-adapter:f9M3IntegrationTest
Nereus: :nereus-kafka-adapter:f9M3ProviderIntegrationTest
Nereus: :nereus-kafka-adapter:f9MultiBrokerTakeoverProviderIntegrationTest
Nereus: phase9M3ProviderCheck
Nereus: phase9M3KafkaCheck
Nereus: phase9M3Check
Nereus: phase9M3FinalCheck --rerun-tasks
```

Final uses real KRaft broker process + real Oxia + selected primary WAL/Object provider，not in-memory storage。

### Exit limits

M3 supports non-transactional `delete`-policy user topics for the gate。It is not a full Kafka compatibility claim；internal
coordinator/transaction/compaction remain M4/M5。

### 7.1 Current partial implementation evidence（2026-07-23）

- implemented the Nereus-side `KafkaRecordBatchCodec`、`KafkaAppendBatchEncoder`、exact append-result validator、
  `KafkaFetchAssembler` and authority-bound `DefaultKafkaPartitionStorage` with owned byte arrays/read-only buffers；
- `:nereus-kafka-adapter:f9M3CodecTest --rerun-tasks` passes against test-only Kafka 3.9.0-generated batches，covering
  uncompressed/GZIP、multi-batch、producer facts、CRC/length/magic/compression corruption、offset gaps、containing-entry
  Fetch and sparse compacted coverage；
- partition tests cover two-phase durable-end then exact derived HW/LSO publication、acks 0/1/-1 invariant、same-partition serialization、
  speculative gap rejection、known-not-committed retry、uncertain/result-mismatch fencing、containing-entry/upper-bound/
  first-overflow Fetch and resign drain；the next same-partition append and `STABLE_APPEND` event wait for exact
  `publishDerivedOffsets` confirmation；
- `KafkaBoundedAppendExecutorTest` proves owned remaining-byte snapshot、global byte lease、logical
  `threads + queueCapacity` rejection before task/I/O、release-once on every terminal path、strict equal-key FIFO、
  cross-key concurrency、single-worker fairness、close-then-drain of logical lane work and the rule that response-future
  cancellation cannot cancel admitted append work；`KafkaAppendFailureClassifierTest` proves only explicit
  known-not-committed remains writable，while
  authority/offset/unknown/uncertain/known-committed failures fence and checksum/format/invariant failures go offline；
- `KafkaFetchOperationTest` proves actual-byte minBytes、stable-event wakeup、event coalescing、one read in flight per
  partition、deadline final read、request-wide ordered byte budget、executor rejection before storage read、leadership/runtime
  cancellation cleanup and callback exactly once；`DefaultKafkaPartitionStorageTest` proves event publication after stable
  state and that listener failure cannot reclassify append I/O；
- `KafkaFetchWaveOperationTest` further proves an opaque stock-compatible whole-request wave registers signals before the
  initial read、coalesces events with one wave in flight、always performs one deadline final read after the event-reread
  safety budget（including a deadline race with an enough in-flight wave）、rejects before source I/O、isolates caller
  cancellation and cleans the subscription before callback；
- `KafkaPartitionLeaderManagerTest` proves exact-open deduplication、higher leader-term and same-owner broker-epoch takeover、
  conflicting/stale authority rejection、late-old-open fencing、stale-resign isolation、shutdown fencing and strict
  opener-result identity/epoch/state validation；durable authority acquire/recovery remains the opener responsibility；
- `f9MultiBrokerTakeoverProviderIntegrationTest` supplies that durable authority/recovery layer with two independent
  activated Object-WAL runtime graphs against real Oxia and one shared provider root：broker B's higher leader epoch
  immediately preempts broker A before TTL，recovers A's exact committed batch，forces A's next append into
  `WRITE_FENCED_RECOVERY_REQUIRED` and continues at the recovered end。`DefaultKafkaPartitionStorageTest` separately locks
  the implementation rule that `FENCED_APPEND`/expired-session/head-offset conflicts fence even when the failed append is
  provably known-not-committed。This is R-tier provider evidence；
- `f9MultiBrokerTakeoverProcessIntegrationTest` now supplies P-tier release/KRaft evidence：node 1 combined controller/broker
  commits offset 0 on exact assignment `[1]`；node 2 broker-only joins the same cluster；Admin singleton reassignment
  atomically reaches `leader=2, replicas=[2], ISR=[2]` with no ongoing reassignment while node 1 remains alive；node 2
  recovers offset 0 and commits/reads offset 1。The controller test
  `testNereusStorageFeatureAtomicallyHandsOffSingletonReplica` requires a single change record with no transitional RF2，
  and `testLocalReplicaRemovalResignsWithoutDeletingSharedBinding` requires the departing broker to resign rather than
  delete the shared binding。Fresh task execution passes 73/73 actionable tasks in 1m04s。Old already-in-flight append、
  BookKeeper/profile、coordinator/internal-topic、multi-controller and chaos cuts remain open；
- fresh `phase9M3ProviderCheck --rerun-tasks` passes 64/64 actionable tasks on this source。The aggregate reruns the
  146/146 scenario manifest、29-blob Nereus source lock、Apache Kafka baseline lock、M1/M2/M3 codec predecessors and all
  three provider gates：Object sync/async round trip、two-bookie BookKeeper WAL-only/profile composition and the new live
  two-runtime takeover；
- `KafkaStorageProfilePolicyTest` freezes exactly five activated canonical profiles and proves every adapter append uses the
  profile default durability plus `PROFILE_DEFAULT` completion；legacy alias、non-default durability and weakened completion
  are rejected before I/O。This is deterministic partial evidence for KF-APP-016 only；the real provider/KRaft profile
  matrix remains open；
- `DefaultKafkaPartitionStorageManagerTest` proves binding-before-open with the real deterministic lifecycle、exact
  binding/profile open plans、authority-open dedupe、operation-owned completion、stale-resign isolation、drain-before-delete and
  shutdown fencing；it now also proves the concrete runtime's executable-profile set rejects an unavailable profile before any
  durable binding write；
- `StableStreamHeadSnapshotTest`、`KafkaLeaderAuthorityIntegrationTest` and `DefaultStreamStorageAppendTest` prove the
  protocol-neutral exact-head seam、canonical durable digest、authority-session projection、renewal digest change、core facade
  delegation、genesis commitVersion `0` and exact mixed-chain ancestor proof；
- `DefaultKafkaCheckpointSourceValidatorTest` and `DefaultKafkaPartitionOpenerTest` prove exact authority/profile/session
  source loading、foreign/stale head fencing、descendant-bound ancestor delegation、remaining-deadline recovery launch、exact
  frozen result validation and writable storage construction；`DefaultStreamStorageAppendTest` and
  `DefaultKafkaPartitionStorageTest` prove the public exact-session renewal path、strict monotonic returned token、renewed token
  use by later appends、failure-to-write-fence transition、leadership-loss event and no queued append dispatch after authority
  loss。The product/fork recovery composition is now wired；native log consumption remains open；
- `KafkaListOffsetsResolverTest` proves stable-snapshot earliest/latest、compressed exact-record timestamp lookup、
  bounded cross-page scan、max-timestamp lowest-offset tie-break、budget exhaustion without approximate answers、invalid
  inspector result rejection and leadership loss during inspection。This is deterministic Nereus-side partial evidence for
  KF-FET-009；local fork tests now prove the stock 4.3 compressed `MemoryRecords` exact iterator、minimum-offset filtering、
  buffer-state preservation、max-timestamp tie-break、Kafka sentinel/request projection、immediate/async
  `OffsetResultHolder` conversion、cancellation propagation、delayed-operation wakeup-once and exhaustive adapter error
  mapping。The stock `Partition` seam additionally proves exact-epoch installation、identity-safe removal and revocation
  before a newer leader epoch is published；`ReplicaManager.fetchOffset` supplies the existing delayed-purgatory wakeup。
  `NereusListOffsetsLifecycleTest` additionally proves only-after-recovery installation、exact stock/adapter identity and
  leader-epoch validation、operation-owned open completion、late-old-open cleanup、stale-resign isolation and
  remove-before-resign/delete/shutdown ordering。The fifth local fork commit adds same-epoch recovery-pending fail-closed
  routing、a stock-compatible `AsyncTopicDeltaLifecycle`、exact local-change composition and optional
  `BrokerMetadataPublisher` routing。`NereusTopicDeltaLifecycleTest`、`ReplicaManagerTest` and
  `BrokerMetadataPublisherTest` prove stock-state-first preparation、old/new image identity selection、delete-before-recreate、
  callback-after-success、coordinator-election-after-recovery and `firstPublishFuture` non-readiness semantics。Time-index
  checkpoint candidate remains open；provider-backed BrokerServer activation and the real combined-node native-storage
  KRaft baseline now pass through `phase9M6KafkaProcessCheck`，including single-node graceful restart of committed
  `__consumer_offsets` and `__transaction_state` truth；
- `NereusKafkaObjectWalRuntimeConfigurationTest` freezes the concrete provider graph to exactly
  `OBJECT_WAL_SYNC_OBJECT + OBJECT_WAL_ASYNC_OBJECT` without BookKeeper，or exactly those two Object profiles plus all three
  BookKeeper profiles when the provider-neutral BookKeeper configuration/context is present；
  both graphs retain matched cluster/writer/session/scan limits and disabled legacy auto-session；
  `NereusKafkaObjectWalRuntimeFactoryTest` proves activation scope is rejected before provider IO，and a checked
  provider-construction failure is propagated with every resource registered before that cut closed；
  `f9M3ProviderIntegrationTest` passes against real Oxia plus the filesystem ObjectStore provider and covers deterministic
  ACTIVE/readiness seeding、runtime capability resume/verification、binding、authority acquire/recovery、leader open、stable
  Produce、committed Fetch、async NCP2 publication/byte-exact normal read and owned provider shutdown。
  `f9MultiBrokerTakeoverProviderIntegrationTest` uses the same production factory but constructs two independent runtime
  ownership graphs over one real durable authority；the higher KRaft leader epoch replaces the live old head session，
  performs exact committed replay，fences the old runtime on its next append and continues Produce/Fetch at the recovered
  offset。`phase9M3ProviderCheck` now depends on this task；
  `f9BookKeeperWalOnlyProviderIntegrationTest` separately provisions the exact F1-BK ledger-ID namespace/publication
  activation，borrows a real two-bookie client，then proves Kafka leader open、strict append、physical-reference publication
  and cold generation-zero Fetch against the shared Oxia graph。The BookKeeper client remains borrowed and the provider-neutral
  runtime/stores/readers are closed through the product ledger。The same gate now opens `BOOKKEEPER_WAL_ASYNC_OBJECT` and
  `BOOKKEEPER_WAL_SYNC_OBJECT`，verifies NCP2 COMMITTED publication、byte-exact normal reads and the sync append
  required-generation completion barrier。Fork `ebf1d76163` adds the complete stock-owned typed
  BookKeeper binding、exact file/version secret reference、pre-I/O cross-field validation、BookKeeper client construction and
  product-before-client close wrapper；six server config tests、eight mapper/ownership tests、Checkstyle、SpotBugs and
  Spotless pass。The same head also switches the Nereus-enabled fork to one SLF4J 2.0.17 API and one Log4j
  SLF4J2 provider，excludes the legacy/provider-conflicting artifacts and removes the Kafka 1.7 API from the release tar；
  the full 189-test bridge slice and exact two-jar tar inventory pass。
  `f9BookKeeperWalOnlyProcessIntegrationTest` now adds real release-distribution P-tier evidence：stock
  ZooKeeper long-hierarchical metadata、two bookies、combined-node Kafka Admin create/Produce/Fetch/ListOffsets、normal
  shutdown、fresh-JVM recovery and continued append all pass。The same Object provider is also opened under
  `OBJECT_WAL_ASYNC_OBJECT` in the real Oxia integration，and `f9ObjectWalAsyncObjectProcessIntegrationTest` proves
  first-JVM append/read plus fresh-JVM recovery/continued append over real Oxia and LocalStack。
  `f9BookKeeperWalAsyncObjectProcessIntegrationTest` appends four batches、waits for an NCP2 object in real
  LocalStack、normally stops and recovers/appends in a fresh JVM；`f9BookKeeperWalSyncObjectProcessIntegrationTest`
  proves a one-batch append does not complete before its required NCP2 object is COMMITTED/readable，then performs the same
  cold recovery。The WAL-only/async/sync tasks are composed by `phase9M6KafkaBookKeeperProcessCheck`。The next deletion
  slice moves `BookKeeperDeletionActivationCoordinator` and its request/result/stream-proof contract into
  `nereus-bookkeeper`，adds provider interfaces for root/stream/scope evidence，and lets both Pulsar and Kafka retain their
  adapter-specific stream authority producer。Kafka now scans all 64 binding shards plus all 64 F4 registration shards，
  proves exact authoritative binding/L0/direct-materialization agreement，then starts one-shot deletion activation between
  materialization and retention。`KafkaBookKeeperStreamCoverageProofProducerTest` covers deterministic digest、both complete
  shard sets、WAL-only、missing registration and L0 drift；`CompositeKafkaRuntimeBackgroundServiceTest` covers forward
  start、reverse close and rollback。`f9BookKeeperLedgerDeletionProviderIntegrationTest` now starts real Oxia plus two
  bookies，forces one sealed async ledger、waits for NCP2 and terminal-source protection retirement，observes the exact
  `SEALED -> MARKED -> DELETING -> DELETED` chain，proves the provider ledger is absent and reads the same Kafka bytes from
  NCP2。`TerminalWorkflowMetadataRetirementTest` separately locks the Kafka-specific
  `KAFKA_RECORD_BATCH_V1 -> KAFKA_RECORD_BATCH` logical/payload mapping used by terminal proof。The task is wired into
  `phase9M5RetentionCheck` as `f9BookKeeperLedgerDeletionProviderIntegrationTest`。The same real provider gate now
  decorates the exact borrowed operations instance after runtime start，injects failure only after the target Kafka WAL
  ledger delete has applied，asserts that exact ledger ID，and proves metadata-absence convergence to `DELETED` plus NCP2
  readability。The compatibility context constructor still supplies the standard client adapter；
  `f9BookKeeperWalAsyncObjectProcessIntegrationTest` additionally uses the release distribution with one-entry rollover，
  waits for metadata `DELETED` and independent-client `NoSuchLedger`，normally stops the first JVM，then proves a fresh
  JVM can recover offset 0 from NCP2 and continue append/fetch/ListOffsets。Release-process response-loss restart and
  multi-broker takeover remain open；
- the fresh 2026-07-28 aggregate additionally locks the planner path required by repeated physical deletion cycles：after
  an NCP2 higher generation replaces a retired/deleted BookKeeper prefix，a wider task selects that NCP2 prefix together
  with the still-readable BookKeeper tail。Only the exact
  `KAFKA_RECORD_BATCH <-> KAFKA_RECORD_BATCH_V1` byte-equivalent mapping is normalized；all other logical-format
  mismatches remain fail-closed。The deterministic planner reproducer、focused materialization suite and real
  `f9BookKeeperLedgerDeletionProviderIntegrationTest` pass。The combined current-slice aggregate
  `phase9M3KafkaForkCheck phase9M5CompactionCoreCheck phase9M6ActivationMetadataCheck phase9M6KafkaFeatureCheck
  phase9M6CheckpointQuarantineCheck --rerun-tasks`
  passes 109/109 outer tasks against fork `ebf1d76163`，including nested 92/92 stock and 95/95 artifact-enabled Kafka
  builds。This is partial aggregate evidence only；it does not close delete-response-loss、takeover or M7；
- the 2026-07-28 Kafka NCP2 materialization core checkpoint adds the distinct
  `nereus-kafka-committed-v2` / `NEREUS_COMPACTED_PARQUET_V2` committed policy，without reinterpreting NCP1 durable
  records。`DefaultMaterializationWorker` now has an NCP2-only ranged writer path that preserves every Kafka
  `RecordBatch` byte payload and exact `[baseOffset,lastOffset+1)` range，publishes `KAFKA_RECORD_BATCH_V1` row
  metadata，checks task/output entry and record accounting，and freezes `OUTPUT_READY` only after strict whole-object
  verification by `RangedMaterializationFormatVerifier`。Planner、registry scanner、required-generation coordinator and
  generation committer now accept an explicit `DIRECT_STREAM` authority mode while all compatibility constructors remain
  `PROJECTION_REQUIRED`，so Pulsar NCP1/NTC1 behavior is unchanged。The Kafka activation guard admits
  `GENERATION_PUBLISH` with the same ACTIVE/readiness proof and revalidation boundary used by direct-stream compaction。
  `KafkaObjectMaterializationRuntime` now owns one process-shared direct NCP2 service、staging directory and bounded worker
  executor while borrowing the provider metadata/ObjectStore/scheduler/read graph。The activated runtime closes it through the
  normal product resource ledger。`KafkaAsyncAppendAdmissionGuard` requires an ACTIVE `GENERATION_PUBLISH` proof，applies the
  materialization lag gate and revalidates immediately before both Object/BookKeeper async appends；sync profiles retain their
  required-generation barrier。The profile resolver and fork mapper now map all five profiles when BookKeeper is installed，
  retain the exact selected default and place NCP2 staging below the authoritative cache root。Focused tests、the real
  Object/BookKeeper provider gates and both BookKeeper materialization fresh-process gates pass；
- `NereusKafkaObjectWalRuntimeFactory` now owns durable checkpoint read pins、reader/verifier/recovery coordinator、
  configured `recoveryChunkRecords/recoveryChunkBytes` paging and `DefaultKafkaPartitionRecoveryLauncher`。
  `DefaultKafkaRecoveryBatchSourceTest` proves exact bounded COMMITTED/EXACT_START pages and fail-closed empty/non-Kafka
  results；`KafkaCheckpointPublicationRecoveryIntegrationTest` proves multi-page replay。The production Object-WAL graph now
  owns a separate immutable Oxia checkpoint-failure store and injects the same
  `DurableKafkaCheckpointFailureQuarantine` into recovery and retention。Exact persisted refs skip object I/O；new eligible
  permanent failures await durable create/response-loss reconciliation before fallback，and quarantine metadata failure
  fails closed；
- fork `NereusKafkaRecoveryStateCodecTest` proves three stock magic-v2 RecordBatch recovery cases covering CRC、single
  entry/batch、compressed/uncompressed dense offsets、timestamp/leader-epoch derivation、trailing/source mismatch and M3
  producer/transaction/NKC1 rejection。`NereusKafkaRecoveryStateFactoryTest` proves two exact live-Partition publication/stale
  epoch cases；`NereusKafkaRecoveryStateFactoryBridgeTest` proves two one-time/pre-bind cases。State is provisional under the
  Partition write lock and topic-lifecycle failed-open cleanup removes it before coordinator readiness；
- fork `NereusUnifiedLogFactoryTest` proves the per-broker factory selects only the dedicated
  `${cacheDir}/{brokerId}/partition-logs` root、ignores stale cache logs、does not use `log.dirs`、requires non-zero topic ID
  and creates an exact identity shell。An actual empty recovered state plus exact manager storage publication makes the shell
  report writable；exact instance removal revokes it。At `dc8c66388a`，stock validation/offset assignment precedes stable
  adapter append，LEO advances only after exact result validation，Fetch returns exact adapter bytes as `MemoryRecords`，and
  synthetic local size stays zero。Fault injection proves invalid stable result and post-stable failure resign/fence；
  required-acks routing has a stock `Partition` regression test。Stock-without-artifacts `LogManagerTest`、
  `PartitionTest` and `BrokerStorageRuntimeFactoryTest` preserve local behavior；
- at `ee608625e4`，`NereusBrokerStorageAppendExecutorTest` proves exact copy-before-return、same-partition FIFO and
  request-limit rejection；the product executor test additionally proves cross-partition concurrency、single-worker fairness、
  cancellation isolation and close/drain。A stock `ReplicaManagerTest` proves the optional path defers append、validation stats
  and response until executor completion；runtime tests prove disabled `None`、enabled `Some` and combined append/product
  drain。Focused stock/artifact-enabled tests and core format/static gates pass；fresh exact-head aggregate evidence is
  recorded below；
- at `bba3ef0121`，stock `BrokerStorageFetchExecutor` preserves the disabled purgatory branch and submits one ordered
  whole-request read closure in enabled mode。`NereusBrokerStorageFetchExecutorTest` proves subscribe-before-read event
  reread/listener cleanup、logical-cap rejection before a third storage wave and accepted deadline completion；
  `ReplicaManagerTest` uses two partitions to prove deferred response、order/cardinality、per-partition error and worker-side
  action-queue drain。Runtime tests prove disabled `None`、enabled `Some` and append/fetch/product combined drain；
- at `47d36a1d9f`，the internal read-control queue retains one runner for every logically admitted Fetch operation，
  preventing a simultaneous stable-event burst from rejecting accepted work while preserving the
  `threads + queueCapacity` logical cap。The deterministic capacity test holds two admitted requests，rejects the third
  before read，wakes both exact listeners and requires all four waves plus drain；
- the sixth local fork commit registers the complete 58-key inert `ConfigDef` with safe disabled default，builds an immutable
  side-effect-free typed snapshot and executes enabled-only provider/budget/liveness plus broker-role/RF/minISR/remote-log/
  cleaner/AutoMQ/request-limit/directory validation。Six snapshot tests、four validator tests and the complete stock
  `KafkaConfigTest` pass with server/core checkstyle and SpotBugs。This is deterministic partial evidence for KF-SRC-007、
  KF-SRC-008 and KF-OPS-001/002；runtime creation、activation/cluster ID、controller create-topic enforcement、secret
  redaction and real-process cuts remain open；
- the M3 head rejects idempotent/transaction/control input until M4 owns producer/transaction state；isolated M4 commits
  `ec7f0db991` and `032974067c` now own stock import/replay、transactional shell semantics、request executor parameter
  preservation and internal-topic ready ordering，but are not part of the clean M3 aggregate lock；
- the organization fork exists and the published branch
  `nereus/future9-native-kafka-storage@bb7e8937c5` contains the nineteen reviewed M3 commits、two M4
  producer/transaction and ordering-test commits、three M5 DeleteRecords/retention/virtual-log commits、one
  compaction-authority commit、one stock-source isolation fix、one explicit native-storage launcher commit、one
  controller activation scheduling commit、one durable feature/control commit、one aggregate Spotless alignment commit and one
  cache-directory KRaft identity commit、one typed BookKeeper runtime/client-ownership commit、one async Object-WAL
  profile-mapping commit、one BookKeeper Object-profile/cache-root NCP2 mapping commit、one BookKeeper ledger-GC
  configuration/digest-mapping commit、one materialization-retirement configuration/digest-mapping commit and one
  stock config-fixture alignment commit、one logging-runtime isolation commit and one enabled-format
  default-BookKeeper-profile fixture completion commit、one atomic shared-storage reassignment commit and one
  local-replica-removal binding-preservation commit。The
  SSH-published remote head matches the clean working clone。Produce hands off exact owned bytes
  to a bounded per-partition FIFO executor；Fetch hands off the complete stock `readFromLog` request to a bounded event/deadline
  wave executor。CLI/KafkaRaftServer production runtime selection is executable through
  `bin/nereus-kafka-server-start.sh`；stock `ControllerServer` now owns a product-neutral metadata-publisher/runtime seam，
  and the artifact runtime deterministically schedules first activation only while locally current。The real single-node
  release-distribution provider-backed KRaft baseline and same-node fresh-JVM user/group/transaction-state cold restart pass；
  stable open-transaction forced-exit/abort recovery and the separate two-bookie `BOOKKEEPER_WAL_ONLY`
  release-distribution cold restart also pass；the in-process two-runtime Object-WAL live takeover and the two-release-process
  KRaft singleton reassignment both pass，while multi-controller、already-in-flight old append、BookKeeper-profile takeover、
  checkpoint/virtual-segment/coordinator migration and broader kill-cut final gates remain open；
- `phase9KafkaBaselineSourceLockCheck` pins the clean local Apache Kafka
  `427b409cf440f745ad6195673d3342f6bd3974d4` / `4.3.0-SNAPSHOT` probe and 10 relevant source blobs；
  `phase9M3CodecCheck` aggregates that probe、M2 deterministic predecessors and adapter codec tests，but deliberately
  does not use the `phase9M3Check` completion name。`phase9KafkaForkDevelopmentSourceLockCheck` additionally locks the
  fork branch/local+remote head/base ancestry/forty-one-commit count/organization remote/one-hundred-twenty-one log-IO/bridge/recovery/
  metadata-lifecycle/configuration/runtime-composition/retention/compaction
  plus stock-isolation/launcher/controller-runtime/feature-control blobs and markers；`phase9M3KafkaForkCheck` publishes exact
  `0.1.0-f9-dev` artifacts，verifies stock-without-artifacts compilation and runs all three fork bridge test classes plus
  seven manager-to-Partition lifecycle tests、seven topic-delta lifecycle tests、five stock Partition seam tests、two
  focused ReplicaManager tests、all seven BrokerMetadataPublisher tests、six typed-config tests、four config-validator tests、
  four product-runtime mapper tests、three KRaft context adapters、four deferred-runtime tests、two recovery-state bridge tests、
  three recovery codec tests、two exact Partition recovery-state factory tests、authoritative log-shell/factory tests、
  one borrowed-scheduler test、
  complete `KafkaConfigTest`、three stock runtime-factory tests、five adapter-backed runtime tests、two append-executor
  integration tests、stock single-node KRaft
  restart and server/core/storage format/static-analysis
  gates。The `672429d94f` aggregate rerun correctly failed because stock `Partition` referenced an artifact-only recovery class；
  `9a6ebed6d9` replaces it with stock `LeaderEpochAwareRecoveryState` and focused stock/artifact-enabled builds pass。The
  `9a6ebed6d9` gate against Nereus `main@6f2ab6f` passes 80/80 outer tasks；nested stock-without-artifacts and artifact-enabled
  Kafka builds pass 92/92 and 95/95 actionable tasks，including real provider recovery、static/format gates and stock KRaft
  restart。For `7739351b7c`，the exact-head aggregate again passes 80/80 outer tasks；nested stock-without-artifacts and
  artifact-enabled Kafka builds pass 92/92 and 95/95 actionable tasks，including the authoritative log-shell/factory test、
  146/146 scenario synchronization、real provider recovery、stock KRaft restart、Checkstyle、SpotBugs and Spotless。Because the exact
  branch is not remote, both task names deliberately retain `Development`/`Fork` partial semantics。
  At `dc8c66388a`，the updated source lock、product adapter error mapping、artifact-enabled append/fetch/fencing tests and
  exact required-acks Partition test are included in another successful exact-head aggregate：80/80 outer tasks，nested
  stock-without-artifacts 92/92 and artifact-enabled 95/95 actionable tasks，including 146/146 scenarios、real provider
  recovery、stock KRaft restart、Checkstyle、SpotBugs and Spotless。At `ee608625e4`，the bounded keyed product executor、
  stock optional seam、Kafka wrapper、ReplicaManager handoff and combined runtime drain are included in a fresh successful
  exact-head aggregate with the same 80/80 outer、92/92 stock and 95/95 artifact-enabled actionable task counts，including
  146/146 scenarios、real provider recovery、stock KRaft restart、Checkstyle、SpotBugs and Spotless。
  At `47d36a1d9f`，after the first Fetch aggregate exposed and fixed the admitted simultaneous-wakeup queue race，a fresh
  exact-head rerun passes 80/80 outer、92/92 stock-without-artifacts and 95/95 artifact-enabled actionable tasks，including
  the deterministic capacity/wakeup regression、146/146 scenarios、real provider recovery、stock KRaft restart and all
  format/static gates。

## 8. F9-M4 — Idempotence, transactions and internal topics

### Slices

1. canonical NKC1 producer/open-txn/aborted/epoch/segment/time/byte sections（all section 1–7 product models + strict codecs + full composition implemented）；
2. `NereusProducerStateManager`/txn/time/epoch facades；
3. stable post-commit failure fence/replay；
4. idempotent producer retries/epochs/sequences；
5. transaction data/markers/LSO/read isolation；
6. group/transaction internal-topic open ordering；
7. coordinator restart/failover；
8. checkpoint/trim recovery across internal topics。

### Tasks

```text
:nereus-kafka-adapter:f9ProducerStatePropertyTest
:nereus-kafka-adapter:f9TransactionIntegrationTest
Kafka fork: nereusF9ProducerIdempotenceTest
Kafka fork: nereusF9TransactionTest
Kafka fork: nereusF9CoordinatorTest
phase9M4Check
phase9M4FinalCheck --rerun-tasks
```

Final includes Kafka upstream producer/group/transaction focused suites plus real two-broker takeover。

Current partial gate（2026-07-27）：`:nereus-kafka-adapter:f9ProducerStatePropertyTest` and
`phase9M4ProducerStateCheck` cover section 1/2/7 structural invariants、frozen canonical bytes、sequence wrap and 200
deterministic randomized round trips。`KafkaAppendBatchEncoderTest` additionally proves byte-exact acceptance of
idempotent、transactional and abort-control magic-v2 batches without moving producer semantics into the adapter。
The same gate now covers section 3 bounds、carried-forward/current-empty-epoch semantics、frozen bytes、corruption cases
and another 200 deterministic randomized round trips；section 5/6 cross-section segment equivalence、checkpoint bounds、
monotonic time/offset/logical-byte samples、current-empty-segment semantics、frozen combined bytes、corruption cases and
another 200 deterministic randomized round trips；section 4 dense virtual ranges、lifecycle、roll/config history、
canonical config digest、frozen bytes、corruption cases and another 200 deterministic randomized round trips。The full
composition test freezes type 1–7 output order/bytes，accepts only outer-verified optional sections，and rejects
checkpoint/stable-end、virtual/logical-byte sets、logical bytes、time/sample segment bounds and max-timestamp mismatches。
`KafkaCanonicalCheckpointPublicationFactoryTest` further proves the exact ACTIVE binding/source/leader capture produces
the matching header and all seven sections，while append-in-flight、state-map/end and leader-epoch mismatches fail before
object I/O。
Isolated Kafka fork commit `ec7f0db991` adds stock `ProducerStateEntry.fromBatchMetadata`、in-memory
`NereusProducerStateManager`/`NereusTransactionIndex`、complete seven-section checkpoint hydration、exact COMMITTED-tail
replay and exact manager publication into `NereusUnifiedLog`。The shell test exercises stock verification guard、
transactional CLIENT append、COORDINATOR abort marker、durable-end-to-HW advancement、open-transaction LSO、
READ_COMMITTED bounds and actual-page aborted filtering；codec/manager/factory/shell plus stock restore regression total
10 passing focused tests，with Checkstyle and SpotBugs。Follow-up `032974067c` adds three regressions：the actual
ReplicaManager storage-executor closure preserves stock transaction verification guard and TV2 marker version；group and
transaction elections wait for the ready callback；and the transaction-state ready callback waits for exact recovered
storage installation。All 13 focused tests pass together。Both commits are now included in the SSH-published
`nereus/future9-native-kafka-storage@bb7e8937c5` branch。
The task deliberately does not use the `phase9M4Check` completion name；publication snapshot/object round trip、fresh
process restart/takeover index recovery、real internal-topic coordinator replay/restart/failover、upstream focused suites
and real two-broker evidence are still required before M4 completion。

## 9. F9-M5 — Retention and compaction

### Slices

1. virtual segment/config history/index checkpoint；
2. retention planner/checkpoint barrier/trim response-loss；
3. DeleteRecords mid-batch；
4. ranged compaction SPI + Kafka decoder/strategy/rewrite；
5. two-pass spill worker/NTC2 publication；
6. irreversible compaction-coverage root CAS；
7. mandatory compacted prefix + committed tail fetch；
8. internal-topic compaction and policy transitions；
9. same-view fallback/repair/GC coexistence；
10. differential stock LogCleaner oracle。

### Tasks

```text
:nereus-kafka-adapter:f9RetentionTest
:nereus-kafka-adapter:f9CompactionPropertyTest
:nereus-materialization:f9KafkaCompactionTest
:nereus-kafka-adapter:f9CompactionIntegrationTest
Kafka fork: nereusF9DeleteRecordsTest
Kafka fork: nereusF9CleanerOracleTest
Kafka fork: nereusF9InternalTopicCompactionTest
phase9M5Check
phase9M5FinalCheck --rerun-tasks
```

Current partial gate（2026-07-27）：`:nereus-kafka-adapter:f9RetentionTest` and
`phase9M5RetentionCheck` cover the Kafka-artifact-neutral `KafkaRetentionPlanner`、`KafkaRetentionCheckpointGate/Services`、
`KafkaRetentionCoordinator`、`KafkaDeleteRecordsCoordinator`、`KafkaTrimBarrier`、
`KafkaRetentionDurableTrimListener`、`DefaultKafkaPartitionMaintenance` and
`KafkaPartitionMaintenanceRuntime`。The planner freezes
the current section-4 config offset/digest，implements stock strict time and logical-segment-size prefix predicates，unions
them at the farthest HW-bounded closed-segment boundary and never selects the active segment。The barrier requires an
object-SHA-verified rooted NKC1 reference sufficient through the candidate，revalidates ACTIVE binding/leader/authority/config
and a recomputed candidate，then confirms durable trim from the stable stream head before notifying local state；an applied
trim with a lost response converges idempotently。The checkpoint gate selects newest sufficient roots、uses a closed
fallback allowlist and requires new publication at stable end；the coordinator coalesces concurrent triggers without
letting one caller cancel shared work。The services adapter composes exact-reference pinned recovery and canonical
publication/root reload，with a canonical seven-section local-file object-store round trip；the durable listener performs
response-loss-safe binding observed-logStart CAS before calling the exact local updater。Product commits `3eb6b63` and
`57dcf35` compose checkpointed per-partition maintenance with a bounded、non-overlapping、internal-topic-first periodic
owner and drain it through the runtime background-service lifecycle。Kafka fork commits `4c060aec89` and `feabf6c686`
provide stock DeleteRecords invocation、partition-lock capture、same-log local updater and owned writable-partition
enumeration；`378e9f8967` supplies the live virtual segment/config/index facts consumed by the planner。The focused
retention suite and fork UnifiedLog/Partition/config regressions pass。This task deliberately does not use the
`phase9M5Check` completion name：delete-response-loss/multi-broker takeover/chaos evidence、compaction
full stock-cleaner differential oracle and aggregate gates remain required。Compaction production fork
registration/concrete authority capture is now implemented separately by product `e18bf36` and fork `58342d9dca`。

The product-side DeleteRecords slice now accepts only Kafka-normalized non-negative offsets，rechecks delete policy and
the frozen HW，returns the current durable low watermark without I/O for already-deleted requests，and otherwise routes the
exact (including mid-batch) logical offset through the same rooted-checkpoint/revalidation/durable-response-loss barrier。
Its revalidation freezes KRaft config/leader authority but deliberately does not round or recompute a retention segment
candidate。Fork commit `4c060aec89` now supplies stock policy/leader/range validation、`-1 -> HW` conversion、
partition-lock capture and durable-result publication into the same `NereusUnifiedLog`。This closes the deterministic
fork boundary for slice 3；real provider batch-boundary/response-loss/restart and delayed-Fetch wake-up gates remain open。

`:nereus-kafka-adapter:f9CompactionPropertyTest` and `phase9M5CompactionCoreCheck` now add the first slice-4 partial
gate。`nereus-materialization` owns immutable ranged decode/rewrite records rather than reusing the F4 one-entry/one-record
contract；`KafkaCompactionPlanner` selects only the consecutive closed virtual-segment prefix at/below LSO and outside the
strict stock minimum-lag boundary，resuming at mandatory coverage end while freezing policy/coverage facts。The adapter
`KafkaTopicCompactionCodecV1` strictly decodes one exact magic-v2 batch，emits every KCK2-tagged logical record with source
SHA/base/index，and rewrites a selected value、transactional record or commit/abort marker to one round-trip-verified batch
at its original absolute offset。The codec freezes its message-format digest and preserves compression unless the plan
explicitly allows an uncompressed fallback。`KafkaCompactionStrategyV1` consumes full-horizon collector facts and decides
latest/superseded keyed data、unique null-key retention、committed/aborted/open transactions and full-scan-proven
tombstone/control-marker delete horizons，including first-pass horizon assignment and the later exact `now == horizon`
boundary。`KafkaCompactionPassOneCollector`/`KafkaCompactionTwoPassExecutor` now provide a bounded dense two-pass reference：
aborted/open data cannot become key winners，full/output fact SHA must reproduce，and retained records are rewritten and
mapped to ordered non-empty NTC2 rows under explicit budgets。`ExactSourceSetVerifier` also freezes/validates the exact
COMMITTED target identity and rejects output sources that are not the decision-set prefix。
`KafkaCompactionWriteRequestFactory` now binds the verified result range/source-set SHA/accounting and fixed
strategy/key/rewrite/message-format identities into the existing strict NTC2 writer request，rejecting a task/result coverage
mismatch before writer admission。`ExactSourceSetCodecV1` and `KafkaCompactionPlanCodecV1Test` additionally freeze a
byte-stable EXS1/KCP1 restart image and reject corrupt target/task/transaction/compatibility facts。
`KafkaCompactionPlanRecordMapper` plus the separate `KafkaCompactionPlanMetadataStore` bind the KCP1 bytes/SHA/ranges to an
immutable partition child；in-memory and real-Oxia contracts cover idempotent create、restart read and exact-version delete。
`KafkaCompactionPlanCoordinator` now closes the non-atomic KCP1/task workflow：authority is checked before plan create and
again inside `MaterializationTaskStore` immediately before task mutation，after an exact task-addressed KCP1 reread；restart
recovers KCP1 by materialization task ID and cross-validates the full task。`DefaultCommittedSourceSetResolver` now bounds and
tiles the current COMMITTED generation index，rereads every selected generation identity and rechecks retained
stream/registration authority；`KafkaCompactionSourceResolver` derives the exact output prefix/task and composes that proof
into the task mutation guard。`KafkaCompactionBatchSource` now opens independent backpressured decision/output streams from
the recovered KCP1 and verifies the complete frozen source identity/accounting without fallback。
`KafkaCompactionWinnerIndex` now replaces the production pass-one winner map with owner-only KCSR V1 sorted runs on the
shared `StagingFileManager` budget。Runs are KCK2-key ordered、whole-file SHA-256 verified、fan-in-16 merged and reduced to
an output-coverage `BitSet`；success、corruption、decode failure and cancellation close/delete all runs and release permits。
Restart deliberately discards scratch runs and deterministically recomputes them from recovered KCP1 exact sources。
`KafkaCompactionWinnerIndexTest` covers spill/no-spill and two-run restart equivalence、newer decision-tail suppression、
same-length sealed-file corruption and cancellation cleanup。`KafkaCompactionStreamingExecutor` now runs both recovered
streams under one-batch demand，reproves pass-one/output facts and writes survivors to a whole-file-SHA-verified KCRS V1
spool；`KafkaCompactionParquetPublisher` demand-replays that single-use spool into a staged NTC2 object only after exact
terminal accounting is known。Product tests compare the staged content SHA with the reference executor，perform local
upload plus strict NTC2 verification and prove cancellation releases an existing KCSR run/KCRS permits。
`KafkaCompactionTerminalRetirer` additionally requires exact terminal task/plan roots and a stable no-admission authority
guard，then conditionally deletes task first and KCP1 second with bounded exact-reload convergence for both response-loss
cuts。The F4 policy/output/index boundary now carries a distinct NTC2 policy identity、requires task/output physical-format
agreement and preserves valid zero-survivor generations while keeping COMMITTED generations non-empty。
`KafkaCompactionPublicationCoordinator` then performs guarded if-absent upload、HEAD/full NTC2 verification、durable
OUTPUT_READY、generic Generation commit and canonical generation-set coverage CAS in that order；bounded exact reload
handles PUT/CAS response loss，while a changed coverage basis leaves the committed generation non-mandatory。Its
durable-output recovery entry accepts only exact `OUTPUT_READY/PUBLISHING/PUBLISHED` task/output/KCP1 tuples，revalidates
binding/authority and resumes idempotent Generation publication plus activation without a staging file；an already activated
canonical digest/range/policy/epoch converges without another coverage CAS。
`KafkaActivatedGenerationSetResolver` now discovers the unique bounded gap-free path matching the ACTIVE binding digest and
passes exact generation/index identities to generation-constrained core reads；the Object-WAL runtime wires the corresponding
NTC2 reader。The product runtime therefore reloads binding coverage per Fetch and executes a sparse `TOPIC_COMPACTED`
prefix followed by a `COMMITTED` tail；mandatory-view failure has no cross-view fallback。
`KafkaCompactionPlanMetadataStore` additionally exposes partition-scoped bounded continuation，and
`KafkaCompactionPlanOrphanScanner` retires only grace-expired task-absent exact plans under a stable no-admission guard，
including exact-absence response-loss recovery。`KafkaCompactionScheduler` provides a borrowed-resource、non-overlapping
startup/fixed-delay owner with at most one active and one coalesced pending pass，trigger priority aggregation and
cancellation-isolated callers。`KafkaCompactionPartitionPass` now supplies the complete single-partition
planner→source→KCP1/task→claim/heartbeat→executor→publisher→coverage→retirer composition。It recovers task-rooted KCP1 before
fresh admission，routes every durable lifecycle，requeues skew-safe expired claims by exact CAS，reconstructs previous
activation from the frozen plan and coalesces concurrent callers。`KafkaCompactionPartitionPassTest` runs that workflow through
actual Parquet NTC2、local-file object storage and durable in-memory task/generation/binding roots，ending with activated
coverage and both task/KCP1 roots retired。`KafkaCompactionRuntime` now consumes a bounded immutable fork-owned snapshot，
rejects duplicate durable IDs/over-limit results，orders internal topics first，rechecks process-current writable leader
epoch before launch，bounds cross-partition concurrency，attempts all accepted work and drains it before partition-manager
shutdown。`KafkaCompactionProductionRuntimeFactory` now composes that runtime with direct-stream exact-source resolution、
per-plan stream-bound readers、shared staging/spill、streaming NTC2 publication/full verification、activation- and
partition-authority-fenced Generation publication、coverage activation and terminal retirement。
`NereusKafkaObjectWalRuntimeFactory` conditionally owns this graph from
`NereusKafkaObjectWalActivationContext.compaction`，late-binds it to the same product partition manager through
`KafkaRuntimeBackgroundServiceFactory`，and closes staging only after accepted compaction work drains。Production fork
commit `58342d9dca` now maps the bounded config and supplies current-leader registration plus
partition-lock/KRaft/local-log canonical and stock marker capture。Real-provider fresh-process gates and the full cleaner
differential oracle remain pending。This closes the
deterministic adapter no-resurrection/write-composition boundary but does not yet claim end-to-end client compaction
visibility。

The Kafka-native Generation authority slice now adds canonical projection-free F4 stream registration、a distinct
`LiveStreamSubject` proof、an ACTIVE/readiness-backed Kafka activation guard and a caller-supplied partition-authority fence
that is revalidated immediately before the Generation `COMMITTED` CAS。The old constructors remain
`PROJECTION_REQUIRED`，so existing Pulsar/F4 publication and source-resolution contracts are unchanged。
`KafkaPartitionLifecycleCoordinator` installs/verifies this registration before an ACTIVE binding is returned。
Production Object-WAL runtime construction now assembles these pieces with the concrete compaction pass；the remaining seam
is real-provider process restart/takeover plus the broad stock-cleaner differential gate；fork-owned partition registration
and partition-lock/KRaft/local-log capture are implemented。

No-resurrection is a release blocker，including policy compact→delete、missing newest NTC2 and restart cuts。

## 10. F9-M6 — Activation/controller/operations

### Slices

1. ConfigDef/typed config/secrets validation；
2. controller KRaft feature/RF1/ISR/reassignment enforcement；
3. capability/readiness/activation codecs/stores/coordinator；
4. async metadata-delta/internal coordinator ordering；
5. startup/health/admission/shutdown ownership；
6. scanners/schedulers/backpressure priorities；
7. metrics/logs/admin diagnostics/runbooks；
8. empty-cluster first activation and rolling compatible maintenance；
9. incompatible old/disabled broker exclusion。

Current partial implementation：`NereusKafkaRuntime`、`DrainReason`、`KafkaStorageAdmissionState`、immutable
`KafkaStorageHealth` and the atomic admission gate are present in the adapter。`KafkaStorageAdmissionTest` proves readiness
recovery before drain、stable pre-I/O rejection、one-winner concurrent drain and irreversible drain/close against late
callbacks。`DefaultNereusKafkaRuntime` and `KafkaRuntimeResources` now make start/drain operation ownership、timeout-view
isolation、late-start fencing、manager-first close、exact OWNED/BORROWED identity、reverse-order close and attempt-all failure
aggregation executable。`NereusKafkaRuntimeConfiguration`、`NereusKafkaRuntimeDependencies` and
`NereusKafkaRuntimeFactory` now make the post-provider product graph executable：one binding keyspace/lifecycle、one
authority/recovery opener、one partition manager、one codec pair and one process runtime，with fixed/extra provider resources
entered into the exact close ledger；the Object-WAL creator now additionally owns checkpoint read pins and concrete
checkpoint/COMMITTED replay composition。Its optional BookKeeper slice borrows the client、requires exact F1-BK ACTIVE
publication/readiness、installs the BookKeeper appender/reader/physical-reference/profile resolver in the same graph and skips
Object materialization registration for `BOOKKEEPER_WAL_ONLY`，but installs direct NCP2 registration/materialization for
both BookKeeper object-producing profiles。Kafka scheduler/clock and the fork recovery-state factory
remain borrowed。The BookKeeper client is borrowed by the product graph but owned by the fork's outer
`NereusKafkaOwnedProviderRuntime`，which closes the product graph before the client and remains idempotent under repeated close。
The local Kafka fork also
has an explicitly injected generic `BrokerStorageRuntimeFactory` with
stock restart coverage and exact BrokerServer start/ready/metadata/drain/close ordering。`NereusBrokerStorageRuntimeFactory`
and `NereusBrokerStorageRuntime` now add typed runtime/scan-limit creators、failure rollback、one exact ReplicaManager binding、
drain-reason mapping and synchronous lookup revocation without a duplicate manager owner。This is deterministic partial evidence
for KF-OPS-012/014/017。`KafkaStorageProtocolActivationRecord`、`KafkaBrokerCapabilityRecord`、
`KafkaStorageReadinessRecord` and their closed codecs now freeze the V1 protocol tuple/profile sets/digests/broker identities；
`KafkaStorageActivationMetadataStore` supplies exact-key create/CAS、ACTIVE monotonicity、immutable capability heartbeat、
readiness epoch/metadata-offset monotonicity and applied-but-response-lost recovery。Deterministic contracts and a real-Oxia
close/reconnect gate pass。`KafkaBrokerCapabilitySpecification`/`Publisher`、the frozen canonical compatibility digest and
`KafkaStorageActivationVerifier` now cover broker-epoch publication/renewal and exact KRaft↔ACTIVE↔readiness↔all-capability
admission；heartbeat owns only its scheduled future and a failed CAS permanently invokes the fencing callback。
`KafkaStorageFirstActivationCoordinator` now executes empty-image proof → capability aggregation → readiness → PREPARED → second
empty-image/capability proof → ACTIVE，resumes PREPARED without mutation and recovers a compatible concurrent winner。ACTIVE
re-entry does not reapply the first-activation emptiness rule；it reloads capabilities for the current KRaft broker epoch set、
requires the compatibility digest to remain equal and CAS-refreshes readiness while leaving the one-way ACTIVE record unchanged。
The Object-WAL production creator now wraps every fork
snapshot with a 64-shard exact binding-registry existence scan，so first activation cannot accept a KRaft-empty cluster that
already has durable Nereus Kafka binding history。The fork now maps its typed config plus exact broker epoch into provider-neutral
Object-WAL/Oxia/StreamStorage/capability/ListOffsets configurations with deterministic compatibility/provider/code digests and
pre-I/O profile/provider rejection。The fork now also performs no-I/O factory assembly，waits for the exact registered epoch，
constructs the activation-backed S3/Object-WAL runtime from borrowed scheduler/clock，captures one KRaft/local-log snapshot and
one-time binds a recovery-state factory bridge to the exact ReplicaManager。Concrete M3 recovery execution is wired across the
product/fork boundary。Fork `faaffc8a75` isolates maintenance hooks behind stock interfaces and proves artifact-free stock
main/test compilation；fork `3bd92c7244` adds an executable native-storage launcher that passes a fresh production broker
factory through the shared stock `Kafka.run` lifecycle into `KafkaRaftServer`；fork `9773c8f817` adds the parallel
`ControllerStorageRuntimeFactory` path and a `MetadataPublisher`-driven activation scheduler。Focused tests prove
current-controller-only execution、one in-flight/coalesced callbacks、retriable-only retry、leadership-loss cancellation、
one durable fault per controller epoch and owned close。The same `d23dc5c787` head registers explicit-only
`nereus.storage.version`、advertises it only from enabled broker/controller processes、supports dedicated controllers、
requires explicit enabled formatting、waits for finalized level 1 before activation and enforces single-copy controller
mutations。Fork `ecde6964c5` additionally prepares the exact authoritative cache root with a validated KRaft V1
`meta.properties` and non-reserved directory ID before `LogManager` loads directory identities。
`phase9M6KafkaFeatureCheck --rerun-tasks` locks that head and runs the isolated server-common、server、metadata and
artifact-enabled core feature suites。`phase9M6CheckpointQuarantineCheck --rerun-tasks` additionally
composes 146/146 manifest validation、immutable store/codec contracts、recovery/retention ordering、production Object-WAL
resource ownership and real-Oxia close/reconnect lookup。`phase9M6KafkaProcessCheck --rerun-tasks` publishes the exact
`0.1.0-f9-dev` artifacts、builds `:core:releaseTarGz` from the source-locked fork and runs the actual
`bin/nereus-kafka-server-start.sh` combined-node process against four-shard Oxia and pinned LocalStack S3。It requires
explicit `nereus.storage.version=1` format、broker/controller registration and activation、Admin single-copy topic creation、
acks=all Produce offset 0、one committed transaction at data/marker offsets 1/2、byte-exact read-committed consumer Fetch、
one real group rebalance plus committed offset 2、earliest=0/latest=3 ListOffsets、at least one S3 object and normal SIGTERM
shutdown completion；then a fresh JVM reuses the exact KRaft identity/directories，publishes a higher broker epoch，forces
the ACTIVE controller path to CAS-refresh readiness and concurrently recovers the user partition、`__consumer_offsets` and
`__transaction_state`。The second process must load group offset 2，reuse the same transactional ID for data/marker offsets
3/4，resume the group at visible offset 3、commit offset 4，verify earliest=0/latest=5 and complete a second normal shutdown。
It then starts a third JVM，stably appends an open-transaction data batch at offset 5 and forcibly terminates the broker。
A fourth JVM must recover the internal-topic transaction state；reinitializing that same transactional ID must first resolve
the interrupted transaction with ABORT marker 6，then commit data/marker offsets 7/8。A read-committed consumer and the
existing group both start at visible offset 7，the group commits 8，and latest is 9。
The same aggregate also runs `f9BookKeeperWalOnlyProcessIntegrationTest`。It starts stock ZooKeeper
long-hierarchical metadata plus two real bookies，keeps F1-BK activation/readiness in Oxia，formats the same durable Kafka
feature and starts the real release launcher with `BOOKKEEPER_WAL_ONLY`。The first JVM must Admin-create a topic、append and
fetch offset 0、verify earliest=0/latest=1 and shut down normally；a fresh JVM must recover that batch、append/fetch offset 1、
verify earliest=0/latest=2 and shut down normally。The Java 21 `java.io` module opening is confined to the in-process
BookKeeper fixture and is not added to the Kafka runtime command。
The aggregate additionally runs `f9ObjectWalAsyncObjectProcessIntegrationTest`。It selects the exact
`OBJECT_WAL_ASYNC_OBJECT` default over the installed Object provider，writes/fetches offset 0 in the first real JVM，
normally shuts down，then requires a fresh JVM to recover offset 0、append/fetch offset 1 and verify
earliest=0/latest=2 over the same Oxia/S3 state。
The recovery coordinator retries retriable page-read failures at the exact cursor with 10–250 ms exponential backoff under
the original deadline；the deterministic regression requires two reads but exactly one publication。The root build now
recognizes M6 feature/process and direct
process-test task names as F9 development gates，so the published coordinate cannot silently remain an older
`0.1.0-f9-dev` artifact。The provider-level Object-WAL old-token fence and release-process post-handoff recovery/continuation
are covered；durable already-in-flight append cuts、multi-controller、priority budgets、BookKeeper-profile takeover、
coordinator migration and checkpoint/virtual-segment cuts remain open。

### Tasks

```text
:nereus-kafka-adapter:f9ConfigTest
:nereus-kafka-adapter:f9ActivationTest
:nereus-kafka-adapter:f9ActivationOxiaIntegrationTest
:nereus-metadata-oxia:f9ActivationOxiaIntegrationTest
phase9M6ActivationMetadataCheck
phase9M6KafkaFeatureCheck
phase9M6CheckpointQuarantineCheck
:nereus-kafka-adapter:f9MultiBrokerTakeoverProcessIntegrationTest
phase9M6KafkaProcessCheck
phase9M6KafkaBookKeeperProcessCheck
Kafka fork: nereusF9ControllerTest
Kafka fork: nereusF9BrokerLifecycleTest
Kafka fork: nereusF9MetadataPublisherTest
Kafka fork: nereusF9OperationsTest
phase9M6Check
phase9M6FinalCheck --rerun-tasks
```

Final is three-broker KRaft with controller failover、rolling restart、capability mismatch、all selected profiles and clean
shutdown/kill cuts。

## 11. F9-M7 — Scale, chaos and compatibility aggregate

### Required dimensions

- 16,384 partition bindings across 64 shards；hot-shard/full-scan restart；
- 10,000 active partitions/broker target or documented environment-adjusted equivalent with invariant extrapolation；
- multi-million record ranged batches/compaction sources；
- 1,000 concurrent Produce/Fetch operations under byte/queue budgets；
- 128-source/large NCP2/NTC2 tasks；
- broker/process/provider/network response-loss matrix；
- Kafka client compatibility across supported protocol versions；
- upstream focused core/group/transaction/admin suites；
- mixed Nereus storage profiles，not mixed local/Nereus topics；
- full predecessor aggregate executed，no `UP-TO-DATE`/skipped evidence for release run。

### Tasks

```text
phase9CompatibilityCheck
phase9ScaleCheck
phase9ChaosCheck
phase9M7Check
phase9FinalCheck --rerun-tasks
```

`phase9FinalCheck` depends on all M1–M7 final gates and emits one deterministic report mapping every scenario ID to test
class/method/result/artifact hash/environment。

## 12. Gate implementation in Gradle

Root target additions：

- `KafkaCheckoutGateService` maxParallelUsages=1；
- reuse `DockerIntegrationGateService` for real Oxia/BK/Object/KRaft ports/resources；
- source-required selector only for `phase9*`/adapter fork tasks；
- `publishPhase9DevelopmentArtifacts` to isolated build repository；
- exact Kafka checkout/version/commit/clean-tree check before Exec；
- environment variables passed explicitly，no developer Maven local fallback；
- task output captures commit IDs/test counts and rejects zero-executed/skipped mandatory tests。

Never make ordinary Phase 1–4/Pulsar checks require a Kafka checkout。

## 13. Test class naming and traceability

Each scenario test method carries stable ID in name or annotation：

```java
@F9Scenario("KF-APP-007")
void responseLossAfterHeadCasFencesUntilExactRecovery() { ... }
```

Aggregator scans compiled test descriptors/results and fails on：missing scenario、duplicate ID、wrong milestone、skipped test、
mock-only evidence for real-required row、or source hash mismatch。Document 08 is generated/checked against a machine-readable
manifest committed beside tests only when implementation starts；the design Markdown remains reviewed source intent。

## 14. Failure-injection infrastructure

Named hooks，never sleeps as correctness synchronization：

```text
before/after primary write
before/after protection
before/after stream-head CAS and response
before/after authority session CAS
before/after binding root CAS
before/after checkpoint PUT/HEAD/verify/root CAS
before/after NTC2 generation commit/coverage CAS
before/after trim CAS/response
before/after topic delete seal/delete/root CAS
before/after Kafka local derived-state update/publication
```

Hooks support pause、fail-known、lose-response、process-kill。Tests coordinate latches/events and assert durable state after fresh
runtime，not internal future timing only。

## 15. Performance gate design

Correctness gates precede claims。M7 records，without hard-coding unsupported marketing numbers：

- per-profile Produce throughput/p50/p95/p99 and stable acknowledgement latency；
- Fetch throughput/latency for committed/compacted/cache-cold；
- recovery time vs checkpoint lag；
- memory/queue/executor utilization；
- compaction throughput/spill amplification/object IO；
- broker takeover time；
- CPU/allocation profile and thread/blocking audit。

Release threshold is set against an approved hardware/provider baseline and checked into the evidence manifest；no threshold is
inferred from AutoMQ marketing or a mock benchmark。

## 16. Mandatory review stops

| Stop | Review focus | Required approvers/domain |
| --- | --- | --- |
| M0 | architecture/truth/scope/source | Nereus storage + Kafka |
| M1 | public compatibility/durable bytes | API + formats + Pulsar regression |
| M2 | metadata/session/checkpoint | Oxia + recovery/security |
| M3 | fork ordering/Produce/Fetch | Kafka log/server + storage |
| M4 | idempotence/transactions/coordinators | Kafka transaction/group |
| M5 | retention/compaction/no-resurrection | Kafka cleaner + F4/GC |
| M6 | controller/activation/operations | KRaft + SRE/security |
| M7 | aggregate evidence/release | all owners |

Review stop is not satisfied by author self-approval or passing unit tests alone。

## 17. Definition of done

F9 can change from `Designed/In progress` to `Implemented/final-gated` only when：

- all documents match final code/durable goldens；
- every scenario row has executable passing evidence at required environment；
- exact Nereus/Kafka source commits and clean trees recorded；
- disabled stock Kafka compatibility passes；
- full upstream focused compatibility suites pass；
- all five Nereus profiles claimed are actually activated/tested or unsupported ones removed from claim；
- no transaction/group/compaction/retention caveat contradicts advertised compatibility；
- multi-broker authority/unknown-outcome/process-kill gates pass；
- old broker/reader exclusion and upgrade procedure pass；
- metrics/alerts/runbooks/security review complete；
- release aggregate runs fresh with no mandatory skipped/up-to-date tasks。

Design document completion alone satisfies none of the implementation bullets。
