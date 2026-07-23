# 07 — Implementation Plan and Gates

> 状态：F9-M1/M2/M3 implementation slices complete；F9-M4 producer/open/aborted canonical state and strict V1 codec partial slice implemented；M2 ordinary/direct real-service gates pass；inherited final gate blocked by local Pulsar source drift
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

- `phase9SourceLockCheck` locks AutoMQ `1c648d...` / `3.9.0-SNAPSHOT` and 23 current Nereus ranged/head-foundation source blobs；
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
- partition tests cover stable-only LEO/HW/LSO publication、acks 0/1/-1 invariant、same-partition serialization、
  speculative gap rejection、known-not-committed retry、uncertain/result-mismatch fencing、containing-entry/upper-bound/
  first-overflow Fetch and resign drain；
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
  checkpoint candidate、provider-backed BrokerServer activation and real native-storage KRaft baseline remain open；
- `NereusKafkaObjectWalRuntimeConfigurationTest` freezes the first concrete provider graph to exactly
  `OBJECT_WAL_SYNC_OBJECT`、matched cluster/writer/session/scan limits and disabled legacy auto-session；
  `NereusKafkaObjectWalRuntimeFactoryTest` proves activation scope is rejected before provider IO，and a checked
  provider-construction failure is propagated with every resource registered before that cut closed；
  `f9M3ProviderIntegrationTest` passes against real Oxia plus the filesystem ObjectStore provider and covers deterministic
  ACTIVE/readiness seeding、runtime capability resume/verification、binding、authority acquire/recovery、leader open、one byte-exact
  stable Produce、committed Fetch and owned provider shutdown。
  This is provider-backed adapter evidence，not yet a Kafka BrokerServer/KRaft or production S3 gate；
- `NereusKafkaObjectWalRuntimeFactory` now owns durable checkpoint read pins、reader/verifier/recovery coordinator、
  configured `recoveryChunkRecords/recoveryChunkBytes` paging and `DefaultKafkaPartitionRecoveryLauncher`。
  `DefaultKafkaRecoveryBatchSourceTest` proves exact bounded COMMITTED/EXACT_START pages and fail-closed empty/non-Kafka
  results；`KafkaCheckpointPublicationRecoveryIntegrationTest` proves multi-page replay。The installed checkpoint failure
  observer is currently no-op，so durable quarantine/audit is still open；
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
- M3 rejects idempotent/transaction/control input until M4 owns producer/transaction state；
- this is not M3 completion：the organization fork exists and local branch
  `nereus/future9-native-kafka-storage@47d36a1d9f` contains nineteen reviewed commits and the seventy-three-file
  log-IO/bridge/request/recovery/metadata-lifecycle/configuration/runtime-composition seam，but the current GitHub credential has
  read-only permission，so the branch is not pushed and KF-SRC-004 remains incomplete。Produce hands off exact owned bytes
  to a bounded per-partition FIFO executor；Fetch hands off the complete stock `readFromLog` request to a bounded event/deadline
  wave executor。CLI/KafkaRaftServer production runtime selection and the real KRaft final gate remain open；
- `phase9KafkaBaselineSourceLockCheck` pins the clean local Apache Kafka
  `427b409cf440f745ad6195673d3342f6bd3974d4` / `4.3.0-SNAPSHOT` probe and 10 relevant source blobs；
  `phase9M3CodecCheck` aggregates that probe、M2 deterministic predecessors and adapter codec tests，but deliberately
  does not use the `phase9M3Check` completion name。`phase9KafkaForkDevelopmentSourceLockCheck` additionally locks the local
  fork branch/head/base ancestry/nineteen-commit count/remotes/seventy-three log-IO/bridge/recovery/metadata-lifecycle/configuration/runtime-composition
  blobs/markers；`phase9M3KafkaForkCheck` publishes exact
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

1. canonical NKC1 producer/open-txn/aborted/epoch/segment/time/byte sections（section 1/2/7 product model + strict codec implemented；section 3–6 pending）；
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

Current partial gate（2026-07-24）：`:nereus-kafka-adapter:f9ProducerStatePropertyTest` and
`phase9M4ProducerStateCheck` cover section 1/2/7 structural invariants、frozen canonical bytes、sequence wrap and 200
deterministic randomized round trips。The task deliberately does not use the `phase9M4Check` completion name；stock
`ProducerStateManager` import/replay、idempotent/transaction request paths、LSO/aborted filtering and internal-topic
coordinator ordering are still required before M4 completion。

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
checkpoint/COMMITTED replay composition，while Kafka scheduler/clock and the fork recovery-state factory remain borrowed。The local Kafka fork also
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
empty-image/capability proof → ACTIVE，resumes PREPARED without mutation，recovers a compatible concurrent winner and treats ACTIVE
as idempotent without reapplying the first-activation emptiness rule。The Object-WAL production creator now wraps every fork
snapshot with a 64-shard exact binding-registry existence scan，so first activation cannot accept a KRaft-empty cluster that
already has durable Nereus Kafka binding history。The fork now maps its typed config plus exact broker epoch into provider-neutral
Object-WAL/Oxia/StreamStorage/capability/ListOffsets configurations with deterministic compatibility/provider/code digests and
pre-I/O profile/provider rejection。The fork now also performs no-I/O factory assembly，waits for the exact registered epoch，
constructs the activation-backed S3/Object-WAL runtime from borrowed scheduler/clock，captures one KRaft/local-log snapshot and
one-time binds a recovery-state factory bridge to the exact ReplicaManager。Concrete M3 recovery execution is wired across the
product/fork boundary；Kafka controller activation scheduling、CLI/KafkaRaftServer log selection、durable checkpoint-failure
quarantine/audit、priority budgets
and real native-storage shutdown/process cuts remain open。

### Tasks

```text
:nereus-kafka-adapter:f9ConfigTest
:nereus-kafka-adapter:f9ActivationTest
:nereus-kafka-adapter:f9ActivationOxiaIntegrationTest
:nereus-metadata-oxia:f9ActivationOxiaIntegrationTest
phase9M6ActivationMetadataCheck
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
