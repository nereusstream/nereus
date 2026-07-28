# 03 — Kafka Fork, Log and Broker Integration

> 状态：Implementation in progress；Nereus-side M3 codec/ListOffsets/checkpoint-pinned paged recovery、Kafka-fork record/async-result bridges、M4 producer/transaction state、M5 retention/compaction slices and M6 runtime/config/lifecycle seams are implemented；stock-source isolation、显式 `NereusKafka` launcher、controller-leader-only activation、durable feature/format、cache-root KRaft identity、ACTIVE broker-epoch readiness refresh、complete BookKeeper typed runtime/client ownership、five-profile mapping 与 product-side durable checkpoint quarantine 已实现；real release-distribution combined-node Oxia/S3 user/internal-topic/transaction recovery、Object async cold restart、BookKeeper WAL-only/async/sync cold-restart、BookKeeper three-profile two-process post-handoff、BookKeeper provider-applied cut、real-Oxia two-runtime Object-WAL live takeover、three-voter ACTIVE controller failover and the complete six-way readiness/PREPARED/ACTIVE store-publication gates 均通过；remaining activation process cuts and extended kill/chaos gates remain open
> 2026-07-29 状态增量（覆盖上一行末尾的旧 open-item 描述）：fork `df238bb387` 与 product `f9MultiBrokerTakeoverProcessIntegrationTest` 已通过 two-release-process Object-WAL/KRaft singleton live reassignment；product `f9InFlightTakeoverProcessIntegrationTest` 以第三个 controller/broker JVM、Toxiproxy、`jcmd` 栈采样和 `SIGSTOP/SIGCONT` 闭合 Object-WAL already-dispatched old append；`f9BookKeeperProfileTakeoverProcessIntegrationTest` 闭合三 BookKeeper profile 的 post-handoff P-tier matrix；`f9BookKeeperInFlightTakeoverProcessIntegrationTest` further proves a real Bookie-acked、Oxia-`WRITING` append is abandoned/sealed by the new leader and cannot publish after the old JVM resumes；`f9MultiControllerFailoverProcessIntegrationTest` further proves a three-voter ACTIVE controller kill/election/reconciliation and native IO continuity；`f9ActivationCutFailoverProcessIntegrationTest` further kills the exact active controller before or after real readiness create、PREPARED create and ACTIVE CAS and proves higher-epoch recovery with broker `[4]` native IO；`f9ActivationTransportRecoveryProcessIntegrationTest` further resets the actual Oxia transport and proves same-controller-epoch retry to ACTIVE plus native IO；remaining gaps are transaction/internal-topic coordinator migration、checkpoint/virtual-segment cuts、initial empty-cluster snapshot/proof and capability-aggregation process cuts and broader kill/response-loss chaos
> 参考：AutoMQ Kafka fork `1c648d84819d5c3fef2af585f02149c397584870`
> 初始原则：保留 stock Kafka validation/coordinator/protocol，替换 durable partition-log owner

## 1. Integration shape

F9 计划维护一个与选定 Apache Kafka release 对齐的窄 fork。AutoMQ 证明 `UnifiedLog → LocalLog → LogSegment`
是可行 seam；F9 不复制 `s3stream`、`Elastic*` 或 AutoMQ enterprise/API extensions，而是新增 `Nereus*`
classes，并把 provider/Oxia 细节封装在 `nereus-kafka-adapter`。

```text
Kafka stock protocol/controller/coordinators
  -> ReplicaManager
       -> optional BrokerStorageAppendExecutor
       -> request-wide byte validation + exact owned MemoryRecords capture
       -> per-TopicIdPartition FIFO / cross-partition bounded workers
       -> optional BrokerStorageFetchExecutor
       -> whole ordered stock readFromLog waves
       -> bounded read worker + event/deadline coalescing + separate callback worker
  -> Partition
  -> RequiredAcksAwareAppend (optional stock-package seam)
  -> NereusUnifiedLog (exact recovery/storage publication + synchronous worker-side IO bridge)
  -> stock validation/offset assignment
  -> NereusLocalLog stable-append callback / NereusUnifiedLog.read
  -> KafkaPartitionStorage
  -> StreamStorage

Ephemeral cache-only stock state machine
  -> NereusLocalLog
  -> one or more synthetic virtual-segment shells with logical size/index facts
  -> local segment files remain byte-empty
  -> never durable truth, never restart recovery input

Request handler
  -> returns after bounded Produce/Fetch admission；never waits on Nereus storage IO
```

初版不需要 `NereusKafkaApis`：stock `KafkaApis` 已使用 callback-based ReplicaManager API，Produce/Fetch 的
异步存储调度通过 stock-owned optional executor seam 注入 `ReplicaManager`，不新增 manager subclass。只有未来需要协议
扩展时才新增 API subclass。

## 2. Planned Kafka-side classes

目标 package 均位于 Kafka fork，不放入 Nereus core：

| Class | Base/role | Correctness responsibility |
| --- | --- | --- |
| `kafka.log.UnifiedLogFactory` | stock per-broker construction seam | exact local fallback plus injected authoritative factory |
| `kafka.log.nereus.NereusUnifiedLogFactory` | authoritative factory | select dedicated cache root；disable local scan/maintenance；require topicId |
| `kafka.log.nereus.NereusUnifiedLog` | extends `UnifiedLog` | exact publication；stock-validated stable append；bounded adapter read/Fetch assembly |
| `kafka.log.nereus.NereusLocalLog` | extends `LocalLog` | stable-append callback + ephemeral LEO/segment state；never writes record bytes |
| `org.apache.kafka.storage.internals.log.BrokerStorageManagedLog` | stock inert maintenance seam | metadata-offset update、owned-leader enumeration 与 DeleteRecords hook，不链接 Nereus artifact |
| `org.apache.kafka.storage.internals.log.PartitionLeaderAuthority` | stock inert partition-lock seam | generic capture/publish closure 必须在 `leaderIsrUpdateLock` 内执行 |
| `org.apache.kafka.storage.internals.log.RequiredAcksAwareAppend` | optional stock inert seam | preserve exact protocol required-acks without changing ordinary UnifiedLog |
| `kafka.log.nereus.NereusLogSegment` | extends `LogSegment` | virtual roll/size/index facade；no durable file |
| `kafka.log.nereus.NereusCanonicalLogState` | partition-lock-owned state | stable-only virtual roll/config history/time-index/logical-position state；checkpoint + committed-tail rebuild |
| `kafka.log.nereus.NereusLogRecords` | records facade | exact MemoryRecords encode/read and owned buffers |
| `kafka.log.nereus.NereusProducerStateManager` | extends `ProducerStateManager` | in-memory stock semantics + checkpoint bridge |
| `org.apache.kafka.storage.internals.log.ProducerStateEntry.fromBatchMetadata` | stock inert factory seam | exact five-batch queue import without overwriting marker-updated lastTimestamp |
| `kafka.log.nereus.NereusTimeIndex` | Kafka `TimeIndex` facade | derived timestamp lookup/checkpoint state |
| `kafka.log.nereus.NereusTransactionIndex` | Kafka `TransactionIndex` facade | derived aborted-txn lookup/checkpoint state |
| `kafka.log.nereus.NereusLeaderEpochCache` | epoch cache facade/adapter | derived epoch ranges；no local checkpoint truth |
| `kafka.server.storage.BrokerStorageAppendExecutor` | optional stock-owned seam | request prevalidation、owned submit、drain contract without Nereus types |
| `kafka.server.nereus.NereusBrokerStorageAppendExecutor` | product-backed implementation | typed limits、exact capture、per-partition FIFO、Kafka error mapping |
| `kafka.server.storage.BrokerStorageFetchExecutor` | implemented optional stock-owned seam | immutable whole-request submit、opaque stock read closure and drain without Nereus types |
| `kafka.server.nereus.NereusBrokerStorageFetchExecutor` | implemented product-backed implementation | bounded logical admission、partition events、stock wave validation、Kafka error mapping |
| `kafka.log.nereus.NereusKafkaExceptionMapper` | mapper | Nereus error/outcome → Kafka exception |
| `kafka.log.nereus.NereusKafkaRecoveredState` | fresh derived state | hydrate full canonical checkpoint；replay exact stock RecordBatch producer/transaction/offset/index state |
| `kafka.log.nereus.NereusKafkaRecoveryStateCodec` | adapter recovery codec | one fresh state per leader open；all seven NKC1 sections hydrate before exact committed-tail replay |
| `kafka.server.nereus.NereusKafkaRecoveryStateFactory` | exact Partition publisher | validate topicId/name/partition/leader epoch and install frozen provisional state |
| `org.apache.kafka.storage.internals.log.LeaderEpochAwareRecoveryState` | stock inert seam | keep `Partition` compilable without unpublished Nereus artifacts |
| `kafka.server.nereus.NereusBrokerStorageRuntime` | runtime bridge | exact ReplicaManager binding、boot/readiness/drain/shutdown delegation |
| `kafka.server.nereus.NereusBrokerStorageRuntimeFactory` | typed factory | disabled isolation、explicit runtime/scan-limit creators、failure rollback |
| `kafka.server.nereus.NereusKafka` | artifact-only launcher | select a fresh production runtime factory and delegate the unchanged lifecycle to `Kafka.run` |

Adapter-side counterpart：

| Class | Key target methods |
| --- | --- |
| `NereusKafkaRuntime` | `start()`、`admission()`、`partitionStorageManager()`、`close()` |
| `KafkaPartitionStorageManager` | `openLeader`、`resign`、`delete`、`reconcile` |
| `KafkaPartitionStorage` | `append`、`recoverAppend`、`read`、`trim`、`stableSnapshot`、`close` |
| `KafkaProduceBufferSnapshot` / `KafkaBoundedAppendExecutor` | exact owned bytes、keyed bounded submit、drain |
| `KafkaFetchOperation` | multi-partition minBytes/maxWait/event/re-read/callback-once |
| `KafkaFetchWaveOperation<T>` | stock-compatible opaque whole-request read wave、event/deadline/callback-once |
| `KafkaAppendBatchEncoder` | exact `MemoryRecords` → ranged `AppendBatch` |
| `KafkaFetchAssembler` | `ReadBatch` list → exact `MemoryRecords`/fetch facts |
| `KafkaRecordBatchCodec` | batch syntax/CRC/offset/producer facts validation |
| `DefaultKafkaPartitionRecoveryLauncher` | checkpoint/read-pin orchestration + bounded COMMITTED replay |
| `DefaultKafkaRecoveryBatchSource` | exact dense `StreamStorage.read` page mapping |
| `KafkaRecoveryStateFactory` | fork-supplied fresh derived-state codec + short publisher pair |

## 3. Exact stock-file modification map

每处修改使用成对 marker，marker 内只做 factory/hook selection，不嵌入存储算法。

### 3.1 `core/.../kafka/server/KafkaConfig.scala`

`d312e8e58d64f326261dd36592a1b5e6398fa5a3` 已实现：

- `AbstractKafkaConfig.CONFIG_DEF` 注册完整 58 个 `nereus.kafka.storage.*` keys（完整表见文档 06）；
- build immutable `NereusKafkaStorageConfig` typed snapshot，解析过程不创建 client/thread/file；
- `validateValues` 调用纯函数 `NereusKafkaConfigValidator.validate`；
- disabled mode 不连接 Nereus，不改变 stock defaults；
- enabled mode 拒绝非 broker process、remote log、log cleaner、AutoMQ elastic stream、RF/minISR conflicts、超出 hard
  format limits 和 cache/spill 与 authoritative log dirs 重叠。

broker-local validator 之外的 durable controller policy 已由 `d23dc5c787` 闭合：feature finalization、
create-topic/create-partitions RF/assignment、minISR、ISR/reassignment/directory mutation 都在 controller state machine
检查。尚未闭合的是 cluster ID/activation 的真实进程交叉验证与 KRaft process readiness；这些不能由
broker-local `KafkaConfig` 代替。

不得在 config constructor 创建 client、thread 或 metadata key。

### 3.2 `core/.../kafka/server/BrokerServer.scala`

目标 inject points：

1. config 已解析、metrics/time 可用后，创建 `NereusKafkaRuntime`；
2. runtime connectivity/capability advertisement 完成后，创建 LogManager/ReplicaManager；
3. 始终构造 stock `ReplicaManager`；enabled runtime 注入 `Some(BrokerStorageAppendExecutor)`，disabled 注入 `None`；
4. request processors 开始前等待 local broker readiness；
5. shutdown 先停止 admission，再关闭 ReplicaManager/partition logs，最后关闭 runtime。

不新增 `NereusReplicaManager`，也不把 field type 写死为 adapter class；对外仍暴露 stock `ReplicaManager`。

`46e67037615a60a39320836cc5f34ddaf4a9b347` 已实现 generic lifecycle seam；`617451957c886d4247f6d2f1a88e44a35edfbba7`
增加 adapter-backed bridge；`94ecf8c105ad2d765aa9fd4a4929ff86c20882a1` 增加 side-effect-free product configuration
mapper；`c27305a7ad955ebc876de20da0fd045e97beba55` 增加 deferred activation-backed product composition：

- `KafkaRaftServer`/`BrokerServer` 通过显式 constructor 参数接收 `BrokerStorageRuntimeFactory`，默认 factory 仅允许
  disabled mode 并返回 no-op；enabled 且未安装 concrete factory 在 LogManager 创建前抛 `ConfigException`；
- context 显式交付 config/cluster ID/broker-epoch supplier/metadata cache/Time/Metrics/KafkaScheduler borrowed dependencies，
  不使用 reflection、service loader 或 process-global singleton；
- runtime create 位于 LogManager 前；`start` 在 lifecycle manager 启动后异步发起，initial metadata publish 后、broker
  unfence/request processing 前等待 ready future；
- runtime 在构造时拥有一个 per-broker `UnifiedLogFactory`；`BrokerServer` 把
  `brokerStorageRuntime.unifiedLogFactory` 显式传给 `LogManager`。disabled runtime 返回
  `UnifiedLogFactory.Local`，不依赖 process-global registry；
- runtime 在 exact `ReplicaManager` 创建后才构造/缓存 `Option[AsyncTopicDeltaLifecycle]` 并传入
  `BrokerMetadataPublisher`；disabled branch 精确保持 `None`，同一 runtime 不能绑定第二个 manager；
- runtime 在 `ReplicaManager` 构造前交付 optional `BrokerStorageAppendExecutor`；disabled branch 是 `None`，enabled
  branch 是 runtime-owned `NereusBrokerStorageAppendExecutor`，不会通过 global registry 或 downcast 查找；
- shutdown 在停止 socket requests 后同步开始 admission drain，在 ReplicaManager 前 bounded `awaitDrained`，在 LogManager
  后 close；earlier stock shutdown failure 仍执行 best-effort idempotent close；
- `NereusBrokerStorageRuntimeFactory` 保留两个 typed `Function` creators 的 injectable constructor，并以
  `production(Function[ReplicaManager, KafkaRecoveryStateFactory])` 增加显式可注入 production path，同时提供
  创建 concrete `NereusKafkaRecoveryStateFactory` 的 no-arg production composition；不使用
  reflection/service loader/global registry；disabled mode 不调用 creator，runtime 已创建后的 scan-config/wrapper
  failure 会 close 并保留 suppressed failure；
- `NereusBrokerStorageRuntime` 把四种 drain reason 显式映射到 adapter enum，以同一
  `KafkaPartitionStorageManager` 构造 lookup/topic-delta lifecycle，并在 delegate drain 的同步边界撤销全部 lookup；
- `NereusKafkaRuntimeConfigurationMapper` 仅在真实 broker epoch 已知后接受 enabled typed snapshot；它精确构造
  ObjectStore/Oxia/StreamStorage/runtime/capability/ListOffsets 配置，但不创建 provider、线程、client 或文件；
- 当前 executable provider set 是 `{OBJECT_WAL_SYNC_OBJECT}`，object provider token 只允许小写 canonical `s3`；
  其他四种 profile 或自定义 class name 均在资源创建前 `ConfigException`，不能回退到 reflection/service loader；
- KRaft broker epoch 原值进入 capability，generic binding-operation epoch 使用 checked `brokerEpoch + 1`，避免合法
  Kafka epoch `0` 与 product positive-epoch invariant 冲突；
- production factory 构造时只创建无 I/O 的 `NereusKafkaDeferredRuntime`；`start()` 以 25ms poll 等待真实
  post-registration broker epoch，并在 readiness timeout 内才调用 `NereusKafkaProductRuntimeCreator.create(...)`；
- product creator 显式构造 `S3CompatibleObjectStoreProvider`、借入 Kafka scheduler/Time、以当前 immutable KRaft image
  加 conservative local-log scan 形成 activation snapshot，再调用 public
  `NereusKafkaObjectWalRuntimeFactory.createActivated(...)`；任何 durable binding fact 仍由 product 侧 64-shard wrapper 补齐；
- deferred manager 在 runtime ready 前保持 future pending，ready 后每次 dispatch 都再次调用真实 runtime
  `admission().requireReady(...)`；epoch wait、startup failure、drain 与 close 都取消 owned poll 并阻止 late creation；
- `NereusKafkaRecoveryStateFactoryBridge` 只允许 exact factory one-time bind；binding 发生在同一 runtime 第一次
  `asyncTopicDeltaLifecycle(exactReplicaManager)`，在此之前 recovery 返回 retriable `METADATA_UNAVAILABLE`；
  `NereusListOffsetsLifecycle.beginDrain` 只负责 admission/revocation，standalone `shutdown` 仍 deduplicate manager shutdown；
- stock/no-artifact factory tests 和 single-node KRaft start→shutdown→restart 已通过。
- `faaffc8a75` 新增 stock-only `BrokerStorageManagedLog` 与 `PartitionLeaderAuthority`。`Partition` 只构造
  JDK `Supplier`/`Runnable` closure 并在 `leaderIsrUpdateLock` 内执行；`LogManager`/`ReplicaManager` 只匹配
  stock interface。由此 artifact-free `:core:test --tests kafka.KafkaConfigTest` 可完整编译 stock main/test，
  stock source 不再直接 import `com.nereusstream.*` 或 `kafka.*.nereus`；
- stock `kafka.Kafka.main` 现在调用
  `run(args, BrokerStorageRuntimeFactory.Disabled, ControllerStorageRuntimeFactory.Disabled)`；
  package-visible `Kafka.run` 仍完整拥有 signal、shutdown hook、server startup 与 `awaitShutdown`，仅把显式 factory
  传给 `KafkaRaftServer`。artifact-only `kafka.server.nereus.NereusKafka.main` 每次创建 fresh
  `NereusBrokerStorageRuntimeFactory.production()` 和 `NereusControllerStorageRuntimeFactory.production()`，
  再委托同一 `Kafka.run`；可执行
  `bin/nereus-kafka-server-start.sh` 只把 main class 切到该 launcher，不使用 reflection、`ServiceLoader` 或
  process-global registry。broker-only launcher 切片由 `3bd92c7244` 发布；dual-factory/controller 切片由
  `9773c8f817` 发布，feature/control 切片由 `d23dc5c787` 发布，并通过 focused launcher/controller runtime tests 与
  artifact-free stock compile gate。

#### 3.2.1 `ControllerServer` activation runtime（`d23dc5c787`）

stock package 新增三个产品无关 contract，不能 import `com.nereusstream.*` 或 `kafka.server.nereus.*`：

```scala
trait ControllerStorageRuntime extends MetadataPublisher {
  def start(): CompletionStage[Void]
}

final case class ControllerStorageRuntimeContext(
  config: KafkaConfig,
  clusterId: String,
  nodeId: Int,
  metadataCache: KRaftMetadataCache,
  time: Time,
  logDirectories: java.util.List[Path],
  faultHandler: FaultHandler)

trait ControllerStorageRuntimeFactory {
  def create(context: ControllerStorageRuntimeContext): ControllerStorageRuntime
}
```

`ControllerStorageRuntimeFactory.Disabled` 精确保持 stock default：storage disabled 时返回 completed/no-op publisher；
enabled 且没有显式 factory 时在 controller resource I/O 前抛 `ConfigException`。`ControllerServer` 保留原三参数
auxiliary constructor 以兼容 Java/stock callers，四参数 constructor 才接收 factory。startup 在构造完 stock
metadata publishers 后执行 `factory.create(context)` 和 bounded `start()`，随后把同一 runtime 加入
`metadataPublishers`，因此 `MetadataLoader.installPublishers` 统一排序 image 与 `onControllerChange` 回调；
`start()` 不等待 activation terminal。shutdown 在 registration manager 和 publisher removal 前 close runtime，
撤销 scheduled retry 并释放它独占的 product resources。

artifact-only `NereusControllerStorageRuntimeFactory` 先调用
`NereusKafkaRuntimeConfigurationMapper.mapController(storageConfig, clusterId)`；mapping 只冻结
Nereus/Kafka cluster ID、Oxia config、`KafkaStorageActivationPolicy` 和 retry interval，不创建 client、thread 或文件。
runtime `start()` 才通过 `NereusKafkaControllerActivationCreator` 创建 daemon single-thread scheduler、shared Oxia client、
`KafkaPartitionMetadataStore`、`KafkaStorageActivationMetadataStore`、binding-aware cluster snapshot 和既有
`KafkaStorageFirstActivationCoordinator`，并按构造逆序 close。

`NereusControllerStorageRuntime` 的事件规则冻结如下：

- `onControllerChange` 只有 `LeaderAndEpoch.isLeader(nodeId)` 为真才允许新 attempt；失去 leadership 立即清除 pending
  并取消尚未执行的 scheduled retry；
- metadata image 与 leadership callback 只设置一个 `pending` bit；scheduled 或 in-flight 已存在时不再提交第二个 task；
  in-flight 成功后若 pending 再立即执行一次，保证 callback 合并但不丢失最后状态；
- `onMetadataUpdate` 从 `FeaturesImage.isNereusStorageEnabled()` 读取 finalized
  `nereus.storage.version >= 1`；feature 未启用时只缓存 image、不创建 activation attempt，level 1→0 会清除
  `pending` 并取消 scheduled retry，已经进入 coordinator 的 future 仍按下面的 CAS 规则完成；
- 只对解包后的 `NereusException` 且 `retriable()==true` 延迟重试；其他 failure 设置该 controller epoch 的
  `terminalFailure`，只调用一次 `FaultHandler.handleFault(...)`，下一次本节点在新 epoch 重新成为 controller 才清除；
- leadership loss 不强制取消已经进入 coordinator 的 future。该 attempt 仍由 PREPARED/ACTIVE CAS、幂等恢复和并发 winner
  reconciliation 约束；本切片没有把 controller epoch 写入 durable activation record，因此不能声称 in-flight activation
  已被 leadership epoch 硬 fencing；
- `close()` 幂等取消 scheduled retry、阻止 late completion 继续调度、关闭 activation graph 并 `shutdownNow()` scheduler。

combined broker/controller 进程中 `ControllerServer` 先于 broker capability publication 启动；feature level 1 后的
首次 coordinator attempt
可因 broker registration/capability 尚未出现返回 retriable，后续 metadata callback/periodic retry 再完成
PREPARED→ACTIVE，避免 controller startup 同步等待 broker readiness 形成闭环。`NereusKafkaConfigValidator` 现允许
任意包含 KRaft broker 或 controller role 的 enabled process；RF/minISR/remote-log/cleaner/request-limit 等
broker-only restrictions 只对 broker role 执行，cache/spill 与 Kafka metadata/log directory 的隔离仍对
dedicated controller 执行。单节点 combined-role provider-backed KRaft baseline 已由
`phase9M6KafkaProcessCheck` 覆盖。Stored activation 已是 ACTIVE 时，coordinator 不再简单返回：它校验当前
activation policy、加载新 KRaft broker epoch 的 capability、验证 compatibility digest，并以 CAS 更新 broker-set/
readiness epoch；该路径不重新应用 first-activation empty-cluster rule。该 deterministic slice 本身不等同于真实
controller takeover/failover；下述三 voter process gate 提供 ACTIVE 稳态的独立证据。

Fork `df238bb387` adds one process-visible success boundary without changing the durable protocol：
`NereusControllerStorageRuntime.completeAttempt` records a local `reconciledControllerEpoch` only after
`NereusKafkaControllerActivation.activate()` completes successfully。`recordReconciledControllerEpoch()` returns the exact
current epoch only once，then `logReconciledControllerEpoch(...)` emits
`Nereus Kafka storage activation reconciled by controller <nodeId> at epoch <epoch>` outside the synchronized block。
Repeated metadata callbacks、coalesced pending work or retries in the same epoch do not duplicate the marker；becoming
controller in a later epoch makes the next success observable again。Leadership loss still does not cancel an in-flight
durable attempt，and the field is not encoded into activation/readiness records，so this is process evidence rather than a
new fencing authority。

#### 3.2.2 Durable storage feature、advertisement 与 format（`d23dc5c787`）

`server-common/.../NereusStorageVersion.java` 冻结 `FEATURE_NAME = "nereus.storage.version"`、`NSV_0=0`、
`NSV_1=1` 和 `LATEST_PRODUCTION=NSV_1`。`Feature.NEREUS_STORAGE_VERSION` 允许 controller 解析/update 该 feature，
但它被排除在 stock `PRODUCTION_FEATURES`、`PRODUCTION_FEATURE_NAMES` 和
`TEST_AND_PRODUCTION_FEATURES` 之外，避免普通 Kafka format、测试默认 feature 集或 advertisement 静默启用它。

advertisement 只由显式进程配置决定：

- `BrokerFeatures.createDefault(unstable, nereusStorageEnabled)` 仅在 enabled broker 加入 range `0..1`；
- `QuorumFeatures.defaultSupportedFeatureMap(unstable, nereusStorageEnabled)` 仅在 enabled controller 加入 range `0..1`；
- `BrokerServer`/`ControllerServer` 从 immutable `NereusKafkaStorageConfig.enabled()` 传入该布尔值；
- disabled stock overload 始终委托 `nereusStorageEnabled=false`，保持源兼容和 stock 输出不变。

`StorageTool.formatCommand` 只有在用户显式传入
`--feature nereus.storage.version=1` 时才把 `Feature.NEREUS_STORAGE_VERSION` 加入本次 formatter 的 supported set；
disabled config 对该参数 fail closed，enabled config 也不会因使用 Nereus fork 自动 bootstrap level 1。由此，
feature activation 是 operator-visible durable action，不是 launcher 副作用。`FeatureControlManager` 通过标准
`FeatureLevelRecord` finalization 持久化 level，`FeaturesImage.isNereusStorageEnabled()` 是 broker/controller image
consumer 的统一判断。

product adapter 已实现 `NereusKafkaRuntimeFactory`，并让 concrete
`NereusKafkaObjectWalRuntimeFactory` 支持纯 Object 模式，或显式安装
`OBJECT_WAL_SYNC_OBJECT + OBJECT_WAL_ASYNC_OBJECT`，并可额外安装 `BOOKKEEPER_WAL_ONLY`：它组装 Object provider、shared Oxia、L0/physical/binding stores、protection、
callback executor、durable checkpoint read pins、checkpoint reader/verifier/recovery coordinator、bounded COMMITTED page
source、concrete recovery launcher 和同一 manager/runtime graph；real Oxia + local-file provider 的 leader
open/Produce/Fetch gate 已通过。Fork 不再承担 ObjectStore/Oxia/read-pin orchestration，只在 exact ReplicaManager
可用后为每次 open 创建 fresh state codec 和 exact Partition publisher。Product Object-WAL runtime 已在 checkpoint
reader/retention verifier 外围组装同一个 exact-reference durable quarantine store；它无需新增 Kafka-fork seam。
BookKeeper 模式复用 provider-neutral `BookKeeperPrimaryWalRuntime`、Oxia namespace/activation stores、generation-zero
physical-reference publisher、profile resolver 和 exact reader registry；只有 F1-BK namespace 与 ACTIVE publication
已由 operator provision、broker readiness exact 匹配时才允许启动。`f9BookKeeperWalOnlyProviderIntegrationTest`
用真实四分片 Oxia + 两个 bookie 证明 leader open、strict append 和 cold generation-zero Fetch。Fork
`ebf1d76163` 已实现完整 100-key typed storage 配置映射、BookKeeper password file/version identity、pre-I/O cross-field
validation、fork-owned client 和 product-first/client-second close wrapper。`f9BookKeeperWalOnlyProcessIntegrationTest`
进一步从真实 release tarball 启动 combined-node Kafka，使用 stock
`zk+longhierarchical://127.0.0.1:<port>/ledgers` 管理 BookKeeper 自身元数据，同时把 Nereus
namespace/activation/readiness authority 保留在 Oxia；它完成 Admin create、offset 0 Produce/Fetch、
earliest=0/latest=1、正常停机、fresh-JVM offset 0 恢复、offset 1 继续追加和 earliest=0/latest=2。
`f9ObjectWalAsyncObjectProcessIntegrationTest` 以同一真实 release launcher、Oxia 和 LocalStack S3 明确选择
`OBJECT_WAL_ASYNC_OBJECT`，完成首 JVM offset 0 与 fresh-JVM offset 0 恢复/offset 1 继续追加，
并冻结 earliest=0/latest=2。BookKeeper async gate 写入四个 batch、等待真实 LocalStack NCP2 object、正常停机后
fresh-JVM 恢复并继续追加；sync gate 写入一个 batch，并以 append 返回前 required NCP2 COMMITTED/readable
作为同步语义证据，再完成 fresh-JVM 恢复。独立 provider gate 已覆盖 real Oxia 上的 two-runtime Object-WAL
live takeover。后续 fork `fe308359b6` / `bb7e8937c5` / `df238bb387` 与 product release-process gates 又闭合了真实
two-process KRaft singleton takeover、already-dispatched old Object-WAL append fencing，以及 BookKeeper 三
profile 的 two-process post-handoff recovery/continuation。The product-only fault-agent gate additionally closes the
common BookKeeper provider-applied/pre-publication C boundary without a fork hook；three combined release nodes additionally
close ACTIVE-state controller kill/re-election/reconciliation while retaining native IO；three dedicated controllers plus
one broker further close the complete before-provider/after-provider readiness-create/PREPARED-create/ACTIVE-CAS
store-publication matrix；an Oxia-proxied controller/broker gate further closes actual transport reset and same-epoch retry；
尚未实现的是 initial empty-cluster snapshot/proof and capability-aggregation process cuts、coordinator/checkpoint cuts 和
更广 kill-cut process tests。显式
launcher/KafkaRaftServer broker/controller factory selection 已实现；`phase9M6KafkaProcessCheck` 现使用真实 release
distribution、四分片 Oxia 和 pinned LocalStack S3 覆盖显式 feature format、broker/controller registration、
activation、Admin create、Produce/Fetch/ListOffsets、S3 object existence 和 SIGTERM shutdown，并以同一 KRaft
identity 启动第二个 JVM。第一 JVM 除普通 offset 0 外，还提交 transactional data/COMMIT marker、执行真实 group
subscribe/rebalance 并提交 offset；第二 JVM 验证 higher broker epoch readiness refresh、用户分区与两个 coordinator
internal topics 的并发 remote recovery、原 group committed offset reload、同一 transactional ID 的下一次 commit、
group 从下一可见 offset 恢复，以及最终 earliest=0/latest=5。随后第三 JVM 在 open-transaction data offset 5
stable 后被强制终止；第四 JVM 恢复同一 transactional ID，生成 ABORT marker 6、提交 data/marker 7/8，并证明
read-committed/group 都跳过 aborted data、latest=9。新增
`f9MultiBrokerTakeoverProcessIntegrationTest` 先补上 two Kafka-process Object-WAL live reassignment；
`f9InFlightTakeoverProcessIntegrationTest` 再启动 node 3 combined controller/broker、node 1/2 broker-only 三个真实
release JVM，将 RF1 partition 固定在 `[1]`，用 downstream timeout toxic 阻断 shared S3 endpoint 的响应，并以
`jcmd Thread.print -l` 等待 storage worker 同时出现
`NereusUnifiedLog.appendStable` 与 `CompletableFuture.get`。这不是 sleep-based 猜测：命中说明 single-attempt
Produce 已进入 provider-backed stable append 且尚未返回。测试随即 `SIGSTOP` node 1、移除 toxic，从仍活跃的
node 2/controller bootstrap 提交原子 `[1] -> [2]` 重分配，并要求
`leader=2, replicas=[2], ISR=[2]`、无 ongoing reassignment、earliest/latest 仍为 `0/1`。`SIGCONT` 后旧请求以
`FencedLeaderEpochException: append session changed before guarded object upload` 结束，旧进程保持存活，S3 WAL key
集合不变，broker 2 随后在 offset 1 提交并把 latest 推到 2。该 gate 闭合 Object-WAL P/C 边界，但仍不包含
checkpoint/virtual-segment transaction cut、coordinator migration 或 multi-controller activation cuts，
因此不足以宣称 production rollout ready。

`f9BookKeeperProfileTakeoverProcessIntegrationTest` supplies the independent BookKeeper post-handoff matrix without a new
fork seam。One stock ZooKeeper long-hierarchical metadata service and two real Bookies host three isolated authorities，
one for each BookKeeper profile。Each authority starts node 1 combined controller/broker and node 2 broker-only concurrently，
commits/fetches offset 0 on singleton `[1]`，atomically reassigns to `[2]` with exact leader/replicas/ISR and no transitional
reassignment，keeps node 1 alive，then recovers offset 0 and commits/fetches offset 1 on node 2。WAL-only requires zero
bucket objects before and after the handoff；async/sync require a real NCP2 object at both points。The async activation digest
uses the same one-entry rollover/physical-deletion configuration as the broker mapping，so configuration drift is rejected
before BookKeeper I/O。Fresh execution passes 64/64 actionable tasks in 2m17s。This proves P-tier resign/open/recovery/
continuation for all three profiles。

`f9BookKeeperInFlightTakeoverProcessIntegrationTest` supplies the complementary C-tier boundary without changing the Kafka
fork or production Nereus classes。Gradle builds `f9BookKeeperFaultAgent` as a dedicated fat Java-agent JAR and passes it only
to broker 1 through `KAFKA_OPTS`。The advice intercepts the common
`DefaultBookKeeperClientOperations.write(...)` return future after the real `WriteAdvHandle.writeAsync` completes；it writes
an applied marker but does not complete the future observed by `BookKeeperPrimaryWalAppender` until a release marker exists。
The harness accepts the cut only when all of these facts agree：`jcmd` shows
`NereusUnifiedLog.appendStable -> CompletableFuture.get`、the exact Oxia reservation is `WRITING`、and an independent
BookKeeper client reads the exact physical entry unconfirmed。

Broker 1 is then `SIGSTOP`ped while node 3 keeps KRaft live。After exact `[1] -> [2]` reassignment，broker 2's offset-1
Produce lazily opens the writer and runs `BookKeeperLedgerRecovery`；the gate requires the captured reservation to become
`ABANDONED` and its old root `SEALED` before that Produce can return offset 1。Only then does the harness release the agent
future and `SIGCONT` broker 1。The old completion must fail at stale metadata authority、must not kill the JVM or move
earliest/latest away from `0/2`，and the WAL-only bucket must remain empty。Fresh execution passes 66/66 actionable tasks in
1m30s with reusable Gradle configuration cache。The cut is before `WRITING -> DURABLE`，so it is shared by all three
BookKeeper profiles；the profile-specific materialization behavior remains covered by the preceding P matrix。

### 3.3 `core/.../kafka/log/LogManager.scala`

`cfcdd55fbc571bc7187379d65504caa4fe23586e` 已在 stock package 新增
`UnifiedLogFactory` 与 immutable `UnifiedLogOpenContext`。factory 的完整策略面是：

```scala
def logDirectories(configured: Seq[File]): Seq[File]
def prepareLogDirectories(selected: Seq[File]): Unit
def initialOfflineDirectories(configured: Seq[File], selected: Seq[File]): Seq[File]
def loadExistingLogs: Boolean
def scheduleLocalMaintenance: Boolean
def open(context: UnifiedLogOpenContext): UnifiedLog
```

`UnifiedLogFactory.Local` 逐参数调用 stock `UnifiedLog.create`，是 disabled/default path；`LogManagerBuilder` 也显式
传该 local factory。`LogManager.apply` 先让 factory 选择 effective directories/offline directories，然后同一个
factory 同时接管 startup existing-log construction 与 `getOrCreateLog` construction，避免只替换新建路径却让旧
local log 在 restart 时重新成为 truth。

`NereusUnifiedLogFactory` 的当前 executable contract：

- 唯一 log root 是 `${nereus.kafka.storage.cache.dir}/{brokerId}/partition-logs`，不使用 `log.dirs` 作为 partition
  shell root，也忽略 stock initial-offline list；
- `prepareLogDirectories` 只接受上述 exact singleton root；不存在时创建目录并原子写入 KRaft V1
  `meta.properties`（exact cluster ID、broker node ID、fresh non-reserved directory ID），存在时逐字段校验，任何
  version/cluster/node/directory mismatch 都以 non-retriable metadata invariant fail closed。stock factory 此 hook
  inert，因为 `kafka-storage format` 已准备其 configured roots；
- `loadExistingLogs=false`，broker restart 不扫描 cache 下的旧 topic-partition directories；
- `scheduleLocalMaintenance=false`，不启动 cleaner、retention、flusher、recovery/HW/LEO checkpoint、clean-shutdown
  marker 或 clean-shutdown epoch read；这些 local artifacts 都不能成为恢复或 durability evidence；
- `open` 只接受 factory-selected cache root 的直接 child、`isFuture=false`、local start/recovery point 均为 `0`、
  present 且 non-zero KRaft topic ID；identity 从 exact cluster ID/topic ID/topic name/partition 构造；
- 每个 open 返回新 `NereusUnifiedLog` shell；旧 cache bytes 永不加载。shell 仍使用一个 ephemeral
  `NereusLocalLog`/synthetic segment 维持 stock `UnifiedLog` 对象不变量；record bytes 只通过 stable-append callback
  进入 adapter，Fetch 由 `NereusUnifiedLog.read` 直接读取 adapter，local segment 不承载 bytes 或 restart recovery。

`LogManager.startup`、`shutdown`、checkpoint/flush/retention helpers 与 clean-shutdown read 都检查同一个 factory
policy；因此 authoritative mode 不会偶然启动某一项 local maintenance。stock disabled branch 保持默认
`loadExistingLogs=true`、`scheduleLocalMaintenance=true` 和原 `UnifiedLog.create` 行为。

同集群混用 local/Nereus topic 不在首版支持范围。enabled mode 完全忽略 dedicated cache root 中的旧 shell，
而 typed startup validator 继续拒绝 cache/spill 与 authoritative Kafka log dirs 重叠；不能把旧 local log 自动
导入为 Nereus durable state。

#### 3.3.1 Current `NereusUnifiedLog` publication state machine

shell 以 immutable `KafkaPartitionIdentity` 和一个 private guard 持有两项可撤销 publication：

```text
installRecoveredState(epoch, frozenState)
  -> validate exact topicId/topic/partition/epoch + frozen
  -> align shell logStartOffset/LEO/HW to recovered stable facts

installStorage(epoch, recoveredStorage)
  -> validate exact identity/epoch/LEADER_WRITABLE
  -> require storage.stableSnapshot == recoveredState stable facts

nereusWritable(epoch)
  -> recoveredState != null
  -> storage != null
  -> both exact epoch
  -> storage state == LEADER_WRITABLE

removeStorage(epoch, exactStorage)
  -> remove only the same instance/epoch；never revoke a newer publication
```

`NereusKafkaRecoveryStateFactory.publish` 先把 frozen state 安装到 exact live log shell，再调用
`Partition.installNereusRecoveredState`；Partition publication 失败时会撤销 shell state。
`NereusListOffsetsLifecycle` 收到 product manager 返回的 writable storage 后，先验证 stock Partition、shell、
storage identity/epoch/state，再调用 `installStorage`，最后安装 exact ListOffsets lookup。resign/delete/drain/open
failure 按 shell/storage/lookup 的 exact instance/epoch 逆向撤销。

`dc8c66388a` 在该 publication state machine 后增加第一条 correctness-only UnifiedLog 数据面；
`ee608625e4` 把 Produce 入口迁到 bounded ReplicaManager worker；`bba3ef0121` 再把 Fetch 入口迁到
whole-request bounded worker：

```text
KafkaApis.handleProduceRequest
  -> ReplicaManager.appendRecords
  -> BrokerStorageAppendExecutor.validateRequest(all partition bytes)
  -> NereusBrokerStorageAppendExecutor.submit(each TopicIdPartition)
       -> copy exact MemoryRecords before submit returns
       -> keyed bounded executor
       -> rebuild owned MemoryRecords

Partition.appendRecordsToLeader(requiredAcks)
  -> optional RequiredAcksAwareAppend
  -> NereusUnifiedLog.appendAsLeader
  -> stock UnifiedLog validation / compression conversion / offset + leader-epoch assignment
  -> NereusLocalLog.append(lastOffset, validatedRecords)
  -> KafkaPartitionStorage.append(exact read-only duplicate, KafkaAppendContext)
  -> exact stable result validation
  -> update ephemeral shell LEO
  -> stock producer/transaction derived-state update
  -> Partition HW evaluation

NereusUnifiedLog.read(startOffset, maxLength, isolation, minOneMessage)
  -> freeze exact published storage + KafkaStableSnapshot
  -> isolation upper bound = stableEnd/HW/LSO
  -> KafkaPartitionStorage.read(KafkaStorageReadRequest)
  -> validate same publication/snapshot/bounds/overflow/no-M3-aborted-txn
  -> MemoryRecords.readableRecords(exact assembly buffer)
  -> FetchDataInfo with virtual position

KafkaApis.handleFetchRequest
  -> ReplicaManager.fetchMessages
  -> BrokerStorageFetchExecutor.submit(params, ordered fetchInfos, stock read closure)
  -> NereusBrokerStorageFetchExecutor logical operation admission
  -> subscribe current partition storage events before initial read
  -> KafkaFetchWaveOperation
       -> bounded worker invokes complete stock readFromLog wave
       -> initialWave=true: readFromPurgatory=false
       -> event/deadline wave: readFromPurgatory=true
       -> actual record bytes / stock terminal facts decide wait or complete
  -> ordered LogReadResult validation
  -> shared ReplicaManager fetch metrics/response conversion
  -> original responseCallback exactly once on separate bounded callback executor
```

`requiredAcks` 只接受 `-1/0/1` 并原样进入 `KafkaAppendContext`；普通 stock `UnifiedLog` 不实现 optional interface，
继续走原调用。stable result 必须回显 exact acks、assigned range、encoded byte count 和当前 stable snapshot，
否则立即 resign。append timeout/interrupt 是 `MAY_HAVE_COMMITTED` 并 resign；stable commit 后任何 stock shell
步骤失败也 resign，阻止 successor append。LEO 只在 callback 成功返回后推进。synthetic segment file bytes 保持
`0`，但 `NereusLogSegment.size()`、`UnifiedLog.size` 和 `LogOffsetMetadata` 使用 canonical state 中的 exact Kafka
logical bytes/relative position；不能把 cache-file size 当成 retention 或 protocol-visible size。
M4 产品 encoder 已接受通过严格 magic-v2/CRC/producer-fact 校验的 idempotent、transaction 与 control batch；
是否可写仍由 fork 的 stock `UnifiedLog` producer/transaction validation 决定，adapter 不复制该状态机。
隔离 fork commit `ec7f0db991` 已允许 `CLIENT` transactional data 与 `COORDINATOR` marker 通过同一 stable
append bridge，verification guard、epoch/sequence/marker 校验仍由 stock path 执行；follower append 永远拒绝。
隔离 follow-up `032974067c` 在真实 `ReplicaManager.appendRecordsOnStorageExecutor` closure 上先建立 stock
verification guard，再延迟执行 CLIENT transactional append，随后经同一 executor 发送 TV2 COORDINATOR abort
marker；两步都成功且生成 ongoing/aborted stock state，锁定 `verificationGuards` 与 `transactionVersion` 不会在
request-to-worker handoff 中丢失。

Fetch 已按 stock isolation 选择 LOG_END/HW/LSO 上界。READ_COMMITTED 的 aborted transaction list 来自
checkpoint/replay 恢复的 in-memory `NereusTransactionIndex`，并按本次实际返回页的 next logical offset 裁剪，
不会把后续页的 abort 暴露到较小的 Fetch。它复用 adapter containing-entry/first-entry overflow 语义并受
partition bytes、hard response bytes、stable upper bound 与 typed timeout 约束；返回前复核仍是同一份
`LEADER_WRITABLE` storage。`NereusCanonicalLogState` 根据 stable append、checkpoint 与 committed-tail replay
维护 virtual segment base、exact batch position、sparse time/logical samples；Fetch 返回真实
`LogOffsetMetadata`，ListOffsets 先用 time-index 缩小范围，再对 bounded exact COMMITTED payload 做最终验证。

`NereusUnifiedLog` 仍同步等待 future，因为 stock validation/LEO/producer-state ordering 必须看到 stable terminal；
现在 Produce 与 Fetch 的 caller 都是 runtime-owned bounded worker，不再是 Kafka request handler。每个 worker wave 完成后在
`finally` 中调用 shared `defaultActionQueue.tryCompleteActions()`，因为原 request handler epilogue 已经返回。所有
partition future 终态后，ReplicaManager 才复用同一 `completeAppendRecords` 路径计算 produce status、调用一次
validation-stats callback，并进入 stock delayed-produce/response callback。Fetch completion 同样只执行一次 shared
metrics/response path；disabled mode 保留原 `DelayedFetch` purgatory branch。该证据已覆盖 multi-partition
minBytes/maxWait handoff，但仍不能作为真实 BrokerServer/KRaft 数据面完成声明。
`dc8c66388a` exact-head aggregate 已通过 80/80 outer tasks；nested stock-without-artifacts 与 artifact-enabled Kafka
分别通过 92/92、95/95 actionable tasks，包含 required-acks routing、stable append/read/fencing、stock KRaft
restart 与全部 format/static gates。`ee608625e4` 的 fresh exact-head aggregate 同样通过 80/80 outer、
92/92 stock-without-artifacts 和 95/95 artifact-enabled actionable tasks，并包含新 executor/ReplicaManager/runtime
tests、146/146 scenarios、real provider recovery、stock KRaft restart 与全部 format/static gates。
首次 `bba3ef0121` aggregate 暴露 admitted simultaneous-wakeup queue race；`47d36a1d9f` 修复并锁定该回归后，
fresh exact-head aggregate 通过 80/80 outer、92/92 stock-without-artifacts 和 95/95 artifact-enabled actionable
tasks，包含 stock two-partition `ReplicaManager` Fetch、artifact event/deadline/saturation/drain、product
deadline-race、146/146 scenarios、real provider recovery、stock KRaft restart 与全部 format/static gates。

### 3.4 `core/.../kafka/cluster/Partition.scala`

精确 method intent：

| Method | Target behavior in Nereus mode |
| --- | --- |
| `createLogIfNotExists` | pass exact topicId/partition/leader epoch to factory；no name-only open |
| `makeLeader` | wait for `openLeader` recovery publication before accepting writes |
| `makeFollower` | close admission/fence current leader instance；RF1 means no follower data append |
| `appendRecordsToLeader` | optional `RequiredAcksAwareAppend` preserves exact `-1/0/1`；ordinary logs retain stock call |
| `appendRecordsToFollowerOrFutureReplica` | defensive `UnsupportedOperationException`/storage error；controller should make unreachable |
| `maybeIncrementLeaderHW` | candidate is `NereusUnifiedLog.stableEndOffset`，never speculative LEO |
| `readRecords`/`fetchRecords` | use stock isolation bounds over Nereus log；IO runs on fetch executor |
| `deleteRecordsOnLeader` | validate requested offset with stock rules，then durable Nereus trim before success |
| `delete` | metadata-first partition lifecycle，idempotent close/delete |

`makeLeader` 不能在 Kafka partition lock 内等待一个需要同一 lock 的 recovery callback。open 流程在 lifecycle
executor 完成 replay，final publication 才以短 critical section 安装 log/state。

`deleteRecordsOnLeader` 的 Nereus branch 保留 stock read-lock 内的 leader、policy、negative-offset 和
`HIGH_WATERMARK (-1)` conversion。它在锁内捕获 exact `KafkaTrimBarrier.Snapshot` 和 normalized target，
随后在锁外等待 `KafkaDeleteRecordsCoordinator.deleteTo`；只有该 future 返回 durable low watermark 后，才以同一
partition incarnation/leader epoch 的短 critical section 发布 `UnifiedLog.logStartOffset`、推进 producer/epoch
derived state 并唤醒 delayed Fetch/DeleteRecords。Product coordinator 对 target `<= durable logStart` 返回无 I/O
幂等成功；对 advancing target 使用共享 NKC1 checkpoint-before-trim barrier，且不把 mid-batch offset 向 segment
边界取整。任一 leadership/config/root race 在 Nereus mutation 前失败。当前 product API/测试已实现，fork branch
仍待接入。

当前 required-acks seam 仅扩展 stock `storage` package interface 和一个 `Partition` match branch；
`testAuthoritativeAppendPreservesRequiredAcks` 同时证明 `-1` 原样路由、非法值在 adapter I/O 前拒绝，以及普通
`UnifiedLog` fallback 未被替换。

### 3.5 `core/.../kafka/server/metadata/BrokerMetadataPublisher.scala`

AutoMQ source 使用 async topic-delta path。F9 采用同类 ordering，但接口归属于 Nereus。锁定 fork
`c3af5f30fa` 已实现以下 narrow seam：

```text
metadataCache.setImage
  -> ReplicaManager.applyDelta(topicsDelta, newImage, onLeaderStatePublished)
       -> stock Partition.makeLeader
       -> elected leader 同步 beginLeaderEpochAwareOffsetLookup(exact epoch)
  -> AsyncTopicDeltaLifecycle.applyAfterReplicaManager
       -> delete(old image topic ID, metadata offset)
       -> resign(new observed leader epoch)
       -> openLeader(exact cluster/topic/partition/leader/broker/profile/offset/deadline)
  -> each successful operation callback
       -> group/transaction/share coordinator election or resignation
  -> aggregate completion
       -> share coordinator deleted-topic notification
```

`AsyncTopicDeltaLifecycle` 位于 stock-compatible package 且不引用 Nereus artifact；
`BrokerMetadataPublisher` constructor 的最后一个参数是默认 `None`，所以 disabled branch 仍执行原有同步
`ReplicaManager.applyDelta` 和 coordinator 更新。`Some(lifecycle)` 时，`NereusTopicDeltaLifecycle` 从
`TopicsDelta.localChanges(brokerId)` 构造 exact operation；同一 topic-partition 的 delete 必须先于同 delta 的同名
recreation。只有 `electedLeaders` 的成功 open 才触发 election，ISR-only leader update 不重复 election；失败 future
不发 ready callback，并交给 `metadataPublishingFaultHandler`。broker epoch supplier、metadata offset、stock topic ID/
leader state 的同步异常也必须撤销已准备的 exact epoch，不能永久留下 pending marker。

internal-topic coordinator election 必须晚于对应 storage fully recovered；否则 coordinator 可能从未恢复的
`__consumer_offsets`/`__transaction_state` 提供服务。`firstPublishFuture` 仍在 metadata publication 主流程结束时完成，
不是 all-partition readiness barrier。`BrokerServer` 已能从显式 runtime factory 注入
`Some(NereusTopicDeltaLifecycle)`，但尚无 shipped concrete Nereus factory，也尚未把异步 open failure 转成最终的
per-partition offline policy；因此
本节是已测试的 invocation seam，不是可启用 broker runtime 或 KF-OPS-017 完成声明。
`032974067c` 的 publisher regression 同时放入 group 与 transaction 两个 internal topic，证明 ready callback
之前两个 coordinator 都不 election，callback 之后才分别以 exact leader epoch election；另一个 lifecycle
regression 用 `__transaction_state` 锁定 callback 必须等待 exact recovered storage 安装。该证据仍是同进程
deterministic seam，不替代真实 coordinator replay/restart/failover gate。

### 3.6 `core/.../kafka/server/ReplicaManager.scala`

metadata slice 已增加一个 stock-type-only overload：

```scala
def applyDelta(
  delta: TopicsDelta,
  newImage: MetadataImage,
  onLeaderStatePublished: (Partition, Uuid, Int) => Unit
): Unit
```

原二参数方法委托 no-op callback，disabled behavior 不变。三参数方法只对
`localChanges.electedLeaders` 在 `Partition.makeLeader` 返回后、state-change lock 释放前调用 callback；此时
`Partition.isLeader`、topic ID 与 leader epoch 已可精确校验。ISR-only leader updates 不调用 preparation callback。
callback 抛错沿 metadata publication fault path 传播，不通过 reflection 或异步访问 ReplicaManager 内部状态。

`ee608625e4` 保留 stock class 并在 constructor 尾部增加默认 `None` 的
`Option[BrokerStorageAppendExecutor]`；`bba3ef0121` 追加默认 `None` 的
`Option[BrokerStorageFetchExecutor]`。没有 subclass，也不让 stock seam 引用 Nereus type：

```scala
trait BrokerStorageAppendExecutor extends AutoCloseable {
  def validateRequest(entries: Iterable[MemoryRecords]): Unit
  def submit(
    partition: TopicIdPartition,
    records: MemoryRecords,
    append: MemoryRecords => LogAppendResult
  ): CompletionStage[LogAppendResult]
  def drained: CompletionStage[Void]
}

trait BrokerStorageFetchExecutor extends AutoCloseable {
  def submit(
    params: FetchParams,
    fetchInfos: Seq[(TopicIdPartition, PartitionData)],
    read: Boolean => Seq[(TopicIdPartition, LogReadResult)]
  ): CompletionStage[Seq[(TopicIdPartition, LogReadResult)]]
  def drained: CompletionStage[Void]
}
```

`appendRecords` 在 required-acks 校验后分两支：

- `None`：执行原同步 `appendRecordsToLeader`，然后进入共享 `completeAppendRecords`；调用顺序和普通 local-log
  behavior 不变；
- `Some`：先对 immutable entries snapshot 做整请求校验；每个 partition submit 一个 owned task。worker 以
  `RequestLocal.noCaching` 调用 singleton `appendRecordsToLeader`，避免跨线程借用 request-local
  `BufferSupplier`；completion/failure 均转为该 partition 的 `LogAppendResult`。全部 normalized future terminal 后
  只调用一次共享 completion。

整请求字节超限在任何 partition submit 前拒绝。task/byte saturation 是 per-partition known-not-committed
结果；因此同一 Produce request 可以包含 stable-success partition 和 admission-rejected partition，仍按 Kafka
per-partition response contract 聚合。已接纳 future 的 caller cancellation 不传播到底层 append。

`fetchMessages` 在 executor 为 `None` 时逐行保留原 read/purgatory 分支；`Some` 时把 immutable ordered
`fetchInfos` 和一个 stock read closure 提交为单个逻辑 operation。closure 的布尔参数只控制
`readFromPurgatory`：initial wave 为 `false`，event/deadline wave 为 `true`。每一 wave 仍由现有
`readFromLog` 顺序执行全部 partition，因此 global byte budget、第一条溢出、divergence、preferred replica、
remote fetch 与 partition error 都没有在 wrapper 中重写。结果的 order/cardinality 不吻合时 fail closed。

### 3.7 `core/.../kafka/server/DelayedFetch.scala`

stock delayed fetch 只服务 disabled mode；本切片没有修改 `DelayedFetch.scala`。Nereus mode 的
`fetchMessages` 使用 `NereusBrokerStorageFetchExecutor` + `KafkaFetchWaveOperation`，storage event 直接唤醒
bounded worker，不把 operation 放入 purgatory，也不允许 purgatory thread 同步调用 object/BookKeeper IO。

### 3.8 `metadata/.../ReplicationControlManager.java`

在 durable cluster feature `nereus.storage.version >= 1` 时：

- `createTopics` effective replication factor 必须是 1；manual assignments 每项 size 1；effective
  `min.insync.replicas` 必须解析为字符串值 `"1"`；
- `createPartitions` 继承 existing partition RF=1，new assignments size 1；
- `alterPartition` ISR 必须精确等于 current leader singleton；
- `alterPartitionReassignments` 只允许 target one broker，违反时不生成 metadata record；
- preferred/unclean election 不创建 follower-copy assumptions；
- cluster/topic `min.insync.replicas` mutation 的 effective value 必须是 1；
- `AssignReplicasToDirs` 先保留 stock broker registration/epoch 校验，再对每个 Nereus partition 返回
  `UNSUPPORTED_VERSION` 且 records 为空，不改变 storage placement。

Controller 条件来自 KRaft feature/control record，不读取 broker-local config，防止 controller failover 后规则变化。
实现入口是 `FeatureControlManager.isNereusStorageFeatureEnabled()`；
`ReplicationControlManager.createTopics`/`createPartitions`/`alterPartition`/
`alterPartitionReassignments`/`handleAssignReplicasToDirs` 与
`ConfigurationControlManager` 的 effective-config validation 都读取同一 finalized state。feature level 0/不存在时
完全保留 stock 分支。

deterministic evidence 由以下现有 Kafka test class 承担：

- `ReplicationControlManagerTest.testNereusStorageFeatureGatesTopicCreationAndPartitionGrowth`；
- `ReplicationControlManagerTest.testNereusStorageFeatureGatesIsrReassignmentAndDirectories`；
- `ConfigurationControlManagerTest.testRejectNonSingletonMinIsrWhenNereusStorageEnabled`；
- `FeatureControlManagerTest.testUpdateNereusStorageFeature`；
- `FeatureTest.testNereusStorageFeatureRequiresExplicitOptIn`、
  `BrokerFeaturesTest.testNereusStorageSupportIsAdvertisedOnlyWhenEnabled` 和
  `QuorumFeaturesTest.testNereusStorageSupportIsAdvertisedOnlyWhenEnabled`；
- `StorageToolTest.testFormatNereusStorageFeatureRequiresEnabledMode` /
  `testFormatExplicitNereusStorageFeature`；
- `NereusControllerStorageRuntimeTest.waitsForDurableNereusFeatureBeforeActivation`。

Nereus 根仓库的 `phase9M6KafkaFeatureCheck` 将上述 source lock、四个分模块 focused test invocation 和
`0.1.0-f9-dev` artifact runtime 组合为可重复 gate。

### 3.9 Files intentionally unchanged

初版保持 stock：

- Kafka protocol request/response schemas；
- group/transaction coordinator algorithms；
- `KafkaApis`（除 factory type wiring若编译需要）；
- record validation、compression conversion、producer idempotence analysis；
- quota calculation and request channel；
- client compatibility code。

## 4. Adapter API consumed by the fork

### 4.1 Partition identity and open

```java
// target, module-local Kafka types allowed
public record KafkaPartitionIdentity(
        String kafkaClusterId,
        Uuid topicId,
        int partition,
        String observedTopicName) { }

public interface KafkaPartitionStorageManager extends AutoCloseable {
    CompletableFuture<KafkaPartitionStorage> openLeader(
            KafkaPartitionLeaderOpenRequest request);

    CompletableFuture<Void> resign(
            KafkaPartitionIdentity id, int observedLeaderEpoch, Duration timeout);

    CompletableFuture<Void> delete(
            KafkaPartitionIdentity id, long metadataOffset, Duration timeout);

    Optional<KafkaPartitionStorage> current(KafkaPartitionIdentity id);
    CompletableFuture<Void> shutdown();
}
```

`KafkaPartitionLeaderOpenRequest` carries exact `identity/leaderId/leaderEpoch/brokerEpoch/storageProfile/metadataOffset/
timeout` facts。M6 metadata-image reconciliation remains a higher-level runtime method and is not silently represented by the
M3 manager interface。

`observedTopicName` 只用于 logs/metrics；key、CAS 和 stream name 使用 topicId。

### 4.2 Open partition storage

当前 M3 adapter boundary 不把 Kafka artifact 泄漏到 production API；fork 对 stock `MemoryRecords.buffer` 做
read-only duplicate 后传入 `ByteBuffer`。Kafka fork 仍在编译期负责证明该 buffer 来自 stock LogValidator 输出。

```java
public interface KafkaPartitionStorage extends AutoCloseable {
    KafkaPartitionIdentity identity();
    int leaderEpoch();
    StorageProfile storageProfile();
    KafkaPartitionState state();
    KafkaStableSnapshot stableSnapshot();

    KafkaStableSnapshot publishDerivedOffsets(
            long expectedStableEndOffset,
            long highWatermark,
            long lastStableOffset);

    CompletableFuture<KafkaStableAppendResult> append(
            ByteBuffer validatedRecords,
            KafkaAppendContext context);

    CompletableFuture<KafkaStorageReadResult> read(KafkaStorageReadRequest request);
    CompletableFuture<Void> resign();
    void close();
}
```

`KafkaAppendContext` 当前包含 expected start、leader epoch、request deadline、origin tags 和 required acks；required
acks 不改变 Nereus stable boundary，只用于返回 facts/metrics。M5 增加 trim，M2 recovery/checkpoint coordinator 由
storage manager 在 open/periodic path 组合，不把可重复 `recover()` 暴露到已经 writable 的 instance。

M4 将 stable append 与 Kafka-derived visibility publication 分成两个明确阶段：`append` 的 durable result 只推进
`stableEndOffset`/commit version，保留前一版 HW/LSO；fork 在 stock `ProducerStateManager`、transaction index 和
first-unstable state 更新成功后调用 `publishDerivedOffsets(exactEnd, HW, LSO)`。隔离 commit `ec7f0db991`
已在 `NereusUnifiedLog.appendAsLeader` 接入此顺序：stable terminal 后推进 stock HW 到 exact durable end，
重新计算 LSO，再发布 derived snapshot；任一步失败都 fence 已知稳定 append。在 exact end 确认前，同
partition 下一次 storage append 不 dispatch，`STABLE_APPEND` 事件也不发布。expected end 不匹配、offset
越界或 initialized HW/LSO 回退均 fail closed；post-stable publication failure 进入 write-fence/replay。

### 4.3 Current binding-first storage manager（2026-07-23）

`DefaultKafkaPartitionStorageManager` 已实现 product-owned M3 composition boundary：

- `openLeader` 先用 immutable manager owner/epoch/TTL 执行 deterministic `ensureBinding`，再构造只接受 ACTIVE
  binding、exact canonical profile 和 remaining timeout 的 `KafkaPartitionOpenPlan`；
- `KafkaPartitionOpener.open(plan)` 是 durable authority acquire、fresh head/checkpoint recovery 和 storage construction
  的唯一 SPI；fork 不直接拼接这些 Nereus dependencies；
- `KafkaPartitionLeaderManager` 对同 authority + same stream/profile plan 去重；same authority 若 stream/profile 冲突
  则 metadata-invariant fail closed；opener result 还必须匹配 identity、leader epoch、profile 和 writable state；
- observed resign 只有 epoch >= process-current term 才能移除 desired slot；delete 必须先移除/resign local term，再进入
  durable ACTIVE → DELETING → DELETED；shutdown 后 binding 的迟到完成不会启动 opener；
- manager/open result 是 operation-owned future，caller cancel/complete/obtrude 不能取消或伪造底层 binding/recovery。

`DefaultKafkaPartitionStorageManagerTest` 覆盖 real deterministic lifecycle-to-plan composition、exact open dedupe、profile
mismatch、stale resign、drain-before-delete 和 shutdown-during-binding。`DefaultKafkaPartitionOpener` 现已实现 authority
session acquire、exact profile/head/source freeze、checkpoint/replay launcher 和 storage construction 的 deadline-bound
composition；`DefaultKafkaCheckpointSourceValidator` owns re-read fencing and immutable commit-ancestor proof。Kafka fork
metadata callback wiring 仍未完成。`DefaultKafkaPartitionOpener` 另外注入共享 `ScheduledExecutorService`、session TTL
和严格短于 TTL 的 renewal interval；构造出的 writable storage 自己调度 exact-session renewal，不由 fork 保存或
拼接 fencing token。

## 5. Exact batch encode/decode

### 5.1 `KafkaAppendBatchEncoder`

输入是 stock `LogValidator` 已输出的 `validRecords`，offset/leader epoch/compression conversion 已完成。

Algorithm：

1. duplicate `MemoryRecords.buffer`，不改 position/limit；
2. 遍历 raw Kafka batches，使用 `sizeInBytes` 切出 exact read-only slice；
3. `KafkaRecordBatchCodec.validate` 校验 magic、batch length、CRC、base/last offsets、record count、producer fields；
4. 要求第一 base == expected start；相邻 batch base == previous last + 1；
5. 为每个 batch 建 `AppendEntry(recordCount=last-base+1,payload=exact bytes,...)`；
6. event time 使用 batch max timestamp 的 non-negative normalized value，仅为 Nereus hint；
7. `AppendBatch.payloadFormat=KAFKA_RECORD_BATCH`；record count exact sum；
8. batch CRC32C 覆盖 slices concatenation；
9. 调用 `StreamStorage.append(... AppendPrecondition.expectedStartOffset(firstBase))`；
10. exact result validator 对比 start/end/count/bytes。

禁止使用 `MemoryRecords.withRecords` 或逐 record builder 做普通 append；那会重新压缩、改变 CRC/producer
metadata。复制 raw bytes 只改变内存 ownership，不改变内容。

### 5.2 `KafkaFetchAssembler`

输入 `ReadBatch` 必须：

- payload format Kafka；
- ranges increasing，committed source dense / compacted source sparse；
- each exact payload contains exactly one Kafka `RecordBatch`；
- payload base/last 等于 Nereus range；
- Kafka CRC valid；
- total bytes checked against response/hard limits。

assembler 分配 exact-size owned buffer，顺序拼接 batch payload，构造 `MemoryRecords.readableRecords`。不改 offset、
CRC、compression。返回：

```text
records
actualFirstBatchBaseOffset
nextLogicalOffset
sourceCoverageEndOffset
firstEntryOverflow
virtualSegmentBaseOffset
relativeLogicalBytePosition
abortedTransactions
```

start 落入 batch 中间时返回完整 batch；Kafka client iterator 按 requested offset 过滤。storage trim/logStart
已屏蔽不可见前缀，assembler 不返回完全位于 logStart 之前的 batch。

### 5.3 Current Nereus-side implementation（2026-07-23）

当前 `nereus-kafka-adapter` 已实现且由 `f9M3CodecTest` 验证：

- `KafkaRecordBatchCodec`：从调用者 `ByteBuffer.position..limit` 解析一个或多个 magic-v2 batch，保持调用者
  position/limit/order 不变；逐 batch 拷贝 exact owned bytes；严格校验 61-byte minimum、declared length、CRC32C、
  base/last offset、compression id、physical record count、timestamp 和 producer-field coherence；
- `KafkaRecordBatch`：只暴露校验后的 header/range/producer facts，所有 byte array/buffer accessor 都是 defensive
  owned/read-only view；
- `KafkaAppendBatchEncoder`：要求 dense first-base/next-base chain，每个 Kafka batch 对应一个
  `AppendEntry`，保留 exact bytes，entry `recordCount` 取 logical offset span，event hint 取 normalized max
  timestamp，生成 concatenated CRC32C；M4 已允许 codec 验证后的 idempotent/transaction/control batches，原始
  producer/transaction/control facts 只存在于 exact batch bytes，语义校验仍由 fork stock state machine 持有；
- `KafkaAppendResultValidator`：逐项验证 stream/range/end/format/record count/entry count/logical bytes 和空
  schema/projection；
- `KafkaFetchAssembler`：只接受 range 与 raw batch header 完全一致的 Kafka payload；COMMITTED 要求 dense，
  TOPIC_COMPACTED 允许 non-overlapping sparse；按 hard byte limit 分配 exact owned output，同时返回 actual first base、
  logical cursor、coverage、first-overflow、virtual position 和 aborted-transaction facts。
- `DefaultKafkaPartitionStorage`：构造时要求 acquired session 与 recovery frozen authority/session 完全一致；每
  partition append lane 以 `AppendPrecondition.expectedStartOffset` 串行提交；exact stable result 后才原子发布
  `logStart <= LSO == HW == stable LEO`；`KNOWN_NOT_COMMITTED` 清空未执行 successor 并回退 admission 到 stable end，
  uncertain/known-committed/result mismatch 则 write-fence；read 使用 COMMITTED + CONTAINING_ENTRY + explicit
  first-overflow semantics，并按 captured stable upper bound 裁剪完整 batch；storage 周期调用 public
  `renewAppendSession(exactCurrentSession, ttl)`，只接受 stream/writer/epoch/token 相同且 leaseVersion/expiresAt 均严格
  增长的结果。renew exception、null/invalid result 或后续 scheduler rejection 都立即进入
  `WRITE_FENCED_RECOVERY_REQUIRED` 并发布一次 `LEADERSHIP_LOST`；已进入底层的 head append 按真实 outcome 收口，
  但所有尚未 dispatch 的 successor 以 `FENCED_APPEND` 失败。resign 取消 scheduled renewal、停止 admission 后等待
  lane drain；共享 scheduler 的生命周期仍归 runtime，不由 partition storage 关闭。
- `KafkaByteBudget` / `KafkaProduceBufferSnapshot` / `KafkaBoundedAppendExecutor`：在 queue admission 前取得全局
  byte lease 并复制 caller remaining bytes；public inspection view 保持 read-only，但 accepted task 独占同一份已计费
  writable bytes，供 stock `LogValidator` 原地分配 offsets，不产生 budget 外第二份 copy。byte/queue saturation 均在
  append I/O 前返回 `KNOWN_NOT_COMMITTED`，所有 terminal/race path release once；client future cancel 不会取消已经
  入队的 append task，executor close 则拒绝新 admission 并 drain 已接受任务；byte lease 先 release 再完成
  client-visible future，因此 terminal observer 不会看到已完成 operation 仍占用 owned-buffer budget；
- `KafkaAppendFailureClassifier`：生成 protocol-neutral `KafkaAppendFailureDisposition`；只有显式
  `KNOWN_NOT_COMMITTED` 能保持 writable，authority/offset conflict、缺失 outcome、`MAY_HAVE_COMMITTED` 和
  `KNOWN_COMMITTED` 一律进入 `WRITE_FENCE_RECOVERY_REQUIRED`，checksum/format/invariant failure 进入
  `CORRUPT_OFFLINE`。Kafka exception class 映射仍由 fork 持有。
- `KafkaStorageProfilePolicy`：只允许五个已激活 canonical profile；durability 必须等于 profile default，completion
  必须是 `PROFILE_DEFAULT`。`DefaultKafkaPartitionStorage` 只从该 policy 构造 `AppendOptions`，不会根据 request
  `acks` 改写底层 success predicate；legacy `OBJECT_WAL` alias 和显式 weakened policy 在 I/O 前拒绝。
- `KafkaListOffsetsResolver`：把 `EARLIEST`、`LATEST`、`TIMESTAMP`、`MAX_TIMESTAMP` 收敛到一个
  leader-epoch-fenced boundary。每次请求只捕获一个 `KafkaStableSnapshot`；timestamp 类查询从该 snapshot 的
  `logStartOffset` 分页读取到冻结的 `stableEndOffset`，同时强制 records、bytes、read-operation 与 deadline 四重
  上限。`KafkaRecordTimestampInspector` 是 fork-owned exact-record seam：adapter 只交付 read-only exact Kafka
  bytes 与 minimum offset，由 fork 使用 stock `MemoryRecords` 迭代压缩/非压缩 records。命中必须位于本页且
  timestamp query 不得低于 target；预算耗尽、无进展、并发 trim、inspector 越界或扫描中 authority 丢失均失败，
  不返回近似 offset。若一次 outer read 已接收至少一个完整 batch，而剩余 strict byte/record budget 不能容纳
  下一个不可拆 batch，后续空 source page 只终止当前 outer page，resolver 从已返回 batch 的 end offset 发起
  下一页；首个 source page 为空仍按 `READ_RESOLUTION_FAILED` fail closed。max timestamp 相等时选择最低
  logical offset。

adapter 测试 oracle 是 test-only `org.apache.kafka:kafka-clients:3.9.0`，与锁定 AutoMQ `3.9.0-SNAPSHOT` reference
format 对齐；该依赖不进入 adapter production/runtime classpath。Kafka fork 本身则以显式隔离 repository/version
消费 exact F9 development modules，并已在 local fork `c27305a7ad` 落地
`NereusRecordTimestampInspector`、`NereusListOffsetsBridge`、`NereusListOffsetsScanConfig` 和
`NereusKafkaExceptionMapper`，并通过 Kafka-only `LeaderEpochAwareOffsetLookup` 接入 stock `Partition`/
`ReplicaManager` request path。`NereusListOffsetsLifecycle` 包装 product-owned manager，在 manager 返回 fully recovered
writable storage 后构造 resolver/bridge 并安装到相同 leader epoch；它不创建第二份 storage，也不接管 durable
authority/recovery。第五个 commit 另加入 `AsyncTopicDeltaLifecycle`、`NereusTopicDeltaLifecycle` 和 optional
`BrokerMetadataPublisher` routing；第六个 commit 注册 58-key config surface、immutable typed snapshot 和 enabled-only
cross-Kafka validator；第七个 commit 增加 explicit stock-compatible runtime factory、publisher lifecycle injection、
pre-unfence ready wait 和 ordered drain/close；第八个 commit 增加 adapter-backed runtime、typed creator factory、
exact ReplicaManager metadata lifecycle binding 和 lookup-only drain；第九个 commit 增加 closed runtime/product
configuration mapper 与四个 deterministic tests；第十个 commit 增加 Kafka Clock/KRaft snapshot adapters、
borrowed scheduler boundary、one-time recovery-state factory bridge、deferred broker-epoch/runtime lifecycle 和 production
factory composition。第十二个 commit `672429d94f` 增加 `NereusKafkaRecoveredState`、
`NereusKafkaRecoveryStateCodec`、`NereusKafkaRecoveryStateFactory`：每个 entry 必须是一个 exact magic-v2
`RecordBatch`，`validBytes`/CRC/稠密 records/offset span/压缩迭代/timestamp/leader-epoch ranges 均由 stock Kafka
类型校验；冻结状态只在 exact current leader `Partition` 的短 write-lock 临界区 provisional 安装。open 的 final
source revalidation 失败时 topic lifecycle 会撤销该 epoch 的 lookup 和 state，成功后才通知 coordinator leader-ready。
M3 明确拒绝 idempotent/transaction/control batch 与任何 NKC1 derived-state section，M4 才接管这些语义。
第十三个 commit `9a6ebed6d9` 把 `Partition` 持有类型收窄为 stock `LeaderEpochAwareRecoveryState`；
artifact-only `NereusKafkaRecoveredState` 实现该接口，因此 disabled build 不加载/编译任何 Nereus package，同时
enabled build 仍在 Partition lock 内校验 exact topicId/topic-partition/leader epoch/frozen facts。
第十四个 commit `cfcdd55fbc` 增加 stock `UnifiedLogFactory`/`UnifiedLogOpenContext`、LogManager factory
delegation、runtime-owned `NereusUnifiedLogFactory`、ephemeral `NereusUnifiedLog`/`NereusLocalLog` shell，以及
recovered-state → recovered-storage → ListOffsets lookup 顺序发布；第十五个 `7739351b7c` 补齐新 stock seam 的成对
inject marker；第十六个 `dc8c66388a` 增加 optional `RequiredAcksAwareAppend`、stable
`NereusLocalLog` callback、`NereusUnifiedLog` append/read bridge、post-stable fence 与 focused tests；第十七个
`ee608625e4` 增加 stock `BrokerStorageAppendExecutor`、product-backed keyed executor wrapper、ReplicaManager
Produce aggregation 和 drain composition；第十八个 `bba3ef0121` 增加 stock `BrokerStorageFetchExecutor`、
product-backed whole-request wave executor、ReplicaManager async Fetch completion 和 combined drain composition；
第十九个 `47d36a1d9f` 保证内部 control queue 能保留每个已接纳 operation 的一个 runner，修复同时 wakeup 时把已接纳
Fetch 错误降级为 backpressure 的竞态，但不扩大 `threads + queueCapacity` logical admission。第二十个隔离本地
commit `ec7f0db991` 增加 stock producer-entry exact restore seam、in-memory producer/transaction state、
full NKC1 hydration + committed-tail replay、stable-end-to-HW/LSO publication 和 READ_COMMITTED aborted filtering。
第二十一个隔离本地 commit `032974067c` 增加 stock ReplicaManager transactional guard/TV2 marker-version executor
handoff，以及 group/transaction internal-topic recovered-storage-before-election 回归。
第二十二个 `4c060aec89` 把 stock `Partition.deleteRecordsOnLeader` 的 policy/leader/HW normalization 接到
checkpoint-before-trim barrier，并在 durable trim 后推进 `UnifiedLog.logStartOffset`。第二十三个
`feabf6c686` 增加 fork-owned writable partition 枚举、partition-lock capture/local-log updater 和周期性 retention
runtime bridge；对应 product commits `3eb6b63`、`57dcf35` 组合 checkpointed maintenance 与 bounded scheduler。
第二十四个 `378e9f8967` 增加 `NereusCanonicalLogState`、多 virtual-segment shell、stable-only roll、exact KRaft
config metadata offset history、checkpoint + committed-tail 恢复、logical/time indexes、真实 offset metadata 和
bounded timestamp lookup。focused UnifiedLog/recovery/config regression、Checkstyle 与 SpotBugs 均通过。
第二十五个 `58342d9dca` 把 58-key typed config 映射为 bounded compaction runtime，复用
`NereusKafkaOwnedPartitionSourceBridge` 枚举 current leaders，并在 `Partition.leaderIsrUpdateLock` 下冻结
canonical source/HW/LSO 与 producer/transaction state。选择 exact decision horizon 后，`NereusUnifiedLog`
使用 stock `CleanedTransactionMetadata` 扫描 dense RecordBatch，逐 control marker 冻结
`RETAIN_REQUIRED/DELETE_ELIGIBLE`，保留 active producer last marker，并在返回前再次校验 source 和
producer/transaction image。该 branch 已通过 SSH 推送到 `nereusstream/kafka`，工作 clone 为
`/Users/liusinan/apps/ideaproject/nereusstream/kafka`。第二十六个 `faaffc8a75` 以 stock-only maintenance
interfaces 恢复 artifact-free stock compilation；第二十七个 `3bd92c7244` 新增显式 native-storage launcher，
并把 production broker factory 传进共享 Kafka/KafkaRaftServer lifecycle；第二十八个 `9773c8f817` 新增
stock-owned controller runtime/publisher seam、artifact-only activation scheduler，并把 production controller factory
接进同一 launcher lifecycle；第二十九个 `d23dc5c787` 新增 durable storage feature、enabled-only advertisement/
format、dedicated-controller validation、activation feature wait 和 single-copy controller policy；第三十个
`5ebf31cde8` 修正 aggregate `core:spotlessCheck` 捕获的 controller runtime test import order；第三十一个
`ecde6964c5` 为 Nereus authoritative cache root 创建/校验 KRaft V1 `meta.properties` 与 non-reserved directory ID，
使 broker registration 能携带有效 directory identity；第三十二个 `50b46aab2d` 新增 stock-owned
`NereusKafkaBookKeeperConfig`、97-key `ConfigDef`、BookKeeper cross-field validation、WAL/runtime/capability mapping、
password-file exact identity、fork-owned client construction，以及 product-first/client-second idempotent close wrapper。
第三十三个 `80445853a3` 把同一已安装 Object provider 显式暴露为 sync/async 两个 executable profile，并保留所选
Object profile 作为 default；第三十四个 `116052aa53` 将 BookKeeper async/sync 映射到同一
BookKeeper-primary + Object-materialization graph，暴露完整五档 capability/default，并把 staging 目录锚定到
authoritative cache root；第三十五个 `b443750be4` 增加六个 ledger-GC rollout key、typed snapshot/cross-field
validation、compatibility-digest binding 以及 `BookKeeperLedgerGcConfiguration` 映射。第三十六个
`5169b57986` 增加三个 materialization retirement lifecycle key、`RetentionCompaction` typed snapshot/
cross-field validation、`MaterializationConfig` 映射与 compatibility-digest binding；第三十七个
`33f988a83e` 让 stock `KafkaConfigTest` 明确识别 profile-conditional optional fields，并让 enabled validator/
runtime-factory fixtures 显式选择 Object profile，避免默认 BookKeeper profile 在目标断言前因缺失 identity 失败。
第三十八个 `a1b1e3482d` 修复 development-artifact 运行时依赖边界：BookKeeper 4.18 将 SLF4J API 提升到
2.x，因此 `core` 在 Nereus mode 排除 `log4j-slf4j-impl`、`logback-classic` 和 storage dependency
传递的 provider，显式安装 `slf4j-api:2.0.17` 与 `log4j-slf4j2-impl:2.25.3`；provider 自身的 API
传递依赖被排除，`releaseTarGz.eachFile` 同时移除 Kafka 原 `slf4j-api:1.7.36`。发行包必须恰好包含
上述一个 API 和一个 provider。`NereusUnifiedLogFactoryTest.enabledProperties` 同时显式选择
`OBJECT_WAL_SYNC_OBJECT`，使 log-factory 测试不再隐式依赖默认 BookKeeper profile。
第三十九个 `ebf1d76163` 补齐 `StorageToolTest.nereusControllerProperties` 的默认
`BOOKKEEPER_WAL_ASYNC_OBJECT` 身份：deployment、cluster alias、provider-scope SHA-256、ledger-prefix value、
reservation、password file/version 与 readiness SHA-256。`StorageTool` 仍在读取配置并格式化
`nereus.storage.version=1` 前构造完整 typed snapshot；测试不再靠缺字段绕过该 fail-closed 校验。
在相同 fork head 和当前 product source 上，2026-07-28 fresh partial aggregate 同时运行
`phase9M3KafkaForkCheck`、M5 compaction core、M6 activation metadata、Kafka feature 与 checkpoint quarantine：
outer 109/109 tasks 全部重新执行成功；nested stock/artifact-enabled Kafka 构建分别通过 92/92 与 95/95
actionable tasks，后续 feature/control focused builds 通过 86/86、42/42、74/74 与 20/20。该证据确认新的
enabled-format fixture、source lock 和当前 product planner/physical-deletion slice 能在同一次 clean task
selection 中组合；它不替代尚未实现的 two Kafka-process takeover、multi-controller 和 M7 final aggregate。
Later release gates now provide two-process data takeover and ACTIVE-state three-voter controller failover；this earlier
partial aggregate still does not substitute for the later provider-applied activation-cut gate、remaining pre-CAS cuts or
M7 final evidence。
默认 disabled/dry-run
不会启动 scanner；enabled/non-dry-run 只有在 materialization runtime 已激活时才允许创建 retention service。
Product adapter 现已把 provider-neutral deletion coordinator 下沉到 `nereus-bookkeeper`，并新增 Kafka-owned
`KafkaBookKeeperStreamCoverageProofProducer`。它完整扫描 64 个 binding shards 与 64 个 F4 registration
shards，重读 authoritative root，核对 ACTIVE BookKeeper binding 与 exact L0 profile/lifecycle/trim/stable end，
并要求 async/sync 使用 `DirectMaterializationStreamAuthority`；WAL-only 必须没有 object registration。一次性
`KafkaBookKeeperDeletionActivationService` 在 materialization start 后读取 publication activation metadata
version，以同一 broker-readiness 生成 scope/root/`NBKKAFKASTREAM1` stream proof，并由通用 coordinator 一次
CAS 安装三份 digest 与 deletion bit；retention scanner 只在该步骤成功后启动。任一 shard pagination、
hint/root、L0、registration、namespace、readiness 或 activation version 漂移都使启动失败。确定性测试已通过，
`f9BookKeeperLedgerDeletionProviderIntegrationTest` 已在 real Oxia + two bookies 上覆盖 adapter-level
物理删除：BookKeeper async append/rollover、NCP2 COMMITTED、terminal materialization-source protection
retirement、activation-guarded retention、provider ledger absence 和删除后的 NCP2 byte-exact read 全部通过。
该 gate 现在还从 `NereusKafkaBookKeeperWalRuntimeContext` 注入一个只在 runtime start 后 armed 的 narrow
`BookKeeperClientOperations` decorator；它要求目标 Kafka WAL ledger 的 provider delete 已成功后才返回失败，
并断言被注入 ledger ID 与最终退休 ledger 完全相等。Retention 随即重读 provider metadata、记录 first absence、
等待 late-create audit grace，再重验 namespace/activation 与 second absence 后发布 `DELETED`。默认三参数
context 仍创建 `DefaultBookKeeperClientOperations`，fork mapper 和正常 provider gate 不需要改变。
它还锁定 Kafka logical format `KAFKA_RECORD_BATCH_V1` 必须 canonicalize 为 generation index payload format
`KAFKA_RECORD_BATCH`，否则 terminal task 会永久保护旧 ledger。
`f9BookKeeperWalAsyncObjectProcessIntegrationTest` 又用真实 release tarball 强制单 entry rollover，等待 ledger
root `DELETED` 并以独立 client 证明 `NoSuchLedger`；首 JVM 正常停机后，fresh JVM 从 NCP2 读取 offset 0
并继续 append/fetch/ListOffsets。Provider-level applied-delete response loss 已覆盖；release-process
response-loss restart 和 multi-broker takeover 仍需后续 process gate，
当前不能把这条 R-tier evidence 等同于完整 KF-RET-009。
An independent Object-WAL gate now starts two product runtime ownership graphs against the same real Oxia and provider
root。Both broker capability records participate in ACTIVE readiness。Broker B's higher leader epoch preempts broker A's
still-live durable session，replays A's exact committed batch and continues at the recovered end；A's next old-token append
is rejected and the local storage enters `WRITE_FENCED_RECOVERY_REQUIRED`。The companion deterministic regression ensures
the known-not-committed outcome cannot override an authority/session/head fence。This gate is
`:nereus-kafka-adapter:f9MultiBrokerTakeoverProviderIntegrationTest` and is included in `phase9M3ProviderCheck`。The
separate `f9InFlightTakeoverProcessIntegrationTest` supplies the release-process already-dispatched Object-WAL cut；
the BookKeeper three-profile post-handoff matrix and common provider-applied C cut also pass；ACTIVE-state multi-controller
kill failover、the complete six-way readiness-create/PREPARED-create/ACTIVE-CAS store-publication takeover and actual Oxia
transport-reset recovery now pass separately，while initial empty-cluster snapshot/proof and capability-aggregation process
cuts plus coordinator/checkpoint variants remain open。
真实 combined-node
KRaft/Oxia/S3 process baseline 已通过；
同节点 fresh-JVM cold restart 也已通过；独立 BookKeeper WAL-only/async/sync release-distribution
首启/冷重启 gates 也在同一 fork head 通过。这些 gates 没有给 Kafka 引入 Pulsar metadata runtime：
BookKeeper client/bookies 使用 stock
ZooKeeper `LongHierarchicalLedgerManagerFactory`，Nereus durable authority 继续使用 Oxia。Object-WAL singleton 与
already-dispatched append live takeover、BookKeeper three-profile post-handoff takeover and Bookie-acked/pre-publication
stale completion fencing 已覆盖；ACTIVE-state three-voter controller kill/reconciliation 也已覆盖；PREPARED/
response-loss controller、coordinator/checkpoint cuts、kill-cut matrix 与完整 stock cleaner differential matrix 尚未实现。

## 6. Produce execution and threading

### 6.1 Why executor handoff is mandatory

`UnifiedLog.appendAsLeader` 是同步 API；`NereusLocalLog.append` 必须等待 stable storage result，才能让 stock
code 更新 ProducerStateManager。直接在 network request thread join 会把 object/BookKeeper latency 变成 request
handler starvation。`ee608625e4` 因此在 stock `ReplicaManager.appendRecords` 进入 local append 前完成
owned-buffer handoff 并提交到 runtime-owned bounded append executor。

同步 wait 仍保留在 executor worker 内部，以锁定 validation → stable bytes → LEO/derived state 的顺序。request
thread 在所有 partition submit 返回后立即退出；只有 request-wide validation failure 或全部同步 admission
failure 可以在 caller 上直接完成 response。

### 6.2 Buffer handoff

`validateRequest` 和 `NereusBrokerStorageAppendExecutor.submit` 在 request thread：

- 先用 checked long arithmetic 计算整请求 bytes 并与 `append.request.bytes` 比较，失败时不 submit 任何 partition；
- 从 product-owned bounded pool 申请 buffer；
- copy exact `MemoryRecords.buffer`；
- worker 从同一份 budgeted snapshot 构造独占 writable `MemoryRecords` view，允许 stock validation 原地写 offsets；
- validation 后交给 Nereus partition storage 的 payload 再使用 exact read-only duplicate；
- worker 使用 `RequestLocal.noCaching` 执行 stock validation，绝不跨线程使用 request-thread BufferSupplier；
- future terminal callback 后 release once。

queue admission 失败发生在任何 append IO 前，返回 `ThrottlingQuotaExceededException`；已入队后 client cancel
不能取消底层 append，因为可能已经提交。response callback 仍 exactly once，channel lifecycle 决定是否发送。

当前 Nereus-side 实现使用 `KafkaProduceBufferSnapshot.capture(ByteBuffer, KafkaByteBudget)` 精确复制 caller
`position..limit`，不修改 caller state；lease 先于 array allocation 取得，allocation/copy、queue reject、task failure、
success 和 duplicate close 都只释放一次。`KafkaBoundedAppendExecutor` 的逻辑 admission 上限是
`executorThreads + executorQueueCapacity`；同一 ordering key 进入一条 FIFO lane，不同 key 可以并发。每个 runner
只执行一个 work item 后重新排到 pool tail，防止单线程配置下 hot partition 永久饿死其他 partition。关闭 race
返回 `STORAGE_CLOSED + KNOWN_NOT_COMMITTED`；close 只停止外部 admission，已接纳但仍位于逻辑 lane 的 work 可继续
内部 reschedule，最后一个 terminal 才 shutdown pool 并完成非可变 `drainedFuture` view。returned future 只是
response handle，取消它不会传播到 admitted task。

fork wrapper 以 exact `TopicIdPartition` 作为 key，把 owned bytes 重建为 `MemoryRecords.readableRecords`，并把
Nereus rejection/failure 映射成 Kafka `ApiException`。inflight byte budget 与 logical task capacity 在每个 submit
时再次校验。

### 6.3 Append ordering

每 partition 同时只有一个 `NereusLocalLog.append`：

- Kafka `UnifiedLog.lock` 序列化 validation/offset assignment；
- adapter `PartitionAppendLane` 序列化 calls；
- Nereus `StreamLane` 与 head CAS 是底层保证；
- executor 可以并发不同 partitions，不允许同 partition reorder。

等待发生在 append executor，不持有 ReplicaManager global lock。worker 进入
`Partition.appendRecordsToLeader` 后只持有当前 UnifiedLog partition lock；Nereus completion path 不得反向获取
该 lock。metrics、validation stats、delayed-produce eligibility 与 response completion 都在 stable append terminal
后执行。

### 6.4 Stock state ordering retained

locked Kafka baseline 的关键顺序：

```text
analyze/validate records
assign offsets / leader epoch
maybe roll
analyze producer state (no durable state update)
NereusLocalLog.append                 stable wait here
update local LEO
update ProducerStateManager
update transaction index / LSO
publishDerivedOffsets(exact LEO, HW, LSO)
Partition maybeIncrementLeaderHW
Produce response eligibility
```

因此 stable append 失败时 producer state/LEO 尚未推进。virtual segment roll 可能已创建 empty descriptor；recovery
按 committed head 删除/复用该空 descriptor，不能把 roll metadata 当成 data commit。

### 6.5 Acknowledgement semantics

| required acks | F9 behavior |
| --- | --- |
| `0` | no protocol response，but append still waits stable on executor and updates state only on success |
| `1` | success after stable append + stock state update |
| `-1` | RF/minISR=1；same stable append then HW gate completes |
| other | stock `INVALID_REQUIRED_ACKS` |

F9 不提供比 selected Nereus profile completion policy 更弱的 Kafka success。profile 若要求 sync Object evidence，
Produce 也等待；profile immutable，不能按 request acks 改写。

当前 Nereus-side `KafkaStorageProfilePolicy` 将 activated set 显式冻结为
`OBJECT_WAL_SYNC_OBJECT`、`OBJECT_WAL_ASYNC_OBJECT`、`BOOKKEEPER_WAL_ONLY`、
`BOOKKEEPER_WAL_ASYNC_OBJECT`、`BOOKKEEPER_WAL_SYNC_OBJECT`。每档只允许
`storageProfile.defaultDurabilityLevel() + PROFILE_DEFAULT`；最终 success predicate 仍由 core profile resolver
权威解析。`KafkaStorageProfilePolicyTest` 是该 mapping 的 deterministic 证据，不替代 KF-APP-016 要求的五档真实
provider/KRaft Produce matrix。

## 7. LEO, HW and LSO

`NereusUnifiedLog` 暴露：

```scala
def stableEndOffset: Long
def stableEndOffsetMetadata: LogOffsetMetadata
def recoveryState: KafkaPartitionState
```

不变量：

```text
logStartOffset <= LSO <= HW <= stableEndOffset == LEO
```

Serialized path 中 durable stable end 与 LEO 同步前进，但新 stable bytes 在 stock derived state 完成前继续受旧
HW/LSO 限制。HW update：

- leader open 完成时从 stable head 初始化；
- append stable 后 `Partition.maybeIncrementLeaderHW` 取 stable end；
- 绝不使用 future/in-flight expected end；
- HW 只能单调增长且不能越过 current leader authority session；
- old leader completion callback 在 authority 失效后不能更新 installed log。
- `STABLE_APPEND` wakeup and next same-partition dispatch occur only after exact derived-offset publication。

LSO 继续由 stock ProducerStateManager/first unstable offset 算法计算，recovery 从 checkpoint + committed replay
重建。`read_committed` upper bound 是 min(HW,LSO)。

### 7.1 Current process-local leader arbitration（2026-07-23）

`KafkaLeaderAuthority` 精确映射文档 04 的 external authority tuple：`leaderEpoch` 是 authority epoch，decimal
`leaderId` 是 owner ID，KRaft broker registration epoch 是 owner epoch。`KafkaPartitionLeaderManager` 在
`KafkaPartitionOpener`（authority acquire + recovery + storage construction owner）之外再提供 process-local publish gate：

- exact authority 的并发 open 共享一个 operation-owned future；外部 cancel/complete/obtrude 不能改变 open；
- higher leader epoch 立即 supersede；同 leader/leader epoch 只有 higher broker epoch 能 supersede；lower term、同
  leader epoch 的 conflicting owner 和 non-dominating broker term 都 fail closed 为 `FENCED_APPEND`；
- supersede 立即从 `current` 移除并 resign 旧实例；多个 authority open 可以在底层并发，但只有仍等于 desired term 的
  future 能安装；迟到 storage 会被 resign 并以 fenced 完成，绝不重新成为 current；
- opener result 必须 identity、leader epoch、`LEADER_WRITABLE` 全部吻合，否则作为 metadata invariant 拒绝并关闭；
- stale resign notification 是 no-op，不能关闭新 term；shutdown 停止新 open、resign 已安装实例，并让迟到 open
  走 superseded close path。

该 manager 不替代 durable Oxia/head authority CAS；`KafkaPartitionOpener` 必须先完成文档 04 的 session acquisition
和 fresh recovery。Kafka fork metadata callback、BrokerServer config/runtime factory、manager ownership、ordered
shutdown wiring、显式 native-storage CLI selection 与 controller-leader-only first-activation scheduling 已实现；
combined-node native-storage KRaft process baseline 已通过；real-Oxia two-runtime Object-WAL gate 已覆盖 durable
live preemption/replay/old-token fencing；two-release-process singleton reassignment 与 three-release-process
Object-WAL old in-flight append cut，以及 BookKeeper three-profile two-release-process post-handoff matrix 也已通过。
The common BookKeeper `WRITING` C cut also passes through the test-only agent gate。真实 ACTIVE-state controller kill
failover also passes。The dedicated activation fault-agent process test additionally covers before-provider and
after-provider readiness-create、PREPARED-create and ACTIVE-CAS cuts；the Oxia-proxied process gate additionally closes actual
transport reset/retry；initial empty-cluster snapshot/proof and capability-aggregation process cuts plus
coordinator/checkpoint cuts 尚未实现。

### 7.2 Shared-storage singleton reassignment and local lifecycle（2026-07-28）

Nereus feature level 1 的 assignment contract 是“一个 KRaft leader + 一个 shared durable log owner”，不是 stock
replicated local log。`ReplicationControlManager.alterPartitionReassignment` 在该 feature 下必须调用
`changeNereusPartitionReassignment`，不得构造 stock `ReassignmentReplicas` 的
`replicas=[old,new], adding=[new], removing=[old]` 中间状态。新方法按以下顺序执行：

1. 要求当前 partition 为 stable RF1：`replicas.size == 1`，且不存在 adding/removing replica；
2. 要求 request target 恰好一个 broker，并由 `clusterControl.isActive` 验证已注册、未 fenced；
3. 如果 target 等于当前 singleton，返回 no-op；
4. 否则建立一个 `PartitionChangeBuilder`，同时设置 target singleton replicas、ISR、preferred election，并清空
   adding/removing replicas；
5. 生成一条 `PartitionChangeRecord`，由同一个 metadata transaction 原子发布新 replicas/ISR/leader。

这里使用 active-broker predicate，而不是 stock `LeaderAcceptor` 的旧 replica directory 校验：shared-storage
新 owner 在 handoff 前没有旧 local partition directory identity。Feature level 0/absent 继续执行 stock path。
`ReplicationControlManagerTest.testNereusStorageFeatureAtomicallyHandsOffSingletonReplica` 锁定 record 形状，
原有 `testNereusStorageFeatureGatesIsrReassignmentAndDirectories` 继续锁定非 singleton ISR/reassignment 和
directory API 的 fail-closed 行为。

broker metadata lifecycle 还有一个不能从方法名推断的边界：
`TopicsDelta.localChanges(brokerId).deletes()` 只表示“从本 broker 的 local image 删除”，不必然表示 durable
topic/partition 删除。`NereusTopicDeltaLifecycle.apply` 必须用 previous image 取得 exact
`(topicName, topicId, partition)`，再查询 new image：

- new image 仍含同名、同 topic ID、同 partition：这是 reassignment 后的 local replica removal，调用
  `partitionLifecycle.resign(previousIdentity, newLeaderEpoch, ...)`，回调 `Some(newLeaderEpoch)`，不得删除 binding；
- new image 不含该 identity：这是 durable delete，调用
  `partitionLifecycle.delete(previousIdentity, metadataOffset, ...)`，回调 `None`；
- 同名但 topic ID 已变化：仍按 old identity durable delete，随后同一 partition lane 串行 open 新 identity，保持
  delete-before-recreate。

真实进程回归使用 node 1 combined controller/broker、node 2 broker-only。两 JVM 共享 Kafka cluster ID、
controller voter、Nereus cluster/Oxia/Object root，并隔离 KRaft metadata、Kafka log shell 和 Nereus cache。
测试在 `[1]` 上提交 offset 0，启动 broker 2 后通过 Admin 重分配到 `[2]`，等待 exact
`leader=2, replicas=[2], ISR=[2]`，要求 `listPartitionReassignments` 为空且 broker 1 仍存活，再提交 offset 1 并
读取 offset 0/1。`f9MultiBrokerTakeoverProcessIntegrationTest --rerun-tasks` fresh 通过 73/73 actionable tasks，
并由 `phase9M6KafkaProcessCheck` 聚合。它证明 post-handoff recovery/continuation，不证明 handoff 时已经进入
provider 的旧 append 只能由 current term publication，也不证明 coordinator/internal-topic、BookKeeper 或
multi-controller takeover；Object-WAL already-dispatched append 的 release-process cut 由
`f9InFlightTakeoverProcessIntegrationTest` 覆盖，BookKeeper profile 的 post-handoff 等价路径由
`f9BookKeeperProfileTakeoverProcessIntegrationTest` 覆盖；Bookie-acked、Oxia-`WRITING`、old-root-`SEALED`
等价切点由 `f9BookKeeperInFlightTakeoverProcessIntegrationTest` 覆盖。

## 8. Fetch execution

### 8.1 Async read path

`bba3ef0121` 已在 stock `ReplicaManager.fetchMessages` 注入 optional bounded fetch executor。每个 request
冻结 immutable fetch params/info，不携带 request-local buffer supplier；executor 每一 wave 调用完整 stock
`readFromLog`，最终由 `NereusUnifiedLog.read` 在 bounded worker 上等待 adapter future。request caller 在 bounded
admission 后返回，不再同步等待 Nereus storage。disabled `None` branch 与 stock read/purgatory behavior 完全一致。

read request mapping：

| Kafka fact | Nereus fact |
| --- | --- |
| fetch offset | `ReadRequest.startOffset` |
| `minOneMessage` | `ALLOW_FIRST_ENTRY_OVERFLOW` or legacy strict |
| max partition bytes | `ReadOptions.maxBytes` |
| max response bytes | shared assembler budget |
| LOG_END/HW/TXN_COMMITTED | upper bound LEO/HW/LSO in adapter request |
| compact policy | TOPIC_COMPACTED prefix + COMMITTED tail composition |

Storage reader不得越过 upper bound；若一个 containing batch 与 upper bound 相交但 last offset >= upper bound，
assembler 只在 Kafka isolation 允许整个 batch 时返回，否则停止。transaction filtering uses exact aborted-txn facts。

### 8.2 `NereusFetchOperation` state machine

```text
NEW
  -> READING
      -> COMPLETE       error / enough bytes / maxWait=0 / divergence
      -> WAITING        insufficient bytes
WAITING
  -> READING            stable-append/trim/leadership event, coalesced
  -> TIMED_READING      deadline
READING/TIMED_READING
  -> COMPLETE           callback exactly once
  -> WAITING            still insufficient and before deadline
any -> CANCELLED        broker shutdown only；release buffers/listeners
```

同一 operation 最多一个 in-flight read。event 只设置 dirty flag；read completion 若 dirty 再调度一次。`minBytes`
基于实际 assembled bytes，不用 offset delta 猜。timeout response 可以低于 minBytes，符合 Kafka；普通 event read
不足则继续等待。leadership/error event 立即完成 error response。

Operation 注册：

- partition stable-append listener；
- trim/logStart listener；
- leadership/fence listener；
- broker timer deadline。

terminal transition 先 CAS，后注销 listener/release buffers/callback。callback executor 不是 storage IO executor，
避免 slow response callback占满 read pool。

### 8.2.1 Current Nereus-side implementation（2026-07-23）

`KafkaPartitionStorage.subscribe` 与 `DefaultKafkaPartitionStorage` 已实现独立、幂等的 event subscription：

- stable append 只在 exact stable snapshot 发布后发 `STABLE_APPEND`；resign 发 `LEADERSHIP_LOST`；future trim/corrupt
  owner 使用 `LOG_START_CHANGED` / `CORRUPT_OFFLINE`；
- listener snapshot 在 partition lock 内冻结、在 lock 外调用；listener 抛出的任意 failure 都不能改变已完成 I/O；
- leadership/corrupt terminal event 先复制再清空 registration，operation cleanup 再 close 仍为幂等。

`KafkaFetchOperation` 已实现 `NEW -> READING -> WAITING -> TIMED_READING -> COMPLETE` 和显式 `CANCELLED`：

- 每个 request 的 immutable ordered partition list、`minBytes`、global `maxResponseBytes`、`maxWait`、
  `maxRereads` 在构造时校验；重复 partition 或 partition hard limit 大于 request limit 直接拒绝；
- 一个 trampoline control queue 在 caller 提供的 bounded read executor 上串行状态转换，避免 completed future 递归；
  partition reads 可并发，但同一 partition 的前一 future terminal 前绝不发第二次 read；
- event 只置 dirty；一个 read wave 中的多次 event 合并成下一 wave。minBytes 使用实际 included assembly bytes；
  maxWait 到期时执行最后一个 bounded read wave，reread safety budget 用尽时直接使用最新 frozen results；
- request-wide byte budget 按 partition request order 应用；included buffers 总和绝不超过 hard limit，无法容纳的
  non-empty partition 只暴露 `omittedForResponseBudget=true`，不把该 buffer 交给 fork；
- executor rejection 在第一次 storage read 前以 `BACKPRESSURE_REJECTED` terminal；leadership/corrupt/read failure
  fail closed；runtime cancel 取消 read future、deadline 和 listener；所有 terminal path 在独立 callback executor
  complete exactly once；外部 future cancel/complete/obtrude 不能绕过 operation cleanup。

`KafkaFetchOperationTest` 已为 KF-FET-004/005/016 提供 direct-adapter state-machine evidence。Kafka fork 的实际
request path 选择下面的 whole-request wave 状态机，以避免复制 stock per-partition semantics；真实 KRaft broker
process 仍未实现，因此不是 M3 completion claim。

直接把 fixed `KafkaStorageReadRequest.maxOffsetExclusive` 复用到下一次 event reread 会冻结首次 stable upper
bound，不能看到后续稳定 append；同时在 adapter wrapper 中逐项重写 `Partition.fetchRecords` 会丢失 stock leader
epoch、divergence、follower-state 和 request-order byte-budget 语义。为避免这两类错误，product 侧已增加
`KafkaFetchWaveOperation<T>`：

- `KafkaFetchWaveSource<T>.read(initialWave)` 把整次 stock `readFromLog` wave 保持为 opaque payload；`true` 只出现
  在 initial wave，后续 event/deadline wave 使用 delayed-fetch 等价的 side-effect mode；
- `subscribe(wakeup)` 必须在 initial read 排队前覆盖请求全部 partition，关闭返回 subscription 即注销全部 listener；
- control/read 在 caller 注入的 bounded executor 上串行，同一 request 最多一个 wave in flight；in-flight 期间任意
  数量事件只形成一个 dirty bit；
- `maxEventRereads` 只限制 event storm，绝不能吞掉 deadline final read；deadline 与 initial/event wave 竞态时，
  当前 wave terminal 后仍精确发一次 final read；
- `ToIntFunction<T>` 使用最终 stock records 的实际字节判定 `minBytes`；`Predicate<T>` 让 error、divergence、
  preferred replica 等 stock terminal fact 立即完成；
- operation-owned future 禁止 caller `cancel/complete/obtrude`；terminal 先取消 timer/read、注销 listener，再在独立
  callback executor 完成一次。

`KafkaFetchWaveOperationTest` 已覆盖 initial enough、signal coalescing/one-in-flight、event-reread budget 后 deadline
final read（包括 deadline 与 enough in-flight wave 的竞态）、executor rejection before source read、external
cancellation isolation/explicit cleanup。`NereusBrokerStorageFetchExecutor` 现把该状态机接入 fork：

- construction 从 typed Fetch config 创建 bounded read pool、单线程 bounded callback pool 与 operation semaphore；
  semaphore 的 `threads + queueCapacity` permit 从 submit 一直持有到 operation terminal，等待 `maxWait` 期间也占用；
- `subscribe` 对请求中的每个 current `KafkaPartitionStorage` 注册 exact event listener；暂时找不到 current storage
  时跳过监听，不把整个 request 提前失败，initial stock wave 会生成准确的 per-partition error；
- 每一 wave 对结果做 exact partition order/cardinality 校验；actual records byte sum 交给 `minBytes`，任何 partition
  error、divergence、preferred replica 或 remote fetch fact 触发 terminal；
- wrapper failure 通过 `NereusKafkaExceptionMapper` 转成按 partition `LogReadResult`，不绕过 stock response schema；
  worker `finally` drain `defaultActionQueue`，final shared completion 只标记一次 fetch metrics并调用一次 callback；
- `close()` 先停止新 admission，不取消已接纳 operation；`drained` 只在全部 operation terminal 后完成且不能被
  caller 修改。runtime `beginDrain/awaitDrained/close` 同时组合 append executor、fetch executor 和 product runtime。

stock `ReplicaManagerTest` 已用 two-partition request 证明 deferred closure/response、order、per-partition error 与
action-queue drain；artifact tests 证明 event reread/listener cleanup、logical-cap rejection、accepted deadline 与
combined runtime drain。这里已经是 request-path implementation evidence，剩余的是从 production CLI 启用的真实
KRaft broker/process evidence。

### 8.3 Virtual `LogOffsetMetadata`

F9 不伪造 local file position。`KafkaVirtualPositionIndex` 从 committed append facts/checkpoint 维护：

```text
segmentBaseOffset
logicalBytesBeforeSegment
cumulativeLogicalBytesAtEntryStart
```

返回 relative position = checked/capped difference，供 Kafka delayed fetch/metrics comparison；它是 derived index。
缺失时返回 message-offset-only metadata，任何要求 exact physical position 的 path fallback 到 offset comparison，
不能读取 placeholder file。recovery replay会重建 position index。

## 9. ListOffsets and lookup paths

必须覆盖的不只是 Produce/Fetch：

- earliest → binding/stream logStartOffset；
- latest → stable end；
- max timestamp / timestamp lookup → `NereusTimeIndex` verified checkpoint + committed tail scan；
- leader epoch lookup → derived epoch cache + tail replay；
- offset snapshot → logStart/LSO/HW/stable LEO in one partition-lock snapshot；
- transaction aborted lookup → `NereusTransactionIndex`；
- DescribeLogDirs → explicit Nereus virtual storage descriptor，不伪装 local bytes；
- disk alter APIs → unsupported for Nereus mode；
- remote/tiered log API → disabled，because all Nereus bytes already shared primary/higher-generation storage。

timestamp lookup 若 checkpoint index 没有候选，bounded scan committed entries；scan 预算/timeout 超限映射 storage
error，不能返回一个未经证明的 nearby offset。

当前 adapter 已实现 `KafkaListOffsetsRequest` / `KafkaListOffsetResult` / `KafkaListOffsetsResolver`：earliest/latest
直接取同一 stable snapshot 的 log start/end；timestamp/max timestamp 则在同一冻结区间内执行 exact committed-tail
分页扫描。adapter 不解析 record payload，也不把 batch base 伪装成 first-record result；它通过
`KafkaRecordTimestampInspector` 把每页 read-only exact bytes 交给 fork 的 stock `MemoryRecords` iterator，并校验
返回的 offset/timestamp 仍在该页证明范围内。扫描期间 stale epoch、resign 或 write-fence 会返回 `FENCED_APPEND`；
并发 trim 返回 `OFFSET_TRIMMED`；records/bytes/read-count 预算耗尽返回 `METADATA_LIMIT_EXCEEDED`；deadline 到期返回
`TIMEOUT`；首个 source page 空且无进展返回 `READ_RESOLUTION_FAILED`。若当前 outer read 已返回至少一个完整 batch，
剩余 strict budget 导致的空 source page 结束该页而不丢弃已验证 bytes，下一次 bounded resolver read 从精确
`nextLogicalOffset` 继续。上述错误都不会降级成近似 offset。

`NereusTimeIndex` verified checkpoint candidate 尚未接入，因此当前 resolver 从冻结 log start 扫描，属于正确但受硬
预算限制的 fallback。local Kafka fork 的 `NereusRecordTimestampInspector` 已用锁定 4.3 stock
`MemoryRecords.readableRecords(...).batches()` 实现 exact record iteration，保持 caller buffer state，不跳过
minimum offset，并对 max timestamp 做 lowest-offset tie-break。`NereusListOffsetsBridge` 把 `-2/-1/-3` 与非负
timestamp 映射成 adapter query，复用 Kafka 已有 `AsyncOffsetReadFutureHolder` / delayed-operation wakeup contract，
把取消传回 resolver future，并把所有 terminal path 收口为 Kafka result/error；`-4/-5/-6` 明确拒绝。
`NereusKafkaExceptionMapper` 对当前 `ErrorCode` 做 exhaustive switch，保持 fencing、trim、checksum、backpressure 和
timeout 的 Kafka protocol 语义。Kafka-only `LeaderEpochAwareOffsetLookup` 不依赖 Nereus artifact；stock `Partition`
只允许 current leader epoch 安装，按 identity/epoch 移除，并在 higher epoch、follower、offline/delete transition
撤销；new-leader publication 到 exact lookup 安装之间保留同 epoch `recovery pending`，普通 earliest/latest/timestamp
请求返回 `OffsetNotAvailableException`，不会短暂落回尚未恢复的 local-log lookup；stale cancel 不影响新 epoch。
`ReplicaManager.fetchOffset` 把 callback 接到现有 delayed ListOffsets purgatory。三个 bridge test classes 的 12 tests、
`NereusListOffsetsLifecycleTest` 的 7 tests、`NereusTopicDeltaLifecycleTest` 的 7 tests、四个 stock `Partition`
seam tests、一个 `ReplicaManager` publication test、七个完整 `BrokerMetadataPublisherTest`、core/storage checkstyle、
SpotBugs、Spotless 与无 Nereus artifact 的 stock-from-scratch tests 均通过。runtime lifecycle 对 open 做 topic ID/name/partition、
stock leader state/epoch 和 manager-result identity/epoch/profile/writable-state 双重校验；only-after-recovery install，
resign/delete/shutdown 先撤销 lookup 再委托 manager，安装失败先 resign recovered storage 再失败 open，late old open
按旧 epoch 清理且不能移除新 lookup。topic-delta composer 还验证 old-image delete identity、new-image follower/leader
identity、broker epoch、metadata offset、delete→同名 recreation 串行、ready/resigned callback-after-success 和
coordinator election-after-open。Concrete recovery/CLI selection、`UnifiedLog`/factory composition、
leader-epoch cache、`KafkaVirtualPositionIndex`、`NereusTimeIndex` section codec、restart recovery、remote branch push 与
真实 KRaft baseline integration tests 仍为 open M3/M4 work。

## 10. Error and outcome mapping

`NereusKafkaExceptionMapper` 同时看 `ErrorCode`、`AppendOutcome`、partition authority state；不能只看
`retriable` boolean。

| Nereus condition | Kafka exception/error | Partition action |
| --- | --- | --- |
| invalid Kafka bytes/CRC before append | `CorruptRecordException` | remain writable；no IO |
| `OFFSET_CONFLICT` | `KafkaStorageException` | write-fence，full reopen/replay |
| `FENCED_APPEND` / `APPEND_SESSION_EXPIRED` | `FencedLeaderEpochException` or `NotLeaderOrFollowerException` | fence/close current instance |
| `BACKPRESSURE_REJECTED` before IO | `ThrottlingQuotaExceededException` | remain writable |
| timeout + `KNOWN_NOT_COMMITTED` | `TimeoutException` / `REQUEST_TIMED_OUT` | remain/retry only after lane confirms no attempt |
| any `MAY_HAVE_COMMITTED` | `KafkaStorageException` response | `WRITE_FENCED_RECOVERY_REQUIRED` |
| `KNOWN_COMMITTED` response lost | `KafkaStorageException` until recovery publishes | fence then exact recover/replay |
| object/primary checksum mismatch | `CorruptRecordException`/`KafkaStorageException` | offline partition，repair/fallback audit |
| `OFFSET_TRIMMED` | `OffsetOutOfRangeException` | remain readable at new log start |
| unsupported activated format/profile | `KafkaStorageException` | broker/partition not ready |
| metadata unavailable | `KafkaStorageException` | fence if authority cannot be proven |
| executor queue reject | `ThrottlingQuotaExceededException` | no side effect |
| broker shutdown | `NotLeaderOrFollowerException` | drain/fence |

unknown append completion **never** maps to an ordinary retriable client error while accepting later writes。client may retry
to a new/current leader after partition recovery；idempotent producer logic deduplicates committed retry bytes。

当前 `KafkaAppendFailureClassifier` 已把这个表的 partition action 固化为不依赖 Kafka artifact 的
`REJECT_WITHOUT_FENCE`、`WRITE_FENCE_RECOVERY_REQUIRED`、`CORRUPT_OFFLINE`。它会展开
`CompletionException`/`ExecutionException`；任意非 Nereus unknown failure 也 fail closed 为 invariant + write fence。
`KafkaAppendFailureDisposition` 构造器禁止 empty/uncertain/known-committed outcome 搭配 `REJECT_WITHOUT_FENCE`，防止
fork mapper 后续错误降级。Kafka exception/error 的具体实例化仍是尚未实现的 fork responsibility。

## 11. Partition write-fence integration

`NereusUnifiedLog` maintains immutable instance ID plus state reference. Every async completion captures instance/leader
epoch and checks it before local publication。state transitions详见文档 04；fork behavior：

- `LEADER_OPENING`：Produce returns not-leader，Fetch may wait or read only after snapshot published；
- `LEADER_WRITABLE`：normal；
- `WRITE_FENCED_RECOVERY_REQUIRED`：all Produce rejected，new Fetch may serve last published stable snapshot if integrity known；
- `RESIGNING`/`CLOSED`：all new IO rejected；
- `CORRUPT_OFFLINE`：reads/writes storage error until operator/repair resolves。

closing log first unregisters append/fetch listeners，then waits bounded in-flight operations，then releases session。timeout
does not “force success”；authority epoch prevents late writer commit from becoming current leader state。

## 12. Local filesystem contract

Kafka constructors may require `File` paths。F9 creates only ephemeral placeholders under configured
`nereus.kafka.storage.cache.dir/{brokerId}/{topicId}/{partition}`：

- no data `.log` file contains authoritative records；
- no local producer/HW/recovery checkpoint is accepted on restart；
- directory can be deleted while broker stopped without data loss；
- startup recreates and repopulates cache from durable binding/checkpoint/stream；
- disk failure of cache degrades cache/metrics but does not change durable storage，unless JVM cannot allocate required
  temporary resources；
- DescribeLogDirs reports `isFuture=false` and explicit Nereus extension/log message where protocol permits；numeric
  byte values are cached/estimated and labeled non-authoritative in metrics。

## 13. Fork discipline and upgrade audit

Each Kafka baseline upgrade must run a method-level audit for：

```text
UnifiedLog.append ordering around localLog.append / producer state / txn index
LocalLog append/read visibility and override signatures
Partition makeLeader/makeFollower/HW/append/fetch/delete locks
ReplicaManager append/fetch callback and delayed-operation contracts
BrokerMetadataPublisher coordinator-election ordering
LogManager recovery/cleaner/checkpoint scheduling
BrokerServer construction/shutdown order
ReplicationControlManager create/alter/reassign/ISR methods
```

Build fails when marker pairs unbalanced or source signature digest differs from reviewed lock。No reflection、method handles
or package-private hacks may hide drift。

## 14. Planned tests

### 14.1 Fork unit tests

- disabled mode constructs stock managers/classes and runs unmodified behavior fixtures；
- enabled factory selects every Nereus class with exact topicId；zero topicId rejected；
- stock UnifiedLog validation assigns exact offsets before encoder；
- local append failure leaves LEO/producer map/HW unchanged；
- stable success updates state once；duplicate producer request creates no second append；
- buffer snapshot copies/release/rejection/cancellation；
- fork markers/source signature gate。

### 14.2 Produce/Fetch integration

- compression codecs、multi-batch request、large first batch、record format/CRC exact bytes；
- acks 0/1/-1 callback/visibility ordering；
- minBytes/maxWait multi-partition async fetch；
- read committed/uncommitted with aborted transactions；
- fetch start/logStart inside compressed batch；
- ListOffsets earliest/latest/timestamp/epoch；
- bounded executor saturation does not deadlock request processors。

### 14.3 Failure cuts

- response loss before/after stable head CAS；
- leader epoch takeover while old append in-flight；
- broker shutdown with queued/running append/fetch；
- partition delete concurrent open/read；
- corrupt primary and healthy higher-generation fallback；
- checkpoint missing/stale/corrupt with full replay；
- metadata publisher initial image with internal-topic election；
- old broker/format capability exclusion。

具体 scenario IDs 与 test class 名称见文档 08。
