# 03 — Kafka Fork, Log and Broker Integration

> 状态：Implementation in progress；Nereus-side M3 codec/ListOffsets/checkpoint-pinned paged recovery、Kafka-fork record/async-result/recovery-state bridges、stock Partition/ReplicaManager request seam、manager-to-Partition lookup/state lifecycle、optional async metadata-publisher seam、M6 typed config validation、stock-compatible BrokerServer lifecycle injection、adapter-backed typed runtime bridge、authoritative UnifiedLog factory/shell selection、synchronous correctness bridge，以及 bounded ReplicaManager Produce / whole-request multi-partition async Fetch handoff implemented；KafkaRaftServer production selection and real KRaft process gate remain open
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
  -> one or more empty synthetic local segments
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
| `org.apache.kafka.storage.internals.log.RequiredAcksAwareAppend` | optional stock inert seam | preserve exact protocol required-acks without changing ordinary UnifiedLog |
| `kafka.log.nereus.NereusLogSegment` | extends `LogSegment` | virtual roll/size/index facade；no durable file |
| `kafka.log.nereus.NereusLogRecords` | records facade | exact MemoryRecords encode/read and owned buffers |
| `kafka.log.nereus.NereusProducerStateManager` | extends `ProducerStateManager` | in-memory stock semantics + checkpoint bridge |
| `kafka.log.nereus.NereusTimeIndex` | Kafka `TimeIndex` facade | derived timestamp lookup/checkpoint state |
| `kafka.log.nereus.NereusTransactionIndex` | Kafka `TransactionIndex` facade | derived aborted-txn lookup/checkpoint state |
| `kafka.log.nereus.NereusLeaderEpochCache` | epoch cache facade/adapter | derived epoch ranges；no local checkpoint truth |
| `kafka.server.storage.BrokerStorageAppendExecutor` | optional stock-owned seam | request prevalidation、owned submit、drain contract without Nereus types |
| `kafka.server.nereus.NereusBrokerStorageAppendExecutor` | product-backed implementation | typed limits、exact capture、per-partition FIFO、Kafka error mapping |
| `kafka.server.storage.BrokerStorageFetchExecutor` | implemented optional stock-owned seam | immutable whole-request submit、opaque stock read closure and drain without Nereus types |
| `kafka.server.nereus.NereusBrokerStorageFetchExecutor` | implemented product-backed implementation | bounded logical admission、partition events、stock wave validation、Kafka error mapping |
| `kafka.log.nereus.NereusKafkaExceptionMapper` | mapper | Nereus error/outcome → Kafka exception |
| `kafka.log.nereus.NereusKafkaRecoveredState` | fresh M3 derived state | validate/rebuild exact stock RecordBatch offsets/timestamps/leader epochs |
| `kafka.log.nereus.NereusKafkaRecoveryStateCodec` | adapter recovery codec | one fresh state per leader open；M4 sections fail closed |
| `kafka.server.nereus.NereusKafkaRecoveryStateFactory` | exact Partition publisher | validate topicId/name/partition/leader epoch and install frozen provisional state |
| `org.apache.kafka.storage.internals.log.LeaderEpochAwareRecoveryState` | stock inert seam | keep `Partition` compilable without unpublished Nereus artifacts |
| `kafka.server.nereus.NereusBrokerStorageRuntime` | runtime bridge | exact ReplicaManager binding、boot/readiness/drain/shutdown delegation |
| `kafka.server.nereus.NereusBrokerStorageRuntimeFactory` | typed factory | disabled isolation、explicit runtime/scan-limit creators、failure rollback |

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

尚未闭合的 controller/cluster validation：cluster ID/activation match、feature gate、create-topic RF/assignment enforcement
和 KRaft real-process readiness；这些不能由 broker-local `KafkaConfig` 代替。

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

product adapter 已实现 `NereusKafkaRuntimeFactory`，并新增仅支持 `OBJECT_WAL_SYNC_OBJECT` 的 concrete
`NereusKafkaObjectWalRuntimeFactory`：显式组装 Object provider、shared Oxia、L0/physical/binding stores、protection、
callback executor、durable checkpoint read pins、checkpoint reader/verifier/recovery coordinator、bounded COMMITTED page
source、concrete recovery launcher 和同一 manager/runtime graph；real Oxia + local-file provider 的 leader
open/Produce/Fetch gate 已通过。Fork 不再承担 ObjectStore/Oxia/read-pin orchestration，只在 exact ReplicaManager
可用后为每次 open 创建 fresh state codec 和 exact Partition publisher。尚未实现的是 BookKeeper/async-object
creator、controller activation scheduling、CLI/KafkaRaftServer production factory selection、durable
checkpoint-failure quarantine observer 和 native-storage KRaft process
test。当前已有可执行的 Object-WAL provider/runtime/recovery composition、log-shell selection、直接
`NereusUnifiedLog` correctness I/O bridge，以及 bounded Produce/Fetch handoff，但还没有一条可从 stock Kafka CLI 启用并完成
真实 KRaft Produce/Fetch 的 production path。

### 3.3 `core/.../kafka/log/LogManager.scala`

`cfcdd55fbc571bc7187379d65504caa4fe23586e` 已在 stock package 新增
`UnifiedLogFactory` 与 immutable `UnifiedLogOpenContext`。factory 的完整策略面是：

```scala
def logDirectories(configured: Seq[File]): Seq[File]
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
步骤失败也 resign，阻止 successor append。LEO 只在 callback 成功返回后推进，synthetic local log size 保持 `0`。
M3 producer/idempotent、transaction 与 control batch 由 adapter 返回 `UNSUPPORTED_FORMAT`，fork 映射为
`UNSUPPORTED_FOR_MESSAGE_FORMAT`；follower append 永远拒绝。

Fetch 当前只支持 M3 非事务数据；result 必须不含 aborted transaction。它复用 adapter containing-entry/first-entry
overflow 语义并受 partition bytes、hard response bytes、stable upper bound 与 typed timeout 约束；返回前复核仍是
同一份 `LEADER_WRITABLE` storage。virtual segment base/relative position 目前由 request 传 `0/0`，完整 virtual index
仍是后续切片。

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

- `createTopics` effective replication factor 必须是 1；manual assignments 每项 size 1；
- `createPartitions` 继承 existing partition RF=1，new assignments size 1；
- `alterPartition` ISR 必须精确包含 current leader one node；
- `alterPartitionReassignments` 只允许 target one broker；
- preferred/unclean election 不创建 follower-copy assumptions；
- `min.insync.replicas` effective value 必须是 1；
- AssignReplicasToDirs 对 Nereus partition 返回明确 unsupported/no-op result，不改变 storage placement。

Controller 条件来自 KRaft feature/control record，不读取 broker-local config，防止 controller failover 后规则变化。

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
first-unstable state 更新成功后调用 `publishDerivedOffsets(exactEnd, HW, LSO)`。在 exact end 确认前，同 partition
下一次 storage append 不 dispatch，`STABLE_APPEND` 事件也不发布。expected end 不匹配、offset 越界或 initialized
HW/LSO 回退均 fail closed；post-stable publication failure 进入 write-fence/replay。

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
  `AppendEntry`，保留 exact bytes，event hint 取 normalized max timestamp，生成 concatenated CRC32C；M3 明确拒绝
  idempotent/transaction/control batches，它们必须等 M4 producer-state/transaction owner 落地后再启用；
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
  byte lease 并复制 caller remaining bytes，向 task 只暴露 owned read-only view；byte/queue saturation 均在 append
  I/O 前返回 `KNOWN_NOT_COMMITTED`，所有 terminal/race path release once；client future cancel 不会取消已经入队的
  append task，executor close 则拒绝新 admission 并 drain 已接受任务；
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
  不返回近似 offset。max timestamp 相等时选择最低 logical offset。

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
Fetch 错误降级为 backpressure 的竞态，但不扩大 `threads + queueCapacity` logical admission。
Controller scheduling、CLI/KafkaRaftServer production selection 和 real KRaft process gate 尚未实现。当前 branch
尚未推送，因而仍未满足 M3 production fork source-lock entry；bounded Produce/Fetch 单元与组合证据也不构成真实
KRaft runtime claim。

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
- 构造 read-only `MemoryRecords` view；
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
和 fresh recovery。Kafka fork metadata callback、BrokerServer config/runtime factory、manager ownership 与 ordered
shutdown wiring 已实现；controller activation scheduling、CLI production selection 与真实 KRaft process gate 尚未实现。

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
`TIMEOUT`；空页无进展返回 `READ_LIMIT_TOO_SMALL`。上述错误都不会降级成近似 offset。

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
