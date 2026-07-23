# 03 — Kafka Fork, Log and Broker Integration

> 状态：Implementation in progress；Nereus-side M3 codec/ListOffsets、Kafka-fork record/async-result bridges and stock Partition/ReplicaManager request seam implemented；runtime installation remains open
> 参考：AutoMQ Kafka fork `1c648d84819d5c3fef2af585f02149c397584870`
> 初始原则：保留 stock Kafka validation/coordinator/protocol，替换 durable partition-log owner

## 1. Integration shape

F9 计划维护一个与选定 Apache Kafka release 对齐的窄 fork。AutoMQ 证明 `UnifiedLog → LocalLog → LogSegment`
是可行 seam；F9 不复制 `s3stream`、`Elastic*` 或 AutoMQ enterprise/API extensions，而是新增 `Nereus*`
classes，并把 provider/Oxia 细节封装在 `nereus-kafka-adapter`。

```text
Kafka stock protocol/controller/coordinators
  -> NereusReplicaManager (async IO scheduling only)
  -> Partition / UnifiedLog stock validation and producer-state ordering
  -> NereusUnifiedLog
  -> NereusLocalLog
  -> NereusLogSegment / NereusLogRecords / derived indexes
  -> KafkaPartitionStorage (adapter)
  -> StreamStorage
```

初版不需要 `NereusKafkaApis`：stock `KafkaApis` 已使用 callback-based ReplicaManager API，Produce/Fetch 的
异步存储调度放在 `NereusReplicaManager`。只有未来需要协议扩展时才新增 API subclass。

## 2. Planned Kafka-side classes

目标 package 均位于 Kafka fork，不放入 Nereus core：

| Class | Base/role | Correctness responsibility |
| --- | --- | --- |
| `kafka.log.nereus.NereusUnifiedLog` | extends `UnifiedLog` | stock validation/state plus Nereus recovery publication |
| `kafka.log.nereus.NereusLocalLog` | extends `LocalLog` | stable append/read/trim bridge；LEO only after success |
| `kafka.log.nereus.NereusLogSegment` | extends `LogSegment` | virtual roll/size/index facade；no durable file |
| `kafka.log.nereus.NereusLogRecords` | records facade | exact MemoryRecords encode/read and owned buffers |
| `kafka.log.nereus.NereusProducerStateManager` | extends `ProducerStateManager` | in-memory stock semantics + checkpoint bridge |
| `kafka.log.nereus.NereusTimeIndex` | Kafka `TimeIndex` facade | derived timestamp lookup/checkpoint state |
| `kafka.log.nereus.NereusTransactionIndex` | Kafka `TransactionIndex` facade | derived aborted-txn lookup/checkpoint state |
| `kafka.log.nereus.NereusLeaderEpochCache` | epoch cache facade/adapter | derived epoch ranges；no local checkpoint truth |
| `kafka.log.nereus.NereusLogFactory` | factory | open/recover/construct exact topicId binding |
| `kafka.server.nereus.NereusReplicaManager` | extends `ReplicaManager` | bounded append/fetch/lifecycle execution and callbacks |
| `kafka.server.nereus.NereusProduceBufferSnapshot` | owned request bytes | buffer lifetime across async handoff |
| `kafka.server.nereus.NereusFetchOperation` | async state machine | minBytes/maxWait/event/re-read/callback-once |
| `kafka.log.nereus.NereusKafkaExceptionMapper` | mapper | Nereus error/outcome → Kafka exception |
| `kafka.server.nereus.NereusBrokerLifecycle` | runtime bridge | boot/readiness/drain/shutdown ordering |

Adapter-side counterpart：

| Class | Key target methods |
| --- | --- |
| `NereusKafkaRuntime` | `start()`、`admission()`、`partitionStorageManager()`、`close()` |
| `KafkaPartitionStorageManager` | `openLeader`、`resign`、`delete`、`reconcile` |
| `KafkaPartitionStorage` | `append`、`recoverAppend`、`read`、`trim`、`stableSnapshot`、`close` |
| `KafkaAppendBatchEncoder` | exact `MemoryRecords` → ranged `AppendBatch` |
| `KafkaFetchAssembler` | `ReadBatch` list → exact `MemoryRecords`/fetch facts |
| `KafkaRecordBatchCodec` | batch syntax/CRC/offset/producer facts validation |

## 3. Exact stock-file modification map

每处修改使用成对 marker，marker 内只做 factory/hook selection，不嵌入存储算法。

### 3.1 `core/.../kafka/server/KafkaConfig.scala`

目标 change：

- 注册 `nereus.kafka.storage.*` keys（完整表见文档 06）；
- build `NereusKafkaStorageConfig` typed snapshot；
- `validateValues` 调用纯函数 `NereusKafkaConfigValidator.validate`；
- disabled mode 不连接 Nereus，不改变 stock defaults；
- enabled mode 拒绝 ZooKeeper mode、remote log、log cleaner、RF/minISR conflicts 和超出 hard format limits。

不得在 config constructor 创建 client、thread 或 metadata key。

### 3.2 `core/.../kafka/server/BrokerServer.scala`

目标 inject points：

1. config 已解析、metrics/time 可用后，创建 `NereusKafkaRuntime`；
2. runtime connectivity/capability advertisement 完成后，创建 LogManager/ReplicaManager；
3. enabled 时构造 `NereusReplicaManager`，disabled 时构造 stock `ReplicaManager`；
4. request processors 开始前等待 local broker readiness；
5. shutdown 先停止 admission，再关闭 ReplicaManager/partition logs，最后关闭 runtime。

禁止把 field type 写死为 `NereusReplicaManager`；对外仍暴露 `ReplicaManager`，避免 disabled mode cast。

### 3.3 `core/.../kafka/log/LogManager.scala`

涉及方法：`startup`、`getOrCreateLog`、`removeLogAndMetrics`、`shutdown`、scheduled retention/flush/checkpoint。

enabled mode：

- `startup` 不扫描 local log dirs 作为 partition truth；只清理/建立 ephemeral cache dir；
- 不启动 stock `LogCleaner`；
- 不为 Nereus log 写 recovery point/HW/LEO checkpoint files；
- `getOrCreateLog(topicPartition,...,topicId,leaderEpoch)` 必须要求 non-zero topicId，委托 `NereusLogFactory`；
- remove/delete 先走 durable lifecycle coordinator，再从 current-log maps 移除；
- flush scheduler 对 Nereus log no-op，不能把 no-op 当成 producer completion；
- stock disabled branch byte-for-byte保留。

同集群混用 local/Nereus topic 不在首版支持范围，因此 enabled branch 若发现 tracked local log，broker readiness
失败并提示 migration unsupported，而不是把它当 stray 自动删除。

### 3.4 `core/.../kafka/cluster/Partition.scala`

精确 method intent：

| Method | Target behavior in Nereus mode |
| --- | --- |
| `createLogIfNotExists` | pass exact topicId/partition/leader epoch to factory；no name-only open |
| `makeLeader` | wait for `openLeader` recovery publication before accepting writes |
| `makeFollower` | close admission/fence current leader instance；RF1 means no follower data append |
| `appendRecordsToLeader` | require partition storage state `LEADER_WRITABLE`；then use stock `UnifiedLog.appendAsLeader` |
| `appendRecordsToFollowerOrFutureReplica` | defensive `UnsupportedOperationException`/storage error；controller should make unreachable |
| `maybeIncrementLeaderHW` | candidate is `NereusUnifiedLog.stableEndOffset`，never speculative LEO |
| `readRecords`/`fetchRecords` | use stock isolation bounds over Nereus log；IO runs on fetch executor |
| `deleteRecordsOnLeader` | validate requested offset with stock rules，then durable Nereus trim before success |
| `delete` | metadata-first partition lifecycle，idempotent close/delete |

`makeLeader` 不能在 Kafka partition lock 内等待一个需要同一 lock 的 recovery callback。open 流程在 lifecycle
executor 完成 replay，final publication 才以短 critical section 安装 log/state。

### 3.5 `core/.../kafka/server/metadata/BrokerMetadataPublisher.scala`

AutoMQ source 使用 async topic-delta path。F9 采用同类 ordering，但接口归属于 Nereus：

```text
metadataCache.setImage
  -> KafkaPartitionStorageManager.reconcile(topicsDelta, newImage)
  -> NereusReplicaManager.applyDeltaAsync
  -> for each successfully opened/resigned partition callback
  -> group/transaction coordinator election/resignation
  -> deleted-partition coordinator notification
```

internal-topic coordinator election 必须晚于对应 log fully recovered；否则 coordinator 可能从未恢复的
`__consumer_offsets`/`__transaction_state` 提供服务。一个 partition open 失败只 offline 该 partition并触发
fault handling；不能把 `firstPublishFuture` 成功当成 all partitions ready。

### 3.6 `core/.../kafka/server/ReplicaManager.scala`

保留 stock class，抽取两个 `protected` completion helper，避免 subclass 复制整个方法：

```scala
// target seam, default stock call remains synchronous
protected def completeAppendRecordsAfterLocalAppend(...): Unit
protected def completeFetchAfterLogRead(...): Unit
```

stock `appendRecords`/`fetchMessages` 调用它们，disabled behavior 不变。`NereusReplicaManager` override 只负责
提交 IO future，再调用相同 completion logic。helper 不接触 Nereus types。

`appendToLocalLog` 已是 protected，Nereus executor 可以复用。`readFromLog` 是 public，Nereus fetch executor
可以复用。若升级 baseline 改变可见性，source-lock review 必须更新，不用 reflection 绕过。

### 3.7 `core/.../kafka/server/DelayedFetch.scala`

stock delayed fetch 只服务 disabled mode。Nereus mode 的 `fetchMessages` 使用 `NereusFetchOperation`，不允许
purgatory thread 同步调用 object/BookKeeper IO。文件最多增加 factory branch；不得在 `tryComplete` 中 join
storage future。

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
消费 `nereus-kafka-adapter:0.1.0-f9-dev`，并已在 local fork `f36b9123a6` 落地
`NereusRecordTimestampInspector`、`NereusListOffsetsBridge`、`NereusListOffsetsScanConfig` 和
`NereusKafkaExceptionMapper`，并通过 Kafka-only `LeaderEpochAwareOffsetLookup` 接入 stock `Partition`/
`ReplicaManager` request path。当前 commit 尚未推送，因而仍未满足 M3 production fork source-lock entry，也不
构成 Produce/Fetch runtime claim。

## 6. Produce execution and threading

### 6.1 Why executor handoff is mandatory

`UnifiedLog.appendAsLeader` 是同步 API；`NereusLocalLog.append` 必须等待 stable storage result，才能让 stock
code 更新 ProducerStateManager。直接在 network request thread join 会把 object/BookKeeper latency 变成 request
handler starvation。因此 `NereusReplicaManager.appendRecords` 在进入 stock local append 前完成 owned-buffer
handoff并提交到 bounded append executor。

### 6.2 Buffer handoff

`NereusProduceBufferSnapshot.capture` 在 request thread：

- 计算 request/partition bytes with checked arithmetic；
- 从 product-owned bounded pool 申请 buffer；
- copy exact `MemoryRecords.buffer`；
- 构造 read-only `MemoryRecords` view；
- 使用 `RequestLocal.NoCaching` 在 executor 上做 validation，绝不跨线程使用 request-thread BufferSupplier；
- future terminal callback 后 release once。

queue admission 失败发生在任何 append IO 前，返回 `ThrottlingQuotaExceededException`；已入队后 client cancel
不能取消底层 append，因为可能已经提交。response callback 仍 exactly once，channel lifecycle 决定是否发送。

当前 Nereus-side 实现使用 `KafkaProduceBufferSnapshot.capture(ByteBuffer, KafkaByteBudget)` 精确复制 caller
`position..limit`，不修改 caller state；lease 先于 array allocation 取得，allocation/copy、queue reject、task failure、
success 和 duplicate close 都只释放一次。`KafkaBoundedAppendExecutor.submit` 用 fixed thread count + bounded
`ArrayBlockingQueue`，关闭 race 返回 `STORAGE_CLOSED + KNOWN_NOT_COMMITTED`。returned future 只是 response handle：
取消它不会传播到 admitted task。fork 后续仍需把 `MemoryRecords` / request callback 接到这个 protocol-neutral
boundary，并负责 `RequestLocal.NoCaching`。

### 6.3 Append ordering

每 partition 同时只有一个 `NereusLocalLog.append`：

- Kafka `UnifiedLog.lock` 序列化 validation/offset assignment；
- adapter `PartitionAppendLane` 序列化 calls；
- Nereus `StreamLane` 与 head CAS 是底层保证；
- executor 可以并发不同 partitions，不允许同 partition reorder。

等待发生在 append executor，不持有 ReplicaManager global lock。它持有当前 UnifiedLog partition lock，Nereus
completion path不得反向获取该 lock；metrics/checkpoint scheduling 在 append 返回后执行。

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

首版 serialized path 中 stable end 与 LEO 同步前进。HW update：

- leader open 完成时从 stable head 初始化；
- append stable 后 `Partition.maybeIncrementLeaderHW` 取 stable end；
- 绝不使用 future/in-flight expected end；
- HW 只能单调增长且不能越过 current leader authority session；
- old leader completion callback 在 authority 失效后不能更新 installed log。

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
和 fresh recovery。Kafka fork metadata callback wiring 尚未实现。

## 8. Fetch execution

### 8.1 Async read path

`NereusReplicaManager.fetchMessages` 把 storage read 调度到 bounded fetch executor；每个 request snapshot immutable
fetch params/info，不携带 thread-local buffer supplier。executor 调用 stock `readFromLog`，最终由
`NereusLocalLog.read` join adapter future。

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

`KafkaFetchOperationTest` 已为 KF-FET-004/005/016 提供 deterministic adapter evidence；Kafka fork 中
`ReplicaManager.fetchMessages` / stock delayed-fetch callback wiring、真实 KRaft broker process 和 request-level Kafka
exception assembly 仍未实现，因此不是 M3 completion claim。

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
撤销；`ReplicaManager.fetchOffset` 把 callback 接到现有 delayed ListOffsets purgatory。三个 bridge test classes 的
12 tests 加三个 stock lifecycle tests、core/storage checkstyle、SpotBugs、Spotless 与无 Nereus 参数的 stock test
均通过。`UnifiedLog`/factory/runtime lookup installation、
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
