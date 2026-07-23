# Future 9 — Native Kafka Shared-Storage Integration

> 状态：Designed；尚未实现
> 代码级合同：`../phase-9-kafka-native-storage/README.md`
> 参考源码：AutoMQ Kafka fork `1c648d84819d5c3fef2af585f02149c397584870`，`3.9.0-SNAPSHOT`
> 设计基线日期：2026-07-23

## 1. 决策摘要

Future 9（F9）把 **原生 KRaft Kafka broker** 接到 Nereus：Kafka 继续拥有协议、控制器、分区状态、
producer/transaction coordinator 语义；Nereus 成为每个 Kafka partition 唯一的 durable log 和共享存储。

F9 不复用 Future 5 的 KoP 边界：

| 维度 | F5 KoP/Kafka projection | F9 native Kafka |
| --- | --- | --- |
| Kafka server | KoP/Pulsar broker 内的 Kafka 协议投影 | Kafka broker fork |
| partition log | ManagedLedger facade 上的投影 | 直接映射到 `StreamStorage` |
| payload mapping | canonical record-per-offset mapping | exact Kafka `RecordBatch` bytes + ranged entry |
| group/transaction truth | F5 定义的 Oxia projection | 原生 Kafka internal compacted topics |
| leader truth | KoP/Pulsar ownership projection | KRaft partition/leader epoch |
| compatibility target | KoP client compatibility | native Kafka broker/client compatibility |

两者可以消费相同的 Nereus protocol-neutral primitives，但不得共享一份含义不同的 durable binding 或
把一种 payload mapping 当成另一种读取。

## 2. 目标

F9 的最终目标是：

- Kafka client 的 Produce、Fetch、ListOffsets、DeleteRecords、transaction、group coordinator 与 log
  compaction 语义由原生 Kafka 代码路径保持；
- 任意有资格的 broker 都能在 KRaft leader 迁移后打开相同 Nereus stream，不复制本地日志；
- producer acknowledgement 只依赖 Nereus stable append，不依赖 broker 本地磁盘；
- partition 的 committed bytes、offset、leader fencing 和恢复有唯一 correctness owner；
- object materialization、compaction、retention 与物理 GC 复用 Nereus F4/F1-BK 合同；
- 目标接口、durable record、fork method、状态机、错误和测试在实现前全部冻结到代码级。

## 3. 非目标

首个交付不包含：

- 在同一 Kafka 集群按 topic 混用本地 log 与 Nereus log；
- Kafka follower replica 数据复制、ISR quorum durability 或 RF 大于 1；
- 从现有 local-log/AutoMQ 集群在线迁移已有 topic；
- 将 KRaft metadata、consumer group 或 transaction coordinator truth 搬入新 Oxia record；
- 复用 AutoMQ S3Stream 格式或拷贝 AutoMQ 源码；
- 把 stock `RemoteLogManager` 当成 Nereus 的主读写路径；
- 第一个版本就实现多请求并行 append；正确的单 partition 串行路径先通过全部恢复门禁；
- Pulsar 与 native Kafka 同时读写同一个 stream；跨协议 payload migration 需要独立 Future。

## 4. 顶层架构

```text
Kafka client
  -> KafkaApis / ReplicaManager / Partition / UnifiedLog
  -> NereusLocalLog + NereusLogRecords
  -> nereus-kafka-adapter
       -> StreamStorage append/read/trim/recover
       -> KafkaPartitionMetadataStore (Oxia)
       -> checkpoint/object/materialization services
  -> Nereus primary WAL + stable StreamHeadRecord
  -> higher generations / object checkpoints
```

唯一真相分工：

| Truth | Authoritative owner | Derived/cache only |
| --- | --- | --- |
| topic、partition、leader、leader epoch | KRaft metadata log | broker metadata image |
| partition-to-stream identity/lifecycle | one Oxia `KafkaPartitionBindingRecord` | broker cache |
| committed offset/bytes | Nereus stream head + reachable commit chain | Kafka LEO/HW fields |
| exact Kafka batch | committed Nereus entry payload | decoded records/indexes |
| producer/transaction replay state | committed Kafka bytes | checkpoint object + broker memory |
| group/transaction coordinator state | `__consumer_offsets` / `__transaction_state` | coordinator cache |
| compacted read generation | F4 committed generation record | planner/task/checkpoint hints |
| routing/readiness | KRaft + activation/capability records | local routing cache |

对象列表、本地 segment 文件、checkpoint root、broker 内存都不能独立证明一次 append 已提交。

## 5. Partition 与 stream 映射

F9 使用 Kafka `topicId`，而不是 topic name，构造不可混淆的 identity：

```text
KafkaPartitionId = kafkaClusterId + topicId + partitionId
StreamName        = kafka/{clusterId}/{topicId}/{partitionId}/incarnation-1
```

一个 Kafka partition 在一个 incarnation 中只对应一个 Nereus stream。topic 删除后 binding 进入
`DELETING`，stream 被 seal/delete；同名 topic 重建取得新 `topicId` 和新 stream，不允许 alias 旧 bytes。

首个版本要求：

- KRaft mode；
- controller 强制 topic replica assignment 长度为 1，`min.insync.replicas=1`；
- Nereus storage mode 是 cluster-wide immutable activation，不允许 broker 或 topic 局部打开；
- 仅 new/empty Kafka cluster 可启用；已有 log migration 必须另行设计。

RF=1 只描述 Kafka metadata layer。durability 由 stream 创建时冻结的 Nereus `StorageProfile` 决定，
默认目标是 `BOOKKEEPER_WAL_ASYNC_OBJECT`；Kafka broker 不把本地副本数当作 Nereus durability。

## 6. Kafka batch 到 Nereus range

核心映射：

```text
one MemoryRecords append
  -> one AppendBatch(payloadFormat = KAFKA_RECORD_BATCH)
  -> one AppendEntry per Kafka RecordBatch
  -> AppendEntry.recordCount = lastOffset - baseOffset + 1
  -> AppendEntry.payload = exact RecordBatch bytes
```

Nereus logical offset 与 Kafka absolute offset 完全相同，但一个 entry 可以覆盖多个 offset。entry 的
`OffsetRange[start,end)` 必须等于 Kafka batch 的 `[baseOffset,lastOffset+1)`。payload 保留 magic、attributes、
compression、CRC、producer id/epoch、base sequence、timestamps 和 control/transactional bits；Nereus 不解压后
重编码普通 append。

Kafka stock `LogValidator` 先完成 offset assignment/validation。adapter 使用
`expectedStartOffset = firstBatch.baseOffset` 调用 Nereus，并断言返回 range 与 MemoryRecords 的完整范围一致。
不满足时 partition 进入 storage-error/write-fenced 状态，不能修正 offset 后继续写。

## 7. 必须先演进的 protocol-neutral 合同

F9 不允许把 Kafka 类型加入 `nereus-api`。目标演进是：

1. `AppendBatch` 接受 `KAFKA_RECORD_BATCH`，并按 entry `recordCount` 累加总 record count；
2. 新增 append expected-start precondition；默认调用仍维持当前自动分配行为；
3. read 增加 `EXACT_START` 与 `CONTAINING_ENTRY` boundary mode；
4. read 增加 first-entry overflow policy，使不可拆的 Kafka batch 可以单独超过请求 limits；
5. semantic view read 变成 adapter 可消费的 public API；
6. Object WAL reader 在 start 落入 range 内时返回包含 start 的完整 entry；
7. NCP2 支持 ranged entry，NTC2 支持 Kafka-aware sparse compaction；旧格式永不重解释。

推荐的 public surface、精确 invariant 和兼容方法见代码级文档 `02-ranged-entry-api-and-object-format.md`。

## 8. Produce、LEO 与 high watermark

初始实现采用 per-partition serialized append：

```text
UnifiedLog.append
  -> stock validation and offset assignment
  -> NereusLocalLog.append
  -> bounded Nereus append executor
  -> StreamStorage.append(... expectedStartOffset ...)
  -> stable append completion
  -> return to UnifiedLog
  -> ProducerStateManager update / response eligibility
```

在这条保守路径中：

- LEO 只在 stable append 后前进；
- HW 等于 Nereus stable committed end；
- LSO 继续由 Kafka producer/transaction state 计算；
- `acks=1` 与 `acks=-1` 均只在 stable append 后成功；`acks=0` 不返回 response，但后台 append 仍遵守同一提交协议；
- Kafka network thread 不直接执行存储 IO；请求进入有界 executor，partition append 顺序不变；
- 后续 pipeline optimization 必须保持 expected-start、stable-HW 与 unknown-outcome fence，不在 F9 首版 gate 内。

这比参考 AutoMQ 中“先推进 LEO、confirm 后推进 HW”的窗口更严格，目的是让 Kafka 内存中的
ProducerStateManager 不领先于 Nereus truth。

## 9. Unknown completion 与 leader fencing

现有 lease 只能在 TTL 到期后由不同 writer 接管，不能满足 KRaft 新 leader 立即 fencing。F9 要求
protocol-neutral session 增加可选 authority：

```text
AppendAuthority(
  type = KAFKA_PARTITION_LEADER,
  authorityId = clusterId/topicId/partition,
  authorityEpoch = leaderEpoch
)
```

新 epoch 严格大于 durable epoch 时可 preempt 活跃旧 session；相同 epoch 只有同 writer/token 可续租；
较小 epoch 一律 `FENCED_APPEND`。无 authority 的 Pulsar/现有 caller 继续使用原 lease 规则。

任何 `MAY_HAVE_COMMITTED`/`KNOWN_COMMITTED` 但调用结果未收敛的 append 都执行：

1. 原子地把 partition 标成 `WRITE_FENCED_RECOVERY_REQUIRED`；
2. 拒绝后续 append，不把普通 client retry 直接追加为新 attempt；
3. 用 retained attempt 调用 `recoverAppend`；
4. 进程丢失 attempt 时，从 durable head/checkpoint 重新打开并 replay committed stream；
5. 重建 producer/transaction/index state 后才回到 `LEADER_WRITABLE`。

## 10. Fetch 与 compacted view

普通 `delete` policy topic 从 `COMMITTED` view 读取。`compact` policy topic 采用两段式 view：

1. 在 cursor 处尝试 `TOPIC_COMPACTED`；返回 sparse rows，并使用 `sourceCoverageEndOffset` 跨过已压缩掉的 hole；
2. coverage 结束后切到 `COMMITTED` tail；
3. 一个 read candidate 不能在中途跨 view；assembler 在 Kafka 层拼接 response；
4. start 落入原始 batch 中间时，storage 返回完整 containing batch，Kafka fetch 再按协议可见边界处理；
5. `maxBytes` 的首批 overflow 服从 Kafka “至少允许第一批”语义，Nereus 不拆 compressed batch。

Fetch isolation：

- `READ_UNCOMMITTED` 上界是 HW；
- `READ_COMMITTED` 上界是 LSO，并使用 stock aborted-transaction filtering；
- 超出 Nereus stable end 的本地 cache 值不能作为 fetch 上界。

## 11. Checkpoint、恢复与虚拟 segment

Kafka 本地文件不能在 broker failover 后充当 truth。F9 使用 immutable checkpoint object 保存 derived state：

- producer state snapshot；
- aborted transaction index；
- leader-epoch checkpoint；
- virtual segment descriptors；
- time index；
- checkpoint offset、source head/version 和内容摘要。

一个 Oxia partition root 只引用最新 verified checkpoint。发布顺序是：读取 stable head → 生成 immutable
object → exact HEAD/校验 → 在同 stream/session/head guard 下 CAS root → 延迟回收旧 object。

打开 leader 时：验证 KRaft identity/epoch → 取得 authority session → 加载 `checkpointOffset <= stableEnd` 的
checkpoint → 从 checkpoint replay `COMMITTED` bytes 到 head → 重建 Kafka state → 最终 publication。对象 list
和本地 index 只用于修复提示，不能跳过 replay 验证。

virtual segment 保留 Kafka roll、retention 和 index 语义，但数据仍在一个 Nereus stream。segment boundary
必须在第一条属于该 segment 的 append 前由 guarded metadata transition 建立，不能事后根据本地时间猜测。

## 12. Retention、DeleteRecords 与 compaction

F9 遵循 native Kafka retention：consumer group offset **不保护** topic bytes。group offset 只是 coordinator
状态；topic `retention.ms/bytes`、segment boundary 与 DeleteRecords 计算新的 log-start-offset，再通过
`StreamStorage.trim` 推进 logical trim。若 trim 落入 ranged entry，reader 的 containing-entry 合同保证
物理 batch 仍可读，Kafka 层屏蔽小于 log start 的 records。

stock local `LogCleaner` 在 Nereus mode 禁用，因为 Nereus stream offset 不可原地替换。F9 使用 F4
`TOPIC_COMPACTED` generation：

- Kafka codec 解出每条 record 的 key、tombstone、absolute offset 与 batch facts；
- retention engine 以 absolute offset 产生 survivor bitmap；
- writer 把 survivors 重写为合法 Kafka batches，建议首版每条 survivor 一个 batch 以保持 sparse offset；
- NTC2/NCP2 保存原 source coverage 与 survivor offsets；
- internal topics `__consumer_offsets`、`__transaction_state` 走相同路径。

没有通过 internal-topic compaction/transaction 恢复 gate 前，F9 不能宣称 native Kafka compatible。

## 13. 模块与 fork 边界

计划新增独立 module `nereus-kafka-adapter`，不把 Kafka dependency 泄漏到 `nereus-api`/`nereus-core`。
Nereus 侧核心 owner：

```text
com.nereusstream.kafka.config
com.nereusstream.kafka.runtime
com.nereusstream.kafka.partition
com.nereusstream.kafka.codec
com.nereusstream.kafka.metadata
com.nereusstream.kafka.checkpoint
com.nereusstream.kafka.recovery
com.nereusstream.kafka.retention
com.nereusstream.kafka.compaction
```

Kafka fork 新类以 `kafka.log.nereus` / `kafka.server.nereus` 为边界，修改 stock files 时使用窄的、成对
`// Nereus inject start/end` marker。精确 class/method map 见
`03-kafka-fork-log-and-broker-integration.md`。

## 14. 激活与升级

所有 broker 在拥有 writable partition 前必须发布 exact capability digest。controller/metadata publisher 只在
`KafkaStorageProtocolActivationRecord` 从 `PREPARED` 进入 `ACTIVE` 后允许 Nereus topic 创建与 leader
publication。ACTIVE 至少冻结：

- ranged-entry API version；
- Object WAL entry-index version；
- NCP2/NTC2 reader/writer versions；
- Kafka payload mapping version；
- binding/session/checkpoint record versions；
- required broker set and capability digest。

不允许旧 broker 继续拥有 Nereus partition，也不允许 ACTIVE 后降级为 local storage。关闭顺序必须先停止
请求 admission，再 drain/fence partition，最后关闭 Nereus runtime 及 owned clients。

## 15. 交付顺序

| Milestone | Scope | Exit condition |
| --- | --- | --- |
| F9-M0 | source lock + code-level design | all target docs、hashes、method map、scenario traceability reviewed |
| F9-M1 | ranged-entry API + NCP2/NTC2 foundation | legacy exact-start and Pulsar gates unchanged；range property/golden tests pass |
| F9-M2 | binding、authority session、checkpoint | real Oxia/Object-store response-loss and leader-preemption gates pass |
| F9-M3 | native log Produce/Fetch | exact bytes/offsets/acks/HW across restart and broker takeover pass |
| F9-M4 | producer/transaction/internal topics | idempotence、transaction、LSO、coordinator recovery pass |
| F9-M5 | retention/DeleteRecords/compaction | native policies and compacted internal/user topics pass |
| F9-M6 | activation/controller/runtime rollout | RF=1 enforcement、mixed-version exclusion、shutdown/rejoin pass |
| F9-M7 | scale/chaos/compatibility aggregate | full scenario matrix and upstream Kafka compatibility suites pass |

文档完成不代表任何 milestone 已实现。

## 16. 关键不变量

1. `Kafka offset == Nereus logical offset`。
2. 一个 Kafka batch 的 bytes、CRC 与 offset span 不被普通 append 改写。
3. stable head 是 Produce success 与 HW 前进的必要条件。
4. leader epoch 只增不减；新 leader 能立即 fence 旧 leader。
5. topic name reuse 永不复用 stream identity。
6. checkpoint 只能加速 recovery，不能制造 committed data。
7. group offset 不阻塞 Kafka retention。
8. compaction 产生新 generation，不改变 source offsets。
9. default read/append API behavior 对非 Kafka caller 保持兼容。
10. unknown completion 收敛前没有后继 append。
11. local disk、object LIST 和 routing cache 永不成为 correctness truth。
12. `Designed` capability 在 implementation/final gate 前不得对外宣称可用。

## 17. 权威文档

- 总入口：`../phase-9-kafka-native-storage/README.md`
- 当前合同与 AutoMQ 审计：`../phase-9-kafka-native-storage/01-current-contract-and-automq-source-audit.md`
- ranged entry/API/格式：`../phase-9-kafka-native-storage/02-ranged-entry-api-and-object-format.md`
- Kafka fork 接入：`../phase-9-kafka-native-storage/03-kafka-fork-log-and-broker-integration.md`
- Oxia/lifecycle/checkpoint：`../phase-9-kafka-native-storage/04-oxia-binding-session-checkpoint-and-lifecycle.md`
- producer/transaction/compaction/retention：`../phase-9-kafka-native-storage/05-producer-state-transactions-compaction-and-retention.md`
- 配置/rollout/运维：`../phase-9-kafka-native-storage/06-runtime-configuration-rollout-and-observability.md`
- 实施计划与 gates：`../phase-9-kafka-native-storage/07-implementation-plan-and-gates.md`
- 场景证据矩阵：`../phase-9-kafka-native-storage/08-scenario-evidence-matrix.md`
- F9-M0 设计评审：`../phase-9-kafka-native-storage/09-f9-m0-design-review-2026-07-23.md`
