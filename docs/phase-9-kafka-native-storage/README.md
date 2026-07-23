# Phase 9 — Native Kafka Shared-Storage Code-Level Target

> 状态：In progress；F9-M1/M2 implementation complete；F9-M3 Nereus raw RecordBatch + serialized partition IO + bounded append/async Fetch + binding-first leader manager + storage-profile policy + exact bounded ListOffsets scan + activation-backed Object-WAL provider/checkpoint/read-pin/paged-replay runtime + local Kafka-fork stock-RecordBatch recovery-state/metadata-lifecycle/deferred-provider/log-factory slices implemented；F9-M4 NKC1 producer/open-transaction/aborted-transaction canonical state、strict V1 codec 以及 idempotent/transaction/control exact append encoding partial slices implemented；F9-M6 config schema/typed snapshot/pure startup validation + adapter process lifecycle/resource-ownership + activation metadata/coordinator + broker publisher/verifier/runtime startup fence + generic BrokerServer lifecycle partial slices implemented；M2 direct real-service gates pass；fresh inherited final gate blocked by local Pulsar source-lock drift；exact recovery state/storage publication、同步 UnifiedLog correctness bridge，以及有界 ReplicaManager Produce 和 whole-request multi-partition async Fetch handoff 已实现，但 M4 Kafka fork import/replay、transaction request semantics、internal-topic coordinator ordering、CLI/KafkaRaftServer production selection 与真实 KRaft gate 仍未实现
> Future：F9 Native Kafka Shared Storage
> 目标日期基线：2026-07-23
> AutoMQ 参考锁：`1c648d84819d5c3fef2af585f02149c397584870`（`3.9.0-SNAPSHOT`）
> Kafka fork development lock：local `nereus/future9-native-kafka-storage@47d36a1d9fd3ae670e6b799b90df42fb86502e41` from Apache `427b409cf440f745ad6195673d3342f6bd3974d4`（remote push pending）
> F9 implementation base：`main@112c459`；M3 adapter slice base：`main@6fe5a7e`

本目录是原生 Kafka 与 Nereus 集成的代码级 target contract。这里的 class、method、record、key、状态机和
gate 是实现约束。F9-M1 已落地 conditional append、Object/BookKeeper ranged readers、semantic views、
NCP2/NTC2、exact format dispatch/capability 和真实 S3 round trip；F9-M2 已落地 binary-safe authority API、
authority-bound head CAS、V1/V2 dual codec、Kafka partition binding 的显式 codec、真实 Oxia store、
deterministic create/delete、64-shard hint scanner、NKC1、受保护 publication/fallback 和 fresh-state replay
状态机；F9-M3 已开始落地 raw magic-v2 RecordBatch 严格校验、exact per-batch append encoding、serialized stable
partition append、bounded containing-entry Fetch assembly、owned Produce byte budget/bounded executor 和 fail-closed append
outcome classifier，以及 actual-byte minBytes/maxWait/event-coalescing 的 multi-partition async Fetch operation。
process-local leader manager 也已按 KRaft leader/broker term 拒绝 stale/conflicting/late open。Nereus-side ListOffsets
resolver 已以单一 stable snapshot 支持 exact earliest/latest 和由 fork record iterator 驱动的有界 timestamp/max
timestamp committed-tail scan；fork 已把该 resolver 转为 Kafka `OffsetResultHolder` 的同步/异步结果并接好取消、
delayed-operation wakeup 与错误映射；stock `Partition` 已支持同 leader epoch 的 lookup install/remove 并在 epoch/
follower/offline/delete 时撤销，`ReplicaManager.fetchOffset` 已把完成唤醒接到现有 delayed ListOffsets purgatory。
fork-owned `NereusListOffsetsLifecycle` 现把同一个 manager-returned recovered storage 组装为 resolver/bridge，并保证
stock leader epoch publication 之后才安装、resign/delete/shutdown 之前先 identity-safe 撤销；late old open 和 stale
resign 均不能覆盖或移除新 lookup。fork 的 stock-compatible `AsyncTopicDeltaLifecycle`、`ReplicaManager.applyDelta`
leader-publication callback、`NereusTopicDeltaLifecycle` 与 `BrokerMetadataPublisher` optional branch 已落地：新 leader
在 stock state publication 后同步进入 exact-epoch recovery-pending，恢复成功后才通知 internal coordinator election；
follower/delete callback 等待 manager lifecycle 完成，delete→同名 recreation 按 partition 串行；
`firstPublishFuture` 明确不是 partition-readiness barrier。恢复 factory 先把 frozen recovered state 发布到 exact
`NereusUnifiedLog` shell，lifecycle 再把同一个 manager-returned recovered storage 发布到该 shell，最后才安装
ListOffsets lookup；任一步失败都按 epoch/instance 撤销。fork 已注册完整 58-key
`nereus.kafka.storage.*` `ConfigDef`，构建无副作用
immutable typed snapshot，并在 enabled-only pure validator 中拒绝非 broker、RF/minISR、remote log、stock cleaner、
AutoMQ mode、request hard limit 与 authoritative log-directory conflicts；disabled default 不创建资源。Kafka fork
现有 stock-compatible `BrokerStorageRuntimeFactory` 显式注入 seam：runtime 在 LogManager IO 前创建，start future 在
unfence/request processing 前等待，optional metadata lifecycle 传给 publisher，shutdown 在 request handlers 前关闭
admission、ReplicaManager 前等待 drain、LogManager 后关闭 runtime；enabled 且无 concrete factory 会 fail closed。
adapter 已新增 exact `NereusKafkaRuntime`、drain reason、immutable health snapshot 和 thread-safe admission gate，且保证
drain/close 终态不能被 late readiness callback 重新打开；`DefaultNereusKafkaRuntime` 进一步提供 operation-owned
start/drain、non-destructive timeout view、partition-manager shutdown 和 idempotent close，`KafkaRuntimeResources` 明确
OWNED/BORROWED 身份、拒绝同一实例的重复/混合所有权，并按构造逆序 attempt-all close。
`NereusKafkaRuntimeFactory` 已用 immutable product config 和显式 dependencies 组装同一
`StreamStorage`、Kafka binding store、keyspace/lifecycle、opener、partition manager 与 process runtime；provider
resources 按构造顺序转入 ledger，Kafka scheduler/clock/recovery-state factory 保持 borrowed，且 factory 不导入 Kafka
server 类型；runtime configuration 同时冻结 executable profile set，manager 在 durable binding 之前拒绝当前 provider
图无法执行的 profile。`NereusKafkaObjectWalRuntimeFactory` 已提供首个 concrete creator：显式 provider instance、
`ObjectStore`、shared Oxia、L0/physical/binding stores、protection manager、callback executor 和 strict generation-zero
`DefaultStreamStorage` 被组装为仅支持 `OBJECT_WAL_SYNC_OBJECT` 的 runtime，legacy auto-session 被强制关闭，失败切点
逆序回收。该 factory 现在还拥有 checkpoint `ObjectReadPinManager`、reader/verifier/coordinator、按
`recoveryChunkRecords/recoveryChunkBytes` 有界分页的 COMMITTED batch source 与 concrete
`DefaultKafkaPartitionRecoveryLauncher`；fork 只借入每次 open 的 fresh state codec/publisher factory。production
`createActivated` 进一步在同一 shared Oxia 上组装 activation store/runtime：先发布 broker capability，
在双重（wall deadline + 最大尝试数）有界轮询内等待 ACTIVE/readiness，严格校验后才执行原 startup action；续租失败会撤销
admission，关闭顺序保证先取消续租/轮询再关 activation store，借入 scheduler 不关闭。未激活 creator 已降为 package-private
failure-cut test seam。real-Oxia + local-file provider gate 已升级为 activation-backed startup、leader open、authority recovery、
stable Produce/Fetch 与 close。production creator 还会把 fork 的 KRaft/local-log snapshot 与全部 64 个 binding-registry
shard 的首键 existence proof 合并；任一历史 binding hint 都会阻止 first activation，只有全分片为空才保留
`bindingsPresent=false`。
activation control plane 已新增 V1 protocol activation、epoch-scoped broker capability、exact broker-set readiness records 与
closed codecs；同一 deterministic Oxia partition 上的 create/exact-version CAS 会校验 key/value identity、不可变 tuple、
one-way ACTIVE、heartbeat/readiness monotonicity，并恢复 applied-but-response-lost。broker 侧 capability publisher 已按固定周期
续租并在首次失败后停止，ACTIVE verifier 会把当前 KRaft cluster/feature/broker epoch set 与 activation、readiness、逐 broker
capability、profile 和 provider scope 一次性交叉校验后才允许启动继续；兼容能力摘要已冻结为 domain-separated canonical bytes。
`KafkaStorageFirstActivationCoordinator` 已实现 controller 侧的 empty-cluster 双重快照证明、全 broker capability 聚合、
readiness create/CAS、PREPARED create/resume、ACTIVE one-way CAS 与并发 controller 胜者恢复；ACTIVE 幂等重试不再错误要求
集群持续为空。deterministic 与 real-Oxia reconnect gates 已通过。BookKeeper/async-object creator、Kafka controller seam
integration、checkpoint failure 的 durable quarantine/audit observer、CLI/KafkaRaftServer production factory selection、
checkpoint time-index candidate、五档
real-service profile matrix 与真实 KRaft Produce/Fetch/ListOffsets 尚未实现。`NereusUnifiedLog` 已有首条同步
correctness bridge：stock validation/offset assignment 后把 exact bytes 交给 adapter stable append，成功后才推进 shell
LEO；Fetch 把 bounded adapter assembly 转回 `MemoryRecords`，synthetic segment size 保持 `0`。fork-owned
`NereusRecordTimestampInspector` 已在隔离本地 branch 使用
stock Kafka 4.3 `MemoryRecords` 实现；bridge/lifecycle tests、18 个 mapper/context/runtime-focused tests、完整 stock
`KafkaConfigTest` 以及 server/core/storage checkstyle/SpotBugs/Spotless 已通过；`7739351b7c` 对应的
`phase9M3KafkaForkCheck --rerun-tasks` 通过 80/80 outer tasks，nested stock/artifact-enabled Kafka builds 通过 92/92 与 95/95 actionable
tasks；`dc8c66388a` 在加入 stable append/read、required-acks 与 failure-fence 覆盖后再次通过相同
80/80、92/92、95/95 exact-head aggregate。`ee608625e4` 进一步接入 stock-owned optional
`BrokerStorageAppendExecutor`：请求线程先做整请求字节校验并复制每分区 exact `MemoryRecords`，product executor
在 `threads + queueCapacity` 的逻辑上限内按 `TopicIdPartition` FIFO、跨分区并行/公平执行；worker 使用
`RequestLocal.noCaching`，stable terminal 后才聚合 validation stats、delayed produce 与 response callback。
runtime drain 同步停止新 admission，并同时等待已接纳 Produce 与 product runtime。当前 GitHub credential 对
组织 fork 只有 read 权限，所以该 commit 尚未推送，不能升级为 production fork lock。该 exact head 的 fresh
`phase9M3KafkaForkCheck --rerun-tasks` 通过 80/80 outer tasks，nested stock/artifact-enabled Kafka builds
通过 92/92 与 95/95 actionable tasks，包含 146/146 scenarios、real provider recovery、stock KRaft restart 和
全部 format/static gates。Kafka
storage profile policy 已冻结五个 canonical profile，并禁止 request acks 弱化 profile default durability/completion。
M4 的首个产品侧切片新增 Kafka-artifact-neutral `KafkaProducerTransactionState` 和
`KafkaProducerTransactionStateCodecV1`：section 1/2/7 按文档规定的 big-endian canonical layout 编码，严格校验
排序、producer/current-open-transaction 等价、五批 duplicate window、sequence wrap、checkpoint offset、aborted
transaction marker offset 单调性及允许 LSO 低于 marker 的 stock 语义、required/version/flags/count/truncation/EOF，并以 frozen digest 和 200 轮固定种子随机状态证明
decode/re-encode byte exact。当前只允许 normal checkpoint barrier；completed-but-not-finalized open transaction
在没有显式 section flag 前 fail closed。产品侧 `KafkaAppendBatchEncoder` 已解除 M3 数据类型闸门，接受 codec
严格校验后的 idempotent、transactional 与 control magic-v2 batch，并继续逐 batch 保存 exact bytes 与 logical
offset span；该改变不绕过 fork 的 stock producer/transaction validation。当前锁定 fork 尚未提交
`ProducerStateManager` import/replay 和事务 request path，因此不能据此声明 M4 完成。
为接入 stock transaction state，product partition boundary 已把 durable end 与 derived visibility 拆开：
stable append 先推进 exact end/commit version 并保留旧 HW/LSO；fork 必须在 stock producer/transaction 更新成功后
调用 `publishDerivedOffsets(exactEnd, HW, LSO)`，随后才发布 `STABLE_APPEND` 并 dispatch 同 partition 下一次
append。该边界已覆盖 exact-end mismatch、offset 越界、initialized offset 回退、queued append、resign 和 renewal
failure；fork wiring 仍在下一切片。
product 侧 whole-request `KafkaFetchWaveOperation` 把 stock read-wave 保持为 opaque payload，只负责
subscribe-before-read、事件合并、single in-flight wave、独立 callback、显式 cancel/cleanup，以及不受 event-reread
预算影响的 deadline final read。`bba3ef0121` 已将其接入 stock-owned `BrokerStorageFetchExecutor`：
`ReplicaManager.fetchMessages` 在 Nereus mode 把完整 ordered request 交给 bounded worker，每一 wave 仍调用 stock
`readFromLog`；initial wave 与 delayed-fetch 等价的 event/deadline wave 使用准确的 `readFromPurgatory` 模式，
保留 request-wide byte budget、partition order、divergence/preferred-replica/remote-fetch/error facts。实际 records
bytes 决定 `minBytes`，所有 partition 的 stable/leadership event 合并后重读，deadline 总有一次 final read。
disabled mode 仍走原 stock purgatory 分支。首次 aggregate 发现 simultaneous wakeup 可能在 idle worker dequeue
前填满 excess queue；`47d36a1d9f` 保留相同 `threads + queueCapacity` logical cap，但为每个 admitted operation
保留一个内部 control-runner slot，避免把已接纳 Fetch 错误拒绝。当前 exact head 的 fresh
`phase9M3KafkaForkCheck --rerun-tasks` 已通过 80/80 outer tasks，nested stock/artifact-enabled Kafka builds
通过 92/92 与 95/95 actionable tasks，包含新 Fetch race regression、146/146 scenarios、real provider recovery、
stock KRaft restart 与全部 format/static gates。
binding-first storage manager 已把 deterministic ACTIVE binding、exact profile、leader authority 和 remaining recovery
deadline 冻结为 opener plan；protocol-neutral exact stable-head/session/authority/durable-digest snapshot seam 也已落地，
并支持 genesis commitVersion `0`。Exact commit-ancestor reachability、source validator 与 concrete
session/head/recovery opener 已组装；public binary-safe session renewal 与 partition-owned periodic renewal 已落地，
renew failure/invalid token 会立即 write-fence 且阻止 queued append dispatch。Kafka fork generic BrokerServer lifecycle
wiring 已落地并通过 stock KRaft restart；provider-backed runtime composition 与 per-broker log-factory selection 已落地，
controller scheduling、production factory selection 和真实 native-storage KRaft gate 尚未闭合。
fork `617451957c` 已把该 generic seam 接到 adapter contract：显式 typed creators 交付 runtime 与 ListOffsets limits，
同一 product manager 只绑定一个 exact `ReplicaManager`，构造 `NereusListOffsetsLifecycle`/`NereusTopicDeltaLifecycle`，
并在 runtime drain 时同步撤销 lookup admission；disabled build 排除全部 adapter-backed sources。
fork `94ecf8c105` 随后新增无 I/O 的 closed mapper：真实 broker epoch、58-key typed snapshot 和 build labels 被映射为
strict Object-WAL runtime/Oxia/ObjectStore/StreamStorage/capability/ListOffsets 配置；broker epoch `0` 通过
`epoch + 1` 变成合法且单调的 operation epoch。当前只接受显式 `s3` token 和
`OBJECT_WAL_SYNC_OBJECT`，没有 reflection/provider-class fallback。fork `c27305a7ad` 又把该 mapper 接到
`NereusKafkaProductRuntimeCreator`：factory construction 保持无 provider I/O，`start()` 等待真实 broker epoch 后显式创建
S3/Object-WAL activation runtime；Kafka scheduler/Time 被借入，single-image KRaft snapshot 与 conservative local-log fact
交给 product binding-aware wrapper，manager proxy 等待 readiness 且逐操作复查 admission。Recovery-state factory bridge
只在 exact ReplicaManager 出现后 one-time bind；fork 的 `NereusKafkaRecoveryStateFactory` 为每次 open 创建
stock `MemoryRecords`/`RecordBatch` 驱动的新鲜 M3 状态，校验 CRC、单 entry/单 batch、稠密 offset、压缩 record、
timestamp 与 leader-epoch ranges，并在 exact current `Partition` 上短临界区 provisional 发布。final source
revalidation 失败会撤销该 epoch 的 provisional state。M3 对 idempotent/transaction/control batch 和 NKC1 派生 section
继续 `UNSUPPORTED_FORMAT` fail closed，交给 M4。fork `cfcdd55fbc` 新增 per-broker `UnifiedLogFactory` seam、
cache-root-only `NereusUnifiedLogFactory` 和 `NereusUnifiedLog`/`NereusLocalLog` ephemeral state-machine shell；
`LogManager` 在 Nereus mode 不扫描旧 local logs，也不运行 cleaner/retention/flusher/checkpoint/clean-shutdown local truth。
`BrokerServer` 从 exact runtime 注入 factory；恢复 state、storage、ListOffsets lookup 按顺序发布，shell 只有前两者匹配
exact identity/epoch/stable snapshot 后才报告 writable。`7739351b7c` 补齐所有 stock-file 成对 inject marker。当前
`dc8c66388a` 再增加 stock-only `RequiredAcksAwareAppend` seam、`Partition` 的 exact `-1/0/1` 传递与同步
`NereusUnifiedLog` append/read bridge。append timeout、stable-result contract violation 或 stable commit 后 stock
state update failure都会 resign/fence；M3 idempotent/transaction/control batch 映射
`UNSUPPORTED_FOR_MESSAGE_FORMAT`。`ee608625e4` 把 Produce caller 迁到有界 handoff；同步 wait 仍存在于 executor
worker 内的 `NereusUnifiedLog` stock state-machine boundary，不再占用 request handler。`bba3ef0121` 同样把
whole-request Fetch read wave 迁到 runtime-owned bounded worker；逻辑 operation permit 覆盖等待期，独立 callback
executor 负责最终响应，request handler 与 purgatory thread 都不再执行 Nereus storage wait。Controller
first-activation scheduling、CLI/KafkaRaftServer production factory selection、BookKeeper/async provider 和真实
native-storage KRaft process gate 仍未组装。
若以后
实现与本文不同，必须先更新合同、版本和兼容性分析，不能让代码静默改变 durable bytes 或 correctness owner。

## 1. 设计文档

| 文档 | 权威范围 |
| --- | --- |
| `01-current-contract-and-automq-source-audit.md` | 本地 Nereus/AutoMQ 源码事实、可复用边界、gap inventory |
| `02-ranged-entry-api-and-object-format.md` | protocol-neutral API、ranged entry、read boundary、NCP2/NTC2 bytes |
| `03-kafka-fork-log-and-broker-integration.md` | Kafka fork classes/methods、Produce/Fetch、LEO/HW/LSO、error mapping |
| `04-oxia-binding-session-checkpoint-and-lifecycle.md` | keyspace、records/codecs、leader authority、partition lifecycle、recovery |
| `05-producer-state-transactions-compaction-and-retention.md` | producer state、transaction、internal topics、virtual segment、retention/cleaner |
| `06-runtime-configuration-rollout-and-observability.md` | config、runtime ownership、activation、upgrade、metrics/alerts/runbook |
| `07-implementation-plan-and-gates.md` | package/file ownership、milestone DAG、build/test gates、definition of done |
| `08-scenario-evidence-matrix.md` | requirement-to-test traceability、failure cuts、scale/compatibility aggregate |
| `09-f9-m0-design-review-2026-07-23.md` | dated M0 coverage/status/scope audit；非新的运行时合同 |

North-star 摘要见 `../design/nereus-future9-kafka-native-storage.md`。发生冲突时：已实现代码/测试优先于
代码级合同；代码级合同优先于 Future 摘要；本地锁定源码优先于对 AutoMQ 或 Kafka 的记忆。

## 2. F9 与 F5 的不可合并边界

F5 和 F9 是两个独立 consumer：

```text
F5: Kafka client -> KoP/Pulsar facade -> Nereus
F9: Kafka client -> native Kafka fork -> Nereus
```

禁止事项：

- F9 不读写 F5 的 `KAFKA_RECORD_V1` projection record；
- F9 不把 native group/transaction state 存成 F5 Oxia coordinator record；
- F5 不按 F9 ranged `RecordBatch` 解释已有 one-record-per-offset payload；
- 任一 track 若要读取另一 track 的 stream，必须新增显式 mapping/migration version；
- `nereus-kop-adapter` 与计划中的 `nereus-kafka-adapter` 不能互相依赖。

## 3. 目标模块依赖

未来只新增一个 Kafka-aware Nereus module：

```text
nereus-kafka-adapter
  -> nereus-api
  -> nereus-core
  -> nereus-metadata-oxia
  -> nereus-object-store
  -> nereus-materialization
  -> Kafka server/common dependencies (module-local)
```

`nereus-api`、`nereus-core`、`nereus-metadata-oxia` 和 object formats 只接受 protocol-neutral evolution。
Kafka `TopicPartition`、`MemoryRecords`、`RecordBatch`、`Errors` 等类型只存在于 adapter 或 Kafka fork。

计划 package：

| Package | Owner |
| --- | --- |
| `com.nereusstream.kafka.config` | typed Kafka/Nereus config and validation |
| `com.nereusstream.kafka.runtime` | process runtime、owned/borrowed resources、activation |
| `com.nereusstream.kafka.partition` | partition identity、binding、storage facade、state machine |
| `com.nereusstream.kafka.codec` | Kafka batch validation/append/fetch mapping |
| `com.nereusstream.kafka.metadata` | Oxia keyspace、records、codecs、store、scanner |
| `com.nereusstream.kafka.checkpoint` | immutable checkpoint format/store/publication |
| `com.nereusstream.kafka.recovery` | open/replay/unknown-outcome recovery |
| `com.nereusstream.kafka.retention` | log-start、segment retention、DeleteRecords |
| `com.nereusstream.kafka.compaction` | F4 topic codec/strategy and NTC2 rewrite |

## 4. Target call paths

### 4.1 Broker boot

```text
BrokerServer.startup
  -> NereusKafkaRuntimeFactory.create(KafkaConfig)
  -> validate cluster-wide mode / dependency connectivity
  -> advertise exact capability
  -> wait for ACTIVE protocol record
  -> construct LogManager / stock ReplicaManager with runtime-owned factories/executors
  -> accept metadata images
  -> admit client traffic only after readiness
```

### 4.2 Partition leader open

```text
BrokerMetadataPublisher delta
  -> KafkaPartitionStorageManager.onMetadataDelta
  -> resolve/create KafkaPartitionBindingRecord(topicId, partition)
  -> acquire append session with leader authority epoch
  -> load stable stream head + selected checkpoint
  -> replay committed Kafka batches to head
  -> rebuild ProducerStateManager / txn / epochs / indexes
  -> NereusUnifiedLog.publishWritable
  -> Partition becomes leader
```

### 4.3 Produce

```text
KafkaApis.handleProduceRequest
  -> ReplicaManager.appendRecords
  -> Partition.appendRecordsToLeader
  -> UnifiedLog.append (stock validation + offset assignment)
  -> NereusLocalLog.append
  -> KafkaAppendBatchEncoder (one entry per RecordBatch)
  -> StreamStorage.append(expectedStartOffset)
  -> stable AppendResult exact-range validation
  -> Kafka state update / HW = stable end
  -> ProduceResponse
```

### 4.4 Fetch

```text
KafkaApis.handleFetchRequest
  -> ReplicaManager.fetchMessages
  -> Partition.readRecords
  -> NereusLogRecords.read
  -> StreamStorage read(COMMITTED or TOPIC_COMPACTED, CONTAINING_ENTRY)
  -> KafkaFetchAssembler
  -> apply logStart/HW/LSO/isolation/aborted-txn bounds
  -> FetchDataInfo / FetchResponse
```

### 4.5 Delete/retention

```text
KRaft topic delete or DeleteRecords/retention
  -> KafkaPartitionLifecycleCoordinator / KafkaRetentionCoordinator
  -> guarded binding lifecycle or log-start transition
  -> StreamStorage.seal/delete or trim
  -> verify durable state
  -> checkpoint/index cleanup
  -> delayed physical GC through existing reference fences
```

## 5. Correctness owners

| Concern | Single owner | Forbidden alternate owner |
| --- | --- | --- |
| data commit | Nereus stream head + reachable commit | local LEO、object list、Kafka checkpoint file |
| offset allocation | Kafka validation under partition append lock + Nereus expected-start CAS | adapter-side retry with new offset |
| leader fencing | KRaft leader epoch bound into durable append authority | broker routing/cache only |
| stream identity | Oxia binding keyed by cluster/topicId/partition | topic name / log directory name |
| recovery state | committed bytes replayed from verified checkpoint | local disk snapshot alone |
| group/txn coordinator | native Kafka internal topics | F9-specific Oxia coordinator tree |
| compacted visibility | F4 committed generation | cleaner local swap or task output existence |
| logical retention | Kafka policy -> Nereus trim | consumer group offset floor |

## 6. Code-level invariants

### 6.1 Append

- entries ordered by Kafka base offset；ranges dense inside one request；
- `AppendBatch.recordCount == sum(AppendEntry.recordCount)`；
- `expectedStartOffset == first RecordBatch.baseOffset == current committedEndOffset`；
- success result start/end/count exactly equals request；
- payload bytes are read-only snapshots before async handoff；
- no retry creates a new physical attempt while previous completion is unknown；
- partition append lane remains closed until exact recovery converges。

### 6.2 Read

- storage may return one full entry whose range contains requested start；
- after the containing first entry，all ranges are dense for `COMMITTED`；
- `TOPIC_COMPACTED` rows may be sparse，coverage rather than next row proves skipped holes；
- an indivisible first Kafka batch may exceed both maxRecords and maxBytes once；
- fetch assembler never mutates source payload and never exposes offsets below log start or above isolation bound；
- corrupted CRC/format stops the partition；it is not silently skipped。

### 6.3 Metadata

- all durable formats begin with magic/version or equivalent codec discriminator；
- records reject unknown mandatory fields and invalid enum ordinals；
- every mutating CAS guards partition identity、incarnation、lifecycle、authority epoch and root version；
- no correctness transition requires atomic writes across Oxia shards；
- derived observed offsets in binding/checkpoint never override current stream head。

### 6.4 Lifecycle

- same topic ID/partition cannot bind two live streams；
- same-name recreate cannot reuse deleted binding；
- only current KRaft leader epoch obtains writable session；
- losing leadership fences admission before releasing resources；
- delete is metadata-first and restart-safe；late object PUT cannot resurrect a deleted partition；
- close is idempotent and distinguishes owned from borrowed resources。

## 7. Initial compatibility envelope

| Dimension | Initial target |
| --- | --- |
| Kafka mode | KRaft only |
| Kafka fork baseline | version aligned with locked integration branch；initial audit uses AutoMQ 3.9 fork |
| Kafka replication factor | exactly 1 in Nereus mode |
| Nereus storage profile | immutable per stream；all activated executable profiles；default BK async object |
| topic cleanup policy | `delete`、`compact`、`compact,delete` after F9-M5 |
| compression | exact batch bytes；all Kafka-supported codecs present in locked fork |
| message formats | magic versions explicitly allowed by locked fork/config；no implicit downgrade |
| transactions | required for compatibility claim after F9-M4 |
| tiered storage | stock Kafka remote log disabled for Nereus partitions |
| local log migration | unsupported |
| mixed F9/non-F9 broker | forbidden after activation |
| mixed storage topics | unsupported in first release |

## 8. F9 milestone DAG

```text
F9-M0 design/source lock
  -> F9-M1 ranged API + formats
      -> F9-M2 metadata/session/checkpoint
          -> F9-M3 produce/fetch
              -> F9-M4 producer/txn/internal topics
              -> F9-M5 retention/compaction
                  -> F9-M6 activation/multi-broker rollout
                      -> F9-M7 scale/chaos/compatibility aggregate
```

M4 与 M5 可以在 M3 后并行开发，但 M6 writable rollout 必须消费两者的能力版本；M7 必须执行全部前驱，
不能用 mock-only suite 代替真实 Oxia、BookKeeper/Object store 与多 broker process evidence。

## 9. 文档完成门禁（F9-M0）

F9-M0 只有在以下条件全部满足时完成：

- [x] AutoMQ source commit、version、关键 file blob hash 全部锁定；
- [x] Nereus 当前 API/reader/materialization 的 ranged-entry gap 有具体 class/method 证据；
- [x] F5/F9 ownership 与 coordinator-state 差异写入 roadmap/index；
- [x] public API 变更有 source/binary compatibility 策略；
- [x] NCP2/NTC2 byte layout、limits、golden/corruption tests 完整；
- [x] Kafka fork 每个 stock file 的 inject point、subclass 与 fallback path 列出；
- [x] Oxia keys、records、codec field order、CAS guards、scanner bounds 完整；
- [x] append/leader/delete/recovery/checkpoint/retention/compaction 状态机完整；
- [x] config default、validation、activation、shutdown、metrics、alerts 和 runbooks 完整；
- [x] 每个 MUST invariant 映射到 scenario ID 和计划测试 class；
- [x] 文档内部链接和状态词审计通过；
- [x] 没有提交任何 production implementation code。

勾选只代表 design gate，不代表 F9-M1 之后的能力已实现。

## 10. 评审顺序

1. 先读 `01`，确认事实与参考边界；
2. 再读 `02`，确认所有上层工作都建立在可表达 ranged Kafka batch 的中立合同上；
3. 联合评审 `03` 和 `04`，确认 Kafka state 与 Nereus state 没有双写 truth；
4. 评审 `05`，重点检查 internal topic、transaction、retention 与 compaction；
5. 评审 `06` 的 activation、failure mapping 与 runbook；
6. 用 `07`/`08` 逐项反查每个 class、transition 和 failure cut 是否有实现 owner 与 gate；
7. 用 `09` 查看 F9-M0 的 dated 审计结果、未实现边界与下一里程碑入口条件。
