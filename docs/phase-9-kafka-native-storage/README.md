# Phase 9 — Native Kafka Shared-Storage Code-Level Target

> 2026-07-30 M7 leader-chaos slice：product `main@d9f8ccf` adds `scenarioKfScl006` and root
> `phase9ChaosCheck`。Three real release brokers complete six RF1 handoffs `1→2→3→1→2→3→1` with every previous owner
> left alive；each handoff advances the KRaft/binding/append-session authority，changes the fencing token，leaves no
> reassignment，and preserves contiguous Produce/Fetch/ListOffsets under the new durable head even when the client is
> bootstrapped only through the stale broker。Fresh root rerun passes 75/75 tasks in 59s。KF-SCL-006 is
> `IMPLEMENTED_NOT_RUN` until final aggregation；KF-SCL-007..010 remain open

> 2026-07-30 M7 scale slice：product `main@bbe0881` adds root `phase9ScaleCheck` plus five canonical owners for
> KF-SCL-001..005。A fresh `phase9ScaleCheck --rerun-tasks` passes 36/36 executed tasks in 29s，covering exact
> 16,384-binding/64-shard real-Oxia reconnect，10,000 open partition managers and 64-way maintenance bound，1,000
> Produce/Fetch work items under exact queue/byte/thread budgets，near-`Integer.MAX_VALUE` ranged metadata，and a
> 128-source/1,048,576-record two-pass NTC2 task with bounded winner memory、positive spill and complete cleanup。These
> rows are `IMPLEMENTED_NOT_RUN` until the final clean aggregate；SCL006–010 remain open

> 状态：In progress；F9-M1/M2 implementation complete；F9-M3 Nereus raw RecordBatch + serialized partition IO + bounded append/async Fetch + binding-first leader manager + storage-profile policy + exact bounded ListOffsets scan + activation-backed Object-WAL provider/checkpoint/read-pin/paged-replay runtime + provider-neutral `BOOKKEEPER_WAL_ONLY` adapter composition + local Kafka-fork stock-RecordBatch recovery-state/metadata-lifecycle/deferred-provider/log-factory slices implemented；F9-M4 NKC1 全七 section canonical state/strict V1 codecs/full composition、idempotent/transaction/control exact append encoding，以及 Kafka-fork stock producer/transaction import/replay、checkpoint hydration、HW/LSO publication、READ_COMMITTED/aborted-index、transaction guard/marker-version async executor handoff 和 recovered-storage-before-coordinator-election deterministic slices implemented；F9-M5 stock-compatible retention planner + checkpoint-before-trim/response-loss barrier + stock DeleteRecords fork invocation + checkpoint-restorable virtual segment/config/index state + periodic owned-partition retention runtime + ranged Kafka compaction decode/rewrite/exact-source/sorted-spill/KCRS-to-NTC2 preparation + object upload/Generation publication/coverage-CAS linearization + exact activated-generation discovery/generation-constrained runtime reads + binding-rooted compacted-prefix/committed-tail no-resurrection routing + recoverable single-partition pass + terminal dual-root retirement + bounded orphan scan/non-overlapping scheduler-owner + projection-free direct-stream registration/Generation authority fence + activated Object-WAL production composition + fork-owned compaction registration/partition-lock authority/stock transaction-marker pre-scan deterministic slices implemented；F9-M6 config schema/typed snapshot/pure startup validation + adapter process lifecycle/resource-ownership + activation metadata/coordinator + broker publisher/verifier/runtime startup fence + generic BrokerServer lifecycle + stock-source isolation + explicit native-storage launcher + controller-leader-only activation scheduling + durable feature/format + dedicated-controller admission + controller single-copy enforcement slices implemented；checkpoint recovery/retention 的 exact-reference durable quarantine/redacted first-failure audit 已接入 Object-WAL/BookKeeper-WAL-only product runtime，并通过 deterministic 与 real-Oxia reconnect gate；real Oxia + two-bookie adapter gate and real release-distribution/fresh-JVM process gates now prove all three BookKeeper profiles can open a leader、Produce、Fetch、ListOffsets and recover cold；the same installed Object provider exposes both `OBJECT_WAL_SYNC_OBJECT` and `OBJECT_WAL_ASYNC_OBJECT`，and an independent real release/fresh-JVM gate proves async Object-WAL Produce/Fetch/ListOffsets/cold recovery；Kafka NCP2 direct-stream materialization owns one process-shared lifecycle、ranged writer/strict verifier、direct planner/scanner/required-generation/commit authority and async append lag admission；real Oxia/ObjectStore/two-bookie provider gates prove byte-exact NCP2 publication/read for Object async and BookKeeper async/sync，including the sync append completion barrier；Kafka fork now owns 100-key ConfigDef、complete typed BookKeeper binding、password-file identity、BookKeeper client construction/close ordering、six-key ledger-GC rollout policy、three-key materialization-retirement policy and exact five-profile default/capability mapping；the provider graph now conditionally composes materialization → Kafka stream-coverage deletion activation → BookKeeper retention，while safe defaults create none of the deletion owners；deterministic 64+64-shard fail-closed proof、real Oxia + two-bookie adapter-level physical deletion/applied-delete response-loss and release-distribution normal-stop/fresh-JVM NCP2 fallback after physical deletion all pass；real release-distribution combined-node KRaft + four-shard Oxia + pinned LocalStack S3 gate now passes explicit format、registration/activation、Admin create、Produce/Fetch/ListOffsets、object persistence、SIGTERM、fresh-JVM higher-broker-epoch ACTIVE-readiness refresh、user/`__consumer_offsets`/`__transaction_state` concurrent remote recovery、consumer-group committed-offset resume and committed-transaction continuation；real-Oxia two-runtime Object-WAL gate now proves higher-leader-epoch live preemption、exact committed replay、old-writer recovery fencing and new-writer continuation before TTL；Object-WAL P/C takeover、BookKeeper 三 profile post-handoff P matrix、五 profile transaction-marker before/after-provider process cuts 与五 profile mandatory-internal-topic NTC2 真实对象删除/损坏及 repair/re-election 均已通过；checkpoint publication/final aggregate、M5 full-format/restart matrix 与更广 chaos 仍 open
> 2026-07-29 状态增量（覆盖上一行末尾的旧 open-item 描述）：真实 two-release-process Object-WAL/KRaft singleton reassignment、旧 owner resignation、committed recovery 与 new leader continuation 已通过；三 release JVM + Toxiproxy + `jcmd` + `SIGSTOP/SIGCONT` gate 又闭合了 Object-WAL already-dispatched old append 的 P/C 切点，并证明 stale append 在 guarded upload 前被新 session fence、WAL key 集与 durable LEO 不变；stock ZooKeeper + two-bookie + two Kafka release-process gate 闭合 `BOOKKEEPER_WAL_ONLY`、`BOOKKEEPER_WAL_ASYNC_OBJECT`、`BOOKKEEPER_WAL_SYNC_OBJECT` 的 post-handoff recovery/continuation 与 profile object invariants；test-only Byte Buddy agent gate further holds one real Bookie-acked write before `WRITING -> DURABLE`，proves exact physical-entry presence、new-leader `ABANDONED`/`SEALED` recovery、stale completion fencing and final LEO `2`；three combined broker/controller release JVMs with three static voters now prove ACTIVE-state controller kill/failover、per-epoch Nereus activation reconciliation and native IO continuity；a second three-dedicated-controller/one-broker gate now covers before-provider and after-provider cuts for readiness create、PREPARED create and ACTIVE CAS，kills the exact controller and proves higher-epoch recovery plus native IO；a third gate on the same role-separated topology closes both sides of the initial snapshot proof and capability aggregation；the Oxia-proxied dedicated-controller/broker gate proves actual transport reset normalization and same-controller-epoch recovery；native DeleteRecords now also publishes NKC1 before trim、survives forced process death、hydrates the pre-trim checkpoint window、applies the current durable trim and rebuilds virtual segments before continuing native IO；a second Object-WAL process gate loses the successful provider completion before binding/local publication，forces broker death，then proves fresh-process convergence and a same-target no-op without another trim CAS/checkpoint object；remaining open items are transaction/internal-topic coordinator migration and broader BookKeeper/all-profile chaos
> 2026-07-29 trim matrix 增量（覆盖上两行的 response-loss open wording）：Object sync/async 与 BookKeeper WAL-only/async/sync 五档均已通过 provider-applied/caller-unobserved trim、forced restart、same-target no-repeat 与 continued IO；仍 open 的是 transaction/internal-topic coordinator migration、batch middle/end/HW、stock oracle 与 broader chaos
> 2026-07-29 coordinator migration 增量（覆盖上两行的 coordinator open wording）：product `7c25d2e` 在 fork `1cbe8b65a8` 上以两个真实 release JVM 原子迁移 user、`__consumer_offsets-0`、`__transaction_state-0` 到 broker 2，旧 broker 保持存活；group offset 和 completed transactional-ID state 在 recovered-storage-ready 后恢复并继续提交。Ongoing/aborted coordinator takeover、mandatory internal-topic NTC2 failure、batch middle/end/HW、stock oracle 与 broader chaos 仍 open
> 2026-07-29 ongoing transaction migration 增量（覆盖上一行的 ongoing/aborted open wording）：product `efe782d` 新增 `f9OngoingTransactionMigrationProcessIntegrationTest`，在两个 live Object-WAL release brokers 间来回迁移 user 与 `__transaction_state-0`；两个 OPEN transaction 分别跨 handoff COMMIT/ABORT，LSO 收敛、same-ID continuation 与 READ_COMMITTED aborted filtering 均通过。Injected abort-resolution failure、mandatory NTC2、BookKeeper/profile expansion、batch middle/end/HW、stock oracle 与 final aggregate 仍 open
> 2026-07-29 mandatory internal-topic NTC2 deterministic 增量：product `b6b02f4` + fork `89b66ab03b` 在 coordinator election 前加入 binding-rooted、generation-constrained `TOPIC_COMPACTED` availability gate；每个未 trim 的 activated generation 都执行一个有界 probe，任一 `OBJECT_NOT_FOUND`/corruption/metadata failure 都阻止 `openLeader` 完成、撤销 exact pending lookup 并 resign recovered storage，且不存在 COMMITTED fallback。`KafkaInternalTopicNoResurrectionTest`、fork lifecycle tests、完整 `phase9M3KafkaForkBridgeCheck` 均通过；physical repair evidence is recorded in the next increment
> 2026-07-29 mandatory NTC2 真实故障/修复增量（覆盖上一行末尾的 process-cut open wording）：product `0ae8ca9` + fork `768924da60` 的 `f9MandatoryInternalTopicNtc2ProcessIntegrationTest` 在真实 Oxia、LocalStack 与两个 live release Kafka JVM 上激活 `__consumer_offsets-0` NTC2，依次执行物理删除与原 key/metadata 下的 byte corruption；两次迁移都在 coordinator election 前 quarantine exact physical root/index 并保持 group service unavailable，且没有 COMMITTED fallback。恢复原 bytes、user metadata、content type 与 provider CRC 后，product 校验 HEAD length/CRC/ETag、full-read CRC/SHA，CAS 恢复同一 root/index，并以 `REPLACE` 发布新 generation-set digest/activation epoch；两次 ordinary reassignment 都恢复 committed group offset `1`。该行当时只覆盖 Object sync；Object async/BookKeeper expansion 由后续 `4676c12` 增量闭合
> 2026-07-29 retention/DeleteRecords 增量（覆盖上一行末尾的 batch/oracle open wording）：product `77480cb` + fork `bd9963c980` 新增独立 `phase9M5KafkaRetentionOracleCheck` 和 `f9DeleteRecordsBoundaryProcessIntegrationTest`。前者用四个真实 stock `UnifiedLog` closed/active segments 对比 Nereus planner 与 `deleteOldSegments()` 的 selected count/logStart，覆盖 time、size、combined、HW cap、strict equality、compact-only；后者在真实 Oxia/LocalStack/release Kafka 上用三个三-record batches 依次执行 target `3/4/6/-1`，要求 low watermark `3/4/6/9`、latest 恒为 `9`、每次 Fetch 首条 offset/value 与新 logStart 一致，并验证首个 trim 的 rooted NKC1。KF-RET-001/002/003/006 现为 `PASSED_CURRENT_SOURCE`；stock `LogCleaner` compaction differential、injected transaction-resolution 与 final aggregate 仍 open
> 2026-07-29 stock compaction oracle 增量（覆盖上一行的 `LogCleaner` open wording）：product `666bab1`/`08fe686` + fork `c4a0a2d1fa`/`bf8a2946e5` 新增独立 `phase9M5KafkaCompactionOracleCheck`。四个真实 stock `Cleaner` 场景与 Nereus 两遍 executor 比较 survivor offset、key/value、timestamp、compression、delete horizon、transaction/control、producer id/epoch/sequence 和 leader epoch，覆盖 keyed/tail winner、stock null-key drop、tombstone first/later horizon、committed/aborted data、required/empty marker 与 idempotent sparse batch。Oracle 暴露并修复了 null-key 误保留和 `RETAIN_REQUIRED` marker 过早 horizon 两处偏差；fork development dependencies 现在只对隔离 Nereus 坐标标记 changing/zero-cache。KF-CMP-001/002/003/004 现为 `PASSED_CURRENT_SOURCE`；OPEN crossing 仍由产品事务门禁证明，不冒充 stock cleaner 可清理前缀证据，full compression/header matrix、injected transaction-resolution 与 final aggregate 仍 open
> 2026-07-29 transaction-resolution cut 增量（覆盖前文的 injected-resolution open wording）：product `04e661e` + fork `1e3783458b` 新增 test-only Byte Buddy append-boundary agent 与 `f9TransactionResolutionCutProcessIntegrationTest`。真实 Object-WAL 双 broker 先迁移一个 OPEN transaction 的 user partition，再分别在 abort marker 进入 provider 前和 provider 已 durable 完成但 caller completion 丢失后杀死目标 broker；fresh process 从同一 binding 恢复，READ_COMMITTED/LSO 跳过 aborted data，同一 `transactional.id` 继续提交。该门禁同时修复并锁定 stock `TransactionMarkerChannelManager`：metadata 中仍存在但暂时 `leader=-1` 的 partition 必须进入 unknown-broker retry queue，只有 metadata 已不存在才可跳过。强制 fresh rerun 66/66 tasks、1m30s 通过；KF-TXN-008 为 `PASSED_CURRENT_SOURCE`，non-Object profile 与 final aggregate 仍 open
> 2026-07-29 transaction-resolution 五 profile 增量（覆盖上一行的 profile open wording）：product `2d7091d` 新增 `f9TransactionResolutionProfileMatrixProcessIntegrationTest`，把同一 before/after-provider 进程切点扩展到 `OBJECT_WAL_ASYNC_OBJECT`、`BOOKKEEPER_WAL_ONLY`、`BOOKKEEPER_WAL_ASYNC_OBJECT` 与 `BOOKKEEPER_WAL_SYNC_OBJECT`；与原 Object-sync gate 合计十个真实切点场景。每个 BookKeeper cut 使用独立、确定的 ledger-id namespace；WAL-only 要求 bucket 恒空，async/sync 要求 materialized object 可见。首轮真实矩阵暴露 `BookKeeperLedgerHandleCache` 在容量已满时只清理超时 idle handle、不会淘汰已 release 的 LRU handle，第三个 ledger 因而错误返回 `BACKPRESSURE_REJECTED`；生产修复现会先淘汰 released LRU，所有 handle 仍被 lease 时继续 fail closed。矩阵 fresh rerun 66/66 tasks、6m28s 通过；BookKeeper 全量单测、Object-sync 原门禁与 146/146 manifest 联合回归 78/78 tasks、1m40s 通过。KF-TXN-008 五 profile P/C/K 当前源码证据闭合；final aggregate 仍 open
> 2026-07-29 mandatory NTC2 五 profile 增量：product `4676c12` 新增 `f9MandatoryInternalTopicNtc2ProfileMatrixProcessIntegrationTest`，将真实 `__consumer_offsets` NTC2 的 delete/corrupt/fail-closed/exact-repair/re-election 扩展到 Object async 与 BookKeeper WAL-only/async/sync；连同 fresh Object-sync gate 共五 profile、十个场景。WAL-only 使用没有 F4 registration 的 projection-free L0 compaction authority；extended KCP1 decision set 只压缩持久化载荷，保留旧 logical planId/raw bytes，并受 60 KiB stored、1 MiB decoded、4096 sources 硬上限约束。矩阵 64/64 tasks、5m34s，Object-sync 73/73 tasks、1m29s。KF-TXN-016 五 profile P/C/K 当前源码证据闭合；final aggregate 仍 open
> 2026-07-30 M6 final-process aggregate 增量（覆盖上两行的 M6 final-aggregate open wording）：fork `76f62f3b83` makes leader-state publication and `leaderEpochAwareOffsetLookupPending` installation atomic under `Partition`'s state write lock；product `14fb643` source-locks the resulting 53 commits/126 files。Harness commits `3293d76`、`c76a466` and `4a0ec22` close timed readiness Admin clients with `Duration.ZERO`、hold activation listener ports until launch and use the existing 30-second Admin profile for long topic/reassignment calls。The exact fencing、offset、orphan、activation and BookKeeper physical-entry assertions are unchanged。Fresh `phase9M6KafkaProcessCheck --rerun-tasks` passes 94/94 tasks in 34m21s at product `4a0ec22`；F9-M6 process evidence is closed，while M7/`phase9FinalCheck` remains open
> Future：F9 Native Kafka Shared Storage
> 目标日期基线：2026-07-23
> AutoMQ 参考锁：`1c648d84819d5c3fef2af585f02149c397584870`（`3.9.0-SNAPSHOT`）
> Kafka fork development head：`nereusstream/kafka:nereus/future9-native-kafka-storage@76f62f3b83e882105219b6c7687dbde594a8b8a2`（53 commits / 126 files from Apache `427b409cf440f745ad6195673d3342f6bd3974d4`；第 49 个 `bd9963c980` 增加 stock retention oracle，第 50 个 `c4a0a2d1fa` 增加 stock `LogCleaner` compaction oracle，第 51 个 `bf8a2946e5` 刷新隔离 development coordinates，第 52 个 `1e3783458b` 修复 leaderless transaction-marker retry，第 53 个 `76f62f3b83` 在 `Partition.makeLeader` state lock 内原子安装 new-leader offset-lookup fence）；working clone `/Users/liusinan/apps/ideaproject/nereusstream/kafka`；SSH push is configured and the remote head is verified
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
ListOffsets lookup；任一步失败都按 epoch/instance 撤销。fork 已注册完整 100-key
`nereus.kafka.storage.*` `ConfigDef`，构建无副作用
immutable Object/BookKeeper typed snapshot，并在 enabled-only pure validator 中拒绝非 broker、RF/minISR、remote log、stock cleaner、
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
集群持续为空。deterministic 与 real-Oxia reconnect gates 已通过。fork `9773c8f817` 已把 controller seam
接入 stock `ControllerServer`：`ControllerStorageRuntime` 同时是 `MetadataPublisher`，在 publishers 安装前 create/start，
再由 stock `MetadataLoader` 按序发送 controller leadership 与 image callback。artifact-only
`NereusControllerStorageRuntime` 只在本节点是当前 controller 时调度，合并多个 metadata callback，最多保留一个
in-flight attempt；只对 `NereusException.retriable()==true` 按 typed retry interval 重试，失去 leadership 时取消尚未
执行的 retry，non-retriable failure 在 process-local `terminalFailure` 中每个 controller epoch 只交给 fault handler
一次；这不是 durable checkpoint quarantine。`start()` 只创建 minimal shared-Oxia、
partition/activation store 和 binding-aware snapshot/coordinator graph，不等待 first activation，从而允许 combined
controller 先启动、broker 随后注册 capability，再由 retry 完成 PREPARED→ACTIVE。in-flight CAS 不在本切片增加 durable
controller-epoch fencing；它继续依赖 coordinator 的 idempotent CAS/winner recovery，新的 attempt/retry 才受 local-controller
检查约束。Object-WAL runtime 现另外拥有 `KafkaCheckpointFailureMetadataStore` 和
`DurableKafkaCheckpointFailureQuarantine`：recovery/retention 在读取对象前按 exact reference 查询，确认永久失败后必须先
完成 immutable first-failure audit 的 create/reconcile 才能尝试旧 root；raw failure message 不进入 durable bytes，
metadata read/write failure 直接 fail closed。fork `d23dc5c787` 已注册 opt-in
`nereus.storage.version`、仅在 enabled broker/controller 广播 0..1 range、支持 dedicated controller 配置与显式
`kafka-storage.sh format --feature nereus.storage.version=1`，并在 finalized level 1 后才调度 activation；
controller create/config/ISR/reassignment/directory mutation 同时执行 single-copy policy。BookKeeper WAL-only creator
及其首个真实 process profile 已实现；Object async creator 与首个真实 process profile 也已实现；Nereus provider
现已组合 BookKeeper async/sync creator 并通过完整五档 real-service provider matrix；checkpoint time-index candidate、
完整五档 fresh-process profile matrix仍未实现；ACTIVE 稳态三 voter controller kill/reconciliation，以及
all six before-provider/after-provider readiness-create/PREPARED-create/ACTIVE-CAS controller failover gates and actual
Oxia transport reset/same-controller-epoch retry 已通过；the four initial empty-cluster snapshot-proof/capability-aggregation
process cuts also pass。
`NereusUnifiedLog` 已有首条同步
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
runtime drain 同步停止新 admission，并同时等待已接纳 Produce 与 product runtime。此前 HTTPS credential 只有
read/invalid 权限；当前工作 clone 已改用 SSH push，完整 F9 branch 已发布到组织 fork。该 exact head 的 fresh
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
offset span；该改变不绕过 fork 的 stock producer/transaction validation。隔离 Kafka fork commit
`ec7f0db991` 现新增 `NereusProducerStateManager`、`NereusTransactionIndex` 和 stock
`ProducerStateEntry.fromBatchMetadata` restore seam：完整 seven-section NKC1 image 先恢复 producer/transaction、
leader-epoch、virtual-byte/timestamp state，再从 checkpoint offset replay exact COMMITTED tail；五批 duplicate
window 与 marker-updated `lastTimestamp` 独立保留。`NereusUnifiedLog` 继续先走 stock producer/transaction
validation/verification guard，stable append 后把 stock HW 推到 durable end，再发布 exact HW/LSO；abort index 和
READ_COMMITTED 元数据按实际返回页上界裁剪。后续隔离 commit `032974067c` 又用 stock
`ReplicaManager.appendRecordsOnStorageExecutor` 路径证明 transaction verification guard 和 TV2 marker version
在异步 handoff 后仍原样进入 `UnifiedLog`，并同时锁定 `__consumer_offsets`/`__transaction_state` election 只有在
exact recovered storage ready callback 后才发生。codec/replay/factory/shell/stock request/lifecycle 的 13 个聚合
焦点测试已通过；这些 commit 现均包含在远端 F9 branch。真实单节点进程门现已覆盖 committed transaction 与
consumer-group offset 的 fresh-JVM 内部主题恢复，并覆盖 stable open transaction 后强制进程退出、fresh-JVM
ABORT resolution 与 read-committed filtering；真实 checkpoint publication、multi-broker live takeover、
BookKeeper/profile matrix、completed coordinator migration、Object-WAL ongoing transaction bidirectional
COMMIT/ABORT migration 已有真实进程证据，mandatory NTC2 的 deterministic admission gate、五 profile
delete/corrupt + exact repair/re-election gate，以及五 profile transaction-resolution process cuts 也已闭合；
checkpoint cut 与完整 M4
aggregate 仍未完成，因此不能声明 M4 完成。
Section 3 现由 Kafka-artifact-neutral `KafkaLeaderEpochState` 与 `KafkaLeaderEpochStateCodecV1` 实现：
leader epoch/start offset 双严格递增，只有首条可低于 logStart 作为 carried-forward range，所有 start 均不超过
stable end，末条可等于 stable end 表达当前空 epoch；required/version/flags、unsigned count、truncation、
duplicate、ordering、bounds 与 EOF 均 fail closed。冻结 payload SHA-256 和 200 轮固定种子状态覆盖
decode/re-encode byte exact。Section 5/6 现由 `KafkaDerivedIndexState` 与
`KafkaDerivedIndexStateCodecV1` 一起实现，使 time index 只能引用同一 checkpoint 中存在的 logical-byte
segment，并对 segment base、entry offset、timestamp、cumulative logical bytes 与 outer logStart/stableEnd
做跨 section 校验；当前空 segment 只能位于 stable end 且 logical bytes 必须为 0。两个 required payload
均为 strict big-endian V1，frozen combined digest、损坏输入和另 200 轮固定种子状态覆盖 byte-exact round
trip。Section 4 现由 `KafkaVirtualSegmentState`/`KafkaVirtualSegmentStateCodecV1` 实现 virtual segment 与
bounded config history：offset/byte ranges 必须 dense，最后且仅最后一段 ACTIVE，roll sequence/time/config digest
严格可验证，active roll jitter、index-full reason 和所有 effective roll/retention/compaction config 均使用 closed
wire fields。`KafkaCanonicalCheckpointStateCodecV1` 最终按 type 1–7 编码全部 required sections，并交叉校验
checkpointOffset=stableEnd、virtual/logical segment 一一对应、logical bytes 相等，以及 time/logical sample 不跨
virtual segment。Section 4 与 full composition 分别有 frozen digest、corruption/invariant tests；codec-only slice
已完整。`KafkaCanonicalCheckpointPublicationFactory` 已把 partition-lock-frozen canonical image、exact source
head/session 和 ACTIVE binding 组装为 header + seven-section write/publication request，并在 object I/O 前拒绝
in-flight append、state-map/end、leader authority 和 bounds mismatch；production runtime 的 staging/writer
ownership、periodic trigger 和 fork checkpoint capture/export handoff 仍未接入；fork import/replay 已由
`ec7f0db991` 接入 recovery path。
M5 的首个 product slice 已新增 `KafkaRetentionPlanner`、`KafkaRetentionCheckpointGate/Services`、
`KafkaRetentionCoordinator`、`KafkaDeleteRecordsCoordinator`、`KafkaTrimBarrier` 和
`KafkaRetentionDurableTrimListener`。Planner 直接消费 section-4 virtual
segment/config image，按 stock Kafka strict `retention.ms` 与 logical segment-size excess 计算连续 closed-segment
前缀，取 time/size 更远边界并受 HW 限制，active segment 永不被选择。Barrier 只接受同一 frozen snapshot 的 exact
plan，要求 checkpoint offset/object SHA/root 足以覆盖目标，重载并复核 ACTIVE binding、leader/broker authority、
config digest 和重新计算的候选，然后才调用 Nereus trim；成功或异常响应都重载 stable head，只有 durable trim 已达到
目标才通知本地 logStart/binding 更新。当前 deterministic tests 覆盖 strict predicate、size/time union、active/HW
边界、checkpoint failure/unrooted reference、closed corruption fallback/transient pause、config race、正常 trim、
response-loss 收敛、并发 trigger coalescing/cancellation isolation，以及 binding-before-local CAS/response loss/leader
fencing。Checkpoint services 已组合 pinned exact-reference recovery、canonical publication 和 root reload；durable
listener 已组合 binding observed-logStart CAS；canonical seven-section local-file object-store publish/root-reload/verify
round trip 已通过。Product-side DeleteRecords 现保留 stock-normalized exact logical offset（包括 mid-batch），对
already-deleted request 无 I/O 返回 durable low watermark，并通过共享 checkpoint/trim barrier 覆盖 policy/HW/config
race 和 normalized-HW target。Concrete partition-lock capture/local-log updater、periodic scheduling、Kafka-fork
DeleteRecords invocation/fetch wake-up 以及 compaction production registration/concrete authority capture 均已实现；
完整 stock differential oracle 仍待实现，因此 `phase9M5RetentionCheck` 仍只是 partial gate。

2026-07-29 的 release-process checkpoint/trim slice 已闭合此前 open 的 native DeleteRecords + forced-restart
边界。`f9CheckpointTrimRecoveryProcessIntegrationTest` 使用 Kafka 4.3 自带
`kafka-delete-records.sh`，在 `OBJECT_WAL_SYNC_OBJECT`/RF1 partition 写入 6 个约 600 KiB batch，使
`segment.bytes=1 MiB` 形成多个 virtual segment；它要求 DeleteRecords to offset `3` 先产生 rooted NKC1，
再把 stream trim 与 Kafka low watermark 推进到 `3`。随后直接杀死 release JVM，而不是走 graceful shutdown；
fresh JVM 必须恢复 `logStart=3/end=6`、从 offset `3` byte-exact Fetch、保留 checkpoint 引用，并在 offset `6`
继续 Produce，最终 ListOffsets 返回 `3/7`。

该 gate 固定的代码级恢复合同如下：

1. `KafkaRecoveryStateCodec#hydrateCheckpoint` 接收完整 `KafkaCheckpointHeader`；checkpoint sections 必须按
   header 捕获时的 `[logStartOffset, stableEndOffset]` 解码，不能用重启时已经推进的 stream trim 改写历史边界。
2. fork `NereusKafkaRecoveredState` 只允许 `header.logStartOffset <= current durable logStart` 且
   `header.checkpointOffset <= current durable end`；恢复 producer/transaction/segment/index 状态并 replay
   committed tail 后，`NereusCanonicalLogState.advanceLogStart(currentTrim)` 删除 trim 前 virtual segments、
   exact batch positions 与过期 config history。
3. `KafkaPartitionStorage#publishDurableLogStart(long)` 是 product-to-fork 的唯一 durable trim publication seam；
   `NereusUnifiedLog.publishDurableLogStart` 必须在同一 storage/leader epoch 仍 writable 时更新 product snapshot、
   canonical state、ephemeral `NereusLocalLog` segment shells 和 stock `UnifiedLog.logStartOffset`，任一步失败都
   fail closed。
4. recovery/checkpoint source anchor 在相同 `commitVersion` 下校验 immutable `lastCommitId + committedEndOffset +
   cumulativeSize`；`durableHeadSha256` 会因 trim、session 或 metadata-only 更新而变化，不能被误当成 append
   content identity。Binding 的 `observedStableEndOffset` 仍是 advisory：允许落后、禁止领先，当前 stream head
   才是 restart evidence 的权威 end/trim。
5. `NereusTopicDeltaLifecycle` 发现 broker epoch 为 `-1` 时使用 scheduler 异步轮询至 open deadline；metadata
   publisher 线程不能阻塞，也不能取消已准备的 leader lookup。runtime 的 read-pin/protection TTL 必须满足
   `pendingProtectionDuration - maximumClockSkew >= operationTtl`，否则启动前拒绝配置。
6. 大 batch 跨页时，physical reader、read dispatcher 与 compacted Fetch reader 都把
   `READ_LIMIT_TOO_SMALL` 解释为“已有返回页到达边界”而不是数据损坏；只有空 accumulator 下仍无法容纳 first
   entry 才返回原错误。

可执行证据目录为
`nereus-kafka-adapter/build/f9-kafka-checkpoint-trim-evidence/`。Fresh gate 通过后又分别通过
`f9RetentionTest`、checkpoint publication recovery、partition opener/storage、compacted Fetch/runtime-config
focused tests、Kafka fork recovery/canonical-state tests、既有
`f9M6KafkaProcessIntegrationTest` 和 `f9MultiBrokerTakeoverProcessIntegrationTest`；因此该 slice 已进入
`phase9M6KafkaProcessCheck`。The later dedicated boundary gate upgrades KF-RET-006 to
`PASSED_CURRENT_SOURCE`，but neither gate marks the whole F9-M5 final。

同日新增的 `f9TrimResponseLossProcessIntegrationTest` 固定 KF-RET-005 的一个真实
`OBJECT_WAL_SYNC_OBJECT` R/P/C 切片。Test-only `TrimCompletionLossAgent` 只在显式 arm 后拦截一次
`DefaultStreamStorage.trim(StreamId,long,TrimOptions)`：真实 provider future 继续执行；成功后写入 `applied`
marker，却把交给 `KafkaTrimBarrier` 的 future 永久保持 incomplete。旧进程必须同时满足 rooted NKC1、
stream `trim/end=3/6`、binding `observedLogStart=0`、DeleteRecords CLI 仍存活，然后被强制终止。Fresh JVM
恢复 `earliest/latest=3/6` 和 offset `3` Fetch；相同 target `3` 的原生 DeleteRecords 重试返回 low watermark
`3`，且重试前后 stream-head `commitVersion/lastCommitId/metadataVersion/durableHeadSha256`、binding
metadata version/observed offsets/checkpoint references 及 NKC1 object-key set 全部不变，证明没有第二次 trim
CAS、binding rewrite 或 checkpoint publish。最终 append/fetch offset `6` 后为 `3/7`。证据位于
`nereus-kafka-adapter/build/f9-kafka-trim-response-loss-evidence/`，任务已进入
`phase9M6KafkaProcessCheck`。

`f9TrimProfileMatrixProcessIntegrationTest` 复用同一 helper/agent/断言，在
`OBJECT_WAL_ASYNC_OBJECT`、`BOOKKEEPER_WAL_ONLY`、`BOOKKEEPER_WAL_ASYNC_OBJECT` 和
`BOOKKEEPER_WAL_SYNC_OBJECT` 逐档执行相同切点；加上前置 sync Object gate，五档均要求
provider-applied/caller-unobserved、强杀、fresh restart、same-target no-op/no-repeat 和 offset `6` continuation。
三档 BookKeeper 共用真实 stock ZooKeeper + two Bookies，但各有独立 namespace reservation、activation、
bucket/config/cache/log。Fresh matrix 以 75/75 actionable tasks、3m23s 通过，证据目录为
`nereus-kafka-adapter/build/f9-kafka-trim-profile-matrix-evidence/`，并进入
`phase9M6KafkaProcessCheck` 与 `phase9M6KafkaBookKeeperProcessCheck`。这闭合 KF-RET-010 的五-profile
response-loss/checkpoint-barrier process slice；the next `f9DeleteRecordsBoundaryProcessIntegrationTest` and
`phase9M5KafkaRetentionOracleCheck` close batch-middle/end/HW and the stock retention oracle，while broader chaos and
full compression/header、OPEN-boundary stock differential 与 process/restart compaction aggregate remain open。
Ranged compaction 的首个 codec slice 另新增 materialization-side immutable decode/rewrite records 和 adapter
`KafkaCompactionPlanner`/`KafkaTopicCompactionCodecV1`：planner 从 mandatory end 起只选 LSO/min-lag 允许的连续 closed
virtual segments，并冻结到 stable-end decision horizon；codec 严格解码一个 exact magic-v2 ranged batch，为每条
logical record 生成 KCK2 key/null/control 身份与 source SHA/base/index，并把选中的普通、transactional 或
commit/abort record 重写为原 offset 上的单记录 valid batch；compression、timestamp、headers、producer/sequence 与
control meaning 经 decode round trip 验证。`KafkaCompactionStrategyV1` 又以 pass-1 collector facts 确定
latest/superseded key、stock-compatible null-key drop、committed/aborted/open transaction，以及 full-scan-proven tombstone/control
delete horizon（含 first-pass assignment 和 later `now == horizon`）。Bounded
`KafkaCompactionPassOneCollector`/`KafkaCompactionTwoPassExecutor` 已扫描 full horizon、reprove output prefix，并将
survivors 重写/映射为 ordered、non-empty、fetchable NTC2 rows；`ExactSourceSetVerifier` 同时拒绝 target/source-set
drift。`KafkaCompactionWriteRequestFactory` 再把 verified range/source-set SHA/accounting 与固定
strategy/key/rewrite/message-format identity 绑定为 strict NTC2 writer request，并拒绝 task/result coverage mismatch。
`ExactSourceSetCodecV1`/`KafkaCompactionPlanCodecV1` 又将 exact target、output task、decision horizon、LSO/HW、
config/coverage、transaction marker facts 与兼容性元组冻结为 byte-stable EXS1/KCP1 restart image。
`KafkaCompactionPlanRecordMapper`/`KafkaCompactionPlanMetadataStore` 已把 bounded KCP1 bytes/SHA 作为 partition child
做 immutable create、restart read 与 exact-version delete。
`KafkaCompactionPlanCoordinator` 再以 plan-first 顺序处理非原子的 KCP1/task roots：task admission 前会再次验证
authority 并按 materialization task ID 精确回读 KCP1，restart 也从该 ID 恢复并校验完整 task。
`DefaultCommittedSourceSetResolver`/`KafkaCompactionSourceResolver` 又从当前 COMMITTED generation index 选择 bounded、
gap-free exact decision path，逐 generation 回读并验证 stream/registration retained authority，再生成 exact output
prefix/task 及其 mutation guard。
`KafkaCompactionBatchSource` 又从 recovered KCP1 打开相互独立、backpressured、exact-identity-verified 的 decision/output
cold streams，且最后一个 batch 耗尽 demand 时仍能正常完成。
`KafkaCompactionWinnerIndex` 已把 production pass-one key map 替换为 shared staging budget 上的 owner-only KCSR V1
sorted runs：每个 run 严格 KCK2-key ordered、whole-file SHA-256 verified，最多 fan-in 16 多路归并，最终只保留
output coverage winner bitmap；成功、损坏、decode failure 和 cancellation 都删除 run 并归还 permit。restart 不复用
scratch bytes，而是从 recovered KCP1 exact sources 确定性重算；product test 已覆盖 spill/no-spill、decision-tail、
restart recomputation、same-length corruption 和 cancel cleanup。
`KafkaCompactionStreamingExecutor` 又以两个独立 exact streams 完成 decision/output passes，将 survivor 写入
whole-file-SHA-verified KCRS V1，再由 `KafkaCompactionParquetPublisher` demand-stream 为 staged NTC2；产物 SHA 已与
reference executor 对齐并通过 strict reader。`KafkaCompactionTerminalRetirer` 以 exact terminal task root first、
KCP1 second 的顺序收敛双根删除和 response loss。通用 F4 policy/output/index 现已增加独立 Kafka NTC2 policy
identity、task/output format agreement 和 zero-survivor generation accounting，且不放宽 COMMITTED dense invariant。
`KafkaCompactionPublicationCoordinator` 又按 guarded upload/HEAD/full verify/OUTPUT_READY/Generation COMMITTED/
binding coverage CAS 的顺序闭合 write-side linearization，以 canonical gap-free generation-set digest 支持首次、
扩展与替换，并覆盖 PUT/CAS response loss、heartbeat race 和 generation-commit 后 basis drift。它现在还可从
durable `OUTPUT_READY/PUBLISHING/PUBLISHED` task/output 重新进入幂等 Generation publication 与 coverage
activation，不再依赖 crash 前的本地 staging file；恢复路径会重新验证 task/output、binding window、activation
basis 与外部 authority，并把已生效但响应丢失的目标 coverage 识别为成功。
`KafkaCompactedFetchPlanner`/`KafkaCompactedFetchReader` 又把每次 Fetch 绑定到最新 ACTIVE root：mandatory prefix
只发 `TOPIC_COMPACTED` 请求，稀疏/空结果按 coverage 跨洞并在 exact end 切到剩余预算的 COMMITTED tail；强制对象
失败不做 cross-view retry。`KafkaActivatedGenerationSetResolver` 对 binding digest 做 bounded、unique、gap-free
generation path discovery，并把 exact range/generation/publication/index key/version/SHA 作为
`GenerationReadConstraint` 下推到 core resolver；因此未激活的新格式/损坏 generation 不会遮挡已激活集合。
Object-WAL production opener/runtime 已注入这条 generation-aware read path，旧构造路径保持 committed-only。
`KafkaCompactionPartitionPass` 现在为单一 `KafkaPartitionId` 组合完整 durable workflow：先有界扫描 KCP1 并优先
恢复 task-rooted work，再按 `PLANNED/RETRY_WAIT/CLAIMED/OUTPUT_READY/PUBLISHING/PUBLISHED/terminal` 精确分流；
fresh path 才会运行 planner、authoritative source resolver 和 plan-first converge。执行路径使用 secure claim、
周期 heartbeat 与 publication 前 final renewal，打开两条 exact replay、生成 NTC2、发布 coverage，并按 task-first/
KCP1-second 退役；进程重启时从 KCP1 previous coverage 重建 exact activation mode/generation set。类型化 source
failure、retry deadline、过期 claim、CAS response loss 和 concurrent caller coalescing 都不依赖 exception message
或本地 scratch。`KafkaCompactionPartitionPassTest` 已用真实 Parquet writer/reader、local-file object store、in-memory
Generation/binding metadata 和 durable task/KCP1 跑通 planned→claimed→NTC2→PUBLISHED→coverage→dual-root-retired。
`KafkaCompactionScheduler` 现以 borrowed scheduler/executor 提供 immediate startup + fixed-delay passes、最多一个
active 和一个合并 pending pass、trigger-priority aggregate、调用方取消隔离与 close deadline cancellation；
`KafkaCompactionPlanOrphanScanner` 通过 partition-scoped Oxia continuation 做 page/total 双重有界扫描，只在
grace、task 两次 absent、stable no-admission authority guard 和 exact KCP1 reload 都成立后删除 plan，并以
exact absence 收敛 delete response loss。当前形成 `phase9M5CompactionCoreCheck`；把该 pass 注册到 runtime owned
partition 枚举的 product bridge 已实现。`KafkaCompactionProductionRuntimeFactory` 现把 direct-stream
COMMITTED source resolution、按 recovered-plan `streamId` 延迟创建的 exact source reader、shared staging/spill、
streaming KCRS→NTC2 publisher、full object verifier、activation/partition-authority-fenced Generation committer、
coverage publication、KCP1/task coordinator/retirer 和每个 owned partition 的 recoverable pass 组合成一个
`KafkaCompactionRuntime`。`NereusKafkaObjectWalRuntimeFactory` 在 activated context 提供 compaction 配置和
owned-partition source 时，创建并拥有 `StagingFileManager`，把 runtime 后台服务 late-bind 到同一个
`KafkaPartitionStorageManager`，并在 ACTIVE binding 返回前安装 projection-free materialization registration；
后台 start 阻塞 readiness，drain 先等待 compaction 再关闭 partition manager/provider。Product commit
`e18bf36` 与 fork commit `58342d9dca` 已补齐 compaction runtime config、fork-owned partition registration、
current-leader partition-lock/KRaft/local-log canonical capture，以及 exact decision-horizon stock
`CleanedTransactionMetadata` marker pre-scan。Capture 在扫描前后比较 producer/transaction image，并拒绝
source/leader/binding/预算漂移。当前仍需 provider fresh-process restart/takeover gate，以及完整 stock cleaner
differential oracle。
为接入 stock transaction state，product partition boundary 已把 durable end 与 derived visibility 拆开：
stable append 先推进 exact end/commit version 并保留旧 HW/LSO；fork 必须在 stock producer/transaction 更新成功后
调用 `publishDerivedOffsets(exactEnd, HW, LSO)`，随后才发布 `STABLE_APPEND` 并 dispatch 同 partition 下一次
append。该边界已覆盖 exact-end mismatch、offset 越界、initialized offset 回退、queued append、resign 和 renewal
failure；fork `ec7f0db991` 已完成该调用顺序并用 open/abort transaction、LSO 和 bounded
READ_COMMITTED/aborted metadata 测试锁定，真实 provider/process gate 仍待完成。
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
wiring 已落地并通过 stock KRaft restart；provider-backed runtime composition 与 per-broker log-factory selection 已落地。
fork `faaffc8a75` 又以 stock `BrokerStorageManagedLog`/`PartitionLeaderAuthority` 接口隔离所有 maintenance hook，
保证未携带 Nereus artifact 的 stock main/test compilation 不链接产品类型；`3bd92c7244` 把显式 production broker factory
从 `bin/nereus-kafka-server-start.sh` 注入共享 `Kafka.run`/`KafkaRaftServer` 生命周期；`9773c8f817` 再把 production
controller factory 沿同一路径传入 `ControllerServer`，以 stock-owned runtime/publisher seam 驱动 activation；同一
head 还完成 durable feature 注册、enabled-only advertisement/format、dedicated-controller validation 和
single-copy controller policy。`ecde6964c5` 又在 authoritative cache root 生成并校验 KRaft V1
`meta.properties`/non-reserved directory ID。Product ACTIVE path 现会在 broker set/epoch 变化时先加载新 capability，
再以 CAS 提升 readiness epoch；不会重新执行只适用于首次 activation 的 empty-cluster proof。真实 combined-node
provider-backed process gate 已覆盖同节点 fresh-JVM 冷重启；独立 process gates 也已覆盖 Object/BookKeeper live
takeover、in-flight append cuts、ACTIVE-state controller kill、six-way readiness-create/PREPARED-create/ACTIVE-CAS
store-publication takeover、four-way initial snapshot-proof/capability-aggregation takeover and actual Oxia transport
reset/same-epoch retry。当前仍未闭合的是 checkpoint kill cuts 和 chaos/profile 扩展。
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
revalidation 失败会撤销该 epoch 的 provisional state。该 M3 head 对 idempotent/transaction/control batch 和
NKC1 派生 section 使用 `UNSUPPORTED_FORMAT` fail closed；后续 `ec7f0db991` 已替换为 M4 stock-state
hydration/replay。fork `cfcdd55fbc` 新增 per-broker `UnifiedLogFactory` seam、
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
first-activation 的 deterministic scheduling 与 durable feature gate 已由 `d23dc5c787` 组装；Object-WAL checkpoint
durable quarantine 已在 product runtime 组装。Fork `ebf1d76163` 当前 head 将配置面扩展为 100 keys，并为
`BOOKKEEPER_WAL_ONLY` 构造完整 `NereusKafkaBookKeeperConfig`、BookKeeper WAL 映射、exact readiness/password
reference 与 fork-owned BookKeeper client；product graph 仍只借用 client，outer wrapper 按 product-first/client-second
顺序关闭。Mapper 将 Object sync/async 显式映射到同一已安装 Object provider 集合，并在 BookKeeper
runtime 存在时暴露完整五档 capability set、保留 exact configured profile 作为 default，同时把 NCP2 staging
固定到 authoritative cache root 下的 `materialization-staging`。新增六个 BookKeeper ledger-GC key 被映射为
`BookKeeperLedgerGcConfiguration`；三个 materialization retirement lifecycle key 被映射为
`MaterializationConfig`，两组字段均参与 compatibility digest。默认 `enabled=false/dryRun=true` 不创建 scanner。
该 head 还把 Nereus-enabled Kafka 的 logging runtime 收敛为唯一
`slf4j-api:2.0.17 + log4j-slf4j2-impl:2.25.3`：`core` configuration 排除 Kafka 原 1.x binding、
OAuth test fixture 的 Logback provider 和 BookKeeper 传递 provider；`releaseTarGz` 在启用 development artifacts
时过滤 Kafka 原 `slf4j-api:1.7.36`。真实 tar 内容检查不得出现第二个 SLF4J API/provider。
只有 enabled、non-dry-run 且已组装 Kafka materialization runtime 时，provider-neutral
`BookKeeperPrimaryWalRuntime.createRetentionService` 才创建 scanner/service。Kafka adapter 同时构造
`KafkaBookKeeperStreamCoverageProofProducer` 与一次性
`KafkaBookKeeperDeletionActivationService`：先扫描全部 64 个 binding-registry shard、按 hint 重读
authoritative binding root，再逐个核对 exact L0 stream/profile/state/trim/stable-end；随后扫描全部 64 个 F4
materialization-registry shard，要求 async/sync profile 存在 projection-free direct authority，而
`BOOKKEEPER_WAL_ONLY` 不得伪造 object registration。`NBKKAFKASTREAM1` digest 绑定 Kafka cluster、WAL config、
ledger namespace、broker readiness、binding/root/L0 authority 与相关 F4 registration。通用
`BookKeeperDeletionActivationCoordinator` 已下沉到 `nereus-bookkeeper`，在同一 readiness 下依次生成 scope、
root、Kafka stream proof，重验 namespace/readiness 后只用一次 activation CAS 同时安装三份 digest 与
`ledgerDeletionEnabled=true`。后台服务严格按 materialization → deletion activation → retention 启动，反向关闭；
任一分片、注册、L0、readiness 或 CAS 漂移都会使 broker startup fail closed。确定性测试已覆盖全分片、
WAL-only 无物化注册、async 缺失注册、L0 profile 漂移和 composite lifecycle。
`f9BookKeeperLedgerDeletionProviderIntegrationTest` 进一步以 real Oxia、two bookies 和 local-file ObjectStore
执行 enabled/non-dry-run graph：强制 rollover 后等待 NCP2 COMMITTED，证明 terminal task 的
`MATERIALIZATION_SOURCE` 保护按 audit grace 释放、三类 WAL 引用 RETIRED、ledger root 经
`SEALED -> MARKED -> DELETING -> DELETED`，再用 BookKeeper client 证明原 ledger 无法打开，并由同一 Kafka
partition 从 NCP2 byte-exact 读回原始 batch。该 gate 同时回归
`GenerationIndexRecord.payloadFormat` 必须与 `MaterializationOutput.payloadFormat().name()` 比较，而不能与
versioned logical format `KAFKA_RECORD_BATCH_V1` 比较。该 gate 现在还在 runtime start 后对 exact borrowed
BookKeeper operations armed 一次 applied-delete response loss，断言注入 ledger ID 就是 retired Kafka WAL
ledger，并通过 provider metadata absence 收敛到 first/second absence 与 `DELETED`；默认生产 context 仍创建
标准 client adapter。`f9BookKeeperWalAsyncObjectProcessIntegrationTest`
进一步在真实 release tarball 中以 `max.entries.per.ledger=1` 强制 rollover，等待同一 ledger root
`DELETED` 并由独立 BookKeeper client 证明 `NoSuchLedger`；首 JVM 正常停止后，fresh JVM 从 offset 0
经 NCP2 fallback 恢复并继续 append/fetch/ListOffsets。Provider-level delete-response-loss 已覆盖；
release-process response-loss restart 与 multi-broker takeover 仍待，因此 KF-RET-009 仍不升级为 final-gated。
`phase9M6KafkaProcessCheck` 现在从 exact local artifacts 构建真实
Kafka release tarball，使用 `kafka-storage.sh --feature nereus.storage.version=1` format，并以显式 launcher 对
四分片 Oxia 与 pinned LocalStack S3 完成 broker/controller registration、activation、Admin create、acks=all
Produce、consumer Fetch、earliest/latest ListOffsets、S3 object assertion 和 SIGTERM shutdown。首 JVM 还使用
`transactional.id` 提交 data offset 1/COMMIT marker offset 2，并通过真实 group subscribe/rebalance 提交
group offset 2；随后同一 config/KRaft directories 启动 fresh JVM，要求 higher broker epoch readiness CAS refresh，
并发恢复用户分区、`__consumer_offsets` 和 `__transaction_state`，加载原 group committed offset 2，以同一
transactional ID 提交 data offset 3/marker offset 4，再由同一 group 从可见 offset 3 恢复并提交 offset 4，最终验证
earliest=0/latest=5 和第二次正常 shutdown。第三 JVM 随后稳定提交 open-transaction data offset 5 后被强制终止；
第四 JVM 以同一 transactional ID 恢复并先生成 ABORT marker 6，再提交 data/COMMIT marker 7/8，
`read_committed` 从 offset 5 只返回 7，原 group 提交 offset 8，最终 latest=9。恢复页读取遇到 retriable
`NereusException`（包括共享读取预算暂满）
时，在同一冻结 head 与 wall deadline 内以 10 ms 起、250 ms 封顶的指数退避重试；成功页到达前不 replay、不
publish，也不触发 coordinator-ready。M6 process/feature task-name detection 强制根
构建使用并重发 `0.1.0-f9-dev`，避免 release tarball 复用旧 development artifact。
`f9BookKeeperWalOnlyProcessIntegrationTest` 另以 stock ZooKeeper long-hierarchical metadata + two bookies 启动真实
release launcher，完成 offset 0 首写/读/偏移校验、正常停机、fresh-JVM 恢复和 offset 1 继续追加；Nereus
namespace/activation/readiness authority 仍在 Oxia，Kafka 运行时不依赖 Pulsar metadata driver。
`f9ObjectWalAsyncObjectProcessIntegrationTest` 另以真实 Oxia + LocalStack S3 启动 release launcher：首 JVM 以
`OBJECT_WAL_ASYNC_OBJECT` 写读 offset 0 并验证 earliest=0/latest=1，正常停机后 fresh JVM 恢复 offset 0、
继续写读 offset 1 并验证 earliest=0/latest=2；其 profile-default 与 capability set 均由 typed mapper 冻结。
2026-07-28 的 NCP2 物化 checkpoint 新增独立 `nereus-kafka-committed-v2` policy，并让 worker、planner、
registry scanner、Generation committer 与 required-generation coordinator 在显式 `DIRECT_STREAM` authority
下执行 ranged Kafka batch 的写入、校验和发布；现有 NCP1/NTC1 构造路径继续强制 projection authority。
`KafkaObjectMaterializationRuntime` 现由 activated provider 每进程只组合一次，拥有 staging/worker、借用共享
Oxia/ObjectStore/scheduler/read graph，并按 runtime drain 顺序幂等关闭。Object/BookKeeper async append 在
ACTIVE `GENERATION_PUBLISH` proof、lag gate 和 second revalidation 后才入场；BookKeeper sync append 则通过
`RequiredObjectGenerationCompletion` 在返回前等待 exact NCP2 COMMITTED/readable。真实 Oxia + filesystem
ObjectStore + two-bookie provider gates 已对 Object async、BookKeeper async/sync 验证 exact batch bytes、
NCP2 publication 和正常 Kafka read。`f9BookKeeperWalAsyncObjectProcessIntegrationTest` 以四个 batch
跨越默认最小 source-range threshold，等待真实 S3 NCP2 object 后正常停机，并在 fresh JVM 恢复 offset 0、
继续追加；`f9BookKeeperWalSyncObjectProcessIntegrationTest` 以单 batch 验证 append 只有在 required NCP2
COMMITTED/readable 后才完成，再执行同样的 fresh-JVM 恢复。三类 BookKeeper process tasks 已纳入
`phase9M6KafkaBookKeeperProcessCheck`。
2026-07-28 的 fresh partial aggregate 还覆盖了一个此前只在连续物化时暴露的 source-selection 缺口：
generation 0 的 BookKeeper 前缀完成 NCP2 物化并被物理删除后，下一次扩大 source range 必须优先复用
higher-generation NCP2 前缀，再拼接仍可读的 BookKeeper tail；不能因 raw Kafka batch 使用
`KAFKA_RECORD_BATCH`、NCP2 logical format 使用 `KAFKA_RECORD_BATCH_V1` 而错误地回退到已删除 ledger。
`DefaultMaterializationPlanner` 现在只对这一个 byte-equivalent Kafka 映射做兼容归一化，其他 payload/logical
format 组合仍严格拒绝；确定性 planner 回归和 real Oxia + two-bookie physical-deletion provider gate 均通过。
根聚合命令
`phase9M3KafkaForkCheck phase9M5CompactionCoreCheck phase9M6ActivationMetadataCheck phase9M6KafkaFeatureCheck
phase9M6CheckpointQuarantineCheck --rerun-tasks`
在 fork `ebf1d76163` 与当前 product source 上以 109/109 outer tasks 成功结束；嵌套 Kafka stock/artifact-enabled
构建分别通过 92/92 与 95/95 actionable tasks，feature/control focused builds 也通过 86/86、42/42、74/74
与 20/20 actionable tasks。该结果是当前已实现 slice 的 fresh aggregate，不是 F9 final-release aggregate。
同日新增的 `f9MultiBrokerTakeoverProviderIntegrationTest` 以两个独立 production Object-WAL runtime ownership
graphs 共享 real Oxia/Object root：A 在 30 秒 session 未到期时仍保持运行，B 以更高 leader epoch 原子替换 durable
head token、重放 A 的 exact committed batch 并从 end=1 继续追加；A 的旧 token 写入被拒绝后立即进入
`WRITE_FENCED_RECOVERY_REQUIRED`。`DefaultKafkaPartitionStorage` 现在明确区分“普通
known-not-committed 可安全重试”和“即使 known-not-committed 也必须 fence 的 authority/session/head conflict”。
该 task 已进入 `phase9M3ProviderCheck`，但仍只是 R-tier 两 runtime 证据。
Fresh `phase9M3ProviderCheck --rerun-tasks` 已以 64/64 actionable tasks 通过，组合了 146/146 scenario manifest、
29-source Nereus lock、Kafka baseline lock、M1/M2/M3 deterministic predecessors、Object/BookKeeper provider gates 与
新增 live takeover gate。
`f9MultiBrokerTakeoverProcessIntegrationTest` 现在进一步使用 fork `df238bb387` 的真实 release tarball 启动两个
独立 Kafka JVM：node 1 是 combined controller/broker，node 2 是 broker-only；两者共享同一个 KRaft cluster ID、
controller quorum、四分片 Oxia authority、LocalStack S3 Object-WAL root 和 Nereus cluster identity，同时使用各自
独立的 metadata/log/cache 目录。测试先把 RF1 partition 显式分配给 broker 1，Produce/Fetch offset 0；再让 broker 2
注册，调用 stock Admin `alterPartitionReassignments` 请求 singleton `[2]`，并要求 KRaft image 原子变成
`leader=2, replicas=[2], ISR=[2]`、没有 ongoing reassignment，且 broker 1 仍存活。随后 broker 2 必须从共享存储
恢复 offset 0、提交 offset 1，并由真实 cluster byte-exact Fetch 两条 batch。

该进程门发现并固定了两个代码级合同。第一，Nereus feature level 1 的 RF1 reassignment 不能走 stock
`[old,new]` transitional RF2/ISR catch-up；`ReplicationControlManager.changeNereusPartitionReassignment` 只接受
stable RF1 当前状态和一个 active singleton target，并生成一条同时更新 replicas、ISR、leader、清空
adding/removing replicas 的 `PartitionChangeRecord`。第二，`TopicsDelta.localChanges(brokerId).deletes()` 同时表示
“本 broker 不再承载仍存在的 partition”和“topic/partition 真删除”；`NereusTopicDeltaLifecycle` 必须查看新 image：
同 topic name、同 topic ID、同 partition 仍存在时只以新 leader epoch `resign`，只有 identity 已消失或 topic ID
已改变时才 durable `delete` shared binding。否则旧 broker 会在合法 reassignment 时删除全局 binding，使新 leader
恢复失败。

Fresh `:nereus-kafka-adapter:f9MultiBrokerTakeoverProcessIntegrationTest --rerun-tasks` 已以 73/73 actionable
tasks、1m04s 通过，并进入 `phase9M6KafkaProcessCheck`。这关闭了 Object-WAL 两个 release Kafka process 的 KRaft
singleton reassignment、旧 owner resignation、committed replay 和 takeover 后续写 P-tier slice；它没有模拟
reassignment 瞬间已经 dispatch 的旧 append，也没有单独覆盖 coordinator migration、多个 controller 或
kill/response-loss chaos。下述更新已另行闭合 completed coordinator migration 和 native
checkpoint/virtual-segment trim/restart subset；其余边界仍保持 open。

`:nereus-kafka-adapter:f9CoordinatorMigrationProcessIntegrationTest --rerun-tasks` 在 product `7c25d2e` 与同一
fork `1cbe8b65a8` 上复用真实 two-release-process Object-WAL/KRaft topology。Node 1 先持有显式 `[1]` user
partition，并完成普通 offset 0、transactional data/COMMIT marker offsets 1/2、真实 group rebalance 和 committed
offset 2；这会创建并写入 RF1 `__consumer_offsets-0` 与 `__transaction_state-0`。Node 2 注册后，测试先要求三
partition 的 `leader/replicas/ISR=[1]`，再用一个 stock Admin request 将三者全部原子重分配为 `[2]`。完成条件是：

1. user、group、transaction 三个 partition 都达到 `leader=2, replicas=[2], ISR=[2]`；
2. `listPartitionReassignments` 对三者为空，且旧 broker 1 始终存活；
3. broker 2 的 storage-ready callback 先完成共享存储恢复，再允许两个 coordinator election；
4. group coordinator 返回旧 committed offset 2；
5. transaction coordinator 以同一个 transactional ID 初始化下一 producer epoch，并在 user log 的 offsets 3/4
   提交 data/COMMIT marker；
6. READ_COMMITTED 能读取迁移前 offset 1 与迁移后 offset 3；原 group 从第一个可见 offset 3 恢复并提交 offset 4；
7. 最终 user partition earliest/latest 为 `0/5`，两个 JVM 都保持存活且 ObjectStore 非空。

Fresh task 通过 73/73 actionable tasks、1m07s，JUnit process case 为 31.829s；相关
`f9M6KafkaProcessIntegrationTest + f9MultiBrokerTakeoverProcessIntegrationTest` 回归又以 74/74 tasks、1m50s
通过。该 task 已进入 `phase9M6KafkaProcessCheck`，为 KF-TXN-011/012/013 提供 completed-state P/K takeover
slice；它本身不声称 ongoing/aborted transaction coordinator takeover、mandatory internal-topic NTC2
no-resurrection 或 M4 final aggregate 已完成。后续 product `b6b02f4` + fork `89b66ab03b` 已闭合 deterministic
NTC2 admission/no-resurrection gate，product `0ae8ca9` + fork `768924da60` 又闭合真实 Object-WAL
delete/corrupt + exact repair/re-election process cut；non-Object profiles 与 final aggregate 仍未完成。

`:nereus-kafka-adapter:f9OngoingTransactionMigrationProcessIntegrationTest --rerun-tasks` 在 product `efe782d` 与同一
fork head 上复用两个始终存活的 release JVM，但把 coordinator cut 推进到 OPEN state。该测试使用独立 user
partition 和 `__transaction_state-0`，并执行两个相反方向的迁移：

1. broker 1 写入 transactional data offset 0，确认 READ_COMMITTED end/LSO 仍为 0；
2. 一个 Admin map 将 user 与 transaction-state partitions 一起从 `[1]` 迁到 `[2]`，两个 partition 均须达到
   `leader=2, replicas=[2], ISR=[2]` 且 reassignment 为空；
3. 原 producer 通过迁移后的 coordinator COMMIT，marker 落在 offset 1；同一 transactional ID 重新初始化后提交
   data/marker offsets 2/3，READ_COMMITTED end 收敛到 4；
4. broker 2 再写一个 OPEN transaction data offset 4，确认 LSO 保持 4，然后将两个 partition 一起迁回 `[1]`；
5. 原 producer 跨反向迁移 ABORT；测试等待异步 marker offset 5 使 READ_COMMITTED end 收敛到 6，而不是假设
   `abortTransaction()` 与 marker append 同步；
6. 同一 aborted transactional ID 重新初始化并提交 data/marker offsets 6/7；从 offset 4 的 READ_COMMITTED
   Fetch 必须直接返回 offset 6，最终 earliest/latest 与 READ_COMMITTED end 为 `0/8`。

每次 handoff 都同时验证两个 JVM 存活、exact singleton ownership 和 ObjectStore 非空。Task 独占
`build/f9-kafka-ongoing-transaction-evidence`，已进入 `phase9M6KafkaProcessCheck`；fresh execution 为 64/64
actionable tasks、47s，JUnit case 32.753s；completed coordinator + ordinary takeover 回归为 74/74 tasks、1m30s。
该 slice 为 KF-TXN-007/012/014 提供 Object-WAL P/K 证据，但不替代 marker/EndTxn response-loss、进程 kill、
BookKeeper/profile 或 M4 final aggregate；真实 Object-WAL NTC2 fault/repair 由独立 gate 覆盖。

BookKeeper post-handoff 由独立
`:nereus-kafka-adapter:f9BookKeeperProfileTakeoverProcessIntegrationTest --rerun-tasks` 覆盖：同一个 stock
ZooKeeper/two-bookie fixture 上，WAL-only、async-object、sync-object 各启动两个 release Kafka JVM，提交
`[0,1)`、原子重分配 `[1] -> [2]`、要求 exact leader/replicas/ISR、旧进程仍存活，再由 node 2 恢复并提交
`[1,2)`。WAL-only bucket 保持为空，async/sync 均出现真实 NCP2 object。Fresh gate 以 64/64 actionable
tasks、2m17s 通过并进入两个 M6 process aggregate；下面的独立 gate 补充共享 appender 的 C cut。

BookKeeper provider-applied C-tier 由
`:nereus-kafka-adapter:f9BookKeeperInFlightTakeoverProcessIntegrationTest --rerun-tasks` 闭合。该 gate 的
`f9BookKeeperFaultAgent` 是独立 test-only source set；其 fat JAR 只通过 broker-1 的 `KAFKA_OPTS=-javaagent:...`
加载，不进入产品依赖或 release tar。Byte Buddy advice 只包裹
`DefaultBookKeeperClientOperations.write(WriteAdvHandle,long,ByteBuf,BookKeeperOperationDeadline)` 的返回 future：
先等待真实 `WriteAdvHandle.writeAsync` 完成并写入 applied marker，再延迟向
`BookKeeperPrimaryWalAppender` 交付完成结果。测试必须同时看到 `jcmd` 中
`NereusUnifiedLog.appendStable -> CompletableFuture.get`、Oxia reservation lifecycle `WRITING` 和独立
BookKeeper client 对同一 `(ledgerId, entryId)` 的 `readUnconfirmed`，因此不是 sleep-based 或 pre-provider cut。

随后 broker 1 被 `SIGSTOP`，live KRaft 原子完成 `[1] -> [2]`；broker 2 的首次 offset-1 append 懒触发
`BookKeeperLedgerRecovery`，旧 reservation 必须精确进入 `ABANDONED`，旧 root 必须进入 `SEALED`，新 append
返回 offset 1 并可 Fetch。release marker 与 `SIGCONT` 让旧 Bookie-acked future 继续；它必须因 stale metadata
authority 失败，旧 JVM 保持存活，durable earliest/latest 仍为 `0/2`，WAL-only bucket 始终为空。Fresh gate 以
66/66 actionable tasks、1m30s 通过且 configuration cache 可复用，并进入
`phase9M6KafkaProcessCheck` 与 `phase9M6KafkaBookKeeperProcessCheck`。该切点位于三种 BookKeeper profile 共享的
`BookKeeperPrimaryWalAppender`、早于 `DURABLE` 和任何 materialization 分支；结合上面的 three-profile P matrix，
它补齐三 profile 的共同 C 边界。`KF-APP-014` manifest status 仍按全局里程碑策略保留 `PLANNED`，不把这一个
新增 gate 等同于 F9 final aggregate。

ACTIVE 稳态 controller failover 由
`:nereus-kafka-adapter:f9MultiControllerFailoverProcessIntegrationTest --rerun-tasks` 覆盖。该 gate 从同一真实
release tarball 启动 node 1/2/3 三个 `broker,controller` combined JVM；每个节点拥有独立 broker listener、
controller listener、KRaft metadata、Kafka log shell 和 Nereus cache 目录，但共享同一个 Kafka cluster ID、
静态 voter set `1,2,3`、四分片 Oxia、LocalStack S3 Object-WAL authority 和 Nereus cluster identity。测试先等待：

1. Admin broker set 精确等于 `[1,2,3]`；
2. `describeMetadataQuorum` 的 voter set 精确等于 `[1,2,3]`，leader ID 有效、leader epoch/high watermark 非负；
3. 当前 leader 日志出现
   `Nereus Kafka storage activation reconciled by controller <leaderId> at epoch <leaderEpoch>`；
4. 独立 Oxia inspector 读到 immutable activation lifecycle `ACTIVE`，readiness broker IDs 精确为 `[1,2,3]`、
   readiness epoch 不低于 activation epoch、未过期且 capability digest 相等。

为了把 controller failure 与 data replica loss 分离，RF1 user partition 被显式放到“不是当前 controller
leader”的 combined node。测试提交/读取 offset 0 后对当前 controller JVM 执行 `destroyForcibly()`，要求进程非零
退出且不得出现正常 `shut down completed`。两个 survivor 必须选出不同 controller ID、更高 controller epoch，
同时 voter set 仍保持 `[1,2,3]`；新的 leader 也必须为自己的 exact epoch 输出上面的 reconciliation-success 日志。
第二次 Oxia 直读要求 activation record byte-for-byte 不变、readiness epoch 单调不减。随后 survivor bootstrap
必须提交 offset 1、byte-exact 读取 offset 0/1、报告 earliest/latest `0/2`，且 S3 bucket 至少存在一个对象。

`NereusControllerStorageRuntime` 为此新增 process-local `reconciledControllerEpoch`：一次 activation attempt 成功时，
只有该本地 controller epoch 的第一次成功会记录 INFO；同 epoch 的 metadata coalescing/retry success 不重复打点，
新的 controller epoch 会重新记录。这个字段不是 durable fencing token，也不改变 PREPARED/ACTIVE CAS 合同；它只让
进程 gate 能区分“Raft 已选主”和“Nereus activation coordinator 已在新 leader 上完成 reconcile”。断言完成后，
剩余 combined nodes 由 harness 强制终止，因为在三 voter 中先杀一名 leader 后再逐个 graceful shutdown 会在失去
quorum 时卡住 controlled shutdown；正常 SIGTERM 路径继续由 cold-restart gates 独立覆盖。

该 fresh direct gate 已以 64/64 actionable tasks、36s 通过，并进入 `phase9M6KafkaProcessCheck`。它为
`KF-OPS-005` 提供 ACTIVE 稳态 kill/failover 的 P/C 部分证据；它本身不覆盖 publication in-flight cut，下面的
独立 gate 负责 readiness/PREPARED/ACTIVE store-publication before-provider/after-provider 边界。

Activation store publication-boundary cuts 由
`:nereus-kafka-adapter:f9ActivationCutFailoverProcessIntegrationTest --rerun-tasks` 覆盖。该 gate 从同一 release
tarball 启动 node 1/2/3 三个 dedicated controller JVM 和 node 4 一个 dedicated broker JVM；四个进程共享
KRaft cluster ID、static voter set `1,2,3`、四分片 Oxia、LocalStack S3 bucket 与 Nereus cluster identity，
但各自拥有独立 metadata/log/cache 目录。将 broker 与 controller 角色分离后，六次 fault 的 readiness broker set
都稳定为 `[4]`，controller kill 不改变参与 activation proof 的 broker capability tuple。

测试专用 `nereus-f9-activation-fault-agent.jar` 只注入三个 controller 的 `KAFKA_OPTS`，不会进入 production
artifact/config。它为 `createReadiness`、`createActivation` 和 `compareAndSetActivation` 各运行两个 phase：

1. `before-provider` 在实际 Oxia 调用前写 `blocked`，跳过 store method，并返回永不完成的 future；
2. `after-provider` 等实际 Oxia future 成功并写 `applied` 后，仍向 coordinator 返回永不完成的 future。

每个 controller 有独立的 arm/captured/blocked/applied/installed marker；harness 先通过
`bootstrap.controllers` 直接读取 exact voter set、leader ID/epoch，只 arm 当前 leader，再从 Oxia 分别冻结
activation/readiness both absent、activation absent + readiness、`PREPARED`、或 `ACTIVE`。旧 leader 在看到 reconciliation-success 日志前被
`destroyForcibly()`；survivors 必须选出不同 controller ID 和更高 epoch，并为该 exact epoch 输出
`Nereus Kafka storage activation reconciled ...`。Empty-control-plane 场景必须重新完成 proof 并创建 readiness；
readiness-only 场景必须复用原 readiness epoch 和 `kraftMetadataOffset` 创建 PREPARED；durable PREPARED 必须把原
cluster/profile/capability/broker-set digest 与 source metadata offset 原样推进到 ACTIVE；durable ACTIVE 要求
activation record byte-for-byte 不变。最后 node 4 才完成 admission，创建 RF1 topic，并验证 offset 0 的
Produce/Fetch、earliest/latest `0/1` 与正数 Object count。

该 fresh `--rerun-tasks` gate 的最新回归以 75/75 actionable tasks、2m40s 通过，已进入
`phase9M6KafkaProcessCheck`；失败时会保留四份
config/format/server log 和全部 agent marker。它闭合 readiness-create、PREPARED-create 与 ACTIVE-CAS 各自的
before-provider / after-provider P/C 进程矩阵。首个 before-PREPARED 执行还暴露并锁定 production fix：replacement controller
必须用 durable readiness 的 metadata offset 构造 PREPARED，不能使用自身更高的当前 snapshot offset。Agent
不是产品 fault API；下面的独立 gate 负责 initial snapshot/capability proof cuts，而 M7
aggregate/priority/chaos 仍需独立进程证据。因此 `KF-OPS-005` 继续保持 `PLANNED`。

Initial activation proof-boundary cuts 由
`:nereus-kafka-adapter:f9ActivationProofCutFailoverProcessIntegrationTest --rerun-tasks` 覆盖。它复用三个 dedicated
controller + broker `[4]` 的真实 release topology，但 agent 的目标是
`KafkaStorageFirstActivationCoordinator`，不是 Oxia store：

1. `currentSnapshot/before-provider` 在 controller 调用快照证明前阻塞；
2. `currentSnapshot/after-provider` 要求 fork-owned KRaft/local-log snapshot 与 binding-aware 64-shard registry
   existence scan 全部成功，再扣留结果；
3. `loadCapabilities/before-provider` 在读取当前 broker 集合的 capability 前阻塞；
4. `loadCapabilities/after-provider` 要求 broker `[4]` 的 capability 存在，且 cluster identity、broker epoch、
   expiry、five-profile compatibility digest 与 provider-scope digest 全部聚合校验成功，再扣留结果。

四个切点都必须在 readiness/activation 尚未写入时由 direct Oxia reader 冻结为空。after-provider agent 不会让
异常完成的启动期请求消耗一次性 marker；它只原样透传失败，并原子捕获第一条成功完成的 proof/aggregation。
Harness 随后强杀 exact armed controller leader，要求另一个 controller ID 以更高 epoch 重新完成证明、发布
ACTIVE/readiness `[4]`，再通过 native RF1 offset-0 Produce/Fetch/ListOffsets `0/1` 和正数 Object count。

Fresh execution 以 66/66 actionable tasks、1m49s 通过，失败证据归档到
`build/f9-kafka-activation-proof-cut-evidence`，任务已进入 `phase9M6KafkaProcessCheck`。这关闭了
KF-OPS-005 的 initial proof/aggregation P/C 子集；该 row 仍等待 M7 final aggregate/chaos 后才能升级状态。

Actual Oxia transport failure 由
`:nereus-kafka-adapter:f9ActivationTransportRecoveryProcessIntegrationTest --rerun-tasks` 独立覆盖。该 gate 使用一个
dedicated controller（node 1）和一个 dedicated broker（node 2），将两个 release JVM 的 Oxia endpoint 都指向
Toxiproxy。Controller 先建立 exact single-voter KRaft leader/epoch；随后 harness 安装 downstream `reset_peer`
toxic，再启动 broker。四秒故障窗口内两个进程必须存活，且绕过 proxy 的 direct Oxia client 必须观察到
readiness/activation 都不存在。

移除 toxic 后必须由同一个 controller ID/epoch 输出 exact activation reconciliation marker，而不是依靠重新选主
清除 process-local terminal state。最终 Oxia ACTIVE/readiness broker set 必须为 `[2]`，node 2 才能创建 RF1 topic，
完成 offset 0 Produce/Fetch、earliest/latest `0/1` 与正数 Object count；controller log 不得出现 durable-failure
fault message。Fresh execution passes 73/73 actionable tasks in 1m10s，JUnit scenario time 36.512s。

该门首先用
`KafkaStorageActivationMetadataStoreContractTest.normalizesRawTransportFailuresAsRetriableMetadataUnavailable` 复现
production 缺陷：`OxiaJavaKafkaStorageActivationMetadataStore` 曾把任意 runtime transport exception 原样返回，
fork controller runtime 会将其视为 non-retriable 并把当前 epoch 锁为 terminal。现在 read/create/CAS 边界会把
provider synchronous throw 统一转换成 failed future，保留 typed Nereus/condition/invariant failure，并把未知
provider/transport failure 包装为 `METADATA_UNAVAILABLE(retriable=true)`；applied response-loss 的 exact-winner
reload 语义保持不变。完整 store contract 与真实 process gate 均通过。

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
- payload bytes are exact owned snapshots before async handoff；worker alone may mutate the budgeted copy for stock offset
  assignment，then storage receives an exact read-only duplicate；
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
