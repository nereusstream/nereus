---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 5: wire, recovery, and slice contracts

Date: 2026-08-09

ADRs 0028 through 0032 resolved the previous frontier. This record preserves the next independent questions and
recommendations presented to the user. None of the recommendations below is an accepted runtime contract until the
user explicitly confirms it.

## Source facts used for recommendations

- The aggregate is now one incarnation-scoped physical record. Existing Oxia envelopes already support one
  schema/min-reader axis and strict checksum/trailing-byte validation. Kafka's current `nereus.storage.version` has only
  V1 levels 0/1 and advertises a continuous range; its generic feature-update path permits downgrade and cannot prove
  that a cluster was freshly formatted for the V2 clean break.
- Stock Pulsar's offload index has no V2 attempt/data digest/root-self-integrity contract and does not bound several
  decoded counts/lengths. Current ManagedLedger source deletion does not revalidate Object bytes before setting its
  BookKeeper-deleted flag, and current source selection does not retry a failed Object open/range read from BookKeeper.
- A pre-open WalRun root plus content-addressed leaves discovers provider-present extents, but cannot reconstruct an
  absent in-flight group's final compressed/encrypted bytes after process loss. A deterministic nonce alone cannot
  reconstruct its frame set, ordering, DEK/header, or compression output.
- One Object group may contain multiple bindings. Consequently, a WalRun root cannot carry one topic's singular Owner
  Epoch or Storage Epoch without abandoning cross-binding batching; those exact values already belong to frame/binding
  context.
- An unbounded run will eventually exceed any fixed recovery budget. AutoMQ bounds some buffers but LISTs whole WAL
  prefixes and has no total run/page/request/byte/time recovery envelope. Checkpoints do not by themselves bound or
  authorize an ACKed open tail.
- Existing V1 `NRS1/WAL_OBJECT_V1` derives Kafka coverage from record count, lacks commit-set membership and per-frame
  checksum fields, and reads a larger slice before clipping an entry. It cannot be relabeled as the accepted V2 frame
  and commit-set format.
- The deployment registry now atomically owns all virtual-ledger assignments. Protocol Cell identity is independent
  from broker/session and Cell Provider Scope configuration; native Pulsar compares ledger IDs numerically, and the
  bounded registry must retain retired assignments when proving never-reuse.

These are pinned-source capabilities and constraints, not V2 implementation evidence.

## Current frontier

❓ **Q1** - **Topic Binding Aggregate logical schema v1**：现在 incarnation/key 已冻结，aggregate 是只用一个
whole-record schema version，还是让 binding 与 initial epoch 独立演进并产生版本组合？哪些状态属于 immutable
payload？

➡️ 推荐唯一兼容性轴 `aggregateSchemaVersion=1`。`TopicBindingAggregateV1` 包含完整 binding：ID、Cell、protocol、
typed incarnation、Position Domain、payload mapping、Native Write Authority；以及 ordinal 0 initial epoch：ID、反向
binding ID、profile、typed origin、absent sealed end、WAL/payload/checksum/encryption family。所有 kind/version 是
closed wire discriminator；不适用值显式 `NONE`，未知/非法组合 fail closed。`ACTIVE` 是完整记录成功发布的可见性
结论，不是字段；v1 不存 `CREATING`、delete/lifecycle、owner、timestamp、attempt、controller offset、backend
version 或 attributes map。Oxia envelope 使用 `schemaVersion=1,minReaderSchemaVersion=1`；KRaft record wire v0
映射同一个 logical v1，共享 validator/golden vectors但无需物理 bytes 相同。权衡是 whole-record evolution 较重，
换来没有 binding/epoch reader-version 组合爆炸，也保持 exact whole-record equality。

❓ **Q2** - **Kafka V2 feature level 与激活边界**：继续复用 `nereus.storage.version` 还是新建 feature？V2
是否允许在已有 level 0/1 集群在线升级或从 level 2 降级？

➡️ 推荐复用 feature name，但 V2 节点只 advertise/accept `[2,2]`：0 是 stock/disabled，1 永久代表 V1 durable
state 并由 V2 拒绝，2 才启用 aggregate。level 2 只能在全新 KRaft storage format/bootstrap 时显式写入；0→2、
1→2 和 2→1/0 的 safe/unsafe runtime update 全部禁止。level 2 下每个成功 native `CreateTopics` 项必须在同一
atomic result 中含 aggregate；`validateOnly` 仍零记录，`TOPIC_ALREADY_EXISTS` 等原生语义不变。Feature 只证明
节点能 decode/replay schema v1；动态 provider/profile admission 仍逐 topic 校验。权衡是复用现有运维入口但打破
Kafka 当前连续版本范围和 generic downgrade 假设；回退必须重建集群，符合 V2 clean break。

❓ **Q3** - **Pulsar sealed-ledger root v1 wire 与硬上限**：root 使用 stock index/protobuf 延伸，还是单独
canonical binary？在信任 variable length/count 前，parser 的固定攻击面是多少？

➡️ 推荐独立 big-endian fixed-width `NPO1`，不复用 stock index。32-byte header 固定 magic、format/min-reader、
flags、header length、exactly four sections、root length、SHA-256/v1 self-digest family；每节 16-byte typed/versioned/
required/length header，顺序唯一为 `ATTEMPT`、`SEALED_LEDGER`、`DATA_EXTENT`、`SPARSE_INDEX`，末尾 32-byte
SHA-256 覆盖此前全部 canonical bytes。字符串为 strict UTF-8 length-prefix，map 按 unsigned UTF-8 key bytes
排序；拒绝 duplicate、unknown flags/enums、trailing bytes 和 overflow。硬上限：root 8 MiB、ledger metadata
1 MiB、data descriptor 256 KiB、data key 1,024 bytes、sparse rows 65,536、custom metadata 1,024 项/总 1 MiB、
ensemble segments 65,536、每 segment members 1,024、Bookie ID 4 KiB、generic string 64 KiB、entry count
`1..2^31-1`。先 HEAD 限长，再 full GET/self-digest，最后才信任 sections/index。权衡是极端 ledger 必须更早
rollover或增大 block，换来严格有界的 parser/memory surface。

❓ **Q4** - **BookKeeper source deletion 前是否重验 Object**：native `complete=true` 后，是否继续只信一次
offload completion，还是在写 `bookkeeperDeleted=true` 前再验证 pair？

➡️ 推荐在 ManagedLedger 删除切点增加窄
`revalidateOffloadedForSourceDeletion(ledgerId, UUID, persistedDriverMetadata)`。它验证 exact attempt/root、完整
root parse/self-digest、data immutable version/length/SHA proof或 bounded GET、CLOSED/LAC/count/length，以及生产
reader 的首尾与 sparse boundaries；然后在 native metadata CAS 下重检同 UUID、`complete && !bookkeeperDeleted`。
Object I/O 不持 metadata mutex。timeout/throttle/missing/mismatch 保持 flag false、保留 BK、按 ledger backoff；
永久损坏 quarantine/alert。权衡是后台删源多一次 bounded Object 验证且仍有跨 provider TOCTOU 窗口，换来比
stock completion-only 删除更强的最后一道保源门；provider immutable ACL/version retention 仍是必要前提。

❓ **Q5** - **Pulsar native Object/BK read fallback 语义**：当前 pinned source 选中一个 source 后 open/read
失败会直接上抛。0.2 要冻结怎样的单次 fallback，才能提高可用性又不掩盖损坏？

➡️ 推荐 `complete && !bookkeeperDeleted` 时：Object-first 遇 missing/timeout/unavailable/short-read/digest/format
错误，整次 inclusive range 最多一次回退 BK；BK-first 仅 `BKNoSuchLedgerExists` 回退 Object，普通 BK transient
仍走 native retry 后上抛。`bookkeeperDeleted=true` 永远 Object-only，即使物理 BK 残留也禁止偷读。invalid range、
cancel、closed/unsupported 不 fallback；两源失败返回 primary error，secondary 为 suppressed cause且不循环。
一次 range 必须全量来自一个 source，释放所有 partial entries 后从 secondary 整段重读。Object corruption 即使
fallback 成功仍 degraded/quarantine并 veto deletion。权衡是需要下一层 ManagedLedger-owned BK read pin、
pin-drain/delete 和 composite handle 设计；但先冻结用户可见错误与 whole-range 原子语义，不能把现状误写成已支持。

❓ **Q6** - **multi-binding WalRun 的 epoch/fence 放置**：WalRun Root 是携带一个 Topic Owner/Storage Epoch，
还是保留跨 binding batching 并把 exact epoch authority 放在 object-local binding/frame context？

➡️ 推荐保留 cost-first 跨 binding group。WalRun Root 只绑定 Protocol Cell、provider/shard、run/session generation、
format families、prefix 与 recovery contract；不携带单一 topic Owner Epoch/Storage Epoch。每个
`BindingContext`/frame 携带并验证 exact binding incarnation、Storage Epoch、Owner Epoch，group directory 汇总但不
取代它们。权衡是 recovery 必须先读有界 directory 才能按 epoch 过滤，换来继续摊薄 PUT；若把 epoch 放 root，
就必须按 binding/epoch 分 run，成本目标会改变。

❓ **Q7** - **crash 后 absent in-flight group 是否需要 exact ciphertext journal**：root+LIST 能发现已存在
object，但 404 时进程已丢失最终 body。0.2 是增加 PUT 前本地 fsync journal，还是明确放弃跨进程同 body retry？

➡️ 推荐 0.2 不把 broker-local 磁盘变成第二个 durability prerequisite。若强一致 LIST/HEAD 证明旧 conditional
PUT 不存在且该 group 从未 ACK，新 owner 永久 burn 旧 run/sequence、回退到已证明 Durable Frontier，并让协议
idempotent retry 在 fresh run/group 重建；不得声称能重放原 key/body。若 object 存在则按 key/header/idempotency
恢复原结果。权衡是 absent response-loss attempt 在进程崩溃后只能返回失败/等待 client retry，换来无每组本地
fsync、无 host-affine ciphertext journal。若未来要求跨进程原 attempt retry，必须 journal exact post-encryption
ciphertext、长度、key 和完成状态；deterministic nonce 单独不够。

❓ **Q8** - **bounded WalRun lifecycle 与 root lineage**：有 recovery budget 但没有 run 上限，最终必然稳定
超预算。0.2 如何保证每个 ACKed run 始终可恢复？

➡️ 推荐每 run 强制 `maxExtentCount`、`maxCanonicalBodyBytes`、`maxRunAge`，并限制同时可恢复的 predecessor
runs；run ID 永不复用。达到任一阈值前停止旧 run admission、drain/reconcile、seal，再发布 successor root，且
run-root enumeration 本身也有 bounded authority。数值由 M3/M7 provider/RTO evidence 后置。权衡是更多 root/
rollover 控制面工作和小尾 group，换来不会把无限 prefix 伪装成“预算化恢复”。Seal、checkpoint、retirement root
的具体权限与 handoff 顺序依赖这个 lifecycle，下一轮再冻结。

❓ **Q9** - **recovery envelope 是否反向约束 ACK/admission**：budget 只是 takeover 超时参数，还是 normal
append 也必须保证未来 worst-case recovery 仍在 envelope 内？

➡️ 推荐后者。总 envelope 分别累计 live roots/runs、LIST pages/keys/key bytes、HEAD/GET requests、full-GET/
canonical bytes、decoded frames/commit sets、memory、concurrency、retry attempts 与 wall time；hint/checkpoint
失败转 fallback 不能重置计数。只有“所有仍需恢复的 run/open tail 仍在最坏情况 envelope 内”时才允许新 ACK。
预测将越界时先 rollover/throttle/stop admission；实际耗尽则不得跳过对象、推进 frontier 或执行 GC。权衡是
provider 变慢时会提前形成 availability backpressure，换来 recovery budget 是 correctness invariant 而非最终
必然 fail-closed 的装饰配置。

❓ **Q10** - **WalRun Root 自身的发现权威**：owner-open/restart 如何找到当前 root？再 LIST 一个 root prefix，
还是用低频 control metadata 指针？

➡️ 推荐每个 shard 只有一个 CAS 发布的
`CurrentWalRunPointer { walRunRootKey, walRunRootSha256, shardRunEpoch }`。owner-open/rollover/handoff 写一次；root
可以直接作为 immutable metadata value，或作为 provider object 由 pointer 绑定 exact SHA。successor root 记录
predecessor key/SHA，恢复从 current pointer 有界回溯到已发布 retirement frontier；group header 也绑定 root SHA。
响应不确定时 reread pointer，只接受 exact candidate 或已提交 winner。权衡是 topic-open/rollover 增加低频 metadata
CAS 并依赖其可用性，换来不再引入第二套 root-prefix LIST、一眼确定 current lineage/trim，且 admitted normal append
仍保持零 metadata I/O。exact pointer/root wire 与 lineage bounds 依赖 Q6/Q8，后置。

❓ **Q11** - **`.nwg` 内的 AppendUnit Directory authority 与 co-location**：frame/commit-set index 位于
对象本体、sidecar、manifest还是 footer？一个 Kafka commit set 能否跨两个 ObjectExtent？

➡️ 推荐新的 major physical format `NWG1`，每个 `.nwg` 在 fixed header 后包含唯一权威、严格有界且可独立
range-read/校验/解密的 `BindingContextTable + AppendUnitDirectory`；不能只存在于 sidecar/manifest/footer。
一个 Kafka commit set 必须完整、连续地落在一个 ObjectExtent，放不下就先 seal group，单 set 超硬限在位置
分配前拒绝。每个 stored frame block 独立 compression/AEAD/CRC decode，v1 禁止跨 frame compression或整组
AEAD stream。Header/directory CRC 保护 bounds/membership，每 frame CRC 保护 decoded native payload，Object SHA
保护最终 body；不再增加 set CRC。权衡是 per-frame descriptor/AEAD tag 和较差跨帧压缩率，换来单 batch/entry
range read、无 sidecar 原子性和不必整对象解压；exact field IDs/limits 下一层再冻结。

❓ **Q12** - **virtual-ledger slice 绑定的 durable owner identity**：slice 跟 broker、cluster alias、provider
scope，还是稳定 Protocol Cell？

➡️ 推荐 owner tuple 为
`{deploymentId, reservationDomainId, protocol=PULSAR, immutable PulsarProtocolCellId}`，另有 immutable
`sliceAssignmentId` 且 bounds 属于其 identity。broker/process/session、display alias、provider endpoint/credential、
Cell Provider Scope 都只是运行/准入属性；restart/scale/provider rotation 继续用旧 slice。退役后即使复用显示名，
也必须新 Cell ID、新 slice。权衡是必须正式定义 durable `PulsarProtocolCellId`，换来正常运维变化不消耗有限
slice，也不把 provider configuration 误当 protocol authority。

❓ **Q13** - **slice lifecycle 与 retirement**：撤销是否立即等于退役？容量耗尽是 lifecycle state 吗？

➡️ 推荐单向 `ACTIVE -> RETIRING -> RETIRED`。仅 ACTIVE 可分配；RETIRING 立即停新 ID 但保留所有既有
ledger/MessageId，不声称链与资源已清空；RETIRED 是永久 registry tombstone，record/bounds 永不删除复用。
`EXHAUSTED` 是由 counter/bounds 导出的容量状态，不进入 lifecycle；broker/session drain 不触发 retirement。
权衡是多一步管理状态且 assignment table 永久增长，换来停止分配与完成下游退役不会被混为一谈；
RETIRING→RETIRED 的 exact proof 依赖 allocator/chain，后置。

❓ **Q14** - **slice geometry 与 registry lifetime capacity**：每 Cell 可变区间，还是 deployment-wide固定规格？

➡️ 推荐 0.2 每 Cell 恰好一个 immutable、等长、相对保留域 base `2^62` 按 `2^k` 对齐的 contiguous slice。
保留域 cardinality 是 `2^62-1`，因此 `maxSlicesNumeric=floor((2^62-1)/2^k)`，顶部不足一个完整 slice 的
`2^k-1` IDs 永不分配。另设 hard `maxRegistryBytes` 与 lifetime `maxAssignmentsEver`，后者包含所有 retired Cell
incarnation；最终 cap 是 numeric 和 encoded limits 较小者。exact `k` 依赖支持期 create/rollover/recovery/gap
budget，后置。权衡是固定规格浪费数值空间、不能适配极端异构 Cell，但大幅简化 alignment、overlap、audit 和
bounded registry proof。是否允许 resize/第二 slice 必须在这个几何确认后下一轮决定。

## Deferred descendants

- aggregate canonical publication equality, delete/recreate retirement, Kafka image ownership, snapshot/replay order,
  and physical record API key depend on Q1/Q2;
- root runtime data/block bounds, source-read pin/drain/composite handle, `bookkeeperDeleted` intent-versus-fact state,
  restart delete reconciliation, and physical-delete retry depend on Q3–Q5;
- exact WalRun/root/header fields, pointer/lineage encoding, seal/checkpoint/source-retirement authority, handoff
  transition, GC order, and crash vectors depend on Q6–Q10; exact checkpoint cadence and numeric budgets require
  provider/RTO evidence;
- exact `NWG1` header/directory field IDs, limits, range-read assembly, and compaction vectors depend on Q11;
- slice resize/second-slice policy depends on Q14; exact `k`, retirement proof, epoch layers, allocator response loss,
  ledger head/node publication, rollover/takeover, cursors, trim, compaction, replication, and transactions remain later
  Pulsar descendants;
- `V2-OPEN-OBJ-01`, `V2-OPEN-BK-02`, and `V2-OPEN-BENCH-01` remain executable evidence gates rather than prose
  questions.

## Awaiting explicit confirmation

No recommendation in this round has been promoted into an ADR or normative contract. The user may confirm all fourteen,
confirm a subset by question number, or revise any recommendation. Confirmed answers will be synchronized immediately;
unconfirmed alternatives remain only in this session record and the open-question log.
