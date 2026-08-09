---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 4: incarnation, discovery, and format roots

Date: 2026-08-09

ADRs 0023 through 0027 resolved the previous frontier. This record preserves the next independent questions and
recommendations presented to the user. The user subsequently confirmed all five recommendations. The accepted
contracts are ADRs 0028 through 0032; this session record is not runtime evidence.

## Source facts used for recommendations

- Kafka fork `76f62f3b83e882105219b6c7687dbde594a8b8a2` uses a native UUID topic ID as the controller's topic
  incarnation and delete/recreate fence. The Pulsar fork
  `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` has no equivalent native topic UUID; its Nereus storage-class selector
  already carries a monotonically increasing binding generation and durable deleted state.
- The pinned Pulsar `LedgerOffloader` contract exposes `(ledgerId, attempt UUID, persisted driver metadata)` to read and
  delete. Stock jcloud offload publishes one data object before one index object, but its index omits a data SHA-256,
  exact attempt binding, self-integrity, bounded-length validation, and explicit contiguous-coverage proof.
- AutoMQ research lock `2296e0c9e636dc46f6e369bad5a1815bbaf89f1f` confirms the useful shape of grouped Object
  WAL plus background materialization. Its recovery uses prefix LIST and positional keys, while its WAL metadata does
  not persist the version-bound expected SHA-256 or the bounded recovery contract required by ADRs 0018 and 0025.
- Current Nereus V1 Object WAL keys use time/writer/run/process-local sequence and publish metadata after PUT; they are
  not content-addressed V2 identity. Existing staging can seal exact bytes and calculate length/SHA-256, but it is not a
  cross-process recovery journal.
- Kafka validates and assigns offsets per complete record batch, while a partition append may contain more than one
  batch. Pulsar assigns one `(ledgerId, entryId)` to one exact ManagedLedger entry; client batching, compression, and
  encryption remain inside that entry. Neither a network request nor a transaction is a protocol frame boundary.
- The pinned MetadataStore/Oxia surface can atomically CAS one key but not independently allocated cell-slice keys as
  one transaction. Consequently, multiple per-cell authority records cannot by themselves prevent two concurrent
  administrators from assigning overlapping virtual-ledger ranges.

These are pinned-source capabilities and constraints, not V2 implementation evidence.

## Current frontier

❓ **Q1** - **Topic Binding Aggregate 的 incarnation、authority key 与确定性 ID**：一个 immutable aggregate
究竟以 topic name、随机 Nereus ID，还是协议原生 incarnation 为 ABA fence？Kafka 与 Pulsar 没有相同的原生
identity，丢响应后的 exact compare 又要求候选值可重复生成。

➡️ 推荐冻结 discriminated `TopicIncarnationIdentity`：Kafka 使用 `{topicId UUID, canonicalTopicName}`，其中
`topicId` 是 authority、name 只交叉校验；Pulsar 使用 `{canonical persistenceName, canonicalTopicName,
bindingGeneration > 0}`，只有 native selector 的 `DELETED(g) -> next generation` 才允许同名重建。Oxia aggregate
key 必须包含 protocol kind 与 incarnation：Kafka 叶键用 canonical topic UUID，Pulsar 叶键用 persistence-name 的
domain-separated digest 加 fixed-width generation；value 重复完整 identity，key/value 不一致就 fail closed。
`bindingId` 与 initial `storageEpochId` 分别由 cell + framed incarnation、再由 binding + ordinal 0 做
domain-separated SHA-256 确定性派生，不包含随机 attempt、时间、controller offset 或 backend version。权衡是
Pulsar 必须保留 generation fence 与旧 incarnation 的不可复活证据，换来跨重启/丢响应稳定收敛以及彻底阻断
delete/recreate ABA；aggregate v1 字段、feature level、retirement/GC 和 replay vectors 下一层再冻结。

❓ **Q2** - **sealed Pulsar ledger 的 deterministic keys 与 root v1**：ADR 0024 已选一 data + 一 root，
但 native driver metadata、root 的最低字段、读验证与删除顺序若不冻结，仍无法证明它真的是 ledger-equivalent
attempt，而不是“有两个对象就算完成”。

➡️ 推荐两个 conditional-create key 固定为 cell-provider-scope 下的
`pulsar-offload/v1/ledger-<ledgerId>/attempt-<canonical-lowercase-uuid>/{data|root}`；ledger ID 用无前导零十进制，
native driver metadata 持久化 exact provider location/prefix 与 `keyDerivationVersion=1`，禁止重启后套当前配置。
root v1 是有界 canonical binary，至少包含：format/required flags、完整 attempt binding、sanitized sealed
`LedgerMetadata`、LAC/entry count/logical length、data key + body length + SHA-256/v1 + outer codec/encryption、
`{firstEntryId, entryCount, blockOffset, blockLength}` sparse rows，以及独立的 root self-digest。索引必须证明
`0..LAC` 连续且所有 byte ranges 有界、不重叠。publication 固定 data verify -> root exact verify -> 走真实
`readOffloaded` 打开并核对元数据与边界 -> 完成 future；删除固定 root absent -> data absent，并清理/生命周期覆盖
attempt-scoped multipart residue。权衡是 root 字段更完整、读取前需 full-root 验证，换来 current config 漂移、
partial attempt、index corruption 和 root-first cleanup 都可确定收敛；恶意 provider 的 whole-root replacement 若进入
威胁模型，仍需后续签名/MAC 或外部可信绑定。

❓ **Q3** - **Object WAL group identity、WalRun root 与 crash discovery**：ADR 0025 要求 expected length/SHA
在 body 外，但若每 group 还要同步写 metadata descriptor，就会把额外远端提交重新放进 Object WAL ACK 热路径；
如果不写，又必须解释重启如何发现所有已 ACK group。

➡️ 推荐 run 开放 append 前只持久化一次 immutable `WalRunRoot`，冻结 cell/provider scope、shard、run/session、
Owner/Storage Epoch、exact key prefix、format/codec/encryption/digest family、起始 sequence 与总恢复预算。每组先
seal 最终 canonical request body，再以固定宽度 sequence、body length 和完整 lowercase SHA-256 构造 scoped
conditional-create leaf key，例如
`<run-prefix>/<seq19>/<len19>-sha256-v1-<64hex>.nwg`；leaf key + verified header 就是可重建的
ObjectExtent descriptor，不要求同步 metadata-service row。重启从 root 取得 prefix，以有总页数/对象数/字节/
时间预算且 same-prefix LIST-after-PUT 强一致的 LIST 发现 extents，再验证 header、frame、typed coverage 并按
binding 独立重建连续 Durable Frontier。周期 descriptor pages/sealed manifest 只能异步加速和帮助 GC，不能成为
ACKed tail 的唯一发现权威。权衡是把控制面移出 group ACK、保留 single data PUT 的 cost-first 路径，但增加
恢复 LIST 成本并缩小 provider 兼容面；0.2 对不满足强 LIST 的 provider 直接拒绝 `OBJECT_WAL`，而不是暗中改成
未建模的 per-group rooted ACK。

❓ **Q4** - **protocol frame 与 append atomicity 的精确粒度**：ADR 0026 已冻结 payload bytes，却还没有决定
一个 `MemoryRecords`、Kafka batch、Pulsar entry、网络 request 或 transaction 中哪一个才是 frame，以及多个
frame 属于一次 append 时能否只提交前缀。

➡️ 推荐两层模型。Kafka `1 frame = 1` 个完整、broker 已完成 offset/leader-epoch 赋值的原始 `RecordBatch`，
coverage 直接取 batch 的 `[baseOffset,lastOffset]`，不能由 record count 推导；同一分区、同一次
`MemoryRecords` storage append 解出的全部 frames 构成一个 `KafkaAppendCommitSet`，全部 frame 的 boundary、
CRC、native CRC、coverage 与 ordinal/count 都通过且全部 durable 后才整体可见、ACK 和推进 frontier，禁止只提交
前缀。Pulsar `1 frame = 1` 个 exact ManagedLedger entry = 一个 `(ledgerId,entryId)`，一次 `asyncAddEntry` 自成
commit set，不拆 client batch。ObjectExtent 可聚合多个 frame，但只是物理 group/PUT 边界；network request、
transaction、individual message 都不是 frame。权衡是 Kafka 多 batch append 需要 per-frame index/CRC 与一个小型
commit-set envelope，换来 native batch random read、幂等/事务 header 和空 batch offset span 均无损保留。

❓ **Q5** - **virtual-ledger reservation registry 的原子物理形态**：ADR 0027 要求 deployment registry 分配
不重叠 cell slices，但每 cell 一个独立 key 无法用当前单-key CAS 阻止并发重叠分配。

➡️ 推荐 0.2 以一个 bounded、deployment-wide `PulsarVirtualLedgerNamespaceRegistryRecord` 为唯一分配
authority；完整 assignment table 按 `startInclusive` canonical 排序并以单 key CAS 更新，per-cell lookup 只是
可丢失、可重建的派生索引。记录至少包含 deployment/domain identity、固定总保留区间、native-exclusion proof
digest、单调 `registryEpoch`、有上限的 slice assignments，以及 operator evidence/value digest。每次 CAS 在写前
验证区间有界、互不重叠、未复用；watch/cache 只加速读取。权衡是 registry 更新串行且 value 大小有上限，换来
不依赖不存在的 Oxia multi-key transaction 即可证明全局无重叠。exact max cells/encoded bytes、slice identity/
lifecycle/耗尽、allocator epochs 和 chain state machine 都依赖这个根选择，留到下一轮。

## Deferred descendants

- aggregate logical schema/version, Kafka V2 feature level, Oxia publication equality, delete/recreate retirement,
  and Kafka snapshot/replay invariants depend on Q1;
- sealed-ledger exact binary field IDs/limits, provider durability admission, read-handle concurrency, multipart cleanup,
  and any BookKeeper-delete revalidation hook depend on Q2;
- exact WalRun/leaf binary encoding, encryption nonce or durable local retry journal, checkpoint cadence, GC handoff, and
  crash-cut vectors depend on Q3;
- Kafka commit-set wire encoding/index and read/compaction vectors depend on Q4;
- slice identity/lifecycle/sizing, registry and allocator epoch layers, allocation response loss, ledger head/node
  publication, rollover/takeover, cursors, trim, compaction, replication, and transactions depend on Q5;
- `V2-OPEN-OBJ-01`, `V2-OPEN-BK-02`, and `V2-OPEN-BENCH-01` remain executable evidence gates rather than prose
  questions.

## Confirmed answer and authoritative synchronization

The user answered: “全部按推荐确认”. The decisions were synchronized as follows:

- Q1 / `V2-OPEN-META-03` →
  [ADR 0028](../../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md): protocol-native typed
  incarnations, incarnation-scoped keys, and retry-stable deterministic binding/epoch IDs;
- Q2 / `V2-OPEN-BK-05` →
  [ADR 0029](../../decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md): deterministic attempt keys, bounded
  root v1, data/root/read publication verification, and root-before-data cleanup;
- Q3 / `V2-OPEN-OBJ-07` →
  [ADR 0030](../../decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md): one pre-open WalRun root,
  sequence/length/SHA leaf identities, and bounded strong-LIST crash discovery without per-group metadata commits;
- Q4 / `V2-OPEN-OBJ-08` →
  [ADR 0031](../../decisions/0031-v2-protocol-frame-and-append-commit-set.md): one assigned Kafka batch per frame with an
  all-or-none partition commit set, and one Pulsar entry per frame/commit set;
- Q5 / `V2-OPEN-PUL-OBJ-03` →
  [ADR 0032](../../decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md): one bounded deployment-wide
  single-key-CAS registry is slice-allocation authority and per-cell lookups are derived.

Implementation and executable evidence remain NotStarted.
