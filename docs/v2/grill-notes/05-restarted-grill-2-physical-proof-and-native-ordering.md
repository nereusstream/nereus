---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 3: physical proof and native ordering

Date: 2026-08-09

ADRs 0019 through 0022 resolved the previous frontier. This record preserves the next independent questions and
recommendations presented for confirmation. Nothing in this file is an accepted runtime contract until the user
explicitly confirms it and the result moves into the corresponding normative documents/ADRs.

## Source facts used for recommendations

- Kafka fork `76f62f3b83e882105219b6c7687dbde594a8b8a2` already publishes `CreateTopics` through one bounded atomic
  controller result. `RaftClient.prepareAppend` guarantees all supplied records commit if any commit. The currently
  pinned MetadataStore/Oxia public Java APIs instead expose single-key conditional puts; their batching is not an
  all-conditions-or-no-writes transaction. A composite aggregate works on both substrates without a cross-key saga.
- Pulsar fork `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` records one UUID/completion state for a normal sealed-ledger
  attempt and expects `readOffloaded`/`deleteOffloaded` to present a whole-ledger contract. Its stock offloader already
  uses deterministic data and index objects; native metadata does not inventory arbitrary child extents.
- The current Nereus S3 adapter calculates an uploaded-body CRC32C but places it in user metadata. It does not request a
  provider checksum, enable checksum-mode HEAD, or retain provider version/checksum-type fields. AWS SDK S3 2.47.5 has
  SHA-256/CRC32C, `FULL_OBJECT`/`COMPOSITE`, version-ID, and checksum-mode APIs, but no M3 provider evidence exists yet.
- Kafka magic-v2 and Pulsar wire checksums are both CRC32C but protect different, protocol-specific subranges. Pulsar
  payloads may remain compressed or client-encrypted. Neither native checksum value can be relabeled as the accepted
  `FramePayloadChecksum` without freezing the V2 payload mapping.
- Pulsar ManagedLedger stores ledgers in numeric order, and public `Position`/`MessageIdAdv` comparisons compare ledger
  ID before entry ID. Native long-ledger allocation can enter the V1 high-ID range. An explicit metadata chain therefore
  still needs numerically compatible IDs and an enforced disjoint namespace to preserve stock behavior.

These are source capabilities and constraints, not V2 runtime evidence.

## Current frontier

❓ **Q1** - **Topic Binding Aggregate 的物理形态**：已经确认 Binding + initial Storage Epoch 必须原子可见，
0.2 还要不要把它们做成两个物理 record，再依赖 KRaft batch 或 Oxia `CREATING` saga？

➡️ 推荐一个不可变 `TopicBindingAggregateRecord` 直接包含完整 Binding 和 initial Epoch。Kafka 把这一个 record
加入原生 atomic `CreateTopics` batch；MetadataStore/Oxia 对一个 key 做 `putIfAbsent`，响应丢失就 reread 后做
exact-content compare。两个逻辑 store 只是投影 typed view。代价是 whole-record schema evolution/CAS 和少量字段
聚合，但 0.2 默认路径不再需要 cross-key transaction、half-record repair 或 `CREATING` saga；后者只保留为未来
确实必须拆 key 时的 ADR 0019 fallback。

❓ **Q2** - **sealed Pulsar ledger 的 Object layout**：一个 `(ledgerId, UUID)` attempt 是一份 data object，
还是多个独立 data extents 再加 root？失败发生在 root 前、或删除先删 root 时，如何找到全部 partial children？

➡️ 推荐 0.2 一个 sealed ledger 对应一个 bounded immutable data `ObjectExtent` + 一个 deterministic immutable
sparse-index/root object；两者都严格 attempt-scoped。data 可用 multipart upload，但完成后仍是一个 provider
object；ledger bytes/entries/age rollover 保证 bounded。data 先于 root 发布，只有两者、`0..LAC` 连续覆盖、
digest 和 ledger-equivalent `ReadHandle` 全部验证后 offload future 才成功。两个 key 都可从 `(ledgerId, UUID)`
确定，所以 root 不存在时也能幂等清理。权衡是首版放弃多个独立 extent 的并行度，但避免再造 attempt inventory
与 partial-delete 状态机，最接近原生 Pulsar 已验证的生命周期。

❓ **Q3** - **首版 checksum 算法与 provider proof**：两个 checksum domain 已确认，但首版算法、provider
HEAD 可接受字段、以及不支持 provider checksum/version 的兼容路径还没有冻结。

➡️ 推荐 `ObjectExtentDigest = SHA-256/v1`、`FramePayloadChecksum = CRC32C/v1`。expected extent digest 放在
request body 外的 immutable Object Extent descriptor，避免 digest value 自己参与被 hash 的 body；另外建模
`ProviderObjectProof { providerVersionId, canonicalRequestBodyLength, checksumAlgorithm, checksumType, value }`，
不复用 V1 含糊的 generic checksum/user metadata。PUT/HEAD 只有同一个 immutable version、exact length、
same SHA-256 且 `FULL_OBJECT` 时才是 fast proof；否则 bounded full GET 后重算 SHA-256，两条都无法完成就拒绝
`OBJECT_WAL`。权衡是 Object 层多一点 SHA-256 CPU，部分兼容 provider 在罕见 response-loss 时多一次 GET，
换来 collision-resistant content identity；frame 热路径仍用低成本 CRC32C。

❓ **Q4** - **Kafka/Pulsar frame 的 canonical payload bytes**：ADR 0021 的“decoded protocol payload”是指
解开 Object envelope 后的协议原生 bytes，还是继续把 Kafka 解压成 records、Pulsar 拆成 individual messages
再重新 canonicalize？

➡️ 推荐只解开外层 Object encryption/compression，不重编码 application records/messages。Kafka checksum
覆盖 exact assigned protocol-native `MemoryRecords`/完整 record-batch byte sequence 与边界；Pulsar checksum
覆盖 exact ManagedLedger entry bytes，保留协议自己的 compression、client encryption 和 batching。原生 Kafka/
Pulsar CRC 仍按各自 byte domain 再验证。权衡是不新增 per-application-record checksum，但避免昂贵/有损/对
Pulsar opaque encrypted payload 不可行的 canonical reserialization，同时由 frame CRC 覆盖全部协议 blob。

❓ **Q5** - **Pulsar virtual ledger ID 的数值兼容与 namespace reservation**：显式 Ledger Chain 是权威，
但 stock broker/client 仍按 ledger ID 数值排序；单纯使用 V1 的 high range 也不能阻止 native generator 碰撞。

➡️ 推荐在 Pulsar deployment contract 中保留 `[2^62, 2^63 - 2]` 给 virtual ledgers，由 deployment-level
reservation registry 给各 Protocol Cell 分配不重叠、永不复用的 slice，并修改、验证 native ledger-ID generator
永不进入整个保留区间；deployment/cell reservation 缺失、重叠、漂移或撤销就 fail closed。cell-scoped
single-key CAS allocator 只在自己的 slice 内发严格递增 ID，允许 gap、永不复用。显式 predecessor/head 仍是
chain authority；数值单调只是兼容 stock `Position`/`MessageIdAdv` compare 的 projection。权衡是多一个 Pulsar
fork 和部署准入义务，换来不改公共 MessageId 排序、不发生 native/cross-cell 静默碰撞，也不给 Object identity
任何位置权威。

## Deferred descendants

- aggregate schema fields/versioning and backend conformance vectors depend on Q1;
- multi-extent offload, if later required, needs a separate durable attempt inventory/root-order/delete ADR after Q2;
- exact binary checksum field encoding and real-provider admission matrix depend on Q3/Q4;
- virtual-ledger node schema plus create/seal/rollover/uncertain-publication recovery depend on Q5;
- cursor, trim, compaction, replication, transaction recovery, online profile transition, and cross-protocol projection
  remain later descendants or outside 0.2 under the already accepted scope ADRs;
- `V2-OPEN-OBJ-01`, `V2-OPEN-BK-02`, and `V2-OPEN-BENCH-01` require executable evidence rather than a prose answer.

## Answer status

Awaiting explicit confirmation. Until then, `V2-OPEN-META-02`, `V2-OPEN-BK-04`, `V2-OPEN-OBJ-05`,
`V2-OPEN-OBJ-06`, and `V2-OPEN-PUL-OBJ-02` remain open and their recommendations are non-normative.
