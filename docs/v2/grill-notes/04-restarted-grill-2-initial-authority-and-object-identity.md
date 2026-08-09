---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 2: initial authority and Object identity

Date: 2026-08-09

ADRs 0015 through 0018 resolved the previous frontier. This record preserves the next independent questions and
recommendations that were presented. The user subsequently confirmed all four recommendations. The accepted contracts
are ADRs 0019 through 0022; this session record is not runtime evidence.

## Source facts used for recommendations

- 0.2 now creates exactly one initial Storage Epoch per Topic Incarnation, so binding/epoch partial visibility is the
  remaining create-time correctness cut; it cannot be delegated to an online transition state machine.
- The pinned Pulsar development checkout
  `5.0.0-M1-nereus@11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` exposes both ledger-based
  `LedgerOffloader.offload(ReadHandle, UUID, ...)` and an evolving `streamingOffload(...)` API shape. Its ordinary
  prefix-offload path selects non-current ledgers and wraps each ledger attempt with native prepare/complete metadata.
- Object WAL groups are bounded Physical Extents and may carry multiple bindings from exactly one Protocol Cell. Their
  object identity therefore cannot serve as a Pulsar ledger ID or Kafka/Pulsar position.
- Compression, client-side encryption, provider-side encryption, and protocol decode can produce different byte
  domains; one checksum name without a declared domain would make ADR 0018 ambiguous.

## Current frontier

❓ **Q1** - **初始 Binding 与 Storage Epoch 如何原子可见**：topic create 超时、重试、controller/store
failover 或并发 open 时，能否看到只有 Binding 或只有 initial Epoch 的半成品？

➡️ 推荐把 Binding + initial Epoch 作为一个 visible aggregate。KRaft 用 replay-atomic controller batch；
MetadataStore/Oxia 在精确 API 满足合同时使用 transaction，否则写 deterministic `CREATING` intent，幂等补齐
两条记录后才切 `ACTIVE`。open/append/read 对半成品只能恢复或拒绝，不能推导默认 epoch。

❓ **Q2** - **Pulsar BK async Object 首版是 sealed-ledger 还是 streaming offload**：0.2 是否把当前仍在
append 的 ledger 同时流式写入 Object，还是只处理 sealed、non-current ledger？

➡️ 推荐 0.2 只走 sealed-ledger `offload(ReadHandle, ...)`。这让 attempt/completion、immutable coverage、
fallback 和 source deletion 与原生 ledger 生命周期同一个粒度。代价是 Object 冷副本最多落后一个 ledger
rollover；用 ledger size/age 上限和 lag admission policy 约束，而不是把 Object 放回 ACK 路径。

❓ **Q3** - **Object WAL checksum 的 byte domain**：provider durability proof、Object format 完整性和协议
payload 完整性是否共用一个 checksum？

➡️ 推荐两个显式层级：Object Extent digest 覆盖 Nereus 压缩和 client-side encryption 后、提交给 provider
的 canonical request body；frame checksum 覆盖 decode 后的协议 payload/record bytes。provider checksum 只有
在算法、byte scope 和 immutable version 都匹配时才能替代前者；两层字段不能互相代替。

❓ **Q4** - **Pulsar Object WAL 的 virtual ledger ID 与 Ledger Chain 权威**：谁分配
`PulsarPosition(ledgerId, entryId)` 中的 ledger ID，如何避免 Object group key 或数字排序变成协议真相？

➡️ 推荐 Pulsar Cell 在 MetadataStore/Oxia 中拥有 `PulsarVirtualLedgerStore`，从明确保留的 identity domain
分配唯一 virtual ledger ID，并显式发布 append-only Ledger Chain 顺序。entry ID 在一个 virtual ledger 内串行
分配；Object groups 仅是 Physical Extents，不能成为 ledger ID。ledger create/rollover 是低频控制元数据，
正常 append 不做远端 metadata commit。

## Deferred descendants

- offload Object root/extent granularity and completion publication depend on Q2;
- exact object-format checksum field names and encryption metadata depend on Q3;
- virtual-ledger rollover, recovery, trim, cursor, compaction, and replication cuts depend on Q4;
- online Storage Epoch transitions and cross-protocol projection remain outside 0.2 by ADRs 0015 and 0016;
- Kafka BookKeeper layout and benchmark thresholds remain evidence prerequisites, not questions to answer from prose.

## Confirmed answer and authoritative synchronization

The user answered: “全部按推荐确认”. The decisions were synchronized as follows:

- Q1 / `V2-OPEN-META-01` →
  [ADR 0019](../../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md): binding plus initial epoch have one
  atomic visible aggregate, with deterministic `CREATING` recovery where a backend cannot atomically publish all
  immutable records;
- Q2 / `V2-OPEN-BK-03` →
  [ADR 0020](../../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md): 0.2 offloads only sealed non-current Pulsar
  ledgers and excludes active-ledger streaming;
- Q3 / `V2-OPEN-OBJ-04` →
  [ADR 0021](../../decisions/0021-v2-object-wal-checksum-domains.md): Object Extent Digest and decoded Frame Payload
  Checksum are separate, non-substitutable integrity domains;
- Q4 / `V2-OPEN-PUL-OBJ-01` →
  [ADR 0022](../../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md): a Pulsar-cell
  MetadataStore/Oxia authority allocates virtual ledger IDs and explicit Ledger Chain order; Object identity never
  becomes a position.

Implementation and executable evidence remain NotStarted.
