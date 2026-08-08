---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 frontier: 0.2 scope and durability authorities

Date: 2026-08-09

This record recomputes Grill 2 after the latest documentation updates and preserves the questions and recommendations
that were presented. The user subsequently confirmed all four recommendations. The accepted contracts are ADRs 0015
through 0018; this session record is not runtime evidence.

## Why the frontier changed

[ADR 0014](../../decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md) already resolves Provider
Infrastructure sharing: physical infrastructure may be shared, Cell Provider Scope/session and correctness/deletion
authority may not. Repeating that question would not advance the design tree.

The remaining design tree separates product-scope choices from evidence gates. Four independent decisions can be made
now. Decisions about the exact Storage Epoch state machine, historical backfill, Pulsar BookKeeper/Object migration,
Projection Map representation, and semantic transfer depend on the answers below.

## Source facts used for recommendations

- The inspected local Pulsar checkout is clean
  `5.0.0-M1-nereus@11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9`; this is design input, not a V2 runtime receipt.
- `LedgerOffloader.offload` documents that ManagedLedger persists an attempt UUID before invoking the offloader and
  records completion afterward; its completion means the long-term copy is durable enough for BookKeeper deletion.
- `ManagedLedgerImpl` executes prepare-metadata → offload → complete-metadata, opens offloaded reads from the recorded
  driver/UUID, and consults `OffloadContext` before marking/deleting the BookKeeper source.
- Current Nereus Object abstractions and V2 prose carry exact length/checksum identity. ETag remains explicitly
  insufficient under multipart or server-side encryption; response loss therefore needs provider checksum/version proof
  or byte verification.

## Current frontier

❓ **Q1** - **0.2 的 Storage Epoch 交付深度**：0.2 是只实现可演进的数据模型和不变量，还是同时交付在线
profile transition？可选边界是：

1. 只实现 append-only chain、typed frontier 和 single-admitting-epoch 不变量，不提供在线切换 API；
2. 只支持 `BOOKKEEPER_WAL_ONLY` ↔ `BOOKKEEPER_WAL_ASYNC_OBJECT`；
3. 同时支持 Kafka Object ↔ BookKeeper，甚至 Pulsar BookKeeper ↔ Object。

➡️ 推荐 1。它保留未来演进所需的 durable model，但把 drain/seal/activate、rollback、history backfill 和
Pulsar MessageId/cursor 迁移从 0.2 核心路径移除。代价是 0.2 用户不能在线改变已有 Topic Incarnation 的
成本/性能档。

❓ **Q2** - **Kafka/Pulsar projection 与 migration 是否进入 0.2 runtime**：是只保留
`AccessProjection`/`MigrationLink` 边界及禁止双写权威的不变量，还是交付 secondary protocol serving
或 Kafka↔Pulsar authority transfer？

➡️ 推荐 0.2 只保留模型和拒绝双 Native Write Authority 的验证，不实现 Projection Map 存储、语义状态
翻译或 authority-transfer runtime。这样验收资源集中在 Kafka 强于 AutoMQ、Pulsar 不弱于原生 Pulsar；
代价是同一业务数据的跨协议访问与迁移继续延期。

❓ **Q3** - **Pulsar BK async Object 的权威归属**：`BOOKKEEPER_WAL_ASYNC_OBJECT` 的 offload attempt、
completion、fallback 和 BookKeeper 删除，是由原生 ManagedLedger metadata、Nereus parallel manifest，
还是两套共同授权？

➡️ 推荐原生 ManagedLedger metadata 为唯一 offload/lifecycle authority，Nereus 通过自定义
`LedgerOffloader` 产生 Object format；Nereus manifest 只能做 derived read/materialization index。
收益是复用 Pulsar 已有 attempt UUID、completion、fallback、retention/delete 状态机并最大化原生兼容；
代价是格式和生命周期受 ledger/offloader API 约束，Kafka 与 Pulsar 的 async-object publication 不会完全
共用一套控制逻辑。

❓ **Q4** - **Object WAL 丢 PUT 响应时的 durability proof**：是否采用按 provider capability 分层的
验证合同？

➡️ 推荐：HEAD 只有在返回 exact length + 与 immutable version 绑定的可信 content checksum 时才能直接
定案；否则执行有界 GET 并重算 checksum；ETag 永远不能单独作为证据；无法提供 deterministic immutable
create、所需 read-after-write 或有界验证的 provider 不允许承载 `OBJECT_WAL`。代价是极少数 uncertain
PUT 会增加 GET 成本，收益是 ACK/recovery 不依赖 provider-specific ETag 假设。

## Not on this frontier

- `V2-OPEN-MIGRATION-02..03` and `V2-OPEN-PUL-MIGRATION-01` depend on Q1.
- `V2-OPEN-PROJECTION-01..03` depend on Q2.
- sealed-ledger versus streaming Pulsar offload mechanics depend on Q3.
- `V2-OPEN-OBJ-01` is an M3 proof gate, not a product choice.
- `V2-OPEN-BK-02` requires the 10k/100k Kafka partition resource spike; that fact must not be guessed from prose.
- benchmark commit/threshold selection remains an M8 source/evidence task.

## Confirmed answer and authoritative synchronization

The user answered: “全部按推荐确认”. The decisions were synchronized as follows:

- Q1 / `V2-OPEN-MIGRATION-01` →
  [ADR 0015](../../decisions/0015-v2-0.2-storage-epoch-runtime-scope.md): one initial Storage Epoch per Topic
  Incarnation and no 0.2 online transition runtime;
- Q2 / `V2-OPEN-PROJECTION-SCOPE-01` →
  [ADR 0016](../../decisions/0016-v2-0.2-cross-protocol-runtime-scope.md): retain boundary/dual-authority rejection, no
  0.2 projection or migration runtime;
- Q3 / `V2-OPEN-BK-01` →
  [ADR 0017](../../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md): ManagedLedger metadata is sole Pulsar
  offload/lifecycle authority and Nereus manifest is derived;
- Q4 / `V2-OPEN-OBJ-02` →
  [ADR 0018](../../decisions/0018-v2-object-wal-uncertain-put-proof.md): capability-tiered HEAD/full-GET proof, never
  ETag alone, and reject unverifiable providers.

Implementation and executable evidence remain NotStarted.
