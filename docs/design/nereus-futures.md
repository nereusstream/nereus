# Nereus Future 拆分设计

> 本文是 `pip/Nereus/nereus-overall-architecture.md` 的配套 future 拆分文档。
> 当前阶段只定义模块边界、设计范围和后续技术文档结构；验证、benchmark、chaos 和
> compatibility suite 在各 future 设计冻结后推进。
> 写 future 文档前先阅读 `pip/Nereus/nereus-terminology.md`。

## 1. 总体原则

Nereus 的目标架构一步到位，对标 Ursa 的 Oxia 控制面、共享对象数据面、
stateless/leaderless broker、offset index、multi-stream WAL object、compaction 和
lakehouse-native stream-table duality。

Future 拆分不是产品阶段缩水，而是把完整目标架构切成可设计、可评审、可实现的模块：

- L0: Core Stream Storage
- L1: Pulsar Compatibility Projection
- L2: KoP / Kafka Compatibility Projection
- L3: Compaction + Lakehouse
- L4: Routing / Elasticity / Operations

每个 future 都必须保持同一个内部真相：

```text
streamId + offset
```

Pulsar `MessageId`、ManagedLedger `Position`、Kafka offset、cursor progress、lakehouse
snapshot 都是这个内部坐标的外部投影或衍生状态。

## 2. Future 1：Core StreamStorage + Object WAL

Detailed design: `pip/Nereus/nereus-future1-core-stream-storage.md`.

### Motivation

建立 Nereus 的 Ursa-parity 核心：stream offset、object WAL、Oxia offset index、
commit-time offset assignment 和 read resolver。

### Scope

- `StreamStorage` API；
- stream metadata；
- append session；
- object WAL writer；
- object manifest；
- Oxia offset index；
- commit-time offset assignment；
- read resolver；
- basic MessageId projection。

### Non-scope

- 完整 ManagedLedger facade；
- Shared/Key_Shared cursor 复杂语义；
- KoP group/transaction；
- lakehouse catalog；
- routing brown-out。

### Design Gate

Future 1 的设计必须能完整解释：

- producer ack 线性化点；
- object upload 与 Oxia offset index commit 的顺序；
- multi-stream WAL object 的 partial slice visibility；
- stale epoch fencing；
- `streamId + offset` 到 object range 的读路径。

## 3. Future 2：ManagedLedger Facade

Detailed design: `pip/Nereus/nereus-future2-managed-ledger-facade.md`.

### Motivation

让 Pulsar broker 通过现有 ManagedLedger 形态接入 Nereus，同时不让旧 ledger 模型成为
底层 truth。

### Scope

- `ManagedLedgerFactory`；
- ManagedLedger-compatible runtime；
- virtual ledger projection；
- `Position(ledgerId, entryId)` 映射；
- entry projection；
- topic open/load/unload；
- basic cursor 接入；
- storage class `nereus`。

### Non-scope

- 改写 `PersistentTopic` 主流程；
- 完整 Shared/Key_Shared ack hole 处理；
- geo-replication；
- delayed delivery。

### Design Gate

Future 2 的设计必须能完整解释：

- 一个 committed stream range 如何形成 virtual ledger；
- MessageId 如何稳定映射回 `streamId + offset`；
- topic unload/reload 后如何恢复读写；
- BookKeeper WAL profile 和 Object WAL profile 如何共用同一 facade。

## 4. Future 3：Cursor / Subscription State

Detailed design: `pip/Nereus/nereus-future3-cursor-subscription.md`.

### Motivation

把 Pulsar subscription 的 durable progress 从 BookKeeper cursor ledger 迁移到
Oxia + object snapshot。

### Scope

- mark-delete offset；
- read-position offset；
- individual ack ranges；
- cursor CAS；
- cursor snapshot object；
- Exclusive/Failover cursor；
- Shared cursor；
- cursor recovery；
- retention low-watermark。

### Non-scope

- Key_Shared 全量顺序优化；
- pending ack transaction；
- delayed delivery timer；
- replicated subscription。

### Design Gate

Future 3 的设计必须能完整解释：

- mark-delete 和 individual ack holes 如何并存；
- cursor snapshot 与 Oxia small state 如何保持版本一致；
- cursor update 与 trim/retention/GC 的安全关系；
- broker failover 后 subscription 如何恢复。

## 5. Future 4：Compaction + Generation Replacement

Detailed design: `pip/Nereus/nereus-future4-compaction-generation.md`.

Related design basis: `pip/Nereus/nereus-storage-object-format.md` and
`pip/Nereus/nereus-commit-protocol.md`.

### Motivation

让 multi-stream WAL object 转换为 per-stream read-optimized object，并通过 Oxia
generation replacement 原子切换读路径。

### Scope

- compaction planner；
- WAL object reader；
- compacted object writer；
- generation overlay；
- highest-generation read resolver；
- old generation fallback；
- compaction checkpoint；
- GC protection。

### Non-scope

- 外部 lakehouse catalog 完整提交；
- SDT delivery；
- topic compaction 的全部协议细节。

### Design Gate

Future 4 的设计必须能完整解释：

- compaction 为什么不改变 offset；
- replacement 前后 reader 如何不中断；
- 新 generation 损坏时如何 fallback；
- active reader/cursor 如何保护旧 objects；
- topic compaction 如何复用 generation replacement。

## 6. Future 5：KoP Compatibility

Detailed design: `pip/Nereus/nereus-future5-kop-compatibility.md`.

### Motivation

让 Kafka 客户端通过 KoP 访问同一套 Nereus stream storage，不引入第二套 durable log。

### Scope

- Kafka offset 等于 stream record offset；
- ProduceResponse base offset；
- Fetch offset index lookup；
- group offset state in Oxia；
- idempotent producer sequence；
- transaction marker projection；
- read_committed/read_uncommitted；
- Kafka topic compaction projection；
- leader epoch projection。

### Non-scope

- 新 Kafka broker 实现；
- Kafka protocol handler 重写；
- 与 Pulsar 分离的 Kafka storage。

### Design Gate

Future 5 的设计必须能完整解释：

- KoP produce ack 如何使用 Oxia commit result；
- record batch base offset 如何在 object WAL 模式下生成或重写；
- group coordinator failover 后 offset 如何恢复；
- Kafka transaction visibility 如何映射到 Nereus transaction state；
- Kafka compaction 与 Pulsar topic compaction 如何共享底层 compaction service。

## 7. Future 6：Lakehouse SBT / SDT

Detailed design: `pip/Nereus/nereus-future6-lakehouse-sbt-sdt.md`.

Related design basis: `pip/Nereus/nereus-storage-object-format.md` and
`pip/Nereus/nereus-commit-protocol.md`.

### Motivation

把 lakehouse 作为 Nereus 的一等能力，而不是 BookKeeper/offload 之后的外部复制。

### Scope

- Stream-Backed Table (SBT)；
- Stream-Delivered-to-Table (SDT)；
- Iceberg catalog commit；
- Delta/Hudi adapter boundary；
- catalog snapshot；
- catalog repair；
- delivery idempotence；
- lag metrics。

### Non-scope

- producer ack 依赖 lakehouse catalog；
- 外部查询引擎写回 stream object；
- 每个 catalog 的深度优化。

### Design Gate

Future 6 的设计必须能完整解释：

- SBT snapshot 如何追溯到 offset index generation；
- catalog commit 成功但 Oxia commit 失败如何 repair；
- Oxia visible offset 为什么始终是 stream truth；
- SDT 失败为什么不影响 stream read；
- Iceberg first、Delta/Hudi optional 的 adapter 边界。

## 8. Future 7：Routing / Brown-out / Elasticity

Detailed design: `pip/Nereus/nereus-future7-routing-brownout-elasticity.md`.

### Motivation

实现 stateless/leaderless serving 的产品体验：broker 可弹性伸缩，故障时不搬数据，
preferred broker 只做 locality 和 cache。

### Scope

- broker session；
- zone-aware routing ring；
- preferred broker；
- append session transfer；
- fencing token；
- degraded broker detection；
- brown-out eviction；
- readmission warmup；
- cross-zone fallback；
- lookup / KoP metadata projection；
- cache invalidation。

### Non-scope

- broker durable ownership；
- partition 数据搬迁；
- 依赖 broker 本地磁盘恢复 correctness。

### Design Gate

Future 7 的设计必须能完整解释：

- 任意 broker 为什么可以服务任意 stream；
- stale broker commit 为什么会被 Oxia fencing 拒绝；
- routing remap 为什么不需要数据搬迁；
- client retry 如何与 preferred broker/readmission 配合；
- Pulsar lookup 和 Kafka MetadataResponse 如何投影同一 routing state。

## 9. Future 8：Advanced Pulsar Semantics

Initial design boundary: `pip/Nereus/nereus-overall-architecture.md`.
Dedicated detailed design doc: `pip/Nereus/nereus-future8-advanced-pulsar-semantics.md`.

### Motivation

把 Nereus 从“能跑 Pulsar basic topic”提升为 Pulsar-native 商业产品能力。

### Scope

- Key_Shared ordering；
- delayed delivery；
- pending ack transaction；
- transaction buffer；
- replicated subscription；
- schema/system topic bootstrap；
- topic compaction compatibility；
- geo-replication；
- namespace/topic policy interaction。

### Non-scope

- 改变 L0 stream offset truth；
- 为某个 Pulsar 特性引入独立 durable log；
- 让 broker local state 成为 correctness source。

### Design Gate

Future 8 的设计必须能完整解释：

- Key_Shared rebalance 后的顺序边界；
- delayed delivery timer 如何从 durable state 恢复；
- pending ack transaction 如何与 cursor state 对齐；
- schema/system topic 如何在 `nereus` storage class 下 bootstrap；
- geo-replication 如何处理 source/target stream offset translation。

## 10. 后续文档模板

每个 future 的技术细节文档使用同一个结构：

```text
1. Motivation
2. Scope
3. Non-scope
4. Layer boundary
5. API
6. Oxia metadata schema
7. Object or snapshot format
8. State transition
9. Failure model
10. Compatibility impact
11. Future gate
```

验证、benchmark、chaos、compatibility suite 和 model checking 是 future 设计冻结后的
验收工作，不进入当前总体设计的主线。
