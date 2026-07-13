# Nereus Capability Tracks and Delivery Plan

> 状态：Current roadmap
> `Future 1-8` 是稳定的能力轨道编号，不是“全部尚未开始”的阶段标签。

## 1. 交付原则

Nereus 的 north-star architecture 一次定义，工程按能力依赖增量交付。每个轨道必须保持：

```text
logical truth = stream head + reachable commit log
logical coordinate = streamId + offset
physical selection = generation-aware read index
protocol/table state = projection
```

跨轨道规则：

- 下层 API 不引入 Pulsar/Kafka 类型；
- 所有成功 append 都有 stable offset，不允许 profile 跳过 Oxia head commit；
- async 只延后 object/read-optimized generation，不延后逻辑提交；
- higher generation 不改变 offsets/projections；
- routing 不成为 durable ownership；
- catalog 不进入 producer ack；
- `Designed/Reserved` 不得写成 `Implemented`。

## 2. 当前交付状态

| Track | Delivery mapping | Status | Next gate |
| --- | --- | --- | --- |
| F1 Core Stream Storage | Phase 1 M0-M8 + Phase 1.5 P15-M0-M6 | Implemented/final-gated | F2/F4 consume the stable L0 surface |
| F2 ManagedLedger Facade | Phase 2 F2-M0-M6 | In progress（M0/M0R/M0R2 + P15-M6 + F2-M1-M4 complete） | F2-M5 real broker restart/failover and broker E2E |
| F3 Cursor/Subscription | later phase | Designed | F2 projection + F1 trim/read stable |
| F4 Materialization/Compaction | later phase | Designed | generation schema + generic read target |
| F5 KoP/Kafka | later phase | Designed | F2 facade + stable offset/projection + txn boundary |
| F6 Lakehouse | later phase | Designed | F4 compacted generation and GC references |
| F7 Routing/Elasticity | later phase | Designed | F1 session/fencing + F2/F5 lookup projections |
| F8 Advanced Pulsar | later phase | Designed | F2/F3/F4/F7 foundations |

Phase 1 implements only `OBJECT_WAL_SYNC_OBJECT` execution。Phase 1.5 changes the L0 abstraction/recovery/lifecycle
foundation but intentionally keeps that executable-profile boundary。Future 2 consumes the same strict Object-WAL
profile from the completed P15-M6 surface；BookKeeper and async profiles remain reservations until their adapters/state machines pass their
own gates。

## 3. Dependency graph

```mermaid
flowchart LR
    F1["F1 Phase 1 Core Storage"] --> P15["Phase 1.5 L0 Foundation"]
    P15 --> F2["F2 ManagedLedger Facade"]
    P15 --> F4["F4 Materialization / Compaction"]
    F1 --> F7["F7 Routing / Elasticity"]
    F2 --> F3["F3 Cursor / Subscription"]
    F2 --> F5["F5 KoP / Kafka"]
    F4 --> F5
    F4 --> F6["F6 Lakehouse"]
    F2 --> F8["F8 Advanced Pulsar"]
    F3 --> F8
    F4 --> F8
    F7 --> F8
    F5 -. shared retention/txn contracts .-> F8
```

这不是所有设计工作的严格串行计划。F2-M0R2 新发现的 P15-M6 cumulative-result handoff 与 F2-M1
projection foundation 已完成；F2 production 现在进入 F2-M2。F4 production
仍不能越过它依赖的 cursor/reader/reference correctness contracts。

## 4. F1 — Core Stream Storage

Detailed design: `nereus-future1-core-stream-storage.md`
Implemented Phase 1 contract: `../phase-1-core-stream-storage/README.md`
Active Phase 1.5 contract: `../phase-1.5-core-storage-foundation/README.md`
Delivery decision: `../decisions/0004-insert-phase-1-5-generic-storage-foundation.md`

### Owns

- protocol-neutral `StreamStorage`；
- stream identity/lifecycle；
- append session and fencing；
- primary WAL boundary and Object WAL v1；
- stream head / commit log / generation-0 read index；
- resolve/read/trim/recovery；
- checksum、format、timeout、close contracts。

### Does not own

- ManagedLedger/MessageId semantics；
- durable cursor/group/transaction semantics；
- higher-generation compaction workers；
- routing and catalog operations。

### Current implementation gate

Phase 1 done requires M0-M8 and the full definition in
`../phase-1-core-stream-storage/05-implementation-plan-and-tests.md`。BookKeeper/async profiles are not
part of that done definition。

### Phase 1.5 delivery extension

F2-M0R exposed the original shared L0 prerequisites and M0R2 exposed one result handoff，so the roadmap places
P15-M0-M6 before F2-M1：

- tagged Object/BookKeeper `ReadTarget` values and generic result/resolve contracts；
- primary-WAL adapter registry with Object WAL v1 parity；
- stable head commit separated from generation-zero materialization；
- legacy-record dual-read and generic-target new-write；
- exact retained append attempt recovery；
- authoritative seal/logical-delete lifecycle。
- P15-M6 public cumulative logical size copied from existing committed truth。

Phase 1.5 P15-M0-M6 still supports only strict Object WAL。BookKeeper IO、`WAL_DURABLE` success、async
workers and higher generations remain outside this delivery。

## 5. F2 — ManagedLedger Facade

Detailed design: `nereus-future2-managed-ledger-facade.md`
Code-level design: `../phase-2-managed-ledger-facade/README.md`
Current milestone: F2-M0/M0R/M0R2 + P15-M6 + F2-M1-M4 complete；F2-M5 real broker restart/failover and broker E2E active；production facade/cursor、generation-safe write-fence handoff and shared-store peer lifecycle implemented

### Owns

- storage class `nereus`；
- `ManagedLedgerFactory` and ManagedLedger facade；
- Pulsar entry encoding；
- virtual ledger / Position / MessageId projection；
- topic open/load/unload and compatibility stats。

### Entry gate

F2-M0/M0R/M0R2 closed the facade design gate，and P15-M6 closed the final cumulative-result prerequisite before
F2-M1-M4 completed and consumed these entry contracts；F2-M5 now integrates them into the Pulsar fork：

- F1 append/read/trim error semantics are stable；
- Pulsar fork/API blobs and repository boundary are locked；
- mapping v1 is one stream/one virtual ledger with `entryId == stream offset`；
- same-name delete/recreate uses a new projection incarnation/stream/virtual ledger；
- non-known append outcomes carry and recover the exact retained attempt before writes resume；
- generic `AppendResult` exposes logical range independently from physical target；F2 does not inspect object fields；
- L0 seal/logical-delete are authoritative and implemented；
- every other Nereus profile is rejected before IO。

### Exit gate

- Position remains stable across restart and future generation replacement；
- old-incarnation Position cannot alias a recreated topic；
- append ack maps selected durability boundary to Pulsar callback once；
- no broker path assumes virtual ledger id is a BookKeeper ledger id；
- storage class coexistence/offload behavior is explicit。

## 6. F3 — Cursor and Subscription State

Detailed design: `nereus-future3-cursor-subscription.md`

### Owns

- mark-delete/read-position offsets；
- individual ack ranges and snapshot objects；
- cursor CAS and failover recovery；
- Exclusive/Failover/Shared durable progress；
- retention low-watermark contribution。

### Entry/exit gates

- F2 can map Position to one persisted-entry offset and preserve MessageId batch indexes as in-entry
  sub-indexes；
- F1 resolver and trim contract is stable；
- cursor snapshot ref is authoritative only through Oxia state；
- ack/trim races and failover redelivery have deterministic tests；
- Key_Shared/delayed/pending-ack remain F8。

## 7. F4 — Materialization, Compaction and Generation Replacement

Detailed design: `nereus-future4-compaction-generation.md`

### Owns

- async materialization task/checkpoint/lag；
- primary-WAL retention gate；
- compaction planner/worker；
- higher-generation publish and reader selection；
- fallback and GC references；
- topic-compaction primitive。

### Entry gate

- F1 generation-0 index and commitVersion contracts stable；
- Phase 1.5 generic read target/dispatcher and stable-commit split implemented；
- conditional higher-generation publish schema is frozen；
- source ranges and checksums form deterministic task identity。

### Exit gate

- publish changes physical target only；
- repair handles upload/publish/checkpoint partial states；
- lag blocks unsafe primary-WAL GC；
- old generations are retained until all reader/cursor/task/catalog references are safe。

## 8. F5 — KoP / Kafka Projection

Detailed design: `nereus-future5-kop-compatibility.md`

### Owns

- Kafka offset/base-offset projection；
- Produce/Fetch and high-watermark mapping；
- group coordinator state/offsets；
- idempotent producer and Kafka transaction projection；
- Kafka compaction and leader projection。

### Entry/exit gates

- Kafka offset is exactly Nereus record offset；
- a Kafka-compatible/canonical payload mapping makes each Kafka record consume one L0 offset；
- `PULSAR_ENTRY_V1` mixed access is rejected until a mapping-version and migration contract exists；
- ProduceResponse only follows stable append result；
- record-batch offset rewrite has layered checksum semantics；
- group offsets remain separate from Pulsar cursors but participate in retention；
- no second Kafka durable log or remote-log truth。

## 9. F6 — Lakehouse SBT / SDT

Detailed design: `nereus-future6-lakehouse-sbt-sdt.md`

### Owns

- Stream-Backed Table and Stream-Delivered-to-Table；
- Iceberg-first catalog adapter boundary；
- lineage、snapshot、delivery idempotence、repair；
- catalog object references and lag metrics。

### Entry/exit gates

- F4 higher generations and compacted object schema stable；
- table snapshot references only logically committed ranges；
- catalog lag/failure does not affect stream visibility；
- repair order begins with stream head/reachable commits and derived index, never object list；
- catalog reference prevents unsafe object GC。

## 10. F7 — Routing, Brown-out and Elasticity

Detailed design: `nereus-future7-routing-brownout-elasticity.md`

### Owns

- broker membership/capabilities/load；
- zone-aware preferred routing；
- degraded/brown-out/readmission；
- Pulsar lookup and Kafka leader projection；
- cache warmup and cross-zone policy。

### Entry/exit gates

- append session and routing role remain separate；
- stale writers are fenced by head commit, not routing cache；
- broker remap does not move durable data；
- Pulsar/Kafka projections derive from one routing state；
- health thresholds are policy, not correctness constants。

## 11. F8 — Advanced Pulsar Semantics

Detailed design: `nereus-future8-advanced-pulsar-semantics.md`

### Owns

- Key_Shared ordering/drain boundary；
- delayed delivery durable recovery；
- pending ack and transaction buffer semantics；
- replicated subscriptions；
- schema/system-topic bootstrap；
- Pulsar topic compaction and geo-replication；
- policy interactions。

### Entry/exit gates

- F2 projection、F3 cursor、F4 generation and F7 routing contracts stable；
- no feature creates a separate durable log；
- transaction/cursor changes use explicit state machines, not assumed cross-shard transaction；
- system topic bootstrap has a resumable order；
- geo-replication stores explicit source/target offset translation。

## 12. Cross-track verification waves

Verification follows architecture dependencies rather than waiting for all tracks：

| Wave | Scope |
| --- | --- |
| V1 | F1 deterministic unit/contract/failure-injection tests |
| V1.5 | Phase 1.5 mixed-metadata、generic target、exact recovery and lifecycle tests |
| V2 | F2/F3 Pulsar facade and cursor compatibility suites |
| V3 | F4 materialization lag、generation、GC and corruption tests |
| V4 | F5/F7 protocol routing/failover compatibility |
| V5 | F6/F8 catalog、advanced semantics、geo/txn integration |

Benchmark、chaos、model checking 和 production profile claims 只能在对应 implementation gate
之后使用；不能用 future design 文本代替证据。

## 13. Document template

每个 capability document 至少包含：

```text
status and prerequisites
motivation
scope / non-scope
layer boundary
API and durable schema
state transitions and linearization points
failure/repair model
compatibility impact
implementation gate
```

状态改变时先更新 `nereus-design-index.md` 和当前代码级 README，再更新本 roadmap。
