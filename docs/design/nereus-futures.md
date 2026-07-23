# Nereus Capability Tracks and Delivery Plan

> 状态：Current roadmap
> `Future 1-9` 是稳定的能力轨道编号，不是“全部尚未开始”的阶段标签。

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
| F1 Core Stream Storage | Phase 1 M0-M8 + Phase 1.5 P15-M0-M6 + F1-BK BK-M0-M6 | Implemented/final-gated；F1-BK BK-M0–M6 complete | F5/F6/F7/F8 may consume the stable lower-storage contracts；online WAL-profile migration remains separate |
| F2 ManagedLedger Facade | Phase 2 F2-M0-M6 | Implemented/final-gated（M0/M0R/M0R2 + P15-M6 + F2-M1-M6 complete） | F3/F4 consume the locked facade/storage boundary |
| F3 Cursor/Subscription | Phase 3 F3-M0-M6 | Implemented/final-gated | F4/F5/F8 consume stable cursor/reference semantics |
| F4 Materialization/Compaction | Phase 4 F4-M0-M6 | Implemented/final-gated；M4 implements recovery/retirement/physical+cursor GC、activation/global domains、restart/scale/late-PUT cuts、atomic readiness rollover and retry-disabled real source deletion；M5 implements durable registration/readiness/activation、protected async Object-WAL、pre-I/O lag admission、coupled production materialization、versioned exact-evidence retention/F3 trim、durable backlog eviction and exact Pulsar policy/admin routing，then final-gates MessageIds、unload/failover/rejoin/post-trim IO and BookKeeper coexistence；M6 BD–BQ close scale/failure/compatibility and the clean 203/203-task aggregate；safe defaults keep production deletion disabled | F5/F6/F8 may consume the final-gated F4 contracts |
| F5 KoP/Kafka | later phase | Designed | F2 facade + stable offset/projection + txn boundary |
| F6 Lakehouse | later phase | Designed | F4 compacted generation and GC references |
| F7 Routing/Elasticity | later phase | Designed | F1 session/fencing + F2/F5 lookup projections |
| F8 Advanced Pulsar | later phase | Designed | F2/F3/F4/F7 foundations |
| F9 Native Kafka Shared Storage | Phase 9 F9-M0-M7 | Designed；F9-M0 documentation gate complete | F9-M1 protocol-neutral ranged-entry API/read/format foundation |

Phase 1 implements only `OBJECT_WAL_SYNC_OBJECT` execution。Phase 1.5 changes the L0 abstraction/recovery/lifecycle
foundation but intentionally keeps that executable-profile boundary。Future 2 consumes the same strict Object-WAL
profile from the completed P15-M6 surface。Future 4's explicit production composition now implements/final-gates
`OBJECT_WAL_ASYNC_OBJECT` without changing that legacy boundary；BK-M5 now installs BK_ONLY、BK_ASYNC_OBJECT and
BK_SYNC_OBJECT in production behind exact activation/capability admission and immutable first-create profile binding。
The exact F1-BK target is
`../phase-bk-bookkeeper-primary-wal/README.md`；it is not Future 5 and does not renumber F5–F9。

## 3. Dependency graph

```mermaid
flowchart LR
    F1["F1 Phase 1 Core Storage"] --> P15["Phase 1.5 L0 Foundation"]
    P15 --> F2["F2 ManagedLedger Facade"]
    P15 --> F4["F4 Materialization / Compaction"]
    F1 --> F7["F7 Routing / Elasticity"]
    F2 --> F3["F3 Cursor / Subscription"]
    P15 --> FBK["F1-BK BookKeeper Primary WAL"]
    F2 --> FBK
    F3 --> FBK
    F4 --> FBK
    F2 --> F5["F5 KoP / Kafka"]
    F4 --> F5
    F4 --> F6["F6 Lakehouse"]
    F2 --> F8["F8 Advanced Pulsar"]
    F3 --> F8
    F4 --> F8
    F7 --> F8
    F5 -. shared retention/txn contracts .-> F8
    P15 --> F9["F9 Native Kafka Shared Storage"]
    F4 --> F9
    FBK --> F9
    F7 -. optional placement projection .-> F9
```

这不是所有设计工作的严格串行计划。F2-M0R2 新发现的 P15-M6 cumulative-result handoff 与 F2-M1-M6
production milestones 已完成；Future 2 已 final-gated。Phase 3 的 M0/M0R 已把 cursor/reference/trim
handoff 冻结到代码级，F3-M1 metadata/snapshot foundation 与 F3-M2 CursorStorage/retention state machines
已 final-gated；F3-M3 facade、F3-M4 Pulsar broker integration、F3-M5 real recovery/retention 与 F3-M6
compatibility/incarnation/F4-handoff 已完成并通过对应 gate。F4-M0 已用本地 Nereus/Pulsar
source 完成代码级设计门禁；F4 production 现在可以从 M1 开始消费 F3 的稳定
cursor/reference/trim boundary，但仍必须通过自己的 generation/GC gates 才能对 production
topic 发布或删除 physical bytes。

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

### F1-BK delivery extension (BK-M0–M6 implemented/final-gated)

Code-level target：`../phase-bk-bookkeeper-primary-wal/README.md`。It consumes Phase 1.5 generic target/head/recovery,
F2/F3 logical projection/cursor and final-gated F4 task/generation/retention contracts. BK-M0–M6 implement, in order,
provider-neutral seams、BK_ONLY、BK_ASYNC_OBJECT、BK_SYNC_OBJECT、Pulsar rollout and aggregate compatibility.
BK-M1–M4 execute the three profiles through the module-local runtime；BK-M5 production rollout is final-gated with
exact capability/readiness admission、ownership transfer and mixed-profile compatibility。BK-M6 closes bounded scale、
fresh-runtime response-loss recovery、two-broker/two-worker contention and the complete predecessor aggregate。
No online profile migration exists。

## 5. F2 — ManagedLedger Facade

Detailed design: `nereus-future2-managed-ledger-facade.md`
Code-level design: `../phase-2-managed-ledger-facade/README.md`
Current milestone: F2-M0/M0R/M0R2 + P15-M6 + F2-M1-M6 complete/final-gated；production facade/cursor、generation-safe write-fence handoff、shared-store peer lifecycle、real dual-broker restart/failover and scenarios 1–19 are implemented and gated

### Owns

- storage class `nereus`；
- `ManagedLedgerFactory` and ManagedLedger facade；
- Pulsar entry encoding；
- virtual ledger / Position / MessageId projection；
- topic open/load/unload and compatibility stats。

### Entry gate

F2-M0/M0R/M0R2 closed the facade design gate，and P15-M6 closed the final cumulative-result prerequisite before
F2-M1-M5 consumed these entry contracts in the facade and Pulsar fork；F2-M6 completed the final
failure/lifecycle acceptance matrix：

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
Code-level design: `../phase-3-cursor-subscription/README.md`
Current milestone: F3-M0/M0R design-gated；F3-M1-M6 implemented/final-gated

### Owns

- durable mark-delete；normal dispatch read position stays broker-local；
- individual whole-ack ranges、partial-batch remaining bits and immutable snapshot objects；
- one-root cursor CAS、per-writable-open owner-session claim/fence、generation/tombstone、destructive `ackStateEpoch`
  and failover recovery；
- Exclusive/Failover/Shared durable progress；
- conservative retention floor and recoverable logical-trim barrier。

### Entry/exit gates

- M0 locks the exact local Pulsar master API/member/call paths；
- F2 maps Position to one persisted-entry offset and preserves MessageId batch indexes as in-entry sub-indexes；
- one Oxia root owns cursor correctness；snapshot ref is authoritative only through that root；
- every writable open claims retention first and every ACTIVE cursor root under one fresh owner session before topic
  publication；Pulsar ownership/watch alone is not the cursor fence；
- broker-supplied ownership checker passes before root claim and final publication/first-create callback；it gates
  authority but never replaces durable session/version fencing；
- normal dispatch read position is never durable；restart begins at first unacked；
- reset/clear-backlog advances `ackStateEpoch`，so monotonic ack cannot rebase across destructive replacement；
- new/recreated cursor and backward reset remain in recoverable `PROTECTION_PENDING` through cursor CAS；trim uses
  separate recoverable `TRIM_PENDING`；
- M1-M6 must add deterministic/real two-broker ack、snapshot、restart、trim and compatibility gates；
- Key_Shared/delayed/pending-ack remain F8。

## 7. F4 — Materialization, Compaction and Generation Replacement

Detailed design: `nereus-future4-compaction-generation.md`
Code-level target contract: `../phase-4-compaction-generation/README.md`
Current milestone: F4-M0 source audit/design gate complete；F4-M1 API/metadata/object IO、F4-M2 committed
generation publication/read/pin/fallback and F4-M3 materialization paths implemented/final-gated. M3 includes the real Parquet
writer/strict-reader/full verifier、NTC1 facade、core adapter and deterministic policy/planner/task/recovery/registry
scanner landed. The exact-source claim-to-output-ready worker、task-protection crash-cut recovery、advisory checkpoint
reconciliation、bounded service lifecycle and Pulsar Entry/NCP1 exact-byte round trip checkpoints are also implemented,
as are the topic-compaction SPI/registry、terminal workflow-metadata retirement、COMMITTED-source bootstrap、
tagged-v1 key encoding、shared-budget sorted-spill two-pass engine and isolated NTC1 worker/publication path.
`phase4M3Check` and the real Oxia/LocalStack-backed final gate pass；F4-M4–M6 and checkpoint-BQ have since completed
the GC、async/retention、scale/failure/compatibility and aggregate final gates. BookKeeper primary-WAL work consumes
those final contracts through `../phase-bk-bookkeeper-primary-wal/README.md` rather than extending F4's scope.

### Owns

- async materialization stream discovery/task/checkpoint/lag；
- primary-WAL retention gate；
- compaction planner/worker；
- higher-generation publish and reader selection；
- fallback and GC references；
- topic-compaction primitive。

### Entry gate

- F1 generation-0 index and commitVersion contracts stable；
- Phase 1.5 generic read target/dispatcher and stable-commit split implemented；
- Phase 3 cursor owner/protection/pending-trim/snapshot-reference boundary implemented and final-gated；
- conditional higher-generation publish、reader lease、recovery checkpoint and physical-GC schemas are frozen by F4-M0；
- guarded provider PUT and long-grace DELETED-root/reference/manifest retirement bound physical audit metadata without
  allowing a stale writer to reuse a deleted key；
- registered-stream discovery is 64-shard、restart-safe and hint-only；projection/head/index remain authoritative；
- generation-zero new writes prepare intent, acquire `REACHABLE_APPEND`, then CAS head；strict success additionally
  acquires index-owned `VISIBLE_GENERATION`；
- source ranges and checksums form deterministic task identity。

### Exit gate

- publish changes physical target only；
- repair handles upload/publish/checkpoint partial states；
- lag blocks unsafe primary-WAL GC；
- old generations are retained until all reader/cursor/task/catalog references are safe；
- DELETED-root audit metadata retires only after dual absence/owner/domain proof and late-PUT race gates；
- `OBJECT_WAL_ASYNC_OBJECT` is executable without implying any BookKeeper profile；
- all M1–M6 gates in the code-level contract pass。

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

## 12. F9 — Native Kafka Shared-Storage Integration

Detailed design: `nereus-future9-kafka-native-storage.md`
Code-level target contract: `../phase-9-kafka-native-storage/README.md`

F9-M0 source/design review completed on 2026-07-23；the dated evidence is
`../phase-9-kafka-native-storage/09-f9-m0-design-review-2026-07-23.md`. This closes only the documentation gate；F9 has
no production module、Kafka fork or executable capability yet.

F9 is deliberately separate from F5. F5 projects the Kafka protocol through KoP on the Pulsar facade；F9 integrates
a KRaft Kafka broker fork directly with Nereus as the partition log. The two tracks share logical storage primitives
but do not share coordinator-state ownership or payload mapping.

### Owns

- cluster-wide native Kafka/Nereus activation and RF=1 controller constraints；
- `topicId + partition` to Nereus stream binding and partition lifecycle；
- exact Kafka `RecordBatch` bytes mapped to ranged Nereus entries；
- native Produce/Fetch、LEO/HW/LSO、leader-epoch and recovery integration；
- Kafka producer-state、transaction/time/virtual-segment checkpoints derived from the committed stream；
- native Kafka retention、DeleteRecords and F4-backed log compaction；
- broker admission、failure mapping、observability and rolling-upgrade gates。

### Entry/exit gates

- one Kafka `RecordBatch` is one `AppendEntry` and consumes its complete Kafka offset span；
- public Nereus append/read contracts support expected-start and containing-entry semantics without changing the
  default exact-start behavior consumed by Pulsar；
- append authority can be fenced immediately by a strictly newer KRaft leader epoch；
- Produce success is emitted only after a stable Nereus append result；unknown completion write-fences and recovers
  the partition before any later append；
- Kafka coordinator truth stays in native compacted internal topics；Oxia does not become a second group or
  transaction log；
- NCP2/NTC2 preserve ranged entries and Kafka batch semantics；NCP1/NTC1 bytes are never reinterpreted；
- KRaft metadata is protocol truth，Nereus stream head is logical data truth，and object checkpoints are derived；
- stock Kafka local-log cleaner and local disk are never alternate correctness owners；
- the full code-level scenario matrix passes before the capability can be called implemented。

## 13. Cross-track verification waves

Verification follows architecture dependencies rather than waiting for all tracks：

| Wave | Scope |
| --- | --- |
| V1 | F1 deterministic unit/contract/failure-injection tests |
| V1.5 | Phase 1.5 mixed-metadata、generic target、exact recovery and lifecycle tests |
| V2 | F2/F3 Pulsar facade and cursor compatibility suites |
| V3 | F4 materialization lag、generation、GC and corruption tests |
| V4 | F5/F7 protocol routing/failover compatibility |
| V5 | F6/F8 catalog、advanced semantics、geo/txn integration |
| V6 | F9 native Kafka produce/fetch、recovery、transactions、compaction、retention and multi-broker fencing |

Benchmark、chaos、model checking 和 production profile claims 只能在对应 implementation gate
之后使用；不能用 future design 文本代替证据。

## 14. Document template

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
