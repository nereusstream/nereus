# Nereus Design Index

> 状态：当前设计索引
> 最近一次实现同步：2026-07-11
> 当前交付阶段：Phase 1.5 P15-M0-M5 complete；F2-M0R2 discovered P15-M6 cumulative-result handoff；P15-M6 next，then F2-M1

本文定义文档权威性、当前代码边界和阅读顺序。目标是让 north-star 设计、当前实现合同、
未来能力和历史 review 各自有清晰位置。

## 1. 产品定义

Nereus 是 Pulsar-native shared-storage streaming engine：

```text
Pulsar native protocol and runtime
+ KoP/Kafka protocol projection
+ Oxia metadata and coordination plane
+ selectable primary WAL and object-materialization profiles
+ shared object data plane
+ broker-locality without durable broker ownership
+ compaction and lakehouse projections
```

所有协议和物理布局最终服从同一个逻辑坐标：

```text
streamId + offset
```

“单一坐标”不表示只有一个元数据记录。当前 append truth 由同 stream 的
`StreamHeadRecord` 与其可达的 immutable commit-log chain 共同解释；offset index 是可修复的
读路径物化索引，object store 只保存 bytes。

## 2. 文档权威性

发生冲突时按下列顺序处理：

1. **已实现行为**：生产代码、可执行测试和 durable golden bytes；
2. **当前代码级合同**：`docs/phase-1-core-stream-storage/` 的已实现 L0 合同、
   `docs/phase-1.5-core-storage-foundation/` 的 implemented L0 evolution contract，以及
   `docs/phase-2-managed-ledger-facade/` 的 reviewed F2 design；
3. **已接受决策**：`docs/decisions/`；
4. **总体设计**：本目录中的 architecture、terminology、commit protocol 和 object format；
5. **能力轨道设计**：文件名以 `nereus-futureN-` 开头；
6. **历史材料**：dated review、legacy design 和 Phase 0 文档。

如果 1 与 2 不一致，不能只改其中一边：实现和代码级合同必须在同一变更中对齐。如果未来设计
与当前实现不同，文档必须明确标注 `Designed` 或 `Reserved`，不能写成已经支持。

## 3. 状态词

| 状态 | 含义 |
| --- | --- |
| `Implemented` | 代码和相应测试已经存在 |
| `In progress` | 当前交付轨道，部分里程碑已实现 |
| `Designed` | 设计已记录，尚未成为可执行能力 |
| `Reserved` | API、enum、key 或扩展点已占位，但没有端到端执行路径 |
| `Historical` | 仅用于解释决策演进，不是当前合同 |

不得用“支持”描述 `Designed` 或 `Reserved` 能力。

## 4. 当前代码快照

| 模块/能力 | 状态 | 当前事实 |
| --- | --- | --- |
| `nereus-api` | `Implemented`（P15-M1/M4；P15-M6 delta designed） | generic target/result、append recovery/lifecycle API implemented；`AppendResult.cumulativeSize` handoff pending |
| `nereus-metadata-oxia` | `Implemented`（P15-M2/M4） | legacy/new codecs、generic new-write、mixed-chain repair/replay、lifecycle CAS、fake/real Docker gates |
| `nereus-object-store` | `Implemented`（M3） | object-store API、WAL v1 writer/reader、entry index、local test fixture、checksums/tests |
| `nereus-core` | `Implemented`（P15-M3/M4） | primary-WAL registry/Object adapters、strict split commit/materialize、read dispatch、exact recovery、seal/delete |
| Phase 1.5 foundation | `In progress`（P15-M0-M5 implemented；P15-M6 next） | original generic target/adapter、recovery、seal/delete gates pass；narrow cumulative-result handoff pending |
| BookKeeper primary WAL | `Reserved` | profile enum exists；generic BK location、writer/reader and coordinator do not |
| Async object materialization | `Reserved` | profile/durability names exist；task/checkpoint/materializer/retention gate do not |
| `nereus-managed-ledger` | `In progress`（F2-M0R2 complete） | exact Pulsar API/call-path、code-level method/state/fence review complete；blocked on P15-M6；module marker-only |
| `nereus-pulsar-adapter` | `In progress`（F2-M0R2 complete；gated） | runtime/S3/bootstrap/binding/capability contract complete；implementation waits for F2 milestones；module marker-only |
| `nereus-kop-adapter` | `Designed` | marker module only；F5 payload mapping gate not implemented |
| Compaction、routing、lakehouse、高级语义 | `Designed` | design docs only |

Phase 1 ordinary and final gates are：

```text
./gradlew phase1Check
./gradlew phase1FinalCheck --rerun-tasks
```

`phase1Check` keeps Docker optional；`phase1FinalCheck` includes the production adapter、capability spike and
full core/Oxia/Object-WAL Testcontainers suites. The post-M8 review also gates one-head snapshots、bounded
range iteration、executor isolation、cache/lane lifecycle and the `com.nereusstream` namespace。

Phase 1.5 gates are：

```text
./gradlew phase15Check
./gradlew phase15FinalCheck --rerun-tasks
```

The P15-M5 versions currently pass；P15-M6 must extend the same gates with cumulative-result fixtures before F2-M1。

## 5. 当前一致性决策

总体设计以以下边界为准：

1. 任意成功 append 都必须先完成 primary WAL durability，再通过 stream-head single-key CAS
   获得稳定 offset；`WAL_DURABLE` 不是“只写 WAL、跳过 Oxia”。
2. Stream-head CAS 是逻辑 append 线性化点；reachable commit log 记录完整提交身份。
3. Generation-0 offset index 和版本匹配的 legacy committed-slice / generic committed-append marker 是可修复
   读/replay 索引。
4. `WAL_DURABLE_AND_INDEX_COMMITTED` 额外等待 generation-0 读索引物化确认。
5. Async profile 延后的是 object-backed/read-optimized generation 发布，不是 offset 稳定性。
6. Compaction/materialization 发布 higher generation 只切换物理读目标，不改变已提交 offset。
7. Watch 只是 cache invalidation hint；object list 只是 audit/GC 输入。
8. Exceptional append uses `AppendOutcome` to distinguish `KNOWN_NOT_COMMITTED`、
   `MAY_HAVE_COMMITTED` and `KNOWN_COMMITTED`；`ErrorCode`/message alone is insufficient。
9. Final Phase 1 exit includes completed M7 production Oxia adapter gates plus the M8 full core/Oxia/Object
   WAL restart and failure scenario。
10. Java packages and Maven coordinates use the owned-domain namespace `com.nereusstream`；ADR 0003 owns
    this compatibility decision。
11. F2 `PULSAR_ENTRY_V1` persists one complete Pulsar entry per L0 offset；`batchIndex` is an in-entry sub-index，
    not another stream offset。
12. F2 same-name delete/recreate publishes a new projection incarnation、stream and virtual ledger；a stale
    Position cannot alias the new topic lifetime。
13. Every post-head-request non-known append result carries an in-process `AppendAttemptId`；the facade recovers
    that exact retained physical attempt or write-fences，and does not claim producer dedup across broker crashes。
14. P15-M6 generic `AppendResult` carries the exact cumulative logical size already stored in committed truth；F2
    never derives it from a stale local total plus this append's bytes。
15. Hybrid first-create/delete/new-lifetime selection is serialized by one broker-metadata storage-class binding；an
    existing live topic cannot switch between BookKeeper and Nereus without a future migration protocol。
16. Phase 1.5 design replaces mandatory object-shaped common results/metadata with a tagged `ReadTarget` and uses
    dual-read/new-write metadata；legacy Phase 1 golden bytes remain frozen and one unchanged head may link a mixed
    commit chain。
17. Stable head commit and generation-zero index materialization become separate idempotent L0 operations, while
    Phase 1.5 public success remains strict and continues rejecting `WAL_DURABLE` before IO。
18. F2-required append recovery、seal and logical delete must be implemented in L0 before F2 production code；logical
    delete never implies physical object deletion。

ADR 0004 and its 2026-07-12 addendum own the delivery-order、generic-target compatibility and support-boundary
decision behind items 14 and 16-18。

详细协议见 `nereus-commit-protocol.md`，当前 Phase 1 精确合同见
`../phase-1-core-stream-storage/02-oxia-metadata-and-commit.md`。

## 6. 权威总体设计文档

| 文档 | 角色 | 状态 |
| --- | --- | --- |
| `nereus-overall-architecture.md` | 产品 north star、truth hierarchy、profiles、L0-L4 边界 | current |
| `nereus-terminology.md` | 共享词汇、状态词和禁止歧义 | current |
| `nereus-commit-protocol.md` | append/head CAS、index repair、generation、cursor/txn/catalog commit | current |
| `nereus-storage-object-format.md` | 已实现 WAL v1 与未来 object families 的版本边界 | current |
| `nereus-futures.md` | 能力轨道、依赖 DAG、交付状态 | current |
| `../phase-1.5-core-storage-foundation/README.md` | active L0 evolution、compatibility、milestones and gates | implemented / final-gated |
| `../automq-like-stream-storage/README.md` | async materialization profile 的专门状态机和门禁 | designed/reserved |
| `../decisions/0002-separate-append-commit-index-and-materialization.md` | 分离逻辑提交、读索引物化和 higher generation | accepted ADR |
| `../decisions/0004-insert-phase-1-5-generic-storage-foundation.md` | Phase 1.5 sequencing、dual-read/new-write and F2 gate | accepted ADR |

## 7. 能力轨道文档

`Future N` 是稳定的能力轨道编号，不表示代码仍处于“只设计不实现”的统一阶段。

| 文档 | 能力轨道 | 当前状态 |
| --- | --- | --- |
| `nereus-future1-core-stream-storage.md` | F1 L0 Core StreamStorage | `Implemented`（Phase 1 + Phase 1.5） |
| `nereus-future2-managed-ledger-facade.md` | F2 ManagedLedger facade | `In progress`（F2-M0/M0R/M0R2 complete；P15-M6 next，then F2-M1） |
| `nereus-future3-cursor-subscription.md` | F3 durable cursor/subscription | `Designed` |
| `nereus-future4-compaction-generation.md` | F4 compaction/materialization/generation | `Designed` |
| `nereus-future5-kop-compatibility.md` | F5 KoP/Kafka projection | `Designed` |
| `nereus-future6-lakehouse-sbt-sdt.md` | F6 SBT/SDT | `Designed` |
| `nereus-future7-routing-brownout-elasticity.md` | F7 routing/brown-out/elasticity | `Designed` |
| `nereus-future8-advanced-pulsar-semantics.md` | F8 advanced Pulsar semantics | `Designed` |

## 8. 阅读路线

### 评审当前 Phase 1 代码

1. `nereus-terminology.md`；
2. `nereus-commit-protocol.md` 的 append/read-index 部分；
3. `../phase-1-core-stream-storage/README.md`；
4. 该目录下 `01` 到 `08` 的 active code-level documents；
5. `09` 及 dated reviews 只作为历史/审计材料。

### 实现 Future 2

1. consume the completed `../phase-1.5-core-storage-foundation/README.md` contract；
2. `nereus-future2-managed-ledger-facade.md`；
3. `../phase-2-managed-ledger-facade/README.md`；
4. 该目录的 `01` 到 `07` reviewed code-level documents；
5. `../phase-2-managed-ledger-facade/spikes/PulsarManagedLedgerApiProbe.java` 与锁定的
   Pulsar fork commit；
6. 当前里程碑对应的代码和可执行 gate。

### 评审 Phase 1.5

1. `nereus-future1-core-stream-storage.md`；
2. `../phase-1.5-core-storage-foundation/README.md`；
3. 该目录的 `01` 到 `05` code-level documents；
4. 对照 `../phase-1-core-stream-storage/` 的 frozen legacy contract/goldens；
5. P15-M1-M5 implemented code and passing gates。

### 评审目标架构

1. `nereus-overall-architecture.md`；
2. `nereus-futures.md`；
3. `nereus-storage-object-format.md` 与 `nereus-commit-protocol.md`；
4. 目标能力对应的 future document。

## 9. 文档维护规则

- 每个总体文档开头写明 `Implemented / In progress / Designed / Historical`。
- 代码示例如果不是当前 Java surface，必须标记 `target` 或 `pseudo-code`。
- durable key、record、binary field 以代码级文档和 golden tests 为准。
- 不复制大段 Phase 1 record 定义到总体文档；总体文档链接到代码级合同。
- 不把 review 日期结论当作永久状态；当前状态只在本索引和当前 phase README 维护。
- 架构决策改变时，同时检查 overall、commit protocol、对应 future，以及 active phase 的
  README/编号合同。
- Phase 1.5 implementation touching an existing Phase 1 behavior must update both the successor contract and the
  exact affected Phase 1 document；design-only future types must not be written as current code。
