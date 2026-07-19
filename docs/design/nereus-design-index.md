# Nereus Design Index

> 状态：当前设计索引
> 最近一次设计/实现同步：2026-07-19
> 当前交付阶段：Future 2、Future 3 与 Future 4 均 complete/final-gated；Future 4 M3 format、
> planner/worker、protection/checkpoint/service、Pulsar Entry/NCP1 exact-byte round trip、topic-compaction SPI/registry、
> terminal workflow-metadata retirement、COMMITTED-source topic bootstrap、tagged-v1/sorted-spill two-pass engine
> and isolated NTC1 publication have passed deterministic and real Oxia/LocalStack gates；M4 through checkpoint BC
> additionally implements guarded/protected cursor IO、all-shard physical/cursor live-reference backfill and the
> restart-reconstructable cursor-GC execution/runtime boundary plus strict current-writer inventory/missing-root
> registration、proof-driven deleted-stream registration retirement、the metadata-first fixed-delay lifecycle and exact
> broker typed physical-GC configuration、the configured-scope object-store destructive-capability probe and the
> bounded product-owned atomic deletion-activation coordinator、provider/Pulsar sequencing、exact-scope restart
> fencing and one shared ownerless-reference interpretation；the first real Oxia/LocalStack activation/wrong-scope/
> empty-list/lost-DELETE-response restart slice、post-DELETE/pre-DELETED-root-CAS independent recovery and applied-
> DELETED-CAS response-loss exact reload without repeated DELETE、two-worker shared-intent convergence、all-shard
> mixed-state recovery、fresh-process 1,001-root hot-shard pagination over 1,256 real Oxia roots、10,000-DELETED-root
> dual-window/audit/root-last bounded retirement with remove-on-cancel deadline schedulers and exact 10,000-cursor-root
> live/old/CAS-lost/deleted-cursor classification/deletion and fresh-runtime journaled source/protection post-delete
> cut recovery with an applied-protection-delete lost response、guarded Object-WAL first/retry transmission fencing、
> tombstone reference/manifest late-PUT cuts and post-root external reappearance through a fresh ownerless grace also pass；
> deletion-active readiness rollover now refreshes all proofs and the scope digest in one CAS without a partial epoch；
> the retry-disabled real two-broker gate deletes generation-zero sources and preserves compacted reads、exact
> MessageIds、unload/failover/restart/reverse takeover and stock BookKeeper coexistence；
> M5 checkpoints X–AF close durable registration/readiness/activation、protected async Object-WAL acknowledgement、
> pre-I/O lag admission and coupled production read-repair/materialization composition；checkpoint AG adds stable
> source-verified retention planning and ownership-safe F3 logical-trim delegation；M6 checkpoints BD–BQ close the
> scale/failure/compatibility matrix and the clean 203/203-task aggregate
> 下一项底层交付 F1-BK 已形成 `docs/phase-bk-bookkeeper-primary-wal/` 代码级 target，BK-M0 与 BK-M1
> 已于 2026-07-19 complete/final-gated；BK-M2 `BOOKKEEPER_WAL_ONLY` 已推进到 module/facade runtime 和
> real Oxia + BookKeeper create-response-loss / late-create / restart / delete checkpoints plus cold all-shard Oxia
> evidence，remaining
> matrix/aggregate pending；BK-M3 provider-neutral source/protection/profile/lag、retirement-metadata authority、
> sealed shared-scanner and first real Oxia/BK/Object publication/read/retirement-proof checkpoints 已实现，
> response-loss/physical-release final gate 与 BK-M4–M6 尚未实现，
> production broker 的 BookKeeper primary profiles 仍为 pre-IO rejected。

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
   `docs/phase-2-managed-ledger-facade/` 的 implemented F2 contract，以及
   `docs/phase-3-cursor-subscription/` 的 implemented/final-gated F3 contract；
3. **当前 Phase 4 代码级合同**：`docs/phase-4-compaction-generation/` 的 F4-M0 target、已 final-gated 的
   F4-M1–M6 implementation checkpoints 与 checkpoint-BQ aggregate evidence；它优先于 F4 north-star 摘要，但已实现部分仍以
   代码/测试为最高权威；
4. **当前 F1-BK 代码级 target + BK-M1 evidence**：`docs/phase-bk-bookkeeper-primary-wal/` 的 BK-M0–M6
   implementation contract；BK-M1 foundation complete/final-gated，但不能把 foundation 解释为 profile 可执行；
5. **已接受决策**：`docs/decisions/`；
6. **总体设计**：本目录中的 architecture、terminology、commit protocol 和 object format；
7. **能力轨道设计**：文件名以 `nereus-futureN-` 开头；
8. **历史材料**：dated review、legacy design 和 Phase 0 文档。

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
| `nereus-api` | `Implemented`（P15-M1/M4/M6 + F4-M1–M2） | generic target/result、exact cumulative append snapshot、append recovery/lifecycle API plus F4 view/generation/publication/object-hash values and content-aware inline entry-index identity |
| `nereus-metadata-oxia` | `Implemented`（P15/F2/F3 + F4-M1–M6 final-gated） | existing metadata plus F4 keys/records/codecs/CAS/scans/all-shard fixtures、generation activation/registration、dual-reader task schema V2、1,000-reference and exact 16,448-stream/64-shard registry pagination evidence, plus explicit versioned/semantic `StreamMetadataSnapshot` authority comparisons；hydrated trim observation fields are never correctness authority |
| `nereus-object-store` | `Implemented`（L0 M3 + F4-M1–M6 final-gated） | WAL v1 IO、NCP1/NTC1/NRC1、replayable staging、guarded/capability-proven provider operations、M5 async Object-WAL final evidence and resolved-target/measured-physical/logical-returned accounting for compressed BI reads；BookKeeper higher-generation profiles remain reserved |
| `nereus-core` | `Implemented`（P15 + F4-M1–M6 final-gated） | stable L0 core plus F4 resolver/pin/fallback、exact NCP1 adapter、durable protections/reference domains、protected async Object-WAL acknowledgement/repair and compression-safe higher-generation read metrics；M4 runtime GC remains exact-activation gated |
| `nereus-materialization` | `Implemented`（F4-M1–M6 final-gated） | M1–M3 planner/worker/publication/topic engine、M4 NRC1/recovery/retirement/physical+cursor GC/scale/failure matrix、M5 coupled async read-repair/materialization/lag runtime and M6 32-ref/4,096-candidate/million-entry/128-source task、16,448-stream registry、two-broker/two-worker contention and abandoned protected-intent retirement boundaries are implemented；the current broker safe defaults keep scheduling/deletion disabled |
| Phase 1.5 foundation | `Implemented`（P15-M0-M6 final-gated） | generic target/adapter、recovery、seal/delete and cumulative-result handoff pass ordinary/Docker gates |
| BookKeeper primary WAL | `BK-M2 implementation in progress`（BK-M0/BK-M1 complete） | independent module、generic read/append/accounting/commit/protection/gen0、BookKeeper 4.18 exact allocator/writer/recovery/reader、bounded uncertain-allocation reconciliation、production/fake Oxia stores、whole-ledger retention and module/facade BK_ONLY execution implemented；real Oxia + BookKeeper exact create-response-loss、matching late-create with permanent GC veto、all-shard cold scan、rollover/cold-restart/lost-delete-response/fresh-process dual-absence checkpoints pass，remaining matrix/aggregate pending；production broker admission remains BK-M5 fail-closed |
| Async object materialization | `Implemented / final-gated`（F4-M5 profile + F4-M6 aggregate） | Object-WAL `WAL_DURABLE` boundary、generation-zero repair、pre-I/O proof/lag admission、coupled production read-repair/materialization runtime and exact Pulsar logical-retention rollout pass the retry-disabled real two-broker and Phase 4 aggregate gates；BookKeeper async/sync profiles remain reserved |
| `nereus-managed-ledger` | `Implemented`（F2-M1-M4 + F3-M1-M6 + F4-M1-M6） | F2 ledger facade/cursor boundary plus F3 state machines、F4 projection/cursor reference domains、strict NPR1 authority、restart-reconstructable cursor candidates、AL ownerless snapshot-key inverse、AM exact cursor/retention retirement authority、durable registration/proof/activation、AR exact-scope deletion guard and factory/runtime activation surface、AS shared projection/global-scope interpretation、AT durable DELETING restart evidence、BC bounded atomic readiness-rollover handoff、pre-I/O async admission、versioned stable retention planner/F3 trim service、shared bounded lane、durable backlog accounting、per-ledger facade and registration-backed policy admission are implemented/tested；safe defaults do not activate physical deletion |
| `nereus-pulsar-adapter` | `Implemented`（F2 + F3 + F4 complete/final-gated） | typed runtime/S3 provider plus fork binding/admission/capability/policy/admin compatibility、durable generation registration/readiness/activation、checkpoint-AF coupled Object-WAL composition、checkpoint-AH retention runtime/config mapping、checkpoint-AI exact policy/admin admission、checkpoint-AN metadata-first root/registration/inventory lifecycle、checkpoint-AO broker physical-GC config mapping、checkpoint-AQ ordered proof/atomic delete activation、checkpoint-AR provider/Pulsar/restart-scope composition、checkpoint-AS exact shared reference-domain assembly、checkpoint-AT real post-DELETE independent recovery、checkpoint-AU applied-DELETED-CAS response-loss exact reload、checkpoint-AV two-worker shared-intent convergence、checkpoint-AW all-256-shard mixed-state recovery/opaque LIST progress、checkpoint-AX real-Oxia hot-shard bounded pagination、checkpoint-AY remove-on-cancel shared runtime scheduler、checkpoint-AZ 10,000-cursor-root exact GC、checkpoint-BA source/protection post-delete recovery、checkpoint-BB real post-root external-reappearance inventory/GC、checkpoint-BC atomic deletion-active readiness rollover and checkpoint-BI real two-broker process-wide worker contention/exact read coexistence are implemented/tested；safe defaults keep destructive execution disabled |
| `nereus-kop-adapter` | `Designed` | marker module only；F5 payload mapping gate not implemented |
| Future 3 cursor/subscription | `Implemented / final-gated`（F3-M0-M6） | M1 metadata/snapshot、M2 durable cursor/retention state machines、M3 ManagedCursor facade、M4 Pulsar capability/admission/durable-ack integration、M5 recovery/retention/scale and M6 compatibility/incarnation/F4 handoff pass their gates |
| Future 4 materialization/compaction | `Implemented / final-gated`（F4-M1–M6） | M4 NRC1/recovery、retirement/GC fences、activation/global domains、scale/failure/late-PUT cuts and retry-disabled real source-deletion acceptance are implemented/tested；M5 final-gates async/retention compatibility；M6 covers 32-ref merge、4,096/4,097 candidates、million-entry NRC1、1,000+1,000 references、the 128-source/1,048,576-record task with schema V2/capability V2、exact 16,448-stream registry restart、real two-broker/two-worker compressed exact-read convergence、protected-intent retirement、partitioned admin routes、provider-neutral Hadoop/Oxia logging composition、bounded Docker release scheduling、exclusive and fresh locked-Pulsar checkout builds、inherited cursor TTL expiry-monitor convergence and executable 52/52 traceability；checkpoint BQ passes the BP-source-lock aggregate with 203/203 tasks executed |
| Routing、lakehouse、高级语义 | `Designed` | design docs only |

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

Both gates passed again for P15-M6 on 2026-07-12, including normal and later-head recovery cumulative-result
fixtures。They are now the executable F2-M1 entry gate。

Phase 3 M1 foundation gates are：

```text
./gradlew phase3M1Check
./gradlew phase3M1FinalCheck --rerun-tasks
```

Both passed on 2026-07-14 against the locked local Pulsar source，real Oxia and pinned LocalStack。They prove only
the metadata/snapshot primitive。

Phase 3 M2 state-machine gates are：

```text
./gradlew phase3M2Check --rerun-tasks
./gradlew phase3M2FinalCheck --rerun-tasks
```

Both passed on 2026-07-14。The final gate combines real Oxia cursor/projection CAS with pinned LocalStack S3 snapshot
bytes across independent owner takeover and runtime restart。

Phase 3 M3/M4 ordinary gates are：

```text
./gradlew phase3M3Check
./gradlew phase3M4Check
```

Both passed on 2026-07-14。M3 proves the complete ManagedCursor facade/runtime/hydration boundary；M4 locks the clean
Pulsar fork at `master@12edc9381c147ceec8bedd530acb5be7db339707` and proves cursor capability、typed config/context、
topic/ack/admin admission、durable ack completion ordering and subscription recovery through eight focused broker
suites plus spotless。

Phase 3 M5 gates are：

```text
./gradlew phase3M5Check
./gradlew phase3M5FinalCheck --rerun-tasks
```

Both passed on 2026-07-14。The ordinary gate locks the clean Pulsar fork at
`master@a2bad4cfa260cc4575ae759f8a345ce969c8ec3a` and adds the exact 10,000-root scale/admission gate；the final
gate composes real Oxia、pinned LocalStack and the two-broker recovery suite covering stable MessageId、ack holes、
partial batches、TTL/subscription expiration and stock BookKeeper coexistence。The hash remains historical M5 evidence。

Phase 3 M6 and aggregate gates are：

```text
./gradlew phase3M6Check
./gradlew phase3M6FinalCheck --rerun-tasks
./gradlew phase3Check
./gradlew phase3FinalCheck --rerun-tasks
```

They passed against the F3-M6 locked historical Pulsar fork
`master@ff6e4fdfc03ffd8535ab2ece58d247dd1c64e8b4`。M6 covers exact ordinary/middle-batch MessageIds across history/
seek/unload/failover/restart、cursor internal properties、reset/limit/rollout/incarnation boundaries、read-only F4
snapshot inventory、callback rejection and loaded/unloaded/namespace admin-route audit。The aggregate final task also
reruns Phase 1、1.5 and 2 final gates。Future 3 is `Implemented / final-gated`。

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
19. F3 durable cursor correctness uses one Oxia root per cursor generation；normal dispatch read position is local
    and cannot be restored after broker failure。
20. F3 partial batch state stores Pulsar remaining bits and merges with AND；ack state is snapshotted, never
    truncated。
21. F3 reset/clear-backlog advances a durable `ackStateEpoch`；a non-subsumed monotonic ack may rebase only while that
    epoch is unchanged。
22. F3 new/recreated cursor and backward reset remain in a recoverable stream `PROTECTION_PENDING` lifecycle through
    cursor CAS，lowering the floor when needed；logical trim uses separate `TRIM_PENDING` with exact persisted
    offset/attempt/composed reason，while F4 owns physical GC。
23. Before first F3 durable cursor，the topic projection is monotonically activated with internal
    `nereus.cursor-protocol=1`；the locked F2 decoder rejects that reserved property and therefore fails before an
    empty cursor view can be exposed。
24. Every F3 writable ledger open creates one fresh owner session，claims retention first and every ACTIVE cursor root
    before topic publication；Pulsar ownership/watch alone cannot fence a crash-delayed old cursor CAS。
25. The broker-supplied ManagedLedger ownership checker is required before owner claim and final ledger/first-create
    publication；it is an admission guard，while the durable root session/version remains the CAS fence。

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
| `../phase-2-managed-ledger-facade/README.md` | F2 facade code-level contract and final gates | implemented / final-gated |
| `../phase-3-cursor-subscription/README.md` | F3 API/metadata/wire/state-machine/implementation plan | implemented / final-gated（M0/M0R + M1-M6） |
| `../phase-4-compaction-generation/README.md` | F4 API/metadata/object/state-machine/rollout/implementation target contract | implemented / final-gated（F4-M1–M6 + checkpoint BQ） |
| `../phase-bk-bookkeeper-primary-wal/README.md` | F1-BK writer/reader/ledger lifecycle/retention/profile rollout code-level target | BK-M1 complete/final-gated；BK-M2 through real Oxia/BookKeeper restart/delete checkpoint；BK-M3 through first real Oxia/BK/Object publication/read/retirement-proof checkpoint；BK-M4–M6 not implemented |
| `../automq-like-stream-storage/README.md` | async materialization profile 的专门状态机和门禁 | implemented / final-gated（F4-M5 profile + F4-M6 aggregate） |
| `../decisions/0002-separate-append-commit-index-and-materialization.md` | 分离逻辑提交、读索引物化和 higher generation | accepted ADR |
| `../decisions/0004-insert-phase-1-5-generic-storage-foundation.md` | Phase 1.5 sequencing、dual-read/new-write and F2 gate | accepted ADR |

## 7. 能力轨道文档

`Future N` 是稳定的能力轨道编号，不表示代码仍处于“只设计不实现”的统一阶段。

| 文档 | 能力轨道 | 当前状态 |
| --- | --- | --- |
| `nereus-future1-core-stream-storage.md` | F1 L0 Core StreamStorage | `Implemented`（Phase 1 + Phase 1.5） |
| `nereus-future2-managed-ledger-facade.md` | F2 ManagedLedger facade | `Implemented`（F2-M0/M0R/M0R2 + P15-M6 + F2-M1-M6 final-gated） |
| `nereus-future3-cursor-subscription.md` | F3 durable cursor/subscription | `Implemented / final-gated`（M0/M0R + M1-M6） |
| `nereus-future4-compaction-generation.md` | F4 compaction/materialization/generation | `Implemented / final-gated`（F4-M1–M6 + checkpoint BQ）；精确合同见 `../phase-4-compaction-generation/` |
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

### 评审已实现 Future 3

1. `nereus-future3-cursor-subscription.md`；
2. `../phase-3-cursor-subscription/README.md`；
3. 依次评审该目录的 `01` 到 `06` code-level documents；
4. F3-M6 历史验收使用本地 Pulsar `master@ff6e4fdfc03ffd8535ab2ece58d247dd1c64e8b4`；当前 maintenance/source lock 已推进到 `master@41d1cddb9d29451884002b96de2bc52367cbb8ca`。Phase 4 BQ historical acceptance lock 仍是 `eaf7b9a704890a9265c21f30d9f351e02d00c600`；M0 历史 API/blob audit 仍固定在 `7efae25af39a15407c1397d9e1f4ac4658d09daa`，M4 历史证据固定在 `12edc9381c147ceec8bedd530acb5be7db339707`，M5 历史证据固定在 `a2bad4cfa260cc4575ae759f8a345ce969c8ec3a`；
5. 执行 `phase3Check` 和 `phase3FinalCheck --rerun-tasks`；
6. 后续 F4/F5/F8 必须消费 F3 已冻结的 cursor/reference/MessageId contract，不得另建 correctness owner。

### 实现/评审 Phase 4

1. 确认 `../phase-1.5-core-storage-foundation/README.md`、
   `../phase-2-managed-ledger-facade/README.md` 与 `../phase-3-cursor-subscription/README.md` 的已实现输入合同；
2. 阅读 `nereus-future4-compaction-generation.md` 的 north-star 边界；
3. 以 `../phase-4-compaction-generation/README.md` 为入口，依次评审 `01` 到 `08` 代码级文档；
4. 实现必须按 `07-implementation-plan-and-gates.md` 的 M1–M6 顺序和 mandatory review stops 推进；
5. 重新审计时使用本地 Pulsar
   `master@41d1cddb9d29451884002b96de2bc52367cbb8ca`，不把未发布的 Maven snapshot 当作权威源；checkpoint BQ
   的 Nereus acceptance code lock 是 `main@fa533a934c33f5bcc4fda328c4df64cb96c6b485`；
6. F4-M6/BQ 之前不得将 broker destructive-GC activation 或最终兼容路径写成 Implemented；BQ 之后可以
   声明 Phase 4 final-gated，但仍必须写明 safe delete defaults 和 BookKeeper primary profiles 的保留边界。

### 评审 Phase 1.5

1. `nereus-future1-core-stream-storage.md`；
2. `../phase-1.5-core-storage-foundation/README.md`；
3. 该目录的 `01` 到 `05` code-level documents；
4. 对照 `../phase-1-core-stream-storage/` 的 frozen legacy contract/goldens；
5. P15-M1-M6 implemented code and passing gates。

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
