# Nereus 总体设计文档

本目录描述 Nereus 的目标架构和能力轨道。它不再是从规划工作区复制来的 Phase 0 快照；
自 2026-07-10 起，这里与 `docs/phase-1-core-stream-storage/`、
`docs/phase-1.5-core-storage-foundation/`、
`docs/phase-2-managed-ledger-facade/`、
`docs/phase-3-cursor-subscription/`、
`docs/phase-4-compaction-generation/` 与
`docs/phase-bk-bookkeeper-primary-wal/` 共同构成仓库内设计基线。已实现合同由代码/
测试优先；Phase 4 目录同时记录已通过 M0 的 target contract，以及已 final-gated 的 F4-M1–M6 实现和
checkpoint-BQ aggregate evidence；F1-BK 目录记录当前 BookKeeper primary-WAL 交付。
BK-M0 documentation gate 与 BK-M1 provider-neutral foundation 已于 2026-07-19 complete/final-gated；BK-M2
`BOOKKEEPER_WAL_ONLY` 已实现 keyspace/record/codec、production/fake store、exact allocator/writer/recovery/reader、
whole-ledger retention、module/facade runtime，以及 real Oxia + BookKeeper create-response-loss / late-create / restart /
delete checkpoints and cold all-shard Oxia coverage。BK-M3 已实现 provider-neutral exact-source、durable source protection、async profile/runtime
composition、shared lag、retirement-metadata authority、sealed shared-scanner and first real Oxia/BK/Object
publication/read/retirement、physical ledger deletion and fresh-runtime task/source/output/publication response-loss
plus real-load lag/physically-missing-Object fail-closed checkpoints；BK-M2/M3 aggregate final gates 已通过。
BK-M4 也已实现独立 sync completion policy、exact single-source task reuse、normal-read proof 和
`KNOWN_COMMITTED` restart recovery，并通过普通/final gate；BK-M5 checkpoints A–E.4 已完成并 final-gated
production rollout、admin/retention activation、exact ownership capability filter、readiness-proof rollover、
retry-disabled BK_ONLY 双 broker takeover，以及 BK sync/async/Object mixed-profile MessageId/seek/stock-BK
共存。M5 ordinary gate 105/105、包含全部前驱的 final gate 231/231 均通过。BK-M6 further final-gates
hot/all-shard and maximum-bound scale、fresh-runtime response-loss chaos、mixed-profile/two-worker compatibility and
the whole predecessor chain；its ordinary gate passes 123/123 and the complete delivery final gate passes 236/236。

建议阅读顺序：

1. `nereus-design-index.md`：文档权威性、状态和阅读路线；
2. `nereus-terminology.md`：统一术语及禁止混用的边界；
3. `nereus-overall-architecture.md`：目标架构与已实现边界；
4. `nereus-commit-protocol.md`：append、read-index 和物化发布协议；
5. `nereus-futures.md`：能力轨道、依赖关系和交付顺序；
6. 文件名以 `nereus-futureN-` 开头的文档：各能力轨道详细设计；
7. `../phase-bk-bookkeeper-primary-wal/README.md`：F1-BK BookKeeper primary-WAL 代码级设计与 BK-M0–M6 gate。

Future 1 / Phase 1 和 Phase 1.5 P15-M0-M6 已完成并通过普通/Docker gate；F2-M0 API spike、F2-M0R
和 2026-07-12 F2-M0R2 code-level review 也已完成。M0R2 使用用户提供的 exact Pulsar checkout 关闭了
type collision、binding/state inspection、auto-unfence、ack admission、S3 和 rollout/namespace race 合同。
P15-M6 已把 `AppendResult.cumulativeSize` 从 committed truth 交给 public result，F2-M1 projection、
F2-M2 projection metadata、F2-M3 factory/ledger facade、F2-M4 cursor boundary、F2-M5 broker integration
和 F2-M6 final acceptance 也已实现。真实双 Broker gate 已用 real Oxia、pinned LocalStack Community S3
`4.14.0` 和 stock BookKeeper 验证 ownership failover、进程重启、unload/reload、Position/bytes 稳定与
两类存储共存。Future 2 已 final-gated；Future 3 在该 facade 合同上实现，并把其
cursor/reference/retention 合同作为 Future 4 production 的前置。
2026-07-14 已完成 Phase 3 的 design-only M0/M0R：durable cursor 的 single-root CAS、snapshot bytes、
generation/tombstone、destructive `ackStateEpoch`、local read-position、per-writable-open owner-session claim、
broker ownership guard、retention barrier 和 Pulsar fork method mapping 已冻结。F3-M1 metadata/snapshot
基础代码、golden/contract tests、真实 Oxia 和 LocalStack gate 已完成并 final-gated；F3-M2
CursorStorage/retention state machines、failure injection、并发/property model 与真实 Oxia + LocalStack S3
跨 runtime 恢复门禁也已完成并 final-gated。F3-M3 ManagedCursor facade/runtime/hydration、F3-M4 Pulsar
capability/admission/durable-ack integration、F3-M5 deterministic/10k/two-broker real recovery 及 F3-M6
MessageId/property/reset/limit/rollout/incarnation/F4-handoff gates 均已完成；Phase 3 已 final-gated。
2026-07-14 已完成 Phase 4 F4-M0 本地 Nereus/Pulsar source audit 和代码级设计门禁，冻结
generation publish、object format、task/recovery、reader lease、retention/GC、Object-WAL async 和
Pulsar rollout 合同。2026-07-15 F4-M1 已完成 API/metadata/codecs、guarded object IO、physical
reference values、durable reader pin 与 durable protection handshakes，并增加全分片 metadata store contracts、
conditional delete、43 个 codec golden、stream-scoped CAS guards 及 production/fake 共用 physical-root transition
validation。真实 Oxia gate 验证 slash-aware fixed-depth range scan；pinned LocalStack gate 验证 SDK response/
exact-byte upload completion 与 exact HEAD 后的受限 conditional-delete fallback；`phase4M1Check` 和
`phase4M1FinalCheck --rerun-tasks` 均已于 2026-07-15 通过。F4-M2 也已完成 exact target-reader dispatch、
generation allocation/index compatibility、authoritative scan + durable pin + exact revalidation、same-view
fallback、同对象全引用 quarantine、bounded transient retry 和 restart-safe publication/re-entry state machine。
`phase4M2Check` 与 `phase4M2FinalCheck --rerun-tasks` 已通过；真实 Oxia/LocalStack fixture 覆盖独立 runtime
并发、response loss 后重启、pin/quarantine/fallback。F4-M1–M2 已 final-gated；M3 的真实 compacted
Parquet writer/strict reader、whole-file verifier、NTC1 storage facade、core adapter，以及 deterministic
policy/planner/task-store/recovery/64-shard registry scanner、exact-source reader/claim-to-output-ready worker、
protection owner crash-cut reconciliation、advisory checkpoint reconciliation 与 bounded service lifecycle
checkpoints、Pulsar Entry/NCP1 exact-byte round trip、topic-compaction neutral SPI/registry 与 terminal
workflow-metadata retirement，以及 topic-compaction COMMITTED-source bootstrap、tagged-v1 key encoding、
sorted-spill two-pass engine/worker/publication focused tests 已落地。`phase4M3Check` 与真实
Oxia/LocalStack-backed `phase4M3FinalCheck --rerun-tasks` 已于 2026-07-15 通过；
F4-M3 已 final-gated。F4-M4–M6 也已于 2026-07-19 完成：NRC1 recovery、retirement/GC、generation
registration/readiness/activation、protected async Object-WAL、materialization lag、logical retention、all-shard/
scale/failure cuts、retry-disabled two-broker ownership/MessageId/stock-BookKeeper coexistence，以及 checkpoint-BQ
203/203-task aggregate 均通过。完整细节与 safe-default deletion 边界以 Phase 4 目录为准。
F1-BK 的代码级 contract/evidence 在 `../phase-bk-bookkeeper-primary-wal/README.md`；BK-M0–BK-M6
已完成/final-gated。M1–M4 实现 metadata/store、真实 allocator/writer/recovery/reader、fixed protection/lease、
safe-default whole-ledger retention 和三种 profile；M5 将它们接入 production broker，完成 activation-bound
first-create、ownership capability filter、readiness-proof rollover、双 broker takeover 和 mixed-profile
rollout；M6 closes scale、fresh-runtime process cuts、two-worker contention and aggregate compatibility。在线
profile migration 不在当前交付范围。
Legacy L0 合同以
`../phase-1-core-stream-storage/README.md` 为准；implemented L0 evolution
以 `../phase-1.5-core-storage-foundation/README.md` 为准；F2 合同、里程碑和 gate 以
`../phase-2-managed-ledger-facade/README.md` 及该目录下的编号文档为准。
F3 target contract、M0/M0R 结论和 M1-M6 计划以
`../phase-3-cursor-subscription/README.md` 及该目录下的 `01` 到 `06` 为准。
F4 target contract、M0 结论和 M1-M6 实施计划以
`../phase-4-compaction-generation/README.md` 及该目录下的 `01` 到 `07` 为准。
F1-BK contract、BK-M0–M6 顺序和场景/可执行证据矩阵以
`../phase-bk-bookkeeper-primary-wal/README.md` 及该目录下的 `01` 到 `09` 为准。
