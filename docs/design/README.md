# Nereus 总体设计文档

本目录描述 Nereus 的目标架构和能力轨道。它不再是从规划工作区复制来的 Phase 0 快照；
自 2026-07-10 起，这里与 `docs/phase-1-core-stream-storage/`、
`docs/phase-1.5-core-storage-foundation/`、
`docs/phase-2-managed-ledger-facade/`、
`docs/phase-3-cursor-subscription/` 与
`docs/phase-4-compaction-generation/` 共同构成仓库内设计基线。前四者的已实现合同由代码/
测试优先；Phase 4 目录同时记录已通过 M0 的 target contract、已 final-gated 的 F4-M1–M2 实现、正在进行的
M3 format/planner/recovery/exact-source worker checkpoints 和后续目标。

建议阅读顺序：

1. `nereus-design-index.md`：文档权威性、状态和阅读路线；
2. `nereus-terminology.md`：统一术语及禁止混用的边界；
3. `nereus-overall-architecture.md`：目标架构与已实现边界；
4. `nereus-commit-protocol.md`：append、read-index 和物化发布协议；
5. `nereus-futures.md`：能力轨道、依赖关系和交付顺序；
6. 文件名以 `nereus-futureN-` 开头的文档：各能力轨道详细设计。

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
policy/planner/task-store/recovery/64-shard registry scanner、exact-source reader/claim-to-output-ready worker 与
protection owner crash-cut reconciliation checkpoints 已落地；Pulsar opaque round trip、service 与 M3 gates 仍 pending，
retention/GC 和 async profile 仍不可用。
Legacy L0 合同以
`../phase-1-core-stream-storage/README.md` 为准；implemented L0 evolution
以 `../phase-1.5-core-storage-foundation/README.md` 为准；F2 合同、里程碑和 gate 以
`../phase-2-managed-ledger-facade/README.md` 及该目录下的编号文档为准。
F3 target contract、M0/M0R 结论和 M1-M6 计划以
`../phase-3-cursor-subscription/README.md` 及该目录下的 `01` 到 `06` 为准。
F4 target contract、M0 结论和 M1-M6 实施计划以
`../phase-4-compaction-generation/README.md` 及该目录下的 `01` 到 `07` 为准。
