# Phase 3 Cursor / Subscription Detailed Design

> 状态：Implemented / final-gated；F3-M0 / F3-M0R design gates 和 F3-M1-M6 implementation gates complete
>
> Gate 日期：2026-07-14
>
> 最近兼容性回归同步：2026-07-19；当前 Pulsar maintenance/source lock
> `master@41d1cddb9d29451884002b96de2bc52367cbb8ca`

本目录是 Future 3 的代码级实现合同。它把 Future 2 已稳定的
`managedLedgerName -> streamId -> virtualLedgerId -> offset` 投影扩展为 durable
cursor/subscription progress，但不改变 Future 1 的 append、commit、fencing、visibility 或 trim truth。

F3 的最终 ownership 是：

```text
Pulsar ManagedCursor API
  -> NereusManagedCursor（API/回调/本地 read position）
  -> CursorStorage（ack/reset/property state machine）
  -> CursorMetadataStore（单 key CAS correctness root）
  -> CursorRetentionCoordinator（writable owner session + protection/trim barrier）
  -> CursorSnapshotStore（不可变、被 root 引用的大状态）
  -> StreamStorage（只读 committed bytes/head；trim 仍是 L0 operation）
```

## 1. Locked Inputs

| Input | F3 lock |
| --- | --- |
| Nereus baseline | `nereusstream/nereus@623662d9796af1bf2ff929f41df1a8c946a02279`；F2 final-gated |
| Pulsar fork checkout | `/Users/liusinan/apps/ideaproject/nereusstream/pulsar` |
| Pulsar M0 source/API audit baseline | local `master@7efae25af39a15407c1397d9e1f4ac4658d09daa`；historical blob/member evidence remains pinned |
| Pulsar F3 final implementation/source lock | local `master@ff6e4fdfc03ffd8535ab2ece58d247dd1c64e8b4`；historical F3-M6 MessageId/property/incarnation gate、unloaded binding-aware admin validation and M5 recovery evidence |
| Current compatibility source lock | local `master@41d1cddb9d29451884002b96de2bc52367cbb8ca`；preserves the F3 contract and adds only the F1-BK borrowed-client boundary above the BQ lock；the TTL compatibility evidence itself remains pinned to BQ |
| Pulsar version interpretation | checkout 中的 `5.0.0-M1-SNAPSHOT` 只是本地 master 的 source-project selector，不是已发布的 M1 snapshot |
| Executable Nereus profile inherited from F2 | `OBJECT_WAL_SYNC_OBJECT` only |
| Coordinate contract | one Pulsar Entry = one Nereus stream offset；`Position.entryId == offset` |
| Metadata primitive | Oxia 单 key version-CAS、range scan、watch；不假设 multi-key transaction |
| Snapshot primitive | immutable `ObjectStore.put/read/head`；当前没有 delete API |

升级任一锁定 commit 后，必须重新执行文档 01 的 source/member/call-path audit；不能只依赖 Java
编译是否成功。

## 2. M0 / M0R Outcome

### F3-M0：PASS

M0 已完成以下只读验证：

- 锁定 `ManagedCursor`、`ManagedLedger`、callbacks、ack-set helper 和 broker 调用路径的源码 blob；
- 对本地 master 已编译 class 执行 `javap -public`，确认接口 member surface；
- 核对 `PersistentTopic.initialize/createPersistentSubscriptions`、durable cursor open/delete、
  `PersistentSubscription.acknowledgeMessageAsync`、reset/clear/skip、analyze-backlog/duplicate-cursor 和 dispatcher
  read/replay 路径；
- 锁定可执行常量 `ManagedCursor.CURSOR_INTERNAL_PROPERTY_PREFIX == "#pulsar.internal."`，不采用其附近过时
  Javadoc 的拼写；
- 核对 F2 `PositionProjection`、durable-boundary cursor、runtime/bootstrap、metadata CAS 与 ObjectStore
  能力，确认 F3 不需要修改 L0 append API；
- 识别 stock recovery 不持久化普通 dispatch `readPosition`，而是从 durable ack truth 重建下一读取点。

### F3-M0R：PASS

M0R 已关闭会锁死 F4 或破坏 Pulsar 兼容性的选择：

1. 每个 durable cursor 只有一个 Oxia `CursorStateRecord` correctness root；ack、position properties、
   cursor properties、snapshot ref、generation 和 tombstone 都由一次 CAS 串行化。
2. durable state 不保存普通 dispatch `readPosition`。它是 broker-local hint；重启从第一个未 ack
   entry 重建，允许 redelivery，禁止跳过未 ack 数据。
3. delete/recreate 保留 tombstone 并递增 `cursorGeneration`，旧 broker 不能写入新一代 cursor。
4. whole-entry ack 使用规范化 half-open ranges；partial batch ack 使用 Pulsar remaining-bit semantics，
   合并是 bitwise AND；绝不把 batch index 伪装成 stream sub-offset。
5. 大 ack state 使用 immutable full snapshot + root 内 bounded delta；Oxia root 始终是可见性 owner。
6. metadata/snapshot 超限时 mutation 失败，绝不静默截断 ack truth。
7. 每代 cursor 的 `ackStateEpoch` 从 1 开始；reset/clear-backlog 的 destructive replacement 才递增它。
   Monotonic ack 只可在 epoch 不变时 CAS rebase，不能跨 reset 重新制造已被清除的 ack。
8. 每个 writable ledger open 生成新的 `ownerSessionId`，先 claim retention root，再 claim/stabilize 全部
   ACTIVE cursor roots，之后才允许 broker 初始化/dispatch；对已存在 root 的旧 broker CAS 要么在线性化上
   早于该 root 的 claim 并被读到，要么因 version/session 改变而 fenced。若旧 owner 已在 retention claim
   前持久化 create/recreate 的 `PROTECTION_PENDING`，其尚未存在/未 claim 的目标 cursor CAS 可以与接管恢复
   竞争，但不能用旧 retention version finalize 或成功回调；新 owner 必须在发布前恢复并 claim 赢家。
   即使尚未激活 cursor protocol、没有 cursor，writable open 也会
   create/claim 一个 ACTIVE/no-pending owner-only root；它只负责 fencing，不替代首次 durable cursor 前的
   projection activation marker。Broker checked async open 提供的 ownership supplier 必须在 claim 前和最终
   发布/首次 cursor callback 前通过；它不替代 durable root fence。
9. 每次 new/recreated cursor 和 backward reset 先把 stream `CursorRetentionRecord` 从 ACTIVE CAS 为可恢复
   `PROTECTION_PENDING`（必要时降低 protection floor），cursor root 写入同一 attempt ID 后再 finalize ACTIVE；
   floor raise/trim 在整个窗口内被冻结。Logical trim 另用 `TRIM_PENDING`，因此 F4 加 GC 后也不会出现
   post-barrier missing-create/backward-reset race；pending root 还保存 exact composed L0 reason，crash recovery
   不会猜测或改变 trim audit identity。
10. F3 支持 durable/non-durable Exclusive、Failover、Shared；Key_Shared、transaction pending ack、
   replicated subscription、read-compacted、delayed delivery 继续 fail closed。
11. projection 只解释 F2/L0 已提交事实；它不拥有 append、fencing、visibility 或 physical GC correctness。
12. rollout 使用独立 capability `nereus.cursor-protocol=1`；未全量具备能力时不得创建首个 F3
    durable cursor；首个创建前还必须 CAS 同名 topic projection 的保留 activation marker，使锁定的 F2
    decoder fail closed，而不会把 F3 cursor 当成 missing。Marker-aware decoder、M3 writable runtime 与 M4
    broker admission 必须属于同一个可部署 release；M1/M2 只是 repository/test milestones。

以上选择允许 F4 在不改变 cursor/MessageId 兼容模型的情况下加入 materialization、retention policy、
reference tracking 和 physical GC。M0R 没有遗留 implementation-blocking protocol question。

## 3. Scope

F3 实现范围：

- durable cursor create/open/enumerate/delete/recreate；
- cumulative/individual/batch-index ack；
- Exclusive、Failover、Shared durable progress；
- seek、rewind、durable reset、clear backlog、skip；
- cursor properties 与 mark-delete position properties；
- broker unload/restart/failover 后的 cursor hydration；
- immutable cursor snapshot 及 orphan-observation handoff；
- conservative cursor retention floor 与 L0 trim permit；
- broker ack/admin admission、callback completion 和 capability rollout。

明确不属于 F3：

- 改变 F1 stream append/commit/fencing/visibility；
- BookKeeper primary WAL profile；
- F4 materialization/compaction、policy-driven retention 和 physical object deletion；
- Key_Shared hash ownership、transaction pending ack、replicated subscription、delayed delivery；
- KoP group offset、Lakehouse table metadata、geo-replication offset translation。

## 4. Authoritative Decisions

当本目录与早期 north-star 文档出现冲突时，以本目录为实现合同。最重要的术语和公式如下：

```text
markDeleteOffset = first offset not cumulatively acknowledged
ManagedCursor.getMarkDeletedPosition() = project(markDeleteOffset - 1)
localReadOffset = next broker-local dispatch candidate; never durable
wholeAckRanges = sorted, disjoint, non-adjacent half-open ranges above markDeleteOffset
partialBatchAck[offset] = long[] whose set bits are still unacknowledged
effectiveAckState = fullSnapshot + inline deltas, normalized against current markDeleteOffset
```

`CursorStateRecord` 是唯一 durable cursor authority；snapshot bytes 只有被该 record 引用后才可见。
`CursorRetentionRecord` 是 stream trim 协调 authority，但不改变 cursor ack truth，也不自行执行 F4 GC。
F3 中 `trimConsumedLedgersInBackground` 对 Broker 周期 housekeeping 仍是 no-op；admin `TRIM_TOPIC` 在调用它
之前明确拒绝，不能返回误导性的 no-op success。只有 F4 才能放开该操作并把 policy/admin trim 接到
retention coordinator。

Phase 4 F4-M0 已在
[`../phase-4-compaction-generation/`](../phase-4-compaction-generation/README.md) 把上述 handoff 消费为
代码级 target contract；F3 的当前已实现行为仍保持 no-op/reject，直到对应 F4 里程碑的生产
代码和 rollout gate 完成。

## 5. Document Map

| Document | Purpose |
| --- | --- |
| [01-pulsar-api-and-call-path-audit.md](01-pulsar-api-and-call-path-audit.md) | M0 source lock、exact API、broker call path、repository boundary |
| [02-cursor-domain-and-api-contract.md](02-cursor-domain-and-api-contract.md) | Java domain/API、ManagedCursor method semantics、callbacks、errors、limits |
| [03-oxia-metadata-and-snapshot-format.md](03-oxia-metadata-and-snapshot-format.md) | keyspace、single-key CAS record、binary codec、snapshot bytes、strict validation |
| [04-state-machines-recovery-and-retention.md](04-state-machines-recovery-and-retention.md) | create/ack/reset/delete/snapshot/trim state machine 与 crash recovery |
| [05-facade-broker-and-future-compatibility.md](05-facade-broker-and-future-compatibility.md) | Pulsar facade/fork changes、rollout、F4/F5/F8 boundary |
| [06-implementation-plan-and-gates.md](06-implementation-plan-and-gates.md) | M1-M6 file/test plan、M0/M0R decision matrix、final gates |

## 6. Planned Milestones

| Milestone | Deliverable | Status |
| --- | --- | --- |
| F3-M0 | locked local Pulsar API/source/call-path audit | complete, design-only |
| F3-M0R | code-level protocol and narrow Future 4 compatibility gate | complete, design-only |
| F3-M1 | metadata records/codecs/store + snapshot codec/store | complete/final-gated；真实 Oxia 与 LocalStack conditional-create/restart 证据通过 |
| F3-M2 | CursorStorage ack/reset/property/retention state machines | complete/final-gated；deterministic failure/concurrency models 与真实 Oxia + LocalStack S3 跨 runtime 恢复通过 |
| F3-M3 | durable `NereusManagedCursor` and ledger hydration/enumeration | complete/gated；runtime/provider cursor resources、checked/trusted ownership guard、writable claim/hydration publication boundary、storage-backed exact-name durable open、dual-mode ack/read facade、tombstone delete/recreate、registry/admission/async close drain、完整 public API classification 与全部 12 个计划专项 suites 已实现；`phase3M3Check` 通过 locked Pulsar source/API、managed-ledger 与 adapter gates |
| F3-M4 | Pulsar fork durable subscribe/ack/admin/capability integration | complete/gated；独立 cursor capability、two-stable-snapshot activation、canonical typed cursor config/context、F3 topic/ack/admin admission、durable ack completion ordering、hydrated subscription recreation 与 8 个 fork focused suites 已实现；`phase3M4Check` 通过 exact clean fork source lock、M1-M3 chain 与 spotless/test gates |
| F3-M5 | two-broker/Oxia/ObjectStore recovery and retention barrier gates | complete/final-gated；all 16 required scenarios are covered by deterministic failure models、the exact 10,000-root scale fixture and the real two-broker acceptance gate |
| F3-M6 | compatibility/failure/final gate and F4 handoff | complete/final-gated；stable ordinary/middle-batch MessageIds、reset/property/limit/rollout/incarnation contracts、read-only F4 snapshot inventory、code-level production/test inventory、loaded/unloaded/namespace route and documentation-link audits、callback rejection safety and full Phase 1/1.5/2/3 aggregate gates pass |

后续里程碑不得以“先写一个多 key cursor schema”或“先持久化 readPosition”作为临时实现；这两种临时路径
都会形成不可安全升级的 durable state。
