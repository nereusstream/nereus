# Nereus Terminology

> 状态：Current
> 适用范围：总体设计、Future 1-8、Phase 1/1.5/2/3 实现合同、Phase 4 target contract 和实现注释

Nereus 以 stream 术语描述内部正确性。Pulsar ledger、Kafka log、对象和表都是投影或物理
承载，不能取代逻辑 truth。

## 1. Truth hierarchy

| Term | 精确定义 | 当前 owner |
| --- | --- | --- |
| Logical coordinate | `streamId + offset`；排序、trim、cursor、replication 的统一坐标 | L0 API |
| Stream head | 单 stream authoritative record；保存 committed end、commit version、last commit id、trim 和 session snapshot | Oxia |
| Reachable commit | 从 stream head 的 `lastCommitId` 可达的 immutable commit-log record | Oxia |
| Append truth | stream head + reachable commit-log chain；决定某个 offset range 是否逻辑提交 | Oxia |
| Primary WAL bytes | append payload 的第一份 durable bytes；可以在 BookKeeper 或 object store | WAL layer |
| Generation-0 read index | 从 reachable commit 物化的 primary read target；丢失时可 repair | Oxia derived index |
| Higher generation | 在一个 `(streamId, ReadView)` 内单调分配的替代物理读目标；只有 generation-index `COMMITTED` 状态可见，不改变 offset | Oxia generation index |
| Generation publication | 最终 generation-index 键的同键 `PREPARED -> COMMITTED` CAS；是 physical-target switch，不是 append commit | Oxia generation index |
| Recovery checkpoint | `NRC1` immutable bytes + versioned root；在 root CAS 后可替代旧 commit prefix 的 replay/index-repair 职责 | object store + Oxia recovery root |
| Physical object root | 一个 object key hash 的 lifecycle epoch、identity 和 deletion truth；`ACTIVE/MARKED/DELETING/DELETED`，按 256 个 deterministic shard 可恢复扫描；`DELETED` 是逻辑终态，长宽限期双 absence/owner/domain proof 后其审计记录可条件退役 | Oxia |
| Object manifest | object identity、format、checksums、slices 和引用审计状态；不是 append truth | Oxia + object store |
| Object bytes | immutable WAL/compacted/index/snapshot/table bytes | object store |
| Watch notification | cache invalidation/refresh hint；允许丢失、重复、乱序、合并 | Oxia watch |

“Oxia is the authority”应具体说明是哪一种状态。不要笼统地把 offset index 写成 append truth：
Phase 1 中 append 线性化 truth 是 stream head，offset index 是读路径物化索引。

## 2. Append and durability terms

| Term | Meaning |
| --- | --- |
| Append session | stream-scoped writer session；epoch/token 被 stream-head CAS 校验，不等于 durable ownership |
| Fencing token | 拒绝 stale writer 的单调 session identity |
| Commit intent | head CAS 前写入的 immutable commit-log record；单独存在时不可见 |
| Append linearization point | stream-head `putIfVersion` 成功 |
| Stable offset | 已由 successful head CAS 固化、可从 reachable commit 恢复的 offset |
| Stable append commit | primary WAL 已 durable 且 commit intent 已被 successful head CAS 接入 reachable chain；不等于 strict index success |
| Index materialization | 把 reachable commit 写成 generation-0 offset index 和版本匹配的 legacy committed-slice / generic committed-append marker |
| `AppendAttemptId` | Phase 1.5 实现的 process-local opaque handle；只用于恢复 exact retained physical attempt，不是 durable producer dedup key |
| Exact append recovery | 等原 mutation runner quiesce后，重放同一 commit request 或从 immutable head anchor 分页证明结果；从不准备新 WAL bytes |
| Unknown final state | informal description for `MAY_HAVE_COMMITTED`；code/API 必须使用 structured `AppendOutcome` |
| `AppendOutcome.KNOWN_NOT_COMMITTED` | append failure 已证明该 attempt 没有推进 stream head |
| `AppendOutcome.MAY_HAVE_COMMITTED` | head response 或 bounded ancestry proof 不确定；不得直接创建新 physical append |
| `AppendOutcome.KNOWN_COMMITTED` | head 已知包含该 append；index/result failure 只能 repair/recover，不能重 append |
| `WAL_DURABLE` | primary WAL durable + stable logical commit；不保证 derived read index 已物化 |
| `WAL_DURABLE_AND_INDEX_COMMITTED` | 上述条件 + generation-0 read/replay indexes 已确认 |

`AppendOutcome` 与 `ErrorCode` 正交：前者描述 durable append certainty，后者描述 timeout、metadata、
fencing 等直接失败类别。Message text 不是状态合同。

`WAL_DURABLE` 的名字描述额外等待边界，不允许解释为“WAL quorum/object put 完成就直接返回
broker-local offset”。任何成功 append 都必须返回 stable offset/projection。

## 3. Storage profile terms

| Term | Primary WAL | Object publication | Default durability | Status |
| --- | --- | --- | --- | --- |
| `OBJECT_WAL` | Object store | compatibility alias | strict | deprecated alias |
| `OBJECT_WAL_SYNC_OBJECT` | Object store | generation-0 object target before ack | `WAL_DURABLE_AND_INDEX_COMMITTED` | Phase 1 target |
| `OBJECT_WAL_ASYNC_OBJECT` | Object store | primary WAL committed first；read-optimized generation later | `WAL_DURABLE` | F4-M0 designed；implementation reserved until M5 |
| `BOOKKEEPER_WAL_ONLY` | BookKeeper | disabled | `WAL_DURABLE` | reserved |
| `BOOKKEEPER_WAL_SYNC_OBJECT` | BookKeeper | object-backed target published synchronously | `WAL_DURABLE_AND_INDEX_COMMITTED` | reserved |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | BookKeeper | object-backed target published by worker | `WAL_DURABLE` | reserved |

Ursa-like 和 AutoMQ-like 在 Nereus 中描述 publication policy，不是两套 engine：

- **Ursa-like sync**：producer ack 等待 primary WAL、stable head commit 和要求的 read-index publish；
- **AutoMQ-like async**：producer ack 仍等待 stable head commit，但不等待 secondary/read-optimized
  object generation；
- **BK-only**：没有 Nereus secondary object generation，仍使用同一 offset/session/projection truth。

## 4. Object and read terms

| Term | Meaning |
| --- | --- |
| Multi-stream WAL object | 一个 physical object 包含多个 stream slices |
| Stream slice | 一个 WAL object 中属于一个 stream 的 immutable payload/index range |
| Entry index | slice 内 entry boundaries 到 relative record offsets/physical bytes 的映射 |
| Offset index | stream offset range 到 physical read target 的 generation-aware mapping |
| Read resolver | 从 head/offset index/validated cache 解析 physical target，并在缺失 generation-0 index 时 repair |
| Read target | BookKeeper range、object WAL slice 或 compacted object range；Phase 1.5 tagged abstraction 已实现，当前生产 IO registry 只注册 Object WAL adapter |
| Materialization | 从 primary WAL 复制/转换并发布 object-backed generation |
| Materialization stream registration | 64-shard Oxia discovery hint，使无 topic ownership 的 worker 能找到 stream；scanner 必须重读 projection/head/index/task，记录本身不是可见性或删除 truth |
| Compaction | 生成更适合读取或表查询的 per-stream object；可以是 materialization 的一种 |
| Generation replacement | 发布更高 generation 以切换 physical target，不改变 logical offsets |
| Content-verified output identity | 由 exact content SHA-256 + durable worker output-attempt id 定址的 immutable output；identity 中不包含 generation，也不复活已删 key |
| Topic-compacted view | 仅显式 compacted read 使用的稀疏/有损 `TOPIC_COMPACTED` index/object domain；不与 `COMMITTED` 比较或 fallback |
| Reader lease | resolve 后、IO 前按 physical object/process 建立的有界 durable deletion fence；建立后还必须重验 object root 和 exact index identity |
| Object protection | task/generation/recovery/cursor/catalog 等 durable reference domain 的对象引用；采用 create-then-root-revalidate 关闭与 GC mark 的跨键竞态 |
| Orphan | bytes 或 metadata intent 存在，但没有被当前 authoritative state 引用 |
| Logical delete | stream head 首次进入 `DELETING` 的 authoritative CAS；终止 append/read lifecycle，但不删除 object bytes |

## 5. Protocol projection terms

### Pulsar

| Term | Nereus meaning |
| --- | --- |
| ManagedLedger facade | 让 Pulsar broker runtime 使用 `StreamStorage` 的兼容层 |
| Storage-class binding | broker metadata 中按 topic lifetime 单键选择 `bookkeeper`/`nereus` 的状态；不保存 offset/bytes |
| Projection incarnation | 同名 topic 的一次 Nereus projection 生命期；Nereus delete/recreate 产生新 stream 和 virtual ledger |
| Virtual ledger | 一个 projection incarnation 内 committed stream ranges 的稳定 Pulsar coordinate projection |
| `Position(ledgerId, entryId)` | 通过当前 incarnation projection 映射到 stream entry/read/mark-delete coordinate；角色决定合法边界 |
| `MessageId(..., batchIndex)` | F2 映射为 persisted-entry offset + entry 内 sub-index；`batchIndex` 不消耗 L0 offset |
| ManagedCursor | Oxia cursor state + optional immutable snapshot object 的 facade |
| Mark-delete | first not cumulatively acknowledged persisted-entry offset；partial batch 由 entry-keyed ack set 表示 |
| Cursor correctness root | 每个 stream/cursor-name hash 的单一 Oxia CAS record；同时拥有 generation、ack、properties 和 snapshot ref |
| Cursor owner session | 每次 writable ledger open 新生成并写入 retention + 全部 ACTIVE cursor root 的 128-bit fence；全部 claim/stabilize 后才允许发布 topic，且不进入 MessageId/snapshot |
| Cursor preactivation owner root | marker/cursor 尚不存在时也由 F3 writable open create/claim 的 ACTIVE/no-pending retention root；只 fence 旧 session，不代表 durable cursor 已激活 |
| ManagedLedger ownership guard | Pulsar broker 传给 checked async open 的 namespace/topic ownership supplier；F3 在 claim 前、发布前及首次 cursor callback 边界复核，但它不是 durable CAS token |
| Cursor generation | subscription name 每次 delete/recreate 单调递增的 stale-writer fence；不进入 MessageId |
| Cursor ack-state epoch | 同一 cursor generation 内 reset/clear-backlog destructive replacement 的单调 epoch；普通 ack 只能在 epoch 不变时 rebase |
| Local read offset | broker-local next-dispatch hint；正常读取会推进，永不持久化，重启从 first-unacked 重建 |
| Whole ack range | mark-delete 之上的 fully acknowledged Entry half-open offset range；规范化为 sorted/disjoint/non-adjacent |
| Partial batch ack | entry offset 对应的 Pulsar remaining-bit `long[]`；set bit 表示仍未 ack，合并为 bitwise AND |
| Cursor snapshot reference | cursor root 中使一个 immutable `NCS1` full ack snapshot 可见的引用；object existence 本身不构成可见性 |
| Cursor snapshot inventory | F3 对一个 versioned retention/root scan 与同 stream snapshot-object listing 的只读分类；列出 live refs 和 unreferenced candidates，但自身 never authorizes deletion，F4 必须复核 captured versions/owner 并遵守 pending-lifecycle veto |
| Cursor protection floor | stream-level conservative trim bound；new/recreated cursor 和 backward reset 必要时先降低它 |
| Cursor `PROTECTION_PENDING` | complete create/recreate/backward-reset intent 已冻结 floor raise/trim，直到同 attempt cursor root 被证明并 finalize |
| Cursor `TRIM_PENDING` | logical trim offset、attempt ID 与 exact composed L0 reason 已被冻结且必须恢复/验证完成的 retention 状态；不等于 physical GC completion |
| Cursor protocol activation | topic projection 内部保留的 `nereus.cursor-protocol=1` minimum-reader fence；首个 durable cursor 前 CAS，用户 properties 不可见 |
| Generation protocol activation | topic projection 内部单调 `nereus.generation-protocol=1` reader fence；先建立 matching stream registration，再执行首个 F4 publication/deletion，旧 broker 必须 fail closed |

### Kafka / KoP

| Term | Nereus meaning |
| --- | --- |
| Kafka offset | 在 Kafka-compatible/canonical payload mapping 中等于 Nereus record offset；不能直接套用到 `PULSAR_ENTRY_V1` batch entry |
| Log end/high watermark | 由 stream head 的 `committedEndOffset` 派生 |
| Fetch target | offset resolver 选择的 current physical generation |
| Group offset | Kafka group 独有的 Oxia state；不等于 Pulsar cursor |
| Leader | preferred broker projection；不是 durable partition owner |

## 6. Layer terms

| Layer | Responsibility |
| --- | --- |
| L0 Core Stream Storage | offsets、sessions、WAL、head/commit-log、read index、resolve、trim |
| L1 Pulsar Projection | ManagedLedger、Position/MessageId、cursor/subscription、Pulsar semantics |
| L2 Kafka Projection | KoP produce/fetch/group/txn/leader projection |
| L3 Materialization and Lakehouse | higher generations、compaction、SBT/SDT |
| L4 Routing and Operations | membership、preferred routing、brown-out、cache/ops |

## 7. Delivery terms

- **Future 1-8**：稳定的 capability-track 编号，不代表统一处于未来。
- **Phase 1**：已完成的 Future 1 代码级交付阶段。
- **Phase 1.5 / P15-M0-M6**：F1 与 F2 production 之间已完成并 final-gated 的 L0 foundation delivery；
  它不是新的 Future 编号，也没有扩张 executable profile support。
- **P15-M6**：F2-M0R2 新发现的窄结果交接；把 internal committed truth 已有的 cumulative logical size
  加入 generic `AppendResult`，不改变 durable format/profile/commit boundary；已于 2026-07-12 final-gated。
- **M0-M8**：Phase 1 内部里程碑；M7 是 production Oxia adapter gate，M8 是最终端到端验收/冻结。
- **F2-M0R**：Future 2 API spike 后的代码级复审；锁定 incarnation、append recovery、method matrix 和
  runtime bootstrap，仍不代表 facade 已实现。
- **F2-M0R2**：在 exact target Pulsar checkout 上完成的实现前闭环；锁定 metadata type、durable binding
  inspection、write-fence/ack broker handoff、S3 provider、capability rollout 与 namespace first-create race，
  仍不代表 facade 已实现。
- **F3-M0 / F3-M0R**：Future 3 对 local Pulsar master API/call path 和 cursor/snapshot/owner-session/retention protocol 的
  design-only code-level gate；passed 表示 implementation-ready，不表示 durable cursor code 已存在。
- **F4-M0**：Future 4 对 local Nereus/Pulsar source、generation/object/task/recovery/lease/GC/async/
  rollout 的 code-level target-design gate；complete 表示 M1 可开始，不表示 Phase 4 生产代码已存在。
- **Design gate**：进入实现规划前必须回答的问题。
- **Implementation gate**：代码和测试必须通过的验收条件。

## 8. Terms to avoid

不要使用：

- “offset index CAS 是 Phase 1 append 线性化点”；
- “object manifest/object list 决定可见性”；
- “`WAL_DURABLE` 可以返回临时 offset”；
- “async profile 跳过 Oxia”；
- “preferred broker owns the partition”；
- “ledger/Kafka log 是 storage truth”；
- “compaction 重新分配 offset”；
- “lakehouse catalog 在 producer ack path”；
- “Nereus is AutoMQ for Pulsar”；
- “reserved profile 已经支持”。
- “BookKeeper target value/codec 已存在，所以 BookKeeper profile 已支持”；
- “logical delete 会立即删除对象”。
- “durable read position 可以在 broker failover 后继续 dispatch” 作为 F3 correctness contract；
- “扫描一次所有 cursor 的最小 mark-delete 就可以安全 trim”；
- “cursor snapshot 上传成功就已经可见”。
- “Pulsar topic ownership、watch 或 graceful drain 已足以 fence 旧 broker 的 cursor CAS”。

建议使用：

- “stream-head CAS linearizes the append”；
- “reachable commit-log records explain the committed range”；
- “offset index materializes the read path”；
- “higher generation changes location, not offset”；
- “preferred broker is a locality hint”；
- “object store stores bytes, not truth”。

## 9. Naming

```text
Product: Nereus
Storage class: nereus
Configuration prefix: nereus.*
Repository modules: nereus-*
```

Durable key components必须通过 `KeyComponentCodec`；offset/generation key 使用固定宽度编码。
人类可读 alias 可以放 attributes，但不能替代 durable identity。
