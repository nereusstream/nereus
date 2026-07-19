# Nereus Terminology

> 状态：Current
> 适用范围：总体设计、Future 1-8、Phase 1/1.5/2/3/4 实现合同、F1-BK target contract 和实现注释

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
| BookKeeper ledger-id namespace | F1-BK 要求 deployment 在 exact provider-scope digest 内独占的正 63-bit advanced exact-id prefix；其 reservation/digest 是 profile admission 与 delete activation 条件，避免非条件 delete 的 foreign-ledger ABA | provisioning + broker readiness + Oxia activation |
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
| `AppendCompletionPolicy` | F1-BK target 的 public producer-completion request；与 WAL durability 正交，包含 `PROFILE_DEFAULT`、`STABLE_HEAD`、`GENERATION_ZERO_INDEX`、`REQUIRED_OBJECT_GENERATION` |
| `AppendAckBoundary` | resolver 生成的 internal exact completion predicate；不能由 caller 绕过 profile minimum |
| `REQUIRED_OBJECT_GENERATION` | stable head + generation-zero BK index 之后，要求 exact Object higher generation 已 `COMMITTED` 且通过 resolve/read admission；不是 durability level |

`AppendOutcome` 与 `ErrorCode` 正交：前者描述 durable append certainty，后者描述 timeout、metadata、
fencing 等直接失败类别。Message text 不是状态合同。

`WAL_DURABLE` 的名字描述额外等待边界，不允许解释为“WAL quorum/object put 完成就直接返回
broker-local offset”。任何成功 append 都必须返回 stable offset/projection。

## 3. Storage profile terms

| Term | Primary WAL | Object publication | Default durability | Status |
| --- | --- | --- | --- | --- |
| `OBJECT_WAL` | Object store | compatibility alias | strict | deprecated alias |
| `OBJECT_WAL_SYNC_OBJECT` | Object store | generation-0 object target before ack | `WAL_DURABLE_AND_INDEX_COMMITTED` | Phase 1 target |
| `OBJECT_WAL_ASYNC_OBJECT` | Object store | primary WAL committed first；read-optimized generation later | `WAL_DURABLE` | implemented/final-gated in F4-M5；proof-gated at runtime，aggregate-certified by F4-M6 |
| `BOOKKEEPER_WAL_ONLY` | BookKeeper | disabled | `WAL_DURABLE` | BK-M2 module/facade runtime plus real Oxia/BookKeeper restart/delete checkpoint implemented；aggregate pending，production broker pre-IO rejected until BK-M5 |
| `BOOKKEEPER_WAL_SYNC_OBJECT` | BookKeeper | higher Object generation required before producer completion | profile durability + `REQUIRED_OBJECT_GENERATION` completion | BK-M1 foundation complete；profile not implemented |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | BookKeeper | object-backed target published by shared F4 worker | `WAL_DURABLE` | BK-M1 foundation complete；profile not implemented |

Ursa-like 和 AutoMQ-like 在 Nereus 中描述 publication policy，不是两套 engine：

- **Ursa-like sync**：producer ack 等待 primary WAL、stable head commit、generation-zero BK index 和要求的
  higher Object generation publish/read proof；
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
| Read target | BookKeeper range、object WAL slice 或 compacted object range；Phase 1.5 tagged abstraction 已实现，当前生产 IO registry 只注册 Object WAL adapter；F1-BK 设计不等于 BK reader 已注册 |
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
| Generation lookup capability | broker lookup 中 reserved 的 `nereus.generation-protocol=2` readiness value；`2` 表示可 dual-read task V1/V2 且 new-write V2，不会把上面的 durable topic marker/activation-record protocol 改成 2 |

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
- **F1-BK / BookKeeper Primary WAL Delivery**：Future 1 的后续扩展，不是 Future 5；BK-M0–M6 的代码级
  target 位于 `../phase-bk-bookkeeper-primary-wal/README.md`；BK-M0 documentation gate 已于 2026-07-19
  通过，BK-M1 foundation 已 complete/final-gated；BK-M2 已推进到 real Oxia/BookKeeper restart/delete checkpoint，
  remaining matrix/aggregate pending，BK-M3–M6 仍未实现。
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
- **F4-M1**：Future 4 metadata、physical-object lifecycle、guarded object IO、reader lease/protection primitive
  里程碑；已于 2026-07-15 通过 ordinary/Docker-backed final gate。M1 complete 不表示
  higher-generation read、GC 或 async profile 已启用；generation publication/read 属于 M2，其他能力属于 M3–M6。
- **F4-M2**：Future 4 generation publication、authoritative committed read、exact dispatch、durable pin、
  same-view fallback/quarantine 里程碑；已于 2026-07-15 通过 ordinary 与 real Oxia/LocalStack final gate。
  M2 complete 不表示 compacted object worker、retirement/GC 或 async profile 已启用，这些属于 F4-M3–M6。
- **F4-M4 checkpoint U**：DELETED physical root 的持久化双 HEAD/ownerless-domain absence window、late exact-byte
  cleanup、Phase 1 references-before-manifest 和 root-last conditional retirement ordinary checkpoint；它不表示
  production runtime 已启用 physical deletion。
- **F4-M4 checkpoint V**：F3 cursor snapshot 的 guarded PUT、current-root pending protection、cursor-CAS
  visibility、permanent root protection、response-loss hydrate repair 和 durable reader-lease ordinary checkpoint；
  它关闭新写/读 frontier，但不表示 legacy backfill、snapshot candidate deletion 或 production GC 已启用。
- **F4-M4 checkpoint W**：在 completed same-epoch registration proof 之后遍历全部 64 registry shards，
  对 recovery-root commit tail、完整 generation-zero index 和 F3 active cursor inventory 建立 exact physical
  roots 与 owner-bound permanent protections，并在最终 authority revalidation 后 CAS physical/cursor backfill
  proofs 的 ordinary checkpoint；它不表示 broker cold-topic registration backfill/barrier、object inventory、
  cursor deletion scanner 或 production GC 已启用。
- **F4-M4 checkpoint AL**：为当前 Object-WAL、两种 compacted view、NRC1 与 NCS1 writer key 提供 strict
  inverse，并完整分页注册 grace-old、exact-HEAD、missing-root 对象的 ordinary checkpoint；新 root 重新等待一整段
  orphan grace，listing 不提供删除权威；checkpoint AN 可在 enabled lifecycle 的最后阶段调度 scanner，但当前
  broker safe defaults 不启动该 lifecycle，因此不表示 production GC 已启用。
- **F4-M4 checkpoint AN**：把 complete physical-root scan/routing、complete registration retirement 和
  known-prefix inventory 严格串联，并加入 fixed-delay/non-overlap/restart recovery 的 ordinary checkpoint；它不
  表示 broker GC config、coverage proof 或 destructive activation 已启用。
- **F4-M4 checkpoint AO**：把 17 项 bounded broker physical-GC properties 精确映射到 typed
  `PhysicalGcConfig`，并让 provider 的 pending protection、reader lease、clock skew、orphan grace 只消费这一
  份 cross-validated value 的 ordinary checkpoint；默认仍为 `enabled=false, dryRun=true`，配置本身不构成
  coverage/capability proof 或 destructive activation authority。
- **F4-M4 checkpoint AP**：在隔离高熵 canary key 上证明 configured object-store scope 的 guarded if-absent
  PUT、exact CRC32C/length/ETag HEAD、complete LIST、ETag-bound exact DELETE、lost-response absence convergence、
  idempotent DELETE 与 post-delete LIST absence，并产出不含 credential 的 deterministic V1 capability SHA-256
  的 ordinary checkpoint；它不持久化该摘要、不运行 coverage backfill，也不构成 destructive activation authority。
- **F4-M4 checkpoint AQ**：冻结 exact ACTIVE/readiness/domain/registration authority，依次运行并核验
  physical-root/cursor-root coverage backfill 与 AP canary，再以单个 CAS 同时持久化 capability digest 和打开两个
  V1 deletion bits 的 product-owned coordinator checkpoint；它尚未接入 provider/Pulsar startup，production
  deletion 仍保持关闭。
- **F4-M4 checkpoint AR**：把 AQ 接入 provider/runtime/factory 与 locked Pulsar 零失败 backfill 顺序，并用
  同一 configured-scope digest 同时约束 operation guard、activation 和 mutating startup/DELETING recovery 的
  ordinary checkpoint；默认 `enabled=false, dryRun=true` 仍不调用 destructive path。
- **F4-M4 checkpoint AS**：让 activation guard 与 physical-GC registry 共享同一 registered-stream global
  scope、configured reference bounds 和 projection-domain instance，并以真实四分片 Oxia + pinned LocalStack
  证明 publication-only defer、双 bit activation、wrong-scope restart fencing、empty-LIST MARKED recovery 和
  lost successful DELETE response 收敛到 DELETED 的 real-service checkpoint；它仍不是 M4 final scale/failure gate。
- **F4-M4 checkpoint AT**：在真实 target DELETE 已完成后、旧进程调用 `DELETING -> DELETED` Oxia CAS 前终止，
  并由独立新 runtime 只凭 durable DELETING root、sealed journal 与 HEAD absence 收敛 DELETED 的 real-service
  checkpoint；它关闭一个 process-death cut，但不代表 all-shard/multi-worker/scale failure matrix 已完成。
- **F4-M4 checkpoint AU**：真实 `DELETING -> DELETED` Oxia CAS 已生效但响应丢失；production exact reload 只接受
  完整 expected replacement，目标对象只 DELETE 一次，独立重启也不会再次 DELETE 的 real-service checkpoint。
- **F4-M4 checkpoint AV**：两个独立 worker runtime 同时竞争同一 MARKED root；一个 raw DELETING CAS 获胜，
  失败方 exact reload 同一 shared intent，两条 immutable DELETE recovery 路径幂等收敛一个 DELETED root。
- **F4-M4 checkpoint AW**：一个全新 runtime 在 object LIST 恒空时，仅凭 Oxia root/sealed journal
  恢复覆盖全部 256 shard 的 128 MARKED + 128 DELETING 对象；同时冻结 opaque continuation 只表示
  翻页进度、不推导跨页 logical-key 顺序的 inventory 合同。
- **F4-M4 checkpoint AX**：首进程向真实四分片 Oxia 写入一个热点 physical shard 的 1,001 个 root 与其余
  255 个 shard 各一个后退出；全新 scanner 从空 continuation 以 64 条/page 无重复、无遗漏地读取全部
  1,256 个 immutable identity 的 bounded pagination scale checkpoint。
- **F4-M4 checkpoint AY**：10,000 个 DELETED root 通过 production scanner/coordinator 先持久化 first-absence
  proof，再在独立 orphan window 后按 Phase 1 references、manifest、root-last 顺序退休；32-entry pagination、
  one-at-a-time visitation 与 remove-on-cancel deadline schedulers 共同冻结内存边界的 scale checkpoint。
- **F4-M4 checkpoint AZ**：以 10,000 个同步 cursor-snapshot candidates 冻结 stack-bounded sequential visitor，
  并在 exact 10,000 durable cursor roots + 10,000 objects 上证明 live/current、old、expired CAS-lost pending、
  deleted-cursor 分类和 central restart-safe object deletion 的 scale checkpoint。
- **F4-M4 checkpoint BA**：在 journaled source metadata / object protection 条件删除后丢失进程，并由 fresh
  coordinator/runtime 只凭 exact DELETING root、sealed journal 与 planned-key absence 恢复；同时覆盖真实 Oxia
  protection delete 已生效但响应丢失和 LocalStack object 后续安全删除的 destructive-cut checkpoint。
- **F4-M5 checkpoint X**：把 canonical projection-ref encoding、exact durable registration
  create/refresh/final revalidation、topic create/open/recreate return-before-registration 和 shared
  generation-store production ownership落地的 ordinary checkpoint；它不表示 generation lookup capability、
  two-snapshot broker barrier、cold-topic traversal、topic marker 或 cluster backfill proof 已启用。
- **F4-M5 checkpoint Y**：发布 reserved generation lookup capability，并冻结 exact
  binding/cursor/generation two-stable-snapshot broker readiness identity 的 ordinary checkpoint；它不表示
  cold-topic traversal、durable backfill proof、topic marker 或 activation guard 已启用。
- **F4-M5 checkpoint Z**：对 unloaded topic 捕获 exact NPR1 projection candidate，并执行 canonical bounded
  cold-topic registration traversal/full report 的 ordinary checkpoint；它不表示 durable backfill proof CAS、
  topic marker、activation guard 或 publication/delete bits 已启用。
- **F4-M5 checkpoint AA**：把 broker 的 exact full-readiness identity 与零失败 traversal coverage 交给
  product-owned authority，并以 response-loss-safe CAS 持久化 `streamRegistrationBackfill` proof 的 ordinary
  checkpoint；它不表示 topic marker、activation guard 或 publication/delete bits 已启用。
- **F4-M5 checkpoint AB**：实现 product-owned generation activation guard、exact six-domain/readiness/
  projection/registration proof revalidation 和默认关闭 first-marker switch 的 ordinary checkpoint；它不表示
  cluster ACTIVE transition、mutation call sites、async publication 或 physical deletion 已启用。
- **F4-M5 checkpoint AC**：实现 product-owned、proof-gated publication-only cluster ACTIVE CAS，并让 broker
  zero-failure backfill completion 等待该 activation 的 ordinary checkpoint；它不表示 topic marker、
  mutation call sites、async profile 或 physical deletion 已启用。
- **F4-M5**：Object-WAL async profile、durable registration/readiness/activation、pre-I/O lag、coupled
  materialization、exact retention/backlog projection、bounded F3 logical trim 和 Pulsar admin routing 里程碑；已于
  2026-07-19 通过 ordinary 与 retry-disabled real two-broker final gate。Complete 不表示 BookKeeper primary WAL
  profile 已实现；F4-M6 aggregate compatibility gate 随后已由 checkpoint BQ 通过。
- **F4-M6 checkpoint BD–BE**：32-reference NRC1 merge、4,096 admitted/4,097 rejected generation candidates 和
  streaming one-million-entry recovery checkpoint 的首批 final-acceptance foundation。
- **F4-M6 checkpoint BF**：生产 Oxia adapter 对 1,000 reader leases 与 1,000 source protections 分别执行八页
  完整扫描，并从 empty continuation 重启得到相同 inventory 的 pagination scale checkpoint。
- **F4-M6 checkpoint BG**：单 task 同时达到 128 exact source ranges 与 1,048,576 records，经过 64 KiB
  admission、exact source revalidation、durable create/round trip；同时冻结 task schema V2 dual reader 与 broker
  generation lookup capability V2 的 scale/schema checkpoint。
- **F4-M6 checkpoint BH**：以冻结的 64-shard SHA-256 路由挑选每分片精确 257 个有效 `StreamId`，通过生产
  Oxia codec/CAS adapter 真实写入 16,448 条 registration；process-wide scanner 以 page size 256 对每分片读取
  `256 + 1` 两页，并在全新 scanner/empty continuation 下逐键得到相同无重复 inventory 的 registry scale checkpoint。
- **F4-M6 checkpoint BQ**：在 clean、pushed Nereus
  `main@fa533a934c33f5bcc4fda328c4df64cb96c6b485` 与 Pulsar
  `master@eaf7b9a704890a9265c21f30d9f351e02d00c600` 上执行
  `phase4FinalCheck --rerun-tasks --console=plain`；21m47s 内 203/203 外层 tasks 全部执行并通过，构成 Phase 4
  `Implemented / final-gated` 的唯一 aggregate completion evidence。
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
