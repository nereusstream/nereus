# Nereus

Nereus is a Pulsar-native shared-storage streaming engine built around an Oxia
metadata/coordination plane, selectable primary-WAL/object-materialization profiles,
a shared object data plane, broker-locality without durable broker ownership, and a
single logical `streamId + offset` coordinate.

The project website and namespace authority is `nereusstream.com`; Java packages and Maven coordinates use
`com.nereusstream`. This is the standalone product repository for `github.com/nereusstream/nereus`.
Pulsar and KoP changes are developed in organization-owned fork repositories,
not as patch overlays inside this repo.

## Repository Layout

```text
nereus/
  gradle/                               Gradle wrapper and version catalog
  nereus-api/                           public internal APIs and value types
  nereus-core/                          L0 StreamStorage core
  nereus-metadata-oxia/                 Oxia metadata and coordination adapter
  nereus-object-store/                  object WAL / object IO boundary
  nereus-managed-ledger/                ManagedLedger facade implementation boundary
  nereus-pulsar-adapter/                Pulsar broker integration boundary
  nereus-kop-adapter/                   KoP/Kafka projection boundary
  docs/design/                          north-star and capability-track designs
  docs/phase-1-core-stream-storage/     implemented Phase 1 contracts and milestones
  docs/phase-1.5-core-storage-foundation/ implemented L0 evolution and gates
  docs/phase-2-managed-ledger-facade/   F2 code-level contracts, API spike and milestones
  docs/phase-3-cursor-subscription/      implemented/final-gated F3 code-level contract
  docs/phase-4-compaction-generation/    F4-M0-complete code-level target contract
  docs/automq-like-stream-storage/       async materialization profile design
```

## Related Organization Repositories

Expected upstream forks:

```text
github.com/nereusstream/pulsar  -> fork of apache/pulsar
github.com/nereusstream/kop     -> fork of streamnative/kop
```

The main Nereus repository holds product-owned modules and authoritative design documents.
Forks hold changes that must land inside upstream Pulsar or KoP trees.

## Current Phase

Future 1 / Phase 1 Core StreamStorage M0-M8 is complete:

- protocol-neutral API/value/error contracts；
- Oxia key/record/codec, fake metadata, and production Oxia Java adapter；
- Object WAL v1 writer/reader, entry index, checksums, and resource guards；
- append, resolve/read, trim/recovery, restart, and post-head repair state machines；
- ordinary and Docker-backed final acceptance gates。

Only `OBJECT_WAL_SYNC_OBJECT` is a Phase 1 execution target. BookKeeper and async
materialization profiles are reserved design/API boundaries, not implemented support.

Future 2 F2-M0/M0R/M0R2 design and Phase 1.5 prerequisites are complete. P15-M0-M6 and F2-M1-M6 are implemented/final-gated。
`nereus-managed-ledger` now provides the
writable facade、strict get-only read-only ledger、exact append recovery/write-fence handoff、lifecycle/admin/stats
surfaces and audited unsupported channels。F2-M4 cursor boundary is implemented；F2-M5 has product runtime/S3
gates plus the fork hybrid bootstrap，durable `NSB1` binding open/delete coordinator and claim-before-open topic
feature admission。Producer attach、publish metadata、non-durable subscribe、durable-subscription and transaction
operation gates are also wired before their stock mutations；the limited non-durable cumulative ack gate runs before
ack timestamp/counter/pending-ack/cursor mutation。Loaded-topic admin mutation gates now reject durable-subscription、
backlog/cursor、compaction/offload、truncate、shadow and migration operations。Authoritative live-policy refresh、
namespace/capability convergence、generation-safe write-fence recovery and shared-store peer lifecycle are gated。
A real two-broker Oxia/LocalStack/BookKeeper test proves unload、ownership failover、process restart/reverse takeover、
exact single/batch Position and bytes、hybrid coexistence and real S3 objects。`phase2Check` and Docker-backed
`phase2FinalCheck` exist and pass。F2-M6 now also composes committed-response loss into one recovered callback/
Position，real-Oxia restart repair of both derived projection indexes，and facade close/trim/reopen/terminate/
logical-delete/recreate with retained Object-WAL bytes。The final slice proves real BookKeeper virtual-ledger
isolation、watch-disabled cross-runtime polling wake、Object-WAL failure recovery、stock topic admission/write-fence
behavior and the complete binding/ack/capability/S3 matrix。All F2-M6 scenarios 1–19 pass；Future 2 is complete。

Future 3 / Phase 3 已于 2026-07-14 完成 design-only F3-M0 和 F3-M0R：本地 Pulsar
`master@7efae25af39a15407c1397d9e1f4ac4658d09daa` 的 `ManagedCursor` API、broker ack/recovery/admin
调用路径以及 F2 handoff 已锁定；single-root cursor CAS、generation/tombstone、remaining-bit batch ack、
destructive `ackStateEpoch`、immutable snapshot、local-only dispatch read position、per-writable-open
owner-session claim/fencing、broker ownership guard handoff 和 recoverable retention barrier 已冻结到
Java/file/wire/state-machine/test 级。
F3-M1 metadata/snapshot foundation 已于 2026-07-14 完成并通过普通与 Docker-backed final gate：metadata
records/codecs、single-key CursorMetadataStore/Oxia adapter、F2 activation-marker preservation、ack domain、
immutable NCS1 snapshot codec/store、真实 Oxia restart/range/watch/CAS，以及 LocalStack conditional-create/
restart round-trip 均有可执行证据。`phase3M1Check` 和 `phase3M1FinalCheck --rerun-tasks` 已通过。
F3-M2 CursorStorage/retention state machines 也已完成并通过普通与 Docker-backed final gate：durable
create/ack/reset/property/delete、owner claim/fencing、snapshot spill/hydration、recoverable protection/trim
barrier、CAS/trim 响应丢失和并发模型均有确定性测试；新增真实 Oxia + LocalStack S3 组合门禁验证两个
独立 runtime 接管、旧 owner fencing 与第三 runtime 重启 hydration。`phase3M2Check --rerun-tasks` 和
`phase3M2FinalCheck --rerun-tasks` 已通过。F3-M3 ManagedCursor facade 也已完成并通过
`phase3M3Check`：runtime/provider 拥有并按依赖逆序关闭 cursor metadata/snapshot/retention/storage 资源；
broker ownership checker 贯穿 writable open 的 claim 前与 publication 前边界；writable open 生成 fresh
owner session、claim/hydrate 全部 ACTIVE durable roots 后才构造 ledger；durable `openCursor`、ack/reset、
property、delete/recreate、read/wait/replay 和异步 close 均由完整分类及 conformance suites 覆盖。

F3-M4 已在本地 Pulsar `master@12edc9381c147ceec8bedd530acb5be7db339707` 完成并由
`phase3M4Check` 锁定：fork 独立发布 storage-binding/cursor 两项 capability，首个 durable cursor 要求两次
稳定的全 persistent-broker 快照；broker typed configuration 使用 canonical cursor runtime/context；topic、
subscribe、ack 与 admin admission 按 F3 allowlist/denylist fail closed；durable ack 的客户端成功、计数、
redelivery 和 pending-state 副作用严格排在 cursor CAS 成功之后；topic recovery 在 subscription recreation
前接收已 hydrate 的 durable cursors。BookKeeper-backed topic 保持 stock 行为。

F3-M5 已在当前 Pulsar source lock `master@a2bad4cfa260cc4575ae759f8a345ce969c8ec3a` 完成并通过
`phase3M5Check` 与 `phase3M5FinalCheck --rerun-tasks`。确定性套件覆盖全部 CAS/snapshot/generation/
retention crash cuts，隔离规模夹具验证 10,000 个 cursor、40 个 bounded pages、精确第 10,001 个 admission
拒绝和超限元数据 fail-closed；真实 Oxia、LocalStack S3、两 broker 与 BookKeeper 控制路径验证
Exclusive/Failover/Shared、稳定 MessageId、partial-batch ack、owner/runtime restart、TTL/subscription expiry 和
storage-class coexistence。M5 同时修复了 10k hydration 递归栈溢出、Shared dispatcher 所需 mutable entry list
以及首次 policy-system-topic 初始化时的 namespace lock 递归。

F3-M6 的历史验收基线是 `master@ff6e4fdfc03ffd8535ab2ece58d247dd1c64e8b4`；当前 Phase 4
Pulsar source lock 已推进到 `master@42a4bfd7dfae1d0b23e07dd2b9ebb59f0344782f`。M6 增加
普通与 batch-index MessageId 在 history/seek/unload/failover/restart 后的逐字段恒等验证、cursor internal
property 跨 owner/restart 保留、trim/future reset 边界、root/snapshot hard-limit、activation-marker rollout、
F4 snapshot inventory、同名 topic 新 incarnation 隔离，以及 loaded/unloaded/namespace admin route 静态审计。
真实 gate 还捕获并修复了两个仅在同名 delete/recreate 出现的问题：`DELETED` topic 的 properties probe
必须返回空视图以允许重建，且 `ManagedLedger.asyncDelete` 必须像 stock BookKeeper 一样在 callback 前释放
factory cache，避免新 broker topic 复用已删除 stream。`phase3M6Check`、`phase3M6FinalCheck`、
`phase3Check` 和 `phase3FinalCheck` 构成完整 release gate；Future 3 已实现并 final-gated。

Phase 4 F4-M0 已于 2026-07-14 完成代码级设计门禁，F4-M1–M3 已于 2026-07-15 完成并通过普通与
Docker-backed final gates。M1 已落地
F4 API/metadata/Oxia 基础、materialization 模块边界、guarded/replayable object-store IO、物理对象与 GC
reference-domain 强类型、generation activation proof contract、create/revalidate/release durable reader pin
handshake，以及无保护空窗的 durable protection acquire/owner-transfer/release handshake。metadata checkpoint
还覆盖 generation、64-shard registration、256-shard physical root、conditional delete 契约和共用 root 状态机
校验。43 个 lifecycle/optional codec golden、stream-scoped CAS transition guards、Oxia slash-aware fixed-depth
range bounds、SDK response 与 exact-byte upload 双完成条件，以及 exact HEAD 后受限于 HTTP 405/501 的
conditional-DELETE compatibility fallback 均已由 focused real Oxia 与 pinned LocalStack 验证；
`phase4M1Check` 和 `phase4M1FinalCheck --rerun-tasks` 均已于 2026-07-15 通过。F4-M2 已实现
view-aware read contract、exact target-reader registry/dispatch、generation allocator、strict index hydration、
exact candidate lookup、physical identity resolution、fresh-scan/pin/revalidation resolver，以及 pin-through-IO 的
`ReadCoordinator` same-view fallback、全引用域 quarantine repair 与 bounded transient retry。restart-safe
publication state machine、re-entry reconciler、canonical task/output identity 和 visible-protection owner transfer
也已落地。`phase4M2Check` 与 `phase4M2FinalCheck --rerun-tasks` 已通过；真实 Oxia/LocalStack fixture 验证
两个独立 runtime 并发发布、COMMITTED response loss 后重启恢复、exact pin/quarantine 与同 view fallback。
新的
`docs/phase-4-compaction-generation/` 冻结了 API/module ownership、`NCP1/NTC1/NRC1` formats、
Oxia records/keys/codecs、generation publication、task/recovery/async state machines、durable reader
leases/protections、64-shard restart-safe stream discovery、retention/GC、guarded PUT 与可界的 DELETED-root audit
retirement、Pulsar rollout 与 F4-M1–M6 实施门禁。该目录同时记录 normative target 与已落地检查点；
F4-M1–M3 已 final-gated；M3 真实 Parquet writer/strict reader、whole-file verifier、NTC1 storage facade、
core exact adapter，deterministic policy/planner/task-store/recovery/64-shard registry scanner，以及 stream-scoped
exact-source reader/claim-to-output-ready worker checkpoints 已落地并通过 focused tests；protection owner
crash-cut reconciliation 也已接入 recovery/publication 并通过重复 CAS/response-loss 测试。随后落地的
advisory checkpoint reconciler 只从 current-policy `COMMITTED` generation 的连续覆盖推进，并在 CAS
response loss 后精确 reload；有界 dispatcher/service 则实施全局/单流 worker 上限、非重叠 64-shard full
pass、hint coalescing 和 deadline close。随后新增的 managed-ledger cross-layer test 已证明 unbatched、
compressed batched Entry 的 exact bytes/properties/ordering-key 与 middle-batch MessageId 经 NCP1 往返不变，
且 generation-level projection identity 不泄漏成 per-entry metadata。随后落地的 topic-compaction neutral
decoder/strategy SPI 与 exact frozen-identity registry 已锁定 durable policy 解析边界。M3 的
terminal workflow-metadata retirer 也已通过 exact index/checkpoint/root/protection proof、conditional
delete 与 response-loss reload 收敛 terminal tasks、stale stats 与 old-policy checkpoints。随后完成的
topic-compaction checkpoint 固定 `COMMITTED` source bootstrap 与独立 `TOPIC_COMPACTED` publication，使用
collision-free tagged-v1 key 表示 unkeyed retain-exact records，并以共享 staging budget 下的 checksum-
verified sorted spills 执行两遍 key selection/source replay；NTC1 worker/strict verification/publication focused
tests 已通过。`phase4M3Check` 与真实 Oxia/LocalStack-backed `phase4M3FinalCheck --rerun-tasks` 已于
2026-07-15 通过，覆盖双 worker、claim/response-loss/restart、完整 Parquet bytes 与全 64 分片分页/watch-loss；
F4-M3 已 final-gated。F4-M4 的首个 NRC1 object-protocol checkpoint 已实现 spill-backed one-at-a-time writer、
strict header/footer/directory/range reader、attempt/key identity、body/content 双 SHA 和 authoritative metadata
verifier，并由 golden/corruption/sparse-directory tests 覆盖；`phase4M4CheckpointCheck --rerun-tasks` 已于
2026-07-15 通过。第二个 checkpoint 已把 generation-zero append/recovery 切换为 exact intent preparation、
ACTIVE physical root、commit-owned permanent `REACHABLE_APPEND`、protected head CAS、exact index materialization 与
index-owned permanent `VISIBLE_GENERATION`，并完成 production shared-Oxia runtime 接线。普通 unit tests 及
real-Oxia/F4-M2/M3 integration source compilation 已通过；`phase4M4ProtectedAppendCheck` 已于 2026-07-15
通过完整前置回归链与本地 Pulsar M4 check，Docker-backed M4 execution gate 尚未运行。
后续 M4 checkpoints C–R 已继续落地 recovery-root publication、checkpoint-aware replay/index repair、exact
retirement metadata、可重建 GC plan、root MARK/DRAIN/DELETING fence、全 256 分片扫描、五个 affected-stream
reference domains、root-authenticated retirement journal/destructive recovery，以及 generation-index/source 的
typed retirement handlers。Generation-zero source plan 现在会把 NRC1 commit、source commit/index/marker、当前
exact `COMMITTED` replacement index 与另一个 `ACTIVE` physical root 绑定并在冻结末尾重证。Checkpoint Q 又为
`COMMITTED` view 的 matching higher generation 实现完整 NRC1 range/count/schema tiling、严格更新一代以上的
healthy replacement 证明、candidate-root final fence 和 response-loss-safe
`COMMITTED/QUARANTINED -> DRAINING` CAS；已处于 `DRAINING` 的 source 在进入删除计划前也必须重走同一证明。
Checkpoint R 又补齐 completed-trim 和 `TOPIC_COMPACTED` same-view replacement 两条 eligibility 分支，并在任何
metadata/root read 前执行 source-retirement grace。Checkpoint S 已落地 cluster generation activation 的 exact
record/codec/key、read-only lookup、PREPARED bootstrap、monotonic CAS 与冻结 golden/contract tests，作为后续
global proof 和 M5 rollout 的 durable authority foundation。Checkpoint T 又新增 activation/backfill/domain-set
gated 的 64-shard global scope、future-catalog sentinel，并让 generation/append/materialization/projection/cursor
五个 domain 对 ownerless query 执行同一 authoritative stream set 的完整扫描与重验。Checkpoint U 已实现
持久化双 absence 窗口、late exact-byte cleanup、Phase 1 references-before-manifest 与 root-last CAS 的
DELETED-root audit retirement。Checkpoint V 又把 cursor snapshot 新写入切换为 guarded immutable PUT、
current-root pending protection、cursor CAS、permanent root protection，并让 hydrate/read 在 durable reader
lease 内执行且可修复 CAS response loss；shared physical/protection/read-pin 已接入 production provider。
Checkpoint W 已实现 strict NPR1 projection authority、全 64-shard live-reference physical/cursor-root
backfill、exact commit/index/cursor owner protection、final authority revalidation 和 response-loss-safe dual
activation proofs。Checkpoint X 又把 canonical projection-ref encoding、create/refresh/final-revalidate
registration coordinator、topic open/recreate return-before-registration ordering和 shared generation-store
production ownership落地，从而关闭新建与重新打开 topic 的 registration 前沿。Checkpoint Y 已在 Pulsar fork
发布 reserved `nereus.generation-protocol=1`，并实现 binding/cursor/generation 三协议的 two-stable-snapshot
barrier、包含 broker start timestamp 的 deterministic readiness epoch/full digest，以及 registry-notification
cache invalidation。Checkpoint Z 已实现 exact unloaded projection candidate，以及按 tenant/namespace/topic
canonical 排序、one-namespace-at-a-time、bounded concurrency/deadline 的 broker cold-topic registration
traversal；完整 readiness/binding revalidation、deterministic coverage SHA、full counters 和前 100 个 redacted
failures 已冻结。Checkpoint AA 又把完整 broker-set readiness 转换为 product-neutral identity，并在
零失败 report、final readiness 不变的条件下通过 product-owned、response-loss-safe CAS 持久化
`streamRegistrationBackfill` proof；同 epoch coverage 不可变，新 epoch 会清空其他旧 epoch proof 与
object-store capability。Checkpoint AB 又实现 product-owned generation activation guard：ACTIVE cluster
record、current readiness/registration proof、exact six-domain digest、strict NPR1 projection/L0/registration
truth 和 monotonic marker 被绑定为 short-lived proof，mutation CAS 前会重证；Pulsar first-marker switch
默认关闭，physical delete 还要求 exact projection-domain snapshot 和同 epoch delete proofs。Checkpoint AC
又实现 product-owned publication coordinator：只在显式开关开启、zero-failure registration backfill proof
已经 durable、current readiness/epoch/domain set 完全匹配时，执行 publication-only
`PREPARED -> ACTIVE` CAS；broker backfill completion 会等待 activation，并对 conflict、lost response 和
final readiness drift fail closed。Checkpoint AC 当时仍缺 cursor snapshot candidate/deletion scanner、object inventory、
registration retirement、其余 materialization/GC runtime composition，以及 F4-M5–M6 的最终兼容接线仍不可用；
delete bits 和 production deletion 继续关闭。Checkpoint AD 已进一步落地 opt-in Object-WAL async execution
matrix、protected-head `WAL_DURABLE` ack cut、generation-zero restart/read repair。Checkpoint AE 又把 exact
registration/topic-marker activation 和 materialization lag gate 放入每流 append lane、primary WAL
prepare/upload 之前；lag truth 从 current-policy `COMMITTED` generation 的连续覆盖、root-stable live commits
和可验证 retention stats 重建，advisory checkpoint 只做 ahead-contradiction 检查。F2 topic/L0 metadata 与
request factory 现在可无歧义保存和恢复 sync/async Object-WAL profile，BookKeeper profiles 仍拒绝。该
guard/reader 在 checkpoint AF 已与 generation-aware read、NRC1 replay/index repair、source repair 和 bounded
materialization lifecycle 一起由 production provider 原子装配；Pulsar configuration 也已映射 exact
sync/async Object-WAL profile 与完整 `MaterializationConfig`。Checkpoint AG 又实现 exact retention
policy/config/evidence values、source-index-verified stable candidate planner，以及 ownership/activation/final-
authority gated、只委托 F3 coordinator 的 logical-trim service。Checkpoint AH 继续实现 process-owned bounded
retention lane、same-stream coalescing、whole-operation timeout/close、per-ledger service installation、
`trimConsumedLedgersInBackground` 路由，以及 Pulsar 五项 typed broker config 映射。Checkpoint AI 现已把
local > global > namespace > broker 的 exact retention/backlog facts、checked `RetentionPolicySnapshot`、stable
generation readiness、registration-backed marker admission 和 policy 稳定重读接到 topic open/update；loaded 与
unloaded `TRIM_TOPIC` 均先 fail-closed 校验，再进入同一 F4 logical-retention service。Projection snapshot 只解释
Pulsar facts，最终 mutation authority 仍由 product activation/ownership/F3 coordinator 持有。Physical GC 继续
保持关闭，因此 AI 仍不等于 Phase 4 完成。Checkpoint AJ 现已实现 strict NCS key inverse、完整 bounded
retention/cursor/object/protection inventory、canonical `CURSOR_SNAPSHOT_CANDIDATE` evidence，以及中央 GC 在第二次
drain 后、DELETING intent 前调用的完整重验证。普通 owner/root/list/protection drift 会 unmark，读取失败保留
MARKED 重试；scanner 本身没有 root CAS、protection removal 或 object delete。Checkpoint AK 又把 evidence
归一化为可跨 `ACTIVE -> MARKED` 和进程重启重建的 durable facts，增加 exact MARKED drift rollback，并通过
`CursorSnapshotGcExecutor` 将 candidate 接入中央 mark/drain/revalidate/DELETING/source-retirement 链。
`Phase4PhysicalGcRuntime` 已把 exact six domains、journal、collector 和 executor 交给 production provider/
managed-ledger runtime 统一持有和关闭。Checkpoint AL 又加入当前五类 V1 writer prefix 的 strict key
inverse 与 `ObjectInventoryScanner`：完整分页只把经过 age/skew、exact HEAD、CRC32C/length 和二次 root
absence 验证的旧对象注册为 ACTIVE physical root，并为新 root 重置一整段 orphan grace；listing 从不授权
删除，malformed/young/stale/mismatch/conflict 只计数。Checkpoint AM 已加入 proof-driven stream-registration retirement
foundation：只有精确 DELETED L0、non-live projection、完整且无引用的 F3 cursor/retention authority
与 terminal/audit-grace-expired F4 workflow 事实同时成立时，才依次退休 owner protection、index/task、
recovery root、checkpoint/stats/sequence，最后经过 exact recapture 删除 registration。对应 ordinary gate 已覆盖真实 published
workflow 的 task/two-index/three-protection drain，以及 non-empty NRC1 checkpoint-root/target protection 退休；
所有 delete-response loss 均依赖 exact absence 收敛，physical root 在整个 registration-retirement 测试后仍保留。
Checkpoint AN 进一步实现并接入 metadata-first lifecycle：每轮严格执行完整 256-shard physical-root recovery、
完整 64-shard registration retirement、最后才做 known-prefix object inventory；整轮不重叠，hint 只合并一次
立即重扫，失败后按 fixed delay 重试，close deadline 只取消本轮而不关闭借用 executor。Root router 将
canonical cursor ACTIVE/MARKED 交给完整 F3 inventory/reconstruction，将无 owner 的普通 ACTIVE/MARKED 交给
六域 global proof（有 durable protection 时先廉价跳过），DELETING 交给 sealed-journal recovery，DELETED
交给 dual-absence tombstone retirement。Runtime 同时装配四类 source metadata handler、Phase 1 audit adapter
和 checkpoint-AM coordinator。Checkpoint AO 又把 broker configuration boundary 接到同一个 typed
`PhysicalGcConfig`：Pulsar `ServiceConfiguration` 现在暴露 enabled/dry-run、metadata/object page、并发、domain
hard limit、scan/lease/renew/drain/protection/orphan/tombstone/operation/close 共 17 项 physical-GC 参数，
`NereusBrokerStorageConfiguration` 在构造任何 runtime 前做正数、上限、duration 和跨配置验证。
`DefaultNereusRuntimeProvider` 不再复制 5 分钟 pending protection、2 分钟 reader lease 和 1 天 orphan grace，
而是把同一 `PhysicalGcConfig` 值交给 protection、read-pin 与 cursor snapshot 路径。默认值仍严格为
`enabled=false, dryRun=true`，因此默认启动不会安排 lifecycle；本 checkpoint 没有写 coverage proof、没有打开
activation delete bits，也没有把配置本身当作删除授权。
`phase4M4CursorProtectionCheck` 以及直接相关的 LocalStack-only、real Oxia + LocalStack cursor integration
tests 已于 2026-07-16 通过。
`phase4M4PhysicalRootBackfillCheck --rerun-tasks` 也已于 2026-07-16 通过。
`phase4M4CursorSnapshotGcCheck --rerun-tasks` 已于 2026-07-18 在 Java 21、locked Pulsar
`330eeeb3fa9903ed0123c2a0e261d403c32f0a59` 上通过；root build 与继承的 nested Pulsar regression 均报告
138 个 actionable tasks。
`phase4M4CursorGcExecutionCheck` 覆盖 checkpoint AK 的 restart reconstruction、exact rollback、cursor-only
execution adapter、six-domain runtime composition、safe-default config 和 runtime close ownership；该 gate 已于
2026-07-18 在 Java 21、locked Pulsar `330eeeb3fa9903ed0123c2a0e261d403c32f0a59` 上通过，root build
执行 139 个 actionable tasks，继承的 nested Pulsar regression 报告 138 个 actionable tasks。
`phase4M4ObjectInventoryCheck --rerun-tasks` 覆盖 checkpoint AL 的五类 writer strict inverse、complete
known-prefix pagination、age/HEAD/root 双检、second-grace root registration、dry-run 和 response-loss recovery；
已于 2026-07-18 在 Java 21、同一 locked Pulsar 上通过，root build 执行 131 个 actionable tasks，nested Pulsar
regression 报告 138 个 actionable tasks。
`phase4M4RegistrationRetirementCheck` 覆盖 checkpoint AM 的 exact L0/projection/F3 authority、terminal/audit
grace blockers、published task/index owner-protection drain、non-empty NRC1 root 以及 protection/index/task/root/
registration delete-response loss；它还断言 physical root 不被注册退休路径删除。该 gate 已于
2026-07-18 在 Java 21、locked Pulsar `330eeeb3fa9903ed0123c2a0e261d403c32f0a59` 上通过；root build
报告 132 个 actionable tasks（68 executed），nested Pulsar 报告 138 个（3 executed）。
`phase4M4LifecycleSchedulingCheck` 覆盖 checkpoint AN 的 256/64-shard metadata-first pass 顺序、fixed-delay/
coalescing/close lifecycle、ownerless protection admission 与 MARKED 重建、cursor/generic lifecycle router、
完整 source/tombstone/registration runtime composition 和 provider 启动边界；当前 safe defaults 仍不启动该
loop，gate 不代表 broker deletion activation。该 gate 已于 2026-07-18 在 Java 21、locked Pulsar
`330eeeb3fa9903ed0123c2a0e261d403c32f0a59` 上通过；root build 报告 133 个 actionable tasks
（65 executed），nested Pulsar 报告 138 个（3 executed）。
`phase4M4PhysicalGcConfigCheck` 是 checkpoint AO 的 ordinary gate：它锁定 17 项 broker 默认值、完整 typed
mapper、`enabled && !dryRun` 双开关、lease/drain/timeout cross-validation，以及 provider 对共享 protection/read
参数的消费；同时继续要求 durable coverage/delete activation 独立存在。当前 source lock 为
`master@42a4bfd7dfae1d0b23e07dd2b9ebb59f0344782f`。该 gate 已于 2026-07-18 在 Java 21 下通过；root build
报告 144 个 actionable tasks（63 executed），focused Pulsar build 报告 141 个（全部 executed）。
Checkpoint AP 现已加入 configured-scope object-store destructive capability probe。它在隔离的
`__nereus_capability__/delete-v1/<runId>/probe` key 上执行 guarded if-absent PUT、exact CRC32C/length/ETag HEAD、
complete single-prefix LIST、ETag-bound exact DELETE、lost-response absence recovery、idempotent second DELETE 和
post-delete LIST absence，并在失败时只清理 exact canary。持久化候选值是由 provider class、endpoint、region、
bucket、prefix、path-style 和上述 V1 semantics 组成的 deterministic SHA-256；credential/token 不进入摘要或日志。
该 probe 仍不写 cluster activation record、不设置 deletion bits，也不把 LIST 当作业务对象删除权威；
`phase4M4ObjectStoreCapabilityCheck` 只证明这一独立能力边界。该 gate 已于 2026-07-18 在 Java 21 和 locked
Pulsar `master@42a4bfd7dfae1d0b23e07dd2b9ebb59f0344782f` 上通过；root build 报告 145 个 actionable tasks
（69 executed），focused Pulsar build 报告 141 个（全部 executed）。
Checkpoint AQ 现已加入 product-owned physical-deletion activation coordinator。一次 bounded request 先冻结
ACTIVE publication、exact reference-domain set、broker readiness epoch 和同 epoch registration proof，再运行
physical-root/cursor-root backfill、验证落盘 coverage proofs、运行 AP canary，最后用一个 version CAS 同时写入
capability digest 并打开两个 V1 deletion bits。readiness drift、失败 backfill、same-epoch scope mismatch 均 fail
closed；CAS conflict 不重复 backfill/canary，lost response 只接受 reload 后的 exact durable authority，返回前再做
readiness/activation 双重校验。AQ 目前仍是未接入 provider/Pulsar 启动路径的协议协调器；runtime scope-digest
startup gate、broker composition 和真实 Oxia/LocalStack destructive/scale gate 仍待完成。
`phase4M5RegistrationFrontierCheck --rerun-tasks` 已于 2026-07-16 通过。
`phase4M5GenerationCapabilityCheck --rerun-tasks` 已于 2026-07-16 通过。
`phase4M5ActivationGuardCheck` 已于 2026-07-16 通过。
`phase4M5PublicationActivationCheck --rerun-tasks` 已于 2026-07-16 通过。
`phase4M5RegistrationBackfillCheck --rerun-tasks` 已于 2026-07-16 通过。
`phase4M5RegistrationProofCheck` 已于 2026-07-16 通过。
Checkpoint AE 的四个受影响模块回归和 focused lag/admission tests 已于 2026-07-16 通过；production
composition 已由 checkpoint AF 完成。Checkpoint AG 的九个 policy/config/planner/service focused tests 与
`phase4M5RetentionPlannerCheck` 覆盖 strict time-OR-size、stale source、pending lifecycle、authority drift、
exact call order、no-op 和 durable trim 后 ownership loss；checkpoint AH 又覆盖 coalescing、independent caller
completion、actual async-operation concurrency、bounded queue、timeout/close 和跨层 config bounds。Checkpoint
AI 的 focused Pulsar/Nereus tests 又覆盖 exact precedence/immutable copy、overflow、size/precise-time matrix、
loaded/unloaded trim readiness、marker admission 和 activation 后 policy 稳定重读；
`phase4M5RetentionPolicyAdminCheck` 已于 2026-07-18 在 Java 21 和 locked Pulsar
`master@330eeeb3fa9903ed0123c2a0e261d403c32f0a59` 上通过 153-task 聚合链。AG gate 已于同日在 Java 21 下通过完整前置
Nereus/locked-Pulsar 回归链。
`phase4M5RetentionRuntimeCheck` 又于 2026-07-18 在 Java 21 和 locked Pulsar
`master@68093ba53388c4cdbe6516a35391451646820c71` 上通过 151-task 聚合链。
Phase 4 只计划实现
`OBJECT_WAL_ASYNC_OBJECT`，BookKeeper WAL/profiles 仍需独立 adapter 和 gate。

Start with `docs/design/nereus-design-index.md` for document authority and current status. Use
`docs/phase-1-core-stream-storage/README.md` for the implemented L0 contract and
`docs/phase-1.5-core-storage-foundation/README.md` for the active L0 evolution contract. Use
`docs/phase-2-managed-ledger-facade/README.md` for the implemented F2 contract，and
`docs/phase-3-cursor-subscription/README.md` for the implemented F3 contract. Use
`docs/phase-4-compaction-generation/README.md` for the current F4 code-level target contract and M0 outcome.

## Build

Pulsar APIs are consumed from an exact source composite。The fork's `5.0.0-M1-SNAPSHOT` string is its local Gradle
project version，not a published Maven artifact；source-bound tasks fail early when no checkout is configured：

```bash
export NEREUS_PULSAR_CHECKOUT=/absolute/path/to/nereusstream/pulsar
```

The ordinary repository build may use the locked API baseline `100d3ef0ff7c7da36d497453b141ddff6f34a9d3`。
The Phase 2 broker gates require the clean implemented fork commit currently recorded by
`checkPulsarSourceLock`。

```bash
./gradlew checkPhase0
./gradlew phase1Check
./gradlew phase1FinalCheck --rerun-tasks
./gradlew build
./gradlew phase2Check
./gradlew phase2FinalCheck --rerun-tasks
./gradlew phase3M1Check
./gradlew phase3M1FinalCheck --rerun-tasks
./gradlew phase3M2Check
./gradlew phase3M2FinalCheck --rerun-tasks
./gradlew phase3M3Check
./gradlew phase3M4Check
./gradlew phase3M5Check
./gradlew phase3M5FinalCheck --rerun-tasks
./gradlew phase3M6Check
./gradlew phase3M6FinalCheck --rerun-tasks
./gradlew phase3Check
./gradlew phase3FinalCheck --rerun-tasks
```
