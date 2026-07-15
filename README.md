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

F3-M6 已在当前 Pulsar source lock `master@c2f7c22fdc562022b992a5c7aecb5fd5c02d318d` 完成。M6 增加
普通与 batch-index MessageId 在 history/seek/unload/failover/restart 后的逐字段恒等验证、cursor internal
property 跨 owner/restart 保留、trim/future reset 边界、root/snapshot hard-limit、activation-marker rollout、
F4 snapshot inventory、同名 topic 新 incarnation 隔离，以及 loaded/unloaded/namespace admin route 静态审计。
真实 gate 还捕获并修复了两个仅在同名 delete/recreate 出现的问题：`DELETED` topic 的 properties probe
必须返回空视图以允许重建，且 `ManagedLedger.asyncDelete` 必须像 stock BookKeeper 一样在 callback 前释放
factory cache，避免新 broker topic 复用已删除 stream。`phase3M6Check`、`phase3M6FinalCheck`、
`phase3Check` 和 `phase3FinalCheck` 构成完整 release gate；Future 3 已实现并 final-gated。

Phase 4 F4-M0 已于 2026-07-14 完成代码级设计门禁，F4-M1–M2 已于 2026-07-15 完成并通过普通与
Docker-backed final gate。M1 已落地
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
F4-M1–M2 已 final-gated，M3–M6 尚未实现；真实 compacted object format/worker、retention/GC 与 async
profile 仍不可用。Phase 4 只计划实现
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
