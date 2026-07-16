# Phase 4 Materialization / Compaction / Generation / GC Detailed Design

> 状态：Implementation in progress；F4-M1–M3 已完成并通过 ordinary/Docker-backed final gates。F4-M3 已交付
> compacted Parquet read/write/verifier、deterministic policy/planner/task-store/recovery/registry-scanner、exact-source
> reader/worker、protection crash-cut reconciliation、advisory checkpoint reconciliation、bounded service lifecycle、
> Pulsar Entry/NCP1 exact-byte round trip、topic-compaction neutral SPI/registry、terminal workflow-metadata retirement、
> COMMITTED-source bootstrap、tagged-v1 key encoding、shared-budget sorted-spill two-pass engine、NTC1 worker 与
> isolated publication；F4-M4 正在实现，NRC1 object-protocol、protected generation-zero append、anchor-aware
> planning、guarded recovery-root publication/restart protection reconciliation，以及 checkpoint-aware append
> replay adapter、checkpoint-derived index repair、exact source/object-audit retirement metadata adapters、
> bounded/reconstructable GC config/candidate/plan values、exact reference-domain registry、recoverable
> `ACTIVE -> MARKED -> DELETING` root fence、全 256 分片 root scanner、query-bound stateless revalidation，以及
> affected-stream generation/append-recovery/materialization concrete domains 已落地；共享 bounded canonical
> snapshot builder、managed-ledger generation marker/exact stream projection authority，以及 affected-stream
> projection-generation/cursor-snapshot domains 也已落地；retirement journal 的 fixed-depth Oxia keyspace、
> production/fake store parity、manifest-last seal/load service 与 collector PREPARE-before-MARK/final reload
> 也已实现。首个 root-authenticated destructive-recovery checkpoint 进一步加入 typed metadata-retirement
> registry、journal-driven protection retirement、exact HEAD/delete 与 restart-safe `DELETING -> DELETED` CAS；
> 正常删除、已缺失对象和缺失 journal 的零副作用失败已有聚焦测试。随后
> checkpoint N 已接入 canonical generation-index restart router、exact generation-zero conditional delete、
> higher-generation `DRAINING -> RETIRED` handler，以及每个 destructive batch/physical-delete fence 的 root +
> journal reauthentication。Checkpoint O 进一步实现 legacy/generic marker/commit exact-key inverse、两个
> response-loss-safe typed handler，以及把 NRC1/source commit/index/marker 绑定为三条 removal 的
> `SourceRetirementPlanBuilder`/exact-key revalidator。Checkpoint P 已把每条 NRC1 source entry 继续绑定到至少
> 一个当前 exact `COMMITTED` higher index 和另一个 `ACTIVE` physical root，并在冻结结束重读两者。
> Checkpoint Q 又为 `COMMITTED` view 的 matching higher source 实现 whole-range NRC1 tiling、严格更高代际的
> healthy replacement 证明与 response-loss-safe `COMMITTED/QUARANTINED -> DRAINING` pre-drain；删除计划会对
> 已经 `DRAINING` 的 source 重证同一闭环。Checkpoint R 完成两条剩余 source-eligibility 分支：任一 view 的
> completed-L0-trim exact proof，以及 `TOPIC_COMPACTED` strictly-newer same-view replacement proof；pre-drain 还会在
> 任何 metadata/root read 前执行 `sourceRetirementGrace`。Generation-zero 与 higher DRAINING plan-time reproof
> 复用相同的 trim/view-specific 事实。Checkpoint S 又落地 generation-protocol activation 的 durable
> record/codec/exact key、read-only lookup、PREPARED bootstrap、monotonic CAS 和冻结 golden/contract tests；该
> authority foundation 不会自行启用 publication/delete bits。Checkpoint T 已实现 activation/backfill/domain-set
> gated 的全 64-shard registration scope、future-catalog sentinel，并把五个 storage/managed-ledger reference
> domains 的 ownerless query 改为完整 global scan + exact revalidation。Backfill/broker activation guard、
> production runtime composition、
> cursor/root/audit completion 与最终删除开关仍保持关闭
>
> 设计基线日期：2026-07-14
>
> Nereus 输入基线：`nereusstream/nereus@e330969cd5c2c11cd38d0bd7f687185171ae91e2`
>
> Pulsar 输入基线：本地 `/Users/liusinan/apps/ideaproject/nereusstream/pulsar`
> `master@c2f7c22fdc562022b992a5c7aecb5fd5c02d318d`

> 实现状态日期：2026-07-16

F4-M1 已经落地 API/metadata/object-store 基础、guarded/replayable object IO，以及 core 的物理对象
identity、GC reference-domain proof value、generation activation proof contract 和 durable reader pin handshake。
reader pin 通过同一 `(processRunId, object)` durable lease 复用本地并发读，并在每次 admission 后重新验证
ACTIVE root 与调用方 selection；失败会条件清理刚写入的 lease。durable protection manager 也已实现
create/root/owner post-check、same-key owner transfer、owner-authorized release 和 response-loss fail-safe veto。
metadata checkpoint 还新增了全分片内存 Oxia backend、generation/registration/physical-root/conditional-delete
store contract tests、record contradiction tests，以及 production/fake 共用的 physical-root lifecycle transition validator；
generation index/task 的 create-response-loss recovery 会核对 immutable identity，checkpoint 同版本则核对 policy digest。
当前 registry 已冻结覆盖全部 lifecycle/optional branch、retirement-journal 和 activation schema 的 49 个 envelope vectors，并把 generation index、task、
checkpoint、retention stats、stream registration 和 recovery root 的 ordinary CAS identity/monotonic transition
guards 接入 production adapter。真实 Oxia gate 进一步验证了 slash-aware fixed-depth key ranges、restart、CAS、
pagination 和 conditional metadata delete；pinned LocalStack gate 验证了 guarded upload 必须同时等待 SDK response
和 exact declared bytes、精确 HEAD，以及仅在 conditional DELETE 返回 HTTP 405/501 时于同一 deadline 内降级为
无 `If-Match` DELETE。`phase4M1Check` 与 `phase4M1FinalCheck --rerun-tasks` 已于 2026-07-15 通过，
因此 F4-M1 已 final-gated。F4-M2 当前已落地 view-aware reader/result contract、exact target-key registry 与
maximal-run dispatch、view-scoped generation allocator、严格 higher-generation index hydration、old/new exact
candidate lookup、physical identity root/manifest resolution，以及每次请求都执行 authoritative scan 的
`GenerationReadResolver`。resolver 已实现 4096 candidate hard limit、最高 COMMITTED generation 选择、PREPARED
不可见、COMMITTED/TOPIC_COMPACTED view 隔离、unknown format fail-closed、exact index/head post-pin revalidation 和
仅限同一 view 的 admission fallback；generation-zero repair seam 与 durable reader lease ownership 继续复用
F4-M1 contract。`ReadCoordinator` 现已接入 pinned range：lease 覆盖 exact-reader IO 与 terminal cleanup，永久
missing/checksum corruption 会先释放 lease、best-effort quarantine exact physical root、选中的 higher index
以及扫描中发现的所有同 stream/view、同 object-key `COMMITTED` index，再通过 per-operation
exclusion 执行 fresh same-view fallback。可重试的 `OBJECT_READ_FAILED` 在每个 candidate 上默认允许两次
fresh resolve/read 重试，达到可配置阈值后才 fallback，且不写 quarantine metadata。
`nereus-materialization` 同时已落地严格的 policy/source/task/output 领域值、canonical source/policy/task
identity、128-bit-or-stronger publication id、durable task/output/index mapper、exact HEAD + full-format verifier
组合接口、单次异步操作共享的 monotonic deadline，以及 restart-safe `DefaultGenerationCommitter`。发布器
将 publication id 和 generation 分别冻结到 durable task，创建 deterministic `PREPARED` index，在每次
重进时重新验证 output/head/source index/source root/activation/protection，并仅以 exact
`PREPARED -> COMMITTED` version-CAS 作为可见性点。发布结果丢失、并发发布、已发布重进和经证明的
`ABORTED` 重分配已有生产 codec/CAS adapter 测试；visible-output protection 的 owner 从 task
精确移交到 committed index。`GenerationPublicationReconciler` 提供已恢复 task/output pair 的幂等重进入口；
扫描器属于 M3 worker orchestration。真实 Oxia/LocalStack final fixture 已验证两个独立 runtime 的并发发布、
COMMITTED CAS 成功响应丢失后的进程重启与同一 publication/generation 收敛，以及 higher object 丢失后的
exact root/index quarantine、lease 清理和同 view generation-zero fallback。该门禁同时暴露并修复了 inline
`EntryIndexRef` 在 durable codec round-trip 后按数组引用而非内容比较的问题。`phase4M2Check` 与
`phase4M2FinalCheck --rerun-tasks` 已于 2026-07-15 通过，因此 F4-M2 已 final-gated。
F4-M3 的首个实现 checkpoint 已加入 pinned Parquet/Hadoop/ZSTD
依赖、真实 `PAR1` NCP1/NTC1 schema/metadata writer、close-owned replayable staging result、footer reference，
以及基于 bounded exact object-store ranges 的严格 reader 和独立 NTC1 facade。whole-file CRC32C/SHA/key
verifier、task-aware materialization bridge 与携带调用 `StreamId` 的 core NCP1 adapter 也已落地；golden、
random dense/ZSTD+uncompressed、corruption、streaming/staging、sparse NTC1、cross-stream 和 task-policy binding
tests 均通过。随后落地的第二个 M3 checkpoint 又加入 deterministic policy version、bounded whole-index DAG
planner、fixed-point/overlap suppression、exact-source task create revalidation、64 KiB durable task limit、完整
immutable policy snapshot、claim-expiry/publication recovery、per-stream task scanner 和全 64 分片 registered-stream
scanner。当前配置变化不会再改变或阻塞旧 durable task 的恢复；不同 broker 时钟生成的同一 deterministic
task 也会在 create-response-loss 路径收敛。第三个 M3 checkpoint 又实现了 stream-scoped exact-source reader、
durable read pin 内的 exact index/root/format/offset 再验证、single-source-at-a-time 无损 row publisher、
claim/heartbeat 驱动的 worker、guarded if-absent upload、严格 full-output verification、source/output task
protection 以及 typed durable failure transition。它已通过真实 Parquet + local object-store 的 `CLAIMED ->
OUTPUT_READY` 聚焦测试。第四个 M3 checkpoint 又为 durable protection manager 增加了仅限同
logical owner key、拒绝版本回滚的 `acquireOrTransfer`，并通过
`DefaultMaterializationTaskProtectionReconciler` 在 recovery/publication 中从 durable task 重建 source/output
identity/reference id，收敛 `CLAIMED/OUTPUT_READY/PUBLISHING` owner。重复 expired-claim CAS、跨状态
crash cut 和 protection CAS response loss 测试已通过。第五个 M3 checkpoint 又实现了严格
`MaterializationConfig`、从 authoritative committed indexes 单调推进的 advisory checkpoint reconciler、
全局/per-stream 有界且公平的 dispatcher，以及支持 non-overlap、hint coalescing、deadline cancellation 和
draining close 的 service lifecycle；配置关系、checkpoint CAS response loss、调度公平性与 close race 均有
聚焦测试。第六个 M3 checkpoint 又以 managed-ledger cross-layer test 证明 unbatched、compressed batched
Pulsar Entry 的 exact bytes/properties/ordering key 和 middle-batch MessageId 经 NCP1 往返不变，并保持
generation-level projection identity 不进入原有 per-entry `ReadBatch` surface。第七个 M3 checkpoint 又实现了
protocol-neutral topic-compaction decoder/strategy SPI、closed disposition wire ids、immutable compaction-key
facts 与 exact frozen-identity registry；duplicate/mutable identity 不能改变 durable policy semantics。
第八个 checkpoint 又实现了 terminal workflow-metadata retirement：PUBLISHED 路径验证 exact
COMMITTED index、checkpoint、root 与 index-owned visible protection，清理临时 task protections 后重读
全部事实并 conditional-delete task；failed terminal task、stale stats 和 old-policy checkpoint 也按
bounded prefix proof 收敛，protection/task delete response loss 可精确恢复。topic-compaction execution
engine checkpoint 随后固定 topic task 的 sources 为 lossless COMMITTED indexes，并将 target fixed-point scan
与 source scan 分离；tagged-v1 用独立 tag 编码 decoder key 与 unkeyed offset，消除 namespace collision。
pass one 在共享 `StagingFileManager` byte budget 下产生 SHA-verified sorted runs 和 bounded survivor bitmap，
pass two 重读 exact sources、重证 decoder fact digest 并按 offset 写出 sparse NTC1。worker 复用既有
claim/protection/heartbeat/guarded upload/strict verification/OUTPUT_READY 协议，publication 只进入
TOPIC_COMPACTED view。forced-spill、decoder drift、real Parquet NTC1 worker 和 view-isolation tests 已通过。
`phase4M3Check` 与 `phase4M3FinalCheck --rerun-tasks` 已于 2026-07-15 通过。最终门禁以真实 Oxia 与
LocalStack 验证两个独立 worker 的 durable-claim 收敛、完整 NCP1 upload/read/verification、进程重启复用、
`OUTPUT_READY` CAS 成功响应丢失恢复，并以 deterministic tests 验证 failure classification、全 64 分片逐页扫描、
watch/process-local hint 丢失和无 task 的 committed head 重建。F4-M3 已 final-gated；production rollout 仍保持
disabled，直到 M4–M6 完成 recovery-root/anchor-aware reachability、physical GC、async/Pulsar wiring 与最终兼容门禁。
Phase 4 整体仍为 `Implementation in progress`；physical delete 和 async profile 均未开放。

本目录是 Future 4 的代码级实现合同。Phase 4 把已经提交的 generation 0 物理布局转换为
per-stream、read-optimized 的 higher generation，并补齐 reader pin、source retirement、recovery checkpoint、
physical object GC、Object-WAL async materialization 和 Pulsar retention 接线。它不改变
`streamId + offset`、producer append 线性化点、Pulsar `Position/MessageId` 或 F3 cursor ack truth。

Phase 4 的 correctness ownership 固定为：

```text
StreamHeadRecord + anchor-reachable commit/checkpoint
  = logical append/recovery truth

view-scoped generation-index record in COMMITTED state
  = physical target publication truth

CursorStateRecord + CursorRetentionRecord
  = ack/protection/logical-trim truth

PhysicalObjectRoot + durable protections + reader leases
  = physical deletion fence

task/checkpoint/stream registration/watch/object listing
  = workflow, progress hints, audit discovery; never visibility truth
```

## 1. Locked Inputs

| Input | Phase 4 lock |
| --- | --- |
| L0 append | successful append is primary-WAL durable and stream-head committed；head CAS remains the append linearization point |
| Generation 0 | current legacy/generic offset-index records remain readable；generation 0 stays implicit `COMMITTED` |
| Generic target | Phase 1.5 `ReadTarget` union and target codecs are reused；a compacted object is an `ObjectSliceReadTarget`, not a fake primary-WAL type |
| Pulsar projection | one `PULSAR_ENTRY_V1` Entry is one Nereus offset；`Position.entryId == offset` and batch index remains inside exact Entry bytes |
| Cursor/retention | F3 single-root cursor CAS、owner-session claim、`PROTECTION_PENDING`、`TRIM_PENDING` and read-only `CursorSnapshotInventory` are final-gated inputs |
| Oxia primitive | only per-key create/version-CAS/range-scan/watch is assumed；Phase 4 adds conditional delete but assumes no cross-key transaction |
| Object store | current immutable put/range-read/head is implemented；Phase 4 must add guarded streaming PUT, bounded list and idempotent delete before any GC is admitted |
| Current executable profile | `OBJECT_WAL_SYNC_OBJECT` only；Phase 4 may add `OBJECT_WAL_ASYNC_OBJECT`，not BookKeeper IO |
| Pulsar source interpretation | checkout is local master source；the declared `5.0.0-M1-SNAPSHOT` is not a published dependency |

Any change to either locked commit requires re-running document 01's member/call-path audit before implementation
continues. Compilation alone is not evidence that the retention, admin, capability or deletion boundary is unchanged.

## 2. Design-gate Outcome

The Phase 4 code-level design closes the following choices：

1. `COMMITTED` higher generations are byte-preserving physical replacements. For
   `PULSAR_ENTRY_V1`, every offset returns the exact original complete Entry bytes；split、merge、rebatch and
   re-encoding are forbidden.
2. Lossy key compaction uses a separate `TOPIC_COMPACTED` view, keyspace, range type and reader API. It never
   outranks or serves as fallback for `COMMITTED`.
3. A higher generation is visible only after one `GenerationIndexRecord` at its final view/index key is CASed
   `PREPARED -> COMMITTED`. The task state and object upload are not visibility points.
4. Generation numbers are allocated by one view-scoped per-stream CAS counter. Gaps are valid；reuse is forbidden，
   so every reader has a total, deterministic physical ordering.
5. Ordinary resolve remains in `nereus-core`. The new `nereus-materialization` module produces physical facts but
   does not become the append/read correctness owner.
6. Resolve-to-read acquires a durable per-object/per-process lease, then revalidates both object lifecycle epoch and
   exact generation/index identity before IO. A JVM cache reference is not a deletion fence.
7. Every task/generation/recovery/cursor/catalog protection uses create-then-root-revalidate against a single
   `PhysicalObjectRootRecord`. GC marks that root before denying new references, drains bounded reader leases，
   revalidates all registered domains, then deletes.
8. Generation-zero append is split into intent preparation and protected head CAS. `REACHABLE_APPEND` is durable
   before head visibility；the exact index gains `VISIBLE_GENERATION` before strict success. Async success skips only
   index wait, never root/protection establishment.
9. Compacted output identity is generation-neutral and contains both content SHA-256 and a durable output-attempt id；
   a deleted physical key is never resurrected, while the same surviving output may be reused by publication repair.
10. Primary generation-0 bytes and old commit-log records are not reclaimable merely because a higher generation
   exists. A versioned recovery checkpoint object/root must first replace their append-recovery and derived-index
   repair role.
11. F4 consumes F3 logical trim only through `CursorRetentionCoordinator.requestTrim` under the current writable
   owner session. `PROTECTION_PENDING` and `TRIM_PENDING` veto a new trim/GC decision；snapshot inventory alone never
   authorizes deletion.
12. Phase 4 enables async materialization only for `OBJECT_WAL_ASYNC_OBJECT`. BookKeeper writer/reader/ledger
   retention remains a later, separate profile implementation that reuses this protocol.
13. Pulsar retention and compatible backlog eviction may be admitted only behind a separately negotiated
    `nereus.generation-protocol=1` capability. Pulsar compaction/offload/truncate/read-compacted remain rejected in
    Phase 4；full topic-compaction compatibility belongs to F8.
14. Physical roots use 256 deterministic shards, so restart recovery can enumerate `MARKED/DELETING` lifecycle
    truth even after bytes disappear. Object listing only discovers missing-root orphans/audit candidates；every
    deletion is proved from authoritative metadata and a stable reference-domain registry snapshot.
15. Background work discovery uses a fixed 64-shard `MaterializationStreamRegistrationRecord` registry. The record
    is installed before marker/async admission, but every scanner reloads the current projection、stream head、index
    and task prefix；the registry is never append、visibility or deletion truth.
16. Planner sources are a canonical non-overlapping tiling of complete index ranges. A healthy single
    current-policy higher generation is a fixed point；it is never rewritten just to create a larger generation id.
17. A `DELETED` physical root is logically terminal but not permanent metadata. After a long cross-validated grace,
    two exact HEAD-absence windows and two unchanged owner/domain proofs, F4 conditionally removes Phase 1 object
    references/manifest and finally the root. Every first/retried PUT is owner/root guarded and a new attempt uses a
    fresh key, so bounded audit metadata does not reopen deleted identity reuse.

There is no implementation-blocking protocol question left in this design set. Values such as worker concurrency，
target file size and scan interval remain configuration defaults, not durable-format choices.

## 3. Scope

Phase 4 production scope is：

- view-scoped generation allocator、generation index record and conditional publication；
- lossless `COMMITTED` higher-generation resolve、fallback and quarantine；
- `NEREUS_COMPACTED_PARQUET_V1` writer/reader and strict opaque-payload round trip；
- durable stream registration、task、worker claim、checkpoint、repair and lag/backpressure；
- `OBJECT_WAL_ASYNC_OBJECT` `WAL_DURABLE` append boundary and read-after-success repair；
- recovery checkpoint object/root and safe commit/index/source retirement；
- physical object registry、durable task/generation/recovery protections and reader leases；
- object-store list/delete、mark/drain/delete/tombstone GC state machine；
- cursor snapshot orphan/live-reference GC using the F3 inventory and final root revalidation；
- Pulsar policy/admin logical trim wiring and physical-retention scheduling；
- a storage-level `TOPIC_COMPACTED` primitive with a separate sparse format/index；
- capability rollout、metrics、limits and deterministic/real-service release gates。

## 4. Explicit Non-scope

Phase 4 does not implement：

- a BookKeeper primary WAL adapter or any BookKeeper profile；
- Pulsar read-compacted subscription behavior、compaction admin/status contract or stock compactor replacement；
- Kafka/KoP fetch、group or topic-compaction compatibility；
- transaction visibility、pending ack、deduplication、geo-replication or delayed delivery；
- Pulsar offload APIs、topic truncate、shadow topics or migration；
- Future 6 catalog commit、SBT/SDT delivery or table schema evolution；
- cross-region object replication or object-store versioning policy；
- benchmark/chaos certification before the functional final gate。

The storage-level `TOPIC_COMPACTED` primitive is deliberately not broker admission. F8 must consume it without
changing the view/index/offset contract defined here.

## 5. Repository Boundary

Phase 4 adds one protocol-neutral module：

```text
nereus-api
  ReadView and view-neutral identities only

nereus-metadata-oxia
  F4 keys, records, codecs, CAS/range/delete store surface

nereus-object-store
  compacted/checkpoint formats plus guarded streaming PUT and list/delete primitives

nereus-core
  committed generation resolver, target-reader dispatch, pin-before-read integration,
  WAL_DURABLE read repair, stream recovery interpretation, physical-object lease/protection
  handshake and reference-domain SPI

nereus-materialization                 (new module)
  planner/task/worker/publication/checkpoint/recovery/GC orchestration

nereus-managed-ledger
  cursor snapshot reference domain, retention candidate bridge and logical-trim callback

nereus-pulsar-adapter + local Pulsar fork
  runtime composition, config/capability, topic-policy/admin admission
```

`nereus-materialization` depends on API/core/metadata/object-store and must not depend on Pulsar classes. Core never
depends on `nereus-materialization`；placing the lease/protection/reference SPI in core keeps this edge acyclic.
`nereus-managed-ledger` registers the F3 cursor and F2 projection-generation reference domains through the F4 SPI；
the F4 module never parses a Pulsar cursor/projection root itself. This avoids a dependency cycle and preserves the
current L0 protocol-neutral gate.

## 6. Document Map

| Document | Purpose |
| --- | --- |
| [01-current-contract-and-source-audit.md](01-current-contract-and-source-audit.md) | exact Nereus/Pulsar source lock、current gaps、call paths and code ownership |
| [02-domain-api-and-object-format.md](02-domain-api-and-object-format.md) | target Java API/domain records、reader dispatch、Parquet/sparse/checkpoint object formats |
| [03-oxia-metadata-and-publication.md](03-oxia-metadata-and-publication.md) | keyspace、record fields、binary codecs、generation publish and resolver contract |
| [04-task-recovery-async-and-checkpoint.md](04-task-recovery-async-and-checkpoint.md) | planner/worker state machines、async profile、recovery checkpoint and crash repair |
| [05-reader-retention-and-gc.md](05-reader-retention-and-gc.md) | durable read pins、reference handshake、logical retention、source/snapshot/object GC |
| [06-pulsar-rollout-operations-and-compatibility.md](06-pulsar-rollout-operations-and-compatibility.md) | exact broker/facade changes、capability rollout、policy/admin matrix、F5/F6/F8 handoff |
| [07-implementation-plan-and-gates.md](07-implementation-plan-and-gates.md) | M0-M6 file inventory、tests、failure matrix、ordinary/final release gates |

The north-star summary remains
[nereus-future4-compaction-generation.md](../design/nereus-future4-compaction-generation.md)；when it conflicts with
an implementation detail in this directory, this directory owns the Phase 4 target until production code exists.
After implementation, code + executable tests become the highest authority and both document sets must be updated in
the same change.

### 6.1 F4-M1 implemented checkpoint

The completed F4-M1 checkpoint contains the view/generation/publication/object-key-hash API values, the
`nereus-materialization` module boundary, F4 Oxia keyspace/scan tokens, all ten M1 record families and binary-v1
codecs, production generation/physical-object metadata-store surfaces, version-conditional delete, and guarded
replayable PUT/list/delete. The object-store checkpoint now also includes owner-only bounded staging files,
backpressure-aware replay, deterministic guarded-retry tests, exact base64-prefix expansion, opaque cross-prefix S3
continuations, whole-operation PUT cancellation/deadlines, streaming local-fixture upload and exact HEAD-before-
DELETE validation. S3 success joins the SDK response with exact declared-byte delivery and CRC32C；conditional
DELETE falls back without `If-Match` only after HTTP 405/501 and within the original deadline. Existing object-store
implementations remain source-compatible through fail-closed default methods for new optional operations.

The core checkpoint now additionally implements strict physical/reference proof values、the protocol-neutral
activation-proof contract、durable reader leases and durable protections. Protection acquisition cleans up only a
version whose successful response proves local creation；response-loss recovery and failed transfer post-checks leave
a safe deletion veto. Same-key owner transfer and owner-authorized conditional release have deterministic race tests.

The metadata checkpoint also runs the generation、64-shard stream-registry、256-shard physical-root and exact
conditional-delete contracts against a deterministic partition-aware backend. It verifies response-loss-safe
generation/task create recovery, policy-digest collisions, pagination/token scope, exact version/digest deletion and
legal physical-root lifecycle/epoch transitions. Production and fake physical stores use the same transition
validator, so test doubles cannot admit a state change rejected by the Oxia adapter. Production ranges use Oxia's
slash-aware fixed-depth bounds rather than Java-string trailing-slash successors.

M1 froze ten F4 record families and 43 envelope vectors；checkpoint L extends the registry to thirteen families and
46 vectors with the retirement-journal schema，and checkpoint S extends it to 49 vectors with activation lifecycle/
capability states. The
stream-scoped production adapter reloads the exact expected version before applying closed index/task transitions、
monotonic checkpoint/registration progress、immutable retention boundaries and monotonic recovery-root publication.
The task durable shape explicitly represents `PUBLISHING(publicationId, generation=empty)` before the same-state CAS
attaches the first allocation. Focused real Oxia covers restart、CAS、fixed-depth pagination and conditional metadata
delete；pinned LocalStack covers guarded upload completion、list、HEAD identity and conditional-delete compatibility.

Both `phase4M1Check` and `phase4M1FinalCheck --rerun-tasks` passed on 2026-07-15, so F4-M1 is final-gated. This is
deliberately not a Phase 4 completion claim. No generation publication, resolver, materialization worker, retention
or GC behavior is enabled by M1；generation publication/read arrives in M2 and the remaining execution paths are
F4-M3–M6 work.

### 6.2 F4-M2 implemented checkpoint

The completed F4-M2 checkpoint adds authoritative view-scoped candidate resolution, strict committed-index
validation, exact reader dispatch, durable read-pin lifetime, bounded same-view fallback/quarantine, and the
restart-safe generation publication state machine. Publication freezes one task-owned publication id and generation,
creates one deterministic `PREPARED` index, repeats all publication proofs after recovery, and exposes visibility
only through the exact `PREPARED -> COMMITTED` version-CAS. Lost responses and concurrent publishers converge on
the same durable result；visible-output protection transfers from the task to the committed index.

`phase4M2Check` and `phase4M2FinalCheck --rerun-tasks` passed on 2026-07-15. The final gate uses real Oxia and
LocalStack to cover independent runtimes, response loss plus restart, exact object verification, durable pins,
quarantine, and same-view generation-zero fallback. F4-M2 is complete/final-gated. This remains a milestone claim,
not a Phase 4 completion claim：M3 owns compacted object format/planner/worker and task source/output protections；
M4 owns recovery-root/anchor-aware retirement and deletion eligibility, so physical deletion remains disabled.

### 6.3 F4-M3 implemented checkpoint

F4-M3 is complete/final-gated. Its ordinary gate covers the frozen NCP1/NTC1 format、planner fixed point、durable
task/claim/recovery model、exact-source worker、task protections、checkpoint/service lifecycle、topic-compaction engine
and isolated publication contracts. `phase4M3FinalCheck --rerun-tasks` passed on 2026-07-15 and adds real Oxia plus
LocalStack evidence for two independent workers、claim ownership、full output bytes、restart reuse and lost
`OUTPUT_READY` response convergence. The deterministic registry suite forces page size one over two registrations in
every one of the 64 shards, then repeats the pass with a fresh scanner and no watch/process-local hints. The final gate
also corrected its M2 prerequisite assertion to the protocol fact that a terminal task may retain a temporary output
veto owned by an earlier non-terminal version of the same task key until proof-driven retirement removes it.

This is not a Phase 4 completion claim. Higher-generation production activation and physical deletion remain off；M4
must first deliver recovery checkpoints、anchor-aware retirement and GC, followed by M5 async/Pulsar wiring and M6
compatibility/scale closure.

### 6.4 F4-M4 NRC1 object-protocol checkpoint

F4-M4 的首个实现 checkpoint 已落地 `RecoveryCheckpointWriteRequest/Publication/Entry/Directory/Object/WriteResult`
领域值、`RecoveryCheckpointCodecV1` 与 `DefaultRecoveryCheckpointCodecV1`。NRC1 使用 big-endian `NRC1/NRF1`
header/footer、逐 record CRC32C、body SHA-256 和 complete-object SHA-256；canonical key 同时绑定 cluster、stream、
sequence、content SHA 与随机 attempt id。writer 对 publications/entries 各只保留一个 outstanding item，目录和
publication coverage facts 写入共享 staging budget 下的 owner-only spill；百万级路径不会将全部 metadata rows
聚合进 heap，entry reference 校验复用单个受控随机读句柄。

`openAndVerify` 以同一 monotonic deadline 执行 HEAD、footer/header/directory bounded reads 和 1 MiB 分块的
whole-object hash；publication lookup 使用目录二分，commit lookup 使用 stride 256 的 sparse block，并严格校验
record CRC、embedded digest、coverage refs。`MetadataRecoveryCheckpointVerifier` 通过 authoritative F4 codec
解码 canonical `GenerationIndexRecordCodecV1` bytes 与 generic `StreamCommitTargetRecord` envelope，强制
`metadataVersion=0`、COMMITTED view/lifecycle 和所有 duplicated identity facts 相等。golden/attempt identity、
self-consistent corruption、one-at-a-time/cancellation、513-entry three-block sparse lookup 及 metadata semantic
tests 已加入；`phase4M4CheckpointCheck --rerun-tasks` 已于 2026-07-15 通过 114 个 tasks，是该中间边界的
ordinary gate。

这仍不是 F4-M4 完成声明：recovery-root coordinator/CAS、anchor-aware replay/repair、source/index retirement、
physical/cursor GC 与真实 Oxia/LocalStack M4 final gate 尚未落地，任何删除与 higher-generation production
activation 继续关闭。

### 6.5 F4-M4 protected generation-zero append checkpoint

F4-M4 的第二个实现 checkpoint 已把 strict Object-WAL append 从原先的一步 metadata commit 改为
`prepareStableAppend -> REACHABLE_APPEND -> commitPreparedStableAppend -> materializeGenerationZero ->
VISIBLE_GENERATION`。`PreparedStableAppend` 和 `MaterializedGenerationZero` 分别携带 exact durable key/version/
value SHA；raw head CAS 会重新读取 intent、ACTIVE physical root 与 permanent protection 后才允许更新
`StreamHeadRecord`。`AppendCoordinator` 的普通 append 与 `recoverAppend` 都执行同一序列，head 响应丢失后保留
commit-owned protection，strict success 则必须重新证明 exact index-owned protection。

生产 runtime 现在通过同一 `SharedOxiaClientRuntime` 装配独立的
`OxiaJavaPhysicalObjectMetadataStore`、`DefaultObjectProtectionManager` 与
`DefaultGenerationZeroPhysicalReferencePublisher`，并在 stream-storage drain 后按依赖顺序关闭。fake store 同时
实现 L0 与 physical metadata 契约；旧 metadata-only 动态代理测试显式暴露 physical interface，避免测试绕过
production handshake。普通 metadata/core/pulsar unit tests 已通过，real-Oxia 与 F4-M2/M3 integration source
sets 已完成新协议编译迁移；`phase4M4ProtectedAppendCheck` 已于 2026-07-15 通过完整前置回归链和本地
Pulsar M4 check。Docker-backed M4 execution evidence 仍属于后续 checkpoint/final gate。

这个 checkpoint 只闭合新写入的 physical-reference 空窗。它不发布 NRC1 recovery root、不退休旧 commit/index
证据，也不授权任何 physical delete；因此 F4-M4 仍为 `in progress`。

### 6.6 F4-M4 anchor-aware checkpoint-planning foundation

第三个实现检查点的基础部分已提供 production/fake 共用的分页 `readAppendRecoveryTail` surface。每页都绑定
observed head、recovery-root anchor 和 continuation，按 newest-to-oldest 返回 exact durable key/version/source SHA，
并把 legacy commit canonicalize 为 metadata-version-zero generic NRC1 envelope。core 的
`AnchorAwareCommitWalker` 在一次有界 walk 前后重读 recovery root；root 变化时重启，root 稳定时才交付 live-tail
到 checkpoint anchor 的无歧义 bridge。

`RecoveryCheckpointBuilder` 现在从 grace-old、whole-commit、gap-free live prefix 中，只选择被 exact lossless
`COMMITTED` generation 覆盖的部分；generation scan 对所有 candidate 使用 4,096 hard bound，并生成 canonical
NRC1 write request/publication table/entries。publication 前的 `revalidate` 会重读 exact root、registration、selected
commit evidence 与 generation indexes。`RecoveryCheckpointProtectionManager` 已定义 base-root-owned bounded
pending protection，以及 published-root-owned checkpoint-object/target permanent protections；target root 校验绑定
object key、object id、kind 和完整读取范围。metadata contract、anchor walker 与 builder focused tests 已通过。

这仍只是 checkpoint C 的 planning/protection foundation：`RecoveryCheckpointCoordinator` 尚未执行 guarded
upload、NRC1 full verification、recovery-root CAS 和 crash-cut reconciliation；anchor-aware replay/repair、source
retirement 与任何 GC/delete path 也仍未开放。

### 6.7 F4-M4 guarded recovery-root publication checkpoint

Checkpoint D 已实现 `RecoveryCheckpointCoordinator` 与 `RecoveryCheckpointRootReconciler`。coordinator 在一个
monotonic operation deadline 内完成 activation admission、NRC1 streaming write、每次 provider transmission 前的
root/registration/commit/index revalidation、guarded if-absent upload、exact HEAD + full NRC1 verification、bounded
`RECOVERY_CHECKPOINT_PENDING`、root CAS 和 exact lost-response reload。CAS 是唯一 recovery-prefix publication 点；
失败或竞争不会让 object/task state 替代 root truth。

root CAS 成功后，reconciler 通过新增的 bounded `scanPublications` 逐页读取 immutable NRC1 publication table，
对 embedded raw generation bytes 与真实 Oxia envelope durable SHA 分别验证，并从 exact `COMMITTED` indexes/ACTIVE
physical roots 重建 current-root-owned checkpoint-object/target protections。进程若在 CAS 后、permanent protection
之前退出，重启会先完成该 reconciliation，再删除 deterministic pending protection。真实 in-memory Oxia adapter、
fake physical store 和 local object store tests 已覆盖 root CAS 成功响应丢失及 CAS 后进程退出/重启修复。

`phase4M4RecoveryRootCheck` 是这一中间边界的 ordinary gate；它不证明 checkpoint-aware append replay/index
repair、live commit/index/source retirement、physical/cursor GC 或 Docker-backed M4 final scenarios。上述路径和
所有 delete enablement 继续关闭，F4-M4 仍为 `in progress`。

### 6.8 F4-M4 checkpoint-aware append replay checkpoint

Checkpoint E 已实现 `CheckpointAppendReplayReader`。reader 先通过 `AnchorAwareCommitWalker` 验证 bounded live
tail；若目标位于已发布 prefix，则从 root 选择唯一覆盖 reference，取得 recovery-checkpoint durable read pin，
严格 `openAndVerify` NRC1，并用新增的 `findCommitCoveringOffset` 在 stride-256 sparse directory 中定位 entry。
读完后再次精确重读 root；pin/replay 期间 root 改变会放弃旧证明并整次重启，绝不从旧 object existence 推断
当前 recovery truth。

live 与 checkpoint bytes 现在共同调用 `AppendReplayRecords` 的 exact request-vs-record validation 和 hydration。
`AppendCoordinator` 通过 `AppendRecoverySearcher` seam 区分 live commit 与 current-root checkpoint evidence：live
命中继续执行 generation-zero repair/protection，checkpoint 命中直接返回原 append identity，避免重新依赖可能在
后续退休的历史 primary target。`DefaultStreamStorage` 已提供显式 checkpoint-aware searcher 注入口；旧构造器
仍选择 genesis/live-only adapter，因此在 M5 runtime composition 明确注入前不会静默改变现有执行路径。

`phase4M4CheckpointReplayCheck` 覆盖 513-entry/three-block offset lookup、batch 内 offset、live commit key 缺失、
root-at-pin change restart、commit-id non-aliasing 与 read-pin cleanup。该 checkpoint 不包含 checkpoint-derived
index repair、source/index retirement 或任何 GC/delete enablement，F4-M4 仍为 `in progress`。

### 6.9 F4-M4 checkpoint-derived index repair checkpoint

Checkpoint F 已实现 `CheckpointDerivedIndexRepairer` 与 `GenerationIndexRepairer` terminal seam。repairer 先读取
head/trim，再用 `AnchorAwareCommitWalker` 将目标明确路由到 bounded live tail 或 current-root NRC1 prefix。live
目标继续调用原有 `repairDerivedStreamIndexes`；checkpoint 目标取得 NRC1 durable read pin、严格验证 root/reference/
header/object identity，并按 commit entry 的最多 8 个 publication indexes 读取候选。候选必须是 canonical、
metadata-version-zero、COMMITTED-view、exact-covering record；repairer 按 generation 降序选择 ACTIVE physical root
仍精确匹配的最高候选。

恢复写入前会取得并反复重验 `GENERATION_PUBLISH` activation proof、当前 root、trim、ACTIVE physical root 和
current-root-owned `RECOVERY_CHECKPOINT_TARGET` protection。`GenerationMetadataStore.
restoreCommittedFromCheckpoint` 只允许 unhydrated COMMITTED record，并分别验证 NRC1 raw-record SHA 与 Oxia
durable-envelope SHA；put-if-absent 冲突或响应不确定时，只有完整 canonical value 和 envelope digest 相同才收敛。
root 在 pin/恢复过程中改变会整轮重启；offset 已 trim 时返回 `TRIMMED` 且不创建索引。repairer 只恢复 metadata
authority，随后 `GenerationReadResolver` 必须 fresh scan、pin 并再次验证，projection layer 不成为 correctness
owner。

`phase4M4CheckpointIndexRepairCheck` 覆盖索引删除后恢复、raw/envelope digest 分离、最高不健康候选回退、
target protection、trim no-write、root-at-pin restart、lease cleanup 和 resolver fresh rescan。旧 resolver 构造器
仍使用 live-only compatibility adapter；M4 runtime composition 尚未显式装配 checkpoint repairer，source/index
retirement 与所有 GC/delete enablement 继续关闭。

### 6.10 F4-M4 exact retirement metadata checkpoint

Checkpoint G 已实现独立的 `retirement` metadata package。`SourceRetirementMetadataStore` 严格区分 legacy
`CommittedSliceRecord` 与 generic `CommittedAppendRecord` marker，并分别支持 legacy/generic generation-zero index
和 commit-node 编码。每次删除都会用 `OxiaKeyspace` 重建 canonical key，在正确 stream partition 中重新读取 exact
stored envelope，严格解码并验证 key/value identity、encoded metadata version、调用方冻结的 Oxia version 与
durable-envelope SHA-256，最后才调用 `deleteIfVersion`。设计补充了只读 `getCommittedMarker`，使上层 retirement
plan 能冻结此前没有其他公开读取路径的 marker version/digest；它不创建记录，也不推断删除资格。

`ObjectAuditRetirementStore` 同样只读/条件删除 Phase 1 manifest/reference audit keys，返回 hydration 后的值以及
exact stored-envelope SHA。references 必须先于 manifest 删除；missing 或 delete-response loss 不会在 adapter
内部变成成功，上层必须在同一个 unchanged `DELETED` root proof 下重读并收敛。shared Oxia runtime 只向该包提供
`get + deleteIfVersion` 的 borrowed bridge；bridge 只接受包内可构造的 opaque exact-key capability，既不暴露
put/list/scan，也不允许其他 runtime caller 拼接任意 key 删除。

`phase4M4RetirementMetadataCheck` 覆盖两种 generation-zero index/marker/commit 编码、identity/version/SHA
contradiction、references-before-manifest、missing、response loss 和 close admission。该 checkpoint 只提供后续
coordinator 的 destructive primitive；physical root MARK/DRAIN/DELETING、reference-domain proof、metadata plan
revalidation 和 object delete 尚未实现，因此 physical deletion 仍完全关闭。

### 6.11 F4-M4 bounded GC plan checkpoint

Checkpoint H 已实现 `PhysicalGcConfig`、`GcCandidate`、`GcPlan` 与 secure GC id generator。配置严格验证 page、
并发、stream/domain 数量、精确毫秒 duration、lease renewal/drain/operation safety，并可与
`MaterializationConfig`、`StreamStorageConfig` 交叉验证 orphan/tombstone grace。默认值是
`enabled=false, dryRun=true`；只有同时 `enabled && !dryRun` 才允许未来 coordinator 进入 mutation，配置本身
不覆盖 cluster deletion capability。deadline 使用 `Math.addExact`；溢出返回 ineligible，而不产生环绕时间。

Candidate 初次只能从 exact `ACTIVE` root wrapper 建立，并冻结 root version/epoch、query/evidence 与 root 的最早
eligibility；重启后可从 exact `MARKED` wrapper 建立 `MARKED_RECOVERY` candidate，直接使用当前 version/epoch，
不猜测 MARK 前 version。Plan 只接受 canonical sorted/unique、配置有界、同 query 的 complete/non-veto domain snapshots，
且 protection 必须属于 candidate object/root epoch。每个 protection removal 冻结完整 owner/value、Oxia
version 和 durable-envelope SHA；其他 metadata removal 冻结 type/key/version/envelope SHA。
`referenceSetSha256` protocol v2 直接提交这些 exact removal facts、query identity，以及每个 domain 的
`(id, version, query SHA, snapshot SHA)`；domain snapshot SHA 已提交完整 authority/reference 事实。它不包含
随机 candidate id 或进程时间。MARK 前可计算同一
digest，MARK 后 `fromMarkedRoot` 只接受 exact attempt/digest/object、递增 metadata version 和 `epoch + 1`，所以
进程重启只能从 root 认证的 exact facts 重建。`GcPlan` 仍不持久化；但 DELETING 会逐项删除 source metadata，
因此进入 MARK 前必须先密封一份 sharded retirement journal。它只保存恢复证据，不能脱离同 object/attempt/digest
的 MARKED/DELETING root 自行授权删除。

`phase4M4GcPlanCheck` 覆盖配置关系、毫秒/overflow、128-bit entropy、canonical order、domain truncation/veto、
跨对象 protection、root attempt/digest mismatch 和重建稳定性。该 checkpoint 仍没有 domain 实现、root CAS、
metadata retirement 调用或 object delete；physical deletion 继续完全关闭。

Checkpoint L 已在 journal protocol foundation 之上实现 durable-store slice：除 `GcDomainSnapshotProof`、
manifest/protection/removal records、三种 binary-v1 codecs、46 个 F4 frozen envelope vectors，以及
snapshot/full-fact 与 compact-proof digest 等价测试外，`F4Keyspace` 现有 object/attempt-scoped fixed-depth keys，
`PhysicalObjectMetadataStore` 的 manifest create/get 与 entry create/paged-scan API 在 production in-memory Oxia
adapter 和 fake 中共享 exact create-or-reload/conflict/token-scope contract。`DefaultGcRetirementJournal` 先批量
create entries，再完整分页扫描并重算 reference-set-v2 digest，最后 create manifest；entry/manifest response loss
通过 exact reload 收敛，缺失、额外或冲突 entry 均 fail closed 且不能产生 manifest。

`PhysicalObjectGarbageCollector` 现在强制注入 `GcRetirementJournal`。`mark` 在 activation proof、ACTIVE-root reload
与 `markCas` 之前完成 `prepare` 并逐字段核对 object、attempt、query、domain proofs、removal facts 和 digest；任何
PREPARE 失败都使 root 保持 ACTIVE。`advanceToDeleteIntent` 在 drain admission 和最终 MARKED-root fence 各执行一次
exact `load`，journal 缺失或变化会使 future fail closed 且 root 保持 MARKED，不能进入 DELETING。
`phase4M4RetirementJournalCheck` 覆盖 metadata production/fake parity、manifest-last/restart/response-loss，以及
上述 PREPARE/final-reload crash cuts。DELETING recovery coordinator 尚未消费 journal 并执行 retirement，因此本
checkpoint 仍不授权 source、protection、metadata 或 object delete，physical deletion 继续关闭。

### 6.12 F4-M4 reference-domain and root-fence checkpoint

Checkpoint I 已实现 `GcReferenceDomainRegistry`、`GcPlanMetadataRevalidator`、
`PhysicalObjectGarbageCollector` 的 fence-only 路径和 `PhysicalObjectRootScanner`。Registry 在一个共享 absolute
deadline 内按 canonical `(domainId, protocolVersion)` 顺序收集完整 domain set，严格校验返回 identity，并把
`VETOED`、`INCOMPLETE`、`LIMIT_EXCEEDED` 保留为不同的非授权结果；`stillMatches` 只接受与本地注册集合完全
相同的 `CLEAR` collection，逐 domain 重新读取 authority，任何 `false` 都立即停止后续调用。Collector 构造时
强制存在 exact `projection-generation-v1@1`，且 metadata removal facts 必须通过注入的 revalidator 重新读取，
不能由 plan 自证正确。

`mark` 只接受已满足其 `notBefore` 的 `ACTIVE_DISCOVERY` candidate；该时间由上游资格计算包含
`sourceRetirementGrace` 或对应 candidate grace。MARK 自身把 `deleteNotBefore` 固定为
`markedAt + drainGrace`。在同一 operation deadline 内，它重取 complete domain snapshots、逐页精确比较
versioned protections、重验每个 planned metadata fact、取得并重验 `PHYSICAL_DELETE` activation proof，最后才
对 exact ACTIVE root 执行 `ACTIVE -> MARKED(epoch + 1)` CAS。MARK response loss 只有在 reload 到同一
object/attempt/digest/epoch/value 时收敛。

`advanceToDeleteIntent` 等待 drain grace，逐页扫描 reader leases 并等待到所有
`expiresAt + maximumClockSkew` 之后，再重验 protections、metadata facts 和全部 domain authority；在最终 CAS
前再次扫描 lease/protection/metadata、重读 exact MARKED root 并 revalidate activation proof。任一 fact drift
都会在尚无 destructive side effect 时把同一 root `MARKED -> ACTIVE(epoch + 1)`；CAS response loss 通过 exact
reload 收敛。所有证明稳定后仅写入保留 attempt/digest 的 `MARKED -> DELETING(epoch + 1)` durable intent。
本 checkpoint 不调用 source/audit metadata delete、protection delete 或 object delete，也不把 scanner/collector
装配进 production runtime。

`PhysicalObjectRootScanner` 以 bounded page 顺序扫描固定 `000..255` root shards，验证跨页 key 单调性、串行
调用 visitor，并返回每个 lifecycle 的 exact count。它拒绝 overlapping pass，close 后拒绝新 pass，但不关闭
borrowed metadata store/scheduler；因此后续 recovery 可以从 metadata root truth 找回 `MARKED/DELETING`，而不
依赖 object-store listing。`phase4M4RootFenceCheck` 覆盖 domain ordering/blocker/timeout、MARK 与 delete-intent
lost response、grace/lease 边界、protection/metadata/domain drift unmark、MARKED restart、全 256 分片分页与
“未执行任何删除”边界。具体 domain implementations、DELETING recovery/retirement/delete 和 runtime
composition 仍是后续 M4 工作。

### 6.13 F4-M4 query-bound reference-domain checkpoint

Checkpoint J 修正了 concrete domain 实现前暴露出的两个协议缺口。`GcReferenceDomain.stillMatches` 现在同时
接收 exact `GcReferenceQuery` 和 snapshot；registry 必须把 collection 中的原 query 传回 domain，因此重启后
可以仅从 authoritative query/root facts 重建验证，不依赖 process-local query cache。`GcPlan` 又新增一条
结构性约束：每个 non-veto domain `GcReference` 的 `(ownerKey, ownerMetadataVersion,
ownerIdentitySha256)` 必须被某个 `GcPlannedMetadataRemoval` 精确覆盖。仅把 reference 列入 digest、却不安排
删除 owner record 的 plan 无法计算 MARK digest。

本 checkpoint 实现了三个 metadata-backed domain：

- `GenerationReferenceDomain` 对每个 affected stream 完整扫描 `COMMITTED` 与 `TOPIC_COMPACTED` 两个 view；
  generation-zero 非 tombstone index 可以进入 exact removal plan，higher-generation 只有已先 CAS 到
  `DRAINING` 才可移除，`PREPARED/COMMITTED/QUARANTINED` 引用直接 veto；
- `AppendRecoveryReferenceDomain` 读取 optional recovery root、把 root absence 也编码成 authority token，并从
  root anchor 完整分页读取 live append tail。当前 root 仍引用的 NRC1 object 或 live commit 仍指向的 source
  object 都直接 veto；head/root/commit 变化会使 exact rescan 不匹配；
- `MaterializationReferenceDomain` 完整分页扫描 affected-stream task roots。`PLANNED/CLAIMED/OUTPUT_READY/
  PUBLISHING/RETRY_WAIT` 的 matching source/output 是不可计划删除的 veto；只有 terminal task 不再成为
  physical correctness reference，terminal workflow cleanup 仍由既有 retirer 负责。

三个 domain 都在配置上限内保留 canonical authority/reference lists，超过上限返回 incomplete+veto，而不是
截断为 permission。它们对 `OWNERLESS_ORPHAN_CANDIDATE` 明确返回 incomplete+veto：当前 stream registration
只是 discovery hint，不能被误用为 cluster-wide absence proof；必须等待后续 physical-root/registration backfill
epoch 和 projection/cursor/future-domain global enumeration。`phase4M4ReferenceDomainsCheck` 覆盖 query
回传、reference/removal binding、两 view scan、DRAINING gate、live-tail/task veto、authority drift 与 ownerless
fail-closed。作为 checkpoint-J gate，它不开放 projection/cursor/future-sentinel domains、source plan
construction、DELETING side effects 或 runtime composition；checkpoint K 的新增范围如下。

### 6.14 F4-M4 managed-ledger projection/cursor reference-domain checkpoint

Checkpoint K 将此前 materialization package-private 的 snapshot accumulator 替换为 core-owned
`GcReferenceDomainConfig` 与 `GcReferenceSnapshotBuilder`。所有 metadata-backed domain 现在共享相同的 page、
authority、reference hard limits 和 canonical ordering；第 `max + 1` 个事实只把 snapshot 变为
`complete=false/veto=true`，不会把截断前缀解释为 deletion permission。`nereus-managed-ledger` 只依赖 core 与
metadata，不形成 managed-ledger -> materialization correctness dependency。

F2 projection authority 新增 `ManagedLedgerProjectionMetadataStore.getProjectionByStream(cluster, streamId)`。
它先精确读取 per-stream `VirtualLedgerProjectionRecord`，再按 binding 中的 managed-ledger name 精确读取当前
`TopicProjectionRecord`；两个 wrapper 都携带 canonical key、Oxia version 与 exact stored-envelope SHA-256。
缺 binding 或缺 current topic 都被编码为 domain-separated absence authority，并且 veto。当前 topic 与历史
binding identity 相同时，只有 `DELETED` 或 `nereus.generation-protocol=1` 允许 compatibility proof；`DELETING`
始终 veto。identity 不同时，只有 incarnation 与 storage-class binding generation 都严格增大，才能证明旧
stream 已不可寻址。topic 已发布但 derived binding 尚未修复的 crash cut 因 binding absence 而 fail closed。

generation marker 由新的 `ManagedLedgerProtocolProperties` 与已有 cursor marker 组合校验：external reads 隐藏
两者，external replacement 保留两者，未知 `nereus.*`/`PULSAR.SHADOW_SOURCE` 仍被拒绝。
`activateGenerationProtocol` 只对 exact projection identity/version 做单 key monotonic CAS；response loss 通过
重读 exact topic authority 收敛。该 checkpoint 只提供 marker/CAS 基础，并未实现 M5 cluster activation record、
registration barrier 或 broker runtime activation。

`CursorSnapshotReferenceDomain` 对每个 affected stream 精确读取 F3 retention root 并完整分页 cursor roots。
retention absence 是显式 authority；非 `ACTIVE` retention、retention/cursor projection identity 不一致、缺
retention 但存在 cursor，以及匹配当前 candidate object 的 ACTIVE snapshot root 都 veto。每个 retention/root
authority 使用 exact F3 envelope SHA-256；live root 还产生 owner-bound `cursor-snapshot-root` reference。
`stillMatches` 全量重跑同一 query，因此 retention lifecycle、root、projection 或版本漂移都会使 drain 失败。

`ProjectionGenerationReferenceDomain` 与 `CursorSnapshotReferenceDomain` 都对 ownerless query 返回
incomplete+veto；本 checkpoint 没有把 registration hint 当作 global absence truth，也没有安装 runtime
registry。`phase4M4ManagedLedgerDomainsCheck` 覆盖共享 builder、marker preservation/lost response、F2 exact
authority、projection recreation/derived-repair crash cut、F3 paged roots、pending retention、authority drift、
module boundary 与 ownerless fail-closed。Future catalog sentinel/global enumeration、source plan construction、
DELETING side effects 和 runtime composition 仍未开放。

### 6.15 F4-M4 root-authenticated destructive recovery checkpoint

Checkpoint M 实现 `SourceRetirementCoordinator`、closed `GcMetadataRetirementRegistry` 与显式 deletion result。
入口只接受 `DELETING/DELETED` root；disabled/dry-run 在任何读取前返回。可变路径先重读 exact root 和 sealed
journal，拒绝未注册 removal type，然后按 bounded batch 调用 type-owned handler、条件删除 journaled protection、
重扫 protection、对 immutable object 做 exact HEAD/delete，最后只允许同一 attempt CAS
`DELETING -> DELETED`。对象已缺失只在 root/journal authority 仍精确匹配时作为幂等进度；相同 `DELETED`
record 的 scanner restart 不会错误地再要求 epoch 增长。`phase4M4DestructiveRecoveryCheck` 覆盖正常删除、
already-absent、missing journal、unknown removal type 与 restart，但 checkpoint M 尚未提供 production typed
source handler，也未开放 runtime composition。

### 6.16 F4-M4 generation-index retirement checkpoint

Checkpoint N 为 sealed journal 中已经存在的 generation-index removal 实现两个 production handler。
`KeyComponentCodec` 新增 strict inverse；`F4Keyspace.parseGenerationIndexKey` 只接受当前 cluster、固定深度、
canonical stream component、19-digit offset/generation 与 round-trip 完全相同的 COMMITTED/TOPIC_COMPACTED key。
因此 restart 不依赖丢失的 process-local affected-stream cache，也不允许 generic coordinator 猜 key layout。

`GenerationZeroIndexRetirementHandler` 重读 exact legacy/generic candidate，要求 key、Oxia version、stored-envelope
SHA 与 journal 完全相同，然后通过 focused `SourceRetirementMetadataStore` 条件删除 generation-zero index；
delete response loss 只有在同一 root/journal 下重读为 absent 才收敛。`HigherGenerationIndexRetirementHandler`
只接受 journaled `DRAINING` record，CAS 为保留 immutable publication identity 的 `RETIRED` audit record；
`stateReason` 绑定 GC attempt 和完整 reference-set digest，`stateChangedAt` 固定为 delete-intent time，所以 CAS
response loss 和进程重启只能识别该 exact replacement。reference/removal validation 现在也要求
`referenceType == removalType`，避免同 key 的错误 handler 路由。

Coordinator 同时收紧为每个 metadata/protection batch、uncertain protection/object response、physical HEAD/delete
前和最终 DELETED CAS 前重读 exact DELETING wrapper 与 byte-for-byte 相同的 sealed journal。聚焦测试证明 journal
在第二批或 physical fence 消失时不会继续下一次 mutation/object access。`phase4M4GenerationRetirementCheck`
冻结这些规则。

### 6.17 F4-M4 generation-zero source-retirement checkpoint

Checkpoint O 补齐 journal 只有 exact key 而进程已丢失 source identity 时的恢复路径。Legacy
`committed-slices` key 最后一段是 `(objectId, sliceId)` 的单向 hash，不能从 key 独立恢复 `sliceId`；focused
`SourceRetirementMetadataStore` 因此新增 exact-key read/delete：先严格解析当前 cluster/stream/family，读取并
解码 durable record，再从 value 重建 marker/commit identity 与 canonical key，只有 byte-for-byte round trip、
Oxia version 和 stored-envelope SHA 全部匹配才允许条件删除。`VersionedGenerationZeroCommit` 同时保存 source
encoding、canonical generic commit 及其 NRC1 envelope SHA；generic marker 也保留 read-target identity SHA。

`GenerationZeroMarkerRetirementHandler` 和 `GenerationZeroCommitRetirementHandler` 只处理各自 journal type；
delete response 不确定时仅在相同 root/journal 下 exact-key reload 为 absent 才收敛。新的
`SourceRetirementPlanBuilder` 扫描完整 affected-stream generation namespace，对 generation zero 要求当前
recovery root 的唯一 NRC1 reference 覆盖同一 range/commitVersion，严格打开 NRC1、找到同一 commit，并验证
NRC1 canonical commit SHA、source commit、index 和 marker 完全一致后，才冻结 index + marker + commit 三条
removal；它也实现 `GcPlanMetadataRevalidator`，逐 key 返回当前 authoritative facts。Root 在计划冻结期间变化、
marker/version drift、key/value alias、NRC1 canonical commit 不同或不属于 candidate 的额外 source removal
均 fail closed；每次 exact list 命中后都会从 candidate index 重新构造完整三元组，而不是把 key 存在当权限。

Checkpoint O 在该边界仍只是 ordinary foundation；它本身没有证明 NRC1 publication 的 current index/root
健康性，也未启用 production deletion。

### 6.18 F4-M4 healthy NRC1 replacement checkpoint

Checkpoint P 完成 generation-zero source 的 §9.1 replacement 分支。Planner 按 entry 中 1–8 个 canonical
publication-table index 逐项读取；每一行必须严格 round-trip `GenerationIndexRecordCodecV1`、匹配 raw-record
SHA、同一 stream/`COMMITTED` view、publication/generation/range/commit/cumulative coverage，并保留
`metadataVersion=0`。随后它从 Oxia 重读该行的 exact higher-generation key，要求当前 record 仍为
`COMMITTED`、所有字段只按当前 Oxia version 水合、durable-envelope SHA 与 NRC1 bytes 一致。

候选 index 的 `ObjectSliceReadTarget` 必须指向被删除对象之外的 immutable object；其 exact physical root
必须存在、key/hash/id/kind/slice bounds 一致且 lifecycle 为 `ACTIVE`。失效的 current index 或 root 只淘汰
该 publication 候选，所有 1–8 个候选都失效才 veto；畸形/非 canonical NRC1 则作为 metadata invariant
立即失败。选中 replacement 后，planner 读取 source commit/marker，再精确重读 replacement index/root 和
recovery root，任何 version、digest、lifecycle/epoch 或 identity 漂移都使计划失败。`reload` 的 candidate-
bound reproof 会重新执行同一闭环，因此 journal removal 的 key 存在不能替代健康性证明。

Checkpoint P 仍未执行 higher-generation `COMMITTED/QUARANTINED -> DRAINING`，也未完成 future/global
domains、runtime composition、cursor/root/audit retirement 或 final M4 gate；production deletion 继续关闭。

### 6.19 F4-M4 COMMITTED-view higher-generation pre-drain checkpoint

Checkpoint Q 把 higher-generation retirement 的第一段 lifecycle fence 落为独立
`HigherGenerationPreDrainCoordinator`。它只接受 exact `ACTIVE_DISCOVERY` affected-stream candidate；disabled/
dry-run 在任何 metadata/root read 前返回，ownerless candidate fail closed。Coordinator 有界分页扫描两个 view，
严格验证 stream/view/canonical key，只选择 target 指向 candidate object 且尚未 `RETIRED/ABORTED` 的 higher
index；matching `PREPARED` 直接 veto。当前 checkpoint 对 `TOPIC_COMPACTED` 明确要求尚未实现的 view-specific
replacement proof，因此不会用 COMMITTED recovery evidence 退休 compacted-view source。

对每个 matching `COMMITTED/QUARANTINED/DRAINING` source，新的
`HigherGenerationRecoveryCoverageVerifier` 从 source 的 offset、commit-version、cumulative-size 起点逐条遍历
NRC1 commit entry，要求无 gap/overlap 的 exact tiling，并重算 record/entry/logical-byte/schema totals。每条
entry 复用 generation-zero 的 `RecoveryReplacementVerifier`，但要求 replacement generation 严格大于 source
generation；current exact index 必须仍为 `COMMITTED`，target 必须落在另一个 `ACTIVE` physical root。全部
replacement、recovery root 与 source wrapper 在证明末尾精确重读；entry 和 unique replacement 数均受
`PhysicalGcConfig` hard bound 限制。

只有证明完成且 candidate root 的 version/epoch/identity 仍等于 discovery fact，coordinator 才以 source
metadata version CAS 到 `DRAINING`，reason 固定为 `physical-gc-pre-drain:{candidateId}`。CAS response loss 只在
exact replacement reload、原值未变或同 immutable publication 已由并发者进入 `DRAINING` 三种情况收敛；
其他漂移 fail closed。`SourceRetirementPlanBuilder` 在把 DRAINING index 冻结为 journal removal 前再次执行
whole-range proof，所以 replacement/root 在 pre-drain 之后退化会阻止物理删除。

Checkpoint Q 是 COMMITTED-view replacement 路径的 ordinary checkpoint，不包含 below-trim eligibility、
TOPIC_COMPACTED replacement、production runtime composition、cursor/root/audit retirement 或 final M4 gate；
production deletion 继续关闭。

### 6.20 F4-M4 complete source-retirement eligibility checkpoint

Checkpoint R 用 `HigherGenerationRetirementEligibilityVerifier` 关闭 §9.1 的剩余协议分支。所有 higher source
先尝试 `CompletedTrimRetirementVerifier`：source range 必须完整落在同一 authoritative
`StreamMetadataSnapshot.trimOffset` 以下，source wrapper、完整 stream snapshot 和可选 recovery-root wrapper
会在同一次 proof 内精确重读。Generation-zero planner 也先走同一 completed-trim proof；只有未完成 trim 时
才要求 checkpoint P 的 current NRC1 replacement。因此 below-trim 不是时间或本地 hint，而是 exact L0
metadata + source + recovery-root version set。

未完成 trim 的 `TOPIC_COMPACTED` higher source 由 `TopicCompactedReplacementVerifier` 在同一 stream/view
namespace 有界扫描。候选必须是严格更高 generation 的 current `COMMITTED` index，完整覆盖 source 的
offset、commit-version 和 cumulative-size bounds，并保持 payload/projection identity；其 compacted target
必须位于 candidate 之外、physical format 为 `NEREUS_TOPIC_COMPACTED_PARQUET_V1`，且 exact physical root
仍为 `ACTIVE/TOPIC_COMPACTED`。选中的 index/root 与 source wrapper 在返回前精确重读。该路径绝不借用
COMMITTED NRC1 facts，也绝不让跨 view generation 互相覆盖。

`HigherGenerationPreDrainCoordinator` 现在先比较 candidate `notBeforeMillis`，未经过
`sourceRetirementGrace` 时以 `NOT_ELIGIBLE_YET` 返回，且不读取 L0、generation、checkpoint 或 physical root。
经过 grace 后，COMMITTED NRC1、TOPIC_COMPACTED same-view replacement 和 completed-trim 三条路径共享既有
candidate-root final fence 与 response-loss-safe CAS；已经 DRAINING 的 higher source 和 generation-zero removal
在 plan/reload 时重复各自 exact eligibility proof。

Checkpoint R 是 source-eligibility 的 ordinary completion checkpoint，不是 M4 final gate。Checkpoint T 后
future-sentinel 与 ownerless global absence proof 已落地，但 production runtime composition、cursor/root/audit retirement、real-service
destructive scenarios 和 final M4 gate 仍待完成；production deletion 继续关闭。

### 6.21 F4-M4 generation-protocol activation metadata foundation

Checkpoint S 落地后续 ownerless-global proof 与 M5 rollout 共用的单键 cluster authority。新增
`GenerationProtocolActivationLifecycle`、`ReferenceDomainVersionRecord`、`GenerationBackfillProofRecord`、
`GenerationProtocolActivationRecord` 及 explicit binary codec；record 冻结 protocol version、严格排序且唯一的
domain id/version set、broker readiness epoch、三类 backfill proof、object-store capability digest 和独立
publication/physical-delete/cursor-snapshot-delete bits。V1 要求两个 deletion bits 同步，且任何 physical delete
必须同时具备三类完整 backfill 和 object-store capability proof。

`GenerationProtocolActivationStore` 在固定
`/capabilities/generation-v1/activation` key 上提供 read-only `get`、并发收敛的 PREPARED `getOrCreate` 和
version-CAS。只读入口是 GC sentinel 的必要边界：reference scan 不得因为读取而创建 cluster authority。
transition guard 冻结 protocol/run/prepared identity，拒绝 readiness/time/lifecycle/capability 回退、ACTIVE domain
set 漂移、同一 epoch 内 completed backfill 改写以及无新 epoch 的 object capability 替换。生产 Oxia adapter
保留 exact key/version/durable-envelope SHA，并在 CAS response loss 后只接受 exact desired replacement。
任何 backfill attempt epoch 也不得超前于 record 的 broker readiness epoch。

三个 activation lifecycle/capability envelope 已加入 golden registry；production adapter contract test 覆盖两个
独立 store runtime 的 concurrent bootstrap convergence、publication-only activation、完整 deletion-proof advance、
stale CAS 和 domain/capability regression。`phase4M4ActivationMetadataCheck` 是该 metadata foundation 的 ordinary
gate，不代表 backfill coordinator、future sentinel、ownerless-global enumeration、broker guard 或 runtime
activation 已实现；所有 capability bits 仍由生产配置保持关闭。

### 6.22 F4-M4 activation-gated ownerless global domains checkpoint

Checkpoint T 新增 core `GcGlobalReferenceScope` / `GcGlobalReferenceScopeSnapshot`。affected-stream query 仍直接
使用 query 中的 sorted stream set；只有 `OWNERLESS_ORPHAN_CANDIDATE` 才请求 global scope。scope 每次都重读
durable activation authority，只有 `ACTIVE + physical/cursor deletion bits + 三类 complete backfill + exact
installed domain set + object-store capability` 同时成立时，才完整分页 64 个 registration shards。activation
absence 使用 domain-separated token；registration key/version/stored-envelope SHA 全部进入每个 domain snapshot。
scope 在全量扫描结束后重读 exact activation wrapper，任何 version/value 漂移、分页不前进、duplicate stream
或 authority bound overflow 都返回 incomplete+veto，不把 registration hint 截断成 permission。

`FutureCatalogSentinelDomain` 对 affected-stream 和 ownerless query 都读取同一 activation key。PREPARED/absence
为 incomplete，publication-only stage 为 explicit veto，cluster required domain set 与本地 installed plugins
不完全相等时也 veto；因此 Future 6 在更新 capability/domain set 后、但 plugin 未装齐时，现有 F4 worker
无法删除对象。sentinel 只调用 read-only `get`，绝不在 GC scan 中创建 authority。

`generation-v1`、`append-recovery-v1`、`materialization-v1`、
`projection-generation-v1` 和 `cursor-snapshot-v1` 现在都通过同一 scope 获取 ownerless stream set，再复用
原有 exact per-stream scans。global activation/registration tokens 与各自 index/head/root/task/projection/cursor
tokens 一起进入 snapshot；`stillMatches` 会重新执行 global scope 和完整 domain scan。managed-ledger 只依赖
core scope interface，不依赖 materialization。默认旧构造器仍注入 unsupported scope 并 fail closed，只有
后续 production runtime 显式装配 activation-gated scope 才可形成 clear ownerless proof。

`phase4M4GlobalDomainsCheck` 覆盖 activation absent/read-only、domain-set mismatch、authority overflow、全
64-shard page-size-one scan、activation drift、future-domain mismatch，以及五个 domain 的 ownerless scan/
revalidation。该 gate 不实现实际 backfill executor、broker readiness guard 或 runtime composition，production
capability bits 和 deletion 继续关闭。

## 7. Milestones

| Milestone | Deliverable | Current status |
| --- | --- | --- |
| F4-M0 | local source audit and code-level protocol/design gate | complete in docs；design-only |
| F4-M1 | metadata/object lifecycle primitives、list/delete、reader lease and codecs | complete/final-gated on 2026-07-15 |
| F4-M2 | generation publication、committed resolver、target-reader dispatch and fallback | complete/final-gated on 2026-07-15；real Oxia/LocalStack restart、concurrency、pin/quarantine/fallback evidence passed |
| F4-M3 | lossless/topic compacted format、planner/task/worker and sync-profile materialization | complete/final-gated on 2026-07-15；real Parquet/Oxia/LocalStack two-worker、restart、response-loss、full-byte and all-shard pagination/watch-loss evidence passed |
| F4-M4 | recovery checkpoint、source/index retirement and physical/cursor-snapshot GC | in progress；through checkpoint T, NRC1/recovery replay/index repair、exact retirement metadata、GC plans/root fence/scanner、root-authenticated journal/destructive recovery、typed source handlers、all completed-trim/COMMITTED/TOPIC_COMPACTED source-eligibility paths、grace-fenced higher pre-drain/reproof、durable activation authority、future sentinel and affected/ownerless generation/append/materialization/projection/cursor domains are implemented/tested；backfill/broker guard、runtime composition、cursor/root/audit completion and final gate pending |
| F4-M5 | Object-WAL async profile、Pulsar retention/admin/capability integration | planned |
| F4-M6 | scale、failure、two-broker/Oxia/S3 compatibility and aggregate final gate | planned |

No later milestone may bypass an earlier correctness gate with a process-local mock. In particular：

- M2 may publish but M4 must keep all source bytes protected until checkpoint/GC is final-gated；
- M3 may upload output but only the M2 index CAS may expose it；
- M5 may complete logical trim but only M4 GC may delete bytes；
- M6 cannot label F4 complete while BookKeeper is absent by silently claiming BookKeeper profiles；those profiles
  remain explicitly reserved。

## 8. Phase 4 Completion Definition

Phase 4 is complete only when all of the following are executable evidence：

1. ordinary Pulsar bytes、Position and MessageId are unchanged through generation replacement、fallback、restart and
   broker failover；
2. every publication/retirement/task/checkpoint crash cut converges without exposing `PREPARED` bytes；
3. a reader already admitted before GC either completes under its lease or fails before object deletion；
4. no object is deleted while any generation、commit/checkpoint、task、cursor snapshot or registered future-domain
   reference exists；
5. generation-0 commit/index cardinality can be retired only after a verified recovery checkpoint；
6. `OBJECT_WAL_ASYNC_OBJECT` ack can precede secondary publication but never pre-head physical protection；
   read-after-success remains bounded and recoverable；
7. F3 pending lifecycles、owner session and root versions are revalidated at trim/GC action boundaries；
8. rolling upgrade blocks first publication/deletion until every persistent broker advertises F4；downgrade after
   activation fails closed；
9. loaded、unloaded and namespace/partition admin routes agree on the admission matrix；
10. an unloaded stream with committed head and no task is rediscovered after full process loss, without treating its
    registry hint as projection/head truth；
11. Phase 1、1.5、2 and 3 final gates remain green；
12. DELETED roots and L0 object audit records are eventually bounded without permitting a late first/retried PUT to
    resurrect a retired key。
