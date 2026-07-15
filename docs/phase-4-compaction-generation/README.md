# Phase 4 Materialization / Compaction / Generation / GC Detailed Design

> 状态：Implementation in progress；F4-M1–M3 已完成并通过 ordinary/Docker-backed final gates。F4-M3 已交付
> compacted Parquet read/write/verifier、deterministic policy/planner/task-store/recovery/registry-scanner、exact-source
> reader/worker、protection crash-cut reconciliation、advisory checkpoint reconciliation、bounded service lifecycle、
> Pulsar Entry/NCP1 exact-byte round trip、topic-compaction neutral SPI/registry、terminal workflow-metadata retirement、
> COMMITTED-source bootstrap、tagged-v1 key encoding、shared-budget sorted-spill two-pass engine、NTC1 worker 与
> isolated publication；F4-M4 正在实现，NRC1 object-protocol、protected generation-zero append、anchor-aware
> planning、guarded recovery-root publication/restart protection reconciliation，以及 checkpoint-aware append
> replay adapter、checkpoint-derived index repair、exact source/object-audit retirement metadata adapters，以及
> bounded/reconstructable GC config/candidate/plan values 已落地；
> runtime composition、retirement coordinator/physical GC 与 F4-M5–M6 尚未实现
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
当前又冻结了覆盖全部 lifecycle/optional branch 的 43 frozen envelope vectors，并把 generation index、task、
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

All ten F4 record families now have 43 frozen envelope vectors covering every lifecycle and optional branch. The
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

Candidate 只能从 exact `ACTIVE` root wrapper 建立，并冻结 root version/epoch、query/evidence 与 root 的最早
eligibility。Plan 只接受 canonical sorted/unique、配置有界、同 query 的 complete/non-veto domain snapshots，
且 protection 必须属于 candidate object/root epoch。每个 protection removal 冻结完整 owner/value、Oxia
version 和 durable-envelope SHA；其他 metadata removal 冻结 type/key/version/envelope SHA。
`referenceSetSha256` 直接提交这些 exact removal facts 和每个 domain 的完整 authority/reference 事实；它不包含
随机 candidate id 或进程时间。MARK 前可计算同一
digest，MARK 后 `fromMarkedRoot` 只接受 exact attempt/digest/object、递增 metadata version 和 `epoch + 1`，所以
进程重启只能从 authoritative facts 重建，而不能反序列化第二份 correctness state。

`phase4M4GcPlanCheck` 覆盖配置关系、毫秒/overflow、128-bit entropy、canonical order、domain truncation/veto、
跨对象 protection、root attempt/digest mismatch 和重建稳定性。该 checkpoint 仍没有 domain 实现、root CAS、
metadata retirement 调用或 object delete；physical deletion 继续完全关闭。

## 7. Milestones

| Milestone | Deliverable | Current status |
| --- | --- | --- |
| F4-M0 | local source audit and code-level protocol/design gate | complete in docs；design-only |
| F4-M1 | metadata/object lifecycle primitives、list/delete、reader lease and codecs | complete/final-gated on 2026-07-15 |
| F4-M2 | generation publication、committed resolver、target-reader dispatch and fallback | complete/final-gated on 2026-07-15；real Oxia/LocalStack restart、concurrency、pin/quarantine/fallback evidence passed |
| F4-M3 | lossless/topic compacted format、planner/task/worker and sync-profile materialization | complete/final-gated on 2026-07-15；real Parquet/Oxia/LocalStack two-worker、restart、response-loss、full-byte and all-shard pagination/watch-loss evidence passed |
| F4-M4 | recovery checkpoint、source/index retirement and physical/cursor-snapshot GC | in progress；NRC1 protocol、protected generation-zero append、anchor-aware planning、guarded root publication/restart protection reconciliation、checkpoint append replay、checkpoint-derived index repair、exact retirement metadata adapters and bounded/reconstructable GC plans implemented/tested；runtime composition、reference domains、retirement coordinators/physical GC pending |
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
