# Phase 4 Materialization / Compaction / Generation / GC Detailed Design

> 状态：Implementation in progress；F4-M1–M2 已完成并通过 ordinary/Docker-backed final gates；F4-M3
> compacted Parquet read/write/verifier 与 deterministic policy/planner/task-store/recovery/registry-scanner
> checkpoints 已落地，exact-source worker/service 与 M3 gates 尚未完成；F4-M4–M6 未实现
>
> 设计基线日期：2026-07-14
>
> Nereus 输入基线：`nereusstream/nereus@e330969cd5c2c11cd38d0bd7f687185171ae91e2`
>
> Pulsar 输入基线：本地 `/Users/liusinan/apps/ideaproject/nereusstream/pulsar`
> `master@c2f7c22fdc562022b992a5c7aecb5fd5c02d318d`

> 实现状态日期：2026-07-15

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
`phase4M2FinalCheck --rerun-tasks` 已于 2026-07-15 通过，因此 F4-M2 已 final-gated。M3 还需将
source/output task protection 接入 worker。F4-M3 的首个实现 checkpoint 已加入 pinned Parquet/Hadoop/ZSTD
依赖、真实 `PAR1` NCP1/NTC1 schema/metadata writer、close-owned replayable staging result、footer reference，
以及基于 bounded exact object-store ranges 的严格 reader 和独立 NTC1 facade。whole-file CRC32C/SHA/key
verifier、task-aware materialization bridge 与携带调用 `StreamId` 的 core NCP1 adapter 也已落地；golden、
random dense/ZSTD+uncompressed、corruption、streaming/staging、sparse NTC1、cross-stream 和 task-policy binding
tests 均通过。随后落地的第二个 M3 checkpoint 又加入 deterministic policy version、bounded whole-index DAG
planner、fixed-point/overlap suppression、exact-source task create revalidation、64 KiB durable task limit、完整
immutable policy snapshot、claim-expiry/publication recovery、per-stream task scanner 和全 64 分片 registered-stream
scanner。当前配置变化不会再改变或阻塞旧 durable task 的恢复；不同 broker 时钟生成的同一 deterministic
task 也会在 create-response-loss 路径收敛。Pulsar opaque-entry round trip、exact-source worker/protection、
service lifecycle 和 M3 ordinary/final gates 仍未完成，因此 higher-generation materialization 仍未开放。M4
还需将 source reachability 扩展为
recovery-root/anchor-aware proof。Phase 4 整体仍为
`Implementation in progress`；physical delete、materialization worker 和 async profile 均未开放。

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

## 7. Milestones

| Milestone | Deliverable | Current status |
| --- | --- | --- |
| F4-M0 | local source audit and code-level protocol/design gate | complete in docs；design-only |
| F4-M1 | metadata/object lifecycle primitives、list/delete、reader lease and codecs | complete/final-gated on 2026-07-15 |
| F4-M2 | generation publication、committed resolver、target-reader dispatch and fallback | complete/final-gated on 2026-07-15；real Oxia/LocalStack restart、concurrency、pin/quarantine/fallback evidence passed |
| F4-M3 | lossless compacted format、planner/task/worker and sync-profile materialization | in progress；real Parquet writer/reader/full verifier、NTC1 facade、core adapter、policy/planner/task-store/recovery/registry-scanner checkpoints landed；Pulsar opaque round trip、exact-source worker/protection、service lifecycle and milestone gates pending |
| F4-M4 | recovery checkpoint、source/index retirement and physical/cursor-snapshot GC | planned |
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
