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
> domains 的 ownerless query 改为完整 global scan + exact revalidation。Checkpoint U 又实现 persisted dual
> absence window、late exact-byte cleanup、Phase 1 references-before-manifest 和 root-last CAS 的 DELETED-root
> audit retirement。Checkpoint V 已把 F3 cursor snapshot 新写入切换为 guarded PUT + pending protection +
> cursor CAS + permanent protection，并让 hydrate/read 在 durable reader lease 内执行、可修复 CAS response
> loss；`DefaultNereusRuntimeProvider` 已装配 shared physical store/protection/read-pin 到该路径。
> Checkpoint W 又实现 strict NPR1 projection authority、全 64-shard live-reference physical/cursor-root backfill、
> exact commit/index/cursor owner protection、最终 authority revalidation 和 response-loss-safe dual activation
> proofs。Checkpoint X 已实现 canonical projection-ref encoding、exact create/refresh/final-revalidate
> registration coordinator、topic open/recreate return-before-registration ordering和 shared generation-store
> production ownership。Checkpoint Y 已在 locked Pulsar fork 发布 reserved generation lookup capability，并实现
> binding/cursor/generation three-property two-stable-snapshot barrier、broker-incarnation-aware deterministic
> readiness epoch/full digest 和 registry-notification invalidation。Checkpoint Z 已实现 broker cold-topic
> canonical registration traversal、exact unloaded projection candidate、bounded concurrency/deadline、full
> deterministic coverage report 与 final binding/readiness revalidation。Checkpoint AA 又实现 exact
> full-readiness handoff、零失败 admission、product-owned response-loss-safe durable registration proof CAS、
> same-epoch coverage immutability 和 newer-epoch dependent-proof invalidation。Checkpoint AB 已实现
> product-owned generation activation guard：它把 ACTIVE cluster record、当前 readiness epoch、exact six-domain
> digest、registration proof、strict NPR1 projection/L0/registration truth 和 monotonic topic marker 绑定为
> short-lived proof，并在 mutation CAS 前重证；first marker 由默认关闭的 broker switch 控制，response loss
> 只通过 exact reload 收敛，physical delete 还要求同 epoch delete bits/proofs 和 exact
> `projection-generation-v1` snapshot。Checkpoint AC 已实现 product-owned、publication-only
> `PREPARED -> ACTIVE` coordinator：只有显式开关为 true、current exact readiness 与 durable epoch 相同、
> registration proof 已完成且 domain set 完全匹配时才做 bounded CAS；并发冲突/响应丢失从 durable ACTIVE
> reload 收敛，final readiness drift 使调用失败但旧 epoch ACTIVE 仍被 guard 阻断。Broker 的零失败 backfill
> promise 在 proof 完成后等待该 activation；失败 report 或默认关闭状态不调用 controller。Checkpoint AD 又
> 落地 Phase 4 Object-WAL execution matrix、protected-head 后的 `WAL_DURABLE` ack cut、独立
> deadline/single-flight generation-zero 后台修复、restart/live-tail scanner 和 protected read-after-commit
> repair。旧 `DefaultStreamStorage` 构造路径仍固定使用 Phase 1.5 resolver；只有显式 Phase 4 composition seam
> 才接受 async profile。Checkpoint AE 已把 exact F2 sync/async profile round-trip、registration/topic-marker
> activation proof/revalidation 和 authoritative lag gate 接到每流 append lane、primary WAL prepare/upload
> 之前；current-policy COMMITTED coverage、root-stable live tail 和 source-verified retention stats 共同提供 lag
> truth。Checkpoint AF 又把这些 seam 原子装配到 production provider：同一个 runtime 同时安装 exact
> Phase 4 profile resolver、pre-I/O admission、generation-aware read/failure handling、NRC1 replay/index repair、
> generation-zero startup/source repair、authoritative lag reader，以及 bounded materialization
> scanner/worker/checkpoint/retirement lifecycle。Pulsar broker config 已映射 exact sync/async default profile 和
> 完整 `MaterializationConfig`，并为每个 processRunId 分配独立 staging 目录；sync 仍为默认，async 仍要求
> durable generation activation proof。Checkpoint AG 又实现 exact retention policy/config/evidence values、
> source-index-verified stable candidate planner，以及 ownership/activation/final-authority gated、只委托 F3
> `CursorRetentionCoordinator` 的 logical-trim service。Checkpoint AH 继续实现 process-owned shared bounded
> plan lane、same-stream coalescing、whole-operation timeout/close、per-ledger service installation、facade trim
> routing 与五项 Pulsar typed broker config 映射。Checkpoint AI 又实现 exact immutable Pulsar
> retention/backlog snapshot、checked policy projection、stable cluster readiness、registration-backed marker
> admission、activation 后 policy 稳定重读，以及 loaded/unloaded/partition-child `TRIM_TOPIC` 路由。Cursor
> snapshot logical trim remains independent of object deletion. Checkpoint AJ now adds strict NCS key inversion、
> complete bounded retention/cursor/object/protection inventory、canonical cursor-snapshot candidate evidence and a
> post-drain full revalidation callback in the central GC fence. Owner/root/list drift unmarks and incomplete reads
> retain MARKED for retry；the managed-ledger scanner owns no mutation. Checkpoint AK now makes candidate evidence
> restart-reconstructable across `ACTIVE -> MARKED`, adds exact drift rollback, composes the cursor candidate through
> the six-domain collector/journal/drain/DELETING/source-retirement chain, and installs that owned runtime in the
> production provider. Checkpoint AL adds strict inverses for every current V1 writer prefix and a complete
> known-prefix `ObjectInventoryScanner` that registers only old exact-HEAD missing-root objects, gives each new root a
> second full orphan grace, converges create-response loss only through the exact desired root, and never treats
> listing as deletion authority. Checkpoint AM adds the bounded,
> proof-driven registration-retirement foundation: exact DELETED L0/non-live projection/F3 cursor-retention captures,
> terminal-and-audit-grace workflow drain, owner-protection-before-owner retirement, recovery-root/metadata cleanup,
> final authority recapture and registration-last conditional delete with response-loss convergence. Its ordinary
> gate includes a published task/two-index/three-protection drain and real non-empty NRC1 checkpoint-root retirement,
> with delete-response-loss convergence while physical roots remain untouched. Checkpoint AN now installs one
> metadata-first complete-pass lifecycle：256-shard root routing/recovery, 64-shard registration retirement, then
> known-prefix inventory. It uses fixed delay after completion/failure, coalesces one immediate rescan and drains or
> cancels the active pass by close deadline without owning injected executors. Cursor ACTIVE/MARKED roots use exact F3
> inventory；generic ownerless roots use the six-domain proof after a cheap protection admission；DELETING/DELETED use
> sealed-journal/tombstone recovery. The provider starts this lifecycle only when typed GC is enabled. Coverage/delete
> activation and the final deletion switch remain closed. Checkpoint AO maps all typed physical-GC broker inputs into
> the runtime and removes provider-local lease/protection/orphan constants；the safe defaults remain
> `enabled=false, dryRun=true`, so default startup schedules no pass and configuration is never deletion authority.
> Checkpoint AP adds the exact configured-scope delete canary；checkpoint AQ composes registration/physical/cursor
> coverage and capability evidence into one atomic dual-bit activation；checkpoint AR installs that coordinator in the
> provider/runtime/factory and locked Pulsar sequence, and fences mutating startup/restart recovery by the exact local
> capability digest. Checkpoint AS makes the activation guard and GC registry consume one exact ownerless-reference
> assembly, then proves one destructive/restart slice against real Oxia and LocalStack. Safe defaults still perform
> none of these mutations. Checkpoint AT additionally proves an independent process recovers durable DELETING after
> the real object DELETE completed but before the old process could CAS the root to DELETED. Checkpoint AU proves an
> applied DELETED-root Oxia CAS whose response is lost converges by exact reload without a repeated object DELETE.
> Checkpoint AV forces two independently assembled runtimes to contend on one MARKED root and proves one durable
> DELETING intent plus idempotent exact-delete convergence.
> Checkpoint AW then recovers a mixed 128 MARKED / 128 DELETING population spanning every physical-root shard in a
> fresh process while object inventory is forced empty. That scale fixture also caught and removed an invalid
> cross-page logical-order assumption：opaque object-list continuations, not base64-mapped key order, carry progress.
> Checkpoint AX then persists 1,001 roots in one physical shard plus one in every other shard to real four-shard Oxia,
> closes the writer process and proves a fresh scanner starts each shard from an empty continuation and reads the hot
> shard in exactly 16 bounded pages without duplicates or omissions.
>
> 设计基线日期：2026-07-14
>
> Nereus 输入基线：`nereusstream/nereus@e330969cd5c2c11cd38d0bd7f687185171ae91e2`
>
> Pulsar 输入基线：本地 `/Users/liusinan/apps/ideaproject/nereusstream/pulsar`
> `master@c59da789e88df2b57829de3277c60194b44fceb6`

> 实现状态日期：2026-07-18

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

Checkpoint R 是 source-eligibility 的 ordinary completion checkpoint，不是 M4 final gate。At the W/Z cut，
future-sentinel、ownerless global absence proof、DELETED-root/Phase 1 audit retirement 与 live-reference
physical/cursor-root backfill 已落地，checkpoint Z 也已实现 broker registration traversal/report；durable
registration proof、cursor snapshot candidate/execution、object inventory and runtime ownership were still missing.
Checkpoints AA、AJ、AK and AL have since closed those ordinary slices. Periodic lifecycle scheduling、registration
retirement、broker GC activation、real-service destructive scenarios 和 final M4 gate 仍待完成；production deletion
继续关闭。

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

### 6.23 F4-M4 DELETED-root tombstone-retirement checkpoint

Checkpoint U 实现 `PhysicalRootTombstoneRetirementCoordinator` 的 production
`DefaultPhysicalRootTombstoneRetirementCoordinator`。入口只接受 root scanner 返回的 exact `DELETED` wrapper；
disabled/dry-run 在任何 read 前返回。每次 pass 只读取一次 wall clock，并以同一 monotonic deadline 串行执行
root reload、exact HEAD、reader/protection scan 和 activation-gated ownerless domain scan。第一个 clear pass 将
immutable/deletion-attempt root facts、query identity、handle absence 与所有 canonical domain snapshot digest
编码为 `nereus-deleted-root-tombstone-proof-v1`，通过合法的 `DELETED -> DELETED` CAS 持久化首次 absence
时间；没有进程内 timer 或候选队列承担 correctness。

第二个严格 `orphanGrace + maximumClockSkew` 窗口后，coordinator 必须重得相同 proof。任何 lease/protection、
owner reference、domain incomplete/veto 或 authority drift 都清除/替换 observation 并重新开始窗口。若旧 key
出现 exact immutable bytes，只在 exact root reload、handle absence 和 `stillMatches` ownerless proof 后删除，
response loss 通过新 HEAD absence 收敛；identity mismatch 返回 `QUARANTINED` 且不删除未知 bytes。

带 Phase 1 `objectId` 的 root 会捕获 exact reference/manifest wrapper，逐次重验同一 DELETED root，先条件删除
references、再删除 manifest、重读二者 absent，最后再次执行完整 HEAD/handle/domain proof。root 是最终 metadata
action，并同时绑定 Oxia version 与 durable-value SHA；lost response 只在 exact key absent 时成功。
`PhysicalRootTombstoneRetirementTest` 与 `LatePutAfterTombstoneTest` 覆盖 dual window、authority/owner/handle
drift、late PUT、mismatched bytes，以及 object/audit/root response-loss cuts。
`phase4M4TombstoneRetirementCheck` 已于 2026-07-16 通过完整 ordinary 前置链；它不启用 production runtime
deletion，也不是 M4 final gate。

### 6.24 F4-M4 guarded cursor-snapshot publication and read-pinning checkpoint

Checkpoint V 保留 F3 的 cursor-root CAS visibility point，但把 object lifecycle 收口为两阶段协议。
`CursorSnapshotWriteAuthority` 冻结 upload 前的 exact ACTIVE root、owner session、metadata version 与目标
mutation sequence；`CursorSnapshotPublication` 绑定 immutable reference、exact physical identity 和
`CURSOR_SNAPSHOT_PENDING`。`DefaultCursorSnapshotStore.prepareWrite` 在每个 provider transmission 前重读
同一个 cursor root，要求可选 physical root absent 或 exact `ACTIVE`，然后执行 if-absent upload、strict
HEAD、ACTIVE root registration、pending acquire/revalidate 和最终 owner/root reproof。

`DefaultCursorStorage` 的顺序固定为
`prepareWrite -> compareAndSetCursor -> completeWrite`。CAS 仍是唯一可见性点；`completeWrite` 先
create/transfer/revalidate `CURSOR_SNAPSHOT_ROOT`，再在再次重证 permanent/live root 后删除 pending。若 CAS
或 permanent completion 的响应丢失，后续 hydrate/read 会从当前 exact snapshot reference 创建或转移
permanent protection，因此不会依赖原进程内 publication object；bounded pending 可以随后过期。

读取路径先做 strict identity HEAD，再从 live cursor root 收敛 permanent protection，随后获取 durable
`ObjectReadPinManager` lease；第二次 HEAD、range read 和 NCS1 decode 全部位于 lease 内，成功/失败都释放。
`DefaultNereusRuntimeProvider` 现在共享装配 `OxiaJavaPhysicalObjectMetadataStore`、
`DefaultObjectProtectionManager`、`DefaultObjectReadPinManager` 和 protected snapshot store；
`NereusManagedLedgerRuntime.objectReadPinManager()` 暴露只读 ownership 并按
read-pin -> protection -> physical-store 顺序关闭。F2 的 URL-safe process identity 经 domain-separated
SHA-256/base32 派生为 F4 reader-lease identity，不改变现有 writer/append-attempt wire identity。

`CursorSnapshotStoreTest` 覆盖 pending/permanent 次序、guarded owner drift、CAS-response-loss read repair、
lease-before-range-IO、failure release、collision 和 borrowed-resource close；real S3 与 real Oxia/S3 source
sets 已迁移到同一协议，`cursorS3IntegrationTest` 与 `cursorM2IntegrationTest --rerun-tasks` 已于
2026-07-16 通过。`phase4M4CursorProtectionCheck` 是 checkpoint V 的 ordinary gate 且已通过；它不实现
`CursorSnapshotGcScanner`、legacy backfill、inventory、broker activation barrier 或 production deletion。

### 6.25 F4-M4 physical-root and cursor-root backfill checkpoint

Checkpoint W 把 rollout 前的 live-reference coverage 从设计合同落成代码。Core 新增
`GenerationProjectionAuthorityReader`/snapshot，managed-ledger 新增 strict NPR1
`ManagedLedgerGenerationProjectionRefV1` 与 exact binding/topic reader。NPR1 使用无 padding base64url、
strict UTF-8、完整 identity fields、trailing-byte rejection 和 CRC32C；golden test 冻结 exact ref 与
SHA-256 identity digest。Reader 对 missing/recreated/DELETING/DELETED projection 返回带 presence/absence
authority 的 non-live classification，不把 registration hint 当成 projection truth。

`DefaultPhysicalRootBackfillCoordinator` 只在同 readiness epoch 的 registration proof 已完成后运行。它遍历
全部 64 registry shards，在 canonical fold 下有界并发处理 stream；live stream 会扫描 recovery-root-anchored
commit tail、完整 COMMITTED generation-zero index prefix 和 F3 retention/cursor roots。每个存量对象先执行
exact HEAD，再通过 production `ObjectProtectionManager` 创建/验证 ACTIVE root 与 commit/index/cursor
owner-bound permanent protection。Generation-zero protection reference id 与新 append 路径共享同一公开公式，
因此 backfill 不会产生第二套兼容身份。

每个 stream 在计入 coverage 前会重读 registration、完整 L0 snapshot、projection、recovery root/head 和
cursor inventory digest。完整零失败 traversal 才能 CAS `physicalRootBackfill` 与
`cursorSnapshotBackfill`；CAS response loss 只在 reload 得到 exact desired proofs 时成功。Focused tests
覆盖 empty-registry all-shard proof、lost activation response、shared WAL commit+index、cursor snapshot ETag
root/permanent owner protection，以及 registration proof 缺失时 fail closed。
`phase4M4PhysicalRootBackfillCheck` 是 checkpoint W ordinary gate；它不执行 broker cold-topic registration
backfill/barrier、不设置 delete bits、不运行 cursor deletion scanner/object listing，也不是 M4 final gate；
该 gate 已于 2026-07-16 使用 `--rerun-tasks` 通过。

### 6.25a F4-M4 cursor-snapshot GC discovery checkpoint

Checkpoint AJ 实现 `CursorSnapshotGcScanner` 的 read-only candidate/revalidation boundary。Scanner 读取 exact
retention authority，完整分页最多 10,000 个 cursor roots、canonical F3 snapshot objects 和每对象 protections；
任何下一页越界都 fail closed，不把上限当截断。`CursorSnapshotKeys.parse` 严格反解
`cursorNameHash/cursorGeneration/snapshotId.ncs`。候选必须同时满足 listing age + clock skew、ACTIVE
`CURSOR_SNAPSHOT` root、length/ETag identity、complete unreferenced inventory，以及只有 owner-bound permanent 或
skew-safe expired pending cursor protection。每个 candidate 的 retention/root/live-ref/list/root/protection facts
被绑定进 canonical query evidence，并按 one-at-a-time visitor 执行。

中央 `PhysicalObjectGarbageCollector` 新增 final-candidate callback，在第二次 reader/protection/metadata drain
之后、最终 MARKED root/journal/activation fence 之前调用。Cursor revalidation 重跑完整 inventory，只接受 exact
ACTIVE root 或其 one-epoch MARKED successor；普通 owner/root/list/protection drift 返回 false 并 unmark，读取/
limit failure 保留 MARKED 重试。Scanner 不包含 root CAS、protection delete 或 object delete。Focused tests 覆盖
真实 local object listing、MARKED successor、owner drift、pending expiry+skew 和 inventory overflow。
`phase4M4CursorSnapshotGcCheck --rerun-tasks` 已于 2026-07-18 在 Java 21、locked Pulsar
`330eeeb3fa9903ed0123c2a0e261d403c32f0a59` 上通过；root 与 nested Pulsar build 均报告 138 actionable tasks。
Production scheduling、candidate-to-plan adapter、coverage/delete bits 和 destructive runtime composition 仍关闭。

### 6.25b F4-M4 cursor-snapshot GC execution checkpoint

Checkpoint AK closes the process-restart hole between candidate discovery and the central collector. Cursor candidate
evidence no longer includes ephemeral discovery time or an ACTIVE-only metadata wrapper；it binds the normalized ACTIVE
owner epoch、immutable physical identity、stable listing/authority/protection facts and durable eligibility time.
`CursorSnapshotGcScanner.recoverMarked` therefore reconstructs the same query from an exact MARKED root after a fresh
process start. A complete changed inventory produces a different digest；an incomplete/backend/limit read fails and
leaves MARKED. `PhysicalObjectGarbageCollector.unmarkDrifted` conditionally rolls only that exact unreconstructable
MARKED wrapper back to ACTIVE and converges a lost CAS response without entering delete intent.

`CursorSnapshotGcExecutor` now maps scanner protections into the canonical GC plan, executes
`mark -> drain/final cursor revalidation -> DELETING`, resumes journal-authenticated protection/object retirement, and
can independently resume MARKED or DELETING roots. `Phase4PhysicalGcRuntime` installs the exact six activation-domain
implementations、global registration scope、retirement journal、central collector and executor in
`DefaultNereusRuntimeProvider`; the checkpoint-AK cursor plan itself contains no metadata removals. Checkpoint AN
extends the same source-retirement runtime with all four generation-zero/higher-generation metadata handlers.
`NereusManagedLedgerRuntime` owns and closes it before cursor and metadata stores. `NereusRuntimeConfiguration`
carries cross-validated `PhysicalGcConfig`, while the existing broker bridge intentionally supplies safe defaults.

Focused tests prove ACTIVE discovery stops at drain grace, a fresh MARKED reconstruction reproduces the original plan
and completes exact protection/object deletion, changed cursor authority unmarks without deleting bytes, and uncertain
recovery-unmark CAS converges. This checkpoint does not schedule the root/registration scans, publish coverage/delete
bits, map broker GC knobs, enumerate missing-root object inventory or retire registrations. Consequently provider
startup still performs no destructive work and production deletion remains disabled/default-dry-run. Checkpoint AN
later supersedes only the “no scheduler” boundary under explicit `enabled=true`; it does not change those safe defaults.

`phase4M4CursorGcExecutionCheck --rerun-tasks` passed on 2026-07-18 under Java 21 against locked Pulsar
`master@330eeeb3fa9903ed0123c2a0e261d403c32f0a59`；the root build executed 139 actionable tasks and the inherited
nested Pulsar regression reported 138 actionable tasks.

### 6.25c F4-M4 object-inventory registration checkpoint

Checkpoint AL closes the current-writer missing-root discovery boundary without granting object-store listing any
deletion authority. `WalObjectKeys` now owns the canonical Object-WAL path builder and strict inverse while preserving
the existing canonical-component `writerRunIdHash` contract；`RecoveryCheckpointFormatV1`、
`CompactedObjectFormatV1` and `CursorSnapshotKeys` expose cluster/view-wide strict inverses. The product registry
contains exactly five non-overlapping V1 families：Object-WAL、COMMITTED compacted、TOPIC_COMPACTED、NRC1 recovery
checkpoint and NCS1 cursor snapshot. Unknown prefixes are not listed and malformed members of a known prefix cannot
be promoted into metadata.

`ObjectInventoryScanner` performs a complete ordered family/page pass with one bounded async operation at a time. It
prechecks the physical root, requires listing age beyond `orphanGrace + maximumClockSkew`, performs exact HEAD and
requires positive length plus CRC32C, then rechecks root absence. Disabled/dry-run mode reports `WOULD_REGISTER`；an
enabled non-dry-run pass creates an exact ACTIVE root with listing `lastModified` as creation time and
`passStart + orphanGrace + maximumClockSkew` as a second grace boundary. A lost create response succeeds only after
the complete desired root reloads unchanged. Existing roots、young/missing-age objects、malformed keys、stale listing
entries、HEAD mismatch and root conflict are exhaustively counted and never mutated by this pass.

The scanner has no MARK、protection removal or object delete call. `Phase4PhysicalGcRuntime` owns and closes it；at
checkpoint AL it was not scheduled and therefore did not change startup behavior or enable physical deletion.
Metadata-first scheduling、registration-retirement runtime composition、broker GC mapping/coverage activation and
real-service destructive M4 evidence remained outside checkpoint AL.

`phase4M4ObjectInventoryCheck` is the ordinary checkpoint gate. Its focused suites cover old exact-HEAD registration
with a second grace, page-size-one outcome accounting, disabled/dry-run behavior, create-response-loss convergence,
concurrent different-root classification, strict writer-key inverses and malformed-prefix-member rejection.
The aggregate gate passed with `--rerun-tasks` on 2026-07-18 under Java 21 against locked Pulsar
`master@330eeeb3fa9903ed0123c2a0e261d403c32f0a59`；the root build executed 131 actionable tasks and the inherited
nested Pulsar regression reported 138 actionable tasks（3 executed、135 up-to-date inside that nested invocation）。

### 6.25d F4-M4 registration-retirement gate checkpoint

Checkpoint AM closes the deleted-stream registry-retirement protocol boundary. Core now exposes an exact bounded
per-stream external-reference authority snapshot. The managed-ledger implementation validates the strict NPR1
subject, captures the F3 retention key plus every cursor key/version/value digest, re-reads retention after pagination
and reports reference-free only for stable ACTIVE retention with DELETED-only cursors. Transitional lifecycle,
NPR1 mismatch and configured overflow fail closed.

`StreamRegistrationRetirementCoordinator` captures the exact registration/L0/projection/F3 basis, requires DELETED
L0 and an empty post-anchor recovery tail, and accepts only terminal/audit-grace-expired tasks and higher indexes. It
derives the exact task/index/NRC1-root object scope, removes owner protections before each still-present terminal
owner, conditionally drains index/task/root/checkpoint/stats/sequence metadata, rescans all prefixes, re-captures the
original authorities and deletes the registration last. Every uncertain metadata/protection delete reloads the exact
key; a changed value fails closed. The coordinator has no object/root delete call.

Focused tests cover non-terminal task、live index、audit grace、external-authority drift、non-DELETED L0、empty-root
response loss, a real published workflow with two terminal indexes and three owner protections, and a real non-empty
NRC1 root with checkpoint-object/target protections. The last two tests force protection/index/task/root/registration
delete-response loss and verify the physical roots remain present. `phase4M4RegistrationRetirementCheck` passed on
2026-07-18 under Java 21 against locked Pulsar
`master@330eeeb3fa9903ed0123c2a0e261d403c32f0a59`; the root build reported 132 actionable tasks（68 executed）and
the inherited nested Pulsar regression reported 138 actionable tasks（3 executed）. Periodic registration scheduling,
broker physical-GC mapping/activation and real-service destructive final scenarios were still pending at checkpoint AM.

### 6.25e F4-M4 metadata-first lifecycle scheduling checkpoint

Checkpoint AN closes the process-lifecycle boundary without opening the deletion switch. The new
`StreamRegistrationRetirementScanner` performs a complete ordered 64-shard scan, validates exact key/value/shard
identity, invokes the proof-driven AM coordinator one stream at a time under bounded deadlines and returns exhaustive
per-status counts. `PhysicalGcLifecyclePass` then freezes the only permitted whole-pass order：complete 256-shard
physical-root metadata scan first, complete registration retirement second, and object-store inventory last. A failed
stage stops the pass；the next pass restarts from shard/prefix zero, so listing can never overtake durable recovery.

`DefaultPhysicalGcLifecycleService` schedules an immediate first pass and then fixed delay from completion, including
after failure. Passes never overlap；any number of hints while one is active set exactly one immediate rescan. Close
rejects new work, cancels a scheduled trigger, waits for the admitted pass and cancels its exposed/source futures at
`closeTimeout`, without shutting down the borrowed scheduler or callback executor.

Each root pass receives a fresh `Phase4PhysicalRootLifecycleRouter`. Canonical cursor ACTIVE roots are de-duplicated
per stream and evaluated through the full F3 cursor inventory；cursor MARKED roots reconstruct their exact prior plan.
Generic ACTIVE roots first read one authoritative protection page, skipping the expensive ownerless global proof when
any protection exists；absence is only an admission optimization, never delete authority. The remaining unprotected
root uses `OWNERLESS_ORPHAN_CANDIDATE` and all six global domains. Ownerless MARKED recovery rebuilds the exact query/
plan, unmarks only complete veto/digest drift and leaves incomplete/overflow facts MARKED. DELETING resumes the sealed
journal with all generation-zero commit/index/marker and higher-index handlers；DELETED runs dual-absence Phase 1 audit
and root-last retirement；QUARANTINED is retained.

`Phase4PhysicalGcRuntime` now also owns the 64-shard scanner, checkpoint-AM coordinator, Phase 1 source/audit retirement
adapters and tombstone coordinator, and drains them before shared metadata/object resources. The provider calls
`start`, but the runtime schedules only when `PhysicalGcConfig.enabled()` is true. The current broker mapping still
constructs `enabled=false, dryRun=true`, and cluster coverage/deletion activation is still closed, so ordinary broker
startup schedules no pass and checkpoint AN is not production-delete activation. `phase4M4LifecycleSchedulingCheck`
is the ordinary AN gate；real Oxia/LocalStack destructive/scale scenarios, broker knob mapping and the final M4 gate
remain pending. The gate passed on 2026-07-18 under Java 21 against locked Pulsar
`master@330eeeb3fa9903ed0123c2a0e261d403c32f0a59`; the root build reported 133 actionable tasks（65 executed）and
the inherited nested Pulsar regression reported 138 actionable tasks（3 executed）.

### 6.25f F4-M4 broker physical-GC configuration checkpoint

Checkpoint AO closes the broker-to-runtime configuration boundary without granting physical deletion. Pulsar
`ServiceConfiguration` now owns 17 explicit fields for lifecycle enable/dry-run、reader lease/renew、scan interval、
metadata/object page limits、delete concurrency、ownerless stream/domain/reference bounds、operation/close/drain
deadlines and pending/orphan/tombstone grace. Defaults are operationally bounded but rollout-safe：
`nereusPhysicalGcEnabled=false` and `nereusPhysicalGcDryRun=true` remain independent controls.

`NereusBrokerStorageConfiguration.runtimeConfiguration` converts every seconds value to an exact positive/non-negative
`Duration`, applies `PhysicalGcConfig.MAX_PAGE_SIZE`、`MAX_STREAMS_PER_CANDIDATE` and `MAX_DOMAIN_VALUES`, and then lets
`NereusRuntimeConfiguration` enforce lease-renew、drain、operation-timeout、materialization grace and tombstone lifetime
relationships before any Oxia/S3 client is constructed. Invalid values therefore fail broker initialization rather
than partially starting a GC runtime.

`DefaultNereusRuntimeProvider` now takes `configuration.physicalGc()` once and supplies its pending-protection、reader-
lease、clock-skew and orphan-grace values to `DefaultObjectProtectionManager`、`DefaultObjectReadPinManager` and
`DefaultCursorSnapshotStore`. The former hard-coded 5-minute/2-minute/1-day constants are removed, so the write,
read-pin and GC interpretations cannot drift.

`phase4M4PhysicalGcConfigCheck` freezes the Nereus/provider surface, all 17 Pulsar defaults and getters, exact typed
mapping, safe-default/mutation semantics and invalid page/lease tests against locked Pulsar
`master@42a4bfd7dfae1d0b23e07dd2b9ebb59f0344782f`. This gate does not publish `physicalRootBackfill` or
`cursorSnapshotBackfill`, does not prove object-store delete capability, and does not enable either durable deletion
bit. Coverage proof、capability proof、monotonic destructive activation and the real Oxia/LocalStack destructive/scale
gate remain mandatory. The AO gate passed on 2026-07-18 under Java 21；the root build reported 144 actionable tasks
（63 executed）and the locked Pulsar focused/style build reported 141 actionable tasks（all executed）.

### 6.25g F4-M4 configured object-store delete-capability checkpoint

Checkpoint AP implements the real configured-scope capability proof that checkpoint AO deliberately left separate.
`DefaultObjectStoreDeleteCapabilityProbe` derives one deterministic V1 SHA-256 from the provider class、normalized
endpoint、region、bucket、logical prefix、path-style mode and the exact semantics it exercises. Credential references、
access keys、secret keys and session tokens are neither inputs nor output. A caller supplies a lowercase base32 run id
and one positive overall deadline；the probe uses only the isolated
`__nereus_capability__/delete-v1/<runId>/probe` namespace and returns only the capability digest、a hashed probe-key
audit identity and completion time.

One successful run must prove the complete sequence：guarded if-absent PUT of deterministic CRC32C bytes；exact
HEAD with key/length/checksum/non-empty ETag；one complete LIST page containing that exact key/length/last-modified
identity；ETag-bound exact DELETE；HEAD absence even when the DELETE response was lost；a second DELETE returning
`ALREADY_ABSENT`；and a final complete LIST proving absence. A lost PUT response converges only through the exact HEAD.
Any mismatch、incomplete listing、foreign collision or timeout fails closed, and failure cleanup HEADs then deletes
only an exact matching canary. The product never infers safety for existing business objects from this canary.

`phase4M4ObjectStoreCapabilityCheck` freezes the API/value validation、scope identity、operation order、response-loss
recovery、cleanup and focused local-object-store tests. Checkpoint AP does not persist the digest into
`GenerationProtocolActivationRecord`, does not compose/run checkpoint-W backfill, and cannot enable
`physicalObjectDeletionEnabled` or `cursorSnapshotDeletionEnabled`. Product-owned monotonic activation and the real
Oxia/LocalStack destructive/scale gate remain mandatory.
The AP gate passed on 2026-07-18 under Java 21 against locked Pulsar
`master@42a4bfd7dfae1d0b23e07dd2b9ebb59f0344782f`；the root build reported 145 actionable tasks（69 executed）and
the inherited focused Pulsar build reported 141 actionable tasks（all executed）.

### 6.25h F4-M4 product-owned physical-deletion activation checkpoint

Checkpoint AQ implements the bounded activation transaction that composes checkpoint W coverage with checkpoint AP
configured-scope capability evidence. `ManagedLedgerPhysicalDeletionActivationRequest` carries one lowercase-base32
run id、bounded stream concurrency and one overall deadline；the redacted result returns only the readiness epoch、two
coverage digests、capability digest、activation metadata version and `ACTIVATED`/`ALREADY_ACTIVE` status.

`DefaultPhase4PhysicalDeletionActivationCoordinator` first captures exact ACTIVE publication、the canonical reference-
domain set、current stable broker readiness and the completed same-epoch registration proof. If the same scope is
already active it performs no backfill or canary. Otherwise it runs the physical-root/cursor-root backfill, rejects
any failure, reloads and verifies both exact durable coverage proofs, runs the configured-scope canary, rechecks
readiness and performs one version CAS that installs the capability digest and enables both V1 deletion bits together.
Condition conflicts reload and retry only the final CAS（maximum 32 attempts）without repeating backfill/canary；a lost
CAS response converges only from exact durable authority. Final success reloads activation and revalidates readiness.

At checkpoint AQ this coordinator was not yet exposed through `NereusManagedLedgerFactory`, did not start the
physical-GC lifecycle after activation and did not gate restart recovery on the local expected capability digest.
Checkpoint AR below closes those product-composition gaps. Checkpoint AS then closes the first real-service
activation/restart-recovery slice；the remaining destructive/scale matrix remains mandatory before the milestone is
final-gated.

### 6.25i F4-M4 production deletion composition and restart-scope checkpoint

Checkpoint AR constructs one `DefaultObjectStoreDeleteCapabilityProbe` per provider runtime and shares its exact
digest with `ManagedLedgerGenerationProtocolActivationGuard`、the AQ coordinator and
`Phase4PhysicalGcStartupGate`. `Phase4PhysicalGcRuntime` now owns the W backfill coordinator, implements
`ManagedLedgerPhysicalDeletionActivationCoordinator` and is exposed through `NereusManagedLedgerRuntime` plus
`NereusManagedLedgerFactory.activatePhysicalDeletion`. Successful AQ activation starts the already-owned
non-overlapping lifecycle before its result completes.

Startup is intentionally asymmetric. `enabled=false` starts nothing；`enabled=true, dryRun=true` may run audit passes
without durable deletion authority；`enabled=true, dryRun=false` loads the cluster activation record first. Absent or
publication-only authority defers the lifecycle so the broker can finish rollout. Durable delete bits authorize
MARKED/DELETING recovery only when lifecycle/publication/backfills are complete、the six-domain set is exact and
`objectStoreCapabilitySha256` equals the local configured provider/endpoint/region/bucket/prefix/path-style digest.
Scope or domain drift fails non-retryably before any recovery pass. The per-operation activation guard applies the
same digest equality to physical deletion and logical-trim proofs.

The locked Pulsar storage captures `runtimeConfiguration.physicalGc().mutationsAllowed()` once. A zero-failure cold-
topic registration backfill first waits for publication activation；only when that physical mutation switch is true
does it build the AQ request from the same run id/concurrency/timeout, await coverage/canary/atomic activation and then
return the report. Disabled publication、dry-run/default physical GC or any nonzero report invokes no destructive
coordinator. Publication or deletion activation failure fails the caller's completion promise.

`phase4M4PhysicalDeletionActivationCheck` freezes this ordering、the typed runtime/factory surface、same-digest
startup recovery and mismatch failure, plus locked Pulsar formatting/style/focused tests at
`master@c59da789e88df2b57829de3277c60194b44fceb6`. The aggregate gate passed on 2026-07-18 under Java 21 with
147 root actionable tasks；its serialized Pulsar configuration and deletion-activation builds passed with 141 and
129 actionable tasks respectively. It is still an ordinary checkpoint：the real Oxia/LocalStack
destructive restart、response-loss、multi-broker and scale matrix remains the F4-M4/M6 final-gate requirement.

### 6.25j F4-M4 real-service activation/restart recovery checkpoint

Checkpoint AS removes a correctness split that checkpoint AR still permitted. The activation guard previously owned
a projection domain with an unsupported ownerless-global scope and fixed bounds, while the physical-GC registry built
a separate registered-stream global scope from broker configuration. A clear durable backfill could therefore be
accepted by activation but rejected forever during operation revalidation. `Phase4GcReferenceDomainAssembly` now
constructs one `RegisteredStreamGcGlobalReferenceScope` and one `ProjectionGenerationReferenceDomain` from the exact
`PhysicalGcConfig.referenceDomainConfig()`；`DefaultNereusRuntimeProvider` gives that exact projection instance to
`ManagedLedgerGenerationProtocolActivationGuard` and the exact assembly to `Phase4PhysicalGcRuntime`. The canonical
metadata-domain list is also mapped once through `NereusGenerationProtocolReferenceDomains.currentGcV1()`.

`Phase4PhysicalGcOxiaS3IntegrationTest` runs against four-shard `oxia/oxia:0.16.3` and pinned
`localstack/localstack:4.14.0` S3. One process persists publication-only authority and an ownerless compacted object,
proves lifecycle startup is deferred, then executes real physical/cursor coverage backfill plus configured-scope
canary and atomically activates both deletion bits. After the object root reaches durable MARKED, an independent
runtime with a different logical S3 prefix is rejected non-retryably with `METADATA_INVARIANT_VIOLATION` without
changing the root. A correct-scope process then recovers with inventory LIST forced empty；the wrapper performs the
real target DELETE but loses its first response, and recovery confirms HEAD absence before converging the durable root
to DELETED. The focused Testcontainers task passed on 2026-07-18 under Java 21 and Docker 28.5.2 with 38 executed
tasks. The aggregate gate subsequently passed against locked Pulsar
`c59da789e88df2b57829de3277c60194b44fceb6` with 141 actionable tasks（68 executed）。

`phase4M4PhysicalDeletionIntegrationCheck` composes the AR gate、the shared-assembly contract audit、documentation/
module/source-lock checks and that real-service task. Checkpoint AS itself is not `phase4M4FinalCheck`：two
workers/brokers、checkpoint/index/source deletion followed by read repair、all 256 root shards、cursor and tombstone
scale limits、late PUT resurrection and the rest of the failure matrix remain required. Checkpoint AT below closes
only the exact post-DELETE/pre-DELETED-root-CAS process-death cut.

### 6.25k F4-M4 post-DELETE process-death recovery checkpoint

Checkpoint AT extends the real Oxia/LocalStack fixture with
`processRestartAfterDeleteBeforeDeletedRootCasRecoversDurableDeletingIntent`. The first process activates deletion and
persists an ownerless compacted root through MARKED. After drain grace, a second process resumes that root；
`TargetDeleteTrackingObjectStore` delegates the exact target DELETE to LocalStack and records completion only from the
real SDK result. `PhysicalStoreDecorator.failBeforeDeletedRootCas` then matches only the same object hash's
`PhysicalObjectLifecycle.DELETED` replacement, verifies that DELETE already completed and returns non-retryable
`STORAGE_CLOSED` before invoking the Oxia CAS. A deterministic completion future replaces sleeps, so the fixture
observes the cut with S3 HEAD absent and the durable root still DELETING.

After that process closes, a third independent runtime using the same durable scope starts from Oxia authority. The
metadata-first root scanner routes DELETING directly to `SourceRetirementCoordinator.resume`；the exact sealed journal
is reauthenticated, the absent object is accepted only under that unchanged root/attempt/digest, and the root CAS
converges to DELETED. No object LIST、old callback or process-local candidate is involved. The focused method passed on
2026-07-18 under Java 21 and Docker 28.5.2 with 47 executed tasks.

`phase4M4PostDeleteCrashRecoveryCheck` composes the AS gate、the dedicated AT contract audit and the same pinned real-
service source set. It passed on 2026-07-18 under Java 21 and Docker 28.5.2 against locked Pulsar
`c59da789e88df2b57829de3277c60194b44fceb6`；the root aggregate reported 151 actionable tasks（66 executed、85 up-to-
date），while its two pinned Pulsar source-set builds executed 132/132 and 129/129 tasks. AT closes one mandatory
process-death cut, not the full kill-point matrix：MARKED、journal/source/protection deletion、uncertain DELETED CAS、
two-worker/broker、all-shard and scale evidence remain required.

### 6.25l F4-M4 DELETED-root CAS response-loss checkpoint

Checkpoint AU extends the same real Oxia/LocalStack process fixture with
`lostDeletedRootCasResponseReloadsExactDurableReplacementWithoutRepeatedDelete`. After activation, durable MARKED and
drain grace, `TargetDeleteTrackingObjectStore` records both the exact target DELETE completion and invocation count.
`PhysicalStoreDecorator.loseDeletedRootCasResponse` delegates the matching `DELETING -> DELETED` CAS to Oxia first；
only after the real future succeeds does it return a retriable timeout to the production coordinator.

`SourceRetirementCoordinator.completeDeletedRoot` treats that result as uncertain and reloads the exact physical root.
It accepts only the same immutable object、attempt、journal digest and complete DELETED replacement, returns DELETED,
and does not re-enter object deletion. The fixture observes that production reload before reading the root itself,
asserts one LocalStack DELETE, then starts a separately assembled runtime and proves the durable DELETED root remains
terminal with zero additional DELETE calls. No sleep or object LIST supplies authority. The focused method passed on
2026-07-18 under Java 21 and Docker 28.5.2 with 47 executed tasks.

`phase4M4DeletedCasResponseLossCheck` composes the AT gate、a dedicated AU ordering/contract audit and the pinned real-
service source set. It passed on 2026-07-18 under Java 21 and Docker 28.5.2 against locked Pulsar
`c59da789e88df2b57829de3277c60194b44fceb6`；the root aggregate reported 152 actionable tasks（67 executed、85 up-to-
date），and its two pinned Pulsar source-set builds executed 141/141 and 138/138 tasks. AU closes the uncertain DELETED-
CAS response cut, not the remaining MARKED/journal/source/protection process cuts、two-worker/broker、all-shard or
scale evidence.

### 6.25m F4-M4 two-worker destructive convergence checkpoint

Checkpoint AV adds `twoIndependentWorkersConvergeConcurrentDeletingIntentAndExactDeletes`. After one process activates
physical deletion and leaves a durable ownerless MARKED root, two separately assembled runtime instances use the same
real Oxia namespace and LocalStack object scope. `DeletingRootCasRace` holds both workers at the exact
`MARKED -> DELETING` replacement until both have arrived, then delegates both CAS operations to Oxia. Exactly one raw
CAS succeeds；the losing worker follows the production uncertain-intent path and proceeds only after reloading the
same complete DELETING replacement.

`TargetDeleteRace` then holds both recovery paths at the exact immutable target DELETE until both have arrived. V1
deliberately permits these idempotent recovery attempts：DELETING is durable shared intent, not a process lease. Both
provider operations complete safely, the competing terminal paths converge to one equal versioned DELETED root and
the object remains absent. The fixture uses futures rather than sleeps and proves two DELETING-CAS attempts（one raw
success、one raw failure）plus two bounded exact-delete attempts. The focused method passed on 2026-07-18 under Java
21 and Docker 28.5.2 with 47 executed tasks.

`phase4M4TwoWorkerConvergenceCheck` composes the AU gate、a dedicated AV concurrency contract audit and the pinned real-
service source set. It passed on 2026-07-18 under Java 21 and Docker 28.5.2 against locked Pulsar
`c59da789e88df2b57829de3277c60194b44fceb6`; the root aggregate reported 153 actionable tasks（68 executed、85 up-to-
date）, and its two serialized locked-Pulsar builds executed 141/141 and 129/129 tasks. AV closes the independent
worker contention slice, not real two-broker ownership/failover、metadata/source/protection process cuts、all-shard or
scale evidence.

### 6.25n F4-M4 all-shard durable recovery checkpoint

Checkpoint AW adds `freshProcessRecoversMarkedAndDeletingRootsFromEveryShardWithEmptyInventory`. The fixture derives
one real compacted-object key for each value of `F4Keyspace.physicalObjectShard` and persists all 256 objects and
ACTIVE roots through the configured LocalStack/Oxia adapters. After activation, one complete pass seals a retirement
journal and leaves every root MARKED before the process closes. A setup process that never starts the lifecycle moves
only odd-numbered shards to the exact production-shaped DELETING replacement, producing 128 durable MARKED and 128
durable DELETING roots without transferring process-local scan state.

A third independently assembled runtime forces every object LIST family to return empty. Its metadata-first root pass
reports exactly 128 MARKED plus 128 DELETING roots, visits all 256 shards, reauthenticates every sealed journal and
converges every object to durable DELETED with exact LocalStack absence. Thus LIST omission affects only orphan
discovery and cannot suppress registered-root recovery.

The first scale run exposed an obsolete scanner assumption：`ObjectInventoryScanner` compared the first logical key
of a new page with the last logical key of the previous page. That is invalid for the frozen object-store contract,
because the opaque `nls1` cursor traverses disjoint base64url physical prefixes and promises logical ordering only
inside each returned page. Production now verifies exact family prefix and rejects a repeated supplied opaque token,
without inferring cross-page order. Focused unit tests prove descending logical page order is accepted while an
unchanged non-terminal token fails closed. The focused real-service AW method passed on 2026-07-18 under Java 21 and
Docker 28.5.2 with 38 actionable tasks（2 executed、36 up-to-date）；the complete AS–AW real-service source set then
passed with 47/47 executed tasks. `phase4M4AllShardRecoveryCheck` composes the AV gate, scanner/module/source-lock
checks, the dedicated AW contract audit and that source set. The aggregate passed on 2026-07-18 under Java 21 and
Docker 28.5.2 against locked Pulsar `master@c59da789e88df2b57829de3277c60194b44fceb6`；the root build reported 154
actionable tasks（73 executed、81 up-to-date）, and its two serialized locked-Pulsar builds executed 141/141 and
138/138 tasks.

AW closes the required every-root-shard/empty-inventory recovery slice. At that checkpoint, the 1,001-roots-in-one-
shard and 10,000-root tombstone/cursor scale fixtures, source/protection deletion kill points, late PUT matrix and real
two-broker ownership/failover gate remained pending；AX below closes the first of those scale lines.

### 6.25o F4-M4 physical-root pagination scale checkpoint

Checkpoint AX adds `freshProcessPaginatesOneThousandOneRootsInOneShardAndEveryOtherShard`. A first process derives
deterministic object-key hashes with exactly 1,001 roots in physical shard 0 and one root in each of the remaining 255
shards, then persists all 1,256 ACTIVE roots through the production four-shard Oxia adapter in bounded write batches.
No fake metadata store or process-local registry is used, and the process closes before enumeration begins.

A separately assembled process starts `PhysicalObjectRootScanner` from empty continuations with the production page
size of 64. Its metadata-store audit requires the first call for every shard to carry no continuation and every later
hot-shard call to carry one. The scan observes all 1,256 distinct immutable identities exactly once：shard 0 takes 16
pages and every other shard takes one. The focused method passed on 2026-07-18 under Java 21 and Docker 28.5.2 with
47 actionable tasks（2 executed、45 up-to-date）；the complete AS–AX real-service source set then passed with 47/47
executed tasks. `phase4M4RootScaleCheck` composes AW, that source set, materialization checks and a dedicated static
audit. The aggregate passed on 2026-07-18 under Java 21 and Docker 28.5.2 against locked Pulsar
`master@c59da789e88df2b57829de3277c60194b44fceb6`；the root build reported 155 actionable tasks（70 executed、85 up-to-
date）, and its two serialized locked-Pulsar builds executed 141/141 and 138/138 tasks.

AX closes the required `1,001 physical roots in one shard plus at least one root in every other physical shard`
fixture. It does not close the 10,000-root tombstone/cursor bounds, source/protection deletion cuts, late PUT races or
actual two-broker ownership/failover gate.

### 6.26 F4-M5 durable registration frontier checkpoint

Checkpoint X 关闭 cold-topic backfill 开始前必须先关闭的新写前沿。`ProjectionIdentity` 现在拥有共享的
canonical present/absent encoder，materialization durable mapper 与 managed-ledger registration 不再各自复制
length-delimited encoding。`ManagedLedgerMaterializationRegistrationCoordinator` 只暴露 broker-safe 的
`managedLedgerName + expected immutable projection identity`；default 实现线性读取当前 exact topic
projection 和完整 L0 snapshot，验证 name/stream/profile/payload/lifecycle，派生 strict NPR1 ref/digest，
create-or-verify registration，并以 version CAS 单调刷新 `lastHintCommitVersion`。

CAS failure 或 response loss 只在 reload 得到同一 ref/digest/profile 且 hint 已覆盖本次观察值时收敛。
返回成功前再次读取 projection、L0 和 registration；mutable property CAS 不会改变 registration identity，
同名 recreation、DELETING/DELETED 或 L0/profile drift 会 fail closed。`NereusManagedLedgerOpenCoordinator`
现在在 create、existing open、sealed open 和 recreate 的 ledger result 返回前等待该 coordinator，因此
broker topic publication 不会越过 registration。`DefaultNereusRuntimeProvider` 通过同一 shared Oxia runtime
装配并由 `NereusManagedLedgerRuntime` 持有/关闭 production generation metadata store。

`phase4M5RegistrationFrontierCheck` 是 checkpoint X ordinary gate。它不设置 generation marker，不实现
Pulsar generation lookup property/two-snapshot barrier，不遍历 tenants/namespaces/persistent topics，也不 CAS
cluster `streamRegistrationBackfill` proof；其中 broker capability/readiness 已由 checkpoint Y 单独实现，
其余仍是后续 M5 broker rollout checkpoint。该 gate 已于
2026-07-16 使用 `--rerun-tasks` 通过。

### 6.27 F4-M5 generation capability/readiness checkpoint

Checkpoint Y 在 local Pulsar fork 把 `nereus.generation-protocol=1` 作为第三个 broker-reserved lookup
property 发布；operator 配置不能伪造 binding、cursor 或 generation protocol。新的
`NereusGenerationCapabilityReadiness` 冻结 `(brokerReadinessEpoch, brokerSetSha256,
persistentBrokerCount)`。Coordinator 对全部启用 persistent topics 的 broker 连续读取两次完整 lookup
snapshot，要求 broker key set、binding/cursor/generation exact versions 和 canonical readiness identity
完全一致，同时要求本地 registry 始终 started/registered。

Readiness identity 使用 domain-separated、length-prefixed SHA-256，按 broker registry key 排序，并纳入
registry key、advertised `BrokerLookupData.brokerId`、`startTimestamp` 与排序后的 required capability
pairs。同 broker id 的进程重启因此产生新 identity；V1 durable field 使用 SHA-256 前 63 bit 的非负
`long` epoch，完整 lowercase digest 留作进程内精确比较和 backfill report identity。Broker registry
notification 会递增 process-local revision 并清空 cache；通知发生在两次相同 snapshot 之间也必须返回
`NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED`，不能发布 stale readiness。

`phase4M5GenerationCapabilityCheck` 是 checkpoint Y ordinary gate。它验证 exact clean Pulsar source lock、
contract surface、spotless、checkstyle，以及 generation/cursor/binding focused suites；冻结的 two-broker
fixture epoch 为 `4351585672493013605`，full digest 为
`bc63f01d0aa01a65c7205625a2714f0246d8ba7e7b88b8a653137abbc719cc0d`。该 checkpoint 不枚举 cold
topics、不创建 registration backfill proof、不设置 topic generation marker，也不启用 activation/publication/
deletion。Gate 已于 2026-07-16 使用 `--rerun-tasks` 通过。

### 6.28 F4-M5 cold-topic registration backfill checkpoint

Checkpoint Z 在 product 侧新增
`ManagedLedgerMaterializationRegistrationCandidate`：从 exact live `OPEN`/`SEALED` topic projection 捕获
`managedLedgerName + storageClassBindingGeneration + ManagedLedgerProjectionIdentity + strict NPR1 SHA-256`，
并在 broker 写 registration 前后保持 immutable。Factory/coordinator 暴露 broker-safe inspect/ensure 两段 API；
existing unloaded topic 不需要 load 或 acquire ownership。

Locked Pulsar fork 新增 canonical one-namespace-at-a-time traversal。它按 tenant/namespace/topic 排序，system
topic 只进入 coverage digest、不进入 persistent-topic counter；non-Nereus、missing 或 deleting/deleted binding
计入 skip。Live Nereus binding 先读取 exact candidate，再执行现有 idempotent create-or-verify registration，
最后重读 binding；generation/storage-class drift fail closed，registration 后进入 deletion 则作为
`DELETED_AFTER_REGISTRATION` skip。Topic work 以 request concurrency 分批执行并按 canonical order fold，
整个 run 共用一个 deadline；start/end 必须得到完全相同的
`NereusGenerationCapabilityReadiness`。

Report 强制 `persistentTopicsScanned == registered + skipped + failed`，hash 每个 canonical resource/outcome 和
全部 failure，只保留前 100 个 redacted failure。固定 fixture coverage SHA 为
`2f234d6b9baa3a760460090850d22734f94cd72d51fd0f27706fda272fc01d7c`。Broker config 默认并发 `16`、
whole-run timeout `3600s`，并在 `BrokerRegistryImpl` 初始化时把 tenant/namespace/topic resources attach 到
`NereusManagedLedgerStorage`。

`phase4M5RegistrationBackfillCheck` 是 checkpoint Z ordinary gate。它验证 exact clean Pulsar source lock、
product candidate/API、broker traversal/config/lifecycle wiring、coverage golden、bounded concurrency、101-failure
retention、binding drift 和 readiness drift。该 checkpoint 只生成 in-memory report；不 CAS durable
`streamRegistrationBackfill` proof，不设置 topic generation marker，也不启用 activation/publication/deletion。
Gate 已于 2026-07-16 使用 `--rerun-tasks` 通过。

### 6.29 F4-M5 durable registration proof checkpoint

Checkpoint AA 把 broker readiness 与 durable activation authority 的 ownership 边界落成代码。Pulsar
`NereusGenerationCapabilityReadiness` 可无损转换为 core
`GenerationCapabilityReadiness(epoch, full broker-set SHA-256, persistent broker count)`；
`NereusBrokerCapabilityCoordinator` 同时实现 product-neutral readiness provider。Runtime context/provider
把该 provider、shared-Oxia `GenerationProtocolActivationStore`、当前六个 canonical reference domains 和
`DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator` 组合到 managed-ledger runtime，由 factory
暴露单一 completion API。

Broker traversal 在 final full-readiness 比较后生成 report。`failureCount != 0` 时只返回 report，不调用 proof
owner；零失败时在同一 whole-run deadline 内提交
`GenerationRegistrationBackfillCompletion(runId, readiness, coverageSha256, 0)`。Broker 不 import、读取或 CAS
activation store。Product coordinator 在 CAS 前重新获取两次稳定 snapshot 对应的 exact readiness，要求与
report 完全相同，再以最多 32 次 bounded CAS 安装 `streamRegistrationBackfill`。同 epoch 已完成 proof
只接受相同 coverage digest，允许不同 run id 的幂等 rerun；另一 digest 是 metadata invariant failure。

Readiness epoch 前进时，stream proof 更新到新 epoch，同时把 physical-root/cursor-snapshot proof 重置为该
epoch 的 incomplete 并清空 object-store capability；若任一 deletion bit 已启用，则拒绝单独推进 registration
proof。CAS response loss 与 condition conflict 都从 durable activation reload 后按 exact coverage 收敛。
CAS 后必须再次验证 process-local exact readiness，并重读 durable proof；期间 readiness invalidation 会让调用
失败，但已写入的旧 epoch proof 本身不会授权后续 mutation。Activation guard、topic marker、publication/
delete enablement 均不属于本 checkpoint。

`phase4M5RegistrationProofCheck` 是 checkpoint AA ordinary gate。它继承 checkpoint Z 的 clean locked
Pulsar formatting/checkstyle/focused tests，增加 core/managed-ledger/adapter checks 和 contract-surface audit，
覆盖 zero-failure admission、nonzero-failure rejection、same-epoch coverage immutability、newer-epoch dependent
proof invalidation、lost-CAS-response convergence、post-CAS readiness invalidation、runtime ownership/close order
以及 broker 只在成功 final revalidation 后提交 proof。Gate 已于 2026-07-16 通过。

### 6.30 F4-M5 generation activation guard checkpoint

Checkpoint AB 落地 `ManagedLedgerGenerationProtocolActivationGuard`。每次 `requireReady` 都先取得 exact
`GenerationCapabilityReadiness`，再要求 durable cluster activation 已是 `ACTIVE`、publication bit 已开、
required reference domains 与本 runtime 的六项 canonical set 完全一致，并且
`streamRegistrationBackfill` 已在同一 readiness epoch 完成。六项 domain set 的冻结 SHA-256 是
`5b29cf6df71cce198d01299f5bd740f0f123c601e12f04d8251d336a6a2a8c4d`。

Live subject 必须是 strict NPR1 identity。Guard 重读 binding-selected projection、topic metadata version、L0
stream identity/profile/lifecycle 和 exact materialization registration；首次 marker CAS 只有在
`nereusGenerationProtocolEnabled=true` 时允许，写响应丢失只在 exact projection reload 已显示 marker 后
收敛。开关随后关闭不会撤销 monotonic marker。生成的 proof 绑定 topic metadata version、cluster activation
metadata version、readiness epoch、domain-set digest 和 capability bits；`revalidate` 在 mutation CAS 前重读
这些 authority，任何 topic property/version、readiness 或 activation drift 都 fail closed。

Physical delete 只接受 `DomainValidatedDeletionSubject`，除 ACTIVE/publication 条件外还要求 physical/cursor
delete bits、同 epoch physical-root/cursor backfill proof、object-store capability，以及 exact complete、
non-vetoed `projection-generation-v1` snapshot。Logical trim 同样要求 deletion capability，但继续使用 live
projection subject。Runtime provider 使用 shared projection/L0/generation/activation stores 构造 guard，
`NereusManagedLedgerRuntime` 只暴露 typed guard，不把 raw activation metadata 交给 broker。

Pulsar fork 新增默认 `false` 的 `nereusGenerationProtocolEnabled`，checked broker configuration 把它作为
first-activation switch 传入 `NereusRuntimeContext`。该开关不会把 cluster record 从 `PREPARED` 推进到
`ACTIVE`，本 checkpoint 也尚未把 guard 接入 task/index/checkpoint/trim/delete mutation call sites，因此
publication、async profile 和 physical deletion 仍不可用。

`phase4M5ActivationGuardCheck` 是 checkpoint AB ordinary gate。它验证 frozen domain digest、first-marker
disable/enable、existing marker monotonicity、lost-response convergence、missing coverage/registration、
projection/readiness drift、physical-delete snapshot drift、runtime ownership，以及 locked Pulsar broker
configuration 的格式、checkstyle、编译和 focused test。Gate 已于 2026-07-16 通过。

### 6.31 F4-M5 publication-only cluster activation checkpoint

Checkpoint AC 落地 `ManagedLedgerGenerationProtocolActivationCoordinator` 与
`DefaultManagedLedgerGenerationProtocolActivationCoordinator`。Coordinator 是 cluster activation metadata 的
唯一 product owner；broker/factory/runtime 只持有 typed `activatePublication()` 边界。首次 activation 在开关
关闭时不创建 rollout authority；已经 ACTIVE 的 monotonic record 在开关随后关闭时仍可被验证。

每次 activation 都重新取得完整 `GenerationCapabilityReadiness`，要求 durable
`brokerCapabilityReadinessEpoch` 完全相同、`streamRegistrationBackfill` 在该 epoch 完成、required domains
等于本 runtime 的 canonical six-domain set。唯一新写是
`PREPARED -> ACTIVE(publication=true, physicalDelete=false, cursorSnapshotDelete=false)`；已有 physical/cursor
proof 和 object capability 只被原样保留，controller 绝不打开删除位。`activatedAtMillis` 取不早于
prepared/updated/current clock 的单调值。

CAS 最多重试 32 次。Condition conflict 重读 PREPARED 后重算；并发 actor 或丢失成功响应只在 durable value
已是满足同一 readiness/proof/domain 条件的 ACTIVE 时收敛。CAS 后必须验证 process-local cached readiness、
重读 ACTIVE authority，再验证 readiness；若 membership/property notification 在任何 final cut 发生，调用方
失败，但 durable old-epoch ACTIVE 不会越过 checkpoint AB guard。

Pulsar `NereusManagedLedgerStorage.runGenerationRegistrationBackfill` 现在按顺序等待 traversal、零失败 durable
proof，再在 `nereusGenerationProtocolEnabled=true` 时调用 product coordinator；失败 report 和默认 false
均只返回 report，不尝试 activation。Activation failure 使 backfill completion promise 失败，重跑通过
idempotent proof/CAS 收敛。该流程仍未创建 topic marker，marker 只会由后续具体 F4 mutation admission 触发。

`phase4M5PublicationActivationCheck` 是 checkpoint AC ordinary gate。它覆盖 disabled no-create、missing/stale
proof、publication-only fields、existing ACTIVE monotonicity、condition-conflict retry、lost-response reload、
post-CAS readiness invalidation、domain drift，以及 broker 对成功/失败/disabled backfill 的 exact sequencing。
Gate 已于 2026-07-16 通过。

### 6.32 F4-M5 async Object-WAL acknowledgement and protected repair checkpoint

Checkpoint AD 落地 `Phase4StorageProfileResolver` 的 frozen matrix：同步 Object-WAL 只接受
`WAL_DURABLE_AND_INDEX_COMMITTED`；异步 Object-WAL 同时接受 `WAL_DURABLE` 与 strict durability；所有
BookKeeper profile 在 adapter gate 前继续于 primary IO 前拒绝。`AppendCoordinator` 的 legacy constructors
仍注入 `Phase15StorageProfileResolver`，`DefaultStreamStorage` 只新增显式 resolver seam，避免单凭 durable
enum/property 就把未完成的 broker admission 当作 production enablement。

`AsyncObjectWalAppendCoordinator` 现在把 protected stable head 作为 `WAL_DURABLE` success boundary，并以独立
timeout 在 detached single-flight 中执行
`materializeGenerationZero -> VISIBLE_GENERATION protection`。secondary failure 只累计为 repair failure，不会
撤销已经返回的 exact stable offsets；strict durability 则继续等待同一 exact protection。普通 append 与
`recoverAppend` 共享该 cut，close 会在 admitted append drain 后停止新 background work，并在剩余 shutdown
budget 内 best-effort 等待；未完成工作仍由 durable scanner 收敛。

`GenerationZeroRepairScanner` 对 recovery-root double-read 后的 bounded live tail 水合 exact generic commit，
绑定 observed head reachability，跳过 fully trimmed commits，并为每条 materialized index 建立 permanent
`VISIBLE_GENERATION` protection。它明确拒绝从 NRC1-covered offset 重建已退休 generation-zero target；
`CheckpointDerivedIndexRepairer` 新增 injectable live repairer seam，production composition 可以用
`ReadAfterStableCommitRepair` 处理 live tail，再保留既有 NRC1 higher-generation restore path。
`GenerationReadResolver` 因此接受两个 Object-WAL profile，但 default `DefaultStreamStorage` 仍未切换到 F4
generation resolver。

聚焦测试覆盖 detached work 尚未开始即返回、strict protection wait、secondary protection failure 不撤销 ack、
真实 facade append 在 gen-0 materialization 挂起时仍只于 protected head 后返回、恢复后 exact protection
存在、restart scanner、trim-wins 和 read-after-commit repair。`phase4M5AsyncObjectWalCheck` 是 checkpoint AD
ordinary gate；它继承 checkpoint AC 的完整 rollout gate，再运行 core 回归、module/docs audit 和本
checkpoint contract-surface audit。

该 checkpoint 仍不是 async production activation：exact registration/topic marker proof 和
`MaterializationLagGate` 当时尚未在 primary upload 前接入，runtime 也尚未装配 F4 generation reader、
checkpoint repairer、scanner/service lifecycle。

### 6.33 F4-M5 pre-I/O async admission and exact lag checkpoint

Checkpoint AE 新增 `AppendAdmissionGuard`/`AppendAdmissionRequest`，并把 guard 放进
`AppendCoordinator.StreamLane` 的串行执行段：profile 已从 L0 metadata 捕获，但 expected-offset 初始化、
session 获取、primary WAL preparation/upload 都还未发生。这样同一 stream 的并发 append 不能基于同一个旧
head 一起越过 lag gate；recovery 则明确绕过新写入 backpressure，继续收敛已经 durable 的事实。

`ManagedLedgerAsyncAppendAdmissionGuard` 只拦截 exact `OBJECT_WAL_ASYNC_OBJECT`。它按 stream 反查 F2
projection，要求 virtual-ledger binding、current topic、stream id、projection identity、managed-ledger name
和 async profile 完全一致，再用 NPR1 构造 `LiveProjectionSubject`，执行
`GENERATION_PUBLISH` activation/first-marker proof、lag admission 和最终 proof revalidation。同步 Object-WAL
不经过该 guard；所有 BookKeeper profiles 继续由 Phase 4 resolver/request factory 拒绝。F2 topic projection、
create request、open exact validation、append durability selection和 position projection 已同时扩展为 exact
sync/async Object-WAL profile round-trip，避免 L0 与 topic metadata 在 restart/open 后发生 profile 降级。

`DefaultMaterializationLagSnapshotReader` 不以 task count 或 advisory checkpoint 作为 lag truth。它从当前
lossless COMMITTED policy 的 healthy `COMMITTED` indexes 重建自 trim 开始的确定性最远连续覆盖，要求
projection ref、policy digests、NCP1 target type/format 全部匹配；checkpoint 仅允许落后或相等，领先即为
metadata invariant violation。未覆盖 records/bytes/oldest age 从 root-double-read 的 canonical live commit
tail 计算；若覆盖点早于 NRC1 recovery anchor，则只接受连续、source index key/version/digest/coverage 都重证
通过的 `RangeRetentionStatsRecord`，并要求末端 cumulative bytes 精确等于 anchor。任一 gap、漂移、超界或
四次 authority instability 都 fail closed。

`MaterializationLagGate` 的每个 records/bytes/age threshold 都可由零独立关闭。reject 在 primary IO 前返回
retriable `BACKPRESSURE_REJECTED`；throttle 只执行一次 bounded delay（默认 25ms）并重新测量，第二次仍可
升级为 reject。reads、repair、recovery、workers 不经过该 gate。Focused tests 已证明 gate delay/remeasure/
reject/disabled semantics、blocked admission 不触发 writer prepare/head advance、activation proof final
revalidation、exact live-tail lag 和 ahead-checkpoint rejection；四个受影响模块回归于 2026-07-16 通过。

### 6.34 F4-M5 production Object-WAL runtime composition checkpoint

Checkpoint AF 把 checkpoint AD–AE 的 correctness seam 作为一个不可拆分的 production unit 安装。
`Phase4ObjectWalRuntime` 统一拥有 generation-aware reader registry、NRC1 replay、checkpoint-derived index
repair、generation-zero repair scanner、authoritative lag reader，以及 materialization planner/task
recovery/worker/committer/checkpoint/terminal-retirement service。`DefaultStreamStorage` 新增单一
`Phase4ReadComponents` 构造入口，因此 production provider 不可能只启用 async append resolver/guard 却继续
使用会在 `WAL_DURABLE` 与 gen-0 repair 之间误报 EOS 的 legacy read path。

`RegisteredMaterializationStreamScanner` 在恢复旧 task 或规划新 task 前先修复 authoritative generation-zero
source facts；broker restart 后即使 stable head 已存在而 gen-0 derived index 尚未完成，后台 planner 也不会把
缺失的 repairable source 当成空洞。`NereusManagedLedgerRuntime` 拥有该 runtime，并在 generation stores 和
shared Oxia/ObjectStore 之前关闭 materialization lifecycle 与 worker executor。

Local Pulsar fork 的 `ServiceConfiguration`/`NereusBrokerStorageConfiguration` 现已映射 exact
`OBJECT_WAL_SYNC_OBJECT`/`OBJECT_WAL_ASYNC_OBJECT` default profile、worker/planner/staging/claim/retry/policy/
lag/recovery-checkpoint limits，并把 processRunId 附加到绝对 staging base。未知 profile、deprecated alias、
相等或反向 lag thresholds、跨层 worker/read-buffer/object-timeout/close-timeout 关系全部在 client 创建前
fail closed。默认 profile 仍是 sync；改变 broker default 只影响首次创建的 projection，既有 topic 不迁移。

Focused composition、materialization/managed-ledger/adapter regression 和 locked Pulsar configuration test 于
2026-07-16 通过。该 checkpoint 不启用 BookKeeper profile，也不实现 retention policy/admin、cursor snapshot
candidate/deletion、object inventory、registration retirement 或 physical delete。

Checkpoint AG 完成逻辑 retention 的 product-neutral correctness slice。`RetentionPolicySnapshot` 冻结 exact
policy version/time/size 并用 checked arithmetic 转换 Pulsar minutes/MiB；`RetentionCandidate` 把 cursor/time/
size cuts、head/cursor/policy versions、最多 4,096 个 canonical stats tokens 和完整 SHA-256 evidence 绑定为
ephemeral value。`DefaultRetentionCandidatePlanner` 对每个 stats value 重证 exact source-index
key/version/durable SHA 和 COMMITTED-view range/commit/cumulative identity，使用严格时间边界与 time-OR-size
公式，并在 plan/final-revalidate 各要求两次完全相同的 authority capture。`NereusManagedLedgerRetentionService`
按 ownership -> activation -> exact policy -> stable plan -> activation revalidate -> planner revalidate -> F3 trim
-> final ownership 执行；它不直接调用 `StreamStorage.trim`，也不等待 physical GC。九个 focused tests 与
`phase4M5RetentionPlannerCheck` 覆盖 policy/config bounds、stale/incomplete source、pending lifecycle、authority
drift、exact call order、no-op 和 durable trim 后 ownership loss。该 checkpoint 尚未把 policy/admin 和 service
装入 Pulsar/production managed-ledger，`maxConcurrentPlans`/`maxQueuedPlans` 的共享 coalescing lane 也仍待实现。
`phase4M5RetentionPlannerCheck` 已于 2026-07-18 在 Java 21 下通过完整前置 Nereus 与 locked-Pulsar gates。

Checkpoint AH 把 AG service 装入 writable `NereusManagedLedger`。Process-owned `NereusRetentionRuntime` 通过
fixed-size executor 与 bounded queue 限制实际未完成的 async plan 数量，同一 stream 共享一个 admitted root
future，但每个 housekeeping/admin caller 获得独立 completion；operation timeout 覆盖整个 service future，
runtime close 先拒绝新任务再 bounded drain/force-fail。Facade 只在 exact policy snapshot 已安装时调用该 lane，
成功边界仍是 F3 durable logical trim。Pulsar broker config 现映射 page/concurrency/queue/operation/close 五个
边界并在 client construction 前做跨层校验。在 AH source lock 上，topic-policy resolver 尚未把 exact
effective `RetentionPolicies` 安装为 snapshot，loaded/unloaded admin gate 也尚未放行 `TRIM_TOPIC`；checkpoint
AI 在下节关闭了该 logical-retention broker 缺口。Physical deletion 在两个 checkpoint 中都继续不可用。
`phase4M5RetentionRuntimeCheck` 已于 2026-07-18 在 Java 21 和 locked Pulsar
`master@68093ba53388c4cdbe6516a35391451646820c71` 上通过 151-task 完整前置/模块/契约/Pulsar focused 链。

Checkpoint AI closes the broker logical-retention policy/admin cut. `NereusResolvedTopicFeatures` now retains exact
immutable effective `RetentionPolicies` and both typed `BacklogQuota` values, plus precise-time mode and runtime
readiness；the resolver keeps local > global > namespace > broker precedence and checked conversion rejects invalid
or overflowing retention values before publication. `NereusTopicOpenContext` carries the exact derived
`RetentionPolicySnapshot` and rejects any mismatch. When a retention or consumer-eviction policy first needs F4,
`PersistentTopic` waits for ownership、registration-backed marker activation/revalidation and a fresh equal policy
input tuple before installing config/policy/features；a stale async completion cannot install its snapshot. Loaded
admin uses that exact feature snapshot；unloaded bound topics require current cluster generation readiness before
`TRIM_TOPIC` loads, and the loaded route validates again before `trimConsumedLedgersInBackground`. Size eviction and
precise time eviction are admitted only with generation readiness；non-precise time eviction stays rejected, while
producer hold/exception remains non-destructive. The service repeats durable activation/ownership/policy/planner/F3
checks at mutation time, so the broker projection is not a correctness owner. `phase4M5RetentionPolicyAdminCheck`
and its focused Java/style/static-documentation chain are the checkpoint-AI evidence boundary；physical deletion is
still disabled. The aggregate gate passed on 2026-07-18 under Java 21 against locked Pulsar
`master@330eeeb3fa9903ed0123c2a0e261d403c32f0a59` with 153 Nereus/inherited/source/document/Pulsar tasks；its final
four-suite broker invocation passed 129 tasks.

## 7. Milestones

| Milestone | Deliverable | Current status |
| --- | --- | --- |
| F4-M0 | local source audit and code-level protocol/design gate | complete in docs；design-only |
| F4-M1 | metadata/object lifecycle primitives、list/delete、reader lease and codecs | complete/final-gated on 2026-07-15 |
| F4-M2 | generation publication、committed resolver、target-reader dispatch and fallback | complete/final-gated on 2026-07-15；real Oxia/LocalStack restart、concurrency、pin/quarantine/fallback evidence passed |
| F4-M3 | lossless/topic compacted format、planner/task/worker and sync-profile materialization | complete/final-gated on 2026-07-15；real Parquet/Oxia/LocalStack two-worker、restart、response-loss、full-byte and all-shard pagination/watch-loss evidence passed |
| F4-M4 | recovery checkpoint、source/index retirement and physical/cursor-snapshot GC | in progress；through checkpoint AX, NRC1/recovery replay/index repair、exact retirement metadata、GC plans/root fence/scanner、root-authenticated journal/destructive recovery、typed source handlers、all completed-trim/COMMITTED/TOPIC_COMPACTED source-eligibility paths、grace-fenced higher pre-drain/reproof、durable activation authority、future sentinel、five affected/ownerless domains、dual-absence DELETED-root retirement、guarded/protected/pinned cursor snapshots、all-shard physical/cursor live-reference backfill、restart-reconstructable cursor/ownerless candidates、the explicit root lifecycle router、strict known-prefix missing-root inventory registration、registration-last deleted-stream retirement、non-overlapping metadata-first periodic runtime、exact broker typed physical-GC configuration、configured-scope canary、atomic proof composition/delete activation、provider/Pulsar wiring、restart scope-digest fencing、one shared ownerless-reference interpretation、the first real Oxia/LocalStack wrong-scope/empty-list/lost-DELETE-response restart slice、real post-DELETE/pre-DELETED-root-CAS independent-process recovery、applied-DELETED-CAS response-loss exact reload without repeated DELETE、two independent worker runtimes contending/converging on one durable intent、mixed MARKED/DELETING recovery across all 256 root shards with opaque LIST progress and fresh-process 1,001-root hot-shard pagination over 1,256 real Oxia roots are implemented/tested；checkpoint AF separately composes the non-destructive replay/index/source-repair and materialization lifecycle in production, while the remaining real-service scale/destructive matrix and final gate remain pending |
| F4-M5 | Object-WAL async profile、Pulsar retention/admin/capability integration | in progress；checkpoint X implements exact durable registration create/refresh/final revalidation、topic open/recreate return barrier and shared generation-store production ownership；checkpoint Y adds reserved generation capability and deterministic two-stable-snapshot broker readiness/invalidation；checkpoint Z adds exact unloaded projection candidate plus canonical bounded cold-topic traversal/report；checkpoint AA adds product-owned durable registration proof CAS and exact broker readiness handoff；checkpoint AB adds product-owned activation proof/revalidation plus the disabled-by-default first-marker switch；checkpoint AC adds proof-gated publication-only cluster ACTIVE transition and broker sequencing；checkpoint AD adds the opt-in Phase 4 Object-WAL matrix、protected-head `WAL_DURABLE` cut and protected live-tail/read repair；checkpoint AE adds exact F2 sync/async profile round-trip、per-stream pre-I/O activation/revalidation and authoritative lag gate；checkpoint AF installs the coupled production resolver/read-repair/materialization runtime and exact Pulsar profile/config mapping；checkpoint AG adds exact policy/config/evidence values、stable source-verified candidate planning and ownership-safe F3 logical-trim delegation；checkpoint AH adds the shared bounded/coalescing execution lane、production ledger/facade installation and exact typed broker config mapping；checkpoint AI adds exact effective Pulsar retention/backlog projection、generation/marker-gated policy install and loaded/unloaded/partition-child logical trim admission；physical GC composition and final rollout gates remain |
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
