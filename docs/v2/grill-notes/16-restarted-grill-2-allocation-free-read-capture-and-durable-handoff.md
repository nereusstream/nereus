---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 14: allocation-free read capture and durable handoff

Date: 2026-08-09

Round 13 kept partial recovery omission evidence-blocked, fixed the compact optional provider-proof semantics, and
accepted a logical Binding read snapshot with high-frequency frontier publication separated from low-frequency source
handoff. The newly reachable decisions are how a reader captures that state coherently without allocation/global
contention and what durable cut makes the two-stage fallback/protection transition crash-safe. Neither recommendation
below is normative until explicit confirmation.

## Source facts and constraints

- Hidden locator installation followed by release-published `ReadableFrontier` already gives append publication a
  one-way local ordering. A new immutable source-selection object per ACK is therefore unnecessary.
- Allocation-free RCU/hazard acquisition still has a load-versus-retire race: a reader that loads an old generation but
  has not yet published its slot cannot rely on that generation until it revalidates the current pointer.
- Revalidation must occur after coherent frontier/view capture. If a reader revalidates generation G first and then
  loads the frontier, a concurrent switch can expose a G+1 frontier through a pin on G.
- Source-generation swaps are low-frequency, while frontier advance is high-frequency and serialized per Binding. A
  process-global refcount/stamp would introduce unrelated reader and Binding contention.
- A local view that no longer names fallback is insufficient durable deletion evidence after owner crash. The new
  owner must be able to reconstruct whether fallback is still part of the selected manifest view before releasing
  source protection.
- A durable no-fallback generation is necessary but not sufficient while an older Owner Epoch may still hold an
  owner-local pin. Planned handoff can prove quiescence; unplanned takeover needs an authority-backed expiry/grace cut
  or must retain protection.
- `V2-OPEN-OBJ-22` may collect hypothetical skip-hit data at M3/M4 but waits for combined M3/M7 end-to-end recovery
  evidence before reopening. `V2-OPEN-OBJ-24` waits for Provider token/cap/range-benefit evidence. Neither has a
  decision frontier in this round.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-READ-03` |
| Q2 | `V2-OPEN-READ-04` |

❓ **Q1** - **Allocation-free coherent read capture**: Without one snapshot object per ACK/read or one global
refcount, what exact acquire protocol prevents both pin-after-retire and a generation-G pin paired with a frontier or
active-tail view published under G+1?

➡️ Recommend a Binding-local generation pointer plus preallocated reader slots/hazard cells. The logical acquire is:

1. acquire-load current source-selection generation/reference G;
2. publish G into the caller's preallocated Binding/event-loop reader slot;
3. acquire-capture the high-frequency `{ReadableFrontier, activeTailViewVersion}` as one coherent publication unit;
   this may use an existing typed frontier cell or preallocated/versioned state and must not allocate one object per
   ACK/read;
4. acquire-load the current generation/reference again;
5. accept only if it is still G and the captured publication state is compatible with G; otherwise clear the slot and
   retry locally;
6. clear the slot after the complete binding-scoped protocol read batch.

The capture occurs before the final generation reload: a switch before or during capture forces retry, while a switch
after successful validation cannot retire G until the published slot clears. Source-generation swap and append
publication use the existing Binding-local serialized cut. The publisher release-swaps only the low-frequency pointer,
then waits until no slot names the retired generation. Append remains `hidden locator -> release frontier -> ACK`; it
does not update source generation or a process-global sequence. Owner fence and all snapshot identities are validated
through the pinned Binding state.

Slots are preallocated/bounded per event loop or admitted reader domain, not per read. Slot exhaustion backpressures
new reads before pin acquisition. A slot stores the complete, non-reused generation identity/reference as its ABA
fence. Exact coherent frontier-cell representation, array layout, cache-line padding, and VarHandle/RCU implementation
remain evidence-selected; the contract is capture-before-final-revalidation plus generation-specific drain, not a
Java class.

The tradeoff is one local slot publish/clear and normally two pointer loads per read batch. It avoids heap allocation
and process-global atomic contention, but requires hard slot/backlog admission and careful memory-order tests.

❓ **Q2** - **Durable fallback-removal, old-owner quiescence, and crash cut**: Is a local successor view enough to
release source protection, or must the no-fallback transition and every Owner Epoch that could still hold a pin become
provably quiescent before drain and GC?

➡️ Recommend two immutable, fenced manifest generations:

1. `PREFERRED_WITH_FALLBACK`: binds the preferred generation, exact fallback source descriptors, and protection
   generation. The owner installs/pins this view; after older view pins drain it may retire obsolete local index
   structures, but not fallback protection.
2. `PREFERRED_ONLY`: a later manifest-root CAS selects a generation that no longer names fallback. Lost publication
   response converges only by exact root/generation reread.

Durable `PREFERRED_ONLY` is necessary but not sufficient for protection release. Every Owner Epoch that could have
admitted a fallback-bearing read must also be quiescent:

- planned handoff stops old-owner read admission, drains its bounded read-batch slots, and hands off an exact
  generation/owner-fence quiescence proof before protection release;
- unplanned takeover first durably fences the old Owner Epoch. If the ownership authority exposes a qualifying
  expiry/lease proof, release additionally waits through the hard maximum read-batch deadline and admitted clock-skew /
  grace bound. A host-local timer alone is not proof;
- if neither an old-owner drain proof nor a qualifying authority-backed expiry exists, 0.2 retains protection and
  defers GC. It does not invent a distributed per-read refcount to reclaim space.

Only after durable no-fallback selection, old-owner quiescence, and current-owner drain of every fallback-bearing
generation may the owner CAS/release the exact protection generation and admit GC.

A crash before protection release leaks protection safely. A crash after durable `PREFERRED_ONLY` publication but
before drain/release makes the new owner reconstruct that generation, re-establish old-owner quiescence, repeat local
drain/reconciliation, and release the exact protection idempotently. No local view, cache state, host timer, or missing
reader authorizes release; no `PREFERRED_WITH_FALLBACK` generation may remain readable after its protection is removed.

Publication of `PREFERRED_ONLY` requires final validation that the preferred generation is still selected/readable and
that protocol retention no longer requires fallback. Once fallback protection is durably removed, later preferred
corruption follows the already accepted quarantine/unrecoverable-data contract; recovery cannot resurrect an
unprotected source.

The cost is one additional low-frequency manifest generation/CAS per source retirement plus fail-safe retention during
unproven owner quiescence. It buys replayable crash cuts without adding remote per-read pins and prevents a new owner
from guessing whether another process still has an admitted read.

## Deferred descendants

- Q1 must settle before reader-slot ownership, close/drain, cache-line layout, and exact M4 concurrency tests freeze.
- Q2 must settle before protection-release response loss, planned/unplanned owner quiescence, manifest-generation
  retirement, and physical GC ordering freeze.
- Pin/retired-view numeric limits and Q1 implementation family remain evidence outputs after the logical cuts settle.
- `V2-OPEN-OBJ-22`, `V2-OPEN-OBJ-24`, `V2-OPEN-BK-11`, `V2-OPEN-BK-13`, remaining `V2-OPEN-OBJ-17`,
  `V2-OPEN-OBJ-19`, and `V2-OPEN-PUL-OBJ-09` remain evidence-blocked.
- KoP remains documented and deferred outside the 0.2 runtime.

## Original confirmation boundary

At proposal time, no Round 14 recommendation above was normative. The adjusted response below is the explicit
confirmation boundary and supersedes the proposed acquire order and unqualified old-owner grace language above.

## Adjusted response preserved verbatim

````text
Round 14 两项均调整后确认，不按原文直接确认。

Q1 — 确认 allocation-free coherent capture 目标，但调整 acquire 协议和 slot 作用域。

原方案在 publish slot 后、最终 generation 复核前执行 capture。如果 capture 会解引用 G 或访问 G 选择的 locator/index，就存在经典 pin-after-retire 竞态：publisher 可能在 slot 发布前已扫描并退休 G。

建议采用 generation-tagged publication cell 加标准 hazard 顺序：

1. acquire-load current generation/reference G；
2. 将 {Binding identity, G} 发布到当前 read batch 独占的 slot；
3. slot publication 与下一次 pointer load 之间必须具有 Store→Load ordering；不能仅假定 VarHandle setRelease + getAcquire 自动满足。实现可以选择 volatile/seq-cst store 或显式 StoreLoad/full fence；
4. acquire-load current generation；不再是 G 则清 slot 并重试；
5. coherent capture 一个带 generation tag 的 publication cell：
   {sourceGenerationId, ReadableFrontier, activeTailViewVersion}
   并通过单引用或 seqlock 证明没有 torn read；
6. 只有 sourceGenerationId == G 且 seqlock 稳定才能使用 G 的 locator/source；否则清 slot 重试；
7. 在最后一次可能访问 source 的异步 I/O、fallback、decode 或 source-backed buffer 使用完成后清 slot。

pointer 在第 4 步之后切换并不要求 reader 重试：G 已被 slot pin 住，publisher 必须等待。generation tag 防止 G pin 搭配 G+1 frontier，因此不再需要“先 capture、再做唯一一次最终复核”这一危险顺序。

slot 不能按 Binding × event-loop 固定预分配：

- 一个 event loop 可以同时挂起大量异步 GET/read；
- 每个并发未完成的 binding-scoped read batch 必须独占一个 slot；
- 物理上使用按 shard/read-executor/event-loop 分片、跨 Binding 复用的 bounded slot pool；
- 逻辑隔离仍按 Binding/generation 校验；
- slot exhaustion 在 provider I/O 前 backpressure；
- 跨多个 Binding 的 Fetch 要么一次预留全部所需 slots，要么失败后全部释放/拆分，不能持有部分 pin 等待剩余 slots。

publisher swap 使用 release/volatile publication，reclaimer 以 acquire/volatile 读取 slots；clear 至少使用 release。具体数组布局、padding、VarHandle 和 RCU 实现继续由证据选择。

性能判断：

- 正常每个 read batch 增加一次 slot publish/clear、两次 generation load 和少量 seqlock load；
- Object 冷读下相对 GET 成本很小；
- cache hit/内存热读下可能可见，主要风险是 false sharing、slot pool 竞争、generation swap 扫描和切换期 retry storm；
- 禁止 per-read heap allocation、全局 refcount和远端 metadata I/O。

Q2 — 确认 PREFERRED_WITH_FALLBACK → PREFERRED_ONLY 两阶段 durable cut，但把 old-owner quiescence 定义为 capability-tiered 合同。

PREFERRED_ONLY 是停止新 fallback pin 的必要条件，但不能独立授权 protection release。释放还必须满足：

1. PREFERRED_ONLY 已通过 fenced manifest-root CAS 持久化，响应丢失按精确 generation/root reread 收敛；
2. 当前 owner 上所有 fallback-bearing slots 已 drain；
3. 所有可能持有旧 fallback pin 的 Owner Epoch 已有 durable quiescence proof；
4. exact source-protection generation 通过幂等 CAS/release 后，才允许 GC。

planned handoff 不能只依赖现有非权威 handoff hint。需要低频、精确的 durable OwnerReadQuiescenceProof，至少绑定：

- Binding/incarnation；
- fallback manifest/protection generation；
- old Owner Epoch；
- read-admission-stopped fence；
- drained-through read-view generation；
- 最大已接纳 source-access deadline。

连续多次 takeover 应使用可验证的 quiescedThroughOwnerEpoch 或等价有界证明，不能只证明最后一个 owner。

unplanned takeover 中，lease/expiry 只有同时满足以下条件才合格：

- ownership lease 同时授权并限制 read admission，而不只是 writer admission；
- authority 提供可验证的 notAfter/expiry 时间语义；
- 每个 read batch 有硬性的 maxSourceAccessLifetime，覆盖 provider I/O、retry、fallback、decode 和 source-backed buffer 使用；
- 旧 owner 在 lease 不确定/到期后不能接纳新读；
- GC pause、进程暂停或网络恢复后，在任何新的 source I/O、fallback/retry以及响应发布前都重新检查 owner fence和deadline；
- 新 owner 的等待基于 authority time/expiry proof，加 maxSourceAccessLifetime、声明的 clock-skew 和 propagation grace，不能只是本机 sleep。

普通 Owner Epoch、没有时间上限的 session-loss、单纯 KRaft/Pulsar ownership fence或 host-local timer都不足以证明旧 reader 已消失。Backend 不具备上述能力时，0.2 必须保留 protection、延迟 GC；不能用分布式 per-read refcount补救，也不能允许 Topic关闭该保护。

PREFERRED_ONLY 可以对一组有界 fallback sources/ranges 批量发布，避免每个 extent 单独一次 manifest CAS。

性能判断：

- 普通读取仍是零远端 metadata I/O，Q2 不增加读热路径成本；
- 每次有界 source-retirement batch 增加一次低频 manifest generation/CAS；
- 真实代价是无法证明 quiescence 时延长 source retention，因此必须把 retained source bytes/age 纳入 Cell 容量准入和告警；
- 安全边界不能配置关闭，quiescence capability 属于 Protocol Cell/backend 准入能力，不是 Topic 性能开关。

M4 应量化：
- read allocations/op 必须为 0；
- slot publish/clear 与 cache-line contention；
- capture retry rate和generation swap drain p99；
- slot-pool occupancy/scan time；
- PREFERRED_ONLY CAS 频率；
- retained protection bytes/age；
- takeover 到 GC eligibility 的 p99。
````

## Authoritative synchronization

- Q1 is frozen by
  [ADR 0070](../../decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md): standard hazard order,
  explicit StoreLoad, generation-tagged coherent publication, exclusive bounded cross-Binding slots, complete async
  source lifetime, and all-or-release multi-Binding reservation.
- Q2 is frozen by
  [ADR 0071](../../decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md): fenced
  `PREFERRED_WITH_FALLBACK -> PREFERRED_ONLY`, current/old-owner durable quiescence, qualified authority expiry,
  exact protection release, retained-source admission, and no remote per-read refcount.
- Exact slot-reuse/cancellation states, bounded multi-owner proof wire, backend capability encoding, all numeric bounds,
  and evidence-selected memory layout remain open or evidence-blocked.
