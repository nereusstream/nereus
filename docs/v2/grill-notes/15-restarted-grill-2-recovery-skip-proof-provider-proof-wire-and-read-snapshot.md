---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 13: recovery skip proof, provider-proof wire, and read snapshot

Date: 2026-08-09

Round 12 fixed physical-only checkpoint/Seal payload, combined tracker/locator reservation before position allocation,
one owner-local 64-bit ticket per protocol commit unit, and active-tail locator publication before Readable/Durable
frontiers and ACK. It deliberately did not freeze heavyweight Java data structures. The newly reachable frontier is
the authority that lets recovery omit manifest-covered prefix reads, the closed qualified-proof row variant, and the
reader snapshot used while active tail hands off to manifest. No recommendation below is normative until explicit
confirmation.

## Source facts and constraints

- A physical checkpoint row has no Binding or Protocol Coverage. Before reading its authenticated directory, recovery
  cannot infer which Binding manifests cover its members. Therefore “GET only the manifest-uncovered active tail” needs
  a separate exact omission proof; the row alone cannot supply it.
- A shared extent may contain members from several Bindings whose materialization progresses independently. A
  lane-sequence watermark is safe only for a contiguous prefix in which every member of every included extent has a
  selected readable generation; one Binding's coverage is insufficient.
- `ProviderObjectProof` already has semantic fields for immutable version, length, algorithm, scope, and checksum.
  Round 12 rejects opaque blobs, while Root and the physical row already carry provider scope, body length, and expected
  SHA. The remaining issue is what compact closed proof variant, if any, belongs in every row.
- The active-tail publisher and manifest publisher can overlap physically. Locator retirement is safe only after a
  reader can pin one coherent source-selection snapshot; local map mutation order alone is not a reader contract.
- Numeric NPD1/NWG1 caps, packing targets, tracker/index ceilings, and allocator mode remain evidence-blocked and are
  not questions in this round.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-OBJ-22` |
| Q2 | `V2-OPEN-OBJ-23` |
| Q3 | `V2-OPEN-READ-02` |

❓ **Q1** - **Authority to skip manifest-covered extent prefix GETs**: With no binding rows in physical checkpoint,
what exact durable fact may authorize recovery to skip a checkpointed extent rather than authenticate its directory?

➡️ Recommend one separately published, Root-bound `FullyManifestCoveredThrough[laneId]` vector for each WalRun. A
component may advance to sequence `n` only after a fenced reconciler proves that every append unit in every extent from
the prior component through `n` is covered by a currently selected readable manifest generation and retains the exact
source/fallback protection required by that manifest. Cross-binding membership comes from already authenticated
directories at append/materialization time; a single Binding cannot advance the vector by itself.

This vector is not checkpoint/Seal payload, append/ACK metadata, a protocol frontier, or GC permission. It only
authorizes omission of prefix GET for its contiguous covered prefix. It may lag or be absent; then recovery performs the
bounded prefix GET. A materialization hole merely prevents vector advance and cannot backpressure append, physical
checkpoint, Seal, or another Binding. Response loss uses exact reread/CAS, and unknown/mismatched Root or source facts
fail back to GET rather than skipping bytes.

The tradeoff is one low-frequency run-wide materialization-coverage record/reconciler. It avoids per-extent tombstones
and per-binding checkpoint rows, but a slow Binding can reduce this optimization's hit rate without affecting
correctness.

❓ **Q2** - **Closed qualified-provider-proof row variant**: Should each physical row copy the full generic proof, keep
only the immutable provider version needed to pin reads, or omit provider proof entirely?

➡️ Recommend a closed union with exactly two 0.2 variants:

```text
NONE
VERSION_BOUND_FULL_OBJECT_SHA256_V1 {
  boundedCanonicalProviderVersionToken
}
```

Root supplies provider adapter/scope. The surrounding row supplies exact body length and SHA-256; the variant ID fixes
`SHA-256` plus `FULL_OBJECT`, so length/digest/algorithm/scope are not duplicated. The version token is one explicitly
bounded canonical byte/string field, not an SDK object, header map, ETag, or extension blob. Unknown variant, malformed
token, composite scope, or provider mismatch fails closed. The token can pin prefix/frame GETs but never authorizes an
offset or replaces Object identity/directory AEAD.

If provider capability evidence cannot declare one safe format hard cap for that token before M3 freeze, choose `NONE`
for 0.2 rather than admitting an unbounded provider-specific field. Exact cap selection is evidence work, not a Topic
setting.

❓ **Q3** - **Active-tail/manifest reader snapshot and source handoff**: What atomic reader-visible state prevents a
range from disappearing or switching source under an in-flight read while locators retire?

➡️ Recommend a small logical `BindingReadViewSnapshot`, independent of the physical segmented-index implementation. It
binds the owner fence, Position Domain/version, published Readable Frontier, active-tail segment/index version,
manifest root/generation, and source-protection generation. Readers acquire and pin one snapshot, resolve each typed
range through it, and release the pin after the complete protocol read unit.

Active-tail publication installs hidden locator spans and then publishes a successor snapshot/frontiers before ACK.
Manifest handoff first publishes a successor snapshot in which the new preferred generation is readable and the exact
source remains protected fallback; only after old-snapshot pins drain may it remove old locator spans/protection. No
reader combines half of two snapshots. Corruption fallback follows the pinned snapshot and never invents a third source.

On takeover, each Binding remains locally `RECOVERING` until its snapshot is reconstructed; reads/appends for that
Binding wait or fail closed, while independently recovered B may open before A. The snapshot is owner-local derived
state, not a remote metadata record or one-object-per-append index, and normal reads perform no metadata access.

## Deferred descendants

- Q1 must settle before active-tail-only recovery can claim a GET reduction rather than only a bounded GET envelope.
- Q2 must settle before exact checkpoint row field IDs/widths and page-size arithmetic freeze.
- Q3 must settle before locator-retirement concurrency, reader pin APIs, and M4 read-view implementation freeze.
- Exact vector cadence, provider-version token cap, snapshot/index memory limits, and all p99 thresholds are evidence
  outputs after the structures are selected.
- `V2-OPEN-BK-11`, `V2-OPEN-BK-13`, remaining `V2-OPEN-OBJ-17`, `V2-OPEN-OBJ-19`, and
  `V2-OPEN-PUL-OBJ-09` remain evidence-blocked.
- KoP remains documented and deferred outside the 0.2 runtime.

## Original confirmation boundary

At proposal time, no Round 13 recommendation above was normative. The adjusted response below records the explicit
confirmation boundary: Q1 remains open, while only the simplified Q2 and adjusted Q3 move to ADRs/contracts.

## Adjusted response preserved verbatim

````text
Round 13 不按原文全部确认：

- Q1 暂不确认，保留 OPEN。
- Q2 简化后确认。
- Q3 调整后确认。

Q1 — 0.2 暂不引入 FullyManifestCoveredThrough。

只要该向量能够授权跳过 directory GET，它就是 correctness-critical recovery omission authority，不能仅称为 accelerator。原方案还有三个问题：

1. lane vector 本身没有绑定它所依赖的跨 Binding manifest generation、source protection 和 handoff 状态；这些状态变化后，单调 vector 可能变成陈旧证明。
2. 一个早期 shared extent 中的慢 Binding 会长期卡住整条 lane watermark，实际 GET 命中率可能很低；解决 hole 又会走向 bitmap/per-binding rows，重新引入更重的 metadata。
3. 它只优化 takeover/recovery，却增加持续 reconciler、跨 Binding 扫描、CAS、响应丢失和恢复证明状态机，与 Round 12 的“先用 bounded recovery evidence 决定是否增加摘要”原则冲突。

0.2 继续使用：

- bounded parallel prefix GET；
- cumulative GET/bytes/time recovery envelope；
- 已有或后续必需的粗粒度 WalRun retirement frontier。

只有 M3/M7 证明恢复 SLO 不达标，并且该 watermark 具有足够命中率时再重开。未来若需要，优先复用 whole-WalRun retirement certificate；确实需要部分跳过时，再定义显式、Root/Seal-bound、不可回退的 ActiveTailRetiredThrough 或 RecoverySkipCertificate。

该证书只能在 Q3 的 read-view handoff、旧 locator/pin drain 和 active-tail responsibility 永久转移后推进。缺失或不匹配必须回退 GET；它仍不能授权 ACK、checkpoint、Seal 或 GC。

Q2 — 简化后确认 closed provider-proof 语义。

0.2 建议默认使用 NONE；只有具体 Provider 在 M3 前同时证明以下条件时，才启用 VERSION_BOUND_FULL_OBJECT_SHA256_V1：

- version token 有 canonical binary encoding 和 format hard cap；
- token 精确绑定 immutable object version；
- Provider 已证明 FULL_OBJECT SHA-256 scope；
- version-pinned range GET 能带来可量化收益。

proof mode、adapter/canonicalizer version 和 token hard cap 固化在 WalRun Root，而不是 Topic。row 最多保存：

proofTag
tokenLength
boundedCanonicalVersionTokenBytes

bodyLength、SHA-256、Provider adapter/scope 已分别由 row 和 Root 提供，不重复保存。禁止 String normalization、ETag、header map、SDK object 或扩展 blob。token 超限或能力不完整时使用 NONE。

NONE 不应导致 routine recovery 做 whole-object GET：进入 checkpoint 前，extent 已经 provider-resolved；正常恢复仍只做 prefix GET。Version token 只是读取 pin/验证加速。

Q3 — 确认 BindingReadViewSnapshot 的逻辑合同，但拆开高频 frontier publication 和低频 source-generation handoff。

1. 普通 append/ACK：
   - 安装 hidden locator；
   - release-publish Readable/Durable Frontier；
   - 完成 ACK。
   不得为每个 ACK 创建 immutable snapshot 对象。

2. Manifest handoff：
   - 低频发布新的 source-selection generation；
   - reader 通过 RCU/epoch/hazard 或 event-loop reader slot pin 住 generation；
   - 正常读取不得访问远端 metadata，也不得默认分配 pin/snapshot heap object或竞争一个全局 AtomicLong。

3. pin 粒度是一次 binding-scoped protocol read batch，例如一个 partition read/fetch range 或一次 ManagedLedger readEntries；不能按 record/message/frame pin，也不能跨整个连接或无界 streaming session长期持有。

4. snapshot 逻辑范围至少绑定：
   - Binding/incarnation；
   - StorageEpoch/PositionDomain version；
   - owner fence；
   - captured Readable Frontier；
   - active-tail view version；
   - manifest view identity/generation；
   - source-protection generation。

   这些可以由 BindingReadState 和不可变引用共同提供，不要求每份 snapshot 重复复制所有字段。

5. “不混合 source”应限定在同一个原子 append unit或已声明的 whole-range fallback。一个 Kafka Fetch 在同一 snapshot 下可以读取互不重叠的 manifest range 和 active-tail range，不能为了强制整个请求单 source 而增加拆分、GET 和延迟。

6. locator 和 source protection 必须分两阶段回收：
   - 先发布“manifest preferred + protected source fallback”的 successor view；
   - 旧 view pins drain 后，可以退休只属于旧 view 的索引结构；
   - 只要 successor 仍声明 source fallback，就不能删除其 protection；
   - 必须再发布一个不再引用 fallback 的 view，并等待对应 pins drain，之后才能释放 protection/进入 GC。

7. retired view/pin backlog 必须有 count、bytes、age、deadline hard bounds。泄漏或超时 pin 可以阻止 handoff/retirement或新的 read admission，但绝不能通过提前删除 locator/protection来释放容量。

8. Takeover 先完成 Root/checkpoint/LIST 的物理恢复，再按 Binding 独立发布 read view；B 不等待 A 的 typed gap。普通读取始终零远端 metadata I/O。

性能结论：

- Q1 没有 ACK 热路径成本，但属于本轮最明显的过度设计，且慢 Binding 可能让收益很低。
- Q2 默认 NONE 时成本最低；主要风险是 version token 膨胀导致每个 64 KiB checkpoint page 可容纳的 row 数下降。
- Q3 是必要正确性机制；若实现成 per-read allocation 或全局 refcount，会直接损伤高并发读取 p99。采用低频 generation handoff + allocation-free RCU pin 后，正常读路径成本可以控制在本地指针读取和 reader-slot 更新。

M3/M4 应测量 recovery GET 节省率、rows/page、read allocations/op、pin 原子竞争、retired-view bytes/age 和 pin-drain p99，再决定是否重开 Q1。
````

## Authoritative synchronization

- Q1 remains `V2-OPEN-OBJ-22`; 0.2 keeps conservative bounded prefix recovery and does not admit
  `FullyManifestCoveredThrough`. Only M3/M7 recovery evidence may reopen a Root/Seal-bound certificate.
- Q2's `NONE` default, conditional version-bound proof, Root activation boundary, compact binary row, and no-routine-
  full-GET rule are frozen by
  [ADR 0068](../../decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md).
- Q3's logical snapshot, append/handoff split, protocol-read-batch pin, mixed-source boundary, two-stage reclamation,
  backlog safety, and takeover behavior are frozen by
  [ADR 0069](../../decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md).
- Numeric proof/pin/view bounds, provider evidence, exact allocation-free capture, and durable fallback-removal cuts
  remain open or evidence-blocked.
