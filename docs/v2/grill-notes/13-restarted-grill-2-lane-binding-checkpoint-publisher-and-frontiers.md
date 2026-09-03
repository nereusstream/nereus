---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 11: lane binding, checkpoint publication, and per-binding frontiers

Date: 2026-08-09

Round 10 accepted the exclusive leaf prefix hint, up to three lazy lane-local sequences with one vector checkpoint
chain, and owner-takeover constraints for any RANGE allocator candidate. Exact numeric values and allocator mode remain
evidence-blocked. The current independent frontier is therefore the remaining identity/publication authority beneath
the accepted lane structure plus the previously open per-binding ACK-isolation contract. No recommendation below is
normative until explicit confirmation.

## Source facts and corrected constraints

- ADR 0059 cannot freeze the complete key until `laneId` has one canonical text encoding. A first-use mutable lane map
  would need recovery authority or could rebind after response loss; putting three soft classes in the Root would undo
  the accepted Root boundary.
- ADR 0060 allows at most three packing classes and requires each lane ID to remain stable within a run. A product-level
  non-reused class ID can therefore remove per-run lane-map metadata entirely; exact target/linger values still remain
  group audit facts rather than decoder authority.
- ADR 0053/0060 fixes one asynchronous checkpoint predecessor chain and one head CAS. Append ACK does not wait for it,
  so lane builders do not need to race independent page publishers. However response loss, takeover, and unreferenced
  immutable pages still need one exact combiner/head protocol.
- Existing Object WAL prose says shard sequence is not a cross-binding ACK barrier but has not frozen the bounded data
  structure that makes that statement true. One valid multi-binding Object PUT may release healthy binding B even when
  binding A has an earlier unresolved predecessor; corruption of the shared Object itself still affects all members.
- ADR 0061 closes the RANGE takeover shape but does not make exact RANGE wire/range-size/mode decisions ripe without
  evidence. NPD1 numeric/class choices are similarly evidence-blocked and are not part of this round.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-OBJ-17`, `V2-OPEN-OBJ-19` |
| Q2 | `V2-OPEN-OBJ-19` |
| Q3 | `V2-OPEN-OBJ-01` |

❓ **Q1** - **Canonical lane ID and final leaf grammar**: Should lane binding be inferred from the first extent, stored
in the Root, or be a fixed product-catalog identity?

➡️ Recommend a V2 product catalog with at most three non-reused `WalRunPackingClassId` values `0`, `1`, and `2`; the
class ID is also the lane ID. The key token is exactly one ASCII digit, so no first-use lane-map record, Root mutation,
or recovery inference exists. A Topic's resolved class carries that stable ID; a group header/descriptor additionally
stores the policy version and actual close size/linger. Soft target values may change at a group boundary under the
same class/version rules, but a numeric ID is never reassigned to another semantic class in 0.2.

Freeze the complete leaf grammar as:

```text
<wal-run-prefix>/<laneId:[0-2]>/<laneSequence19>/
  <directoryPrefixEnd19>-<bodyLength19>-sha256-v1-<64-lowercase-hex>.nwg
```

Each lane starts at sequence zero, increments by exactly one for admitted candidates, never wraps/reuses, and cannot
ACK past an unresolved sequence. A proven-absent unacknowledged candidate ends that run as ADR 0060 requires. The
tradeoff is coupling three physical key tokens to the 0.2 packing-class catalog, in return for no lane-binding metadata,
no first-use race, and a final canonical key grammar.

❓ **Q2** - **Single checkpoint combiner and head conflict protocol**: Who is allowed to publish the run-wide vector
chain, and how do response loss/takeover avoid page forks without putting checkpoint on the ACK cut?

➡️ Recommend exactly one shard-owner-fenced checkpoint combiner per open WalRun. Lane builders enqueue ACKed structured
descriptors into a bounded local queue; they never publish pages themselves. The combiner serializes immutable pages
and advances one head record containing Root SHA, shard-run/owner fence, page ordinal/key/SHA, and `coveredThrough`
vector through exact CAS. It may batch any contiguous subset from one or several lanes.

Response unknown rereads the head and accepts exact page/head equality. A definitive CAS conflict adopts the committed
head/vector, revalidates the still-uncovered descriptors, and creates only a new successor page; it never merges two
page predecessors or rewrites a page. A stale owner may leave an unreferenced immutable page but cannot advance the
fenced head. Such pages are ignored by recovery, counted against bounded metadata residue, and cleaned only after the
run's ordinary seal/retirement authority permits it. Takeover starts from the committed head/vector and bounded LIST
tails. The tradeoff is one asynchronous publisher per shard, which may become a checkpoint-throughput bottleneck; only
benchmark failure against a predeclared SLO reopens multi-publisher design, not lane count alone.

❓ **Q3** - **Per-binding contiguous commit trackers**: What exact ACK structure prevents one binding's predecessor gap
from becoming a shard/lane-wide head-of-line barrier after a valid multi-binding PUT?

➡️ Recommend one bounded `ContiguousCommitTracker` per
`{TopicBindingId, TopicIncarnation, StorageEpochId, PositionDomainVersion}`. After the complete Object and directory are
verified, the group completion dispatches each Kafka append commit set or Pulsar entry independently to its binding
tracker. Each unit carries deterministic idempotency identity, exact typed coverage, and expected predecessor frontier.
The tracker ACKs only the greatest contiguous prefix in that Position Domain and stores later completed units in a
bounded gap map.

A gap in binding A withholds only A's later successes; binding B from the same valid Object can advance and ACK. If the
Object/directory itself is missing, corrupt, or unverifiable, no member is released. Per-binding gap count/bytes/age and
aggregate shard memory are hard-bounded. Before A exceeds its bound, A is removed from new shared groups and
backpressured/fenced or the owning run rolls; B continues unless an aggregate Cell/provider limit is exhausted.
Recovery rebuilds each tracker independently from authenticated append-unit descriptors and never sorts protocol
coverage by lane or Object order. The tradeoff is bounded per-binding maps and more completion bookkeeping, in return
for making the no-cross-binding-HOL claim executable without per-group remote metadata.

## Deferred descendants

- `V2-OPEN-BK-11` exact NPD1 numeric values and `V2-OPEN-BK-13` class values remain evidence-blocked; exact NWG1
  prefix/directory/row numeric values and packing targets remain blocked on ADR 0058 evidence.
- Q1 must settle before key/parser golden vectors and content-addressed LIST parsing freeze.
- Q2 must settle before exact checkpoint-head/page field IDs and takeover tests freeze.
- Q3 must settle before the M3 append completion API and per-binding recovery data structures freeze.
- `V2-OPEN-PUL-OBJ-09` RANGE record bytes, range size, Cell allocator concurrency, and allocator mode remain blocked on
  ADR 0055/0061 evidence; neither mode is selected.
- Ledger-chain trimming, cursor/replication/transaction recovery, and RETIRING proof remain later descendants.
- KoP remains documented and deferred outside the 0.2 runtime.

## Original confirmation boundary

At proposal time, no Round 11 recommendation above was normative. The adjusted response below records the explicit
confirmation; only its confirmed conclusions move to ADRs/contracts, while evidence-blocked values/modes remain open.

## Adjusted response preserved verbatim

````text
结论：Round 11 的方向基本正确，但 Q2/Q3 存在一个隐藏耦合：如果 B 已经 ACK、A 仍等待前驱，那么这个共享 Object 仍必须能够进入 checkpoint；否则 A 会通过 uncovered-tail 上限间接拖住 B。Q3 也不应实现成“每 Topic 一个常驻 TreeMap + payload 缓存”。

可以直接回复：

```text
Round 11 不全部按原文直接确认：

- Q1：补充 class 语义和 sequence 分配时点后确认。
- Q2：确认单 combiner，但输入必须是 provider-resolved extent，而不是“所有成员均已 ACK”的 extent。
- Q3：确认 per-binding contiguous contract，但必须拆开物理 extent resolution 与协议 frontier，并采用轻量、lazy、可重建实现。

Q1 — 调整后确认固定产品级 class/lane ID 和最终 leaf grammar。

固定 ID 比 first-use 推断或 Root lane-map 更简单，没有明显性能代价。但“ID 永不重绑定”必须先定义其永久语义，否则只是冻结三个无含义数字。

建议冻结：

- 0 = OBJECT_LATENCY
- 1 = OBJECT_BALANCED
- 2 = OBJECT_COST

具体 target/linger 继续等待 benchmark；数值变化通过 packingPolicyVersion 表达，不能改变上述语义。若最终只启用两个 class，未使用 ID 永久保留，不重新分配。

完整 key grammar 按推荐确认：

<wal-run-prefix>/<laneId:[0-2]>/<laneSequence19>/
  <directoryPrefixEnd19>-<bodyLength19>-sha256-v1-<64hex>.nwg

补充约束：

1. laneSequence 只在 group canonical body 已经 seal、通过 admission、即将执行 conditional PUT 时分配。

   builder 创建、linger 超时前取消、资源不足或 early-seal 过程不得提前烧 sequence，否则普通本地调度失败也会制造无法跨越的 lane gap。

2. sequence 一旦分配，就只能：

   - 同一 key/body 重试并收敛；
   - 成功成为 provider-resolved extent；
   - 或证明 absent 后结束旧 WalRun。

3. 同一 group 只能混合相同：
   classId + packingPolicyVersion + resolved quantized policy
   的 bindings。

4. classId 决定 lane；policyVersion 和实际 close bytes/linger 记录在 authenticated header/descriptor，不进入 key。

Q1 可以作为最终 wire 合同接受。

Q2 — 确认单一 checkpoint combiner，但修正 publication eligibility。

一个 open WalRun 只保留一个 shard-owner-fenced combiner、一个 page predecessor chain 和一个 checkpoint-head CAS，这个方向正确。checkpoint 不在 ACK cut 上，因此没有必要为了 lane 并发增加 publisher。

需要冻结：

1. publisher fence 是 shard/WalRun publisher epoch，不是任一 Topic Binding 的 Owner Epoch。

2. takeover 后，新 owner 必须先 CAS checkpoint head：

   - 保留 page ordinal、page key/SHA 和 coveredThrough vector；
   - 只更新 publisher epoch；
   - 然后才能创建下一页。

   这样旧 owner 的后续 head CAS 必然失败。

3. 每个 combiner 只允许一个 page candidate in-flight。

   page key/request identity 必须由 Root、page ordinal、predecessor SHA、page body SHA 确定。响应未知时只能精确 reread同一 candidate。这样每个失败 publisher epoch 最多留下一个不可达 page residue，而不是无界 orphan pages。

4. CAS 冲突只能采用经过完整验证、属于同一 Root 且 vector component-wise 不回退的 committed head。禁止本地合并两个 predecessor。

最重要的调整：

lane builder 不应只提交“所有 protocol members 都已经 ACK”的 descriptor，而应提交：

ProviderResolvedExtentDescriptor

其前提是：

- PUT 已确定成功；
- Object identity/body、header 和 directory 已验证；
- lane sequence 已物理收敛；
- 不再可能成为 provider-absent gap。

checkpoint 是物理 extent inventory，不是“其中每个协议请求都已经 ACK”的证明。它可以记录一个 B 已推进、A 仍等待 typed predecessor 的共享 Object；recovery 再把 append units 分发给各 binding tracker。

如果继续要求整个 Object 的所有 binding 都 ACK 后才能 checkpoint，那么 A 的 gap 会通过 maxUncheckpointedExtents/Bytes/Age 最终 backpressure 整个 shard，破坏 Q3 想提供的隔离。

单 combiner 的 benchmark 至少测量：

- descriptor queue depth/age；
- page publish RPS；
- head CAS conflict/retry；
- forced checkpoint/backpressure 次数；
- takeover 恢复时间；
- seal flush 延迟。

只有它不能满足预声明 SLO 时才重新考虑多 publisher。

Q3 — 接受逻辑合同，但不把 ContiguousCommitTracker 变成新的持久化权威或重型通用 Map。

首先必须区分两种 frontier：

1. LaneExtentResolvedThrough

   只表达 lane sequence 对应的 Object 已成功、验证完成，不再可能成为 absent gap。它解决物理 PUT/恢复顺序。

2. BindingDurableFrontier

   只表达某个 Topic Binding 的 typed Position Domain 连续前缀。它决定协议 ACK/可读性。

一个 extent 可以已经推进 LaneExtentResolvedThrough，但其中 A 的 append unit 仍因前驱缺口等待；同一 extent 中的 B 可以独立推进 BindingDurableFrontier。

因此 lane sequence barrier 只能阻止“前序 Object outcome 仍 unknown/可能 absent”的后续 extent，不能因为 A 的纯协议位置缺口阻止 B。

Tracker identity 建议补全为：

{
  ProtocolCellId,
  TopicBindingId,
  TopicIncarnation,
  StorageEpochId,
  PositionDomainId,
  PositionDomainVersion
}

Owner Epoch 不属于逻辑 frontier identity，但每个 runtime tracker instance 仍然是 owner-local 的：

- 每次 completion 使用本地缓存的 owner fence 做 O(1) 校验；
- 禁止为每次 completion 远程读取 metadata；
- takeover 后旧实例销毁，新 owner 从 durable evidence 重建；
- 旧 owner completion 不能推进新实例。

实现上不要强制通用 TreeMap：

- 正常写路径已经按 binding 串行分配 append ordinal，优先使用有界 ring/window，完成插入和连续释放保持 O(1)。
- 只有 recovery 或真正稀疏 completion 才使用 PositionDomain-aware bounded ordered map。
- tracker lazy 创建；没有 pending unit 时只保留紧凑 frontier，允许回收。
- Object 已 durable 后立即释放 payload、ciphertext、compression buffer；gap entry 只保留 coverage、idempotency identity、descriptor reference 和等待 future。
- 禁止按 target Object 大小计算 gap bytes。

边界至少包括：

- pending unit count；
- descriptor bytes；
- waiting future count；
- oldest gap age；
- shard aggregate tracker bytes。

接近单 binding 上限时：

- 在新的 protocol position 分配前停止 A admission；
- 将 A 移出新的共享 groups；
- backpressure A 并继续收敛已有 gap；
- B 继续运行。

普通容量上限不应直接 fence A，也不应仅因 A 的 typed predecessor gap rollover 整个 WalRun。只有 owner/invariant 失败、provider-unknown lane gap或 aggregate recovery envelope 触发时，才允许 fence/rollover。

错误隔离也要分层：

- ObjectExtentDigest、KMS envelope、header 或 directory AEAD 失败：共享 Object 的所有成员都不能释放。
- 在 Object/header/directory 已验证后，某个 frame/commit-set 的 AEAD、CRC、native checksum 或 typed coverage 失败：只阻塞/失败所属 binding 的完整 commit set；不得自动阻塞其他已独立验证的 binding。
- Kafka 仍以完整 KafkaAppendCommitSet 为 tracker unit；Pulsar 仍以一个 ManagedLedger entry 为 unit。

recovery 按 authenticated append-unit descriptor 分组到各 binding tracker，使用 PositionDomain adjacency 重建；laneSequence、Object key 和物理 LIST 顺序都不能充当协议比较器。

本轮最终结论：

- Q1：确认固定 0/1/2 class/lane ID 和最终 leaf grammar。
- Q2：确认单 publisher、单 vector chain 和精确 CAS；checkpoint 输入改为 provider-resolved extent。
- Q3：确认 per-binding contiguous release，但明确物理 extent resolution 与协议 Durable Frontier 是两层状态。
- 不增加 per-binding remote metadata，不持久化 runtime gap map，不在热路径远程验证 Owner Epoch。
```

Round 11 真正需要避免的性能问题不在 Q1/Q2，而在 Q3 的实现方式：轻量 ring/window 是必要的顺序完成器；为大量 Topic 常驻分配通用有序 Map、保留 payload，才会变成过度设计。[现有 append 合同](../01-correctness-and-append.md)已经要求 binding-scoped contiguous frontier，因此 tracker 应是该合同的可重建实现，而不是第六种权威。
````

## Authoritative synchronization

- Q1's permanent class/lane catalog, complete leaf grammar, compatibility tuple, and sequence-allocation cut are
  frozen by [ADR 0062](../../decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md). Because lane sequence
  is an HKDF/nonce input, the confirmed “seal” cut is clarified as immutable group-plan seal before sequence allocation,
  followed by final ciphertext-body seal;
- Q2's single publisher, publisher-epoch takeover, one candidate, exact CAS, bounded residue, and
  provider-resolved-only checkpoint eligibility are frozen by
  [ADR 0063](../../decisions/0063-v2-provider-resolved-checkpoint-publisher.md);
- Q3's physical/logical frontier split, owner-local reconstructible tracker, lazy ring/sparse-map implementation,
  bounds, buffer release, pressure behavior, and layered failure isolation are frozen by
  [ADR 0064](../../decisions/0064-v2-object-wal-physical-and-binding-frontiers.md);
- exact NWG1 numeric caps, target/linger values, allocator wire/size/mode, and evidence-derived runtime budgets remain
  open or evidence-blocked. No per-binding remote metadata or persisted runtime gap map was introduced.
