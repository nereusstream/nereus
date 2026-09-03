---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 10: prefix hints, packing lanes, and range takeover

Date: 2026-08-09

Round 9 partially accepted ADRs 0056..0058. Exact NPD1/Object/part values and block-class values now wait for
implementation/provider evidence. The current decision frontier is therefore the three independent designs that do not
depend on those measurements: a routine-read prefix hint, multi-policy scheduling under one WalRun, and failover-safe
range reuse. No recommendation below is normative until explicit confirmation.

## Source facts and corrected constraints

- ADR 0021/0025 now separate whole-Object PUT/durability proof from routine Root-bound directory/frame AEAD. A missing
  qualified `ProviderObjectProof` cannot by itself force every random read onto three GETs or a full GET.
- ADR 0030's current leaf key already carries sequence, exact body length, and complete SHA-256, while normal ACK has no
  per-group metadata commit. A hint available only in an asynchronous checkpoint/manifest cannot cover every open tail.
- ADR 0039/0047 fixes one `CurrentWalRunPointer` and one current Root/lineage per shard. A Topic packing class therefore
  cannot be a singular Root identity or cause one pointer per class.
- ADR 0053 currently defines one contiguous extent-sequence interval per checkpoint page and one terminal inventory in
  the Seal. Concurrent scheduling lanes cannot be added without deciding sequence, page, and Seal semantics together.
- The rejected RANGE proposal reacquired a range after every owner change. With one serialized `RANGE_RESERVED` and
  three 10-ms allocator steps, 10,000 ManagedLedgers have a 300-second idealized lower bound before queueing or errors.
- A range is already numerically never reused. Preserving its grant across owner epochs does not weaken that rule; the
  unresolved issue is which head/node facts prevent a stale owner from publishing a candidate.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-OBJ-17` |
| Q2 | `V2-OPEN-OBJ-19` |
| Q3 | `V2-OPEN-PUL-OBJ-09` |

❓ **Q1** - **Self-describing directory-prefix hint**: Where should `directoryEnd` live so routine reads can normally
plan one prefix GET without a provider proof or synchronous per-group metadata row?

➡️ Recommend adding a fixed-width `directoryPrefixEnd19` field to the immutable content-addressed leaf identity. The
exact grammar is finalized together with any lane component, but the hint is derived from the sealed NWG1 body and
co-located with the key's exact body length/SHA-256. The complete leaf is interpreted only under the exact prefix from
the known WalRun Root, and every cached copy binds that Root key/SHA plus the full Object key; a suffix detached from
that Root is not a usable hint. The hint is not cryptographic body proof; only the in-body header / directory AEAD
validates its meaning. A checkpoint, manifest, or in-memory descriptor may repeat it but is never its sole source. The
reader rejects a hint above the wire prefix cap before allocation, optionally pins an available provider version, and
requests `[0,hintEnd)` without requiring `ProviderObjectProof`.

After parsing the returned in-body header: if authoritative `headerEnd <= hintEnd`, reuse the exact subrange and ignore
safe extra bytes; if `headerEnd > hintEnd` but remains within the hard cap, retain the header and fetch only the missing
`[hintEnd,headerEnd)` bytes. Header/Root/key/version mismatch or directory AEAD failure fails or enters the bounded
fallback; the hint never authorizes a frame offset. Duplicate sequence/body identities with conflicting hints remain a
run conflict. The tradeoff is about twenty more key characters and a revised key grammar, in return for a no-metadata
two-GET path for every known extent rather than only asynchronously checkpointed objects.

❓ **Q2** - **Three bounded lanes under one WalRun lineage**: How can latency/cost classes build concurrently without
creating multiple current pointers or reintroducing a global gap/HOL through one extent sequence?

➡️ Recommend one Root/pointer with a format-level hard maximum of three scheduling lanes, but no Root-level packing
class. Each admitted packing class maps to one stable small `laneId`; a group contains only bindings resolved to that
class and records both class and actual close facts in its descriptor/header. Each lane owns one bounded builder,
lane-local `extentSequence`, in-flight/memory limits, and a lane-local contiguous checkpoint-page chain. Leaf identity,
header HKDF/nonce inputs, and descriptors bind `(laneId,laneSequence)` so uniqueness does not depend on cross-lane
completion order.

The Seal binds the exact terminal sequence and final checkpoint-head SHA for every instantiated lane, while the single
successor Root and `CurrentWalRunPointer` remain unchanged. Cell scheduling enforces bounded fairness and aggregate
resource ceilings. A binding may move to a newly resolved packing class only at a group boundary after its previous
lane has no unresolved append for that binding; it need not seal the WalRun. The tradeoff is lane-aware key/page/Seal
wire and up to three builders, in return for no extra lineage, no cost-lane linger blocking a latency lane, and no
global sequence ACK barrier between classes.

❓ **Q3** - **Incarnation-owned range and owner-only takeover**: Which head/node fields let a new broker preserve the
installed range while burning at most one stale candidate and keeping allocator clear off the use path?

➡️ Recommend one exact ManagedLedger head record with separate facts:

```text
ManagedLedgerHead {
  managedLedgerIncarnation
  visibleChainHeadNodeIdAndDigest
  ownerEpoch
  grantId
  rangeStart
  rangeEndExclusive
  nextLedgerId
  allocatorProtocolVersion
}
```

The allocator grant binds the ManagedLedger incarnation/grant/range and never an owner epoch. Takeover CAS changes only
`ownerEpoch` while preserving chain head, grant, range, and cursor. Candidate node identity binds ledger ID, grant ID,
creator owner epoch, and expected visible predecessor digest. Node creation is single-flight per ManagedLedger, and
only an exact head CAS can publish it.

The new owner point-reads/`putIfAbsent`s exact `nextLedgerId`: an already published node is preserved through the head
reread; an exact candidate from the current owner converges by value equality after response loss; an unpublished node
from an old epoch loses publication authority and causes one cursor-only head CAS that burns exactly that ID without
changing the visible chain; absence permits the new candidate. A late old-owner `putIfAbsent` conflict converges
through the same one-ID burn. Any failed head CAS fences that creator.

Once head installation durably contains the grant, range use may begin; allocator `RANGE_RESERVED -> IDLE` clear is a
recoverable background step, though the next grant cannot reserve until exact allocator/head reconciliation clears it.
Whole-tail burn is limited to Topic deletion, ManagedLedger retirement, protocol incompatibility, or unrecoverable head
corruption. Orphan nodes are permanent no-reuse evidence and count against byte/count admission. Capacity uses a
declared finite churn rate and planning horizon plus recovery reserve, continuously reporting remaining, committed,
burned, orphan, and reserved IDs. The tradeoff is one parallel head takeover CAS and at most one point lookup/burn per
uncertain ledger, in return for avoiding serialized range reacquisition across every ledger on a failed broker. This
still does not select RANGE_LEASED over STRICT_SERIALIZED.

## Deferred descendants

- `V2-OPEN-BK-11` NPD1 hard numeric values and `V2-OPEN-BK-13` final block classes wait for ADR 0056/0057
  implementation/provider receipts.
- Q1/Q2 must settle before the revised leaf/header/checkpoint/Seal wire and golden vectors can freeze.
- Q3 must settle before range size, grant/head/node wire IDs, allocator recovery implementation, and mode selection.
- Ledger-chain trimming, cursor/replication/transaction recovery, and RETIRING proof remain later descendants.
- KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 10 recommendation above is normative. Confirmed conclusions must move to ADRs/contracts; adjustments and both
allocator modes remain in the open log.

## Adjusted response preserved verbatim

The user first summarized the review as:

> 结论：Round 10 已经明显收敛。Q1、Q3 的复杂度基本值得；Q2 仍有局部过度设计——“三个调度 lane”合理，但“三条独立 checkpoint chain”会把调度隔离升级成三套恢复状态，没必要提前承担。

The user then supplied this exact disposition:

> Round 10 不全部按原推荐确认：
>
> - Q1：调整措辞后确认。
> - Q2：确认最多三个 lane、lane-local sequence；不确认三条独立 checkpoint chain。
> - Q3：补齐 reservation takeover 后确认正确性结构，但仍不选择 RANGE_LEASED mode。
>
> Q1 — 调整后确认 directoryPrefixEnd19 进入 leaf key。
>
> 这是少量冗余换取稳定热路径，不属于明显过度设计：
>
> - 已知 extent 可以直接 prefix GET → frame GET。
> - open tail 不依赖异步 checkpoint/manifest。
> - 不需要 ProviderObjectProof 或额外 HEAD。
> - 约 20 个 key 字符相对于 Object body 和减少一次 GET 的收益很小。
>
> 需要明确：
>
> 1. 字段语义是 exclusive directoryPrefixEnd，必须满足：
>    fixedHeaderBytes <= directoryPrefixEnd <= bodyLength
>    且不得超过 maxHeaderAndDirectoryPrefixBytes。
> 2. 它只是 canonical planning hint，不是 Object content digest 的组成部分，也不能授权 frame offset。
> 3. exact content identity 仍是 bodyLength + SHA-256；完整 Object key 是物理 immutable identity。
> 4. checkpoint/manifest 不应逐字重复完整 Object key。应保存结构化的 laneId、sequence、directoryPrefixEnd、bodyLength、SHA，并通过 Root prefix 重建 key。否则每页 256 个 descriptor 会仅因该字段多约 5 KiB。
> 5. Q2 若接受 lane-local sequence，最终 key grammar 应一次冻结，例如：
>    <laneId>/<laneSequence19>/<directoryPrefixEnd19>-<bodyLength19>-sha256-v1-<64hex>.nwg
> 6. directoryPrefixEnd 暴露近似 directory 大小；0.2 可以接受这点，但应记录为 object-key metadata leakage tradeoff。
>
> 短 hint、长 hint 的增量复用规则按推荐确认。
>
> Q2 — 部分确认。三个 scheduling lane 合理，但三条独立 checkpoint page chain 过度设计。
>
> 确认：
>
> - 一个 Root、一个 CurrentWalRunPointer。
> - format hard max 为三个 lane，实际 lane 按需创建。
> - 每个 admitted packing class 映射稳定 laneId，且一个 laneId 在一个 WalRun 内禁止重绑定到其他 class。
> - 每个 lane 有独立 builder、laneSequence、PUT/ACK 顺序和 in-flight limit。
> - leaf key、HKDF、nonce、header 和 descriptor 都绑定 laneId + laneSequence。
> - 每个 lane 内必须保持 sequence ACK barrier；不得 ACK n+1 后再让 n 变成无法恢复的 provider-absent gap。
> - binding 只有在旧 lane 已无未 ACK、未知结果或未收敛 append 后才能切换 lane。
> - 一个 lane 的 absent gap 可以迫使整个 WalRun seal/recover，但不能丢失其他 lane 已 ACK 的 extent。
>
> 不确认每个 lane 一条独立 checkpoint chain。checkpoint 不在 ACK cut 上，没必要为了消除 append HOL 引入三个 predecessor chain、三个 checkpoint head 和三套 Seal chain。
>
> 建议改成一个 run-wide vector checkpoint chain：
>
> WalRunCheckpointPage {
>   rootSha
>   pageOrdinal
>   predecessorPageSha
>   extents[] ordered by (laneId, laneSequence)
>   coveredThrough[up to 3 lanes]
> }
>
> 每页可以推进一个或多个 lane，但对每个 lane 都必须从上一页 vector 连续推进。这样只有：
>
> - 一条 page predecessor chain；
> - 一个 checkpoint-head CAS；
> - Seal 中一个 terminalSequence vector；
> - 一个 finalCheckpointHeadSha。
>
> 它仍不会让 cost lane 的空洞阻止 checkpoint page 推进 latency lane。
>
> 性能上还必须增加以下限制：
>
> - 三个 builder 必须 lazy instantiate，禁止按 target 预分配。
> - 如果候选 target 是 4/16/64 MiB，不能直接把每 shard 84 MiB 乘以 plaintext/compressed/ciphertext/in-flight copies。
> - Root 的 maxExtentCount、maxCanonicalBodyBytes、recovery bytes 和 checkpoint uncovered bounds 都是三个 lane 的 aggregate hard cap，不能每 lane 各获得一份完整预算。
> - 同时保留 per-lane maxUncheckpointedAge，避免低流量 lane 永久不进入 checkpoint。
> - host pressure只能 early seal/backpressure，不能改变 laneId 或已持久化 class。
>
> 只有 benchmark 证明单一 vector checkpoint chain 的异步发布能力不足时，才重新考虑 lane-local chains。
>
> Q3 — 补充两个竞态后确认 RANGE takeover 正确性结构。
>
> 确认以下核心结论：
>
> - range 永久属于 ManagedLedger incarnation/grant，不属于 broker owner。
> - takeover 只更新 head.ownerEpoch，保留 visible chain、grant、range 和 nextLedgerId。
> - candidate 绑定 ledgerId、grantId、creatorOwnerEpoch、expected predecessor ID/digest。
> - stale owner 最多留下一个 candidate；新 owner 不采纳，cursor-only CAS 只 burn 该 ID。
> - 已安装 range 在 owner 变化后继续使用。
> - head install 完成后即可使用 range，allocator clear 不进入该 ManagedLedger 的使用关键路径。
> - 整个 tail 只在删除、retirement、协议不兼容或不可恢复损坏时 burn。
> - RANGE_LEASED 与 STRICT_SERIALIZED 仍均未被选择。
>
> 还需要补齐：
>
> 1. allocator reserve 与 head install 之间发生 takeover 时，新 owner 必须能够完成同一个 RESERVED grant。
>
>    因此 reservation 的 expected allocation state 应绑定：
>    managedLedgerIncarnation + visibleChainHead + priorGrant/cursor
>
>    不能把旧 ownerEpoch 作为 grant lifetime identity。新 owner 更新 head ownerEpoch 后，只要上述 allocation state 未变化，就可以安装原 grant并完成 allocator clear。
>
> 2. CAS 失败必须区分：
>
>    - response unknown：精确 reread，candidate/head 同值则收敛成功；
>    - definitive version/value conflict：才 fence creator。
>
> 3. allocator clear 虽然不阻塞已安装 range 的使用，但它仍阻塞下一次全 Cell grant。必须有高优先级、任意当前 owner 可执行的 reconciler，并纳入 pending age/queue evidence。
>
> 4. 永久 orphan 只发生在 ownership loss 与 rollover candidate 窗口重叠时。0.2 保留它比立即引入 orphan GC 更简单，可以接受；但必须计入 metadata bytes/count、可观测性和容量 admission。
>
> 本轮最终结论：
>
> - Q1：确认 leaf-key prefix hint，最终 grammar 与 lane key 一次冻结。
> - Q2：确认最多三个 lazy lane 和 lane-local sequence；checkpoint 改为单一 vector page chain，不接受三条独立 chain。
> - Q3：确认 incarnation-owned range、owner-only fencing和单 ID burn；补齐 RESERVED grant 跨 takeover 收敛。
> - 不因本轮确认自动选择任何 allocator mode。

The user closed with this performance priority and source pointer:

> 性能上，Q1 是净收益；Q3 用一次可并行 head fencing 换掉全 Cell range 重申请，收益很大。真正要控制的是 Q2：如果实现成三个 eager builder 加三条 checkpoint chain，内存、metadata 写和 Seal 恢复状态都会接近三倍。[ADR 0053](../../decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md)中的 checkpoint 本来就是异步加速器，因此优先使用单一 vector chain 更符合它的定位。

## Authoritative synchronization

- Q1's exclusive leaf hint, proof separation, structured descriptor, incremental range reuse, and accepted leakage are
  frozen by [ADR 0059](../../decisions/0059-v2-object-wal-leaf-prefix-hint.md);
- Q2's three lazy lanes, lane-local sequence/ACK barriers, aggregate budgets, and one run-wide vector chain are frozen
  by [ADR 0060](../../decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md); three lane-local chains are rejected;
- Q3's incarnation-owned grant, RESERVED takeover, exact response-loss classification, background clear, and permanent
  orphan accounting constrain any RANGE candidate through
  [ADR 0061](../../decisions/0061-v2-pulsar-range-grant-owner-takeover.md);
- exact NWG1 numeric/wire values, packing-class values and canonical lane encoding, range-size/wire evidence, and both
  allocator modes remain open.
