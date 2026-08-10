---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 18: anchor terminal and batch metadata retirement

Date: 2026-08-10

Round 17 accepted one fused selector cut that removes fallback, closes E, grants E+1, and persists a closure anchor. It
also accepted per-source `first_i`, explicit N release CAS/bounded O(N) reconciliation, selector-only immutable batch
activation, and mandatory derived batch retirement. It did not freeze how several unresolved anchors survive repeated
takeovers without an unbounded selector, who may publish the one terminal cut, or how compact batch tombstones
eventually disappear without becoming source-GC authority. Nothing below is normative until explicit confirmation.

## Source facts and constraints

- A fallback-relevant selector transition cannot rely on backend value history. Its anchor must remain durably
  verifiable until an asynchronous terminal cut exists.
- Repeated takeovers can close several fallback-capable epochs before planned drain or qualified expiry is available.
  Keeping only the immediate predecessor loses older anchors; retaining an unbounded chain violates selector hard caps.
- The old owner may lose authority before its reads drain. A stale owner cannot publish new terminal truth merely
  because it once owned E; a current fenced reconciler needs durable planned-drain evidence or qualified expiry.
- A compact batch tombstone prevents full-row retention and stale same-key ambiguity, but one permanent tombstone per
  handoff is still unbounded for a long-lived Binding.
- Any batch-retirement frontier may authorize only metadata compression/deletion. Exact source-protection release and
  physical GC already have separate authorities and must remain separate.
- Q1/Q2 do not freeze numeric limits, backend-specific bytes, proof-window/fold layout, or capability encoding.
  `V2-OPEN-READ-08/09` remain evidence gates.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-READ-14` |
| Q2 | `V2-OPEN-READ-15` |

❓ **Q1** - **Bounded closure anchors and terminal publisher**: How do unresolved anchors survive repeated
takeover/fallback transitions, how does `STOPPED` remain available at the hard cap, and which publisher may create the
one terminal cut for an old epoch?

➡️ Recommend one bounded logical pending-anchor set owned by the selector authority:

1. Every successor selector carries forward all unresolved fallback-relevant anchors in ascending Read Admission Epoch
   order and appends the newly closed E. A membership-neutral view update copies the set unchanged. An epoch that never
   intersected fallback needs only immediate response-loss convergence and never enters the long-lived set.
2. Each anchor is immutable and binds closed E/Owner Epoch, predecessor and successor selector SHAs, transition digest,
   exact capability-evidence digest, and closure kind. The physical backend may inline the bounded set or atomically
   create/reference immutable anchor rows in the selector transaction; pre-read plus selector CAS is forbidden.
3. Admission reserves one emergency STOPPED anchor slot/byte envelope. When the normal pending-anchor bound is full,
   the owner may close the current E and enter STOPPED using that reserve, but no successor `ADMITTING` grant is allowed
   until enough anchors terminalize and capacity is reclaimed.
4. A deterministic Binding/incarnation/E terminal key is create-only. Candidate bytes bind the exact anchor SHA,
   drained read-view cut, capability evidence, and planned-drain receipt or qualified-expiry evidence. The closed
   verifier runs before create; first valid terminal wins, unknown response exact-rereads, a different valid existing
   terminal becomes the common terminal SHA, and an invalid occupant quarantines the epoch.
5. Only the current selector-fenced owner or its Cell reconciler may publish. A previous owner may supply an immutable
   planned-drain receipt created while authorized, but after losing authority it cannot create terminal state. Without
   that receipt, the current publisher must use fully qualified ADR-0074 expiry evidence.
6. A validated terminal cut makes its anchor removable. Removal is an exact selector CAS/atomic transaction that may
   prune several anchors and may piggyback another selector transition; it is not on the read path. A concurrent
   takeover must either carry the old set or observe the exact pruned successor—never silently drop entries.
7. Selector bytes, pending-anchor count/age, terminal rows, invalid occupants, STOPPED duration, prune conflicts, and
   response-loss residues have hard caps and M4 metrics. Pressure cannot force-clear an anchor or infer expiry.

This keeps takeover at one selector CAS and terminal work asynchronous. The cost is bounded selector/anchor bytes,
copy/validation work proportional to the pending set, terminal creates, and occasional prune CAS conflicts; ordinary
reads still use one cached fence and perform no metadata I/O.

❓ **Q2** - **Full-batch tombstone and final metadata retirement**: After every member protection is released, what
compact fact replaces the full batch, and how can even those tombstones be reclaimed safely for a long-lived Binding?

➡️ Recommend a two-stage metadata-only retirement protocol:

1. After a bounded authoritative O(N) scan proves every member `RELEASED/retired` and no selector, lineage, recovery,
   or response-loss reference remains, an exact conditional replacement changes the full batch into a same-key compact
   `RETIRED_V1` tombstone. Unknown response uses exact reread. A quarantined/unknown member blocks this transition.
2. The tombstone retains only Binding/incarnation, `SourceRetirementBatchId`, full-batch SHA, selector-transition
   digest, `sharedLast`, member-set digest/count, capability digest, and tombstone format/SHA. It carries no member rows,
   proof bitmap, released count, or source-GC permission.
3. One Binding-incarnation monotonic `BatchMetadataRetiredThroughEpoch` may advance over a bounded ordered scan of
   selected batch transitions only when every encountered batch at or below the candidate epoch is a valid tombstone
   and every reference/unknown-response veto is absent. Sparse epochs without a batch are permitted because selector
   epochs never reuse and an old epoch can never activate a new batch.
4. The frontier CAS is response-loss recoverable by exact reread and can cover many tombstones. Only after it covers a
   tombstone may the backend delete that tombstone and inert precreate residue. A stale recreate remains unselected and
   is rejected/cleaned because its closed epoch is at or below the frontier.
5. The frontier authorizes only retirement-batch metadata deletion. It cannot change a source-protection row to
   RELEASED, satisfy `[first_i,sharedLast]`, release a pin, authorize Object deletion, or repair a quarantined member.
   Missing/mismatched scan evidence retains tombstones.
6. If a backend cannot provide the required ordered authoritative scan and monotonic CAS, it keeps compact tombstones
   and admits them against a hard lifetime budget; it may not delete them using local age or cache absence.
7. M4/M5 measure full/tombstone bytes, O(N) scan time, replacement and frontier CAS frequency, rows retired per CAS,
   stale-residue rate, quarantine blockage, recovery time, and long-lived Binding capacity.

This adds one low-frequency conditional replacement per completed batch and an amortized frontier CAS per group of
tombstones. It bounds full batch bytes and, on qualified backends, tombstone count without adding a mutable batch-
completion record or changing source-GC correctness.

## Deferred descendants

- Q1 must settle before selector/anchor/terminal wire IDs, exact publisher authorization, pending-set pruning, and
  repeated-takeover/STOPPED recovery vectors freeze.
- Q2 must settle before full/tombstone/frontier wire IDs, backend scan conformance, stale residue cleanup, and long-
  lived Binding capacity gates freeze.
- Exact proof-window/head/fold representation and numeric caps remain evidence gate `V2-OPEN-READ-08`.
- Exact capability/receipt binary encoding and admitted backend generations remain evidence gate
  `V2-OPEN-READ-09`.
- `V2-OPEN-OBJ-22/24`, `V2-OPEN-BK-11/13`, remaining `V2-OPEN-OBJ-17/19`, and
  `V2-OPEN-PUL-OBJ-09` remain evidence-blocked.
- KoP remains documented and deferred outside the 0.2 runtime.

## Explicit adjusted confirmation

The following response is preserved verbatim. It confirms only the adjusted boundaries stated here; the original
recommendations above remain a non-normative record where they differ.

````text
Round 18 不按原文全部确认：

- Q1：调整后确认。
- Q2：部分确认；确认 same-key compact tombstone，暂不确认 tombstone 删除和 retired-through frontier。

Q1：确认 selector authority 拥有有界 unresolved closure-anchor 集合、保留 emergency STOPPED 容量、terminal deterministic create-only/first-valid-wins，以及异步批量 prune。

但补充以下限制：

1. “selector-owned”只冻结逻辑权威，不冻结为每次 CAS 都反序列化、逐项校验或远端读取全部 anchor。

0.2 优先使用很小的 bounded inline canonical set；membership-neutral transition 只复制已经验证的 canonical bytes。暂不引入 anchor page/index/多级 chain。只有 benchmark 证明 inline cap 无法满足 takeover SLO 时再重开物理布局。

2. Admission 必须预先证明：

current pending bytes
+ 本次新 closure anchor
+ 完整 emergency STOPPED envelope
<= backend hard value/transaction cap

Emergency reserve 禁止被普通 ADMITTING transition、prune residue 或动态配置占用。正常容量耗尽时只能关闭 E 并进入 STOPPED，不能丢 anchor，也不能继续让 E 接纳读取。

3. terminal publisher 的“当前 owner 身份”不能成为非事务 backend 的正确性前提。

在 selector 检查与 terminal create 之间，publisher 可能已经失去 owner fence。正确性必须来自 terminal candidate 自身绑定的：

{closureAnchorSha, closedEpoch, OwnerEpoch,
 exact drained/last-admitted view cut,
 capability evidence,
 planned-drain receipt 或 qualified-expiry evidence}

事务 backend 可以原子校验 selector fence；非事务 backend 中，owner/reconciler fence 只负责 ACL、限流和审计，closed verifier 才是安全依据。Cell reconciler 也必须拥有明确的单调 reconciler epoch，不能只依赖角色名称。

4. planned-drain 和 qualified-expiry candidate 使用同一个 closed verifier。Unknown response reread到不同 bytes 时，只要该 existing terminal 对同一 anchor 是完整合法 variant，就作为共同 terminal SHA；不能只接受字节相等。

5. prune 必须异步、可批量、可与其他 selector transition piggyback。CAS 冲突只能重新排队，不能进入 read/append ACK cut，也不要求每个 terminal 单独执行一次 prune CAS。

6. terminal rows 不能永久无限增长。它们后续只能在已经进入 durable proof/fold、且没有 active interval/recovery 引用后按 V2-OPEN-READ-08 的证据协议退休；本轮不再新增一套 terminal progress 状态机。

性能结论：普通读取仍然零 metadata I/O。成本集中在 takeover selector CAS 的 O(K) payload/copy、每个相关 epoch 一次 terminal create，以及低频 prune CAS。K 必须很小并由 takeover p99、selector bytes、STOPPED duration 和 prune-conflict benchmark 反推，不能由 Topic 放大。

Q2：确认以下部分：

FULL_V1
  -- exact-version CAS -->
RETIRED_V1

full batch 在所有 member protection 均已 RELEASED/retired，且 selector、lineage、recovery、response-loss 引用全部消失后，可以替换为同 key compact tombstone。

需要明确它是“immutable logical BatchId + irreversible storage lifecycle”，而不是宣称同一 key 的物理 bytes 永远不变。状态只允许 FULL_V1 → RETIRED_V1，禁止恢复为 FULL。

响应丢失必须这样收敛：

- delayed create/reread 看到 BatchId/fullBatchSha 匹配的 RETIRED_V1，解释为“该 full batch 曾成功提交且已经退休”；
- 不得重建 full batch，也不得当成普通冲突；
- BatchId 或 fullBatchSha 不匹配时 fail closed；
- tombstone 只证明 batch metadata 已退休，绝不证明 source protection 已释放或物理对象可以删除。

但 0.2 暂不确认 BatchMetadataRetiredThroughEpoch 及 tombstone 删除。

原因是 tombstone 一旦允许被删除，retired-through frontier 就不再是普通 accelerator，而会成为 metadata absence、late recreate 和恢复解释的持久化权威。所有 batch create、selector replay、恢复和 residue cleanup 都必须查询或绑定它，还必须证明：

- 每个 epoch 最多一次 batch activation；
- ordered activation scan 无 gap、phantom 或被压缩掉的历史；
- frontier 与 Binding incarnation/selector lineage 精确绑定；
- 一个已关闭 epoch 永远不能重新激活 batch；
- stale create 在 tombstone 缺失后仍只能成为 inert residue。

这会引入新的 scan/index、frontier CAS、恢复分支和 prefix HOL；一个早期 quarantined batch 还可能阻止全部后续 tombstone 回收。当前没有 tombstone 容量证据证明值得在 0.2 支付这些复杂度。

因此 0.2 先保留 compact tombstone，并以 Binding/Cell lifetime count/bytes budget 准入；达到上限时停止新的 fallback/handoff admission，不能按年龄删除。

只有 M4/M5 证明 tombstone 生成率和长期容量确实无法接受，并且具体 backend 提供可信 ordered activation scan、monotonic CAS 与恢复证据后，才重新讨论 frontier。届时应明确命名为 BatchMetadataRetirementAuthority，并严格限制为 metadata 删除权限，永远不能参与 protection release 或 source GC。

最终结论：

- Q1：按上述约束调整后确认。
- Q2：确认 FULL_V1 → RETIRED_V1；tombstone 删除和 retired-through frontier 保持 OPEN、等待容量及 backend capability 证据。
````

## Authoritative synchronization

- Q1 resolves `V2-OPEN-READ-14` in
  [ADR 0079](../../decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md): the selector owns one
  small bounded inline canonical set, admission preserves a dedicated emergency STOPPED envelope, terminal correctness
  comes from the immutable candidate plus one closed verifier, and prune remains asynchronous and batched.
- Q2's accepted 0.2 cut is frozen by
  [ADR 0080](../../decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md): an exact-version same-key
  `FULL_V1 -> RETIRED_V1` replacement is irreversible and lost-response recoverable, grants no source-GC authority,
  and remains permanently retained under lifetime budgets.
- `V2-OPEN-READ-15` remains evidence-blocked only for tombstone deletion. No retired-through/frontier authority is an
  accepted 0.2 contract. Exact proof/fold and capability encodings remain evidence gates `V2-OPEN-READ-08/09`.
