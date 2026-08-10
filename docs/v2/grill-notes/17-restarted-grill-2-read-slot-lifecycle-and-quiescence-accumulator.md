---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 15: read-slot lifecycle and quiescence accumulation

Date: 2026-08-10

Round 14 accepted standard generation-tagged hazard capture and capability-tiered durable source retirement. The next
frontier is the async slot reuse state that prevents a late callback clearing a new reader, the bounded proof unit that
covers repeated takeovers without assuming native Owner Epoch ordering, and the closed backend capability record.
Nothing below is normative until explicit confirmation.

## Source facts and constraints

- A read slot remains pinned after request cancellation/deadline while provider I/O, fallback, decode, or a source-
  backed buffer can still touch the source. A wall-clock timeout cannot safely make the slot reusable.
- Reusing one slot creates a second ABA domain independent from source-generation ABA: a late callback from batch A
  must not clear the same slot after batch B acquires it, even if both read the same generation.
- Current V2 contracts define Owner Epoch as exclusive ownership/fencing identity. They do not yet freeze a Binding-
  scoped total order for read-admitting owners or an authority-time lease contract.
- One latest-owner proof cannot cover a takeover gap; one proof row per owner x source would grow without a useful
  bound. Round 14 does allow a bounded source-retirement batch.
- A generic `hasLease=true` flag cannot establish the six accepted expiry conditions. Backend/time-authority identity,
  protocol version, lifetime/skew/grace caps, and conformance evidence must travel together.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-READ-05` |
| Q2 | `V2-OPEN-READ-06` |
| Q3 | `V2-OPEN-READ-07` |

❓ **Q1** - **Async slot reuse, cancellation, and late-callback ABA**: After a caller times out or cancels, what exact
owner-local state prevents its delayed provider/decode callback from clearing a slot already reused by another read?

➡️ Recommend one full 64-bit nonzero `ReadBatchSlotTicket` allocated from the pool shard and stored both in the slot and
the caller's existing async state. Slot identity is `{poolShard, slotIndex, ticket}`; every cancel, terminal completion,
and clear validates the complete ticket. A stale callback that sees another ticket cannot mutate the slot.

The minimal logical lifecycle is `EMPTY -> ACTIVE(ticket) -> DRAINED(ticket) -> EMPTY`; cancellation takes the optional
`ACTIVE(ticket) -> CANCEL_REQUESTED(ticket) -> DRAINED(ticket)` path. Cancellation/deadline stops new source operations
but does not clear. Only provider completion or acknowledged cancellation, fallback/decode termination, and final
source-backed-buffer release permit `DRAINED -> EMPTY`. A leaked or over-deadline slot is quarantined/alerted and
consumes capacity; pressure backpressures new reads rather than force-clearing it. Pool/process close stops acquisition
and completes only after every active ticket drains.

Ticket wrap fails closed; a pool shard may rebase only while empty. The ticket is internal runtime state, not product
wire, metadata, API, or Topic configuration, and its primitive handle must not cause a per-read heap allocation.

The cost is one local ticket increment and exact state/CAS checks. It closes a real use-after-retire hole; the evidence
risk is cancellation storms, provider calls that ignore cancellation, and leaked-slot capacity.

❓ **Q2** - **Bounded multi-owner quiescence accumulator**: What is the smallest durable authority that proves every
read-admitting owner relevant to one bounded fallback set is quiescent without assuming native Owner Epoch values are
contiguous or storing an owner x source matrix?

➡️ Recommend one `SourceRetirementBatchId` and exact fallback-set digest per bounded `PREFERRED_ONLY` publication, plus
a Binding-incarnation-scoped monotonic `ReadAdmissionEpoch` assigned by the same fenced ownership transition only when
that owner may admit reads. A backend-native Owner Epoch may be reused only if conformance proves the identical order;
otherwise it is mapped, not compared directly.

One CAS-updated `OwnerReadQuiescenceAggregateV1` per active retirement batch stores at least:

```text
Binding/incarnation
SourceRetirementBatchId
fallbackSetSha256
requiredFromReadAdmissionEpoch
quiescedThroughReadAdmissionEpoch
drainedThroughReadViewGeneration
safeAfterAuthorityTime
proofProtocolVersion
```

It advances only through the next read-admitting epoch after validating that epoch's exact planned-drain or qualified-
expiry proof. Unknown response accepts only exact reread equality; a gap, source-set mismatch, view regression, or time
regression blocks release. The current aggregate is bounded state-machine authority, so it need not retain an
unbounded owner list; audit history may be append-only evidence but is not recovery authority.

The tradeoff is one low-frequency aggregate/CAS per quiesced owner per still-active retirement batch. Batching avoids
per-extent records, but too many concurrent batches multiply takeover work, so active-batch count/bytes and owner x
batch CAS rate require admission/evidence.

❓ **Q3** - **Closed Protocol Cell/backend quiescence capability**: How does 0.2 persist which backends may release
protection after unplanned takeover without turning six correctness conditions into loosely related flags?

➡️ Recommend one closed versioned capability in Protocol Cell/backend admission:

- `DURABLE_DRAIN_ONLY_V1`: planned exact `OwnerReadQuiescenceProof` may advance the aggregate; unplanned takeover
  retains protection;
- `AUTHORITY_EXPIRY_V1`: additionally permits unplanned expiry proof and binds backend adapter/protocol version,
  read-authority/time-authority identity, `notAfter` semantics, hard `maxSourceAccessLifetime`, max clock skew,
  propagation grace, pause/recovery recheck contract, and current conformance receipt.

The safe default is `DURABLE_DRAIN_ONLY_V1`. No Topic/Namespace/host may promote the capability. Missing, stale,
downgraded, or mismatched capability retains protection; it does not reinterpret an existing proof. A capability
change applies only after a new Cell/backend admission generation and cannot retroactively qualify earlier reads.

The cost is a stricter backend admission matrix and potentially long retention after crashes. It avoids a generic
lease flag silently granting deletion authority and keeps ordinary reads free of metadata I/O.

## Deferred descendants

- Q1 must settle before exact slot cell encoding, executor migration, close/drain, and cancellation-storm tests freeze.
- Q2 must settle before proof wire IDs, batch rollover/retirement, response-loss vectors, and proof-record GC freeze.
- Q3 must settle before Kafka/Pulsar backend capability receipts and unplanned-takeover GC tests freeze.
- Numeric slot/batch/time/retention caps and single-reference-versus-seqlock layout remain evidence-selected.
- `V2-OPEN-OBJ-22`, `V2-OPEN-OBJ-24`, `V2-OPEN-BK-11`, `V2-OPEN-BK-13`, remaining `V2-OPEN-OBJ-17`,
  `V2-OPEN-OBJ-19`, and `V2-OPEN-PUL-OBJ-09` remain evidence-blocked.
- KoP remains documented and deferred outside the 0.2 runtime.

## Explicit adjusted confirmation

The following response is preserved verbatim. It confirms only the adjusted boundaries stated here; the original
recommendations above remain a non-normative record where they differ.

````text
Round 15 不按原文全部确认：

- Q1 实质简化后确认。
- Q2 只确认逻辑证明合同，不确认 per-batch mutable accumulator。
- Q3 补齐 evidence binding 后确认。

Q1 — ticket 必要，但 slot 热路径只保留 FREE/PINNED(ticket)，不把整个 async 生命周期塞进 slot 状态机。

建议：

1. slot 的唯一原子所有权字段是 activeTicket：
   - 0 表示 FREE；
   - 非 0 表示 PINNED(ticket)。

2. cancel、deadline、provider I/O、fallback、decode 和 buffer 引用状态继续由现有 read-batch async lifecycle 管理，不复制进 slot。

3. 非终态 callback 只做 activeTicket == ticket 的 acquire/equality 检查，不执行 slot CAS。ticket 不匹配只能 no-op 并增加采样计数器，禁止逐条打印日志。

4. slot 只需要两个关键原子切点：
   - acquire：从 FREE 获得一个新 ticket；
   - 唯一 terminal drain：CAS(activeTicket: ticket -> 0)。

   如果 ticket 和 state 分成两个独立原子，旧 callback 可能先看到旧 ticket，暂停后再用单独 state CAS 清掉新请求，因此 terminal clear 必须把 ticket 放进同一个原子比较条件。

5. cancellation 的线性化点只是“禁止新的 source use”。它不能直接释放 slot。实现必须通过以下任一方式证明 drain：
   - 同一 event-loop/owner executor 串行化所有 source-use scheduling；或
   - owner-local tryAcquireSourceUse + outstanding-use accounting。

   只有 provider completion/具有真实 source-access 终止语义的 cancel acknowledgement、fallback/decode 结束和最后一个 source-backed buffer 释放后，才能触发 terminal CAS。

6. 不要求一个 shard-wide AtomicLong 每次读都分配 ticket。优先使用：
   - per-slot reuse generation；或
   - 每个 pool shard/thread 批量领取 ticket range。

   不能因为 ticket 分配制造新的共享 cache-line 热点。

7. 不必坚持“完整 64-bit ticket + 独立状态位”。可以使用一个 64-bit SlotLeaseWord 原子编码 active bit/state + 至少 61/62-bit generation；原子比较 state+generation 比名义上的完整 64 bit 更重要。wrap 直接 retire slot/fail closed，0.2 不需要 rebase 协议。

8. provider 不响应取消时，slot 进入 bounded quarantine 并继续占用硬容量；不能 force-clear 后复用。强制进程终止可以销毁整个本地 pool，但不能发布 durable quiescence proof。

因此 Q1 的正常成本应控制为一次 slot acquire、普通 equality load 和一次 terminal clear，而不是每个 provider/decode callback 都 CAS。

Q2 — 确认 ReadAdmissionEpoch 和连续 owner-proof 需求，但不确认每个 active retirement batch 一个 CAS accumulator。

原方案虽然给 batch 数加了上限，实际写放大仍是：

owner takeover 数 × active retirement batch 数

owner quiescence 是 Binding/read-admission 事实，不是 source-specific 事实，不应为每个 fallback batch重复提交一次。

建议冻结以下逻辑模型：

1. ReadAdmissionEpoch：
   - Binding/incarnation scoped；
   - 单调、不复用；
   - 与 owner 获得 read-admission authority 在同一个 atomic visible transition 中发布；
   - epoch 尚未持久化时，owner 不能接纳读取；
   - Native Owner Epoch 只有通过相同严格排序 conformance 后才能直接复用。

2. 每个 immutable SourceRetirementBatch 只保存：
   - SourceRetirementBatchId；
   - fallbackSetSha256；
   - exact fallback view/protection identities；
   - firstFallbackCapableReadAdmissionEpoch；
   - lastFallbackCapableReadAdmissionEpoch。

3. 每个 ReadAdmissionEpoch 最多产生一次 source-independent durable quiescence proof，绑定：
   - Binding/incarnation；
   - ReadAdmissionEpoch 和 exact Owner Epoch identity；
   - drainedThroughReadViewGeneration；
   - safeAfterAuthorityTime；
   - proof/capability digest；
   - planned-drain 或 qualified-expiry proof identity。

4. Binding 维护一个有界 quiescence proof window/head。retirement batch 释放前，必须证明其
   [firstFallbackCapableEpoch, lastFallbackCapableEpoch]
   区间内每个 read-admitting epoch都被连续覆盖；gap 只阻塞覆盖它的 batch，不能自动毒化后来才创建、旧 owner 从未见过的 source。

5. 一个 owner proof 可以被多个 retirement batch引用，因此元数据复杂度是 O(owner + batch)，而不是 O(owner × batch)。

6. 连续 proof 可以后台压缩成 bounded frontier/segment，但物理表示应继续由 M4 证据选择；本轮不冻结 OwnerReadQuiescenceAggregateV1 per batch。

7. proof window、active batch、未 quiesce epoch 数和 durable bytes 必须有 hard cap。达到上限时停止新的 source-retirement/read admission并保留 protection，不能丢 proof 或跳过 owner gap。

这种结构会多出低频、每 owner 一次的 durable proof，但避免每次 takeover 对所有 active batch逐个 CAS。

Q3 — 两种 closed capability 对 0.2 足够，但名称只能是 discriminator，真正授权来自不可变 capability evidence digest。

保留：

- DURABLE_DRAIN_ONLY_V1
- AUTHORITY_EXPIRY_V1

两种 variant 必须共享一个 immutable envelope：

- Protocol Cell/backend admission generation；
- backend adapter/protocol/config digest；
- ReadAdmissionEpoch contract version；
- proof protocol/verifier version；
- conformance receipt identity/SHA；
- capability record SHA/version。

AUTHORITY_EXPIRY_V1 再额外绑定：

- read/time authority identity；
- notAfter semantics；
- maxSourceAccessLifetime；
- max clock skew；
- propagation grace；
- pause/recovery recheck contract。

每个 owner grant、ReadAdmissionEpoch、quiescence proof、proof-window fold、retirement batch和protection-release CAS 都必须绑定 exact capability generation/digest。不能读取“当前 capability”后追溯解释旧 owner或旧 reads。

补充规则：

1. DURABLE_DRAIN_ONLY_V1 只是说明允许验证 durable planned drain，不代表某个 owner 已经 drain；仍需 exact proof。
2. AUTHORITY_EXPIRY_V1 只是允许 expiry verifier，不代表任意 lease/session loss 已合格；每个 owner仍需 exact authority-time evidence。
3. conformance receipt 在 admission generation 创建时必须有效并被 digest 固定；后续升级不能追溯授权旧 epoch，安全撤销或 verifier 缺失一律 RETAIN。
4. capability record只保存 receipt identity/digest，不复制完整测试报告，避免控制元数据膨胀。
5. capability 只在 Cell open、ownership、handoff和GC控制路径验证。普通读取只使用已缓存的 owner/deadline fence，继续保持零远端 metadata I/O。
6. Topic、Namespace、host不能提升 capability，也不能关闭 fail-safe retention。

性能结论：

- Q1：必要，但应减少为 acquire + terminal clear；主要风险是 cancellation storm、slot quarantine和pool exhaustion。
- Q2：原 per-batch accumulator 是本轮最大过度设计点；改为每 owner 一份 source-independent proof 后，可消除 owner × batch CAS。
- Q3：不会明显影响读热路径；主要代价是严格准入和证据不足时延长 source retention。

M4/M5 应量化：

- atomic operations/read batch；
- cancellation p99、late-callback count和quarantined slots；
- quiescence proof records/owner；
- metadata writes/takeover；
- proof-window bytes/age和compaction成本；
- active retirement batches；
- retained protection bytes/age；
- protection release p99。
````

## Authoritative synchronization

- Q1 resolves `V2-OPEN-READ-05` in
  [ADR 0072](../../decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md): one atomic
  `FREE/PINNED(generation)` lease, equality-only callbacks, complete terminal drain, bounded quarantine, no force clear,
  and no shared per-read ticket hotspot.
- Q2 resolves the logical part of `V2-OPEN-READ-06` in
  [ADR 0073](../../decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md): one monotonic
  Read Admission Epoch order, immutable retirement intervals, one reusable source-independent proof per epoch, and no
  mutable per-batch accumulator. Physical proof-window/fold representation remains evidence gate
  `V2-OPEN-READ-08`.
- Q3 resolves `V2-OPEN-READ-07` in
  [ADR 0074](../../decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md): two closed
  discriminators authorized only by immutable, historically bound capability evidence. Canonical encodings and actual
  backend admission remain evidence gate `V2-OPEN-READ-09`.
