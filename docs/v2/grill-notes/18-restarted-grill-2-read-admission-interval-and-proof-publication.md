---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 16: read-admission interval and proof publication

Date: 2026-08-10

Round 15 accepted one minimal slot lease, source-independent owner proof, and immutable capability-evidence binding. It
explicitly rejected callback lifecycle state in the slot and mutable owner x retirement-batch accumulators. The next
frontier is now limited to two durable cuts: deriving an exact fallback-capable Read Admission Epoch interval across a
handoff/takeover race, and publishing at most one reusable proof per epoch without recreating per-batch writes. Nothing
below is normative until explicit confirmation.

## Source facts and constraints

- A `SourceRetirementBatch` can release protection only if its first/last epoch interval cannot omit an owner that saw
  fallback. Conservatively including an extra epoch delays release but is safe; omitting one is unsafe.
- Updating each source on every owner takeover would recreate owner x source writes. The interval must instead derive
  from immutable view publication plus the Binding's contiguous Read Admission Epoch order.
- `PREFERRED_ONLY` can race a takeover. If an old owner freezes `lastEpoch=E` after E+1 has begun admitting fallback
  reads, the batch is unsound; the two cuts need one fenced linearization order.
- Planned drain and qualified expiry may race to prove the same epoch, and their evidence bytes need not be identical.
  The logical contract needs one canonical winner without requiring a mutable proof per retirement batch.
- Q1/Q2 do not select proof-window/fold Java or wire layout. `V2-OPEN-READ-08/09` remain evidence gates.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-READ-10` |
| Q2 | `V2-OPEN-READ-11` |

❓ **Q1** - **Fallback-capable epoch interval and takeover linearization**: How is
`[firstFallbackCapableReadAdmissionEpoch, lastFallbackCapableReadAdmissionEpoch]` derived without updating every
fallback source on every takeover, and what prevents a concurrent old owner from freezing the interval too early?

➡️ Recommend deriving the interval from two immutable, owner-fenced view cuts:

1. The `PREFERRED_WITH_FALLBACK` generation binds its exact fallback/protection identities and the current durable
   `ReadAdmissionEpoch` as `firstFallbackCapableReadAdmissionEpoch`.
2. Every takeover that may admit reads first publishes the next never-reused `ReadAdmissionEpoch` with the exact Owner
   Epoch and capability-evidence digest. It does not rewrite each still-visible fallback source; every intervening epoch
   is conservatively fallback-capable.
3. The `PREFERRED_ONLY` manifest-root CAS is conditional on the exact prior fallback view, current Owner Epoch, and
   current `ReadAdmissionEpoch`. Its immutable `SourceRetirementBatch` freezes that epoch as
   `lastFallbackCapableReadAdmissionEpoch`.
4. If takeover wins first, the stale CAS has a definitive fence conflict and the new owner recomputes `last`. If the
   no-fallback CAS wins first, the new owner observes `PREFERRED_ONLY` before admitting under E+1, so E+1 is outside the
   interval. Unknown response converges only through exact root/generation/batch reread.
5. Removing and later reintroducing the same physical source creates a new fallback-view/protection identity and a new
   interval; intervals are never merged by object key alone.

Before admitting a new Read Admission Epoch, the Binding must also pass the accepted hard-cap check for one additional
unquiesced epoch/proof-window liability. This is one low-frequency ownership/handoff control cut, not per-read or per-
source metadata I/O.

The main tradeoff is conservative coverage: a fallback introduced late or removed early within E still includes all of
E. That may retain bytes longer, but it avoids a sub-epoch counter and owner x source state.

❓ **Q2** - **One immutable proof per Read Admission Epoch**: How do planned-drain and qualified-expiry reconcilers
publish one reusable source-independent proof, recover an unknown response, and avoid both a mutable per-batch record
and an unbounded proof-candidate set?

➡️ Recommend one deterministic Binding/incarnation + `ReadAdmissionEpoch` proof key and one immutable canonical value.
Creation uses conditional put/create-only semantics:

- the value binds every ADR-0073 proof field and the exact ADR-0074 capability generation/digest;
- the first qualifying proof wins; a later planned/expiry candidate does not replace it;
- response unknown rereads the exact key: exact value equality proves the attempted publication succeeded;
- a different existing value is accepted as logical epoch coverage only after the closed verifier validates its exact
  Binding, epoch, Owner Epoch, read-view cut, authority-time cut, capability digest, and proof identity; otherwise the
  epoch fails closed and is quarantined for operator evidence rather than overwritten;
- the proof can be referenced/admitted once into the Binding's bounded proof window and reused by every intersecting
  retirement batch. Window/head/fold representation remains M4 evidence-selected under `V2-OPEN-READ-08`.

This costs at most one low-frequency immutable proof write per read-admitting epoch. First-valid-wins may preserve a
later safe time than another candidate and delay release, but it avoids selector CAS, replacement races, and multiple
candidate rows. Proof records/window liability remain under the already accepted count/bytes/age hard caps.

## Deferred descendants

- Q1 must settle before exact retirement-batch interval/batching and owner-grant response-loss vectors freeze.
- Q2 must settle before proof-key/wire IDs, proof-window admission, fold substitution, and proof-record retirement
  freeze.
- Exact proof-window/head/fold representation and numeric caps remain evidence gate `V2-OPEN-READ-08`.
- Exact capability/receipt binary encoding and admitted backend generations remain evidence gate
  `V2-OPEN-READ-09`.
- Event-loop serialization versus outstanding-use accounting, bit layout, padding, and quarantine limits remain M4
  evidence-selected implementation choices under ADR 0072 rather than product decisions.
- `V2-OPEN-OBJ-22/24`, `V2-OPEN-BK-11/13`, remaining `V2-OPEN-OBJ-17/19`, and `V2-OPEN-PUL-OBJ-09` remain evidence-
  blocked.
- KoP remains documented and deferred outside the 0.2 runtime.

## Explicit adjusted confirmation

The following response is preserved verbatim. It confirms only the adjusted boundaries stated here; the original
recommendations above remain a non-normative record where they differ.

````text
Round 16：Q1、Q2 均调整后确认。

Q1：接受用整个 Read Admission Epoch 保守表达 fallback-capable 区间，避免 sub-epoch 以及 owner × source 元数据。

但需要补充以下合同：

1. takeover/read grant 与 PREFERRED_WITH_FALLBACK → PREFERRED_ONLY 必须竞争同一个 Binding/incarnation 级 selector CAS，或者使用被证明等价的原子事务。selector 至少比较：
   {selectedViewSha, OwnerEpoch, ReadAdmissionEpoch, readAdmissionState}
   禁止用跨 key 的应用侧重读代替原子条件。

2. takeover 执行：
   (PWF, O, E) → (PWF, O2, E+1)

   PREFERRED_ONLY 执行：
   (PWF, O, E) → (PO, O, E, retirementBatch[last=E])

   谁先成功，另一方必须冲突并重新计算。E+1 只有在 selector 已显示 PREFERRED_ONLY 后，才允许在无 fallback 状态下接纳读取。

3. firstFallbackCapableReadAdmissionEpoch 只在新的 fallback/protection identity 首次出现时设置。相同 fallback 穿过后续 PREFERRED_WITH_FALLBACK generation 时必须继承原 first，不能重置为当前 epoch。一个 batch 包含不同 first 的 source 时，使用最早 first 形成保守区间。

4. proof-window liability 只在当前存在 fallback，或本次 transition 正在引入 fallback 时，为新 epoch 预留。完全没有 fallback 的 epoch 不应因为历史 retirement batch 而生成 proof 或阻塞 takeover。

这样 takeover 只需要原本就必须存在的一次 Binding 级控制 CAS，不重写每个 source，也不给普通读取增加 metadata I/O。代价主要是 takeover 冲突重试，以及整 epoch 保守覆盖可能延长 source 保留时间；它影响存储成本，不影响稳定态读吞吐。

Q2：接受 deterministic Binding/incarnation/ReadAdmissionEpoch key、create-only、first-valid-wins、unknown-response exact reread，以及不同但有效的 existing proof 可作为既有 coverage。

但 proof 发布前必须先存在不可逆的 epoch terminal cut，至少绑定：
{Binding/incarnation, ReadAdmissionEpoch, OwnerEpoch,
 last admitted/drained read-view cut,
 capability evidence digest,
 admissionClosedFence 或 qualified authority notAfter}

terminal 后不得重新开放相同 epoch；任何新读取只能进入 E+1。planned drain 和 qualified expiry 必须绑定并验证同一个 terminal cut SHA，否则唯一 key 本身不能证明该 epoch 已停止接纳读取。

还需要限定：

- 只有被 fence 的授权 publisher 能 create；
- candidate 必须先通过 closed verifier；
- canonical value 不得包含随机数、本地时间或非确定序列化；
- 无效 occupying value 只能 fail closed/quarantine，禁止覆盖；
- proof 按需生成：只有 epoch 与 fallback-capable interval 相交时才要求写入，不为所有无 fallback epoch预写；
- proof-window/head/fold 的物理布局继续留给 M4 evidence，不在本轮冻结。

性能上，每个相关 epoch 最多增加一次低频 immutable proof write，不是 owner × batch 写放大，也不增加普通读的原子操作或远端 I/O。first-valid-wins 可能选择较晚的 safeAfter，从而延长保留时间；0.2 接受这一保守成本，换取不引入 proof selector、替换竞态和多 candidate 状态。

因此本轮最终结论是：
Q1 调整后确认；
Q2 调整后确认；
selector linearization、terminal epoch 和 proof 按需生成属于不可关闭的正确性合同，不做 Topic 可选开关；可配置的只应是 Cell/Binding admission 上限、reconciler 频率和证据选出的容量参数。
````

## Authoritative synchronization

- Q1 resolves `V2-OPEN-READ-10` in
  [ADR 0075](../../decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md): takeover/read grant
  and no-fallback publication share one Binding selector CAS or proven equivalent transaction; the whole-epoch
  interval inherits each source's first epoch, mixed-first batches use the earliest, and no-fallback epochs create no
  proof liability.
- Q2 resolves `V2-OPEN-READ-11` in
  [ADR 0076](../../decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md): one irreversible
  terminal cut precedes a fenced, closed-verified, deterministic create-only proof generated only for an intersecting
  fallback interval. Physical proof-window/head/fold representation remains evidence gate `V2-OPEN-READ-08`.
- Exact selector terminal-state publication and immutable retirement-batch construction remain the independent
  Round 17 frontier under `V2-OPEN-READ-12/13`; exact capability encoding/backend admission remains evidence gate
  `V2-OPEN-READ-09`.
