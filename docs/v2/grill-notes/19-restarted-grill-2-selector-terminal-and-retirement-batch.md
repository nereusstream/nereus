---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 17: selector terminal and retirement batch

Date: 2026-08-10

Round 16 accepted one Binding/incarnation selector linearization point, conservative whole-epoch fallback intervals,
one irreversible epoch-terminal prerequisite, and deterministic on-demand proof publication. It did not freeze how a
successful selector transition remains a verifiable admission-closure fence until an asynchronous terminal cut exists,
or how the selector activates one bounded immutable retirement batch without creating a second completion authority.
Nothing below is normative until explicit confirmation.

## Source facts and constraints

- The accepted takeover transition must remain one Binding-level selector CAS or proven equivalent transaction. It
  must not grow into one write per source or add metadata I/O to ordinary reads.
- `PREFERRED_WITH_FALLBACK -> PREFERRED_ONLY` freezes `lastFallbackCapableReadAdmissionEpoch=E` but keeps E selected.
  Protection cannot be released until E later becomes terminal and its relevant reads are proven quiescent.
- A successful transition away from E must prevent every later read admission under E, yet in-flight reads may drain
  asynchronously. Closure and quiescence are therefore distinct facts.
- A selector-selected retirement batch must be complete and immutable at the selector linearization point. An
  unselected candidate is inert; a mutable completion accumulator would recreate a second handoff authority.
- Q1/Q2 do not freeze proof-window/fold layout, capability-token encoding, Java layout, or numeric limits.
  `V2-OPEN-READ-08/09` remain evidence gates.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-READ-12` |
| Q2 | `V2-OPEN-READ-13` |

❓ **Q1** - **Selector terminal-state publication**: How does E stop admitting reads, remain verifiably closed across
later takeovers, and eventually obtain one terminal cut without adding another synchronous takeover write or allowing
E to reopen?

➡️ Recommend the following minimal logical state machine:

1. The selector has only an `ADMITTING` state for a current epoch and a fail-closed `STOPPED` state for deletion,
   quarantine, or the absence of an admissible successor. `CLOSING`, `DRAINING`, and proof progress do not become
   reader-visible selector states.
2. One successful selector CAS from an admitting E to an admitting E+1 atomically removes read-admission authority
   from E and grants it to E+1. This is the `admissionClosedFence` for E. A takeover changes Owner Epoch in that same
   CAS; a same-owner rollover keeps Owner Epoch and changes only the never-reused Read Admission Epoch plus the exact
   selected view/capability facts.
3. The old and new canonical selector tuples, their SHAs, and the successful conditional-transition identity form one
   durable closure anchor. The backend mapping must keep that anchor verifiable until E either gains a terminal cut or
   is proven irrelevant to every fallback interval. It may be carried by the selector/backend transaction, but it may
   not require a second synchronous takeover write. Exact physical representation remains M4 evidence work.
4. After the CAS, no new slot may publish E. Reads already admitted under E retain their slot and source lifetime and
   drain normally. Only then may a fenced publisher create the immutable terminal cut for E; qualified authority
   expiry may substitute only under ADR 0074. The cut binds the exact closure anchor, last admitted/drained read-view
   cut, capability evidence, and closure proof.
5. `PWF(O,E) -> PO(O,E,retirementBatch[last=E])` freezes the fallback interval but does not terminalize E. Before that
   batch can release protection, the owner/reconciler performs one same-owner `PO(O,E) -> PO(O,E+1)` selector CAS and
   later terminalizes E. It may keep serving no-fallback reads under E until reconciler cadence or storage-retention
   pressure justifies that rollover; doing so delays release but is safe.
6. A response-unknown selector CAS converges by exact reread. If the selector has advanced again, the durable closure
   anchor must still prove the E transition; current-epoch comparison alone cannot invent missing history. Fallback-
   relevant unresolved anchors consume bounded count/bytes/age liability. Exhaustion stops new read admission or
   takeover before losing an anchor; it never reopens E or manufactures a terminal cut.
7. A no-fallback E needs neither a terminal record nor proof unless a fallback interval actually intersects it.
   Takeover therefore reserves terminal/proof liability only for current or newly introduced fallback, as accepted in
   Round 16.

This retains one low-frequency selector CAS on takeover and no ordinary-read metadata I/O. The tradeoff is an
asynchronous closure-anchor backlog and a possible same-owner epoch rollover before old fallback bytes can be freed.

❓ **Q2** - **Immutable retirement-batch construction and completion boundary**: What exactly becomes active with the
`PREFERRED_ONLY` selector cut, how is its identity recovered after response loss, and can one blocked source retain or
corrupt the state of unrelated sources in the same batch?

➡️ Recommend treating `SourceRetirementBatch` as an immutable eligibility envelope, not a mutable completion record:

1. The batch's exact member set is the fallback/protection identities removed by one selector transition. For the 0.2
   `PWF -> PO` cut this is the complete bounded fallback set of the selected old view; no source may be added, removed,
   split, merged, or rebound after selection.
2. Its canonical bytes contain the accepted Binding/incarnation, old/new view identities, sorted exact fallback and
   protection identities, `fallbackSetSha256`, earliest inherited first epoch, `last=E`, and exact capability-evidence
   binding. `SourceRetirementBatchId` is a domain-separated digest of those bytes; it contains no random value or local
   time.
3. The selector CAS is the sole activation point. A backend may inline the bounded canonical batch or reference an
   already-created immutable batch by exact key/SHA. In the latter mapping, an unselected record is inert and
   recoverable as residue; the CAS cannot select a missing, mutable, or unverified record. Cross-key application-side
   reread is not a substitute for selector atomicity.
4. The admitted fallback-view bound must fit one retirement batch. 0.2 therefore does not add partial fallback-removal
   views merely to split an oversized batch; admission rejects a view whose member/count/byte envelope could not later
   be retired atomically. Exact numeric caps and inline-versus-reference backend mapping remain M4 evidence choices.
5. Proof coverage and batch identity are shared, but protection release remains exact per source-protection
   generation. Once the common interval proof, relevant pins, and that source's protection conditions pass, its
   idempotent release CAS may converge independently. One corrupt/quarantined source retains itself without blocking
   eligible siblings.
6. The immutable batch is retained as the audit/eligibility fact; it does not gain a mutable released bitmap, remaining
   count, or completion CAS. Completion is derived from exact source-protection states. A reintroduced source uses a
   new protection identity and fallback interval and cannot join the old batch.
7. Unknown create/select/release responses converge only by exact key/value/SHA reread. Active/unselected batch count,
   bytes, age, and residue consume bounded Cell/Binding capacity; pressure backpressures handoff or admission and never
   rewrites a selected batch.

This costs at most one immutable batch publication plus the already required selector CAS per bounded handoff. It
avoids one CAS per extent and avoids turning batch completion into a second source-lifecycle authority. Independent
per-source release prevents a single retained source from amplifying storage cost across the whole batch.

## Deferred descendants

- Q1 must settle before selector wire/state IDs, closure-anchor encoding, terminal publisher roles, and repeated-
  takeover response-loss vectors freeze.
- Q2 must settle before retirement-batch key/wire IDs, selector inline/reference mapping, residue handling, and exact
  source-release convergence freeze.
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
Round 17：Q1、Q2 均调整后确认。

Q1：确认 selector 只保留 ADMITTING/STOPPED、closure 与 quiescence 分离、异步发布 terminal cut，不引入 CLOSING/DRAINING 读路径状态。

但删除原方案第 5 点的额外 same-owner rollover，改为：

PWF(O,E,ADMITTING)
  -> PO(O,E+1,ADMITTING, retirementBatch[last=E])

由同一次 selector CAS：

1. 冻结 fallback interval 的 last=E；
2. 不可逆地关闭 E；
3. 授予同 owner 的 E+1 no-fallback read；
4. 产生 E 的 durable closure anchor。

这样不必先 PWF→PO 保留 E，再等待 reconciler 执行 PO(E)→PO(E+1)。原方案会平白增加一次 selector CAS、一个 unresolved anchor，并让已经 PREFERRED_ONLY 的 source 因 reconciler cadence 继续保留。

takeover 仍与它竞争相同 expected tuple：

PWF(O,E) -> PWF(O2,E+1)

谁先成功，另一方必须冲突并重算。重新引入 fallback 时也使用新 protection identity，并建议在同一次 selector CAS 中进入新 epoch；fallback membership 未变化的普通 view 更新不必 rollover。

补充合同：

- closure anchor 必须由 successor selector value 携带 predecessor/transition digest，或与 selector CAS 在同一事务中持久化；不能假设 backend 的旧版本或 CAS history 永久可查询。
- STOPPED 恢复只能进入新的 never-reused epoch，禁止重新开放原 E。
- hard cap 耗尽时可以 fence 旧 E 并进入 STOPPED，阻止新的 ADMITTING grant；不能为了等待容量而让旧 E 继续获得读权限。
- response unknown 时，在 exact reread 收敛前禁止继续以 E 接纳新读。
- 与 fallback interval 从未相交的 epoch 不需要 terminal/proof。

调整后 Q1 不增加正常读 metadata I/O。每个 fallback-relevant transition 仍需一次 selector CAS，以及后续异步 terminal/proof publication；主要风险是 anchor backlog、takeover 冲突和 source-retention 延迟，不是稳定态读吞吐。

Q2：确认 transition-exact immutable batch、确定性 digest ID、selector 唯一激活点、完整 view 有界准入、per-source protection release，以及不增加 mutable released bitmap/count/completion authority。

但需要四项调整：

1. 每个 source/protection row 保存自己的：
   firstFallbackCapableReadAdmissionEpoch

   batch 可以保存 min(first_i) 作为摘要，但单个 source 的释放必须验证：
   [first_i, sharedLast]

   不能让所有 source 共用 batch 最早 first。否则早期 epoch 的 proof hole 会阻塞后来才加入、原本可以独立释放的 source，形成批级 HOL。

2. 明确实际控制面成本：

   - inline 模式：1 selector CAS；
   - reference 模式：1 immutable batch create + 1 selector CAS；
   - N 个 protection identity 最多需要 N 次独立 release CAS；
   - crash recovery/final reconciliation 需要 bounded O(N) authoritative state scan。

   该设计避免的是 mutable batch-progress/completion CAS，不是消除所有 per-source/per-extent CAS。

3. 不支持原子引用验证的 backend 必须 inline batch。只有 backend 能在 selector transaction 中原子证明 immutable batch key/SHA 已存在时，才允许 precreate + reference；跨 key pre-read 再 CAS 不等价。

4. 完整 batch 不能永久无限保留。所有 source protection 已 RELEASED/retired，且没有 selector、lineage、recovery 或 response-loss 引用后，应允许替换为紧凑 retired tombstone，或按受证明的 lineage-retirement 协议回收。该动作只是派生压缩，不成为新的 GC authority。

单个 quarantined source 不阻塞 sibling 的 release，但仍会阻止完整 batch retirement并占用 batch count/bytes/age budget，这一点必须如实计入 backpressure，而不能宣称完全无批级影响。

因此最终结论：

- Q1：合并 PWF→PO 与 E→E+1 后确认。
- Q2：加入 per-source first epoch、如实声明 O(N) 成本并补 batch retirement 后确认。
- selector/terminal/batch membership 属于不可配置的正确性合同；可配置的仍只是 Cell/Binding hard caps、reconciler cadence 和 evidence-derived 容量参数。
````

## Authoritative synchronization

- Q1 resolves `V2-OPEN-READ-12` in
  [ADR 0077](../../decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md): one selector CAS freezes
  `last=E`, closes E, grants same-owner no-fallback E+1, and durably anchors the transition; only
  `ADMITTING/STOPPED` exist, response unknown fences E, and STOPPED recovers only to a fresh epoch.
- Q2 resolves `V2-OPEN-READ-13` in
  [ADR 0078](../../decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md): every source row owns
  `first_i`, release checks `[first_i,sharedLast]`, inline/reference atomicity and N/O(N) costs are explicit, progress
  remains derived, and full batch metadata must eventually compact without becoming source-GC authority.
- Exact bounded closure-anchor/terminal publication and compact retired-batch representation remain the independent
  Round 18 frontier under `V2-OPEN-READ-14/15`; proof-window/fold and capability encodings remain evidence gates
  `V2-OPEN-READ-08/09`.
