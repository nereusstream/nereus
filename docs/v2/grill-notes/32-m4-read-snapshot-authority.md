---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m1
---

# M4 Grill 32: Binding read-view authority and linearization

Date: 2026-08-31

## Status and authority

This note preserves the complete Round 32 question frontier and the complete response from the one fixed
`gpt-5.6-sol` / `max` reviewer requested for M4 Grills 32 through 35. The reviewer was reused as one read-only agent;
it made no file, commit, push, evidence, or runtime change. The response was then synchronized into the
[M4 index](../detailed_design/m4/README.md) and
[M4-A normative detailed design](../detailed_design/m4/m4-a-read-view-authority.md).

This session record is non-normative. Accepted ADRs and the M4 detailed design outrank it. The response's OPEN
physical and evidence choices remain open; they are not silently converted into a contract by inclusion here.

## Submitted independent frontier

| Question | Recommended alternative | Required cut |
| --- | --- | --- |
| Q32.1 | B | allocation-free cached Binding authority plus hazard publication, StoreLoad, generation recheck, then stable matching publication-cell capture; no remote metadata or durable read ticket |
| Q32.2 | B | one capture per bounded Binding protocol batch; only internal authorized retry/fallback remains under it; every new operation/redirect recaptures |
| Q32.3 | B | one coherent capture per Binding; disjoint ranges may mix captured sources, atomic units/declared ranges remain source-pure, multi-Binding admission is all-or-release |
| Q32.4 | B | a visible slot pins G after publication; G+1 does not replan the batch; takeover stops new use but owner-local slots never become durable quiescence proof |
| Q32.5 | B | capacity and stable capture precede I/O; cancellation stops new source use; only complete terminal drain clears exact lease; nontermination quarantines capacity |
| Q32.6 | B | M4 binds immutable M3 Final/closure identities through a separate future dependency check and never weakens the exact-source `v2M3Check` |

The reviewer was also required to accept, map, or reject the pasted terms `ReadSnapshot`, `readVersion`,
`min-read-snapshot ACK`, and per-physical-Object `readingSlot`; identify every remaining evidence-selected physical
choice as OPEN; and explicitly weigh correctness against hot-path cost, overdesign, and complexity.

## Fixed reviewer response preserved verbatim

<!-- BEGIN FIXED REVIEWER RESPONSE -->

## Q32.1 — Authority and linearization

**Decision: B, with one precision.** The generation recheck is a prerequisite; the read-view admission linearizes at the final successful stable publication-cell capture whose generation tag matches the rechecked `G`.

Required acquisition sequence:

1. Validate the cached Binding authority: exact Binding/incarnation, `BindingReadSelector` tuple, `ADMITTING` Read Admission Epoch, Owner/Storage fence, and protocol-specific fence.
2. Acquire exclusive slot lease `L`.
3. Acquire-load the immutable cached admission/source-generation reference `A/G`.
4. Publish exact `{Binding, G}` under `L`.
5. Establish StoreLoad/full-fence ordering.
6. Acquire-load `A/G` again. Any identity, selector, owner, admission-epoch, or generation mismatch clears `L` and retries before dereferencing `G`.
7. Capture the stable generation-tagged publication cell. For Kafka, this transitively binds the partition state root, leader/Storage/Owner fences, LEO/HW/LSO, active-tail/source-map and side-index generations.
8. Accept only when the cell is stable and `cell.sourceGenerationId == G`.

For a single-reference cell, the linearization point is its successful acquire-load and tag validation. For a seqlock, it is the successful final sequence validation. It is not the earlier generation load, and it is not a claim that provider I/O or response publication has completed.

A loose collection of “previously validated” cache fields is insufficient: the second recheck must compare one immutable authority reference or an equivalent exact tuple. Otherwise selector closure or takeover could race admission and allow a slot to publish a closed epoch.

There is:

- no synchronous Oxia/manifest/control-metadata I/O;
- no per-read metadata row;
- no global `readVersion`;
- no durable read ticket or snapshot ACK;
- no deep-copy snapshot.

The unavoidable hot-path cost is one slot acquisition/publication, one StoreLoad/full fence, two authority/generation loads, a small stable-cell read, and one eventual terminal CAS. That is preferable to remote I/O, heap allocation, global refcount contention, or deep-copy cache pressure.

**Failure behavior:** unstable authority, generation mismatch, or torn/tag-mismatched cell clears the exact lease before source dereference and retries within the bounded admission budget. Invalid or uncertain owner/admission authority safe-fails or redirects before source I/O.

**OPEN physical details:** single-reference versus seqlock, VarHandle operations, cache-line layout/padding, pool sharding, and how the existing protocol state root is referenced without allocating.

**ADR:** No new ADR. ADRs 0069, 0070, 0072, 0075, 0077, and 0087 already own this contract. The M4 charter should state this exact interpretation.

## Q32.2 — Logical batch, retry, redirect, and fallback

**Decision: B.** One capture belongs to exactly one bounded Binding-scoped protocol read batch.

Examples are one Kafka partition Fetch/range sub-operation or one ManagedLedger `readEntries` operation. The batch starts with successful capture and remains unfinished until every possible source access and source-backed buffer use has drained.

An internal retry or fallback remains in the same batch only when all of these hold:

- it serves the same inbound protocol operation, Binding, requested logical interval, isolation/upper bound, and response attempt;
- the captured view already authorizes the exact source and protected fallback;
- retry/fallback budgets remain;
- the cached owner/deadline fence still permits starting that source use;
- it does not split an atomic append unit or violate a declared whole-range fallback;
- it does not require replacing previously externally observable bytes or native response state.

A provider transport retry may remain internal only when it still addresses the same authenticated immutable physical source. A redirect that changes owner, Binding authority, source identity, or protocol endpoint authority requires a new batch.

A new capture is mandatory for:

- every new client protocol operation or client retry;
- every subsequent Kafka Fetch, even with a sequential cursor;
- every new `readEntries` call;
- broker/owner redirect or reassociation;
- any retry after a Binding/incarnation, selector epoch, Owner/Storage/Kafka leader fence, or requested range changes.

The cursor is only an exact-match acceleration hint. It never carries a pin. The next operation captures first, accepts the cursor only if every bound identity/version matches, and otherwise discards it.

Once bytes or native response state are externally observable, the implementation may not recapture, retroactively replan, or replace already exposed data. It may continue only along the captured plan. A captured fallback for a later, still-unobserved independent range remains legal; whole-range fallback becomes illegal after any byte of that fallback unit/range is exposed. If the native protocol cannot represent a partial failure safely, the response fails or the connection closes according to native semantics. The slot nevertheless remains pinned until transport and buffer drain complete.

**Tradeoff:** this can retain a slot across provider retries and network-buffer lifetime, but prevents cross-version response splicing and indefinite connection/session pins.

**OPEN physical details:** retry counts/backoff, exact provider redirect classification, protocol-adapter response-commit cut, native error mapping, and evidence-selected maximum batch lifetime.

**ADR:** No new ADR. This is the direct operational boundary of ADRs 0069/0070/0072 and ADR 0087’s cursor rule.

## Q32.3 — Cross-segment, cross-source, and multi-Binding consistency

**Decision: B.** Coherence is per Binding; no global metadata revision is invented.

For each Binding, the captured plan is deterministic:

1. resolve the Storage Epoch interval through the Position Domain;
2. order ranges by protocol position;
3. choose acknowledged active tail for unsealed coverage;
4. choose the manifest-selected preferred generation for sealed coverage;
5. use only the exact still-protected fallback declared by the capture;
6. fail closed on a gap, ambiguous overlap, or unavailable unauthorized source.

`ObjectMaterializedFrontier` remains a routing hint; the captured Source Map and generation-tagged references own exact coverage.

Disjoint, non-overlapping ranges may intentionally mix Object/manifest and BookKeeper/active-tail sources. The following remain source-pure:

- one Kafka append commit set;
- one Pulsar entry;
- any path whose accepted contract declares whole-range fallback, including Pulsar sealed-ledger fallback.

Results remain in native protocol/Position Domain order. Internal I/O may be parallel, but completion order cannot reorder the logical response.

For a multi-Binding request:

- reserve every required slot before any source I/O;
- if any reservation or capture fails to stabilize, release the entire set and retry, safe-fail, or split only on a protocol-legal boundary;
- never retain partial pins while waiting for remaining capacity;
- give each Binding its own coherent capture.

Different Bindings may therefore reflect different valid instants. That is not torn state because there is no cross-Binding atomic-snapshot contract. The no-torn-world invariant is: within each Binding, every returned byte, source route, upper bound, transaction/index reference, and fence comes from one capture; no `G` source map may be paired with a `G+1` frontier or protocol state.

**Tradeoff:** this avoids a global revision, cross-Binding metadata transaction, and one-backend-per-request restriction, while requiring bounded all-or-release slot admission.

**OPEN physical details:** interval-plan representation, range coalescing, parallel scheduling, and the all-or-release reservation algorithm/fairness policy.

**ADR:** No new ADR. ADRs 0069, 0070, and 0087 already freeze these semantics.

## Q32.4 — Publication and takeover during an accepted read

**Decision: B.** A source-generation switch after successful capture does not invalidate or replan the batch.

For an ordinary `G -> G+1` publication:

- an already visible slot continues to pin `G`;
- new admissions capture the current reference and normally use `G+1`;
- retirement waits until every slot naming `G` drains;
- the old batch continues using only its captured plan.

The slot is resource-lifetime protection, not ownership authority. On selector closure, takeover, lease uncertainty, or fence loss:

- no new batch may publish the closed Read Admission Epoch;
- an existing batch may start new source use only while its exact cached owner/deadline fence permits it;
- after pause, network recovery, uncertainty, or expiry, the old owner rechecks before retry, fallback, new source I/O, or response publication;
- failure of that check closes new use, cancels/drains what can be cancelled, and safe-fails rather than publishing stale success;
- already issued provider work and source-backed buffers still keep the slot pinned until real termination.

A captured fallback remains physically protected even if a successor view becomes `PREFERRED_ONLY`. Protection release still requires, separately:

1. durable exact `PREFERRED_ONLY`;
2. current-owner fallback-bearing slot drain;
3. contiguous durable planned-drain or qualified-expiry proof for every relevant historical Read Admission Epoch;
4. exact source-protection-generation release CAS.

A new owner never adopts an old process’s slots. Process death or an empty local pool is not durable quiescence proof. Without qualifying durable proof, the fallback remains retained and capacity-charged.

**Tradeoff:** accepted reads remain stable across benign publication, while stale ownership may terminate availability and retain capacity rather than weaken correctness.

**OPEN physical details:** local admission-gate invalidation, event-loop serialization versus outstanding-use accounting, provider cancellation semantics, and evidence-selected lifetime/skew/grace values.

**ADR:** No new ADR. This reconciliation is already required by ADRs 0070–0077.

## Q32.5 — Admission failure and terminal lifetime

**Decision: B.**

Before source I/O:

- reserve the exact slot lease, or all leases for a multi-Binding request;
- complete authority/generation/cell capture;
- retry slot exhaustion or unstable capture only within a bounded admission budget;
- on exhaustion, release all reservations and backpressure or return a typed safe failure.

No partial capacity wait may issue source I/O, and no failure path may continue without a pin.

The slot’s exact atomic state machine remains deliberately minimal:

```text
FREE
  -> PINNED(L)
  -> FREE only by CAS(exact L -> FREE)
```

`L` is the slot-reuse generation protecting against late callbacks; it is distinct from source generation `G`. `{Binding,G}` is the hazard payload owned under `L`.

Cancellation/deadline/authority loss linearizes only closure of the “new source use” gate. It does not clear the slot. Success, failure, cancellation, and deadline all converge on the same terminal-drain predicate:

- no future source use can be acquired;
- provider work completed or cancellation was acknowledged with real source-access termination semantics;
- fallback and decode completed;
- every source-backed buffer, including a buffer handed to response transport, was released.

Only then may the unique terminal path CAS `PINNED(L) -> FREE`. A stale callback seeing another lease is a sampled no-op.

`CANCEL_REQUESTED`, provider/decode phases, outstanding-use count, and `QUARANTINED` must not become additional slot states. Quarantine is an operational condition in which the slot remains `PINNED(L)`. A nonterminating provider consumes bounded hard capacity indefinitely; timeout, memory pressure, or operator preference never force-clears it. Process destruction can discard the owner-local pool but supplies no durable quiescence proof.

There is no separate snapshot ACK. Append ACK remains the existing locator/frontier publication cut. Durable closure anchors, terminal cuts, and quiescence proofs are low-frequency epoch facts, not per-read acknowledgements.

**Tradeoff:** capacity can remain quarantined, but the design avoids use-after-retire, a callback-heavy slot state machine, per-callback CAS traffic, and unsafe time-based reuse.

**OPEN physical details:** lease-word bit packing, ticket allocation, pool sizes/shards, admission retry budget, quarantine limits, event-loop versus outstanding-use implementation, and protocol error mapping.

**ADR:** No new ADR. ADRs 0070 and 0072 already freeze the semantic state machine.

## Q32.6 — Frozen M3 dependency under M4 commits

**Decision: B.** Normal M4 work must depend on immutable M3 closure; it must not recertify later HEAD as M3-tested source or broaden the M3 descendant allowlist.

The following can be frozen now:

- M3 tested source: `e5e53e62865c21845621037bea5f18c092bd4259`;
- exact Final path: `docs/v2/evidence/v2-m3/final/e5e53e62865c21845621037bea5f18c092bd4259/m3-final.json`;
- exact Final SHA-256: `81c7004a923e5b96cab0a3c8b4f1fa26d71606a2208bbabe779f0d872f84f84a`;
- Final-bound source-lock SHA-256: `2a46f31c90912f3f3f10d2365b9f9ffcc8070a847c4c7e202177ea903cdc240b`;
- allocator mode/candidate: `RANGE` / `RANGE_64`;
- eleven child receipts and 26 promoted scenarios;
- retained exclusions: `M6_PROCESS_ACTIVATION`, `M8_NATIVE_PARITY`;
- M3-to-M4 closure/navigation commit: `efab430aed37b3f7c32d09b88ae935c1aea1c902`.

Important trap: `docs/v2/evidence/v2-m3/final/m3-final.json` is a different historical Final with SHA `1089d4f6...`; it is not an alias for the frozen e5 Final and must not satisfy the M4 dependency check.

The current M3 Final checker intentionally verifies a linear evidence-only descendant history. M4 source or general documentation commits are supposed to violate that allowlist. Therefore:

- do not add M4 paths to the M3 evidence allowlist;
- do not weaken descendant validation;
- do not rewrite or copy the M3 Final;
- do not change its tested commit to later HEAD;
- do not rerun the allocator campaign for ordinary M4 design or implementation.

A separate M4 dependency check should validate the historical frozen tuple and closure ancestry while M4’s own gates validate the new source and affected integration. M3 reopens only when an explicit frozen predicate is invalidated: allocator/source lock, workload/qualification/RANGE-size/harness, canonical protocol or persistent bytes, Final/archive identity, or certified-source equality. An unrelated M4 reader, validator, pin, scenario, or refactoring change is not itself a reopen.

**OPEN implementation/evidence:** checker name/schema, whether it validates frozen Git-tree objects or a canonical dependency manifest, exact M4 Final fields, treatment of later unrelated source-lock additions, affected-regression inventory, and positive/negative checker receipts. These must be implemented and evidenced under M4; this round does not manufacture that gate.

**ADR:** No new ADR for the dependency principle; it is already normative in the implementation plan and M3 execution index. Any attempt to alter M3 lineage/reopen semantics or weaken `v2M3Check` would require an explicit amendment ADR.

## Vocabulary disposition

| Pasted term | Disposition |
| --- | --- |
| `ReadSnapshot` | **Map**, do not retain as a new generic authority. Use accepted `BindingReadViewSnapshot` for the logical capture. Existing protocol-local names such as `KafkaPartitionReadSnapshotV1` may remain implementation types when they are immutable shared references, not newly allocated M4 snapshots. |
| `readVersion` | **Reject as a singular/global contract.** Map legitimate checks to the exact cached selector/admission reference, source generation `G`, protocol state version, active-tail view version, and independent Owner/Storage/Kafka fences. One scalar cannot replace them. |
| `min-read-snapshot ACK` | **Reject.** There is no per-read/minimum durable ACK. Retirement uses current-slot drain plus contiguous per-epoch durable quiescence proof and exact protection release. |
| per-physical-Object `readingSlot` | **Reject.** One unfinished Binding-scoped batch owns one slot from a bounded sharded cross-Binding pool. Per-object slots would create redundant physical authority, scale state with object count, increase contention, and fail to model one batch spanning several sources. Use the hazard slot and `SlotLeaseWord` vocabulary. |

`ReadPathDecision`, if retained, must be a pure deterministic derivation from the captured view. It is neither persisted authority nor permission to recapture each physical candidate independently.

## Round 32 conclusion

**The frontier is closed enough to synchronize as M4 design input, with all six recommendations accepted as B subject to the adjustments above.** The sync must correct the stale M4 vocabulary, distinguish the stable-cell linearization point from its generation-recheck prerequisite, and preserve the frozen M3 dependency through a separate future M4 checker. Physical representations, numeric bounds, error mappings, checker implementation, and evidence remain explicitly **OPEN**. `V2-READ-001/003/004/005/007` remain `PLANNED`; this round establishes no implementation, receipt, scenario promotion, or M4 Final.

<!-- END FIXED REVIEWER RESPONSE -->

## Authoritative synchronization

- Q32.1 through Q32.5 are synchronized in
  [M4-A](../detailed_design/m4/m4-a-read-view-authority.md) without creating a new ADR.
- Q32.6's immutable historical identity is synchronized in the
  [M4 index](../detailed_design/m4/README.md#frozen-m3-dependency); its checker shape remains Grill 35 work.
- The implementation-plan M4 row now uses `BindingReadViewSnapshot`, generation-tagged hazard slots, and terminal
  source drain rather than the rejected global revision/per-Object pin vocabulary.
- `V2-OPEN-READ-08`, `V2-OPEN-READ-09`, and `V2-OPEN-READ-15` remain evidence-blocked. No prose-only resolution is
  claimed.

Round 32 changes no production source, evidence artifact, source lock, scenario status, M3 closure, or M4 runtime
state.
