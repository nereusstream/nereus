---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m1
---

# M4 Grill 34: hazard-slot, cancellation, and reclamation races

Date: 2026-08-31

## Status and authority

This note preserves the complete Round 34 question frontier and the complete response from the same fixed
`gpt-5.6-sol` / `max` reviewer used for M4 Grills 32 and 33. The reviewer was reused as one read-only agent; it made no
file, commit, push, evidence, or runtime change. Its answer was synchronized into the
[M4 index](../detailed_design/m4/README.md) and
[M4-C normative detailed design](../detailed_design/m4/m4-c-hazard-slot-reclamation.md).

This session record is non-normative. Accepted ADRs and the M4 detailed design outrank it. Every item marked OPEN in
the response remains open; inclusion here does not promote a physical layout, threshold, provider mapping, or
milestone ownership choice into a final contract.

## Submitted independent frontier

| Question | Recommended alternative | Required cut |
| --- | --- | --- |
| Q34.1 | B | close the classic publish/scan race with hazard publication plus StoreLoad and generation recheck; add no per-read reclaimer handshake or refcount |
| Q34.2 | B | linearize only at a stable G-tagged cell after exact authority recheck; a later pointer switch leaves admitted G pinned |
| Q34.3 | B | make E admission and fused PO/E+1 closure compete through the existing exact selector authority without per-read ACKs or slot mutation |
| Q34.4 | B | cancellation closes only the batch new-use gate; exact L clears only after one complete terminal source drain |
| Q34.5 | B | retain the outer lease while transferring the inner pin once, release-before-acquire, under the original budget and observability cut |
| Q34.6 | B | quarantine unprovable provider use and backpressure; process disappearance never supplies durable quiescence |
| Q34.7 | B | freeze one semantic interface and leave owner serialization versus outstanding-use accounting evidence-selected and mutually exclusive |
| Q34.8 | B | keep local slot drain, durable proof, exact protection release, and physical deletion as separate ordered predicates |
| Q34.9 | B | freeze canonical state/race subcases, safety/liveness invariants, and hot-path budgets while leaving layout and numeric choices OPEN |

The reviewer was required to identify overdesign, memory-order gaps, hot-path atomic/cache-line costs, callback ABA,
same-lease attempt confusion, crash/nonresponse behavior, M4/M5 ambiguity, ADR boundaries, scenario ownership, and all
remaining evidence-selected choices.

## Fixed reviewer response preserved verbatim

<!-- BEGIN FIXED REVIEWER RESPONSE -->

Round 34 verdict: accept recommendation B for Q34.1–Q34.9, with three tightenings:

1. Reclamation may use one post-swap complete clean scan, but a pinned slot whose exact lease-bound payload is not stably readable is `INCONCLUSIVE`, never “no hazard.”
2. Same-owner fused fallback closure does not revoke the exact fallback already captured by an admitted E batch. Cancellation, owner/fence loss, or uncertainty does close its new-source-use gate.
3. Lease ownership remains only `FREE|PINNED(L)`; acquisition, cancellation, attempts, decode, and quarantine are ordinary batch-lifecycle predicates, not new `SlotLeaseWord` phases.

No recommendation requires an ADR amendment. Any forced clear, widened P4 transfer, direct slot-to-delete authority, split lease state, or weaker memory-order contract would require one.

## Q34.1 — Reader/publication/reclaimer race

Accept B, tightened with a stable-scan contract.

The reader order is:

```text
acquire current G
claim fresh L
publish exact lease-bound {Binding,G}
StoreLoad/full fence
acquire current generation/authority again
stable-capture a G-tagged publication cell
dereference G only after both validations succeed
```

The retiring publisher/reclaimer order is:

```text
release/volatile publish current = G+1
scan every slot that can serve this Binding
wait/retry every exact {Binding,G} or INCONCLUSIVE slot
retire owner-local G only after a complete clean scan
```

The two sufficient interleavings are exact:

- Hazard publication precedes the scan’s stable observation: the scanner observes exact `{Binding,G}` and cannot retire G.
- The scanner observes the slot before hazard publication: after publishing, the reader’s StoreLoad-ordered generation recheck observes G+1 and rejects before dereferencing G.

There is no safe third case based on elapsed time, cache observation, grace periods, host scheduling, or “probably no reader.”

Slot payload rules:

- If claiming `PINNED(L)` and publishing `{Binding,G}` are separate physical operations, a scanner that sees L without a complete payload associated with exact L must return `INCONCLUSIVE`.
- A pinned payload may be treated as nonmatching only after a stable lease/payload/lease observation, or an equivalent atomic/seqlock publication.
- Terminal `CAS(L -> FREE)` is the hazard-removal cut. Source drain happens-before that release/acq-rel CAS.
- Stale payload may remain after FREE because scanners exact-match L. An old owner must not blindly clear payload after freeing the slot and thereby overwrite a new lease’s payload.
- Any optional payload scrub must occur while exact L is still owned, or use an exact ownership-safe mechanism; physical scrubbing is not part of correctness.

Memory ordering is frozen semantically:

- current-generation publication is release/volatile;
- hazard publication plus the following pointer load supplies StoreLoad ordering; release-store plus acquire-load alone is insufficient;
- reader generation/cell loads and reclaimer slot scans are acquire/volatile;
- terminal FREE publication is at least release.

A global instantaneous array snapshot is unnecessary. The pool membership being scanned must be stable, and every slot must yield stable `FREE`, stable exact payload, or `INCONCLUSIVE`. One complete clean post-swap scan is sufficient for owner-local G retirement; a mandatory second scan or grace interval would be overdesign. If blockers were seen, the implementation must later prove they cleared through stable reread, a whole rescan, or an equivalent formally tracked scan—it cannot retire from the stale first observation.

One scan never releases durable source protection or authorizes physical deletion.

Tradeoff: the stable per-slot check slightly increases low-frequency swap scanning, but avoids a refcount or reclaimer handshake on every read.

Frozen: the two-case proof, StoreLoad cut, exact payload matching, conservative inconclusive result, and post-swap scan. OPEN: payload/reference/seqlock layout, exact VarHandles, scan sharding, stable pool-membership mechanism, retry strategy, padding, and scan cadence. ADR amendment: none.

## Q34.2 — Recheck versus tagged publication-cell switch

Accept B.

The generation recheck is necessary but is not the admission linearization point. Admission linearizes only at one stable acquire capture of a publication cell tagged with exact G, after the authority/generation recheck.

Outcomes are deterministic:

- Stable cell tag G: accept G.
- Stable cell tag G+1: reject and retry before source use.
- Torn/unstable seqlock cell: retry.
- Pointer switches after stable G capture: continue under G; the visible hazard protects it.
- Pointer switches after generation recheck but before cell capture: accept only if the actual stable cell obtained is G; otherwise retry.

Selector, admission epoch, owner, Storage, protocol, deadline/capability, and source-generation facts need not be physically deep-copied into one object. They must be either:

- one immutable authority reference; or
- an exact tuple whose version/digest/seqlock validation proves all components belonged to one authority state.

An ad hoc collection of separately cached fields is insufficient. At minimum the stable authority binds Binding/incarnation, Storage/Position Domain, selected-view identity, owner fence, `ReadAdmissionEpoch`, `ADMITTING`, source generation G, protocol fence/root identity, and applicable capability/deadline generation. The tagged cell binds G to the frontier, active-tail/index view, and protocol-state references it exposes.

This prevents G locators or source state from being paired with a G+1 frontier without synchronizing each read with the publisher or copying a graph.

Tradeoff: a single immutable reference is simpler; a seqlock/exact tuple can reduce object churn or fit existing publication structures but costs extra loads/retries. Evidence must choose.

Frozen: stable exact authority and generation-tagged cell coherence. OPEN: single reference versus seqlock, tuple layout, publication-cell contents, and retry thresholds. ADR amendment: none.

## Q34.3 — Admission racing fused `PWF E -> PO E+1`

Accept B.

The race is between read admission under the exact cached selector authority and the fused selector CAS:

- **Read stabilizes E first:** its exact G/E slot and captured fallback remain valid through terminal drain. The later closure may publish PO/E+1, but it cannot rewrite the slot, revoke the captured fallback, or release its protection.
- **Fused closure wins first:** it closes E, freezes `sharedLast=E`, grants PO/E+1, and persists the closure anchor. The reader’s exact authority recheck fails, so it clears L and may recapture E+1.
- **Selector-CAS response unknown:** E admission closes locally until exact reread establishes successor equality, predecessor equality for a fenced retry, or a conflicting state requiring recomputation.

The control path must publish/close the local E admission authority consistently with the durable selector result. It may not leave a permissive stale local reference after a successful or unknown closing CAS.

Four cuts remain separate:

1. Local drain of current-owner slots naming fallback-bearing G/E.
2. Durable terminal/quiescence proof for every relevant old-owner Read Admission Epoch.
3. Exact source-protection-generation release.
4. Revalidation and physical deletion.

The first is owner-local M4 evidence. It is neither durable old-owner proof nor permission for cuts 3 or 4. Closure performs no per-read ACK, no slot mutation, and no owner×read metadata update.

Tradeoff: whole-epoch retention may preserve fallback longer, but avoids per-read durable writes and selector-slot coordination.

Frozen: selector competition, read-wins/closure-wins behavior, response-unknown admission stop, and separation of the four cuts. OPEN: local authority publication mechanics and the Round 35 M4/M5 receipt/writer boundary. ADR amendment: none.

## Q34.4 — Cancellation, authority loss, callback ABA, and terminal use

Accept B.

Cancellation, deadline, or actual owner/fence loss first closes the batch-local new-source-use gate. It then requests provider cancellation where applicable. It never clears L.

The gate and source-use acquisition must linearize so each proposed use is exactly one of:

- acquired before closure and included in terminal drain; or
- rejected after closure and never issued.

For owner/event-loop serialization this is an ordered local decision. For outstanding-use accounting it requires an atomic gate/count relationship outside `SlotLeaseWord`.

Terminal ordering is:

1. Close new source use.
2. Stop/reject retry and fallback scheduling.
3. Obtain provider completion or cancellation acknowledgement that proves real source-access termination.
4. Finish fallback/decode and release every source-specific pin.
5. Release every source-backed buffer, including transport retention.
6. Let the unique terminal owner perform `CAS(exact L -> FREE)`.

A cancelled future or timeout without provider access-termination semantics is not step 3.

Callbacks:

- acquire/equality-check exact L;
- on mismatch, make no slot or batch mutation and increment only sampled telemetry;
- on match, still validate their ordinary attempt/batch state.

That latter check matters for a primary callback arriving during the same L after its attempt was closed; L prevents cross-lease ABA, while the ordinary attempt identity prevents within-batch attempt confusion. Provider phases must not be encoded in `SlotLeaseWord`.

Cause precedence:

1. Already externally committed protocol state cannot be rewritten.
2. Authority/fence loss vetoes an otherwise uncommitted stale success.
3. Otherwise the first serialized terminal decision between accepted provider result and cancel/deadline wins.
4. Round 33’s primary/fallback rule applies when both attempts fail: primary outward, fallback and cleanup errors suppressed.
5. Cleanup failure may be suppressed as an outward cause but still blocks lease release whenever termination is uncertain.

Tradeoff: complete drain can extend slot lifetime after the client has already seen cancellation, but clearing earlier would permit use-after-reclamation.

Frozen: gate closure, exact-L callbacks, terminal predicate, and cause hierarchy. OPEN: provider cancellation mappings, attempt-state representation, transport-buffer accounting, and protocol-specific response-commit cuts. ADR amendment: none.

## Q34.5 — One-shot fallback pin-transfer race

Accept B.

The outer `{Binding,G}/L` lease never transfers. Inner source use changes only after:

```text
primary attempt/access ends
all partial primary results and buffers are released
primary source-specific pin is released
cached exact attempt/G/owner fence is rechecked
secondary source use is acquired through the same open gate
```

Race outcomes:

- Cancel or owner/fence loss wins before secondary acquisition: secondary is not issued.
- Secondary acquisition wins first: it is counted as issued use and later cancellation must drain or terminate it.
- Same-owner fused PWF→PO closure occurs between attempts: it does not revoke the exact fallback captured by admitted E. The secondary remains legal if the captured owner/deadline and protection facts remain valid.
- Owner change, selector uncertainty, expiry, or actual authority loss occurs between attempts: close the gate and reject secondary acquisition.
- Primary cleanup error with proven source/pin termination: record/suppress it and fallback may proceed.
- Primary cleanup with uncertain termination or pin release: do not acquire the secondary; retain L and safe-fail/quarantine as necessary.
- Late primary notification after proven termination: equality-check L, then no-op against the closed primary-attempt identity.
- Late fallback callback after slot reuse: L mismatch, no-op.
- Any external observability from the affected purity unit forbids fallback.

P4 retains ownership of its inner transfer and native deletion pin. M4 must not wrap it in another source-pin machine. No current accepted source contract requires simultaneous primary and secondary pins; adding one would need explicit source-specific proof and should not become a generic M4 feature.

The original cumulative retry/time/byte budget is never reset.

Tradeoff: sequential pin transfer may add fallback latency, but avoids double-resource retention and deletion races.

Frozen: continuous outer lease, release-before-acquire, one transfer, same budget, and observability cut. OPEN: non-P4 inner pin API, exact budgets, and buffer-release mechanics. ADR amendment: none; changing P4 requires its ADR/evidence amendment.

## Q34.6 — Provider nonresponse, process close/crash, and quarantine

Accept B.

A provider whose termination cannot be proved leaves exact L pinned. Quarantine is accounting/health state outside `SlotLeaseWord`; it consumes bounded hard capacity. Exhaustion stops admission before source I/O.

Planned close:

1. stop new admission;
2. close every batch’s new-use gate;
3. request cancellation;
4. drain providers, attempts, decode, and buffers;
5. clear only exact leases that reach the terminal predicate.

If close cannot drain a provider, it cannot publish planned-drain quiescence for that epoch. The process may eventually be terminated, but that is not a local force-clear or durable protection-release proof.

Crash destroys volatile local memory. Recovery must use the accepted terminal/capability path, including planned proof or qualified expiry. An empty new pool, missing old process, TCP close, generation switch, watchdog, operator command, or host-local timeout does not qualify.

Liveness guarantees are deliberately bounded:

- slot search/admission retry terminates with success, backpressure, or typed safe failure before I/O;
- memory, pool, and quarantine capacity are bounded;
- a responsive provider permits eventual terminal clear and planned drain;
- an unresponsive provider may indefinitely retain a slot, generation, source protection, and storage;
- protection release and physical deletion are not guaranteed when durable quiescence cannot be proved.

A configured `maxSourceAccessLifetime` used by qualified-expiry evidence is an admission/capability contract, not permission for the live process to clear an unresponsive lease after a timer.

Tradeoff: availability and storage reclamation may stop under provider failure. That is preferable to an unsafe recovery escape hatch.

Frozen: no force-clear and conditional liveness. OPEN: capacity/lifetime limits, admission policy, close deadlines, provider termination receipts, and quarantine telemetry. ADR amendment: none.

## Q34.7 — Physical implementation family

Accept B. Do not choose a winner in prose and do not combine their costs.

The smallest semantic interface—logical, not a required Java API—is:

```text
reserve/publish exact hazard(Binding,G) -> L
tryAcquireSourceUse(L, attemptIdentity) -> accepted/rejected
releaseSourceUse(L, attemptIdentity)
closeNewSourceUse(L, terminalCause)
terminalClearExactLWhenDrained()
```

A logical source-use permit spans every operation that may touch the protected source, including provider issue, retry/fallback, decode, and retained source-backed buffers. It need not allocate an object.

Two implementation families remain mutually exclusive for one batch/pool:

| Family | Mechanism | Main cost/risk |
|---|---|---|
| Owner/event-loop serialization | All gate, start, completion, and buffer-retention cuts run on one owner; plain local accounting | executor hops, queue latency, migration/close ordering |
| `tryAcquireSourceUse` plus outstanding accounting | Gate and use count linearize across callback threads | atomic RMW/cache-line traffic per use and more cancellation contention |

Combining executor serialization with atomic use accounting would pay both costs without adding safety. A generic provider/fallback/decode state machine in the slot is also rejected.

Selection evidence must include:

- allocation count per read and callback;
- atomic RMW, equality-load, and fence counts;
- slot acquire/publish/clear latency;
- executor-hop versus outstanding-accounting p50/p99/p999;
- cache-line invalidations, false sharing, L1/LLC misses, and scaling by readers/core;
- capture retries during G switches;
- pool occupancy, fairness/skew, scan duration, and drain p99;
- cancellation latency, provider nonresponse, quarantined slots, and late-callback samples;
- hot-cache, cold-Object, fallback, and multi-Binding workloads.

Frozen: semantic interface and prohibition on mixed mechanisms/slot state-machine expansion. OPEN: which family wins, whether different separately evidenced runtimes use different families, and all physical fields/atomics. ADR amendment: none; ADR 0072 intentionally leaves this evidence-selected.

## Q34.8 — Slot/protection/delete boundary

Accept B, with milestone ownership left explicitly for Round 35.

There is no direct slot-versus-delete CAS. The required chain is:

```text
fused durable PWF E -> PO E+1 selector publication
  -> current-owner fallback-bearing slot drain
  -> exact terminal cut and contiguous capability-qualified old-epoch proof
  -> exact source-protection-generation release CAS
  -> M5 revalidation and physical deletion
```

A local scan proves only that the scanned owner-local pool no longer protects G. It cannot cover a crashed owner, an earlier Read Admission Epoch, a missing proof-window interval, another source-protection generation, or provider deletion eligibility.

M4 must at least produce and prove:

- correct hazard admission and complete terminal drain;
- a trustworthy local drained-through read-view cut;
- absence of current-owner fallback-bearing slots at the relevant cut;
- read-side capability/epoch identities needed by the closed verifier;
- proof-window/head/fold candidates and measurements currently assigned to M4 scope.

M5 owns, under the current index:

- exact source-protection release execution;
- release-response reconciliation;
- final source/provider revalidation;
- physical deletion, orphan/GC execution, and recovery.

Round 35 must explicitly freeze who writes terminal cuts/proofs/folds, which receipt owns contiguous historical coverage, and which module hosts the closed release-precondition verifier. Those are real M4/M5 ownership ambiguities; this round must not silently assign them.

Pulsar P4’s native BK pin/delete race remains the separate accepted M2 authority and is not routed through this Object-WAL M5 chain.

Frozen: the downstream predicate order and absence of direct slot-delete authority. OPEN: the cross-milestone writer/receipt boundary. ADR amendment: none unless milestone ownership changes accepted semantic authority rather than merely locating an implementation.

## Q34.9 — Canonical state, race, and evidence matrices

The following phases do not add `SlotLeaseWord` states.

### State matrix

| Logical phase | `SlotLeaseWord` | Hazard payload / gate / use | Only legal next cut |
|---|---|---|---|
| Available | `FREE` | No authoritative payload or source use | Acquire fresh nonzero L |
| Claimed, capture not accepted | `PINNED(L)` | Payload exact or temporarily inconclusive; gate closed; no source I/O | Publish/recheck/capture, or terminal-clear |
| Accepted | `PINNED(L)` | Stable exact `{Binding,G}`; gate open; every use accounted | Start counted use or close gate |
| Draining | `PINNED(L)` | Gate closed; issued use, attempt, decode, pin, or buffer remains | Complete/release remaining use |
| Terminal-ready | `PINNED(L)` | Gate closed; termination proved; zero remaining source lifetime | Unique `CAS(L -> FREE)` |
| Quarantined | `PINNED(L)` | Gate closed but termination cannot be proved | Remain pinned until real termination or process destruction |
| Reuse-wrap | Slot removed from pool | L would repeat | Permanently retire slot; no online rebase |

### Race/evidence matrix

| Race / boundary | Frozen outcome | Failure or quarantine behavior | Canonical subcase |
|---|---|---|---|
| Hazard publish before retire scan | Scan sees exact G and waits | No retirement | `V2-READ-005/hazard-publish-before-retire-scan` |
| Post-swap scan before hazard publish | Reader’s fenced recheck sees G+1 and rejects before dereference | Clear exact L; bounded retry | `V2-READ-005/retire-scan-before-hazard-publish` |
| Scan meets claimed but unstably published payload | `INCONCLUSIVE`, never “absent” | Retry/wait; stuck claimant can quarantine capacity | `V2-READ-005/lease-payload-stable-scan` |
| Pointer switches after generation recheck and stable G cell is captured | Accepted G survives; visible slot protects it | Later retirement waits | `V2-READ-004/admitted-view-survives-generation-switch` |
| Cell tag is G+1 or seqlock/reference read is torn/unstable | No source use; clear/retry | Safe failure after admission budget | `V2-READ-005/tagged-cell-mismatch-or-torn-capture` |
| Fused PO/E+1 closure wins before admission stabilizes | E read rejected; recapture E+1 | No E slot/source use | `V2-READ-001/fused-closure-wins-read-admission` |
| E read stabilizes before fused closure | Slot/view retains fallback through terminal drain | Protection retained until downstream predicates | `V2-READ-001/read-admission-wins-fused-closure` |
| Selector CAS response unknown | Stop E admission until exact convergence | Retain fallback/protection | `V2-READ-001/selector-unknown-stops-admission` |
| Cancel before primary acquisition | Gate closes; no provider use | Drain zero-use lease, exact clear | `V2-READ-007/cancel-before-source-use` |
| Cancel during primary | Gate closes; cancel issued; wait for real termination and buffers | Nonresponse quarantines L | `V2-READ-007/cancel-during-provider-use` |
| Cancel between primary release and secondary acquisition | If cancel wins, secondary rejected; if acquisition wins, secondary is counted and drained | No unaccounted attempt | `V2-READ-007/cancel-between-primary-and-fallback` |
| Same-owner fused closure between attempts | Captured E fallback remains usable under valid owner/deadline | Later protection release waits | `V2-READ-001/admitted-fallback-survives-closure` |
| Owner/fence loss between attempts | New-use gate closes; no secondary acquisition | Safe failure; L drains | `V2-READ-007/authority-loss-between-attempts` |
| Late callback after slot reuse | Exact L mismatch, sampled no-op | No mutation of new batch | `V2-READ-007/late-callback-after-slot-reuse-aba` |
| Lease generation wrap | Slot retires permanently | Capacity reduces/backpressures; no reuse | `V2-READ-007/lease-generation-wrap-retires-slot` |
| Provider never terminates | L remains pinned | Bounded quarantine; eventual admission backpressure | `V2-READ-007/provider-nontermination-quarantines-capacity` |
| Planned pool close | Stop admission and drain; clear only terminal-ready leases | Incomplete close cannot issue planned-drain proof | `V2-READ-007/pool-close-no-force-clear` |
| Process crash / empty replacement pool | Local memory disappears but proves no durable quiescence | Retain protection pending accepted proof/expiry | `V2-READ-001/process-crash-is-not-quiescence` |
| Multi-Binding partial slot reservation | Release all partial leases before I/O; retry/fail/split legally | Never wait while holding a partial set | `V2-READ-005/multi-binding-all-or-release` |
| Fallback completes but decode/transport retains source buffer | Outer L stays pinned until final release | Leak/nonresponse quarantines capacity | `V2-READ-004/fallback-decode-buffer-lifetime` |
| Primary cleanup cannot prove source/pin termination | Secondary is not acquired | Primary outward; cleanup suppressed; L quarantined if needed | `V2-READ-007/uncertain-primary-cleanup-blocks-transfer` |
| Early protection release or physical delete attempted from local slot scan | Reject; require full durable downstream predicate chain | Retain/quarantine; no delete | `V2-READ-001/early-protection-or-delete-rejected` |
| Pulsar P4 BK deletion races native source pin | Existing P4 fence/drain/CAS order applies | Native deletion waits or fails safely | `V2-BK-010/source-pin-delete-race` |

These are subcases under existing canonical IDs, not new registry IDs or evidence-status claims.

Safety invariants:

- No generation-owned state is dereferenced before exact hazard publication, fenced generation recheck, and stable matching cell capture.
- No owner-local G retirement occurs while a matching or inconclusive slot exists.
- No L is cleared while any future or issued source use, inner pin, decode, or source-backed buffer may remain.
- No late callback mutates a different L.
- No fallback transfer overlaps uncertain primary source use or crosses the observability cut.
- No local drain directly releases protection or authorizes deletion.

Bounded-liveness invariants:

- Admission and capture retry are bounded and fail/backpressure before I/O.
- Pool, lease, quarantine, and retained-source liability are bounded.
- Responsive providers permit drain; nonresponsive providers may block capacity and reclamation indefinitely.
- Safety is guaranteed; eventual read success, protection release, and physical deletion are conditional, not guaranteed.

Normal-path budget target per Binding batch:

- one fresh `FREE -> PINNED(L)` acquisition;
- one exact hazard payload publication;
- one StoreLoad/full fence;
- two authority/generation acquire loads;
- one small stable tagged-cell capture;
- equality loads during callbacks/source use;
- one unique terminal `CAS(L -> FREE)`.

The owner-serialization family should add no source-use RMW. The outstanding-accounting family may add source-use RMWs only if evidence justifies their cache-line cost. Both forbid per-read heap snapshots, process-global refcounts/tickets, per-callback slot CAS, and ordinary-read remote metadata I/O.

Required evidence metrics include allocations/read, RMW/load/fence counts, publish/clear cycles, cache-line invalidations and false sharing, pool occupancy/fairness, scan duration, capture retry rate, swap-drain p99, cancellation p99, provider termination, quarantined slots, late-callback samples, fallback transfer/buffer lifetime, multi-Binding rollback cost, retained-protection bytes/age, and hot-cache versus cold-Object latency.

OPEN physical/evidence choices remain:

- single-reference versus seqlock authority/cell;
- exact lease-bound payload publication and stable-scan layout;
- VarHandle modes and full-fence realization;
- pool sharding, size, padding, fairness, and stable scan membership;
- lease packing/allocation and wrap test thresholds;
- owner serialization versus outstanding-use accounting;
- scan cadence, retry/backoff, and drain scheduling;
- provider cancel/termination mappings and attempt identity;
- transport-buffer accounting and response-observability cut;
- admission, source-lifetime, quarantine, and retained-storage numeric caps;
- M4/M5 terminal/proof/fold writer and receipt ownership for Round 35.

## Round 34 conclusion

The hazard-slot and reclamation frontier is closed enough to synchronize as M4 design input. Recommendations B are accepted with the stable-scan, same-owner-closure, and lease-bound-payload clarifications above. The design proves the classic race without a refcount, second reclaimer handshake, per-read ACK, grace timer, slot state machine, or forced-clear path, while preserving the intended one-acquire/one-fence/one-terminal-CAS hot-path shape.

Physical layouts, implementation-family selection, numeric caps, provider termination mappings, proof-window representation, and the M4/M5 durable writer/receipt boundary remain explicitly OPEN. `V2-READ-001/004/005/007` remain `PLANNED`; `V2-BK-010` retains only its existing M2 `PASSED_CURRENT_SOURCE` scope. This round creates no implementation, test result, receipt, scenario promotion, M4 Final, protection release, or physical deletion authority.

<!-- END FIXED REVIEWER RESPONSE -->

## Synchronized accepted cuts

- The scanner exact-matches lease-bound payload and treats an unstable claimed slot as `INCONCLUSIVE`.
- One complete clean post-swap scan is sufficient only for owner-local generation retirement; it never proves durable
  quiescence or deletion.
- A same-owner fused closure cannot revoke fallback already captured by a successfully admitted E batch.
- Cancellation closes one batch-local new-source-use gate and all terminal paths converge on the exact-L drain.
- `SlotLeaseWord` keeps only `FREE|PINNED(L)` ownership; attempts, phases, quarantine, and counts live outside it.
- The two physical lifecycle families remain mutually exclusive, evidence-selected choices.

## Still OPEN

The physical cell/payload representation, VarHandle realization, sharding/padding/fairness, lease packing, physical
lifecycle family, provider cancellation mapping, response-commit cut, numeric caps, proof-window representation, and
M4/M5 durable writer/receipt boundary remain OPEN. This note creates no implementation, receipt, scenario PASS, M4
Final, protection release, or physical-deletion authority.
