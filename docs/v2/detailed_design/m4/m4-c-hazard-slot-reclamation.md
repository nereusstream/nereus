---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M4-C hazard-slot, terminal drain, and reclamation races

## Status and authority

This document synchronizes the accepted result of
[M4 Grill 34](../../grill-notes/34-m4-hazard-slot-reclamation-races.md) with ADRs 0069, 0070, 0072, 0075, and 0077
and M4-A/M4-B. Those accepted authorities remain higher. M4-C freezes no physical layout, numeric threshold,
provider-specific cancellation mapping, or M4/M5 durable writer boundary. No new ADR is required.

This is design input only. It creates no implementation, test result, receipt, scenario promotion, M4 Final,
protection release, or physical-deletion authority.

## Reader and reclaimer orders

The reader uses this semantic order:

```text
acquire current G
claim fresh nonzero lease L
publish exact lease-bound {Binding,G}
StoreLoad/full fence
acquire and exactly revalidate the current authority/G
stable-capture one G-tagged publication cell
dereference G only after both validations succeed
```

The retiring publisher/reclaimer uses:

```text
release/volatile publish current = G+1
stably scan every slot that can serve the Binding
wait or retry every exact {Binding,G} and every INCONCLUSIVE slot
retire owner-local G only after one complete clean post-swap scan
```

The proof has only two sufficient cases. If hazard publication precedes the scan's stable observation, the scan sees G
and waits. If the scan observes the slot before hazard publication, the reader publishes the hazard and its
StoreLoad-ordered recheck observes G+1, then rejects before dereference. Time, grace, cache absence, scheduling, or a
second best-effort scan cannot supply another correctness case.

A pool scan need not be one global instantaneous array snapshot, but scan membership must be stable and each member
must yield exactly one of stable `FREE`, stable exact lease-bound payload, or `INCONCLUSIVE`. If claiming L and
publishing the payload are distinct, `PINNED(L)` with no stably associated complete payload is `INCONCLUSIVE`, never
absence. A pinned payload is nonmatching only after a stable lease/payload/lease observation or an equivalent atomic
or seqlock cut.

One complete clean post-swap scan is sufficient only for owner-local G retirement. When a blocker was observed, later
retirement requires a stable reread, complete rescan, or equivalent formally tracked scan proving it cleared. No scan
releases durable source protection or authorizes physical deletion.

The semantic memory-order floor is:

- current-generation publication is release/volatile;
- hazard publication and the following pointer load provide StoreLoad/full-fence ordering; release-store plus
  acquire-load alone is insufficient;
- reader authority/cell loads and reclaimer slot scans are acquire/volatile; and
- source drain happens-before the release or acquire-release terminal `CAS(exact L -> FREE)`.

Payload bytes may remain stale after FREE because every scan exact-matches L. An old lease cannot scrub after FREE and
overwrite a successor payload. Optional scrubbing occurs while exact L is still owned or through another
ownership-safe mechanism and is not a correctness prerequisite.

## Stable authority and tagged cell

The generation recheck is necessary but is not the admission linearization point. Admission linearizes only at the
stable acquire capture of one exact G-tagged publication cell after the authority/G recheck:

| Cell result | Outcome |
| --- | --- |
| stable tag G | accept G |
| stable tag G+1 or any other generation | clear exact L and bounded-retry before source use |
| torn or unstable cell | clear exact L and bounded-retry before source use |
| current pointer switches after stable G capture | continue under captured G; visible slot protects it |

The selector, admission, owner, Storage, protocol, deadline/capability, and G facts are either one immutable authority
reference or one exact version/digest/seqlock-validated tuple. A loose set of independently cached fields is invalid.
The stable authority binds at least Binding/incarnation, Storage/Position Domain, selected view, Owner fence,
`ReadAdmissionEpoch`, `ADMITTING`, G, protocol fence/root, and applicable capability/deadline generation. The tagged
cell binds G to its exposed frontier, active-tail/index, and protocol-state references. This prevents G source state
from being paired with a G+1 frontier without deep-copying the graph or synchronizing every reader with the publisher.

## Fused closure race

Admission under `PREFERRED_WITH_FALLBACK(O,E)` and the fused `PWF E -> PO E+1` selector CAS compete through the exact
accepted selector authority:

- if the read stabilizes E first, its exact G/E lease and captured fallback remain valid through terminal drain; a
  later same-owner closure cannot rewrite the slot, revoke that fallback, or release its protection;
- if fused closure wins first, it closes E, freezes `sharedLast=E`, grants PO/E+1, and persists the closure anchor;
  the read's exact authority recheck fails before source use and it may recapture E+1; and
- on selector-CAS response unknown, local E admission remains closed until exact reread proves successor equality,
  predecessor equality for a fenced retry, or conflict requiring recomputation.

Successful or unknown closure cannot leave a permissive stale local E authority. Closure mutates no read slot and
writes no per-read ACK. Same-owner closure alone does not close an admitted batch's new-use gate; cancellation,
deadline, actual owner/fence loss, selector uncertainty, or expiry does.

The following predicates remain distinct and ordered:

1. owner-local drain of current slots that can name fallback-bearing G/E;
2. durable terminal/quiescence proof for every relevant historical Read Admission Epoch;
3. exact source-protection-generation release; and
4. provider/source revalidation and physical deletion.

The local scan/drain proves only predicate 1.

## Slot ownership and batch lifecycle

`SlotLeaseWord` has exactly two ownership values:

```text
FREE | PINNED(L)
```

The following logical phases do not add atomic slot states:

| Logical phase | Lease word | Ordinary batch facts | Legal next cut |
| --- | --- | --- | --- |
| available | `FREE` | no authoritative payload/use | acquire fresh nonzero L |
| claimed, capture pending | `PINNED(L)` | payload exact or temporarily inconclusive; gate closed; no I/O | finish capture or terminal-clear |
| accepted | `PINNED(L)` | stable exact payload; gate open; all use accounted | acquire counted use or close gate |
| draining | `PINNED(L)` | gate closed; some attempt/decode/pin/buffer remains | finish or release remaining use |
| terminal-ready | `PINNED(L)` | gate closed; termination proved; zero source lifetime | unique exact-L terminal CAS |
| quarantined | `PINNED(L)` | gate closed; termination unproved | stay pinned until real termination or process destruction |
| reuse wrap | removed from pool | next L would repeat | retire slot permanently; no online rebase |

Cancellation, deadline, or actual authority loss first closes the batch-local new-source-use gate and then requests
provider cancellation where supported. Use acquisition and gate closure linearize so a proposed use was either
accepted before closure and included in drain or rejected and never issued. The gate/count relationship is ordinary
batch state outside `SlotLeaseWord`.

Terminal order is exact:

1. close new source use;
2. reject new retry/fallback scheduling;
3. receive provider completion or cancellation acknowledgement proving real source-access termination;
4. finish fallback/decode and release every source-specific pin;
5. release every source-backed buffer, including transport retention; and
6. let the unique terminal owner execute `CAS(exact L -> FREE)`.

A cancelled future or elapsed timeout is not provider termination. Callbacks acquire/equality-check L, make no slot or
batch mutation after mismatch, and record only sampled telemetry. After an L match they still validate ordinary
batch/attempt identity, which prevents a closed primary attempt's callback from affecting a later fallback attempt
within the same lease.

Externally committed protocol state is never rewritten. Authority/fence loss vetoes an uncommitted stale success;
otherwise the first serialized accepted-provider-result versus cancel/deadline terminal decision wins. Round 33's
primary/fallback cause precedence remains unchanged. A cleanup error may be outwardly suppressed yet still block
terminal clear whenever source termination is uncertain.

## Fallback transfer under one outer lease

The outer `{Binding,G}/L` lease never transfers. One inner source transfer follows:

```text
end primary attempt/access
release every partial primary result/buffer
release the primary source-specific pin
recheck cached exact attempt/G/owner fence
acquire secondary use through the same open gate
```

If cancel or owner/fence loss wins before secondary acquisition, secondary is not issued. If acquisition wins first,
it is counted and must drain. Same-owner fused closure between attempts preserves the fallback captured by admitted E
while exact owner/deadline/protection facts remain valid; owner change, uncertainty, expiry, or actual authority loss
closes the gate. Proven primary cleanup failure may be recorded/suppressed and permit fallback, but unproved source or
pin termination blocks secondary acquisition and may quarantine L. Late notifications must pass both exact-L and
attempt checks. Any external observability for the affected purity unit forbids transfer.

Pulsar P4 owns its existing inner pin transfer and deletion race. M4 adds no wrapper pin machine or generic simultaneous
double-pin feature. The one-shot fallback keeps the original cumulative byte/time/retry budget.

## Nonresponse, close, crash, and liveness

Unproved provider termination leaves L pinned. Quarantine is bounded accounting/health state outside the lease word,
and exhaustion backpressures before source I/O. Planned close stops admission, closes every batch gate, requests
cancellation, drains all provider/attempt/decode/buffer use, and clears only terminal-ready exact leases. It cannot
publish planned-drain proof if any provider fails to drain.

Process termination destroys volatile local memory but is not a force-clear or durable quiescence proof. Recovery uses
the accepted planned-drain or capability-qualified expiry path. A replacement empty pool, missing process, TCP close,
watchdog, generation switch, operator action, or host-local timer never qualifies. `maxSourceAccessLifetime` is a
durable admission/capability fact, not a live-lease timeout escape.

Admission retry, memory, pool, and quarantine are bounded. Responsive providers permit eventual terminal clear.
Unresponsive providers may indefinitely retain slot, generation, protection, and storage. Eventual read success,
protection release, and deletion are conditional; safety is unconditional.

## Evidence-selected implementation family

The logical, non-API interface is:

```text
reserve/publish exact hazard(Binding,G) -> L
tryAcquireSourceUse(L, attemptIdentity) -> accepted/rejected
releaseSourceUse(L, attemptIdentity)
closeNewSourceUse(L, terminalCause)
terminalClearExactLWhenDrained()
```

A use permit spans every action that may touch protected source state, including issue, retry/fallback, decode, and
retained source-backed buffers, without requiring a heap permit object.

Exactly one physical family is selected per evidenced runtime/pool:

| Family | Mechanism | Principal cost/risk |
| --- | --- | --- |
| owner/event-loop serialization | serialize gate, start, completion, and buffer-retention cuts; plain local accounting | executor hops, queue latency, migration/close order |
| `tryAcquireSourceUse` plus outstanding accounting | atomically linearize gate and use count across callback threads | RMW/cache-line traffic per use and cancellation contention |

Combining both pays both costs without adding safety and is rejected. Provider/fallback/decode phases never become a
generic slot state machine. Implementation evidence selects the family and may select different families only for
separately evidenced runtimes.

## Canonical race subcases

These names are subcases under existing IDs, not registry rows or evidence claims.

| Boundary | Required outcome | Canonical subcase |
| --- | --- | --- |
| hazard publish before scan | exact G blocks retirement | `V2-READ-005/hazard-publish-before-retire-scan` |
| post-swap scan before publish | fenced recheck rejects before dereference | `V2-READ-005/retire-scan-before-hazard-publish` |
| claimed slot with unstable payload | scan yields `INCONCLUSIVE` and waits/retries | `V2-READ-005/lease-payload-stable-scan` |
| switch after stable G capture | admitted G survives and remains pinned | `V2-READ-004/admitted-view-survives-generation-switch` |
| mismatched/torn tagged cell | no source use; bounded retry/failure | `V2-READ-005/tagged-cell-mismatch-or-torn-capture` |
| fused closure wins | reject E and recapture E+1 | `V2-READ-001/fused-closure-wins-read-admission` |
| E admission wins | fallback remains valid through terminal drain | `V2-READ-001/read-admission-wins-fused-closure` |
| selector response unknown | stop E admission; retain protection | `V2-READ-001/selector-unknown-stops-admission` |
| cancel before source use | no provider use; exact clear after zero-use drain | `V2-READ-007/cancel-before-source-use` |
| cancel during provider use | wait for real termination and buffer drain | `V2-READ-007/cancel-during-provider-use` |
| cancel between attempts | cancel or counted-secondary acquisition wins exactly once | `V2-READ-007/cancel-between-primary-and-fallback` |
| same-owner closure between attempts | admitted fallback survives closure | `V2-READ-001/admitted-fallback-survives-closure` |
| authority loss between attempts | no secondary; safe-fail and drain | `V2-READ-007/authority-loss-between-attempts` |
| callback after lease reuse | exact-L mismatch sampled no-op | `V2-READ-007/late-callback-after-slot-reuse-aba` |
| lease reuse wrap | retire slot and reduce capacity | `V2-READ-007/lease-generation-wrap-retires-slot` |
| provider nontermination | exact L stays quarantined | `V2-READ-007/provider-nontermination-quarantines-capacity` |
| planned pool close | drain only; never force-clear | `V2-READ-007/pool-close-no-force-clear` |
| process crash | local loss is not durable quiescence | `V2-READ-001/process-crash-is-not-quiescence` |
| multi-Binding partial reserve | release all before I/O | `V2-READ-005/multi-binding-all-or-release` |
| decode/transport retains fallback buffer | outer L remains until final release | `V2-READ-004/fallback-decode-buffer-lifetime` |
| uncertain primary cleanup | block secondary and quarantine as needed | `V2-READ-007/uncertain-primary-cleanup-blocks-transfer` |
| release/delete from local scan | reject; require complete durable chain | `V2-READ-001/early-protection-or-delete-rejected` |
| native P4 source-pin deletion | retain existing P4 authority | `V2-BK-010/source-pin-delete-race` |

## Downstream protection/deletion boundary

There is no slot-versus-delete CAS. Object-WAL deletion requires:

```text
fused durable PWF E -> PO E+1
  -> current-owner fallback-bearing slot drain
  -> exact terminal cut and contiguous capability-qualified historical proof
  -> exact source-protection-generation release CAS
  -> M5 final revalidation and physical deletion
```

M4 proves hazard admission, terminal drain, local drained-through cuts, absence of current-owner fallback-bearing
slots, read-side capability/epoch identities, and proof-window/head/fold candidates/measurements. The existing index
assigns protection-release execution/reconciliation and final provider deletion/recovery to M5. Round 35 freezes the
terminal/proof/fold writer, contiguous-coverage receipt, and closed release-precondition verifier ownership; M4-C does
not preselect them. P4 remains a separate M2 native authority.

## Performance and OPEN choices

The normal target remains one `FREE -> PINNED(L)` acquisition, one payload publication, one StoreLoad/full fence, two
authority/G acquire loads, one small stable tagged-cell capture, equality reads during use/callbacks, and one exact-L
terminal CAS. Owner serialization adds no source-use RMW; outstanding accounting must justify every additional RMW.
Both prohibit per-read heap snapshots, global refcounts/tickets, per-callback slot CAS, and ordinary-read remote
metadata I/O.

Evidence must measure allocations, RMW/load/fence counts, publish/clear cycles, cache-line invalidation/false sharing,
L1/LLC behavior, readers/core scaling, executor hops or accounting latency, pool occupancy/fairness, scan duration,
capture retry, swap drain, cancellation, provider termination, quarantined slots, late callbacks, fallback/buffer
lifetime, multi-Binding rollback, retained protection, and hot-cache/cold-Object latency.

The following stay OPEN:

- single-reference versus seqlock authority/cell and exact lease-bound payload layout;
- VarHandle/full-fence realization, pool membership/sharding/padding/fairness, scan cadence, and retry scheduling;
- lease packing/allocation, wrap-test thresholds, and physical lifecycle family;
- provider cancel/termination mapping, attempt state, transport-buffer accounting, and response-observability cut;
- admission, lifetime, quarantine, retained-storage, and other numeric caps; and
- the Round 35 M4/M5 terminal/proof/fold writer, verifier, and receipt boundary.

`V2-READ-001/004/005/007` remain `PLANNED`. `V2-BK-010` retains only its existing M2
`PASSED_CURRENT_SOURCE` scope.
