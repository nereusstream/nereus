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

## Awaiting explicit confirmation

No Round 18 recommendation above is normative. Confirmed conclusions must move to ADRs/contracts; adjustments and all
evidence-selected values/representations remain in the open log.
