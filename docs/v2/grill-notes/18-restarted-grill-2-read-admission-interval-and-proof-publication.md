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

## Awaiting explicit confirmation

No Round 16 recommendation above is normative. Confirmed conclusions must move to ADRs/contracts; adjustments and all
evidence-selected values/representations remain in the open log.
