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

## Awaiting explicit confirmation

No Round 15 recommendation above is normative. Confirmed conclusions must move to ADRs/contracts; adjustments and all
evidence-selected values/representations remain in the open log.
