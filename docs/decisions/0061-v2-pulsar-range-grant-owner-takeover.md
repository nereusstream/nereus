# ADR 0061: V2 Pulsar range-grant owner takeover

## Status

Accepted as a correctness constraint on any 0.2 `RANGE_LEASED` allocator candidate. It does not select or activate
`RANGE_LEASED`, reject `STRICT_SERIALIZED`, or freeze the final record wire/range size. Implementation and runtime
evidence have not started at M0.

## Context

Binding a range to a broker owner and burning its unused tail after takeover would make failover reacquire thousands of
ranges through one Cell allocator. At 10,000 ManagedLedgers, three serialized 10-ms range-allocation writes already
have a 300-second idealized lower bound. A reserved range must therefore survive owner fencing without letting an old
owner publish a stale Ledger Chain node.

## Decision

A range grant belongs permanently to one `ManagedLedgerIncarnation` and `grantId`, never to a broker owner epoch. The
candidate head separates chain visibility, owner fencing, and allocation cursor:

```text
ManagedLedgerHead {
  managedLedgerIncarnation
  visibleChainHeadNodeIdAndDigest
  ownerEpoch
  grantId
  rangeStart
  rangeEndExclusive
  nextLedgerId
  allocatorProtocolVersion
}
```

Owner takeover performs an exact head CAS that changes only `ownerEpoch`; it preserves the visible chain, grant,
range, and cursor. A ledger candidate binds `ledgerId`, `grantId`, `creatorOwnerEpoch`, and exact expected predecessor
node ID/digest. Candidate creation is single-flight per ManagedLedger, and only an exact head CAS may publish it.

The Cell allocator's `RANGE_RESERVED` fact binds
`{managedLedgerIncarnation, grantId, range, requestId, expectedAllocationState}`. The expected allocation state is the
exact `{visibleChainHeadNodeIdAndDigest, priorGrant/range, nextLedgerId}` tuple, not an owner epoch. If takeover occurs
after reserve and before head installation, the new owner first fences through the owner-only head CAS, then may install
that same RESERVED grant when the allocation state is unchanged. It finally reconciles allocator clear; it does not
reserve another range merely because the creator owner changed.

Once an exact head value durably contains the grant, that ManagedLedger may use the range. The allocator
`RANGE_RESERVED -> IDLE` clear is not on this range's use path, but it still blocks the next Cell-wide grant. A
high-priority reconciler executable by any current owner must converge it through exact allocator/head rereads; pending
age, queue depth, reconciliation latency, and append impact are mandatory evidence/telemetry.

At exact `nextLedgerId`, a current owner converges an uncertain candidate/head write by exact reread equality. A node
from a stale owner epoch is never adopted: a cursor-only head CAS advances `nextLedgerId` by exactly one without
changing the visible chain, permanently burning that candidate ID. Because creation is single-flight, one ownership
loss leaves at most one such candidate. An absent slot permits the new owner to create its candidate. A late old-owner
`putIfAbsent` conflict converges through the same one-ID burn.

CAS outcomes are classified explicitly:

- response unknown: exact reread accepts candidate/head/grant equality as success or continues deterministic recovery;
- definitive version/value conflict: fences that creator and follows the committed owner/state; it never treats an
  unknown response as a definitive fence.

Whole-tail burn is limited to Topic deletion, ManagedLedger retirement, allocator-protocol incompatibility, or
unrecoverable grant/head corruption. Orphan candidates are permanent no-reuse evidence in 0.2 rather than a new GC
protocol. They count against metadata byte/count admission, capacity planning, and observable committed/burned/orphan /
reserved counters. Range sizing remains versioned Cell policy under a finite churn rate/horizon and recovery reserve;
Topic/host configuration cannot enlarge it.

## Consequences

- `V2-OPEN-PUL-OBJ-09` remains open for exact wire/keys, range-size evidence, the shared allocator reservation protocol,
  and final mode selection; the owner-takeover correctness shape is no longer open.
- Installed ranges survive broker failover, replacing serialized mass reacquisition with parallel head fencing and at
  most one exact stale-candidate burn per affected ManagedLedger.
- A stuck clear can block the next grant even though it cannot revoke the installed range, so reconciler priority and
  queue/age evidence are release gates.
- Permanent orphan metadata is an accepted bounded capacity cost; 0.2 does not add orphan GC.
- M1 proves every reserve/install/clear and node/head response-loss cut in the evidence-only harness, including exact-
  equality convergence, same-RESERVED takeover, one-candidate burn, and production-SPI absence. M3 proves definitive-
  conflict fencing, late-old-owner writes, tail preservation, clear reconciliation, and mass-takeover behavior.

This decision refines ADRs 0022, 0027, 0032, 0041, 0048, 0049, 0054, and 0055 and is tracked by `T-POSITION-01`,
`T-POLICY-01`, `V2-POSITION-010/011/017/018`, and `V2-OPEN-PUL-OBJ-09`.
