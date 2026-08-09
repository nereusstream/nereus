# ADR 0063: V2 provider-resolved checkpoint publisher

## Status

Accepted for the 0.2 `OBJECT_WAL` checkpoint eligibility, single-publisher fencing, response-loss, and takeover
contract. Exact record field IDs and evidence-derived throughput budgets remain downstream M3 work; implementation has
not started at M0.

## Context

A checkpoint is a physical extent inventory accelerator, not proof that every protocol append unit inside an Object has
advanced its binding frontier. If publication waits until all bindings ACK, one binding's typed predecessor gap consumes
the shared uncovered-tail budget and indirectly backpressures unrelated bindings. Multiple lane publishers are still
unnecessary because checkpoint publication is outside the append ACK cut.

## Decision

One open WalRun has exactly one shard-owner-fenced asynchronous checkpoint combiner, one page predecessor chain, and one
checkpoint-head CAS. The publisher fence is a monotonic `CheckpointPublisherEpoch` scoped to
`{ProtocolCellId, shardId, WalRunRootSha, shardRunEpoch}`; no Topic Binding Owner Epoch can fence or authorize this
run-wide publisher.

Lane builders enqueue only runtime `ProviderResolvedExtentDescriptor` values into a bounded local queue. An extent becomes
provider-resolved only after:

- its conditional PUT outcome has converged to success;
- exact Object identity/body length/SHA and required provider/body proof have validated;
- the Root-bound header and authenticated directory have validated;
- its lane sequence has physically converged and can no longer become a provider-absent gap.

Protocol ACK, `BindingDurableFrontier` advancement, or the absence of a typed predecessor gap is not an eligibility
condition. A checkpoint may therefore inventory a shared Object in which binding B has advanced while binding A still
waits. Recovery later reads authenticated append-unit evidence and dispatches each member to its own binding tracker.

`coveredThrough[laneId]` is the contiguous `LaneExtentResolvedThrough` component, not a protocol frontier. Aggregate
`maxUncheckpointedExtents/Bytes` and per-lane age count provider-resolved descriptors waiting for page coverage; they do
not count time spent waiting for a binding's typed frontier after physical resolution.

The combiner allows exactly one immutable page candidate in flight. Candidate request identity is deterministic from
`{rootSha, pageOrdinal, predecessorPageSha, pageBodySha}`. The checkpoint head binds at least Root SHA,
shard-run/publisher epoch, page ordinal, exact page key/SHA, and the `coveredThrough` vector.

On publisher takeover, the new shard owner first exact-CASes the head to a new publisher epoch while preserving page
ordinal, page key/SHA, and every vector component. Only after that fence commits may it construct a successor page. A
stale publisher may write its one already-admitted immutable candidate, but its later head CAS fails.

Response and conflict handling is exact:

- response unknown rereads the head/page and accepts only the same candidate and head value;
- a definitive CAS conflict may adopt only a fully validated head for the same Root whose vector is component-wise
  non-regressing and whose publisher epoch is current;
- the current publisher creates a new successor from the committed head; no process locally merges two predecessors,
  rewrites a page, or lets a stale publisher continue.

Because each publisher epoch admits at most one candidate, a failed epoch leaves at most one unreachable immutable
page. Such residue is ignored by recovery, counted against metadata byte/count admission, and cleaned only after
ordinary Seal/retirement authority permits it.

ADR 0065 fixes the physical wire boundary: the runtime descriptor may carry `walRunRootSha` for defensive admission,
but a Root-bound page row does not repeat it. Rows carry no binding/read frontier, ACK, gap, or per-binding coverage.
Any optional provider-version/qualified-proof field is a closed, bounded, canonical, deterministic field set rather
than an opaque provider blob.

## Consequences

- A binding-level typed gap cannot consume uncovered-tail budget after its Object is provider-resolved.
- One asynchronous publisher avoids three checkpoint heads without joining protocol ACK frontiers.
- The combiner queue and publisher remain bounded runtime accelerators; neither becomes a per-binding remote metadata
  authority or enters the append ACK cut.
- M3/M7 evidence must report descriptor queue depth/age, page publish RPS, head-CAS conflicts/retries, forced
  checkpoint/backpressure count, takeover recovery time, Seal flush latency, and unreachable-page residue.
- Multiple publishers may be reconsidered only after the single combiner fails a predeclared SLO under the accepted
  aggregate bounds.

This decision is refined by ADR 0065, refines ADRs 0039, 0047, 0049, 0053, and 0060 and is tracked by
`T-OBJECT-01`, `T-HANDOFF-01`, `V2-OBJ-002/014/015/018/020/022`.
