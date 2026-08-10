# ADR 0070: V2 generation-tagged read publication and hazard slots

## Status

Accepted for the 0.2 `OBJECT_WAL` allocation-free coherent-capture ordering, generation-tagged publication cell,
bounded cross-Binding slot-pool scope, asynchronous pin lifetime, and memory-order contract. Exact array layout,
padding, single-reference-versus-seqlock representation, VarHandle operations, and numeric pool limits remain M4
evidence work. Slot reuse and terminal source drain are refined by ADR 0072; implementation has not started at M0.

## Context

ADR 0069 fixes one logical Binding read snapshot and one pin per Binding-scoped protocol read batch. A slot published
after a reclaimer has already scanned can still be safe only if the reader rechecks the generation before it first
dereferences generation-owned state. Capturing the frontier before that recheck creates a classic pin-after-retire
race. Conversely, a generation-only pin without a generation-tagged frontier cell can pair G with G+1 coverage.
Event loops also host many concurrent asynchronous reads, so one fixed slot per Binding or event loop is not a valid
capacity model.

## Decision

Every read batch acquires one exclusive slot from a bounded pool sharded by shard, read executor, or event loop and
reused across Bindings. Logical validation remains Binding/generation scoped. The normal acquisition order is:

1. acquire-load the current source-generation identity/reference G;
2. publish exact `{Binding identity, G}` into the batch's exclusive slot;
3. establish Store-to-Load ordering between slot publication and the next pointer load. The contract requires a
   volatile/sequentially-consistent store or an explicit StoreLoad/full fence; release-store plus acquire-load alone is
   not assumed to provide it;
4. acquire-load the current source generation again. If it is not G, clear the slot and retry without dereferencing G;
5. coherently capture one generation-tagged publication cell
   `{sourceGenerationId, ReadableFrontier, activeTailViewVersion}` through a single-reference snapshot or a stable
   seqlock read;
6. dereference G-selected locator/source state only when `sourceGenerationId == G` and the publication-cell read is
   stable; otherwise clear and retry;
7. clear the slot only after the last possible source access, including asynchronous provider I/O, retry, fallback,
   decode, and use of a source-backed buffer.

A pointer switch after step 4 does not force retry: the visible slot pins G and the publisher/reclaimer must wait for
it. Source-generation swap uses release/volatile publication, reclamation reads slots with acquire/volatile semantics,
and clear is at least a release operation. A slot contains the complete generation identity as its generation ABA
fence; generation identities are not silently recycled within one Binding incarnation.

Pool capacity is reserved before provider I/O. Each concurrently unfinished Binding-scoped read batch owns one slot;
slots are not preallocated as Binding x event-loop products. A multi-Binding request either reserves every required
slot atomically before source access or releases all partial reservations and fails/splits along protocol-legal
boundaries. It never holds a partial pin set while waiting for the remainder.

Slot exhaustion backpressures before source I/O. Correctness cannot be disabled, and Topic policy cannot enlarge the
pool hard cap. There is no per-read heap pin/snapshot allocation, process-global refcount, or remote metadata I/O.

## Consequences

- `V2-OPEN-READ-03` is resolved without fixing one Java data structure.
- The hot path adds one bounded slot acquire/publish/clear, two generation loads, and a small coherent-cell read.
- False sharing, pool contention, generation-swap scans, retry storms, and asynchronous cancellation/late-completion
  behavior require explicit evidence; a timeout is not permission to clear a slot while source use may continue.
- M4 must prove every publication/reclamation interleaving, StoreLoad variant, seqlock/single-reference stability,
  cross-Binding pool isolation, multi-Binding all-or-release behavior, source-buffer lifetime, zero allocations/op,
  slot publish/clear cost, cache-line contention, slot occupancy/scan time, capture retry rate, and generation-swap
  drain p99.
- ADR 0072 resolves slot-reuse ABA and cancellation/late-callback terminal drain without adding callback CAS traffic or
  duplicating the read-batch lifecycle in the slot.

This decision is refined by ADR 0072, refines ADRs 0049, 0067, and 0069, and is tracked by `T-MANIFEST-01`,
`T-POLICY-01`, and `V2-READ-001/003..005/007`.
