# ADR 0072: V2 slot lease word and terminal source drain

## Status

Accepted for the 0.2 `OBJECT_WAL` read-slot reuse ABA fence, minimal hot-path ownership state, cancellation cut,
terminal source-use drain, bounded quarantine, and ticket-allocation constraints. Exact bit packing, choice between
owner-executor serialization and outstanding-use accounting, cache-line layout, and numeric pool/quarantine limits
remain M4 evidence work; implementation has not started at M0.

## Context

ADR 0070 fixes generation-tagged hazard acquisition and requires a slot to remain pinned through every asynchronous
source use. Reusing a slot creates a separate ABA domain: a callback from read A must not clear the slot after read B
has acquired it. Encoding cancel, provider, fallback, decode, and buffer lifecycle states in the slot would duplicate
the existing read-batch lifecycle and add CAS traffic to every callback.

## Decision

The slot's only atomic ownership field is one 64-bit `SlotLeaseWord`:

- `0` means `FREE`;
- a nonzero word means `PINNED(generation)`; and
- the same atomic compare domain contains active state and at least a 61/62-bit reuse generation. A split ticket and
  state pair is forbidden because an old callback could validate one word and clear a later lease through the other.

The exact bit layout is not frozen. Allocation may use a per-slot reuse generation or pool-shard/thread ticket ranges,
but it must not require one shard-wide increment on every read. Generation wrap retires that slot and fails closed;
0.2 has no online rebase protocol.

Only two slot ownership cuts require an atomic state transition:

1. acquisition changes `FREE` to one fresh nonzero lease word; and
2. the unique terminal drain performs `CAS(exactLeaseWord -> FREE)`.

Nonterminal provider, fallback, decode, deadline, cancel, and buffer callbacks perform only acquire/equality reads of
the exact lease word. A mismatch is a no-op recorded by a sampled counter, never a per-event log and never a slot CAS.
Those callbacks keep their ordinary state in the existing read-batch asynchronous lifecycle; that state is not copied
into the slot.

Cancellation linearizes only the prohibition on acquiring new source use. It does not free the slot. An implementation
must prove complete source drain through either one owner/event-loop executor that serializes every source-use
scheduling cut, or owner-local `tryAcquireSourceUse` plus outstanding-use accounting. Terminal clear is allowed only
after provider completion or cancellation acknowledgement with real source-access termination semantics, fallback and
decode completion, and release of the final source-backed buffer.

A provider that does not terminate after cancellation leaves the slot in bounded quarantine and continues consuming
hard capacity. Exhaustion backpressures before new source I/O; timeout, memory pressure, or operator preference never
force-clears the lease. Process termination may destroy the owner-local pool, but that destruction is not durable
owner-read quiescence evidence and cannot release source protection.

`SlotLeaseWord` is internal runtime state. It is not product wire, metadata, API, configuration, a durable read ticket,
or a second source-generation authority.

## Consequences

- `V2-OPEN-READ-05` is resolved without putting a full asynchronous state machine in each slot.
- The target normal-path cost is one local acquire, equality load(s), and one terminal CAS; ordinary callbacks do not
  contend through CAS.
- M4 must prove cancellation/source-use linearization, provider cancellation semantics, late callbacks, ticket/state
  ABA, wrap retirement, pool close, quarantine exhaustion, zero per-read allocation, atomics/read batch, cancellation
  p99, sampled late-callback count, quarantined slots, and cache-line contention.

This decision refines ADRs 0069 and 0070 and is tracked by `T-MANIFEST-01`, `T-POLICY-01`, and
`V2-READ-004/005/007`.
