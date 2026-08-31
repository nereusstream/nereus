---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M4-A Binding read-view authority and batch lifetime

## Status and authority

This document synchronizes the accepted result of
[M4 Grill 32](../../grill-notes/32-m4-read-snapshot-authority.md) with ADRs 0069, 0070, 0072, 0075, 0077, and 0087.
Those ADRs remain the higher authority. No new ADR is required because M4-A selects no new persistent format, wire,
external consistency contract, or milestone ownership boundary.

M4-A is implementation input only. It creates no runtime source, test result, receipt, scenario promotion, or M4
Final.

## Authority and linearization

`BindingReadViewSnapshot` is a logical, allocation-free capture. It is not a required heap object, metadata row,
wire, API, global revision, or durable read ticket. One bounded Binding-scoped protocol read batch acquires it through:

```text
validate cached exact Binding/selector/admission/owner/protocol authority
  -> acquire exclusive SlotLeaseWord L
  -> acquire-load immutable authority/source-generation reference A/G
  -> publish {Binding,G} under L
  -> StoreLoad/full fence
  -> acquire-load and exactly revalidate A/G
  -> stable generation-tagged publication-cell capture
  -> accept only when cell.sourceGenerationId == G
```

Any authority, selector, Read Admission Epoch, Owner/Storage/Kafka-leader, generation, stability, or tag mismatch clears
the exact lease and retries within a bounded admission budget before dereferencing G. A loose collection of cached
fields is insufficient; revalidation compares one immutable authority reference or an equivalent exact tuple.

The generation recheck is a prerequisite, not the linearization point. With a single-reference cell, read-view
admission linearizes at the successful acquire-load and generation-tag validation. With a seqlock, it linearizes at
the successful final sequence validation. Provider I/O and response publication occur later and are not covered by a
claim that capture has completed.

Ordinary capture performs no Oxia, manifest-authority, Object metadata, or other remote control-metadata I/O. It adds
no global `readVersion`, deep copy, durable snapshot ACK, per-read metadata row, or process-global refcount.

## Logical batch boundary

One capture belongs to exactly one bounded Binding-scoped protocol read batch, such as one Kafka partition Fetch/range
sub-operation or one ManagedLedger `readEntries` operation. It never belongs to a record, frame, connection, or
unbounded stream.

An internal retry or fallback remains under the same capture and lease only when it keeps the same inbound operation,
Binding, logical interval, isolation/upper bound, response attempt, captured authorized source set, atomicity contract,
and cached owner/deadline permission. A transport retry must address the same authenticated immutable physical source.

A new capture is mandatory for a new client operation/retry, subsequent Kafka Fetch, new `readEntries` call,
owner/broker redirect, changed Binding/incarnation, changed selector/admission epoch or fence, changed requested range,
or changed physical source identity. A sequential cursor is only an exact-match acceleration hint; it never carries a
pin and is discarded when any bound identity differs.

After bytes or native response state become externally observable, the implementation cannot recapture or replace
them. It may continue only along the captured plan. A later unobserved independent range may use its captured fallback;
a declared whole-range fallback becomes illegal after any part of that range is exposed. Failure then follows native
protocol semantics while the lease remains until complete transport/buffer drain.

## Range and Binding coherence

Coherence is per Binding. Each capture resolves one position-ordered plan from the Storage Epoch, exact Source Map,
acknowledged active tail, manifest-selected generation, and any exact protected fallback. A gap, ambiguous overlap, or
unauthorized source fails closed. `ObjectMaterializedFrontier` is only a routing hint.

Disjoint non-overlapping ranges may use different captured sources. One Kafka append commit set, one Pulsar entry, and
every separately accepted whole-range fallback remain source-pure. I/O may execute in parallel, but logical output
keeps native Position Domain order.

A multi-Binding request reserves all required slots before any source I/O. Partial reservation is released rather than
held while waiting. Each Binding has its own capture; the design adds no cross-Binding atomic metadata revision. The
no-torn-world invariant is Binding-local:

```text
every returned byte, route, upper bound, protocol-state/index reference, and fence
comes from one accepted Binding capture; G state is never paired with a G+1 cell
```

## Publication, takeover, and old-read survival

A benign `G -> G+1` publication after successful capture does not replan the batch. The visible slot pins G, new
admissions use the current reference, and retirement waits for every G slot to drain.

The slot is owner-local resource-lifetime protection, not durable ownership or quiescence authority. Selector closure,
takeover, lease uncertainty, or fence loss prevents new admission under the closed epoch. An existing batch starts new
provider I/O, retry, fallback, or response publication only while its exact cached fence/deadline permits it and must
recheck after pause, network recovery, uncertainty, or expiry. Already-issued work and buffers keep the lease until
real termination even when stale success is withheld.

A successor `PREFERRED_ONLY` view does not by itself release fallback protection. Release remains gated by durable
selection, current fallback-bearing slot drain, contiguous historical Read Admission Epoch proof, and the exact
source-protection release CAS. A new owner never adopts old process slots, and process death or an empty pool is not a
durable quiescence proof.

## Slot ownership and terminal drain

The slot's only atomic ownership state is:

```text
FREE -> PINNED(L) -> FREE only by CAS(exact L -> FREE)
```

Lease generation `L` prevents late-callback slot-reuse ABA and is distinct from source generation G. `{Binding,G}` is
the hazard payload owned under L. Cancellation, deadline, and authority loss only close acquisition of new source use;
they do not clear the slot.

Success, failure, cancel, and deadline share one terminal predicate:

- no future source use can be acquired;
- provider work completed or cancellation acknowledged real source-access termination;
- fallback and decode completed; and
- the final source-backed buffer, including transport-owned buffers, was released.

Only the unique terminal path may clear exact L. Stale callbacks are sampled no-ops. Provider/decode phases,
`CANCEL_REQUESTED`, outstanding-use accounting, and quarantine are not additional slot states. A nonterminating
provider leaves the slot pinned and consumes bounded hard capacity; no timeout, pressure, or operator preference may
force-clear it. Process destruction discards local runtime state but supplies no durable source-protection proof.

Slot exhaustion or unstable capture retries only inside a bounded pre-I/O admission budget, then backpressures or
returns a typed safe failure. A read never continues without its complete slot set.

## M3 dependency boundary

M4-A binds the historical e5 M3 Final and closure identities listed in the
[M4 index](README.md#frozen-m3-dependency). It does not add M4 files to the M3 evidence-only descendant allowlist,
change the tested source, copy or rewrite the Final, or rerun the allocator campaign. A separate M4 dependency checker
will verify that historical closure while M4 gates validate later source and affected integration. Its schema, exact
projection, negative cases, and Final fields remain Grill 35 work.

## Non-negotiable performance boundary

The target stable-path work is one bounded slot acquire/publish, one StoreLoad/full fence, two authority/generation
loads, one small stable-cell capture, equality reads during use, and one terminal CAS. The design forbids per-read heap
snapshot/pin allocation, a shard-wide ticket increment on every read, process-global refcount contention, per-callback
CAS, synchronous remote metadata, and per-physical-Object slots.

M4 evidence must measure allocations/read, atomics/read, capture retry rate, publish/clear cost, false sharing,
pool occupancy/fairness, generation-swap scan/drain p99, source lifetime, cancellation p99, late callbacks, quarantine,
and hot-cache as well as cold-Object latency.

## Evidence-selected implementation choices

The following remain OPEN and are not final contracts in this design:

- single-reference versus seqlock publication cell;
- exact VarHandle operations, array/padding layout, and pool sharding/fairness;
- interval-plan representation, range coalescing, and parallel scheduling;
- event-loop serialization versus owner-local outstanding-use accounting;
- exact 64-bit lease packing and non-hotspot reuse-generation allocation;
- pool, retry, quarantine, lifetime, skew, and grace numeric limits;
- provider redirect/cancel classification, protocol response-commit cut, and native error mapping; and
- M4 historical-dependency checker representation and affected regression inventory.

These choices require implementation measurements or backend facts. Until selected by evidence, they remain explicit
open inputs and cannot be cited as scenario PASS.
