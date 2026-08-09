# ADR 0069: V2 Binding read-view generation and pin boundary

## Status

Accepted for the 0.2 `OBJECT_WAL` logical read snapshot, append-versus-handoff publication split, read-pin granularity,
source-mixing boundary, two-stage reclamation, backlog safety, and takeover behavior. Exact allocation-free capture
algorithm, durable fallback-removal cut, and numeric bounds remain downstream M4 work; implementation has not started
at M0.

## Context

ADR 0067 requires locator-before-frontier-before-ACK and pin-safe locator retirement, but a literal immutable snapshot
per ACK or heap pin per read would add allocation/atomic contention to the hottest paths. Source-generation handoff is
low-frequency and has different lifetime from monotonic frontier publication. The logical contract must separate them
without allowing a reader to observe torn source selection or permitting capacity pressure to delete a still-pinned
fallback.

## Decision

`BindingReadViewSnapshot` is a logical captured state, not a required Java object, product wire, metadata row, API, or
configuration. Normal append/ACK does only:

1. install the required active-tail locators hidden;
2. release-publish the binding's `ReadableFrontier` and `BindingDurableFrontier` after the complete range is visible;
3. complete ACK.

It does not create an immutable snapshot object or publish a source-selection generation per ACK. Manifest/source
handoff instead publishes a low-frequency generation. Normal reads use an allocation-free owner-local pin mechanism
such as RCU/epoch/hazard state or an event-loop reader slot; they perform no remote metadata I/O and do not allocate a
pin/snapshot heap object or contend on one process-global `AtomicLong` by default.

One pin covers exactly one binding-scoped protocol read batch: for example, one partition fetch/range or one
ManagedLedger `readEntries` operation. A record, message, frame, or batch index does not receive its own pin, while a
connection or unbounded streaming session cannot retain one indefinitely.

The logical captured scope binds at least:

- Topic Binding and Topic Incarnation;
- Storage Epoch and Position Domain identity/version;
- owner fence;
- captured `ReadableFrontier`;
- active-tail view version;
- manifest view identity/generation; and
- source-protection generation.

`BindingReadState` and shared immutable references may supply these facts; each capture need not copy them. The exact
allocation-free coherent-capture algorithm remains open, but a reader may never combine fields from incompatible
generations.

Source purity is required for one atomic append unit and for any read path whose existing contract declares
whole-range fallback. It is not a one-source-per-request rule. Under one valid logical snapshot, a Kafka Fetch or
Pulsar Object-WAL read batch may read disjoint ranges from manifest and active tail while preserving Position Domain
order and never splitting one Kafka append commit set or Pulsar entry across sources. Pulsar sealed-ledger async
offload retains its separately accepted whole-range source-pure fallback.

Locator and source-protection reclamation has two distinct stages:

1. publish a successor view containing `manifest preferred + protected source fallback`; after older-view pins drain,
   retire index structures used only by those older views;
2. while any successor still names fallback, retain its protection. Publish a later view that no longer names the
   fallback, wait for every pin on fallback-bearing views to drain, and only then release protection or admit physical
   GC.

Publishing the replacement view always precedes retirement. A cache hit, materialization intent, or local absence of a
reader is not proof. The exact durable crash/restart cut between the second view and protection release remains open.

Retired-view and pin backlog have finite count, byte, age, and deadline hard bounds. A leak or timeout may block
handoff/retirement and may backpressure new read admission, but it can never reclaim a locator/protection early,
advance a frontier, or authorize GC. Values are evidence-derived ceilings rather than Topic switches.

Takeover reconstructs Root/checkpoint/LIST physical inventory first, then publishes read views independently per
Binding. Binding B does not wait for A's typed gap. Old-owner runtime pins/snapshots are never adopted as authority;
durable protection remains fail-safe until the new owner completes the accepted handoff protocol. Ordinary reads
remain zero-remote-metadata operations.

## Consequences

- `V2-OPEN-READ-02` is resolved at the logical contract level without freezing per-read or per-ACK heap objects.
- High-frequency frontier publication remains a local release-publish operation; low-frequency source selection owns
  generation and pin-drain work.
- Kafka requests may efficiently span disjoint source ranges without weakening append-unit atomicity; the Pulsar
  sealed-ledger whole-range fallback remains unchanged.
- Stuck pins trade bounded availability/capacity for safety; deletion is never the escape hatch.
- M3/M4 must measure hypothetical recovery-GET savings, read allocations/op, reader-slot atomic contention,
  retired-view count/bytes/age, pin-drain p99, and takeover recovery, and prove every publication, mixing, timeout,
  two-stage reclamation, and owner-fence cut. Skip-rate measurement does not itself admit a recovery-omission authority.
- Exact allocation-free coherent capture and durable fallback-removal state remain the next design frontier.

This decision refines ADRs 0049, 0059, 0064, 0066, and 0067 and is tracked by `T-APPEND-01`, `T-MANIFEST-01`,
`T-OBJECT-01`, `V2-READ-001/003/004`, and `V2-OPEN-READ-03/04`.
