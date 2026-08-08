---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Manifest, read, retention, and GC

## Immutable physical descriptors

Every physical source or materialized output has an immutable descriptor containing:

- Protocol Cell, Topic Protocol Binding, Topic Incarnation, and Storage Epoch identity;
- typed Protocol Coverage and Position Domain version;
- source kind and epoch-scoped profile;
- an `ObjectExtent` or `BookKeeperExtent`;
- generation, format, payload mapping, and policy version;
- byte length, protocol entry/record count, min/max timestamp, and checksums;
- index descriptors required for protocol-position and timestamp lookup;
- creation Owner Epoch/task identity.

A mutable record never changes the meaning of an immutable object or ledger range.

## Manifest authority

One Topic Protocol Binding manifest root selects the typed logical read view across its Storage Epoch chain. It
references immutable descriptors and advances by fenced compare-and-set. A publication may add a preferred generation
only when:

- the output covers exactly durable, binding-scoped Protocol Coverage;
- all bytes and indexes are validated;
- the source set, task identity, policy, and output format still match;
- ownership/worker fencing is current;
- an equal or higher generation has not already won.

Publication is idempotent. Duplicate workers converge on the same deterministic task/output identity or cancel as stale.

## Logical view and physical overlap

The correctness invariant is one unambiguous binding-scoped logical read view, not physical non-overlap. During
publication and grace, primary WAL, source segments, and a materialized generation may cover the same Protocol Coverage.

The resolver first selects the Storage Epoch interval through the binding's Position Domain, then uses this order:

1. current active tail for unsealed acknowledged Protocol Coverage in the active epoch;
2. manifest-selected preferred generation for sealed Protocol Coverage;
3. exact source generation/Physical Extent as fallback while source protection remains valid;
4. fail closed when neither the selected generation nor a permitted source can prove the requested bytes.

Cache is never authority. A cache hit is validated against the selected descriptor generation and checksum family.

## Timestamp and protocol-position indexes

Kafka Offset, Pulsar Position/entry, batch, and timestamp indexes are first-class descriptor members where their
protocol path requires them. They are built from the same typed source cut as the payload and published atomically
through the manifest root. Timestamp lookup uses bounded candidate scans and protocol-native sentinel semantics; it must
not linearly scan a full partition or ManagedLedger under normal operation.

## Materialization and compaction

Materialization converts readable primary-WAL/sealed sources into read-optimized Object segments. Compaction may change
record visibility but preserves typed Protocol Coverage and protocol transaction/control-marker rules.

Planner input is a frozen manifest/source root. A local metadata snapshot may schedule work but final publication
revalidates durable authority. A newer generation or policy invalidates stale work before activation.

## Logical trim and physical GC

Logical trim advances a binding-scoped typed Trim Frontier independently from physical deletion. Physical GC requires:

- manifest no longer selects the source as the only readable generation;
- all protocol cursor/group/transaction retention floors pass the complete source;
- no reader pin, recovery root, task protection, source-protection record, Access Projection, Projection Map, or
  Migration Link still requires the source;
- Owner Epoch, worker epoch, Storage Epoch, and configured scope are revalidated;
- response-loss state has converged and grace has elapsed;
- deletion identity matches the immutable provider object or ledger.

Deletion is metadata-first, retry-safe, and fail-closed. A provider success with lost response must converge without
deleting a recreated foreign object or repeating an unsafe operation.

## Corruption

A corrupt preferred generation is quarantined. The reader may fall back only to a still-protected verified source. If
the source was safely retired and the preferred generation is corrupt, the result is an unrecoverable data error; the
system does not synthesize records or silently skip the requested Protocol Coverage.

Relevant tradeoffs: `T-MANIFEST-01`, `T-POSITION-01`, and `T-PROJECTION-01`. Required scenarios:
`V2-READ-001`, `V2-READ-002`, and `V2-PROJECTION-001`.
