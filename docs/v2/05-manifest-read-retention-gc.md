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

- Protocol Cell, Cell Provider Scope, Topic Protocol Binding, Topic Incarnation, and Storage Epoch identity;
- typed Protocol Coverage and Position Domain version;
- source kind and epoch-scoped profile;
- an `ObjectExtent` or `BookKeeperExtent`;
- generation, format, payload mapping, and policy version;
- canonical Object-request-body length plus SHA-256/v1 Object Extent Digest, typed Provider Object Proof where
  available, protocol entry/record count, min/max timestamp, and CRC32C/v1 frame descriptors where applicable;
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
Worker processes or executors may be shared, but queue budgets, task roots, fencing, admission, and publication
authority are cell-scoped. A stalled or stale task in one cell cannot consume another cell's reserved share or publish
against its manifest.

## Logical view and physical overlap

The correctness invariant is one unambiguous binding-scoped logical read view, not physical non-overlap. During
publication and grace, primary WAL, source segments, and a materialized generation may cover the same Protocol Coverage.

The resolver first selects the Storage Epoch interval through the binding's Position Domain, then uses this order:

1. current active tail for unsealed acknowledged Protocol Coverage in the active epoch;
2. manifest-selected preferred generation for sealed Protocol Coverage;
3. exact source generation/Physical Extent as fallback while source protection remains valid;
4. fail closed when neither the selected generation nor a permitted source can prove the requested bytes.

For Pulsar Object WAL, ledger/entry lookup first resolves the explicit virtual Ledger Chain from Pulsar authority, then
maps ledger-keyed Pulsar Coverage to Object Extents. It never derives Ledger Chain order from a manifest, Object key, or
numeric ledger-ID order.

Cache is never authority. Cache keys and accounting include Protocol Cell and Cell Provider Scope; each cell has an
independent capacity share. A cache hit is validated against the selected descriptor generation and both declared
checksum domains/families.

The Object Extent descriptor remains outside the canonical body it digests. A Provider Object Proof may accelerate
verification but cannot replace the expected descriptor, and protocol-native Kafka/Pulsar bytes remain exact across
cache and materialization boundaries unless a new format explicitly defines a rewrite.

For an open Object-WAL run, the canonical sequence/length/SHA leaf key plus verified group header reconstructs that
descriptor; the pre-open WalRun Root supplies its exact scope/prefix and recovery budgets. Asynchronous descriptor pages
or sealed manifests may become preferred indexes, but cannot hide an ACKed tail that remains discoverable by bounded
provider LIST.

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
- Protocol Cell, Cell Provider Scope, physical-delete capability, Owner Epoch, worker epoch, and Storage Epoch are
  revalidated;
- response-loss state has converged and grace has elapsed;
- deletion identity matches the immutable provider object or ledger.

Deletion is metadata-first, retry-safe, and fail-closed. A provider success with lost response must converge without
deleting a recreated foreign object or repeating an unsafe operation.

Pulsar sealed-ledger offload cleanup is root-first: deterministic persisted attempt facts derive both keys, root absence
is proven before data deletion, and completion requires both objects plus covered multipart residue absent. This pair
rule does not grant a Nereus manifest native ManagedLedger deletion authority.

A GC executor may be shared only as a capacity pool. Every request enters through a cell-scoped task root and delete
capability, and foreign provider keys, ledgers, scopes, or credentials fail closed before provider I/O.

## Corruption

A corrupt preferred generation is quarantined. The reader may fall back only to a still-protected verified source. If
the source was safely retired and the preferred generation is corrupt, the result is an unrecoverable data error; the
system does not synthesize records or silently skip the requested Protocol Coverage.

Relevant tradeoffs: `T-MANIFEST-01`, `T-POSITION-01`, `T-PROJECTION-01`, and `T-FABRIC-01`. Required scenarios:
`V2-READ-001`, `V2-READ-002`, `V2-PROJECTION-001`, and `V2-FABRIC-003`.
