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

- protocol topic identity, incarnation, and logical range;
- source kind and profile;
- object key or ledger identity;
- generation, format, payload mapping, and policy version;
- byte length, record count, min/max timestamp, and checksums;
- index descriptors required for offset and timestamp lookup;
- creation writer/task identity.

A mutable record never changes the meaning of an immutable object or ledger range.

## Manifest authority

One stream manifest root selects the logical read view for sealed ranges. It references immutable descriptors and
advances by fenced compare-and-set. A publication may add a preferred generation only when:

- the output covers exactly durable source ranges;
- all bytes and indexes are validated;
- the source set, task identity, policy, and output format still match;
- ownership/worker fencing is current;
- an equal or higher generation has not already won.

Publication is idempotent. Duplicate workers converge on the same deterministic task/output identity or cancel as stale.

## Logical view and physical overlap

The correctness invariant is one unambiguous logical read view, not physical non-overlap. During publication and grace,
primary WAL, source segments, and a materialized generation may cover the same logical range.

The resolver uses this order:

1. current active tail for unsealed acknowledged ranges;
2. manifest-selected preferred generation for sealed ranges;
3. exact source generation as fallback while source protection remains valid;
4. fail closed when neither the selected generation nor a permitted source can prove the requested bytes.

Cache is never authority. A cache hit is validated against the selected descriptor generation and checksum family.

## Timestamp and offset indexes

Offset and timestamp indexes are first-class descriptor members. They are built from the same source cut as the payload
and published atomically through the manifest root. Timestamp lookup uses bounded candidate scans and protocol-native
sentinel semantics; it must not linearly scan a full partition under normal operation.

## Materialization and compaction

Materialization converts readable primary-WAL/sealed sources into read-optimized Object segments. Compaction may change
record visibility but preserves logical offset coverage and protocol transaction/control-marker rules.

Planner input is a frozen manifest/source root. A local metadata snapshot may schedule work but final publication
revalidates durable authority. A newer generation or policy invalidates stale work before activation.

## Logical trim and physical GC

Logical trim advances protocol-visible retention independently from physical deletion. Physical GC requires:

- manifest no longer selects the source as the only readable generation;
- all protocol cursor/group/transaction retention floors pass the complete source;
- no reader pin, recovery root, task protection, or source-protection record remains;
- owner/worker epoch and configured scope are revalidated;
- response-loss state has converged and grace has elapsed;
- deletion identity matches the immutable provider object or ledger.

Deletion is metadata-first, retry-safe, and fail-closed. A provider success with lost response must converge without
deleting a recreated foreign object or repeating an unsafe operation.

## Corruption

A corrupt preferred generation is quarantined. The reader may fall back only to a still-protected verified source. If
the source was safely retired and the preferred generation is corrupt, the result is an unrecoverable data error; the
system does not synthesize records or silently skip the range.

Relevant tradeoff: `T-MANIFEST-01`. Required scenarios: `V2-READ-001` and `V2-READ-002`.
