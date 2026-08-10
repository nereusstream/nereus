# ADR 0043: V2 Pulsar topic generation selector and retired tombstone

## Status

Accepted for the 0.2 Pulsar metadata integration. Implementation and runtime evidence are not started at M0.

## Context

Pulsar has no native topic UUID that can fence same-name deletion and recreation. ADR 0028 therefore includes a
positive binding generation in the Pulsar incarnation identity, but leaving an old incarnation key absent after
cleanup would permit a delayed `putIfAbsent` retry to resurrect it. Keeping every full aggregate forever avoids that
ABA window but retains more metadata than correctness requires after exact retirement.

## Decision

Pulsar uses two permanent fences.

One name-scoped `PulsarTopicGenerationSelector` is the compact ABA authority for a canonical persistence name. Its
generation increases monotonically, its `DELETED(generation)` fact is durable, and generation overflow fails closed.
Deletion never makes the selector absent or permits an earlier generation to become current again.

The incarnation-scoped full `TopicBindingAggregateRecord` remains immutable while the incarnation is live, deleting,
readable, or referenced by any Storage/Owner Epoch, Object or BookKeeper extent, cursor/projection, task, open handle,
retention/GC protection, or required audit grace. Only after an exact reference-free retirement proof may one
exact-version CAS replace the full aggregate, as the final retirement mutation, with a compact permanent
`RetiredTopicIncarnationTombstone` at the same authority key.

The tombstone binds protocol and complete incarnation identity, binding generation, the original aggregate SHA-256,
and the retirement-proof digest. That incarnation key is never deleted or reused, so a delayed `putIfAbsent` cannot
resurrect the full aggregate. A later generation uses a new incarnation key and does not wait for old payload
compaction. Hard lifetime row/count/byte admission includes every selector and tombstone, including retired history.

ADR 0051 refines selector publication to exact `RESERVED -> ACTIVE -> DELETING -> DELETED` single-key CAS transitions.
ACTIVE plus exact aggregate identity is revalidated on topic open, ownership acquisition, and metadata-version change,
then cached as a local versioned fence; normal append/read performs no per-access Oxia operation.

## Consequences

- `V2-OPEN-PUL-META-01` is resolved.
- One small permanent row per historical incarnation is accepted to release the full aggregate without reopening an
  ABA window.
- Topic-name recreation does not reuse generation, key, binding ID, or initial Storage Epoch ID.
- Selector state/recovery and cached admission are refined by ADR 0051. Tombstone wire, retirement receipt, exhaustive
  reference domains, and audit grace remain downstream metadata gates.
- M1 proves monotonic selection, durable deletion, overflow rejection, stale-create response loss, tombstone wire/
  capacity, same-name recreation isolation, and rejection of replacement without proof. M5 proves exact replacement
  ordering, every reference veto, and irreversible full-to-tombstone CAS recovery.

This decision is refined by ADR 0051, refines ADRs 0023, 0028, and 0033, and is tracked by `T-META-01`, `T-GC-01`,
`V2-META-002/003/005..007`.
