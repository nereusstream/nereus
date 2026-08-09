# ADR 0064: V2 Object WAL physical and binding frontiers

## Status

Accepted for the 0.2 `OBJECT_WAL` physical-resolution, binding-frontier, runtime completion, resource-bound, and
failure-isolation contract. Exact numeric runtime limits remain evidence/admission outputs; implementation has not
started at M0.

## Context

Lane sequence recovery and protocol ACK answer different questions. A shared Object can be physically durable and safe
to checkpoint while one binding waits for protocol predecessor coverage and another binding advances. Treating one
generic “ACK” or one persistent ordered map as both truths either recreates cross-binding head-of-line blocking or adds
a sixth metadata authority with large per-topic memory retention.

## Decision

0.2 separates two typed frontiers:

- `LaneExtentResolvedThrough` with `{WalRunRootSha, laneId, laneSequence}` is the greatest contiguous lane sequence whose
  Object outcome has converged to verified provider success and can no longer become an absent gap. It is physical
  WalRun/checkpoint/recovery state and is never a protocol position.
- `BindingDurableFrontier` is the greatest contiguous prefix in one binding's Position Domain. It alone determines
  that binding's protocol durability/ACK eligibility and is never compared through lane sequence, Object key, or LIST
  order.

The lane barrier blocks sequence `n+1` only while an earlier Object outcome is unknown or may still be provider-absent.
Once the earlier extent is provider-resolved, a pure typed-position gap in binding A cannot stop physical lane progress,
checkpoint publication, or binding B's independent frontier.

Each runtime completion tracker is identified by:

```text
{
  ProtocolCellId,
  TopicBindingId,
  TopicIncarnation,
  StorageEpochId,
  PositionDomainId,
  PositionDomainVersion
}
```

Owner Epoch is not part of the durable logical-frontier identity, but every tracker instance is owner-local. Each
completion performs an O(1) check against the already cached owner fence; it performs no remote metadata read. Takeover
destroys the old instance, the new owner rebuilds from durable extent evidence, and a stale-owner completion cannot
advance the new instance.

The tracker is a reconstructible runtime mechanism, not persistent authority. The normal serialized allocation path
uses a bounded ring/window over owner-local completion order so insertion and contiguous release are O(1). Recovery or
genuinely sparse completion may use a Position-Domain-aware bounded ordered structure. Trackers are created lazily;
when no pending unit remains, only a compact frontier is retained and the heavier structure may be reclaimed.

After provider resolution, payload, ciphertext, compression, and request buffers are released immediately. A pending
gap entry retains only typed coverage, protocol idempotency identity, an authenticated descriptor reference, and any
owner-local waiting future. Gap accounting never charges a nominal target Object size. Hard observed bounds include at
least pending-unit count, descriptor bytes, waiting-future count, oldest-gap age, and aggregate shard tracker bytes.

Near a per-binding bound, admission stops binding A before allocating another protocol position, removes A from new
shared groups, backpressures A, and continues resolving its existing gap; unrelated binding B continues. An ordinary
capacity limit or typed predecessor gap does not by itself fence A or roll the WalRun. Fencing/rollover requires an
owner/invariant failure, a provider-unknown/absent lane outcome, or aggregate recovery-envelope pressure.

Failure isolation follows the validation layer:

- ObjectExtentDigest, KMS envelope, fixed header, or directory AEAD failure prevents release of every member of that
  shared Object;
- after Object/header/directory validation, a frame/commit-set AEAD, CRC, native checksum, or typed-coverage failure
  blocks/fails only its owning binding's complete commit set and does not automatically block independently validated
  bindings;
- one complete `KafkaAppendCommitSet` remains the Kafka tracker unit, and one ManagedLedger entry remains the Pulsar
  tracker unit.

Recovery groups authenticated append-unit descriptors by tracker identity and uses the binding's Position Domain
comparison/adjacency to rebuild contiguous coverage. It never treats `laneSequence`, Object key, checkpoint order, or
provider LIST order as a protocol comparator. Runtime gap maps, waiting futures, and owner-local completion order are
not persisted and add no per-binding remote metadata operation.

## Consequences

- `V2-OPEN-OBJ-01` is resolved: physical extent resolution and binding ACK/frontier are distinct contracts.
- Provider-resolved shared Objects can enter checkpoint while member bindings converge independently.
- The normal path avoids a permanent TreeMap and payload retention for every Topic; sparse ordered structures are
  bounded exceptions used for recovery/out-of-order completion.
- Per-binding pressure isolates admission before position allocation, while shared physical corruption and aggregate
  recovery failure remain explicit common failure domains.
- M3 must prove both frontiers, cached owner fencing, takeover destruction/rebuild, ring wrap and sparse recovery,
  every per-binding/aggregate bound, early buffer release, A/B isolation, shared-versus-frame corruption, Kafka
  commit-set atomicity, Pulsar entry atomicity, and zero append-path metadata I/O.

This decision refines ADRs 0007, 0008, 0031, 0037, 0038, 0040, 0047, 0053, 0060, and 0063 and is tracked by
`T-APPEND-01`, `T-PROTOCOL-01`, `T-OBJECT-01`, `V2-APP-001..003`, and `V2-OBJ-001/002/006/007/012/018/020/021`.
