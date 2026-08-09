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

The tracker is a reconstructible runtime mechanism, not persistent authority. ADR 0066 requires one combined
tracker-slot plus active-tail-locator reservation before protocol position allocation, then assigns one full 64-bit
owner-local ticket per complete Kafka commit set or Pulsar entry. Full ticket equality is the ring-slot ABA fence;
coverage/Position Domain adjacency remains ordering authority. The normal path uses a bounded ring/window so insertion
and contiguous release are O(1). Recovery defaults to bounded collect plus Position Domain sort and then reuses that
ring; a long-lived ordered map requires later benchmark evidence. Trackers are created lazily, and no implementation is
required to allocate one heavyweight object per Binding.

After provider resolution, payload, ciphertext, compression, and request buffers are released immediately. A pending
gap entry retains only typed coverage, protocol idempotency identity, an authenticated descriptor reference, and any
owner-local waiting future. Gap accounting never charges a nominal target Object size. Hard observed bounds include at
least pending-unit count, descriptor bytes, waiting-future count, oldest-gap age, and aggregate shard tracker bytes.

Near a per-binding bound, admission stops binding A before allocating another protocol position, atomically refuses the
combined tracker/locator reservation, removes A from new shared groups, backpressures A, and continues resolving its
existing gap; unrelated binding B continues. An ordinary
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

Recovery groups authenticated append-unit descriptors by tracker identity, performs bounded collect/sort, and uses the
binding's Position Domain comparison/adjacency to rebuild contiguous coverage and fresh local tickets. It never treats
`laneSequence`, Object key, checkpoint order, provider LIST order, ticket, or cached predecessor as a protocol
comparator. Runtime gaps, waiting futures, tickets, and owner-local completion order are not persisted and add no
per-binding remote metadata operation. ADR 0067 installs compact active-tail locator coverage before publishing
Readable/Durable frontiers and ACK. ADR 0069 separates that high-frequency publication from low-frequency pinned
source-selection generations without changing the physical-versus-binding frontier split. ADR 0070 fixes the hazard
ordering and generation-tagged frontier/view capture for readers of that publication.

## Consequences

- `V2-OPEN-OBJ-01` is resolved: physical extent resolution and binding ACK/frontier are distinct contracts.
- Provider-resolved shared Objects can enter checkpoint while member bindings converge independently.
- The normal path avoids a permanent TreeMap, one-heavy-object-per-Binding requirement, and payload retention; bounded
  collect/sort is the default recovery fallback.
- Per-binding pressure isolates admission before position allocation, while shared physical corruption and aggregate
  recovery failure remain explicit common failure domains.
- M3 must prove both frontiers, cached owner fencing, takeover destruction/rebuild, ring wrap and sparse recovery,
  every per-binding/aggregate bound, early buffer release, A/B isolation, shared-versus-frame corruption, Kafka
  commit-set atomicity, Pulsar entry atomicity, and zero append-path metadata I/O.

This decision is refined by ADRs 0066/0067/0069/0070, refines ADRs 0007, 0008, 0031, 0037, 0038, 0040, 0047, 0053,
0060, and 0063 and is tracked by `T-APPEND-01`, `T-PROTOCOL-01`, `T-OBJECT-01`, `V2-APP-001..003`,
`V2-OBJ-001/002/006/007/012/018/020..024`, and `V2-READ-003..005`.
