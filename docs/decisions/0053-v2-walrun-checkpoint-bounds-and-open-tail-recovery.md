# ADR 0053: V2 WalRun checkpoint bounds and open-tail recovery

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

Asynchronous checkpoint pages can accelerate recovery without adding metadata I/O to the ACK cut, but an optional
accelerator cannot be the only way to discover an acknowledged open tail. Topic-scoped checkpoint switches are also
invalid because one WalRun may contain frames from several compatible Topic Protocol Bindings.

## Decision

`WalRunCheckpointPageV1` is an immutable control-metadata record covering at most 256 extent descriptors and 64 KiB of
canonical bytes in aggregate. ADR 0060 refines it to one run-wide vector chain: a page binds the Root SHA, page ordinal,
one predecessor-page SHA, structured descriptors ordered by `{laneId, laneSequence}`, a `coveredThrough` vector for up
to three instantiated lanes, and physical extent evidence. ADR 0063 permits only `ProviderResolvedExtentDescriptor`
input. Pages and the one checkpoint-head CAS publish after physical provider resolution, not after every member's
protocol ACK, and are not part of the append durability cut.

ADR 0065 makes the page row physical-only: Root SHA appears once in the page header, not in every row; the row carries
lane/sequence, directory-prefix end, body length, Object SHA-256, and only an optional closed bounded qualified-provider
proof. No binding/read frontier, ACK, gap, or per-binding coverage is encoded.

Checkpoint policy is resolved at `Protocol Cell x shard` scope and persisted in the next `WalRunRootRecord`. Proactive
cadence may be configured or disabled, but hard aggregate `maxUncheckpointedExtents` and
`maxUncheckpointedBytes` plus per-lane `maxUncheckpointedAge` are always finite and enforced. Before any bound is
exceeded, the shard must force checkpoint progress, backpressure, or roll over; it cannot continue admitting an
unbounded uncovered tail. A configuration change takes effect only from the next WalRun.

Each page may advance one or several lanes but must advance every changed vector component contiguously from its
predecessor. Each component is `LaneExtentResolvedThrough`; it is not `BindingDurableFrontier`. Open-run recovery
validates the one predecessor/vector chain and then always performs bounded strong LIST for every uncovered lane tail.
A missing or invalid chain falls back to the run's full bounded LIST envelope. Handoff may use the same validated head
but cannot skip that LIST. Directory reconstruction issues bounded parallel prefix GETs only for candidates not safely
excluded by manifest/source authority; unknown coverage is conservatively included. This path never expands to a
whole-Object GET and all GET/bytes/time work charges the cumulative recovery envelope.

Rollover flushes the single final gap-free vector chain and `WalRunSealRecord` binds one final checkpoint-head SHA, the
exact terminal sequence vector, and minimum aggregate count/byte completeness facts before successor publication. This
sealed canonical inventory is mandatory and cannot be disabled. Provider length/digest/version proof still validates
the bytes; checkpoint metadata cannot override them or assert that every binding member ACKed.

## Consequences

- `V2-OPEN-OBJ-18` is resolved.
- Periodic pages add asynchronous metadata volume and possible seal delay, while avoiding per-group ACK metadata I/O and
  bounding normal takeover work.
- Checkpoint cadence is an accelerator; hard uncovered-tail bounds, LIST fallback, and the sealed inventory are
  correctness contracts.
- Uncovered-tail bounds count provider-resolved descriptors not yet inventoried; a typed predecessor gap after physical
  resolution cannot consume those bounds.
- Three lane-local predecessor chains/checkpoint heads are rejected. One lane may remain unchanged while the vector
  page advances another, so asynchronous checkpoint publication does not impose a cross-lane physical or protocol ACK
  barrier.
- M3/M7 must prove every page-chain corruption/gap/fork, all three uncovered-tail limits, disabled proactive cadence,
  backpressure/rollover, open-tail LIST, handoff, final seal binding, and next-WalRun policy activation.

This decision is refined by ADRs 0060/0063..0065, refines ADRs 0030, 0039, 0047, and 0049 and is tracked by
`T-OBJECT-01`, `T-HANDOFF-01`, `T-POLICY-01`, `V2-OBJ-014..022`, and `V2-OPEN-OBJ-19/22/23`.
