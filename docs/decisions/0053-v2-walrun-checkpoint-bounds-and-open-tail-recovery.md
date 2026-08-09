# ADR 0053: V2 WalRun checkpoint bounds and open-tail recovery

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

Asynchronous checkpoint pages can accelerate recovery without adding metadata I/O to the ACK cut, but an optional
accelerator cannot be the only way to discover an acknowledged open tail. Topic-scoped checkpoint switches are also
invalid because one WalRun may contain frames from several compatible Topic Protocol Bindings.

## Decision

`WalRunCheckpointPageV1` is an immutable control-metadata record covering at most 256 contiguous extents and 64 KiB of
canonical bytes. It binds the Root SHA, page ordinal, exact sequence interval, predecessor-page SHA, every
content-addressed extent descriptor, and per-binding typed coverage. Pages and the checkpoint-head CAS publish only
after ACK and are not part of the append durability cut.

Checkpoint policy is resolved at `Protocol Cell x shard` scope and persisted in the next `WalRunRootRecord`. Proactive
cadence may be configured or disabled, but hard `maxUncheckpointedExtents`, `maxUncheckpointedBytes`, and
`maxUncheckpointedAge` are always finite and enforced. Before any bound is exceeded, the shard must force checkpoint
progress, backpressure, or roll over; it cannot continue admitting an unbounded uncovered tail. A configuration change
takes effect only from the next WalRun.

Open-run recovery validates any contiguous page chain and then always performs bounded strong LIST for the uncovered
tail. A missing or invalid chain falls back to the run's full bounded LIST envelope. Handoff may use the same validated
head but cannot skip that LIST.

Rollover flushes a final gap-free page chain and `WalRunSealRecord` binds its final checkpoint-head SHA before successor
publication. This sealed canonical inventory is mandatory and cannot be disabled. Provider length/digest/version proof
still validates the bytes; checkpoint metadata cannot override them.

## Consequences

- `V2-OPEN-OBJ-18` is resolved.
- Periodic pages add asynchronous metadata volume and possible seal delay, while avoiding per-group ACK metadata I/O and
  bounding normal takeover work.
- Checkpoint cadence is an accelerator; hard uncovered-tail bounds, LIST fallback, and the sealed inventory are
  correctness contracts.
- The current page wire assumes one contiguous extent sequence. If `V2-OPEN-OBJ-19` accepts concurrent packing lanes,
  lane-local page/Seal inventory must refine this ADR without creating another current Root/pointer.
- M3/M7 must prove every page-chain corruption/gap/fork, all three uncovered-tail limits, disabled proactive cadence,
  backpressure/rollover, open-tail LIST, handoff, final seal binding, and next-WalRun policy activation.

This decision refines ADRs 0030, 0039, 0047, and 0049 and is tracked by `T-OBJECT-01`, `T-HANDOFF-01`,
`T-POLICY-01`, `V2-OBJ-014..016`, and `V2-OPEN-OBJ-19`.
