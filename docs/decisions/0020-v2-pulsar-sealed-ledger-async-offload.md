# ADR 0020: V2 Pulsar sealed-ledger asynchronous offload

## Status

Accepted for 0.2 Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0017 makes native ManagedLedger metadata the sole Pulsar offload/lifecycle authority. The execution unit still has
to choose between offloading an immutable, non-current ledger through `LedgerOffloader.offload(ReadHandle, ...)` and
streaming entries from the ledger that is still accepting appends.

Streaming the active ledger can reduce cold-copy lag, but it introduces a second live progress frontier and makes
attempt completion, crash recovery, fallback, and source-deletion proof more complex. The 0.2 objective is native Pulsar
parity on the BookKeeper path without moving Object latency into append.

## Decision

0.2 offloads only sealed, non-current ManagedLedger ledgers:

- The native ledger offload attempt and its UUID cover one immutable ledger and use the ledger-based
  `LedgerOffloader.offload(ReadHandle, UUID, ...)` contract.
- The current append-admitting ledger is never streamed to Object storage by this profile in 0.2. The
  `streamingOffload(...)` path is outside the release contract.
- The offloader completes only after the full sealed-ledger coverage and its authoritative Object root are durable and
  readable. ManagedLedger then publishes native completion as required by ADR 0017.
- Ledger size, entry count, age/rollover policy, offload concurrency, and lag admission policy bound cold-copy delay and
  BookKeeper capacity. They never add a synchronous Object wait to a completed BookKeeper append.
- Failure, retry, cleanup, read fallback, and BookKeeper deletion remain ledger-attempt scoped and fail closed through
  ManagedLedger metadata.

This decision fixes execution timing and native attempt granularity. It does not yet choose whether one ledger maps to
one Object or to multiple bounded Object Extents behind one root.

## Consequences

- `V2-OPEN-BK-03` is resolved.
- Object durability may lag by as much as the configured ledger rollover plus offload queue time.
- 0.2 avoids active-ledger streaming checkpoints and their additional recovery/delete state.
- M2 must prove rollover-triggered eligibility, non-current-ledger selection, attempt retry/cleanup, unavailable Object
  behavior, lag throttling, offloaded read/fallback, and native source-deletion cuts.

The 0.2 Object pair is refined by [ADR 0024](0024-v2-pulsar-sealed-ledger-object-layout.md), and its deterministic
root/lifecycle by [ADR 0029](0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md). This decision refines ADR 0017 and is
further refined by [ADRs 0035](0035-v2-pulsar-npo1-sealed-ledger-root-format.md) and
[0036](0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md), plus
[ADRs 0044](0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md) and
[0045](0045-v2-pulsar-dual-source-read-handle-and-pins.md). It is tracked by `T-BK-01`, `V2-BK-001..002`, and
`V2-BK-004..010`.
