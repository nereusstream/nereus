# ADR 0007: V2 WAL-linearized append

## Status

Accepted for the V2 design. Implementation and runtime evidence are not started at M0.

## Context

V1 allocates and stabilizes append ranges through remote metadata records. That model makes normal append latency and
availability depend on control-metadata round trips and spreads the append truth across mutable metadata, WAL state,
and repair logic.

V2 is a clean implementation line. It does not preserve the V1 append API, metadata schema, or compatibility path.
The V1 behavior remains available at `v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`.

## Decision

For each stream incarnation:

1. control metadata grants one exclusive writer epoch before append admission;
2. the active owner allocates dense logical offsets in its serialized writer lane;
3. the selected primary WAL establishes a durable, contiguous per-stream prefix;
4. an append is acknowledged only after its complete range belongs to that durable prefix;
5. normal append performs no remote control-metadata read or mutation;
6. manifests and lifecycle roots are published after durability and are not append linearization points.

An API may use non-blocking asynchronous I/O. That implementation detail does not change the acknowledgement boundary.
A timeout or lost provider response is an uncertain outcome and must be resolved from deterministic write identity,
writer epoch, range, length, and checksum rather than by allocating a new range blindly.

## Invariants

- `V2-INV-APP-01`: at most one writer epoch may admit new ranges for one stream incarnation.
- `V2-INV-APP-02`: every successful append returns one dense range `[baseOffset, endOffset)`.
- `V2-INV-APP-03`: acknowledgement implies durable primary-WAL coverage for the whole returned range.
- `V2-INV-APP-04`: a fenced completion cannot advance the durable prefix or become readable.
- `V2-INV-APP-05`: recovery never infers success only from a cached offset or control-metadata head.

## Consequences

- The hot path no longer pays per-append KRaft/Oxia/MetadataStore latency.
- Owner recovery must reconstruct the durable end from the primary WAL and bounded lifecycle roots.
- Writer-lane serialization and ownership renewal become explicit availability and backpressure boundaries.
- Provider-response-loss handling is part of correctness, not an optional retry optimization.

This decision is tracked by `T-APPEND-01` and scenarios `V2-APP-001` through `V2-APP-003`.
