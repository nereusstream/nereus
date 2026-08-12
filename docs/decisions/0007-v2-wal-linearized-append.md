# ADR 0007: V2 WAL-linearized append

## Status

Accepted for the V2 design and refined by ADRs 0011 and 0087. Implementation and runtime evidence are not started at
M0.

## Context

V1 allocates and stabilizes append ranges through remote metadata records. That model makes normal append latency and
availability depend on control-metadata round trips and spreads the append truth across mutable metadata, WAL state,
and repair logic.

V2 is a clean implementation line. It does not preserve the V1 append API, metadata schema, or compatibility path.
The V1 behavior remains available at `v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`.

## Decision

For each Topic Protocol Binding and Topic Incarnation:

1. control metadata grants one exclusive Owner Epoch before append admission;
2. the Native Write Authority allocates positions according to the binding's Position Domain;
3. the selected primary WAL establishes a durable, contiguous Protocol Frontier for that binding;
4. an append becomes protocol-ACK eligible only after its complete typed Protocol Coverage belongs to that durable
   frontier and its protocol-local readable/state publication is complete; a native protocol may require a later
   visibility frontier such as Kafka HW for `acks=all`;
5. normal append performs no remote control-metadata read or mutation;
6. manifests and lifecycle roots are published after durability and are not append linearization points.

Kafka allocation returns a dense Kafka Offset Range. Pulsar allocation returns Pulsar-native Position/MessageId
semantics through its ManagedLedger path. This ADR does not define a cross-protocol numeric offset.

An API may use non-blocking asynchronous I/O. That implementation detail does not change the acknowledgement boundary.
A timeout or lost provider response is an uncertain outcome and must be resolved from deterministic write identity,
Topic Protocol Binding, Topic Incarnation, Storage Epoch, Owner Epoch, typed Protocol Coverage, length, and checksum
rather than by allocating new positions blindly.

## Invariants

- `V2-INV-APP-01`: at most one Native Write Authority and Owner Epoch may admit new positions for one Topic
  Incarnation.
- `V2-INV-APP-02`: every successful append returns the exact protocol-native position result and typed Protocol
  Coverage allocated by its Position Domain.
- `V2-INV-APP-03`: acknowledgement implies durable primary-WAL coverage for that complete Protocol Coverage.
- `V2-INV-APP-04`: a fenced completion cannot advance the typed Durable Frontier or become readable.
- `V2-INV-APP-04A`: the publication that advances a frontier is itself protected by the exact owner/protocol fence;
  publishing first and checking the fence afterward is forbidden.
- `V2-INV-APP-05`: recovery never infers success only from a cached position or control-metadata head.
- `V2-INV-APP-06`: profile durability never substitutes for Kafka LEO/HW/LSO or a native protocol state transition.

## Consequences

- The hot path no longer pays per-append KRaft/Oxia/MetadataStore latency.
- Owner recovery must reconstruct the typed Durable Frontier from the primary WAL and bounded lifecycle roots.
- Writer-lane serialization and ownership renewal become explicit availability and backpressure boundaries.
- Provider-response-loss handling is part of correctness, not an optional retry optimization.

This decision is tracked by `T-APPEND-01`, `T-POSITION-01`, `T-KAFKA-01`, scenarios `V2-APP-001` through
`V2-APP-003`, and `V2-KAF-DATA-001..022`.
