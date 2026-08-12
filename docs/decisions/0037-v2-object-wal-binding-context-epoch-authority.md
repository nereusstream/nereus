# ADR 0037: V2 Object WAL binding-context epoch authority

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

An Object-WAL group may batch frames from multiple Topic Protocol Bindings in one Protocol Cell. A singular Topic Owner
Epoch or Storage Epoch in the WalRun Root therefore cannot authorize all frames unless 0.2 gives up cross-binding
batching and creates a separate run for every binding/epoch. The cost-first profile depends on retaining compatible
cross-binding PUT amortization without allowing a physical run identity to become logical append authority.

## Decision

The WalRun Root is physical/run authority only. It binds Protocol Cell, Cell Provider Scope, shard, run/session
generation, format and codec families, exact prefix, sequence/recovery contract, and the rule that every frame must pass
binding-scoped epoch validation. It does not carry one singular topic Owner Epoch or Storage Epoch.

Every Object body contains a bounded `BindingContextTable`. Each context identifies exactly one binding incarnation and
contains the deterministic binding ID, complete typed Topic Incarnation Identity, Storage Epoch ID/ordinal, and Owner
Epoch identity/token needed to validate its frames. Every frame refers to exactly one context and repeats or commits to
the authority facts required to reject descriptor/context substitution.

Kafka leader epoch is not a singular WalRun Root fact either. Each Kafka append-unit directory row/context binds its
exact partition and Kafka leader epoch; all member frames and assigned RecordBatch headers cross-check it. A leader-
epoch transition closes that partition's current append/run admission without forcing unrelated bindings to share one
Root-level epoch.

The group directory may summarize contexts and typed coverage for bounded lookup, but it cannot authorize a frame,
replace the exact context, compare positions across bindings, or advance a frontier. Recovery validates the run root,
then the complete context table, then every frame/context reference before reconstructing any binding's Durable
Frontier.

ADR 0064 makes the runtime tracker owner-local without putting Owner Epoch into durable frontier identity. Every
completion still validates its cached context/owner fence in O(1); stale-owner work cannot advance the new instance,
and no completion adds a remote metadata read. Once the shared Object/header/directory is valid, context/frame failure
is isolated to that binding's complete append unit where the failing layer permits it.

Cross-binding grouping remains limited to compatible bindings in one Protocol Cell. An epoch or incarnation mismatch
rejects only by the fail-closed append/recovery contract; a group-level shard/run epoch never substitutes for a topic
Owner or Storage Epoch.

## Consequences

- `V2-OPEN-OBJ-09` is resolved.
- Recovery must range-read and validate a bounded binding-context directory before filtering by topic epoch.
- 0.2 preserves PUT amortization instead of paying one run/object stream per binding.
- Exact context/header field IDs, substitution-proof commitments, limits, and range layout remain downstream NWG1 wire
  gates.
- M3 must prove multi-binding/multi-epoch rejection, context/frame substitution, stale Owner Epoch, independent
  frontiers, and absence of any singular topic epoch in run-root authority.

This decision is refined by ADR 0064, refines ADRs 0014, 0030, and 0031 and is tracked by `T-OBJECT-01`, `T-FABRIC-01`,
`V2-OBJ-002`, `V2-OBJ-005..007`, and `V2-OBJ-021`.
