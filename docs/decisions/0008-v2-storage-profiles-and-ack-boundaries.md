# ADR 0008: V2 storage profiles and acknowledgement boundaries

## Status

Accepted for the V2 design. Implementation and runtime evidence are not started at M0.

## Context

V1 exposes overlapping profile names that combine provider choice, acknowledgement timing, and materialization policy.
The result obscures the product tradeoff: Object storage is chosen for cost, while BookKeeper is chosen for latency and
predictable quorum durability.

## Decision

V2 exposes exactly three topic-level storage profiles:

| Profile | Acknowledgement boundary | Background object work | Optimization target |
| --- | --- | --- | --- |
| `OBJECT_WAL` | the complete range is durable in an Object WAL group | materialize Object WAL into read-optimized Object segments | storage cost |
| `BOOKKEEPER_WAL_ONLY` | the complete range is acknowledged by the configured BookKeeper quorum | none | write latency and predictable hot-path performance |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | the complete range is acknowledged by the configured BookKeeper quorum | offload sealed BookKeeper ranges into read-optimized Object segments | hot-path performance plus eventual cold-cost reduction |

There is no BookKeeper profile that waits synchronously for both BookKeeper and Object storage. Such a path would pay
the latency and request cost of both systems without improving the normal product objective. A future compliance-driven
dual-durability product would require a separate ADR and is not reserved by V2.

`OBJECT_WAL` does not perform a second background upload to become durable: the WAL is already in Object storage.
The API is asynchronous, but the acknowledgement waits for provider durability. Post-ack work is object-to-object
materialization, indexing, compaction, and safe retirement.

## Consequences

- Cost-first and performance-first choices are explicit instead of hidden behind sync/async aliases.
- The BookKeeper async-object path owns a two-provider lifecycle and therefore needs stronger publication and GC proof.
- Performance claims must compare profiles under their declared objective; BookKeeper latency cannot be presented as an
  equal-cost Object comparison.

This decision is tracked by `T-OBJECT-01`, `T-BK-01`, and scenarios `V2-OBJ-001`, `V2-BK-001`, and `V2-BK-002`.
