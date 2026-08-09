# ADR 0049: V2 configuration scopes and persisted semantics

## Status

Accepted for all 0.2 Kafka and Pulsar profiles. Implementation and runtime evidence are not started at M0.

## Context

V2 needs tunable latency, cost, compression, checkpoint, recovery, and resource policies, but an unrestricted flag at
topic, broker, or process scope could silently change durable bytes or recovery behavior after ownership moves. Per-topic
flag combinations would also fragment cross-topic Object batching and make compatibility impossible to audit.

## Decision

Correctness invariants, recovery semantics, and durable compatibility contracts are never feature flags and cannot be
disabled by topic, tenant, Cell, shard, host, or process configuration.

Tunable policy has exactly three scopes:

1. **Topic/Tenant-or-Namespace policy** expresses latency/cost intent through a small closed set of typed policy
   classes and quantized levels. It may select linger, group/block target, compression eligibility, BookKeeper
   retention, and related SLO policy. It cannot raise a format/parser hard cap or alter a wire invariant, checksum,
   fencing rule, recovery authority, or publication state machine.
2. **Protocol Cell/shard policy** owns shared checkpoint cadence and hard lag bounds, allocator and group scheduling,
   recovery budgets, queues, and shared concurrency. A multi-binding WalRun never accepts a topic-scoped switch that
   changes run recovery semantics.
3. **Host/process capacity** supplies only resource ceilings for threads, memory, direct buffers, cache, I/O, KMS, CPU,
   and concurrency. Capacity pressure may reject admission, backpressure, or force a contract-defined early seal or
   rollover; it cannot reinterpret persisted bytes or weaken recovery.

The Product/Deployment publishes the admitted typed-policy catalog and one source-qualified base semantic default.
Topic/Tenant-or-Namespace policy inherits or explicitly overrides that default. A Protocol Cell may reject a class or
cap its resource use but cannot choose a different semantic default; a host never selects one. Pulsar NPD1 resolves in
the order Deployment base -> Namespace -> explicit Topic override and persists the result in the offload attempt.

For the same typed quantity, the effective runtime budget is
`min(topic/tenant-or-namespace request, Protocol Cell/shard budget, host/process capacity)`. An absent or incompatible
lower ceiling does not select a weaker correctness mode; admission fails or backpressures.

Every resolved value that affects persisted bytes or recovery semantics is recorded at its durable activation boundary:
`StorageEpoch`, hard-recovery `WalRunRootRecord`, Object-group descriptor/header, or sealed-ledger offload-attempt
facts. A failover reads that persisted value and does not recompute it from the target host. Changes take effect only
at the next contract-defined epoch, run, Object group, or offload attempt, never halfway through one.

A configurable identity is scoped to exactly one activation lifecycle. One enum or versioned class cannot combine a
Storage-Epoch encoding choice, an Object-group packing/linger choice, a sealed-ledger offload-attempt choice, and host
capacity. Related policies may be validated for compatibility, but each value is persisted and changed only at its own
boundary. Topic-specific soft packing is not a singular WalRun Root recovery identity. ADR 0060 maps admitted classes
onto at most three lazy lanes, persists group facts, and keeps every Root/recovery budget aggregate; exact class values
and canonical lane binding remain open.

Policy compatibility is part of batching admission. Cross-topic batching uses the resolved typed policy class and
quantized fields; V2 does not expose an unbounded map of per-topic boolean flags.

## Consequences

- Performance tuning remains possible without making correctness or durable compatibility host-dependent.
- Typed classes may reject some bespoke combinations and host pressure may reduce throughput rather than silently
  downgrade semantics.
- M1/M2/M3 must prove catalog/default resolution, minimum-budget arithmetic, hard-cap non-escalation, failover
  configuration drift, lifecycle-specific activation boundaries, Cell/host non-reinterpretation, early
  seal/backpressure under resource pressure, incompatible batching rejection, and that every correctness gate remains
  non-disableable.

This decision is refined by ADRs 0056 through 0061, refines ADRs 0012, 0014, 0029, 0030, 0037, and 0047, and is tracked
by `T-POLICY-01`, `V2-POLICY-001`, `V2-BK-012/013`, `V2-OBJ-016..018`, and `V2-POSITION-011`.
