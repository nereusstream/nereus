---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Grill 2 record: provider sharing and Protocol Cell isolation

Date: 2026-08-09

This record preserves the complete decision input and rationale from Grill 2. The accepted contract is
[ADR 0014](../../decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md); this record is not runtime evidence.

## Question

Does V2 over-design Provider sharing, how much does it cost, and how deeply does it affect the architecture?

## Assessment presented for confirmation

Sharing external Object Storage or BookKeeper infrastructure is not over-design. Making Kafka and Pulsar share one
stateful Provider runtime, credential context, Object group, queue, cache state, or GC authority would be over-design for
0.2 and would turn resource reuse into a multi-tenant correctness and security model.

The three considered boundaries were:

1. shared physical provider infrastructure with cell-scoped sessions — recommended;
2. optional compatible SDK transport/client pooling below those sessions — deferred optimization;
3. shared cross-cell Object groups, provider state, cache, workers, and GC authority — rejected for 0.2.

The broad third option would affect Provider identity and lifecycle in M1, Object grouping/admission/recovery in M3,
cache/task/GC isolation in M5, drain/handoff/recovery in M7, and shared-failure/noisy-neighbor evidence in M8.

## Confirmed answer

The user confirmed:

> Share physical provider infrastructure, use Cell-scoped Provider sessions, and do not share Object groups or
> correctness state across Protocol Cells in 0.2.

The resulting contract is:

- one external Object Storage or BookKeeper deployment may serve multiple Protocol Cells;
- each cell owns its namespace, credential/KMS and operator scope, admission/quota, retry/circuit-breaker state, cache
  partition, task root, GC capability, and lifecycle;
- compatible lower-level transport may be pooled only behind independently owned sessions;
- one cell's close, throttle, credential failure, stale task, or delete request cannot mutate another cell;
- Object WAL groups remain inside exactly one Protocol Cell;
- shared worker processes/executors are capacity pools with cell-scoped queues, budgets, fencing, and authorities;
- dedicated provider infrastructure is an optional stronger deployment topology;
- a failure of intentionally shared physical infrastructure may still affect every attached cell.

This closes `V2-OPEN-FABRIC-01`; implementation and evidence remain planned.
