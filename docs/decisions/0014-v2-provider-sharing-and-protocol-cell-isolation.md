# ADR 0014: V2 provider sharing and Protocol Cell isolation

## Status

Accepted for the V2 design. Implementation and runtime evidence are not started.

## Context

“Shared provider” can mean either multiple Protocol Cells use the same external Object Storage or BookKeeper
infrastructure, or the cells share one stateful runtime session, batching domain, credential context, cache, task root,
and deletion authority. Those choices have very different correctness, security, lifecycle, and failure-containment
costs.

V2 needs the infrastructure-utilization benefit of a Storage Fabric without turning a provider-reuse optimization into
an implicit cross-protocol authority or a mandatory multi-tenant runtime.

## Decision

External **Provider Infrastructure** may be shared by multiple Kafka and Pulsar Protocol Cells. Examples include an
Object Storage service/account or a BookKeeper cluster. Shared physical infrastructure is a deployment choice and may
create a common physical failure domain.

Every Protocol Cell nevertheless owns a distinct **Cell Provider Scope** and one or more process-local **Cell Provider
Sessions**:

- the scope binds Protocol Cell identity, provider endpoint identity, an exclusive namespace, credential/security
  scope, allowed encryption/KMS scope, admission/quota scope, operator owner, and physical-delete capability;
- secret values are not persisted in the scope; only stable references or identity versions may be bound;
- a session owns cell-local admission, retry/circuit-breaker state, open groups, in-flight accounting, metrics, drain,
  and close lifecycle;
- closing, throttling, rotating credentials, or failing one session cannot close or mutate another cell's session;
- a session may borrow a compatible lower-level SDK transport or client pool, but that pool owns no protocol position,
  manifest, task, cache, or GC authority and cannot erase the session boundary.

Object WAL groups never cross Protocol Cells in 0.2. A group may batch multiple compatible Topic Protocol Bindings only
inside one Cell Provider Scope. Cross-cell batching is not required for Storage Fabric sharing.

Worker processes, executors, and observability pipelines may be physically shared. Their queues, budgets, task roots,
fencing, cache namespaces/accounting, and physical-GC authorization remain cell-scoped. A shared executor is a capacity
mechanism, not a shared correctness state or deletion authority.

Dedicated buckets, BookKeeper clusters, worker processes, or other provider infrastructure remain optional deployment
choices for stronger SLO, compliance, or physical-failure isolation. V2 does not claim that logical Protocol Cell
isolation survives an outage of intentionally shared physical infrastructure.

## Consequences

- V2 keeps most infrastructure-utilization and common-lifecycle benefits without requiring one cross-cell runtime
  object graph.
- Separate sessions consume additional clients, connections, threads, and idle capacity and forgo cross-cell Object
  batching.
- Any later transport pooling requires explicit ownership/lease semantics, compatible configuration, independent cell
  admission, and close/credential-rotation tests; it is an optimization, not an API obligation.
- Namespace collision, foreign read/delete/publication, cell-local close/throttle/credential failure, shared-provider
  outage, and noisy-neighbor behavior require explicit multi-cell evidence.
- Protocol Cell is the minimum logical failure-attribution and provider-authorization boundary. Tenant policies may
  further subdivide a cell and are not replaced by this decision.

This decision refines ADR 0009 and ADR 0011 and is tracked by `T-FABRIC-01`, `V2-FABRIC-001`, `V2-FABRIC-002`, and
`V2-FABRIC-003`.
