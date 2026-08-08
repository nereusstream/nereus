---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: DocumentationOnly
authority: NormativePlan
sourceTuple: v2-m0
---

# Implementation plan and gates

## Milestones

| Milestone | Scope | Status at M0 | Required aggregate |
| --- | --- | --- | --- |
| M0 | V1 archive references, V2 ADRs/contracts, source/scenario manifests, tradeoff register, documentation gate | DocumentationGated | `v2M0Check` |
| M1 | new V2 API, format identities, capability-split metadata SPI; remove superseded V1 API | Planned | `v2M1Check` |
| M2 | owner lane, offset contract, BookKeeper foundation, Pulsar offload/position spikes | Planned | `v2M2Check` |
| M3 | Object WAL groups, per-stream durable prefix, provider-response-loss recovery | Planned | `v2M3Check` |
| M4 | manifest, offset/timestamp indexes, resolver, readable active tail | Planned | `v2M4Check` |
| M5 | materialization, compaction, retention, source protection, physical GC | Planned | `v2M5Check` |
| M6 | Kafka KRaft integration and protocol compatibility | Planned | `v2M6Check` |
| M7 | fencing, planned handoff, bounded recovery, mixed-profile operations | Planned | `v2M7Check` |
| M8 | scale, chaos, exact-source AutoMQ comparison, Pulsar native parity, release evidence | Planned | `v2M8Check` and `v2FinalCheck` |

Only M0 tasks are registered at M0. A future task name in this plan is not an implementation or PASS claim.

## Milestone delivery contract

Every implementation milestone must land one coherent set:

1. production and test implementation;
2. the affected normative V2 contract and ADR/open-decision update;
3. scenario Markdown and JSON statuses;
4. tradeoff status or mitigation changes;
5. source-lock changes, when an external or fork source changed;
6. ordinary deterministic gate;
7. exact-source receipt for any `Verified` or performance/parity claim.

Superseded V1 Java, tests, Phase/Future prose, and literal checks are deleted with the V2 slice that replaces them. They
are not marked deprecated and are not kept as compatibility shims; branch `v0.1` preserves the old product line.

## Status model

Normative documents carry:

- `designStatus: Accepted | Proposed`;
- `implementationStatus: NotStarted | InProgress | Verified`;
- `evidenceStatus: NotRun | DocumentationOnly | CurrentSourceReceipt`;
- `sourceTuple` pointing to the structured lock.

`Verified` requires `CurrentSourceReceipt` and a non-empty receipt path. Historical or focused evidence must use a
lower status.

Scenario statuses are `PLANNED`, `IMPLEMENTED_NOT_RUN`, `PASSED_CURRENT_SOURCE`, `FAILED`, or
`BLOCKED_ENVIRONMENT`. Moving a row to `PASSED_CURRENT_SOURCE` requires the exact product/fork/baseline tuple and
receipt to be present in the same change.

## M0 gate

`v2DocumentationCheck` validates required files and front matter, the three-profile vocabulary, JSON structure,
source/archive identity, tradeoff IDs, scenario synchronization, local Markdown links, and receipt/status consistency.
`v2M0Check` is the M0 aggregate and is wired into CI.

The legacy Phase 3, Phase 4, and BookKeeper documentation checks remain available for V1 implementation evidence, but
their literal requirements no longer govern the V2 overview/index.

## Final evidence

M8 evidence is append-only under `docs/v2/evidence/<source-tuple-id>/`. The receipt records:

- exact clean product, Kafka, Pulsar, AutoMQ, BookKeeper/provider, and client identities;
- environment, object request accounting, durability/replication, dataset, warmup, duration, and thresholds;
- scenario results with no missing, duplicate, or silently skipped mandatory rows;
- artifacts and checksums required to reconstruct the run.

The acceptance baseline fields in [source locks](source-locks.json) intentionally remain `NOT_PINNED` at M0.
