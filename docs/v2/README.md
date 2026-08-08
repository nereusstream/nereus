---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: DocumentationOnly
authority: NormativeIndex
sourceTuple: v2-m0
---

# Nereus V2 design index

Nereus V2 replaces the V1 append correctness model. It is not an incremental optimization, compatibility layer, or
online migration of V1. The V2 product is a Storage Fabric containing independent Kafka and Pulsar Protocol Cells. It
combines shared storage lifecycle contracts with two deliberately different data paths: cost-first Object WAL and
performance-first BookKeeper WAL.

## Current status

- `main` develops `0.2.0-SNAPSHOT` from source tuple `v2-m0`.
- M0 freezes the V2 documentation contract only; V2 Java implementation and runtime evidence have not started.
- Existing Java modules and Phase/Future evidence on `main` are V1 residue until replaced by a V2 milestone.
- The ordinary CI Pulsar API checkout remains a legacy V1-residue build baseline until the V2 Pulsar slice replaces
  that code; it is recorded separately and is not the V2 fork-development or parity baseline.
- The V1 implementation authority is branch `v0.1` at
  `a14d925da5763f36208f8ddca7bef31f3eb90b0b`; it is historical evidence, not a V2 contract.
- No V1 API, durable schema, object format, or online compatibility obligation is carried into V2.

## Authority order

When V2 sources disagree, use this order:

1. accepted V2 ADRs;
2. normative contracts in this directory;
3. the current milestone implementation plus its exact-source executable receipt;
4. V2 scenario matrix and implementation plan;
5. V1 Phase/Future documents and external research as historical input only.

Code does not become V2 authority merely because a similarly named V1 class still exists. A milestone may claim
`Verified` only when its implementation, normative docs, scenario status, source tuple, and receipt are synchronized.

## Frozen protocol and profile model

| Profile | ACK waits for | Object lifecycle | Product objective |
| --- | --- | --- | --- |
| `OBJECT_WAL` | durable Object WAL group coverage | background object-to-object materialization | cost first |
| `BOOKKEEPER_WAL_ONLY` | BookKeeper quorum | no Object copy | performance first |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | BookKeeper quorum | sealed Protocol Coverage asynchronously offloaded | performance first with later cold-cost reduction |

A Topic Protocol Binding is immutable for one Topic Incarnation and fixes Protocol Cell, protocol kind, Position Domain,
payload mapping, and Native Write Authority. Kafka Offset and Pulsar Position are separate truths; shared storage uses
typed Protocol Coverage and never creates a universal logical offset.

A profile is immutable within one Storage Epoch. A binding may have an append-only epoch chain with protocol-native
cutover frontiers, but the supported online transition matrix and exact transition state machine are not yet accepted.
Operational batching, cache, throttling, and compaction policy remain separately mutable.

## Reading order

1. [V2 Context Map](../../CONTEXT-MAP.md) and the linked Kafka, Pulsar, and Shared Storage glossaries
2. [Correctness and append](01-correctness-and-append.md)
3. [Protocol binding, Storage Epochs, and profiles](02-storage-profiles-and-topic-binding.md)
4. [Object WAL](03-object-wal.md)
5. [BookKeeper and Pulsar](04-bookkeeper-and-pulsar.md)
6. [Manifest, read, retention, and GC](05-manifest-read-retention-gc.md)
7. [Metadata backends and handoff](06-metadata-backends-and-handoff.md)
8. [Protocol integrations and product gates](07-protocol-integrations.md)
9. [Implementation plan and gates](08-implementation-plan-and-gates.md)
10. [Scenario evidence matrix](09-scenario-evidence-matrix.md)
11. [Architecture tradeoffs](tradeoffs.md)
12. [Open questions](open-questions.md) and [grill session records](grill-notes/)
13. [Structured source locks](source-locks.json) and [structured scenarios](v2-scenarios.json)

Accepted decisions:

- [ADR 0007: WAL-linearized append](../decisions/0007-v2-wal-linearized-append.md)
- [ADR 0008: storage profiles and ACK boundaries](../decisions/0008-v2-storage-profiles-and-ack-boundaries.md)
- [ADR 0009: protocol-native data paths](../decisions/0009-v2-protocol-native-data-paths.md)
- [ADR 0010: topic profile binding](../decisions/0010-v2-topic-profile-binding.md) — superseded by ADR 0012
- [ADR 0011: position domains and multi-protocol Storage Fabric](../decisions/0011-v2-position-domains-and-multi-protocol-fabric.md)
- [ADR 0012: Storage Epochs and profile evolution](../decisions/0012-v2-storage-epochs-and-profile-evolution.md)
- [ADR 0013: cross-protocol projection and migration boundary](../decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md)

## Open design gates

| Gate | Required decision/evidence | Must close before |
| --- | --- | --- |
| `V2-OPEN-OBJ-01` | prove per-binding typed durable frontiers inside a multi-binding Object group without shard-wide HOL | M3 layout freeze |
| `V2-OPEN-OBJ-02` | freeze provider verification for PUT-response loss; ETag alone is insufficient | M3 provider contract |
| `V2-OPEN-BK-01` | choose native ManagedLedger offload authority or an exact metadata integration | M2 Pulsar async-object implementation |
| `V2-OPEN-BK-02` | validate one-active-ledger-per-Kafka-partition at 10k and 100k partitions | M2 Kafka BK layout freeze |
| `V2-OPEN-MIGRATION-01..03` | freeze supported profile transitions, runtime states, and historical-data policy | any online profile-transition implementation |
| `V2-OPEN-PUL-MIGRATION-01` | choose new-incarnation cutover or a proven hybrid ledger model | Pulsar BookKeeper/Object transition |
| `V2-OPEN-PROJECTION-01..03` | freeze map granularity, authority-transfer failure semantics, and protocol semantics | Kafka/Pulsar projection or migration runtime |
| `V2-OPEN-FABRIC-01` | freeze namespace, quota, credentials, and failure isolation across cells | production multi-cell provider sharing |
| `V2-OPEN-BENCH-01` | pin clean AutoMQ and native Pulsar acceptance baselines plus thresholds | M8 performance execution |

## Maintenance rule

Every V2 milestone change must update the normative contract, affected ADR/context language, tradeoff/open-question
status, scenario manifest, and gate in the same coherent change as the implementation. Confirmed decisions move out of
the non-normative question log; unconfirmed proposals never become contracts by repetition. Evidence is append-only and
source-qualified; an older PASS never becomes current-source evidence automatically.
