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

A profile is immutable within one Storage Epoch. The durable model permits an append-only epoch chain with
protocol-native cutover frontiers, but ADR 0015 limits 0.2 to exactly one initial epoch per Topic Incarnation and no
online profile-transition API/state machine. Operational batching, cache, throttling, and compaction policy remain
separately mutable.

ADR 0016 retains Access Projection/Migration Link identities and rejects a second Native Write Authority, while
excluding cross-protocol serving and authority-transfer runtime from 0.2. For Pulsar
`BOOKKEEPER_WAL_ASYNC_OBJECT`, ManagedLedger ledger/offload metadata is the sole lifecycle authority and Nereus supplies
a sealed-ledger `LedgerOffloader`; any Nereus manifest is derived. Binding plus initial epoch become visible atomically.
Pulsar Object WAL positions come from a cell-owned virtual-ledger authority, never Object identity. Object WAL uncertain
PUTs use capability-tiered HEAD/full-GET proof and separate Object-extent/payload checksum domains.

Provider sharing is physical, not authoritative. Multiple cells may use the same external Object Storage or BookKeeper
infrastructure, while each cell owns its Cell Provider Scope/session, namespace, credential/KMS and operator scope,
admission, task/cache roots, and GC authority. Object groups do not cross Protocol Cells in 0.2. Compatible transport
pooling is optional and remains below the independently owned sessions.

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
- [ADR 0014: provider sharing and Protocol Cell isolation](../decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md)
- [ADR 0015: 0.2 Storage Epoch runtime scope](../decisions/0015-v2-0.2-storage-epoch-runtime-scope.md)
- [ADR 0016: 0.2 cross-protocol runtime scope](../decisions/0016-v2-0.2-cross-protocol-runtime-scope.md)
- [ADR 0017: Pulsar ManagedLedger offload authority](../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md)
- [ADR 0018: Object WAL uncertain PUT proof](../decisions/0018-v2-object-wal-uncertain-put-proof.md)
- [ADR 0019: initial binding/epoch atomic visibility](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md)
- [ADR 0020: Pulsar sealed-ledger async offload](../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md)
- [ADR 0021: Object WAL checksum domains](../decisions/0021-v2-object-wal-checksum-domains.md)
- [ADR 0022: Pulsar Object WAL virtual-ledger authority](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md)

## Open design gates

`V2-OPEN-FABRIC-01` was resolved by ADR 0014. ADRs 0015 through 0018 resolve `V2-OPEN-MIGRATION-01`,
`V2-OPEN-PROJECTION-SCOPE-01`, `V2-OPEN-BK-01`, and `V2-OPEN-OBJ-02`; ADRs 0019 through 0022 resolve
`V2-OPEN-META-01`, `V2-OPEN-BK-03`, `V2-OPEN-OBJ-04`, and `V2-OPEN-PUL-OBJ-01`. The rows below are the remaining active
0.2 decisions or evidence gates.

| Gate | Required decision/evidence | Must close before |
| --- | --- | --- |
| `V2-OPEN-META-02` | freeze the physical record shape behind the accepted Topic Binding Aggregate | M1 binding/epoch store freeze |
| `V2-OPEN-OBJ-01` | prove per-binding typed durable frontiers inside a multi-binding Object group without shard-wide HOL | M3 layout freeze |
| `V2-OPEN-OBJ-05` | freeze initial extent/frame algorithms and provider-bound proof fields/fallback | M3 Object format/provider contract |
| `V2-OPEN-OBJ-06` | freeze canonical Kafka and Pulsar frame-payload byte mappings | M3 Object format/protocol mapping |
| `V2-OPEN-BK-02` | validate one-active-ledger-per-Kafka-partition at 10k and 100k partitions | M2 Kafka BK layout freeze |
| `V2-OPEN-BK-04` | freeze the sealed-ledger data extent/root layout and partial-attempt cleanup | M2 Pulsar offloader layout freeze |
| `V2-OPEN-PUL-OBJ-02` | freeze numeric compatibility and enforceable namespace reservation for virtual ledger IDs | M1/M3 Pulsar Object path freeze |
| `V2-OPEN-BENCH-01` | pin clean AutoMQ and native Pulsar acceptance baselines plus thresholds | M8 performance execution |

`V2-OPEN-MIGRATION-02..03`, `V2-OPEN-PUL-MIGRATION-01`, and `V2-OPEN-PROJECTION-01..03` remain recorded as deferred
future-design questions. ADRs 0015 and 0016 make them non-blocking for 0.2.

## Maintenance rule

Every V2 milestone change must update the normative contract, affected ADR/context language, tradeoff/open-question
status, scenario manifest, and gate in the same coherent change as the implementation. Confirmed decisions move out of
the non-normative question log; unconfirmed proposals never become contracts by repetition. Evidence is append-only and
source-qualified; an older PASS never becomes current-source evidence automatically.
