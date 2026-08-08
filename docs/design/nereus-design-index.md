---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: DocumentationOnly
authority: ProductDesignIndex
sourceTuple: v2-m0
---

# Nereus design index

## Current product line

`main` is the Nereus V2 development line and uses version `0.2.0-SNAPSHOT`. M0 establishes the V2 documentation
baseline; it does not claim that the current Java implementation already satisfies V2.

The V1 product line is preserved at
`v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`. Existing Phase/Future documents and their historical receipts describe
that line unless a document explicitly declares `productLine: V2`.

## Product direction

V2 replaces V1's per-append remote-metadata commit model with:

- exclusive Owner Epochs and protocol-native serialized writer lanes;
- process-local position allocation inside the binding's Kafka or Pulsar Position Domain;
- ACK after the returned typed Protocol Coverage is durable in the selected primary WAL;
- zero remote control-metadata reads and mutations on normal admitted append;
- immutable manifests and asynchronous read-optimized materialization;
- immutable Topic Protocol Bindings plus append-only, profile-bearing Storage Epochs;
- Kafka KRaft and Pulsar MetadataStore/Oxia control backends;
- independent Kafka and Pulsar Protocol Cells over shared storage lifecycle contracts.

The architecture is summarized in [Nereus overall architecture](nereus-overall-architecture.md). Normative details start
at the [V2 design index](../v2/README.md); domain boundaries and vocabulary start at the
[V2 Context Map](../../CONTEXT-MAP.md).

## V2 authority

Conflicts are resolved in this order:

1. accepted V2 ADRs;
2. normative files in `docs/v2/`;
3. milestone implementation and exact-source executable receipts;
4. structured scenarios and implementation plan;
5. V1 documents and external research.

Current accepted decisions:

- [ADR 0006: clean break from V0.1](../decisions/0006-v0.2-clean-break-from-v0.1.md)
- [ADR 0007: WAL-linearized append](../decisions/0007-v2-wal-linearized-append.md)
- [ADR 0008: storage profiles and ACK boundaries](../decisions/0008-v2-storage-profiles-and-ack-boundaries.md)
- [ADR 0009: protocol-native data paths](../decisions/0009-v2-protocol-native-data-paths.md)
- [ADR 0010: topic profile binding](../decisions/0010-v2-topic-profile-binding.md) — superseded by ADR 0012
- [ADR 0011: position domains and multi-protocol Storage Fabric](../decisions/0011-v2-position-domains-and-multi-protocol-fabric.md)
- [ADR 0012: Storage Epochs and profile evolution](../decisions/0012-v2-storage-epochs-and-profile-evolution.md)
- [ADR 0013: cross-protocol projection and migration boundary](../decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md)

## V2 document map

| Document | Design status | Implementation status | Purpose |
| --- | --- | --- | --- |
| [V2 index](../v2/README.md) | Accepted | NotStarted | authority, reading order, open gates |
| [Correctness and append](../v2/01-correctness-and-append.md) | Accepted | NotStarted | ownership, WAL linearization, uncertain append |
| [Protocol binding and Storage Epochs](../v2/02-storage-profiles-and-topic-binding.md) | Accepted | NotStarted | immutable protocol identity and epoch-scoped profiles |
| [Object WAL](../v2/03-object-wal.md) | Accepted | NotStarted | group objects, per-binding typed durable frontier, recovery |
| [BookKeeper and Pulsar](../v2/04-bookkeeper-and-pulsar.md) | Proposed | NotStarted | native positions plus open offload/ledger-layout decisions |
| [Manifest/read/retention/GC](../v2/05-manifest-read-retention-gc.md) | Accepted | NotStarted | typed logical read view and physical lifecycle |
| [Metadata and handoff](../v2/06-metadata-backends-and-handoff.md) | Accepted | NotStarted | KRaft/Oxia capability backends and hint semantics |
| [Protocol integrations](../v2/07-protocol-integrations.md) | Accepted | NotStarted | Kafka targets, Pulsar parity, KoP deferral |
| [Implementation plan](../v2/08-implementation-plan-and-gates.md) | Accepted | NotStarted | M0-M8 and evidence rules |
| [Scenario matrix](../v2/09-scenario-evidence-matrix.md) | Accepted | NotStarted | evidence promotion contract |
| [Tradeoff register](../v2/tradeoffs.md) | Accepted | NotStarted | stable decision IDs, costs, mitigations |
| [Open questions](../v2/open-questions.md) | Proposed | NotStarted | non-normative proposals awaiting explicit confirmation |

Structured sources:

- [V2 source locks](../v2/source-locks.json)
- [V2 scenarios](../v2/v2-scenarios.json)

## Topic profiles and Storage Epochs

V2 exposes exactly:

| Profile | ACK boundary | Objective |
| --- | --- | --- |
| `OBJECT_WAL` | verified durable Object WAL group | cost first |
| `BOOKKEEPER_WAL_ONLY` | BookKeeper quorum | performance first |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | BookKeeper quorum; sealed Protocol Coverage offloads in background | performance first with later cold-cost reduction |

Object API calls are asynchronous, but an Object WAL ACK waits for object durability. The later background operation is
materialization, not a second durability upload. BookKeeper does not synchronously dual-write Object storage.

A Topic Protocol Binding fixes protocol identity, Position Domain, payload mapping, and Native Write Authority for one
Topic Incarnation. Its append-only Storage Epoch chain selects profiles over protocol-native frontier intervals. A
profile is immutable within an epoch; the supported transition matrix and runtime transition state machine remain open.

## Implementation and evidence status

At M0:

- designStatus is Accepted except the explicitly Proposed BookKeeper/Pulsar layout decisions;
- implementationStatus is NotStarted for V2 runtime;
- scenario rows are PLANNED;
- AutoMQ comparison and native Pulsar acceptance baselines are intentionally not pinned;
- V1 current-source PASS receipts are not inherited.

The M0 documentation aggregate is `./gradlew v2M0Check`. Future tasks are registered only when their implementation
owner and executable surface exist.

## V1 historical evidence

The following remain in `main` temporarily because the corresponding V1 code has not yet been replaced. They are
historical implementation evidence, not V2 architecture authority:

- [V1 core stream storage](../phase-1-core-stream-storage/README.md)
- [V1 generic storage foundation](../phase-1.5-core-storage-foundation/README.md)
- [V1 ManagedLedger facade](../phase-2-managed-ledger-facade/README.md)
- [V1 cursor/subscription](../phase-3-cursor-subscription/README.md)
- [V1 materialization/compaction](../phase-4-compaction-generation/README.md)
- [V1 BookKeeper primary WAL](../phase-bk-bookkeeper-primary-wal/README.md)
- [V1 native Kafka integration](../phase-9-kafka-native-storage/README.md)
- [V1 commit protocol](nereus-commit-protocol.md)
- [V1 object format](nereus-storage-object-format.md)
- [V1 terminology](nereus-terminology.md)
- [V1 Futures roadmap](nereus-futures.md)

Each V2 milestone deletes the superseded V1 source, tests, prose, and literal checks in the same coherent change. V1
history is recovered from branch `v0.1`, not by keeping compatibility shims or deprecated APIs on `main`.

## KoP

[KoP/Kafka compatibility](nereus-future5-kop-compatibility.md) is retained as a V1-derived design reference. Its V2
status is Designed / deferred from the 0.2 runtime and release gates. It must be re-audited before activation and is not
deleted as part of the core V2 rewrite.

## Maintenance

Every V2 milestone synchronizes:

1. normative design, affected ADR, and context language;
2. tradeoff/open-question state;
3. Markdown and JSON scenarios;
4. source locks;
5. implementation gate and exact-source receipt.

A source or threshold change invalidates inherited current-source evidence until rerun. Documentation wording alone
cannot promote implementation or acceptance status.
