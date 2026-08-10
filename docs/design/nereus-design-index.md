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
- independent Kafka and Pulsar Protocol Cells over shared storage lifecycle contracts;
- Cell-scoped Provider scopes/sessions over optionally shared physical provider infrastructure.
- typed Topic/Tenant, Protocol Cell/shard, and host/process policy scopes that cannot disable correctness or reinterpret
  persisted state after failover.

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
- [ADR 0014: provider sharing and Protocol Cell isolation](../decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md)
- [ADR 0015: 0.2 Storage Epoch runtime scope](../decisions/0015-v2-0.2-storage-epoch-runtime-scope.md)
- [ADR 0016: 0.2 cross-protocol runtime scope](../decisions/0016-v2-0.2-cross-protocol-runtime-scope.md)
- [ADR 0017: Pulsar ManagedLedger offload authority](../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md)
- [ADR 0018: Object WAL uncertain PUT proof](../decisions/0018-v2-object-wal-uncertain-put-proof.md)
- [ADR 0019: initial binding/epoch atomic visibility](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md)
- [ADR 0020: Pulsar sealed-ledger async offload](../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md)
- [ADR 0021: Object WAL checksum domains](../decisions/0021-v2-object-wal-checksum-domains.md)
- [ADR 0022: Pulsar Object WAL virtual-ledger authority](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md)
- [ADR 0023: Topic Binding Aggregate physical record](../decisions/0023-v2-topic-binding-aggregate-record.md)
- [ADR 0024: Pulsar sealed-ledger Object layout](../decisions/0024-v2-pulsar-sealed-ledger-object-layout.md)
- [ADR 0025: initial checksum algorithms and provider proof](../decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md)
- [ADR 0026: protocol-native frame payload bytes](../decisions/0026-v2-protocol-native-frame-payload-bytes.md)
- [ADR 0027: Pulsar virtual-ledger numeric compatibility](../decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md)
- [ADR 0028: Topic incarnation keys and deterministic IDs](../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md)
- [ADR 0029: Pulsar sealed-ledger root and lifecycle](../decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md)
- [ADR 0030: Object WAL run root and content-addressed discovery](../decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md)
- [ADR 0031: protocol frame and append commit set](../decisions/0031-v2-protocol-frame-and-append-commit-set.md)
- [ADR 0032: Pulsar virtual-ledger reservation registry](../decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md)
- [ADR 0033: Topic Binding Aggregate logical schema v1](../decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md)
- [ADR 0034: Kafka feature level 2 bootstrap activation](../decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md)
- [ADR 0035: Pulsar NPO1 sealed-ledger root format](../decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md)
- [ADR 0036: Pulsar native dual-source read and deletion safety](../decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md)
- [ADR 0037: Object WAL binding-context epoch authority](../decisions/0037-v2-object-wal-binding-context-epoch-authority.md)
- [ADR 0038: Object WAL provider-absent crash contract](../decisions/0038-v2-object-wal-provider-absent-crash-contract.md)
- [ADR 0039: bounded WalRun lifecycle, recovery, and root pointer](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md)
- [ADR 0040: NWG1 append-unit directory and co-location](../decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md)
- [ADR 0041: Pulsar virtual-ledger slice contract](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md)
- [ADR 0042: Kafka topic aggregate KRaft record and image ownership](../decisions/0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md)
- [ADR 0043: Pulsar topic generation selector and retired tombstone](../decisions/0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md)
- [ADR 0044: Pulsar NPD1 sealed-ledger data blocks](../decisions/0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md)
- [ADR 0045: Pulsar dual-source read handle and source pins](../decisions/0045-v2-pulsar-dual-source-read-handle-and-pins.md)
- [ADR 0046: NWG1 run key, AEAD, and authenticated directory](../decisions/0046-v2-nwg1-run-key-aead-and-authenticated-directory.md)
- [ADR 0047: WalRun Root, seal, and successor publication](../decisions/0047-v2-walrun-root-seal-and-successor-publication.md)
- [ADR 0048: Pulsar virtual-ledger fixed-slice exhaustion](../decisions/0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md)
- [ADR 0049: configuration scopes and persisted semantics](../decisions/0049-v2-configuration-scopes-and-persisted-semantics.md)
- [ADR 0050: Kafka aggregate wire and publication validation](../decisions/0050-v2-kafka-aggregate-wire-and-publication-validation.md)
- [ADR 0051: Pulsar selector state machine and cached fence](../decisions/0051-v2-pulsar-selector-state-machine-and-cached-fence.md)
- [ADR 0052: Pulsar BookKeeper delete state and retention policy](../decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md)
- [ADR 0053: WalRun checkpoint bounds and open-tail recovery](../decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md)
- [ADR 0054: Pulsar virtual-ledger bootstrap geometry](../decisions/0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md)
- [ADR 0055: Pulsar virtual-ledger allocator evidence protocol](../decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md)
- [ADR 0056: NPD1 checked envelope and derived entry row](../decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md)
- [ADR 0057: NPD1 policy default authority and evidence](../decisions/0057-v2-npd1-policy-default-authority-and-evidence.md)
- [ADR 0058: NWG1 directory-prefix capacity and evidence](../decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md)
- [ADR 0059: Object WAL leaf directory-prefix hint](../decisions/0059-v2-object-wal-leaf-prefix-hint.md)
- [ADR 0060: WalRun lazy lanes and vector checkpoint](../decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md)
- [ADR 0061: Pulsar range-grant owner takeover](../decisions/0061-v2-pulsar-range-grant-owner-takeover.md)
- [ADR 0062: Object WAL packing catalog and leaf sequence](../decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md)
- [ADR 0063: provider-resolved checkpoint publisher](../decisions/0063-v2-provider-resolved-checkpoint-publisher.md)
- [ADR 0064: Object WAL physical and binding frontiers](../decisions/0064-v2-object-wal-physical-and-binding-frontiers.md)
- [ADR 0065: physical checkpoint row and Seal payload](../decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md)
- [ADR 0066: pre-position reservation and completion ticket](../decisions/0066-v2-pre-position-reservation-and-completion-ticket.md)
- [ADR 0067: active-tail readable publication and index boundary](../decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md)
- [ADR 0068: checkpoint provider-proof mode and row encoding](../decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md)
- [ADR 0069: Binding read-view generation and pin boundary](../decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md)
- [ADR 0070: generation-tagged read publication and hazard slots](../decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md)
- [ADR 0071: durable owner-read quiescence and protection release](../decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md)
- [ADR 0072: slot lease word and terminal source drain](../decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md)
- [ADR 0073: Read Admission Epoch and source-independent quiescence window](../decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md)
- [ADR 0074: quiescence capability evidence and historical binding](../decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md)
- [ADR 0075: Binding read selector and fallback-interval linearization](../decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md)
- [ADR 0076: Read Admission Epoch terminal cut and on-demand proof](../decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md)

## V2 document map

| Document | Design status | Implementation status | Purpose |
| --- | --- | --- | --- |
| [V2 index](../v2/README.md) | Accepted | NotStarted | authority, reading order, open gates |
| [Correctness and append](../v2/01-correctness-and-append.md) | Accepted | NotStarted | ownership, WAL linearization, uncertain append |
| [Protocol binding and Storage Epochs](../v2/02-storage-profiles-and-topic-binding.md) | Accepted | NotStarted | immutable protocol identity and epoch-scoped profiles |
| [Object WAL](../v2/03-object-wal.md) | Accepted | NotStarted | group objects, per-binding typed durable frontier, recovery |
| [BookKeeper and Pulsar](../v2/04-bookkeeper-and-pulsar.md) | Proposed | NotStarted | native positions and sealed-ledger Object pair; open Kafka ledger scale gate |
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
profile is immutable within an epoch. In 0.2 the runtime creates exactly one initial epoch per incarnation and exposes no
online profile transition. Binding plus initial epoch are one immutable physical aggregate record keyed by a typed
protocol-native incarnation with deterministic binding/epoch IDs and one closed logical schema v1. Kafka activates it
only through fresh-bootstrap feature level 2; the chain model is retained for future evolution.

## Implementation and evidence status

At M0:

- designStatus is Accepted except the Kafka BookKeeper layout that remains Proposed behind its scale gate;
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
