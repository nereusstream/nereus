---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Grill 1 record: protocol position, Storage Fabric, and migration boundaries

Date: 2026-08-08

This record preserves all user feedback from Grill 1. “Confirmed” items are normative only through the linked ADRs and
contracts. “Proposal/open” items remain non-normative and are copied to [open questions](../open-questions.md).

## Confirmed: protocol positions and shared storage

Kafka and Pulsar do not share one `long logicalOffset`, but they may share one Nereus Storage Fabric.

The domain contexts are:

- Kafka Context: `KafkaOffset` is the only Kafka protocol position truth.
- Pulsar Context: `PulsarPosition(ledgerId, entryId)` is the only Pulsar protocol position truth.
- Shared Storage Context: does not create a third position truth.

Shared storage separates:

```text
ProtocolCoverage
    ├── KafkaOffsetRange
    └── PulsarCoverage

PhysicalExtent
    ├── ObjectExtent
    └── BookKeeperExtent
```

The four protocol/storage combinations are:

| Combination | Protocol position truth | Protocol Coverage | Physical Extent |
| --- | --- | --- | --- |
| Kafka + Object WAL | Kafka Offset | Kafka Offset Range | Object Extent |
| Kafka + BookKeeper | Kafka Offset | Kafka Offset Range | BookKeeper Extent |
| Pulsar + BookKeeper | Pulsar Position | Pulsar Coverage | BookKeeper Extent |
| Pulsar + Object WAL | Pulsar Position | Pulsar Coverage | Object Extent |

Kafka on BookKeeper does not expose BookKeeper ledger/entry coordinates as Kafka offsets. Pulsar on Object WAL does not
expose object keys or byte ranges as Pulsar MessageIds.

Pulsar Coverage across ledgers is ledger-keyed rather than subtractable:

```text
PulsarCoverage {
    ledger-10: [500, 1000)
    ledger-11: [0, 300)
}
```

The ManagedLedger/ObjectManagedLedger Ledger Chain proves ordering. V2 does not introduce a permanent
`ledgerBase + entryId` position mapping.

## Confirmed: multi-protocol Storage Fabric

One Storage Fabric may contain multiple Kafka and Pulsar Protocol Cells:

```text
Nereus Storage Fabric
├── Kafka Protocol Cell
│   ├── Kafka brokers
│   ├── KRaft
│   └── Kafka Topic Protocol Bindings
├── Pulsar Protocol Cell
│   ├── Pulsar brokers
│   ├── MetadataStore / Oxia
│   └── Pulsar Topic Protocol Bindings
└── Shared Data Plane
    ├── Object Storage
    ├── BookKeeper
    ├── Manifest / Segment
    ├── Materialization workers
    ├── Cache
    ├── Retention / GC
    └── Observability
```

Kafka and Pulsar topics may use different profiles while sharing Object Storage, and may share a BookKeeper cluster when
isolation rules allow. They may also share materialization, verification, compaction, GC, cache, and operations.

The hard boundary is:

> One Topic Incarnation has one native Position Domain and one Native Write Authority at any time.

Positions are meaningful only with Topic Protocol Binding and Topic Incarnation. Kafka offset `100` from two topics
cannot be compared; the same Pulsar ledger/entry numbers before and after topic recreation identify different positions.

## Confirmed: access projection and authority

The same business data may be accessed through both protocols only as one native authority plus one Access Projection.
The target protocol cannot become a second native position allocator.

Example source/target mapping:

```text
source: KafkaOffsetRange [1000, 2000)
target: PulsarCoverage {
    ledger-501: [0, 600)
    ledger-502: [0, 400)
}
```

Allowing Kafka and Pulsar to allocate native positions concurrently would require a cross-protocol sequencer, competing
owners, per-append control-plane coordination, a global position, and dual-authority recovery. V2 explicitly avoids
that model.

## Confirmed: protocol binding and Storage Epoch

Protocol identity and storage profile are separate:

```text
TopicProtocolBinding
    + append-only StorageEpoch chain
```

Topic Protocol Binding is immutable within a Topic Incarnation and contains:

- Binding ID;
- Protocol Cell ID;
- protocol kind;
- Position Domain;
- payload mapping;
- Topic Incarnation.

Each Storage Epoch is immutable and contains:

- epoch identity;
- one storage profile;
- start/end Protocol Frontier;
- WAL/physical format;
- checksum/encryption family;
- lifecycle state.

A Kafka profile change preserves Kafka Offset continuity while changing the Physical Extent selected after an exact
Kafka frontier. A Pulsar profile change uses an exact frontier in its Ledger Chain rather than a global logical offset.
The domain model does not require dual write.

## Confirmed: migration concepts are distinct

```text
same Storage Fabric, multiple protocols -> Protocol Cell
same data, secondary protocol access     -> Access Projection
same protocol, storage profile change    -> Storage Epoch
Kafka/Pulsar authority change            -> Migration Link + Projection Map
```

Cross-protocol migration is not a Storage Epoch change because the Topic Protocol Binding and Native Write Authority
change.

## Proposal/open: Storage Epoch transition mechanics

The proposed sequence is:

```text
ACTIVE_OLD
    ↓
PREPARING_NEW_EPOCH
    ↓
DRAINING_OLD_WRITER
    ↓
OLD_EPOCH_SEALED
    ↓
NEW_EPOCH_ACTIVE
    ↓
MATERIALIZING_HISTORY
    ↓
RETIRING_OLD_PHYSICAL
    ↓
COMPLETED
```

Proposed rules:

1. create the new epoch as prepared;
2. stop old-epoch append admission;
3. drain already accepted requests;
4. seal the old WAL;
5. persist the exact protocol-native cutover frontier;
6. activate the new epoch;
7. optionally migrate historical data;
8. delete old physical data only after reader, retention, projection, task, and GC proofs pass.

The exact states and failure semantics are not yet accepted.

## Proposal/open: transition difficulty and staging

The proposed order from easiest to hardest is:

1. Pulsar `BOOKKEEPER_WAL_ONLY` ↔ `BOOKKEEPER_WAL_ASYNC_OBJECT`, because primary WAL and Position stay BookKeeper.
2. Kafka `OBJECT_WAL` ↔ BookKeeper profiles, because Kafka Offset stays stable across an epoch frontier.
3. Pulsar BookKeeper ↔ Object WAL, because stock ManagedLedger assumes BookKeeper-backed ledger semantics.
4. Kafka ↔ Pulsar authority migration, which is a cross-protocol saga rather than a profile change.

For Pulsar BookKeeper ↔ Object WAL, the initial proposal uses:

```text
old Topic Incarnation
    -> backfill / catch-up
new Topic Incarnation
    -> alias / routing cutover
```

A future alternative is `HybridManagedLedger` with Object virtual ledgers and real BookKeeper ledgers in one Ledger
Chain. Neither implementation choice is accepted.

## Proposal/open: cross-protocol migration

The proposed saga is:

```text
SOURCE_ACTIVE
    ↓
TARGET_PREPARED
    ↓
BACKFILLING
    ↓
TAIL_CATCHING_UP
    ↓
TARGET_CAUGHT_UP
    ↓
SOURCE_FENCED
    ↓
TARGET_ACTIVATED
    ↓
SOURCE_RETIRED
```

Projection Map may be persisted per segment rather than per message. Example:

```text
Pulsar: (ledger-10, entry-5, batchIndex=0..2)
Kafka:  KafkaOffsetRange [100, 103)
```

The unresolved semantic set includes consumer groups/subscription cursors, partial batch ACK, transactions, compaction
tombstones, delayed delivery, Key_Shared, schemas, properties/headers, and producer deduplication. A universal logical
offset does not resolve these semantics.

## Excluded from this round

KoP implementation and KoP design changes are excluded. The feedback noted that a future KoP path could act as a Kafka
protocol projection over a Pulsar-native topic, but this is retained only as session input and is not a current V2
contract.

## Documentation actions requested by the feedback

- ADR 0011 records Position Domains and the multi-protocol Storage Fabric.
- ADR 0012 records Storage Epochs and profile evolution.
- ADR 0013 records the Access Projection/Migration Link boundary.
- ADR 0007 and ADR 0009 are refined by ADR 0011.
- ADR 0010 is superseded by ADR 0012.
- the context map and glossaries define the ubiquitous language.
- tradeoffs add `T-POSITION-01`, `T-MULTIPROTOCOL-01`, `T-MIGRATION-01`, and `T-PROJECTION-01`.
- manifests, handoff, trim, and GC use binding-scoped typed coverage/frontiers.
