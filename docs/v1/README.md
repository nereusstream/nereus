---
productLine: V1
documentationStatus: FrozenHistorical
authority: HistoricalEvidenceIndex
sourceTuple: v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b
---

# Nereus V1 documentation archive

This namespace contains the frozen V1 design, Phase/Future contracts, performance notes, release evidence, and
delivery history. It is not current V2 architecture and cannot establish a V2 implementation or evidence claim.

The exact V1 product authority is the protected `v0.1` branch at
`a14d925da5763f36208f8ddca7bef31f3eb90b0b`. Files are moved here without rewriting their historical contracts.

## Archive map

- [Historical delivery log](delivery-log.md)
- [V1 design and Future documents](design/)
- [Phase 0 repository plan](phase0/)
- [Phase 1 Core StreamStorage](phase-1-core-stream-storage/)
- [Phase 1.5 generic storage foundation](phase-1.5-core-storage-foundation/)
- [Phase 2 ManagedLedger facade](phase-2-managed-ledger-facade/)
- [Phase 3 cursor/subscription](phase-3-cursor-subscription/)
- [Phase 4 compaction/generation](phase-4-compaction-generation/)
- [Phase 9 native Kafka storage](phase-9-kafka-native-storage/)
- [BookKeeper primary WAL](phase-bk-bookkeeper-primary-wal/)
- [AutoMQ-like V1 storage design](automq-like-stream-storage/)
- [V1 performance notes](performance/)
- [V1 release record](releases/v0.1.0.md)

## Historical decisions

- [ADR 0002: separate append commit, read-index materialization, and object materialization](decisions/0002-separate-append-commit-index-and-materialization.md)
- [ADR 0004: insert Phase 1.5 generic storage foundation](decisions/0004-insert-phase-1-5-generic-storage-foundation.md)
- [ADR 0005: native Kafka fork and adapter boundary](decisions/0005-native-kafka-fork-and-adapter-boundary.md)

Repository-wide decisions such as the owned Java/Maven namespace remain under `docs/decisions/`. The V1/F9 Kafka
boundary in ADR 0005 carries an explicit V2 successor map. ADR 0006 is the active V1/V2 clean-break decision and
remains transition authority under `docs/decisions/`.
