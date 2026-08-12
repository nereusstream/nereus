---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Kafka BookKeeper offset, run, and range-index direction

Date: 2026-08-12

The user confirmed the Kafka offset direction summarized below. Normative semantics are recorded in
[ADR 0086](../../decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md); the M2 implementation
shape is recorded in
[the detailed design](../detailed_design/m2/kafka-bookkeeper-offset-range-index.md). This note preserves the complete
feedback surface and the adjustments needed to keep it consistent with already accepted V2 contracts.

## Confirmed feedback surface

1. Kafka uses an AutoMQ-like logical Offset Domain; BookKeeper uses a Pulsar-like ledger lifecycle; a layered packed
   range index maps between them.
2. The three useful granularities stay distinct: partition append/Produce commit set for atomicity, RecordBatch for
   read lookup, and sealed ledger run for rollover/materialization/GC/source switching.
3. All profiles share Kafka offsets. Physical generations may move from BK to Object or compacted Object without
   changing offsets. Consumer-group committed offsets remain Kafka cursors and need no special BK map.
4. The existing conceptual `KafkaOffsetRange -> contiguous BookKeeperExtent -> per-entry logical span` mapping is a
   useful starting point. One RecordBatch per entry is preferred initially; absolute offset need not be duplicated in
   every wrapper because the assigned RecordBatch header plus run/checkpoint anchors are available.
5. Per-append Oxia reservation/mapping/protection transitions are too expensive for the normal data path. Owner, run,
   rollover, and generation are low-frequency control-plane facts.
6. A reader that fetches and hashes the full append extent to find one requested batch causes unacceptable random-read
   amplification.
7. The accepted two-level lookup is `KafkaOffsetRange -> BookKeeperRun -> RangeIndexBlock.floor(offset) -> DATA entry`.
8. A ledger may interleave `RUN_HEADER`, `DATA`, `RANGE_INDEX_BLOCK`, and `RUN_FOOTER` control/data entry types. Index
   blocks are asynchronous/rebuildable; ACKed active-tail locators stay in bounded owner memory; seal flushes complete
   index/footer authority.
9. Candidate evidence inputs remain: checkpoint every 1,024 RecordBatches or 1 MiB, 16--64 KiB index blocks,
   4,096--16,384 active-tail locators, 8--32 in-flight appends, and a 4--32 MiB recovery tail. None is a frozen default.
10. Fetch floor-searches run, block, and locator, then reads one DATA entry or a small adjacent range. Cache is an
    acceleration hint, never sole authority.
11. Integrity is layered: BookKeeper digest, Nereus entry CRC, and Kafka RecordBatch CRC/header for routine reads;
    block/run SHA for scrub, seal, recovery, and materialization. Routine Fetch does not recompute one extent-wide hash.
12. Offset admission, BookKeeper I/O, and ordered commit publication are separate stages. Admission and entry
    submission stay ordered; futures may overlap; B completing before A cannot advance the committed frontier.
13. A durable append-group descriptor lives in the BookKeeper data path. A single-batch group may inline it; a
    multi-entry group has one terminal descriptor. Exact same identity/payload converges, conflicting overlap fails.
14. Takeover fences the old owner, resumes from the last valid index checkpoint, scans a bounded tail, accepts only
    complete gap-free commit groups, seals the old run, publishes it, and opens a new run.
15. `OBJECT_WAL` uses its Object directory; `BOOKKEEPER_WAL_ONLY` keeps run/index data in BK; async Object may combine
    sealed BK ranges into a new preferred Object generation while keeping BK protected fallback until all deletion
    conditions pass.
16. Default layout is one logical ledger chain per Kafka partition. A global mixed-partition ledger is rejected because
    it couples retention and loses local entry continuity. Cold-partition pooled lanes are future evidence-driven work.
17. Initial code-shape suggestions were `KafkaBookKeeperBatchLocator`, `BookKeeperRangeIndexBlock`,
    `BookKeeperRunFooter`, `PartitionOffsetSequencer`, `OrderedAppendCommitQueue`,
    `BoundedBookKeeperWritePipeline`, and a new discriminated `NBKE2` frame family. Tiny-batch packing is P2/future.

## Contract adjustments applied during normalization

- V2 0.2 has exactly three canonical profiles. V1 sync/async aliases and the cited five-profile runtime are historical
  residue, not new NTA1 profiles or separate offset designs.
- RecordBatch Offset Range is derived from assigned batch header `baseOffset/lastOffsetDelta`, not `recordCount`.
  `logicalOffsetCount` may equal `lastOffsetDelta + 1` after checked validation.
- The coarse Kafka run/manifest authority follows the accepted Kafka/KRaft capability boundary; this note does not
  create a new per-append Oxia authority.
- `StreamId`, `clusterAlias`, V1 reservation records, NBKE1, and V1 extent-wide `rangeChecksum` are implementation
  evidence only. V2 uses typed Binding/incarnation/partition/epoch/Provider-Scope identities.
- The suggested transitional dual write of old Oxia reservation plus new BK footer/index is rejected by the V2 clean
  break. M2 replaces the old path and removes it in a separate mechanical commit; it does not ship a compatibility gate.
- The example Object/BK generation switches are physical source changes. Online profile/Storage-Epoch transition is
  still outside 0.2 under ADR 0015.
- Concurrent multi-entry groups must reserve and submit contiguous ledger entry ranges in admission order so group
  entries do not interleave. Completion can overlap, but physical/logical publication cannot skip a predecessor.
- DATA and control entries share one entry sequencer: checkpoint/footer entries occur only between commit sets, and a
  checkpoint covers only the greatest contiguous committed prefix. Takeover seals under a separate recovery fence
  without rewriting the old run's creator Owner Epoch; post-terminal provider entries remain inert residue.
- SHA-256/v1 is retained for block/run/group integrity; BLAKE3 is not introduced without a separate algorithm decision.

No implementation, performance receipt, scenario promotion, or M2 PASS was produced by this decision.
