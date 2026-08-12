---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesignWithOpenEvidence
sourceTuple: v2-m0
---

# M2 Kafka BookKeeper offset, run, and range-index design

## Delivery boundary

This design implements [ADR 0086](../../../decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md)
and is refined by [the Kafka protocol-frontier design](kafka-produce-fetch-frontiers-and-recovery.md).
It is not part of M1/K1. M1 establishes topic metadata authority; M2 establishes the Kafka BookKeeper data path.
Existing V1 appender/reader/reservation code is evidence only and is replaced rather than wrapped, dual-written, or
retained as a compatibility path.

M2 completes one coherent vertical slice:

1. typed Kafka-partition BookKeeper run domain and lifecycle;
2. `NBKE2` discriminated `RUN_HEADER`, `DATA`, `RANGE_INDEX_BLOCK`, and `RUN_FOOTER` frames;
3. one-RecordBatch-per-DATA-entry mapping and packed immutable index blocks;
4. pre-position locator/tracker reservation, local offset admission, bounded async writes, and ordered commit;
5. targeted Fetch plus active/sealed floor indexes;
6. checkpoint-tail recovery, run seal, and response-loss reconciliation;
7. deterministic faults, real BookKeeper validation, resource/scale receipts, and documentation gates.

Tiny-batch packing, shared pooled ledgers, and online profile transitions are excluded.

## Authority and units

| Unit | Purpose | Durable authority |
| --- | --- | --- |
| `KafkaAppendCommitSet` | atomic visibility/ACK | BookKeeper DATA members plus terminal append-group descriptor |
| complete Kafka RecordBatch | offset floor lookup and targeted read | authenticated `NBKE2 DATA` plus `RangeIndexBlock` locator |
| `KafkaBookKeeperRun` | owner/rollover/recovery/materialization/retirement | low-frequency run root/manifest plus `RUN_HEADER/RUN_FOOTER` |

Kafka Offset and assigned RecordBatch headers are logical truth. Ledger IDs, entry IDs, Object keys, and byte offsets
are physical. Consumer-group committed offset uses the same partition floor lookup and has no storage-specific map.

## Run model

The production model must use typed Binding/Topic/partition/epoch identities rather than V1 `StreamId` or
`clusterAlias` authority. A logical model has at least:

```text
KafkaBookKeeperRunV1
  bindingId
  topicIncarnation
  partitionId
  storageEpochId
  ownerEpoch
  kafkaLeaderEpoch
  providerScopeIdentity
  runId
  ledgerId
  ledgerRootEpoch
  kafkaStartOffset
  kafkaEndOffset?                 // absent only while ACTIVE
  firstDataEntryId
  lastPhysicalEntryIdExclusive?
  latestIndexBlockEntryId?
  footerEntryId?
  state = ACTIVE | SEALED | RETIRED
```

The manifest stores coarse run coverage. It never stores one row per append or RecordBatch. ACTIVE run lookup also
uses the owner-local tail view; SEALED lookup uses the final footer/index directory.

## `NBKE2` framing obligations

The exact wire table is a required implementation-readiness child, but it must preserve these semantics:

- every entry starts with closed magic/version/type and checked total length;
- DATA contains one exact raw broker-assigned Kafka RecordBatch in the first implementation;
- RUN_HEADER, DATA, commit descriptor, range-index anchor, protocol checkpoints, and RUN_FOOTER bind the run's exact
  Kafka leader epoch; raw batch fields are cross-checked where applicable;
- DATA validates `baseOffset`, `lastOffsetDelta`, and raw batch CRC; record count is not offset span;
- the terminal DATA/control payload binds complete append-group range, membership, physical bounds, epochs, identity,
  and payload digest;
- RANGE_INDEX_BLOCK is immutable, packed, checksummed, ordered, gap-free within its declared Kafka/entry bounds, and
  contains no object graph or map encoding;
- RUN_FOOTER binds all physical terminals and authenticates the complete ordered index directory/root;
- every integer/length/count uses checked arithmetic, bounded allocation, strict EOF, and corruption rejection.

One ledger-entry sequencer orders both DATA and control entries. It submits every DATA member of a commit set
contiguously; index/footer control entries may appear only between commit sets. A range-index block covers only the
greatest contiguous committed/ACK-eligible prefix and cannot itself advance a frontier.

The initial implementation uses SHA-256/v1 for group/block/run integrity and CRC32C/v1 for entry-local Nereus framing.
BookKeeper digest and Kafka RecordBatch CRC remain separate validation layers.

## Packed locator model

The logical locator is:

```text
KafkaBookKeeperBatchLocator
  baseOffsetDelta
  logicalOffsetCount              // lastOffsetDelta + 1, checked
  entryIdDelta
  appendGroupDelta
  payloadOffset?                  // absent/zero in first implementation
  payloadLength?                  // optional optimization, not authority without DATA validation
```

Exact fixed/varint representation and block caps are evidence-driven. Decoder allocation follows actual validated
locator count. Runtime uses primitive arrays or direct packed views, not one long-lived Java object per locator and not
a general `TreeMap`.

## Write pipeline

The owner executes:

```text
combined tracker/locator capacity reservation
  -> Kafka-native offset admission
  -> contiguous ledger-entry reservation/submission in admission order
  -> bounded overlapping BookKeeper futures
  -> per-group exact validation
  -> ordered contiguous commit queue
  -> hidden locator installation
  -> acquire exact partition publication cut
  -> fenced-CAS/check Binding/incarnation/storage/owner/Kafka-leader/stateVersion
  -> coherent locator + producer/transaction/leader-epoch + Readable/Durable/LEO publication
  -> release cut and wake local waiters/replica progress
  -> optional response-time fence check
  -> ACK or outcome-unknown/native fenced error
```

Offsets and entry IDs are assigned in admission order. Completion may be out of order; publication may not. Index
checkpoint publication is asynchronous and never delays ACK. Hard memory/in-flight limits are reserved before offset
allocation and cause backpressure before a hole can be created.

Kafka leadership/Owner/Storage transitions compete on the same publication cut. If a transition wins first, the stale
callback cannot advance a frontier or wake a reader. If publication wins first, the commit set legally belongs to the
old Kafka leader epoch even when the later response check withholds success. A Kafka leader-epoch change always closes
the old ACTIVE run and opens a new run.

## Read path

The common random-read path is:

```text
run floor lookup
  -> index-block floor lookup
  -> cached or one-block read
  -> packed-locator floor + coverage check + cross-block/run successor
  -> targeted DATA entry read
  -> BK digest + NBKE2 CRC + Kafka header/CRC validation
```

The reader must not fetch the complete append extent merely to validate an old range checksum. Compaction holes select
the first surviving successor batch when the floor does not cover the requested offset. A Fetch spanning
multiple adjacent RecordBatches may coalesce the minimum continuous entry range after each requested locator is
validated.

## Recovery and seal

Takeover invalidates old local admission, opens the old ACTIVE ledger, selects the last compatible range-index /
producer-state / transaction-index / leader-epoch checkpoint vector, and scans the bounded tail. Complete group
descriptors determine `physicalRecoveredEndOffset`, not an automatically adopted Kafka LEO. Native election supplies
the elected replica's `electionAdoptableEndOffset`; the candidate catches `replicaAppliedEndOffset` up to that boundary
and starts with `min(physicalRecoveredEndOffset,electionAdoptableEndOffset)`. Later physical residue is inert. Recovery
reconstructs producer/transaction/first-unstable/leader-epoch state; native Kafka separately restores/recomputes HW and
then derives LSO. Recovery finalizes index coverage/footer, publishes SEALED, and opens a new leader-epoch-bound run.
Missing/corrupt checkpoints fall back only within the declared cumulative recovery envelope; they do not justify an
unbounded full-ledger scan.

The old run retains its creator Owner Epoch. A qualified recovery/seal fence is a separate footer fact, and only the
fresh run admits with the new Owner Epoch. Provider entries beyond the recovered logical end are inert residue.

## Implementation cuts

Recommended commits are reviewable and independently gated:

1. domain/state/validator plus wire design and goldens;
2. `NBKE2` codecs and corruption tests;
3. run allocator/header/footer and fake BookKeeper cuts;
4. offset sequencer, entry sequencer, bounded pipeline, and ordered commit queue;
5. index builder/checkpointer, active-tail locator, and targeted reader;
6. takeover/tail recovery and response-loss matrix;
7. real BookKeeper, 10k/100k scale, read-amplification and throughput evidence;
8. remove the replaced V1 path and close the M2 gate in a separate mechanical change.

No cut introduces a dual-write compatibility mode.

## Open evidence-derived values

The following are candidates only until M2 receipts select them:

- index checkpoint batch/byte cadence;
- block encoded bytes and locators per block;
- active-tail locator count/bytes/age;
- recovery-tail entries/bytes/time;
- per-partition in-flight groups and global in-flight bytes;
- run rollover size/entry/age thresholds;
- handle-cache and open-ledger admission at 10k/100k partitions.

Topic policy cannot enlarge hard format/recovery bounds. Cell/host pressure may backpressure or seal early but cannot
change an existing run's bytes or recovery interpretation.
