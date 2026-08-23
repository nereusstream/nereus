---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: NormativeImplementationSlice
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json
---

# M2 Kafka K6 packed targeted and sequential reader

K6 turns K5's coherently published active-tail members and K0-W `RANGE_INDEX_BLOCK` rows into one common
BookKeeper read path. `KafkaPackedBatchLocatorIndexV1` and `KafkaPackedIndexDirectoryV1` retain locator and footer
rows in primitive arrays. They perform coverage-aware floor lookup: a floor row is accepted only when its complete
RecordBatch range covers the requested offset; otherwise lookup selects the first successor across a block or run.
The long-lived representation is not a Java object or `TreeMap` per RecordBatch.

`KafkaBookKeeperReadSnapshotV1` binds one immutable K1 root to the exact run table and transaction view used by the
request. Every run repeats the Binding, Topic Incarnation, partition, Storage Epoch and provider identity. An old
sealed run may carry an earlier Owner or Kafka leader epoch, but neither may exceed the captured fence. The active run
must match the exact published active-tail generation. Replica, `read_uncommitted`, and `read_committed` select
Readable/LEO, HW, and LSO respectively. Durable end is never consulted as a read upper bound, and no locator whose
complete coverage crosses the captured upper bound is returned.

For a sealed run, `KafkaBookKeeperTargetedReaderV1` reads or cache-hits exactly one bounded index block, validates its
provider entry digest, NBKE2 ledger/entry identity, entry-local CRC and run/directory binding, then reads the selected
DATA entry. The DATA path repeats provider digest and NBKE2 validation and independently parses the raw Kafka magic-v2
header, strict batch length, assigned offset coverage, leader epoch, and Kafka CRC32C through the K2 validator. Active
tail reads already own the exact packed member locator and therefore target the DATA entry directly. Ordinary reads
never fetch a whole append extent, run, footer checksum domain, or append-group aggregate merely to validate an old
RecordBatch. The index-block LRU is bounded disposable acceleration, not authority.

Sequential reads preserve complete RecordBatch boundaries. The byte budget controls coalescing; the first batch is
returned whole even when it exceeds the budget, and later batches are never split. A returned
`KafkaBookKeeperReadCursorV1` carries run identity, captured fence, source generation, index-block identity, locator
ordinal, next entry ID, next Kafka offset, and snapshot state version. Each request captures a fresh snapshot and
accepts the cursor only on exact agreement. A mismatch discards it and replans through the packed lookup; the cursor
never retains a provider or source-generation pin across requests.

`read_committed` returns the validated native DATA/control stream through LSO together with the coherently captured
aborted-transaction metadata whose range intersects the response. Storage does not filter aborted bytes. Definitive
absence, fencing, provider failure, and corruption are distinct closed outcomes and never become an empty successful
read.

`v2M2KafkaK6Check` executes 23 zero-skip tests in three suites. It covers packed floor/coverage/successor behavior,
directory/run compaction gaps, active per-member flattening, sealed one-block-plus-one-DATA reads, active direct reads,
bounded cache eviction, provider outcomes, NBKE2 and raw Kafka CRC corruption, LEO/HW/LSO bounds, byte-budget and
first-oversized behavior, cross-block sequential reads, cursor acceptance/invalidation, aborted metadata, and
complete-batch upper-bound refusal.

This focused fake-provider/local-state gate alone proves no Kafka broker Fetch adapter, delayed Fetch/purgatory
integration, ACK/HW/LSO advancement, real BookKeeper behavior, scenario promotion, Kafka Final, or global M2 PASS.
Kafka Final now binds it with K7-K10, and K9 selected the operational cache, cursor, block, tail, and read-coalescing
values from real-BookKeeper evidence.
