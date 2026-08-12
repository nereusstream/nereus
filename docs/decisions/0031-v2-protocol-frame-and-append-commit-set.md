# ADR 0031: V2 protocol frame and append commit set

## Status

Accepted for Kafka and Pulsar `OBJECT_WAL`; ADR 0086 extends the same Kafka frame/commit-set boundary to both Kafka
BookKeeper-primary profiles. Implementation and runtime evidence are not started.

## Context

ADR 0026 freezes exact protocol-native payload bytes but leaves frame and append atomicity granularity open. A Kafka
partition storage append can contain multiple complete record batches even though the common magic-v2 produce path
usually contains one. One Pulsar add operation persists exactly one ManagedLedger entry, which may itself contain a
client batch, compression, encryption, or a transaction marker. Network requests and transactions have different
boundaries from both storage append paths.

## Decision

Kafka uses two explicit layers:

- one `KafkaFrame` is one complete raw `RecordBatch` after the broker assigns offsets and leader epoch;
- every frame decoded from one partition's single `MemoryRecords` storage append belongs to one
  `KafkaAppendCommitSet`.

Kafka frame coverage is the batch header's exact `[baseOffset, lastOffset]`; it is never inferred from record count.
The commit set carries bounded frame count and ordinal membership. Every frame boundary, length, CRC32C/v1 frame
checksum, native Kafka checksum, coverage, and ordinal/count must validate, and every member must be durable, before
any member is visible or acknowledged and before the binding Durable Frontier advances. A partial commit-set prefix is
never published as a successful append.

One `PulsarFrame` is the exact byte sequence of one ManagedLedger entry and therefore one `(ledgerId, entryId)`. One
`asyncAddEntry` is already one frame/commit set. A Pulsar client batch remains within that entry and is not split into
application-message frames.

An Object Extent may physically group multiple Kafka commit sets and Pulsar frames from compatible bindings. Object
group/PUT boundaries do not redefine protocol append atomicity. A network request, transaction, individual Kafka
record, or individual message inside a Pulsar batch is not a frame or commit set. Native protocol checksum and semantic
validation remain independent of the V2 frame checksum.

ADR 0064 separates the shared Object's physical resolution from each commit set's typed frontier. After the Object,
KMS envelope, fixed header, and directory authenticate, a complete Kafka commit set or Pulsar entry may validate and
advance independently. A failure inside one complete frame/commit set blocks that unit; it does not split the unit or
automatically fail another binding's independently verified unit. A shared Object/header/directory failure still blocks
all members.

ADRs 0066/0067 reserve tracker/locator capacity before position allocation and assign exactly one owner-local
completion ticket to one whole Kafka append commit set or Pulsar entry. Frames, Kafka records, and Pulsar batched
messages never become independent ticket/ACK units. One shared verified extent feeds their compact read locators before
frontier publication and ACK.

For Kafka BookKeeper-primary profiles, ADR 0086 persists the same complete commit-set evidence in the BookKeeper data
path. One assigned RecordBatch remains one lookup/DATA unit, while one partition storage append remains the all-or-none
publication/ACK unit. BookKeeper I/O futures may overlap, but the ordered commit frontier cannot pass an earlier group;
the range-index checkpoint is asynchronous and never splits or replaces commit-set authority.

ADR 0087 further requires the publication of a complete Kafka commit set to advance its locator, producer-state delta,
partition transaction/aborted-state delta, Kafka leader-epoch state, append result, and Readable/LEO frontier in one
coherent partition-local cut. A commit set never represents cross-partition transaction atomicity.

## Consequences

- `V2-OPEN-OBJ-08` is resolved.
- Kafka pays for per-batch frame descriptors plus a small all-or-none commit-set envelope, preserving native random
  reads, producer/transaction headers, empty batches, and non-count-derived offset spans.
- Pulsar retains one-entry MessageId, batching, partial-batch-ack, compression, and encryption semantics.
- NWG1 in-body directory authority and commit-set co-location are refined by ADR 0040. Exact field IDs/layout, read
  assembly, and compaction vectors remain downstream gates.
- M3 must prove Kafka multi-batch all-or-none cuts and corrupted membership rejection, Pulsar entry identity, empty
  Kafka batch coverage, native-checksum independence, and Object-group boundaries that do not leak into append truth.

This decision refines ADR 0026 and is further refined by [ADRs 0037](0037-v2-object-wal-binding-context-epoch-authority.md),
[0040](0040-v2-nwg1-append-unit-directory-and-colocation.md),
[0064](0064-v2-object-wal-physical-and-binding-frontiers.md),
[0066](0066-v2-pre-position-reservation-and-completion-ticket.md), and
[0067](0067-v2-active-tail-readable-publication-and-index-boundary.md), with Kafka BookKeeper placement refined by
[0086](0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md) and Kafka protocol publication refined by
[0087](0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md). It is tracked by
`T-PROTOCOL-01`, `T-OBJECT-01`, `V2-OBJ-002/004/006/007/012/021/023`, and `V2-READ-003/004`.
