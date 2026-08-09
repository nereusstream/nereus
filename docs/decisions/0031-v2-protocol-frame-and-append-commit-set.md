# ADR 0031: V2 protocol frame and append commit set

## Status

Accepted for Kafka and Pulsar `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

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

## Consequences

- `V2-OPEN-OBJ-08` is resolved.
- Kafka pays for per-batch frame descriptors plus a small all-or-none commit-set envelope, preserving native random
  reads, producer/transaction headers, empty batches, and non-count-derived offset spans.
- Pulsar retains one-entry MessageId, batching, partial-batch-ack, compression, and encryption semantics.
- Exact frame/commit-set binary encoding, index layout, read assembly, and compaction vectors remain downstream gates.
- M3 must prove Kafka multi-batch all-or-none cuts and corrupted membership rejection, Pulsar entry identity, empty
  Kafka batch coverage, native-checksum independence, and Object-group boundaries that do not leak into append truth.

This decision refines ADR 0026 and is tracked by `T-PROTOCOL-01`, `T-OBJECT-01`, `V2-OBJ-004`, and `V2-OBJ-006`.
