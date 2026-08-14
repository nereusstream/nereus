---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m1
---

# M2-K0-W closed NBKE2 v1 wire contract

K0-W implements the closed wire child accepted by the
[M2-K0 implementation-input closure](kafka-m2-k0-implementation-input-closure.md). Production code lives in
`nereus-kafka-bookkeeper` under `com.nereusstream.kafka.bookkeeper.nbke2`. The independently maintained
[machine projection](../../wire/nbke2-v1.json) and [immutable golden matrix](../../wire/nbke2-v1-goldens.tsv) are
normative review inputs; the gate compares them with production constants and encoded bytes rather than generating one
side from the other.

NBKE2 v1 is canonical big-endian. Every frame starts with a 32-byte header: `NBKE2` magic at bytes 0-4, major/minor at
5-6, closed frame type at 7, legal flags at 8, reserved zero at 9, header length at 10-11, checked total length at
12-15, and BookKeeper ledger/entry identity at 16-31. The decoder requires major 1, minor 0, an exact known type, only
the DATA terminal bit, header length 32, an input-equal total length, and exact caller-supplied ledger and entry IDs.

Every frame repeats the exact Binding, Kafka topic incarnation and partition, Storage Epoch, creator Owner Epoch,
Kafka leader epoch, Cell Provider Scope, and run identity. The persisted topic-name length is 1-249 bytes. These
identity bytes precede one of five closed semantic payloads:

- `RUN_HEADER(1)` carries start offset, first DATA entry, and ledger-configuration SHA-256. It is entry 0 and its first
  DATA entry is later than entry 0.
- `DATA(2)` carries one complete raw broker-assigned RecordBatch, its logical coverage, member ordinal/count,
  append-group identity, and storage-attempt identity. Exactly the last DATA member sets flag 1 and carries the sole
  64-byte terminal append-group descriptor. No alternative control-frame representation exists.
- `RANGE_INDEX_BLOCK(3)` carries declared logical/physical bounds and 32-byte ordered, gap-free locator rows. The
  control entry lies strictly after its last covered DATA entry and before its declared successor DATA entry.
- `PROTOCOL_CHECKPOINT(4)` carries the range, producer, transaction, and leader-epoch coverage vector plus three
  individually length-capped canonical sections.
- `RUN_FOOTER(5)` carries terminal logical/physical bounds, qualified seal Owner Epoch, and ordered 24-byte index
  directory rows. Its physical exclusive bound equals its own BookKeeper entry ID plus one.

The persisted format cap is 8 MiB per frame. DATA raw bytes are capped at 8 MiB minus 1024 bytes; locator and footer
directory counts are each capped at 65,536; each checkpoint section is capped at 2 MiB. The implemented
[K0-N numeric child](kafka-m2-k0-numeric-admission.md) separately owns the provider/Kafka-aware new-write formula and
one-before/at/one-after proofs. K0-W does not treat a format maximum as a provider capability or operational default.

Every frame ends with CRC32C/v1 over bytes `[0,totalLength-4)`. Range-index, checkpoint, and footer frames additionally
store SHA-256/v1 over `[0,shaFieldOffset)`. The terminal append-group digest covers the concatenation of exact assigned
RecordBatch bytes in member order. The decoder checks lengths and counts before allocation, uses checked arithmetic,
requires strict EOF, and returns a closed typed rejection for truncation, magic/version/type/flag/reserved mismatch,
identity mismatch, CRC/SHA mismatch, cap violation, overflow, ordering, or trailing bytes.

`v2M2KafkaK0WireCheck` executes 10 focused tests across four suites with zero skip. The matrix round-trips all five
frame types, checks canonical headers, terminal descriptor placement, maximum legal DATA, header/integrity corruption,
length/count/ordinal/overflow/order/truncation/strict-EOF rejection, independent projection parity, and 15 immutable
minimum/representative/maximum vectors. Maximum vectors retain length and SHA-256 without checking multi-megabyte hex
into the repository.

This is non-promotable local implementation readiness. It does not execute a BookKeeper writer, parse Kafka
RecordBatch semantics, open a run, publish a Kafka frontier, promote a scenario, create the K0-E source-qualified
receipt, register `v2M2KafkaInputsCheck`, or claim global M2 PASS.
