---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: NormativeImplementationSlice
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json
---

# M2 Kafka K2 assigned RecordBatch adapter

## Status and authority boundary

M2-K2 is implemented as a non-promotable engine slice. The source-locked Kafka fork at
`8afbc425660f3466bdc3255e3dd4eb43f8685af1` remains the authority for native `RecordBatch` parsing, validation and
error precedence. Nereus does not introduce a second producer-state or duplicate-result implementation and its
production modules import no Kafka SDK type.

`KafkaNativeRecordBatchFactsV1` is the narrow fork-facing boundary. A fork adapter may export facts only after Kafka
native validation succeeds. `KafkaNativeAssignedRecordBatchV1` then independently cross-checks exactly one complete
assigned batch: declared length, magic v2, base offset plus last-offset delta, partition leader epoch, stored CRC32C,
computed CRC32C, and the immutable raw bytes. Native facts or raw bytes that disagree fail closed.

## Run-facing NBKE2 mapping

`KafkaAssignedRecordBatchGroupAdapterV1` requires the exact K1 partition/run fence and one pre-offset-allocation K0
admission ticket per batch. It accepts only contiguous assigned offset coverage under one leader epoch and builds one
`Nbke2DataV1` per complete raw `RecordBatch`. The last member alone carries the terminal append-group descriptor and
the aggregate SHA-256 covers the ordered concatenation of all assigned batch bytes.

`KafkaNbke2AssignedAppendGroupV1` calls the frozen K0-W codec with contiguous physical entry IDs. Codec round trips
prove that the complete raw assigned Kafka bytes and header coverage are unchanged. K2 does not open a ledger, allocate
an offset, sequence an add, publish a frontier, or activate `UnifiedLog`, `Partition`, `ReplicaManager`, or purgatory.
Those responsibilities remain K3/K4/K5 and M6 as assigned by the accepted queue.

## Executed gate

`v2M2KafkaK2Check` executes 17 zero-skip local tests in three suites and 13 exact-source conformance cuts built from the
locked Kafka 4.3.0-SNAPSHOT checkout. The matrix includes ordinary, multi-record, idempotent, transactional, corrupted
payload, corrupted stored CRC, legacy magic, unassigned leader epoch, two-batch substitution, native-fact substitution,
coverage, and raw-byte preservation cuts. The gate also proves the K0 receipt/source-lock prerequisite, exact clean
Kafka HEAD and remote branch, and production Kafka-SDK absence.

This focused result proves only the K2 header/CRC/coverage and NBKE2 adapter boundary. Kafka Final now binds it with
K3-K10 evidence; K2 alone is not appender, real BookKeeper, runtime, scenario, Kafka Final, or global M2 PASS evidence.
