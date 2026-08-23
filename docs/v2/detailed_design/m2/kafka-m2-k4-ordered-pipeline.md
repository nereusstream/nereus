---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: NormativeImplementationSlice
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json
---

# M2 Kafka K4 capacity-first ordered pipeline

K4 implements the three owner-local stages between K2 assigned-batch validation and K5 coherent protocol publication.
`KafkaAppendCapacityControllerV1` reserves groups, entries, and encoded bytes at both partition and global scope. Both
leases must exist before `KafkaOffsetAssignmentV1` is invoked. A capacity rejection never calls the native allocator;
an allocator throw releases both leases without advancing the speculative offset or reserving a ledger entry.

After one exact contiguous Kafka range is returned, `KafkaBookKeeperOrderedPipelineV1` advances its speculative end,
reserves one contiguous K3 entry range, and asks the K2 factory to bind unchanged NBKE2 DATA bytes to that physical
range. Member count, encoded-byte total, logical start/end, and first entry ID must match the pre-offset reservation and
native assignment exactly. Offset assignment, entry reservation, physical validation, and ordered-slot insertion share
one pipeline linearization boundary through the final provider submission and K3 group close, so concurrent callers
cannot insert a contiguous successor ahead of its predecessor or collide with its still-open entry group. A mismatch
fences the pipeline and cannot submit DATA.

Every DATA member is submitted in entry-ID order while BookKeeper stages may overlap. The K3 group reservation closes
only after every accepted submission call has been made, so a control entry cannot split the group. Quorum proofs must
match handle, entry ID, byte count, payload SHA-256, and admitted ACK quorum. Capacity remains owned until the group
reaches its ordered terminal result. K5 additionally consumes the exact ordered run handle, entry range, encoded-byte
count, logical range, and terminal raw-payload aggregate only after K4 revalidates every member and descriptor field.

Completion may be out of order but commit order may not. A durable B waits behind pending A. When A becomes exactly
durable, the queue emits A then B and releases their permits. A definitive failure, response-unknown result, substituted
proof, or ordered-observer failure fences the pipeline; already durable successors receive `FENCED_BY_PREDECESSOR` and
cannot cross the gap. A still-pending fenced successor retains both capacity leases until its own provider operation is
terminal, even though it can no longer publish. With the no-op K4 observer, `COMMITTED_ORDERED` is only the engine seam;
with the K5 coordinator installed, the observer must complete the coherent K1 root publication first. Neither form is
itself a Kafka success ACK.

The K2 validated-batch type is now a private-constructor final value class. Production callers can obtain it only
through `validate(KafkaNativeRecordBatchFactsV1)`; the former public record-constructor bypass is regression-tested.

`v2M2KafkaK4Check` executes 20 zero-skip tests in three suites. It covers one-before/at/one-after capacity, idempotent
release, capacity-before-offset ordering, partition/global rollback, assignment failure, contiguous multi-member
submission, concurrent insertion linearization, byte/range substitution, B-before-A durability, definitive/unknown A
failure, proof substitution, observer failure, and permit ownership through each provider terminal and ordered
completion.

This focused fake-provider gate alone proves no producer/transaction mutation, Kafka ACK, recovery, real BookKeeper
behavior, runtime activation, scenario promotion, or Kafka/global M2 PASS. Kafka Final now binds it with K5 coherent
publication and K6-K10 evidence.
