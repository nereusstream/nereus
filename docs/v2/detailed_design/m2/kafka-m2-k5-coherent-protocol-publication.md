---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m1
---

# M2 Kafka K5 coherent protocol publication

K5 connects K4 ordered BookKeeper completion to the K1 partition state root. `KafkaAppendProtocolHooksV1` has two
stages inside K4's admission linearization boundary. `validateBeforeOffsetAssignment` checks the exact fence and the
new producer/transaction vector against committed state plus every ordered speculative delta; a rejection releases
capacity and never invokes the native offset allocator. After native assignment, `prepareAfterOffsetAssignment`
publishes only `Allocated` and a new immutable speculative-queue reference. It cannot move Durable, Readable/LEO, HW,
LSO, the active-tail locator, or wake a reader. A fence between these two stages leaves a typed allocated recovery gap
and fences the writer rather than pretending that the offset was never assigned.

`KafkaBatchDuplicateIdentityV1` is exactly the Kafka per-RecordBatch producer ID, producer epoch, base sequence, and
last sequence tuple. It contains no payload or storage digest. `KafkaCommittedProducerStateV1` preserves the original
committed offset result for native duplicate resolution and bounds each producer's recent result window to five
batches. A commit set remains an ordered vector and may contain multiple producers or non-idempotent batches. Sequence
and epoch progression is evaluated over committed plus speculative state so two in-flight requests from one producer
cannot both validate against a stale committed tail.

`KafkaTransactionStateV1` records ongoing transactions, completed control-marker ranges, and the aborted-transaction
index. First-unstable offset is derived from the coherently captured transaction state and Kafka HW: a completed
COMMIT/ABORT remains unstable until its marker end is HW-covered. K5 never advances HW or LSO. The
`KafkaLeaderEpochIndexV1` contains only Kafka leader epoch to first committed offset; Owner Epoch and Storage Epoch do
not enter that index.

K4 now revalidates every DATA member's run binding, member ordinal/count, group/attempt identity, contiguous Kafka
coverage, terminal physical range, and aggregate raw assigned-payload SHA-256 before I/O. Its ordered observer receives
one `KafkaOrderedDurableCommitV1` containing the exact run handle, DATA entry range, encoded bytes, logical range, and
aggregate payload identity already checked against every quorum proof.

For the queue head, `KafkaCoherentCommitCoordinatorV1` builds immutable candidate objects for:

- the BookKeeper active-tail locator and original append range;
- committed producer state and duplicate results;
- the remaining speculative queue;
- ongoing/completed/aborted transaction state;
- the Kafka leader-epoch index.

Each object is installed in a deterministic content-addressed owner-local repository before its reference is used.
One K1 `KafkaPartitionCommitSlotV1` then compares the exact Binding/incarnation/generation, Storage Epoch, Owner Epoch,
Kafka leader epoch, predecessor state version, and contiguous Readable end. Only its successful CAS exposes all five
references together and advances Durable plus Readable/LEO. A stale callback exposes none of them. If the post-CAS
notification throws, the root remains legally published and is reported as such; this slice still does not decide a
Kafka response.

`COMMITTED_ORDERED` now means K4 durability plus successful K5 coherent root publication when the coordinator is the
observer. It is not a success ACK. K5 does not implement response-time fence checks, `acks=0/1/all`, HW/ISR progress,
waiter recovery, packed lookup, checkpoint persistence, takeover, real BookKeeper, or Kafka runtime activation.
`bootstrap` creates only a fresh/recovered in-memory seed; K7 owns durable checkpoint restore and election-bounded
reconstruction.

`v2M2KafkaK5Check` executes 21 zero-skip tests in three suites. It covers speculative root isolation, fence/version and
reference rejection, producer sequence coverage and wrap, epoch reset/regression, bounded duplicate results,
non-idempotent batches, ongoing/commit/abort state, HW-sensitive first-unstable offset, multi-producer atomic
publication, validation before allocation, the assignment-to-stage fence cut, B-before-A durability, publication-
versus-fence ordering, exact active-tail locator publication, and notification failure after CAS.

This fake-provider/local-state gate proves no Kafka ACK, HW/LSO advancement, native broker error mapping, packed K6
reader, K7 recovery, K8 replication, real BookKeeper behavior, scenario promotion, or Kafka/global M2 PASS.
