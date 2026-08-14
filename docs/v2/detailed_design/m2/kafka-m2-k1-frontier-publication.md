---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m1
---

# M2-K1 coherent frontier and fenced publication cell

K1 implements the first pure state slice of the accepted
[Kafka Produce/Fetch design](kafka-produce-fetch-frontiers-and-recovery.md). Production code is isolated under
`com.nereusstream.kafka.bookkeeper.protocol` and imports no BookKeeper API, implementation, or SDK type. It uses only
the immutable N1 identity types plus JDK concurrency primitives.

`KafkaPartitionFenceV1` keeps Binding ID, Topic Incarnation, partition, Binding generation, Storage Epoch, Owner Epoch,
and Kafka leader epoch independent and exact. `KafkaPartitionFrontiersV1` enforces the sole ordered frontier invariant:

```text
trimStart <= LSO <= HW <= readable/LEO <= durable <= allocated
```

`KafkaPartitionProtocolStateV1` is one immutable root containing that fence, one monotonic `stateVersion`, the complete
frontier tuple, and immutable generation/digest references for run table, active tail, Source Map, committed and
speculative producer state, transaction index, leader-epoch index, checkpoint vector, and source protection.
`captureReadSnapshot` returns the root reference itself, so capture allocates no new snapshot and cannot combine LEO
from one version with transaction or source state from another. Replica, read-uncommitted, and read-committed bounds
select readable/LEO, HW, and LSO respectively; durable end is never a read bound.

`KafkaPartitionCommitSlotV1` is hidden until it replaces the root through `KafkaPartitionPublicationCellV1`. A slot
must name the exact predecessor fence/version, begin at the current readable end, end at the exact replacement readable
end, carry durability through that end, preserve HW/LSO/trim, advance the active-tail reference, and regress no frontier
or immutable reference generation. A later durable slot cannot publish around an earlier gap.

K5 extends the same cell with `KafkaPartitionSpeculativeSlotV1`. This replacement advances only Allocated and the
speculative producer-queue reference, changes no other component/frontier, and emits no reader/replica notification.
Its fence/version/contiguity and reference-isolation matrix is owned by `v2M2KafkaK5Check`; the original K1 gate count
below remains the frozen K1-only matrix.

Fence/leadership/Storage-Epoch transitions use the same CAS cell. They may install an election-bounded truncated
frontier, but cannot change partition identity, regress ordered numeric fences, or roll back immutable reference
generations. Exactly one competing replacement wins. Notifications carry the exact before/after roots and run only
after successful CAS; a stale callback never wakes a waiter. Notification failure is a closed
`PUBLISHED_NOTIFICATION_FAILED` result, so it cannot disguise or roll back already-published state.

`v2M2KafkaK1Check` executes 26 zero-skip tests in five suites, including frontier and reference validation, all three
read bounds, contiguous/gapped slots, fence/version mismatches, notification failure, B-before-A completion, both
publication-versus-fence interleavings, election truncation, and before/after snapshot coherence. The deterministic
scheduler is test-owned as required by the K0 module boundary.

This local K1 gate is non-promotable and has no Kafka Fast receipt yet. It performs no offset allocation, BookKeeper
I/O, run lifecycle, producer/transaction mutation, waiter registration, Kafka runtime activation, scenario promotion,
or Kafka/global M2 PASS.
