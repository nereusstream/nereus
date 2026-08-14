---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m1
---

# M2 Kafka K3 run lifecycle and entry sequencer

K3 implements the single-partition BookKeeper run lifecycle under the exact Binding, Topic Incarnation, partition,
Storage Epoch, creator Owner Epoch, Kafka leader epoch, Provider Scope, run ID, and ledger identity frozen by K0-W.
`KafkaBookKeeperRunLifecycleV1` creates the exact run ledger, writes and verifies `RUN_HEADER` at entry zero, then
publishes the low-frequency ACTIVE root. A root is never published before the header reaches exact quorum proof.

`KafkaBookKeeperEntrySequencerV1` owns the sole gap-free physical entry sequence beginning at entry one. One DATA-group
reservation covers all of its members contiguously. A second group or any control reservation is rejected until the
exact reservation completes, so a checkpoint/footer cannot split a commit set. K4 will submit DATA and own bounded
pipeline capacity; K3 does not allocate Kafka offsets or claim append durability.

Protocol checkpoints may be submitted only while ACTIVE and at a group boundary. Run drain stops new reservations and
waits for every accepted checkpoint plus any open DATA reservation. Seal then sequences and verifies `RUN_FOOTER`,
closes the ledger with exact last-add-confirmed proof, and CAS-publishes the SEALED root. A successor must preserve the
partition chain and Provider Scope, use a fresh run/ledger identity, start at the predecessor's sealed Kafka end, and
regress neither Owner Epoch nor Kafka leader epoch.

The frozen K0 `KafkaRunRootAuthority` intentionally owns create/open/seal/successor and is not expanded into generic
delete metadata. K3 local `RETIRED` therefore requires explicit proof that the manifest no longer selects the run,
source protection is drained, read pins are zero, and retention elapsed; its durable root remains SEALED. Physical
ledger deletion and mixed-source retirement are outside this fake-provider slice.

`v2M2KafkaK3Check` executes 19 zero-skip tests in three suites. The matrix covers contiguous DATA/control allocation,
control exclusion, exact header/root identity, checkpoint submission, pending-operation and open-group drain, footer/
close/root seal order, successor continuity, retirement eligibility, response-unknown create/header cuts, substituted
append proof, failed close, and successor fence regression.

This gate uses a deterministic fake provider and proves no DATA I/O pipeline, ordered commit publication, recovery,
real BookKeeper behavior, physical deletion, Kafka runtime activation, scenario promotion, or Kafka/global M2 PASS.
