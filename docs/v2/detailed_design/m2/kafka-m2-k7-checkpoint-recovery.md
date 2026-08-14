---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m1
---

# M2 Kafka K7 checkpoint kernel and election-bounded recovery

K7 implements a profile-neutral `KafkaProtocolCheckpointStoreV1` and its BookKeeper `NBKE2` control-entry carrier.
`KafkaRecoveryCheckpointVectorV1` binds range-index, producer, transaction/aborted, and leader-epoch component coverage
to the exact prior run identity. The common state model is independent of BookKeeper and the future Object carrier.
The K7 BookKeeper writer emits aligned compound vectors so one exact boundary can seed suffix replay; the already
frozen NBKE2 wire still permits distinct component values for a future mutually compatible component selector.

`KafkaProtocolCheckpointCodecV1` defines three strict-EOF `KPC1` canonical sections under the K0-W 2 MiB section cap.
It checks magic/version and row counts before allocation, reconstructs bounded producer duplicate results,
ongoing/completed/aborted transactions, and the Kafka leader-epoch index, and rejects state beyond the component's
covered-through boundary. Same-vector content substitution, regression, another run identity, unaligned K7 writes,
and concurrent publication fail closed.

`BookKeeperKafkaProtocolCheckpointStoreV1` reserves the control entry only between complete DATA groups through the K3
sequencer. A returned exact quorum proof publishes the checkpoint. `OUTCOME_UNKNOWN` or an exceptional accepted append
does not become success or failure by timing: the lifecycle retains the payload and rereads the same ledger/entry,
requiring the exact handle, entry ID, SHA-256, and bytes. Definitive rejection, fencing, absence, or substituted bytes
fails the run. The footer must bind the latest exact protocol-checkpoint entry.

`KafkaBookKeeperTakeoverRecoveryV1` first opens and fences the exact prior handle. It validates an optional authoritative
checkpoint hint or falls back to the authenticated RUN_HEADER, then scans the unchecked suffix in physical order.
Every remote entry read, encoded byte, and elapsed nanosecond contributes to one cumulative
`KafkaBookKeeperRecoveryEnvelopeV1`; checkpoint fallback never resets those counters. The scan validates complete
append-group identity, member order/count, aggregate payload SHA-256, NBKE2 CRC/identity, raw Kafka header/CRC/leader
epoch, and protocol deltas from the narrow exact-source `KafkaRecoveryBatchProtocolAdapterV1`. A partial group or the
first definitive gap/conflict never advances the physical candidate.

The recovery result keeps these facts separate:

```text
physicalRecoveredEndOffset
electedReplicaObservedEndOffset
replicaAppliedEndOffset
electionAdoptableEndOffset
```

Recovery succeeds only when the physical candidate and Applied frontier both reach the native election boundary and
that boundary does not split a complete RecordBatch group. The installed LEO is exactly
`min(physicalRecoveredEndOffset,electionAdoptableEndOffset)`. Complete later bytes and post-boundary conflicts remain
inert old-epoch residue and never enter producer, transaction, locator, or leader-epoch state.
`KafkaCoherentCommitCoordinatorV1.bootstrapRecovered` installs the recovered state into one new-leader K1 root and
fresh run. Native Kafka supplies HW independently; K7 derives LSO from that HW and first-unstable offset and never
recovers HW from WAL bytes.

`v2M2KafkaK7Check` executes 26 zero-skip tests in three suites. It covers KPC1 round-trip and parser faults, vector and
component bounds, aligned publication, response-loss reread, concurrency/regression/footer cuts, exact and inert-tail
recovery, checkpoint selection and corrupt fallback, entry/byte/time envelope exhaustion, partial groups, physical
and Applied shortfall, batch-aligned election cuts, open/fence/header faults, and coherent new-leader bootstrap.

This deterministic fake-provider gate proves no Kafka broker recovery adapter, native election implementation, final
HW/ISR behavior, K8 replication journal, real BookKeeper behavior, scenario promotion, Kafka Final, or global M2 PASS.
K9 still selects checkpoint cadence, suffix, timeout, rollover, and resource values from real BookKeeper evidence.
