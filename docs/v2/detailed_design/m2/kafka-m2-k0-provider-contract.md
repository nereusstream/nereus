---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: NormativeImplementationSlice
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json
---

# M2-K0-P Cell-scoped BookKeeper provider contract

K0-P implements the SDK-free provider boundary accepted by the
[M2-K0 implementation-input closure](kafka-m2-k0-implementation-input-closure.md). `nereus-storage-api` now owns the
immutable Cell Provider Scope identity, source/configuration-qualified `BookKeeperCapabilitySnapshotV1`, exact run
ledger handles, closed read/open results, the four-state `ProviderMutationOutcomeV1`, replayable retained payload
contract, `BookKeeperCellSession`, and low-frequency `KafkaRunRootAuthority`.

The capability constructor fails admission unless the tuple has canonical client/server source identities, non-zero
artifact/configuration digests, positive client/server frame limits, a smaller derived add-payload cap, explicit entry
IDs, ordered quorum sizes, an integrity-bearing digest, fencing/recovery, positive timeouts, and a canonical credential
identity version. It contains no BookKeeper, Kafka, Pulsar, or Oxia SDK type.

`nereus-storage-bookkeeper` implements the provider-owned payload and operation lifecycle kernel. A returned operation
lease represents accepted work. Append admission retains one independent immutable payload reference; observer
cancellation, timeout, and response loss do not release it or complete drain. Only exact terminal reconciliation
releases the reference and permits. Drain/close stop admission and complete only when operation count and retained bytes
reach zero. Each registry is Cell-local, so closing one does not mutate another.

`v2M2KafkaK0ProviderCheck` executes 22 focused production-contract tests across four suites with zero skip and checks
the dependency/API boundary. Those results are covered by the current non-promotable `v2M2KafkaInputsCheck` receipt.
This cut still has no real BookKeeper client/server execution, writer, Kafka runtime, scenario promotion, or global M2
PASS; the aggregate proves implementation inputs only.
