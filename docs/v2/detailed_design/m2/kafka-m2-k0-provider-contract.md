---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m1
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
the dependency/API boundary. It remains non-promotable local implementation readiness. This cut has no real
BookKeeper client/server execution, `NBKE2`, numeric admission, writer, Kafka runtime, scenario promotion,
`v2M2KafkaInputsCheck`, or global M2 PASS. K0-E owns source-qualified receipt aggregation after K0-W and K0-N exist.
