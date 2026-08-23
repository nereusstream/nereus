---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: NormativeImplementationSlice
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json
---

# M2 Kafka K8 replica descriptor, journal, and eligibility kernel

K8 freezes the Nereus-local `KRD1` compact replica descriptor before its first durable observation-journal use. It
binds the exact Binding, Kafka topic incarnation, partition, Binding generation, Storage/Owner/Kafka leader epochs,
coherent state version, Kafka range, encoded DATA bytes, aggregate assigned-payload SHA-256, compact source identity,
and four independent producer/transaction/leader-epoch/checkpoint content digests. `KRD1` is strict-EOF,
CRC32C-protected, and capped at 1,024 bytes. The canonical 470-byte golden has SHA-256
`0eb0a4afe61086fb6574e98279a68549d4a80e0bfd47c9d81147e9530946108d`.

`KRO1` wraps one descriptor with a zero-based ordinal, predecessor-record SHA-256, and monotonic observation time.
It is independently strict-EOF and CRC32C-protected under a 1,152-byte cap. The canonical 534-byte chained golden has
SHA-256 `6c8433d3a1b0e4f946b9a58ab0f9eab70f9bc54fd26ae8e2a2e75a67bcd0c58b`. Unknown source/mode codes,
unknown body tails, count/byte overflow, CRC substitution, truncation, fence changes, predecessor changes, offset gaps,
and state-version regression fail closed.

`KafkaReplicaObservationJournalV1` is the bounded kernel over the M6-owned local durable storage port. The port may
return an append proof only after local sync; the kernel requires the exact ordinal, encoded length, and record
SHA-256 before moving its durable cut. A mismatched proof, null/exceptional return, or response loss makes the
journal `INDETERMINATE` and blocks another append until restart recovery rereads the durable bytes. Recovery keeps only
the highest contiguous valid prefix. Corruption/truncation rolls Observed back to that prefix; an already durable
Applied boundary may compact older journal coverage but cannot split one descriptor range.

`KafkaReplicaFollowerKernelV1` validates the complete descriptor/source/fence tuple before journaling. A
`DESCRIPTOR_QUALIFIED` source may advance Observed before raw application only when the current source-map answer is
accessible, durable, exact for the descriptor digest, covers the full range, and matches payload plus all four protocol
digests. A profile that needs a raw read uses `PAYLOAD_REQUIRED`; K8 then journals first and reports Observed only with
the exact Applied proof. A newer replacement source generation may replace BookKeeper only with the same complete
coverage/content/protocol proof. K8 does not authorize original-source protection release or GC.

The ISR/HW eligibility value evaluates the whole accepted tuple on every observation and snapshot:

```text
journalDurableThroughObserved
&& observedEndOffset - appliedEndOffset <= maxApplyLagOffsets
&& unappliedBytes <= maxApplyLagBytes
&& unappliedAge <= maxApplyLagTime
&& recoverableSourceCovers([appliedEndOffset,observedEndOffset))
```

The offset, byte, age, journal-record, and journal-byte inputs are hard deployment bounds. K8 defines no defaults; K9
selected them from source-qualified real BookKeeper and scale evidence, and a Topic cannot enlarge them. The kernel
stops a descriptor before append when it would exceed a lag bound and reevaluates age and source replacement while the
range remains unapplied.

`KafkaReplicaElectionValidatorV1` is an engine-only harness over a native-supplied election kind and adoption boundary.
It never invents that boundary and returns an installable LEO only when both surviving Observed and Applied reach it.
Unclean-election truncation/data-loss semantics remain native Kafka authority.

`v2M2KafkaK8Check` executes 25 zero-skip tests in three suites. They cover both wire goldens and parser faults, exact
BookKeeper descriptor projection, sync proof/response-loss cuts, record/byte bounds, corrupt/truncated prefix recovery,
Applied-base compaction, all offset/byte/age/source eligibility cuts, exact/rejected apply proofs, payload-required
collapse, compatible/incompatible source replacement, journal rollback, and election Applied shortfall/success.

This focused gate alone proves no Kafka replica-Fetch framing or transport, real disk/fsync adapter, broker restart,
native ReplicaManager/Partition ISR/minISR/HW callback, native election evidence, real BookKeeper behavior, scenario
promotion, Kafka Final, or global M2 PASS. Kafka Final now binds it with selected K9 defaults and K10 evidence.
