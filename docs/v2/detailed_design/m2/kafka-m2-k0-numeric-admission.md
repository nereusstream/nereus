---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m1
---

# M2-K0-N checked numeric admission and recovery envelope

K0-N implements the numeric child accepted by the
[M2-K0 implementation-input closure](kafka-m2-k0-implementation-input-closure.md). The independent
[numeric projection](../../wire/kafka-m2-k0-numeric-v1.json) keeps five domains distinct: persisted format hard caps,
immutable Provider capability, new-write admission, operational budgets, and evidence-selected thresholds/defaults.
No candidate benchmark value is copied into an NBKE2 format constant or reported as a selected default.

`KafkaBookKeeperDataAdmissionV1.admitProfile` binds one exact NBKE2 run/Cell Provider Scope to its immutable
`BookKeeperCapabilitySnapshotV1` and the Kafka-native complete-RecordBatch limit. A scope mismatch or a profile that
cannot carry one terminal DATA byte fails before a run opens. For a topic name of `n` encoded bytes, the exact terminal
DATA overhead is `32 + (146 + n) + 56 + 64 + 4`; the non-terminal form omits only the 64-byte terminal descriptor.

The checked profile calculation is:

```text
effectiveMaxDataFrameBytes = min(
    8 MiB NBKE2 v1 frame cap,
    admitted BookKeeper maximum add payload,
    admitted Kafka complete RecordBatch bytes + exact terminal DATA overhead
)

maximumAdmittedRawRecordBatchBytes = min(
    8 MiB - 1024 NBKE2 v1 raw DATA cap,
    effectiveMaxDataFrameBytes - exact terminal DATA overhead
)
```

Every member is conservatively checked with terminal overhead even when its actual encoded frame is non-terminal. This
makes the raw-RecordBatch admission ceiling independent of member position. After all format, provider, Kafka-native,
member-count, and allocation-domain checks pass, `admitBeforeOffsetAllocation` returns the only
`KafkaBookKeeperDataAdmissionTicketV1` constructor path. The ticket records exact actual encoded bytes and may be handed
to the later offset/entry sequencer. A rejection therefore cannot consume an offset or BookKeeper entry ID.

`KafkaBookKeeperRecoveryEnvelopeV1` requires three positive simultaneous dimensions: entry count, encoded bytes, and
elapsed nanoseconds. Bounds are inclusive, progress accumulation is non-negative and checked, and classification has a
stable entry/byte/time precedence. `loweredBy` fails if a Topic or lower authority tries to enlarge any dimension. K9
must source-qualify the exact operational defaults; Cell/host pressure may only provide a fully lower envelope.

`v2M2KafkaK0NumericCheck` executes 10 focused tests across three suites with zero skip. It proves one-before/at/one-after
for the persisted raw DATA cap, Provider add-payload cap, Kafka-native RecordBatch cap, and every recovery dimension;
it also covers exact terminal/non-terminal overhead, scope/profile/member rejection, checked progress overflow,
lower-only hierarchy, and independent projection parity.

This is non-promotable local implementation readiness. It does not select K9 operational defaults, allocate offsets or
entries, execute a BookKeeper writer or Kafka runtime, create the K0-E source-qualified receipt, register
`v2M2KafkaInputsCheck`, promote a scenario, or claim global M2 PASS.
