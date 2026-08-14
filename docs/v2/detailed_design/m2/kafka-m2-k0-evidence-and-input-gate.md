---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m2-kafka-k0
---

# M2-K0-E exact source, receipt, and Kafka Inputs gate

K0-E implements the evidence child accepted by the
[M2-K0 implementation-input closure](kafka-m2-k0-implementation-input-closure.md). The
`m2KafkaK0InputSourceBinding` in `docs/v2/source-locks.json` now binds the trusted M1 Final index and source-tuple SHA,
immutable N1 artifact, K1 Kafka base/fork, K0-M bundle/receipt, NBKE2 projection/goldens, numeric projection, Apache
BookKeeper release source/client artifacts, exact server image, and complete K0 capability input.

The BookKeeper input is `release-4.18.0` tag object `bb51381c...` peeled to source commit `cd063408...`. The pinned
server is `apache/bookkeeper@sha256:c0a12893...`; its linux/amd64 image config is `sha256:d0e78aaf...`, and its embedded
`bookkeeper-server-4.18.0.jar` is byte-identical to the Maven input at SHA-256 `8e64f2b7...`. The committed image-input
attachment records those facts without claiming runtime conformance.

The [capability input](../../wire/bookkeeper-kafka-m2-k0-capability-v1.json) closes normal V3 add to CRC32C, no MDC
request context, no recovery/priority/write flags, 5 MiB client/server frames, explicit entry IDs, 3/3/2 quorum,
sync journal, exact timeouts, and a versioned no-auth identity for local K0/later conformance only.
`BookKeeperV3Crc32cAddPayloadLimitV1` derives the exact `5,242,771`-byte add payload from BookKeeper's four-byte length
prefix, light-protobuf request, 20-byte master key, 32-byte entry metadata, and four-byte CRC32C. Tests compare the
formula with the real source-locked 4.18.0 serializer at the maximum and maximum plus one.

`KafkaM2InputsReceiptV1` is the production JDK-only parser/canonical codec for
`NEREUS_V2_M2_KAFKA_INPUTS_RECEIPT_V1`. It requires exactly sorted `K0_E/K0_M/K0_N/K0_P/K0_W` rows, non-zero suites and
tests, zero failure/error/skip, the exact source tuple, `KAFKA_M2_INPUTS_ONLY`, and `promotionEligible=false`. It rejects
unknown/reordered/trailing bytes, malformed UTF-8, bad identities, missing/duplicated gates, symlink roots, oversized
roots, and every PASS-shaped empty or skipped result.

`v2M2KafkaK0EvidenceCheck` executes nine tests across the exact serializer-limit and receipt-parser suites and verifies
all locked bytes/hashes. `v2M2KafkaInputsCheck` is registered as a distinct aggregate and requires an explicit
`-Pv2M2KafkaInputsReceipt=<canonical-file>` input. It reruns all five non-empty child gates, parses the receipt with the
production validator, requires a clean `HEAD == origin/main`, checks current source-lock and child-input hashes, and
permits only the canonical receipt path after its tested source commit.

The canonical aggregate receipt has not yet been published in this implementation cut, so evidence remains `NotRun`
and the aggregate is not reported PASS. K0-E and the later receipt cannot promote a scenario, prove a run writer or
real BookKeeper behavior, activate Kafka runtime, or claim Kafka/global M2 Final.
