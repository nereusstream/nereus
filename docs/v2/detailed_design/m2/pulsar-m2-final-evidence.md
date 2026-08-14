---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: NormativeDetailedDesign
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json
---

# Pulsar M2 Final evidence

Pulsar Final is a separate source-bound promotion aggregate after P0-P6. It does not reinterpret focused gate output as
scenario evidence. The production `PulsarM2PromotionPolicyV1` owns exactly these eleven complete M2 rows:

- `V2-BK-001`, `V2-BK-002`, and `V2-BK-004..010`;
- `V2-BK-012` and `V2-BK-013`.

`V2-BK-001` requires the current Kafka Final receipt as well as the Pulsar sealed-ledger/native evidence because its
claim covers both BookKeeper profiles. `V2-BK-011` remains `PLANNED`: its irreversible delete-state primitive is
implemented in M2, but the complete row also owns M5 retention/reference behavior. Kafka-owned `V2-BK-003/014..017`,
M6 broker/NAR process activation, and M8 `V2-PUL-001` native feature/performance parity are outside this receipt.

## Receipt mechanics

`PulsarM2FinalReceiptV1` is a strict ASCII canonical JSON codec with a 64-KiB root cap, eleven sorted scenarios, at
most eight sorted suites per scenario, bounded repository-relative attachment paths, checked attachment totals, and
zero failure/error/skip requirements. It rejects BOM, escaping, non-canonical field order, duplicate/unsorted IDs,
unknown enums, trailing bytes, path traversal, symlink roots or path components, attachment length/SHA drift, source
binding drift, missing prerequisites, and unreferenced attachments. `PulsarM2FinalResolverV1` compares the receipt with
the production scenario/suite allowlist before verifying every attachment.

The canonical Final receipt has exactly 32 scenario-suite references and eight attachments: current local functional
JUnit summary, native-fork P5 summary, P6 provider JUnit summary, candidate matrix, native baseline, fixed MinIO
provider result, P6 execution receipt, and current Kafka Final receipt. Its source tuple binds the exact Nereus commit,
Pulsar fork commit, source-lock digest, and the P6/Kafka prerequisite receipt digests.

`v2M2PulsarFinalPolicyCheck` proves only the production parser/resolver/policy mechanics. The published evidence-only
receipt and synchronized scenario registry bind regenerated P6 and Kafka Final roots to the frozen source. Only
`v2M2PulsarFinalCheck` validates that child receipt.

Pulsar Final is not broker-process/NAR activation, Amazon S3 endorsement or performance evidence, M8 native
performance parity, or promotion of `V2-BK-011`. The separate global
[`m2-final.json`](../../evidence/v2-m2/final/m2-final.json) now binds this child with Kafka Final; this receipt alone
never claims `PASS_V2_M2_FINAL`.
