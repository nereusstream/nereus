---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: NormativeDetailedDesign
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json
---

# M2-P5 exact-source native offload provider

P5 connects the P1-P4 production components to the exact Pulsar fork through its native
`SourceSafeLedgerOffloader` SPI. The Nereus build consumes the Pulsar checkout as a Gradle source composite; it does not
fall back to a published `managed-ledger` artifact that lacks the source-safe interface. The external source binding is
`nereus/v2-m2-pulsar-native-offload@a14e0e6f4e49be0677318b4ceefc7b85b445823b` from the immutable P1 base
`072aa1c440f85b808f60e7ea59de8a73c4e2a202`.

## Native provider boundary

`NereusPulsarLedgerOffloaderV1` accepts only a sealed, non-empty `ReadHandle`. It requests native batches with both the
admitted entry-count and decoded-byte limits, rejects a response that crosses either limit, and feeds the incremental
NPD1 encoder while retaining only the returned batch plus one decoded block. Publication remains data before root and
completes only after the production Object reader verifies the immutable pair. Aborted staging files are removed.

The persisted driver metadata fixes policy ID, Cell Provider Scope, key-derivation version, retention class, block
class/target, and compression policy for the attempt. Each sparse row still binds its actual `NONE` or `ZSTD` family.
The key manager creates one random AES-256 attempt key plus a wrapped v1 envelope before staging NPD1. Publication
writes the envelope into NPO1, completion verification unwraps the published envelope through the production read
path, and restart reads validate NPO1 before asking the manager to unwrap that exact envelope. Driver metadata carries
policy/location facts but neither plaintext nor wrapped key bytes. Read, delete, and final source-deletion revalidation
consume the persisted values rather than recomputing current configuration. The native `ReadHandle` adapter reconstructs the sealed metadata
without persisting the BookKeeper password and returns exact entry bytes from bounded NPD1 ranges. Cleanup proves root
absence before data absence and multipart-residue cleanup.

The Pulsar fork owns source selection and physical BookKeeper deletion. It rejects inconsistent compatibility
boolean/delete-state pairs, makes `BK_DELETE_INTENT` permanently Object-only, resumes INTENT without repeating final
Object revalidation, proves BookKeeper absence before DONE, and defers a full logical retention trim until source-safe
deletion reaches DONE. Missing native-provider support fails closed rather than entering the legacy deletion path.

## Focused verification and boundary

`v2M2PulsarP5Check` runs the complete Nereus offload module, the native `DualSourceReadHandleTest` and
`OffloadLedgerDeleteTest`, exact source/branch/cleanliness checks, documentation checks, and every preceding P0-P4 gate.
The P5-focused counts are 3 provider tests, 15 NPD1 tests, 13 native dual-source tests, and 13 native deletion tests,
all with zero failure, error, or skip.

P5 alone is not Pulsar process/NAR wiring, a provider endorsement, selected Object/block-policy evidence, a Pulsar
Final receipt, native performance parity, or global M2 PASS. P6 has since selected the admitted limits and three block
classes from source-qualified evidence, and Pulsar Final binds both cuts; M6 still owns broker-process integration.
