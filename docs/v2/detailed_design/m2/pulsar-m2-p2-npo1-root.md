---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# Pulsar M2-P2 NPO1 sealed-ledger root

P2 implements the ADR-0029/0035 authoritative root over one P1 data Object. NPO1 is independent from stock Pulsar
offload indexes and contains exactly `ATTEMPT`, `SEALED_LEDGER`, `DATA_EXTENT`, and `SPARSE_INDEX` version-1 sections.

The 32-byte header carries `NPO1`, format/min-reader 1, zero flags, header length 32, section count four, total length,
and SHA-256 family 1. Each section uses one 16-byte kind/version/flags/body-length header. A final 32-byte SHA-256 covers
all preceding bytes. Decode rejects a provider/root length above 8 MiB or an exact-length mismatch and verifies the
self-digest before parsing section lengths, strings, counts, or sparse offsets.

The attempt binds ledger ID, canonical UUID, persisted provider scope, key derivation v1, retention class, and the
candidate block target. Sealed-ledger facts bind LAC/count/logical length, creation and fence facts, ensemble/write/ack
quorums, digest type, canonical custom metadata, and ordered ensemble segments without a password. Custom-metadata keys
are strict UTF-8 while values preserve the native bounded binary bytes. The data section binds the derived key, exact
NPD1 bytes/SHA, immutable provider version, and format version. Sparse rows bind each P1 block's entry range, byte range,
decoded length, codec/encryption, and encoded-block SHA.

Wire enum IDs are stable and do not use Java ordinals: retention is `RETAIN_BK=0` or `DELETE_AFTER_VERIFIED=1`; digest
is `CRC32C=1`, `MAC=2`, `CRC32=3`, or `DUMMY=4`; compression is `NONE=0` or `ZSTD=1`; encryption is
`AES_GCM_256=1`.

Validation requires `entryCount=LAC+1`, entry coverage exactly `0..LAC`, block ordinals from zero, byte coverage exactly
from the 32-byte NPD1 header through the data length, strict UTF-8, unsigned-UTF-8 map ordering, unique fields, checked
arithmetic, and the accepted hard caps. Empty ledgers never form an attempt.

`v2M2PulsarP2Check` proves only NPO1 bytes and rejection behavior. Publication/read/delete execution, native Pulsar
integration, selected defaults/provider evidence, scenario receipts, and M2 PASS remain pending.
