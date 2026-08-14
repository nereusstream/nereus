---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# Pulsar M2-P4 Object read handle

The P4 Object child first HEADs the deterministic root key, rejects a provider length above the NPO1 8-MiB cap, and
performs one exact bounded root GET. It matches the provider length/SHA, verifies the NPO1 self-digest and canonical
bytes, then checks persisted ledger/attempt/provider/retention and closed-ledger facts before using any sparse row.

It next HEADs the deterministic data key and requires the exact immutable version, length, and SHA bound by NPO1. The
32-byte NPD1 header must match root block count and total bytes. Normal reads select only intersecting sparse blocks,
issue bounded range GETs, verify the exact encoded-block SHA and authenticated NPB1 facts, and return one contiguous
inclusive entry range. Typed failures distinguish missing, timeout, unavailable, short, integrity, format, invalid,
cancelled, and closed cases for the dual-source policy.

`verifyCompleteLedger` streams the header and every encoded block through one SHA-256 accumulator while decoding at
most one bounded block at a time. It proves exact entry count, logical length, block count, data length, and complete
data SHA without constructing a whole-Object `ByteBuffer`. `PulsarPublishedAttemptVerifierV1` connects this production
path to the P3 completion cut.

`v2M2PulsarP4ObjectCheck` proves the Object child only. Whole-range dual-source fallback, source pins, deletion CAS,
native Pulsar integration, selected defaults/evidence, scenario promotion, and M2 PASS remain pending.
