---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# Pulsar M2-P1 NPD1/NPB1 codec

P1 implements the ADR-0044/0056 data Object codec without selecting the P2/P6 operational defaults. The caller must
supply one P0 candidate, one candidate block target, and a random AES-256 attempt key/UUID. P2 persists only the wrapped
form of that key in the NPO1 attempt section; plaintext key bytes never enter NPO1 or native driver metadata. P6 selects
the production limits and block classes before a native offloader may activate.

## Canonical bytes

The 32-byte big-endian NPD1 header contains magic, version/min-reader 1, zero flags, header length 32, block count,
zero reserved bits, and complete data Object length. Every 64-byte NPB1 header contains magic/version, zero flags,
header length, block ordinal, entry count, codec/encryption IDs, first entry ID, decoded/directory/compressed lengths,
one attempt-and-ordinal-derived 12-byte nonce, and zero reserved bits.

The NPB1 header is AES-GCM AAD. Its ciphertext plaintext is exactly the 16-byte rows followed by the compressed payload;
the encoded block is `64 + ciphertext + 16-byte tag`. `NONE` and `ZSTD` reset per block. Entry rows carry only decoded
offset, payload length, and zero flags. Entry IDs derive by checked addition from the authenticated first ID.

## Streaming and bounds

Encoding reserves the 32-byte header in a target file, writes one bounded block at a time, then checks the final length,
rewrites the header, and computes the complete SHA-256 through a streaming read. It never constructs a data-Object-sized
array. An entry larger than the selected target forms one dedicated block; the P0 candidate entry/block hard bounds
still reject it before output.

Decoding consumes one root-selected encoded block range, verifies its SHA-256, exact NPB1 facts, AAD/tag, directory,
codec result, derived IDs, and no trailing bytes. It does not scan a predecessor block or read the complete Object.

`v2M2PulsarP1Check` proves the codec/golden/corruption matrix only. NPO1, provider publication, native
`LedgerOffloader`, selected defaults, scenario receipts, and M2 PASS remain pending.
