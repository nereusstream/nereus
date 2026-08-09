# ADR 0046: V2 NWG1 run key, AEAD, and authenticated directory

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0040 requires independently authenticated frame blocks but does not freeze the key hierarchy, nonce uniqueness,
directory authentication, or KMS frequency. Per-Object KMS wrapping would add cost and control-plane dependency to the
cost-first hot path, while a topic-wide key or unconstrained nonce generation would weaken isolation and make
uniqueness difficult to prove across recovery.

## Decision

0.2 NWG1 mandates `AES-256-GCM/HKDF-SHA-256 v1` for its client-side encryption envelope.

Each WalRun creates one random 256-bit data key. It is wrapped once under the immutable Cell KMS key identity and
version recorded by the WalRun Root. Every ObjectExtent derives a unique object key from the run key and canonical,
domain-separated shard identity, run epoch, and extent sequence inputs through HKDF-SHA-256. Shard run epochs and
extent sequences are never reused.

Within one ObjectExtent, fixed 96-bit nonces encode a domain and ordinal. One nonce domain is reserved for the combined
encrypted/authenticated `BindingContextTable + AppendUnitDirectory`; a disjoint domain is reserved for frame ordinals.
Nonce reuse under one derived object key is forbidden. The fixed NWG1 header, exact WalRun Root SHA-256, and exact
wrapped-key envelope identity are AEAD additional authenticated data.

Compression happens before each frame's AEAD operation. The exact protocol-native CRC32C/v1 is checked only after
successful authentication, decryption, and decompression. Header/directory CRC remains a corruption detector and does
not replace AEAD authentication.

KMS unwrap and plaintext run-key caching are run-scoped and bounded by the owning Cell Provider Session. KMS key
rotation takes effect only by sealing the current run and creating a successor Root. Per-Object KMS wrapping is not
used by the 0.2 cost-first Object WAL hot path.

## Consequences

- `V2-OPEN-OBJ-15` is resolved.
- Mandatory crypto CPU and a run-sized data-key compromise radius are accepted for run-scoped rather than per-Object
  KMS operations and independently authenticated range reads.
- Loss of the Cell KMS key/version needed by a live run fails admission/recovery closed; provider metadata cannot
  substitute for the Root-bound envelope identity.
- Exact HKDF input framing, nonce bytes, NWG1 header/directory wire, KMS envelope caps, run-key cache erasure, and
  cryptographic golden vectors remain downstream format gates.
- M3 must prove key/nonce uniqueness, AAD substitution rejection, directory/frame domain separation, rotation only at
  rollover, no plaintext leakage, and KMS/cache lifecycle isolation between Protocol Cells.

This decision refines ADRs 0021, 0030, 0037, and 0040 and is tracked by `T-OBJECT-01`, `T-FABRIC-01`, and
`V2-OBJ-006/007/012/013`.
