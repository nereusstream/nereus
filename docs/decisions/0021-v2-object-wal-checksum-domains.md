# ADR 0021: V2 Object WAL checksum domains

## Status

Accepted for the V2 Object format and `OBJECT_WAL`. Exact initial algorithms remain an open implementation gate;
implementation and runtime evidence are not started at M0.

## Context

ADR 0018 requires an expected checksum to resolve an uncertain immutable PUT. Compression, client-side encryption,
provider-side encryption, transport, Object decoding, and protocol payload decoding may refer to different byte
streams. One unqualified `checksum` field would allow a provider checksum over uploaded bytes to be mistaken for
end-to-end payload integrity, or a payload checksum to be used as proof of exact stored-object durability.

## Decision

V2 uses two explicit, non-substitutable integrity domains:

1. `ObjectExtentDigest` covers the exact canonical request body presented by Nereus to the Object provider after Nereus
   compression and client-side encryption. It binds algorithm/version, exact length, immutable object identity/version,
   and encryption/compression format. ADR 0018 may use a provider-reported checksum only when the provider contract
   proves the same byte scope and algorithm for that immutable version; otherwise bounded full GET recomputes this
   digest over the bytes returned through the same provider interface.
2. `FramePayloadChecksum` covers the canonical decoded protocol payload/record bytes defined by the binding's payload
   mapping. It detects corruption across Object decoding, decompression/decryption, frame slicing, and protocol payload
   reconstruction.

Recovery validates `ObjectExtentDigest` before trusting frame boundaries, then validates every relevant
`FramePayloadChecksum` after decode. The two fields have distinct names/type IDs and cannot satisfy each other's gate.
The immutable Object Extent descriptor carries the request-body length plus Object-extent algorithm/version/value;
every frame carries its payload checksum algorithm/version/value. The Object-extent digest value is outside the
canonical request body it hashes and therefore cannot create a self-referential body field.

Storage Epoch format/checksum/encryption families fix which algorithm/version pairs are legal within that epoch. ADR
0025 selects SHA-256/v1 and CRC32C/v1 for 0.2; ADR 0026 defines the protocol-native frame payload bytes. An algorithm or
canonical-byte change requires a new format/epoch contract as applicable; mutable policy cannot silently reinterpret an
existing checksum field.

## Consequences

- `V2-OPEN-OBJ-04` is resolved.
- V2 pays for two integrity layers and stores explicit algorithm/type metadata.
- Provider-native checksums can avoid a response-loss GET only when their documented scope exactly matches the Object
  Extent domain; server-side implementation details or ETag do not qualify by themselves.
- M3 must inject stored-body, encryption/compression metadata, frame-boundary, and decoded-payload corruption and prove
  that each layer rejects only after validating its own domain.

This decision is refined by
[ADR 0025](0025-v2-initial-checksum-algorithms-and-provider-proof.md) and
[ADR 0026](0026-v2-protocol-native-frame-payload-bytes.md), with persisted Object identity/discovery in
[ADR 0030](0030-v2-object-wal-run-root-and-content-addressed-discovery.md) and frame granularity in
[ADR 0031](0031-v2-protocol-frame-and-append-commit-set.md). NWG1 placement is refined by
[ADR 0040](0040-v2-nwg1-append-unit-directory-and-colocation.md). This decision refines ADR 0018 and is tracked by
`T-OBJECT-01`, `V2-OBJ-001`, `V2-OBJ-003..007`, and `V2-OBJ-012`.
