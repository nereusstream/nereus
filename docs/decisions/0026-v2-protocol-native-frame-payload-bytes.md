# ADR 0026: V2 protocol-native frame payload bytes

## Status

Accepted for Kafka and Pulsar `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0021 defines a decoded `FramePayloadChecksum` domain. Kafka magic-v2 and Pulsar wire checksums cover different
protocol-specific subranges, and Pulsar application payloads may remain compressed, batched, or client-encrypted.
Decoding and reserializing application records/messages would add cost, risk byte drift, and be impossible for opaque
client encryption.

## Decision

“Decoded frame payload” means the exact protocol-native bytes after the outer Nereus Object envelope has been decrypted
and decompressed. It does not mean decoded or reserialized application records/messages.

- A Kafka frame payload is the exact assigned protocol-native `MemoryRecords`/complete record-batch byte sequence,
  preserving its serialized batch boundaries and bytes.
- A Pulsar frame payload is the exact ManagedLedger entry byte sequence, preserving Pulsar metadata, batching,
  compression, and client-encrypted payload representation.
- `FramePayloadChecksum` computes CRC32C/v1 over that entire exact frame payload. Frame boundaries and lengths remain
  format metadata and are validated before exposing the payload.
- Kafka and Pulsar native checksums remain independently validated over their original native domains. Their values are
  never relabeled or reused as the V2 frame checksum.
- V2 does not introduce an additional per-application-record/message checksum in 0.2.

Append-to-frame granularity is refined by ADR 0031. Per-frame/commit-set binary and index encoding remains a downstream
format gate and cannot change the canonical byte identity above.

## Consequences

- `V2-OPEN-OBJ-06` is resolved.
- Recovery can return exact protocol-native bytes without canonical re-encoding or access to client encryption keys.
- The frame checksum protects the entire opaque protocol blob, while native checksums and parsers retain their own
  semantic validation responsibilities.
- M3 must prove exact byte round-trip, outer compression/encryption decode, native-checksum independence, corrupted
  boundary/length rejection, Kafka multi-batch handling, and Pulsar batched/encrypted entry handling.

This decision is refined by [ADRs 0031](0031-v2-protocol-frame-and-append-commit-set.md) and
[0040](0040-v2-nwg1-append-unit-directory-and-colocation.md), refines ADR 0021, and is tracked by `T-OBJECT-01`,
`T-PROTOCOL-01`, `V2-OBJ-004/006/012`.
