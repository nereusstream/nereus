# ADR 0040: V2 NWG1 append-unit directory and co-location

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

V1 `NRS1/WAL_OBJECT_V1` derives Kafka coverage from record count, has no authoritative commit-set membership or
per-frame V2 checksum fields, and cannot support bounded native-frame range reads without reading and clipping a larger
slice. A sidecar, manifest-only, or footer-only append-unit index would create another publication/discovery authority
or require reading the whole Object before bounds can be trusted.

## Decision

0.2 uses a new major Object-WAL body format, `NWG1`. After its fixed header, every `.nwg` contains exactly one bounded,
authoritative, independently integrity-checked and range-readable
`BindingContextTable + AppendUnitDirectory`. The directory is part of the Object body; a sidecar, manifest, checkpoint,
or footer may accelerate lookup but cannot replace or overrule it.

One Kafka Append Commit Set is stored completely and contiguously inside one ObjectExtent. It never crosses two group
objects. If the next set would exceed a group limit, the group seals first. A single set larger than the hard format
limit is rejected before protocol positions are allocated. One Pulsar entry/frame is likewise a complete directory
unit.

Each stored frame block has independent compression, AEAD encryption/authentication, and CRC32C/v1 validation. 0.2
forbids compression spanning frames and forbids one whole-group AEAD stream. Header/directory integrity protects bounds,
context references, membership, ordinals, and coverage descriptors. Per-frame CRC protects the decoded exact
protocol-native payload, while Object SHA-256 protects the final canonical provider body. No additional commit-set CRC
is introduced.

The header/directory integrity check is an internal CRC32C/v1 over its canonical stored descriptor bytes and is
validated before trusting bounds or membership. It is neither a third durability/payload identity domain nor a
substitute for `ObjectExtentDigest` or `FramePayloadChecksum`, and CRC alone does not claim cryptographic authenticity.
Exact field placement and canonical coverage framing remain part of the downstream NWG1 wire gate.

Readers validate bounded header/directory facts before issuing frame ranges. Root-bound directory AEAD, not a fresh
whole-Object durability proof, authenticates local offsets and lengths for routine random reads; the selected frame is
then independently authenticated and checksummed. External hints can only plan the bounded prefix and never authorize
a frame offset. Readers never infer Kafka coverage from record count, split a Pulsar entry, or treat physical group
order as protocol order.

After Object/header/directory validation, each complete append unit is independently dispatchable to its binding's
runtime completion tracker. A frame/commit-set AEAD, CRC, native-checksum, or typed-coverage failure blocks that complete
unit but does not automatically block independently verified units from other bindings. Failure of Object digest, KMS
envelope, fixed header, or directory authentication still blocks every member of the shared Object. Checkpoint
publication inventories the provider-resolved Object and does not assert that every member has advanced its typed
frontier.

## Consequences

- `V2-OPEN-OBJ-14` is resolved.
- Per-frame descriptors, AEAD tags, and weaker cross-frame compression are accepted costs for native random reads and
  removal of sidecar atomicity.
- AEAD key hierarchy and authenticated directory/frame domains are refined by ADR 0046, and directory-prefix capacity
  by ADR 0058. Exact header/directory field IDs, byte layout, numeric hard limits, nonce/AAD framing, hint/range
  assembly, and compaction golden vectors remain downstream format gates.
- M3 must prove commit-set co-location, oversize rejection before allocation, directory substitution/bounds failures,
  independent frame decode, no cross-frame compression/AEAD, and range reads that do not decode unrelated frames.

This decision is refined by ADRs 0046/0058/0063/0064, refines ADRs 0026/0031/0037, and is tracked by
`T-PROTOCOL-01`, `T-OBJECT-01`, and `V2-OBJ-002/004/006/007/012/013/016..021`.
