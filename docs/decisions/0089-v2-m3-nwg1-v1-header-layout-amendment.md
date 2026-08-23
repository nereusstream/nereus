# ADR 0089: V2 M3 NWG1 v1 Header layout amendment

## Status

Accepted on 2026-08-23 as an amendment to [ADR 0088](0088-v2-m3-nwg1-implementation-input-closure.md) before any
production NWG1 byte, machine projection, golden vector, receipt, or scenario evidence existed. This closes the last
ambiguity in the 256-byte NWG1 v1 Header input without changing the already accepted body architecture, cryptographic
preimages, directory rows, parser caps, failure model, or evidence boundary.

Because no production writer, reader, projection, or immutable NWG1 byte exists, this is an input correction rather
than a persisted-wire compatibility change. `wireVersion` remains `1`; incrementing it would invent a migration from a
format that has never been emitted. Any incompatible change after the first accepted production bytes requires the
version/amendment rule in ADR 0088.

## Context

ADR 0088 fixed `headerLength=256`, Header-derived length equations, required-zero handling, Header CRC32C, commitments,
and the exact Header as Directory/Frame AAD. It intentionally required the future `docs/v2/wire/nwg1-v1.json`
projection to transcribe an accepted offset table, but the normative documentation did not yet contain that table.
Implementation therefore could not prove that two writers assigned identical offsets or that every reserved byte was
covered.

The earlier overview also described a node session as a Header fact even though the accepted Object identity and HKDF
tuple are exactly `{shardId, shardRunEpoch, laneId, laneSequence}`. A control-plane Root may bind its own run/session
authority, but duplicating that session in every Object Header would introduce a second physical identity input.

## Decision

### Exact 256-byte HeaderV1

The Header is big-endian and has exactly this layout. Offsets are zero-based; array lengths are in bytes.

| Offset | Size | Field | Type/value |
| ---: | ---: | --- | --- |
| 0 | 4 | `magic` | ASCII `NWG1` |
| 4 | 2 | `wireVersion` | `u16=1` |
| 6 | 2 | `headerLength` | `u16=256` |
| 8 | 4 | `headerFlags` | `u32=0` |
| 12 | 2 | `protocolKind` | `u16` closed code |
| 14 | 2 | `requiredZeroA` | all zero |
| 16 | 4 | `shardId` | `u32` |
| 20 | 8 | `shardRunEpoch` | `u64` in the accepted non-negative signed-long domain |
| 28 | 8 | `laneSequence` | `u64` in the accepted non-negative signed-long domain |
| 36 | 4 | `packingPolicyVersion` | `u32` |
| 40 | 8 | `resolvedTargetPayloadBytes` | `u64` |
| 48 | 8 | `resolvedLingerNanos` | `u64` |
| 56 | 4 | `requiredZeroB` | all zero |
| 60 | 8 | `actualPayloadBytesAtPlanSeal` | `u64` |
| 68 | 8 | `actualCloseLingerNanos` | `u64` |
| 76 | 4 | `directoryPlaintextLength` | `u32` |
| 80 | 4 | `directoryStoredLength` | `u32` |
| 84 | 4 | `bindingContextCount` | `u32` |
| 88 | 4 | `appendUnitCount` | `u32` |
| 92 | 4 | `frameCount` | `u32` |
| 96 | 8 | `directoryPrefixEnd` | `u64` |
| 104 | 8 | `canonicalBodyLength` | `u64` |
| 112 | 1 | `laneId` | `u8`, exactly the permanent packing-class ID |
| 113 | 1 | `frameCodecRegistryKind` | `u8` |
| 114 | 1 | `frameCodecRegistryVersion` | `u8` |
| 115 | 1 | `objectDigestKind` | `u8=1`, `SHA256` |
| 116 | 1 | `objectDigestVersion` | `u8=1` |
| 117 | 1 | `aeadKind` | `u8` |
| 118 | 1 | `aeadVersion` | `u8` |
| 119 | 1 | `kdfKind` | `u8` |
| 120 | 1 | `kdfVersion` | `u8` |
| 121 | 1 | `nonceLayoutVersion` | `u8` |
| 122 | 1 | `aeadTagBytes` | `u8=16` |
| 123 | 1 | `actualCloseReason` | `u8`, closed code `1..12` |
| 124 | 32 | `protocolCellCommitment` | raw SHA-256 commitment |
| 156 | 32 | `cellProviderScopeId` | exact typed 32-byte identity |
| 188 | 32 | `walRunRootSha256` | raw SHA-256 of the canonical Root authority |
| 220 | 32 | `wrappedEnvelopeCommitment` | raw SHA-256 commitment |
| 252 | 4 | `crc32c` | CRC32C/v1 |

The table is gap-free and totals exactly 256 bytes. `headerFlags`, bytes `[14,16)` (`requiredZeroA`), and bytes
`[56,60)` (`requiredZeroB`) are zero in every canonical writer output and rejected when non-zero. Header CRC32C treats
bytes `[252,256)` as zero, covers bytes
`[0,256)`, and is written at offset 252. The resulting exact Header is the Header portion of every Directory and Frame
AAD.

There is no node-session field, alias, optional extension, or reserved compatibility tail. The Header contains neither
the Object body digest nor a duplicate packing-class field. `laneId` is the permanent class ID:
`0=OBJECT_LATENCY`, `1=OBJECT_BALANCED`, and `2=OBJECT_COST`.

### Closed Header codes and policy facts

`objectDigestKind/objectDigestVersion=1/1` means SHA-256/v1 over the exact canonical body. It is the closed Object
digest pair for NWG1 v1. The existing `ProtocolKind`, codec registry, AEAD, KDF, nonce-layout and policy code tables
remain unchanged.

`resolvedTargetPayloadBytes` and `resolvedLingerNanos` record the exact values resolved for this sealed plan.
`actualPayloadBytesAtPlanSeal` counts uncompressed protocol-native frame bytes, and `actualCloseLingerNanos` measures
open age through plan seal, excluding compression, KMS, encryption and Provider latency.
The permanent lane meanings are fixed, but the normal target/linger selections and quantitative policy remain owned by
`V2-OPEN-OBJ-19` evidence; this Header table does not select them.

`actualCloseReason` is the first satisfied reason in the already accepted M3-I0 order. The exact non-zero codes are:

| Code | Name |
| ---: | --- |
| 1 | `OBJECT_BODY_CAP` |
| 2 | `DIRECTORY_CAP` |
| 3 | `APPEND_UNIT_CAP` |
| 4 | `FRAME_CAP` |
| 5 | `EARLIEST_REQUEST_DEADLINE` |
| 6 | `HANDOFF` |
| 7 | `RUN_STOP` |
| 8 | `POLICY_CHANGE` |
| 9 | `RESOURCE_PRESSURE` |
| 10 | `EXPLICIT_FLUSH` |
| 11 | `TARGET_BYTES` |
| 12 | `LINGER_EXPIRED` |

All twelve accepted reasons remain distinct. An earlier implementation note counted them as `1..11`; that count was
wrong and creates no authority to delete, merge, or renumber an accepted reason. Code zero and every value outside
`1..12` are rejected.

### Projection authority

The future `docs/v2/wire/nwg1-v1.json` projection must mechanically transcribe this ADR's field names, offsets, widths,
types, fixed values, required-zero bytes, and closed code references. The projection cannot add, omit, reorder, rename,
or reinterpret a field and cannot create an independent Header authority. Production constants and exact comparison
tests must prove byte-for-byte equality with both this table and the projection before any writer persists NWG1 v1.

## Consequences

- ADR 0088 remains the authority for NWG1 v1 architecture, rows, cryptography, parser caps, mutation/trace contracts,
  and gate/evidence boundaries; this ADR amends only the previously missing Header offsets and directly dependent code
  facts.
- The node session may remain a WalRun control-plane/Root authority where separately specified, but it is not copied
  into the NWG1 Header, leaf key, HKDF info, nonce, or Object identity.
- The first production projection and immutable vectors must use `wireVersion=1` and this exact table. There is no
  `wireVersion=2` merely to correct documentation before production bytes exist.
- Positive Storage Epoch ordinal, mixed-policy production support, exact production Zstandard output, synthetic
  complete Root/Pointer wire, target/linger selection, Provider admission and scenario promotion remain outside this
  amendment.
