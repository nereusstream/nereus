# Nereus Storage Object Format

> 状态：Object WAL v1 `Implemented`；cursor snapshot V1 已通过 F3-M1 implementation/final gate；
> 其他 object families `Designed/Reserved`
> Durable Object WAL bytes 以代码、Phase 1 code-level design 和 golden tests 为准。

## 1. Format families

Nereus shared data plane 需要多类 immutable objects：

| Family | Purpose | Visibility/reference authority | Status |
| --- | --- | --- | --- |
| Multi-stream WAL object | primary Object WAL bytes | reachable append + generation-0 index | Implemented v1 |
| Index object | large entry/projection index | offset-index reference | Reserved |
| Stream compacted object | per-stream higher-generation read target | generation publish | Designed |
| Cursor snapshot | large ack state | cursor-state CAS ref | Implemented/final-gated as F3-M1 storage primitive；root publication belongs to M2 |
| Transaction snapshot | large txn/pending-ack state | txn-state ref | Designed |
| SBT/SDT table file | analytical/table projection | catalog snapshot/delivery lineage | Designed |

Object existence never makes a logical range visible。Object keys are placement identities，not ownership or
ordering truth。

## 2. Common invariants

1. Objects are immutable after successful put。
2. Logical offsets are assigned by stream-head commit，not encoded object order。
3. Multi-stream slices commit independently。
4. Every referenced byte range has explicit bounds、format version and checksums。
5. Fixed-width numeric fields use a declared byte order；enum ordinal is never durable encoding。
6. Unknown/unsupported major version、type、compression or required section fails closed。
7. Object list is audit/GC input only。
8. Generation replacement preserves offset/projection semantics。
9. Stored bytes checksum and canonical format checksum can be different domains。

## 3. Implemented Object WAL v1

The executable v1 layout is：

```text
WALObject
  CommonHeader                         48 bytes
  Section(WAL_OBJECT_HEADER)
  Section(STREAM_SLICE_DIRECTORY)
  Section(PAYLOAD_BLOCK)               one per slice in current writer
  Section(ENTRY_INDEX)                 one per slice
  Section(FOOTER)
```

### 3.1 Constants

| Field | v1 value |
| --- | --- |
| magic | ASCII `NRS1` |
| format major/minor | `1 / 0` |
| object type id | `1` (`MULTI_STREAM_WAL_OBJECT`) |
| fixed-width byte order | little endian |
| common header length | 48 bytes |
| section header length | 16 bytes |
| checksum | CRC32C |
| compression | `NONE` only |
| encryption info | empty only |
| payload format | `OPAQUE_RECORD_BATCH` only |
| entry-index location | `OBJECT_FOOTER` only |

Durable section ids：

| Section | Id |
| --- | --- |
| `WAL_OBJECT_HEADER` | 1 |
| `STREAM_SLICE_DIRECTORY` | 2 |
| `PAYLOAD_BLOCK` | 3 |
| `ENTRY_INDEX` | 4 |
| `FOOTER` | 5 |

### 3.2 Common header

Encoded fields in order：

```text
magic[4]
formatMajor: int32
formatMinor: int32
objectType: int32
flags: int32
headerLength: int32
footerOffset: int64
footerLength: int32
headerChecksum: uint32
objectChecksum: uint32
encryptionInfoLength: int32
```

Checksum rules：

- `headerChecksum` covers the 48-byte header with both checksum fields zeroed；
- `objectChecksum` covers the full object with only its own field zeroed；
- API CRC32C value uses lowercase 8-character hex；
- `storageChecksum` covers exact uploaded bytes including populated `objectChecksum`，and is passed to
  `ObjectStore.putObject`；
- every section checksum covers its payload only。

### 3.3 Section envelope

```text
sectionType: int32
sectionVersion: int32             // v1 requires 1
sectionLength: int32
sectionChecksum: uint32
payload[sectionLength]
```

The decoder verifies section bounds/checksum before interpreting payload。Current decoder rejects newer
minor versions instead of silently skipping unknown sections；future compatibility needs explicit tests before
loosening this rule。

## 4. WAL object header

Current logical fields：

```text
objectId
cluster
writerId
writerRunIdHash
writerEpoch
writerVersion
createdAtMillis
compression
streamSliceCount
payloadBlockCount
minEventTimeMillis
maxEventTimeMillis
```

`writerRunIdHash` prevents object id/key collisions after process restart when local sequence restarts。
`cluster`、writer/object/slice dynamic path components are encoded before entering keys。

## 5. Stream slice descriptor

Current descriptor：

```text
sliceOrdinal
streamId
sliceId
writerEpoch
relativeBaseOffset
entryCount / recordCount
logicalBytes
payloadOffset / payloadLength
entryIndexOffset / entryIndexLength
sliceChecksum
payloadFormat
min/max event time
canonical schema refs
```

Rules：

- encoded slices are deterministic by stream id then request order；
- `sliceOrdinal` is assigned after final ordering；
- `sliceId = objectId + "/" + zero-padded ordinal` and never contains final stream offset；
- `relativeBaseOffset` is normally zero in Phase 1；final base offset comes from head commit；
- `logicalBytes` is uncompressed caller-visible payload size；
- `sliceChecksum = CRC32C(slicePayloadBytes || encodedEntryIndexBytes)`；
- descriptor and entry-index ranges stay within object bounds and cannot overflow。

## 6. Entry index v1

```text
EntryIndex
  entryCount
  recordCount
  EntryIndexItem[]

EntryIndexItem
  entryOrdinal
  relativeBaseOffset
  recordCount
  payloadOffset / payloadLength
  eventTimeMillis
  canonical attributes
```

Validation：

- ordinals are zero-based and contiguous；
- first relative offset is zero；following offsets equal previous offset + record count；
- total record count matches header/slice；
- payload ranges are non-negative/in-bounds；zero-length payload is legal；
- Phase 1 opaque entries have `recordCount == 1`；
- attributes use deterministic UTF-8 key order。

Mapping after logical commit：

```text
recordOffset = committedRange.start + item.relativeBaseOffset
objectByteOffset = slice.payloadOffset + item.payloadOffset
```

Future storage policy may choose inline/footer/index object by size，but current writer and reader support
only footer。`INLINE` and `INDEX_OBJECT` are API/metadata reservations，not implemented format support。

## 7. Footer and decode rules

Footer contains：

```text
footerChecksum
objectId
sliceCount
reserved entry-index-directory offset/length
full slice descriptors
```

Decoder order：

1. verify minimum length、magic、version、object type and supported header shape；
2. verify header checksum；
3. verify object checksum；
4. scan and verify every section；
5. verify footer offset/length and footer checksum；
6. decode descriptors and validate all referenced bounds；
7. reject trailing/corrupt payloads with `UNSUPPORTED_FORMAT`。

Checksum-valid but structurally invalid bytes remain invalid；CRC is not a schema validator。

## 8. Object identity and lifecycle

Current Object WAL key is deterministic from cluster、writer identity/run and object sequence。The exact
builder lives in `DefaultWalObjectWriter` and shared key helpers。

Lifecycle domains：

```text
bytes uploaded
  -> manifest validated
  -> zero or more stream slices logically committed
  -> references repaired/audited
  -> eligible for GC only when all slice/task/reader/catalog references are gone
```

An object with no reachable committed slice is orphan。An object with one committed slice and one orphan slice
is not deletable as a whole。

## 9. Offset-index record boundary

Offset index is Oxia metadata，not an object section。For a generation it includes at least：

```text
streamId / offset range / generation
physical target identity and byte range
payload format / schema refs
entry-index reference
slice checksum
projection identity
commitVersion
```

Generation 0 is repairable from reachable append commit。Higher generations are conditionally published by
F4。Phase 1.5 implements the tagged target union and dual-read/new-write generic metadata while retaining the
object-specific legacy decoder/adapter boundary。This does not alter Object WAL bytes or
implement BookKeeper IO。

## 10. Designed compacted object

> Status: Designed

A read-optimized object is per-stream and covers a declared half-open offset range. Its durable header includes a
closed read-view value (`COMMITTED` or `TOPIC_COMPACTED`); view-specific indexes and generation ordering never cross.
Required logical columns/fields：

- stream offset and record identity；
- publish/event time、key、payload/normalized fields；
- schema reference；
- producer/sequence and transaction visibility metadata；
- Pulsar entry/batch projection information；
- source generation/checksum lineage。

For `COMMITTED + PULSAR_ENTRY_V1`, the schema also contains one opaque binary Entry value and boundary per stream
offset. A reader must reproduce the exact generation-0 ManagedLedger Entry bytes; normalized columns are auxiliary and
cannot be the reconstruction authority. Container-level lossless compression is allowed, but Entry split/merge/rebatch/
re-encoding is forbidden. A lossy keyed-compaction result is stored only as `TOPIC_COMPACTED` and is never referenced
by the ordinary offset-index namespace.

Default target is Parquet with row-group offset bounds。Before implementation F4 must freeze：

- projection-preserving schema；
- row-group index/checksum semantics；
- topic-compaction tombstone/coverage behavior；
- closed read-view encoding and separate view index references；
- exact opaque Entry checksum/round-trip rules for `PULSAR_ENTRY_V1`；
- higher-generation publish and fallback validation。

## 11. Snapshot objects

> Status: Cursor snapshot V1 codec/store implemented and F3-M1 final-gated；transaction snapshot only Designed

Common snapshot rules：

- immutable bytes with version、owner identity、base state and checksum；
- authoritative only when an Oxia CAS record references the exact object/version/checksum；
- upload-before-CAS failure creates orphan；
- replacement keeps old snapshot protected until the new reference is durable and readers release it。

### Cursor snapshot

F3 V1 is a full normalized ack-state base。The current cursor root may add bounded whole-range deltas and
partial-entry overrides；root CAS remains visibility authority。

Logical key：

```text
{clusterComponent}/cursor-snapshots/v1/{streamIdComponent}/
  {cursorNameHash}/{cursorGeneration019}/{random128SnapshotId}.ncs
```

Wire layout uses big-endian `NCS1` header、exact stream/projection/cursor/generation/source-sequence identity、
normalized half-open range section、partial-batch remaining-word section and `NCF1` footer。Header、sections and whole
object have explicit CRC32C；all lengths/counts/UTF-8/canonical order/trailing bytes are strictly validated。Exact
fields and checksum coverage are frozen in
`../phase-3-cursor-subscription/03-oxia-metadata-and-snapshot-format.md`。

The writable-ledger `ownerSessionId` is intentionally root-only and absent from immutable snapshot bytes。A new owner
claims the root while preserving the same snapshot reference；root version/session fencing，not snapshot rewriting，
prevents an old broker from publishing a later cursor mutation。

Publish is immutable PUT-if-absent -> exact HEAD -> cursor-root CAS。Stable missing/corrupt reference fails cursor
open；no older snapshot or inline-only fallback is allowed。CAS-lost/old objects remain F4 orphan/reference-GC input。

### Transaction snapshot

Contains affected stream ranges、pending-ack/abort state and transaction identity。Transaction terminal state，
not snapshot existence，controls visibility。

## 12. Designed table files

> Status: Designed

SBT/SDT files carry lineage：

```text
streamId
offsetStart / offsetEnd
source generation and commit lineage
object ids/checksums
schema snapshot
catalog snapshot or delivery id
```

SBT may reuse compacted objects。SDT can create target-specific files。Catalog commits never change stream
offsets or producer acknowledgement。

## 13. Format evolution

- Major version changes incompatible required semantics。
- Minor version may add optional sections only after reader skip/compatibility rules are implemented and tested。
- Durable numeric ids are registry values，not Java enum ordinals。
- New compression must define compressed/uncompressed offsets、checksums and `ReadBatch` source ranges。
- New encryption must define header visibility、nonce/key ref、range-read and checksum order。
- New entry-index location needs writer、metadata codec、resolver、reader and migration tests together。
- Golden bytes are required for every durable encoder change。

## 14. GC reference model

An object can be protected by：

- reachable primary append；
- visible offset-index generation；
- reader lease/cache pin；
- cursor/transaction snapshot ref；
- materialization/compaction/repair task；
- SBT/SDT catalog snapshot/delivery；
- retention/orphan grace policy。

Physical generation replacement also retains superseded offset-index metadata until the same reference scan permits
retirement. Reader protection is an authoritative resolve/pin/read/release lease, not merely an in-process cache hit.
Old incarnation projection mirrors are separately collectible only after current topic authority and every recovery/
task/audit reference have moved away from their stream.

GC may use object listing to find candidates，but all deletion decisions are validated against authoritative
metadata and ownership。

## 15. Implementation sources

- Object WAL code：`../../nereus-object-store/src/main/java/com/nereusstream/objectstore/wal/`
- Exact Phase 1 design：`../phase-1-core-stream-storage/03-object-wal-and-index.md`
- Phase 1.5 generic target metadata：`../phase-1.5-core-storage-foundation/02-metadata-schema-and-compatibility.md`
- Object WAL review：`../phase-1-core-stream-storage/11-m3-object-wal-review-2026-07-08.md`
- Commit semantics：`nereus-commit-protocol.md`
- F4 target：`nereus-future4-compaction-generation.md`
- F3 cursor snapshot target：`../phase-3-cursor-subscription/03-oxia-metadata-and-snapshot-format.md`
- F6 target：`nereus-future6-lakehouse-sbt-sdt.md`
