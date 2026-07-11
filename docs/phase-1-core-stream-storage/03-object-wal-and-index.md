# 03 Object WAL and Index Design

本文定义 Phase 1 的 object store abstraction、object WAL writer/reader、stream slice 和 entry
index 设计。Phase 1 只实现 WAL object，不实现 compacted object writer。

M3 implementation status, verified 2026-07-10 and integrated by M4 on 2026-07-11:

- `nereus-object-store` now exposes the production `ObjectStore` API, object options/results, CRC32C
  helper, and shared range validation；
- `DefaultWalObjectWriter` implements the Phase 1 in-memory WAL encoder with common header, section
  envelopes, footer, deterministic slice descriptors, `OBJECT_FOOTER` entry indexes, storage checksum,
  and WAL canonical object checksum；
- M4 adds the exact `prepare`/`upload` boundary and manifest-source fields to `WalWriteResult` while
  retaining the original one-shot `write` convenience API；
- `DefaultWalObjectReader` reads the full resolved slice payload plus footer entry index, verifies
  `concat(slicePayloadBytes, entryIndexBytes)`, clips by offset/record/byte limits, and uses an injected
  `ReadResourceGuard` plus `WalReadObserver` for M5 core integration；
- `LocalFileObjectStore` lives under `src/testFixtures` only. It gives tests immutable put, head, range
  read, path traversal rejection, duplicate `ifAbsent` handling, checksum validation, and
  `deleteAllForTesting()` scoped to the injected temp root；
- M3 tests currently cover one-slice round trip, multi-slice round trip, pre-upload
  sizing/force-single-stream/ZSTD validation, upload-timeout propagation, advisory target size behavior,
  storage checksum mismatch, corrupt slice/index/object checksums, descriptor bounds, unsupported entry
  index locations, checksum-consistent invalid entry-index/descriptor metadata, entry-index golden bytes,
  read-budget rejection before object IO, multi-range read byte-budget clipping, zero-byte entry reads,
  unsafe local keys, symlink escape, duplicate `ifAbsent`, range-read-past-EOF, failed-write invisibility,
  and test-only cleanup.

Review correction: `11-m3-object-wal-review-2026-07-08.md` found two completion blockers: multi-range
read byte-budget classification could incorrectly return `READ_LIMIT_TOO_SMALL`, and
`LocalFileObjectStore` did not reject final symlink escape. Both have now been fixed with focused tests;
the same fix pass also wraps checksum-consistent invalid WAL/index metadata as `UNSUPPORTED_FORMAT`.
`./gradlew :nereus-object-store:test phase1Check check` passed on 2026-07-10；M3 is complete.

## 1. Object Store Responsibility

Object store layer owns:

- put immutable object bytes；
- range-read object bytes；
- head/checksum verification；
- object key generation policy；
- WAL object encode/decode；
- stream slice directory；
- entry index encode/decode。

Object store layer does not own:

- offset assignment；
- visibility；
- append fencing；
- trim low-watermark；
- GC correctness decision。

## 2. Package Plan

```text
io.nereus.objectstore
  ObjectStore
  PutObjectOptions
  PutObjectResult
  RangeReadOptions
  RangeReadResult
  HeadObjectOptions
  HeadObjectResult
  Crc32cChecksums
  RangeChecks

io.nereus.objectstore.wal
  CompressionType
  WalWriteRequest
  WalWriteOptions
  WalStreamSliceInput
  WalWriteResult
  WrittenStreamSlice
  WalObjectWriter
  WalObjectReader
  WalObjectLayout
  StreamSliceDescriptor
  EntryIndex
  EntryIndexItem
  EntryIndexEncoder
  EntryIndexDecoder
  ReadResourceGuard
  WalReadObserver

io.nereus.objectstore.testing (test fixtures)
  LocalFileObjectStore
```

`ObjectId`, `ObjectKey`, `ObjectRange`, `Checksum`, `PayloadFormat`, `ReadBatch`, and
`ResolvedObjectRange` are imported from `nereus-api`; `nereus-object-store` must not redefine them.

The current implementation keeps `WalObjectHeader`, footer payload, and section headers as internal
binary encoders rather than public Java records. The stable public contract is the object-store API plus
neutral WAL result/descriptors above.

## 3. `ObjectStore` Interface

```java
public interface ObjectStore extends AutoCloseable {
    CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ByteBuffer payload,
            PutObjectOptions options);

    CompletableFuture<RangeReadResult> readRange(
            ObjectKey key,
            long offset,
            long length,
            RangeReadOptions options);

    CompletableFuture<HeadObjectResult> headObject(
            ObjectKey key,
            HeadObjectOptions options);
}
```

Phase 1 intentionally omits delete from the public core path. GC can add a restricted delete interface later.

`PutObjectOptions`:

```java
public record PutObjectOptions(
        String contentType,
        Checksum expectedChecksum,
        boolean ifAbsent,
        Map<String, String> metadata,
        Duration timeout) {
}

public record PutObjectResult(
        ObjectKey key,
        long objectLength,
        Checksum checksum,
        String etag) {
}

public record RangeReadOptions(
        Optional<Checksum> expectedChecksum,
        Duration timeout) {
}

public record HeadObjectOptions(
        Duration timeout) {
}

public record RangeReadResult(
        ObjectKey key,
        long offset,
        long length,
        ByteBuffer payload,
        Optional<Checksum> checksum) {
}

public record HeadObjectResult(
        ObjectKey key,
        long objectLength,
        Checksum checksum,
        Map<String, String> metadata) {
}
```

Rules:

- object keys are immutable；
- overwrite is not allowed for WAL objects；
- `PutObjectOptions.expectedChecksum` and `PutObjectResult.checksum` are storage checksums over the exact
  uploaded bytes；
- `PutObjectOptions.timeout` is the object-store write deadline supplied by the WAL writer. It must be
  positive and should be propagated to the concrete client rather than only wrapped at the caller；
- `putObject` must return object length and actual storage checksum；
- `readRange` must validate range boundaries and optionally checksum the exact returned byte range；
- `RangeReadOptions.timeout` and `HeadObjectOptions.timeout` must be positive and propagated to the
  concrete client；
- `RangeReadOptions.expectedChecksum` must be supplied only when the caller's expected checksum covers
  exactly the requested byte range. WAL slice checksum covers slice payload plus entry index, so
  `WalObjectReader` must verify that after reading the required sections instead of passing it to a
  payload-only `readRange` call；
- Phase 1 object store implementations must copy the remaining `ByteBuffer` bytes or retain an immutable
  read-only view before returning from `putObject`. Callers may not observe partial mutation behavior；
- `RangeReadResult.payload` must be a read-only `ByteBuffer` whose position/limit cover exactly the
  returned range bytes；
- Phase 1 may buffer the whole WAL object in memory, but `WalWriteOptions.maxObjectBytes` is a hard bound
  on the final encoded object length, including common header, section headers, payload blocks, entry
  index bytes, footer, and checksum fields. A future streaming upload API can be added without changing
  visibility semantics；
- `WalWriteOptions.targetObjectSizeBytes` is an advisory flush/grouping target. The writer must not fail a
  single already-planned request only because the final encoded length is greater than the target; it fails
  only when the encoded length is greater than `maxObjectBytes`；
- `putObject` must not be called until the writer has computed the final encoded object length and
  verified it is within `maxObjectBytes`；
- `nereus-object-store` must not import Oxia metadata record classes. It returns neutral object and slice
  descriptors; `nereus-core` or `nereus-metadata-oxia` maps those descriptors to metadata records。

## 4. Object Identity

Object id should be generated before upload:

```text
wo-{yyyyMMddHHmmss}-{writerIdHash}-{writerRunIdHash}-{sequence}
```

`writerIdHash` is produced by `DeterministicIds.stableHashComponent(writerId)`.
`writerRunIdHash` is produced by `DeterministicIds.randomRunIdHash(...)` from a random writer process
incarnation id generated when `DefaultStreamStorage` starts. The run id must have at least 128 bits of
entropy from a cryptographically strong random source. Both helpers live in `nereus-api` and return full
key-safe SHA-256 based components. Raw `writerId` and `writerRunId` must not be embedded in
`objectId` because `objectId` is also used as an Oxia path component. Truncating either hash is not allowed
in Phase 1 without a written collision budget and migration plan.

`sequence` is monotonically increasing within one writer process. It does not need to survive restart
because `writerRunIdHash` changes. This avoids object id collision when a process restarts within the same
timestamp bucket and reuses the same configured `writerId`.

Sequence allocation rules:

- maintain the next object sequence in a thread-safe per-process counter；
- consume one sequence value for every new physical WAL object attempt before object bytes are encoded；
- never rewind or reuse a consumed sequence value within the same `writerRunIdHash`, even when validation,
  upload, manifest write, or commit later fails；
- retry of the same in-memory physical WAL object attempt reuses the same `objectId` and `sliceId`；
- a caller-level retry or a new physical WAL object attempt must allocate a fresh sequence and therefore a
  fresh `objectId`.

Object key:

```text
{clusterComponent}/wal/{yyyy}/{MM}/{dd}/{writerIdHash}/{writerRunIdHash}/{objectId}.nrs
```

`clusterComponent = KeyComponentCodec.encodeComponent(cluster)` using the same shared helper as Oxia
metadata paths.
Raw `cluster` must not be embedded directly in object keys because it may contain `/`, reserved prefixes,
or other filesystem/object-store significant characters.

Object key is for physical layout and operations. It is not semantic truth.

## 5. WAL Object Layout

Phase 1 layout follows `docs/design/nereus-storage-object-format.md` but can start with a minimal encoder.

```text
WALObject
  CommonHeader
  WALObjectHeader
  StreamSliceDirectory
  PayloadBlock[0..N]
  EntryIndexSection[0..N]
  Footer
```

Common header:

```java
public record CommonHeader(
        byte[] magic,
        int formatMajorVersion,
        int formatMinorVersion,
        ObjectType objectType,
        int flags,
        int headerLength,
        long footerOffset,
        int footerLength,
        Checksum headerChecksum,
        Checksum objectChecksum,
        Optional<String> encryptionInfoRef) {
}
```

Variable-length sections use a common envelope:

```java
public record SectionHeader(
        int sectionType,
        int sectionVersion,
        int sectionLength,
        Checksum sectionChecksum) {
}
```

Phase 1 section types are `WAL_OBJECT_HEADER`, `STREAM_SLICE_DIRECTORY`, `PAYLOAD_BLOCK`,
`ENTRY_INDEX_SECTION`, and `FOOTER`. `encryptionInfoRef` must be empty in Phase 1.

WAL header:

```java
public record WalObjectHeader(
        ObjectId objectId,
        String cluster,
        String writerId,
        String writerRunIdHash,
        long writerEpoch,
        String writerVersion,
        long createdAtMillis,
        CompressionType compression,
        int streamSliceCount,
        int payloadBlockCount,
        long minEventTimeMillis,
        long maxEventTimeMillis) {
}
```

```java
public enum CompressionType {
    NONE,
    ZSTD
}
```

Phase 1 format constants:

```text
magic = "NRS1"
formatMajorVersion = 1
formatMinorVersion = 0
objectType = MULTI_STREAM_WAL_OBJECT
defaultCompression = NONE
checksum = CRC32C required
byteOrder = little endian for fixed-width numeric fields
encryptionInfoRef = empty
```

Phase 1 binary ids:

| Name | Id |
| --- | --- |
| `ObjectType.MULTI_STREAM_WAL_OBJECT` | `1` |
| `SectionType.WAL_OBJECT_HEADER` | `1` |
| `SectionType.STREAM_SLICE_DIRECTORY` | `2` |
| `SectionType.PAYLOAD_BLOCK` | `3` |
| `SectionType.ENTRY_INDEX_SECTION` | `4` |
| `SectionType.FOOTER` | `5` |

The ids above are durable binary-format values. Java enum ordinal must never be used for object or section
encoding.

Phase 1 implementation requirement:

- support `CompressionType.NONE` end to end；
- reject `CompressionType.ZSTD` with `UNSUPPORTED_FORMAT` until a block compression layout is specified；
- when compression is added later, the design must explicitly define whether entry offsets are compressed
  physical offsets, uncompressed logical offsets, or block-local offsets, and how clipped reads report
  `ReadBatch.sourceObjectOffset/sourceObjectLength`。

Header/footer encoding rules:

- Phase 1 writer builds the full object bytes in memory before `putObject`, so `footerOffset`,
  `footerLength`, and `objectChecksum` are known before upload；
- on-object CRC32C checksum fields are encoded as fixed-width little-endian unsigned 32-bit values；
- API-level `Checksum.value` for CRC32C uses lowercase 8-character hexadecimal text；
- every variable-length section is encoded as `SectionHeader + payload`, and `sectionChecksum` covers only
  that section payload with canonical deterministic bytes；
- `headerChecksum` is CRC32C over the encoded common header with both `headerChecksum` and
  `objectChecksum` bytes set to zero；
- `objectChecksum` is CRC32C over the entire encoded object with the `objectChecksum` bytes in the common
  header set to zero；
- `storageChecksum` is a separate CRC32C over the exact bytes passed to `putObject`, including the
  populated `objectChecksum` field. It is the checksum used by `PutObjectOptions.expectedChecksum`,
  `PutObjectResult.checksum`, and object-store `headObject`；
- decoder must verify magic, major version, object type, header checksum, footer bounds, footer checksum,
  and object checksum before trusting directory offsets；
- Phase 1 decoder accepts only `formatMajorVersion=1` and `formatMinorVersion=0`. Accepting a newer minor
  version requires compatibility tests for optional sections and skipped unknown sections；
- decoder must reject non-empty `encryptionInfoRef` in Phase 1 with `UNSUPPORTED_FORMAT`；
- all offsets and lengths in header, footer, descriptors, and entry index are non-negative and must stay
  within `WalWriteResult.objectLength`。

Footer:

```java
public record WalObjectFooter(
        ObjectId objectId,
        int streamSliceCount,
        List<StreamSliceDescriptor> slices,
        long entryIndexDirectoryOffset,
        long entryIndexDirectoryLength,
        Checksum footerChecksum) {
}
```

`footerChecksum` is CRC32C over the encoded footer with the checksum field set to zero.

## 6. Stream Slice Descriptor

```java
public record StreamSliceDescriptor(
        int sliceOrdinal,
        StreamId streamId,
        String sliceId,
        long writerEpoch,
        long relativeBaseOffset,
        int entryCount,
        int recordCount,
        long logicalBytes,
        long payloadOffset,
        long payloadLength,
        long entryIndexOffset,
        long entryIndexLength,
        Checksum checksum,
        PayloadFormat payloadFormat,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        List<SchemaRef> schemaRefs) {
}
```

Rules:

- `relativeBaseOffset` is local to the slice and normally `0` for Phase 1 append；
- `sliceOrdinal` is zero-based in the encoded object layout；
- Phase 1 writer generates `sliceId = objectId + "/" + zero-padded sliceOrdinal` before upload. `sliceId`
  is object-local identity and must not encode final stream offsets；
- slices are encoded deterministically by `streamId.value()`, then original request order. The assigned
  `sliceOrdinal` follows the final encoded order；
- final `offsetStart` comes only from Oxia commit result；
- `logicalBytes` counts caller-visible uncompressed payload bytes and is used by metadata to advance
  `cumulativeSize`；
- `payloadOffset` and `payloadLength` refer to the physical object；
- descriptor must be sufficient to build `CommitSliceRequest` after upload；
- slice checksum covers the canonical byte sequence `slicePayloadBytes || entryIndexBytes`。
  `slicePayloadBytes` are the bytes in `[payloadOffset, payloadOffset + payloadLength)`.
  `entryIndexBytes` are the encoded entry index bytes, regardless of whether they are inline, in the WAL
  footer, or in a future index object。
- `payloadOffset/payloadLength` and `entryIndexOffset/entryIndexLength` must be in object bounds and must
  not overlap unless an implementation explicitly stores entry index inline and marks the `EntryIndexRef`
  as `INLINE`。

## 7. Entry Index

Phase 1 needs entry boundaries even without Pulsar integration.

```java
public record EntryIndex(
        int entryCount,
        int recordCount,
        List<EntryIndexItem> entries) {
}

public record EntryIndexItem(
        int entryOrdinal,
        long relativeBaseOffset,
        int recordCount,
        long payloadOffset,
        long payloadLength,
        long eventTimeMillis,
        Map<String, String> attributes) {
}
```

Mapping:

```text
recordOffset = offsetIndex.offsetStart + entry.relativeBaseOffset + batchIndex
objectPayloadOffset = streamSlice.payloadOffset + entry.payloadOffset
```

`EntryIndexItem.payloadOffset` is relative to the beginning of the stream slice payload, not relative to
the whole object. `StreamSliceDescriptor.payloadOffset` is object-relative. This keeps entry indexes
stable if a test encoder lays out the same slice at a different object offset.

Opaque payload rule:

- for `OPAQUE_RECORD_BATCH`, every entry must have `recordCount=1`；
- for `recordCount>1`, the reader must have a protocol-aware way to expose sub-record boundaries or must
  return an entry-level projection that Future 2/5 understands；
- Phase 1 standalone tests should use one record per opaque entry；
- Phase 1 entry index stores entry boundaries and opaque entry attributes only. Protocol-specific
  projection hints belong in `ProjectionRef` or future higher-layer metadata, not in L0 entry index
  semantics。

Validation:

- `entryOrdinal` must be zero-based and strictly increasing；
- the first `relativeBaseOffset` must be `0`；
- each following `relativeBaseOffset` must equal the previous entry's `relativeBaseOffset +
  recordCount`；
- sum of entry `recordCount` equals `EntryIndex.recordCount`；
- entry `payloadOffset/payloadLength` must be non-negative and within the slice payload range. A zero
  `payloadLength` is valid for an empty opaque record；
- non-empty payload ranges should not overlap other entries for `OPAQUE_RECORD_BATCH`；
- entry `eventTimeMillis` must be non-negative and within the slice min/max event-time range；
- entry attributes must be encoded in deterministic UTF-8 key order；
- entry index encoding must be deterministic so checksum golden tests are stable。

Future storage policy:

| Size | Location |
| --- | --- |
| `<= 16 KiB` | inline in offset index value |
| `> 16 KiB` and `<= 4 MiB` | WAL object footer |
| `> 4 MiB` | independent `INDEX_OBJECT` |

Phase 1 supported storage policy:

1. writer emits `OBJECT_FOOTER` for every entry index, regardless of encoded size；
2. reader supports `OBJECT_FOOTER`；
3. `INLINE` and `INDEX_OBJECT` remain API/metadata enum values for forward compatibility；
4. if Phase 1 reader encounters `INLINE` or `INDEX_OBJECT`, it fails with `UNSUPPORTED_FORMAT`；
5. the future size-based policy above can be enabled only after adding codec, resolver, reader, and
   contract tests for the additional locations。

## 8. WAL Writer API

```java
public interface WalObjectWriter {
    PreparedWalObject prepare(WalWriteRequest request);

    CompletableFuture<WalWriteResult> upload(PreparedWalObject preparedObject);

    default CompletableFuture<WalWriteResult> write(WalWriteRequest request) {
        return upload(prepare(request));
    }
}
```

M4 split the original one-shot writer boundary into `prepare` and `upload` while retaining `write` as the
convenience path used by direct M3 callers. `prepare` allocates the object id once, performs the complete
sizing/layout/checksum pass, and returns an immutable `PreparedWalObject` with exact encoded bytes and a
pre-upload `WalWriteResult`. Core adjusts its buffer reservation from the conservative hard cap to
`PreparedWalObject.objectLength()` before calling `upload`. A retry of the same physical attempt reuses the
same prepared object rather than allocating a second object id or object sequence.

Request:

```java
public record WalWriteRequest(
        String cluster,
        String writerId,
        String writerRunIdHash,
        long writerEpoch,
        List<WalStreamSliceInput> slices,
        WalWriteOptions options) {
}
```

```java
public record WalWriteOptions(
        CompressionType compression,
        int targetObjectSizeBytes,
        int maxObjectBytes,
        Duration uploadTimeout,
        boolean forceSingleStreamObject) {
}
```

`WalWriteOptions` validation:

- `compression` is non-null. Phase 1 writer accepts only `CompressionType.NONE` and rejects
  `CompressionType.ZSTD` with `UNSUPPORTED_FORMAT` before object upload；
- `targetObjectSizeBytes > 0`；
- `maxObjectBytes > 0`；
- `targetObjectSizeBytes <= maxObjectBytes`；
- `uploadTimeout` is positive。

`WalObjectWriter` must support a `WalWriteRequest` with multiple slices from the first implementation.
`forceSingleStreamObject` is only a validation/planner guard: when true, the writer rejects a request whose
slices contain more than one `StreamId`; it does not split or rewrite the request. `DefaultStreamStorage`
may set this flag and pass exactly one append work item per object for the first end-to-end implementation,
while direct WAL tests still cover multi-slice layout with the flag disabled.

Before allocating the final output buffer or calling `ObjectStore.putObject`, the writer must run a sizing
pass over the planned layout:

- compute every section length, entry-index length, footer length, and final object length using `long`
  arithmetic and checked additions；
- reject negative, overflowing, or out-of-bounds offsets before any object-store write；
- reject an object whose final encoded length is greater than `WalWriteOptions.maxObjectBytes` with
  `INVALID_ARGUMENT` before upload；
- record `WalWriteResult.objectLength` as the same final encoded length used for the hard-limit check。

When calling `ObjectStore.putObject`, the writer must copy `WalWriteOptions.uploadTimeout` into
`PutObjectOptions.timeout`.

Slice input:

```java
public record WalStreamSliceInput(
        StreamId streamId,
        AppendBatch batch) {
}
```

Result:

```java
public record WalWriteResult(
        ObjectId objectId,
        ObjectKey objectKey,
        long objectLength,
        Checksum objectChecksum,
        Checksum storageChecksum,
        int formatMajorVersion,
        int formatMinorVersion,
        String writerVersion,
        long createdAtMillis,
        List<WrittenStreamSlice> slices) {
}
```

The format/writer/time fields were added in M4 because `nereus-core` must build the immutable object manifest
without depending on package-private WAL layout constants or inventing a second writer version/time source.

`objectChecksum` is the WAL canonical checksum stored in the object header. `storageChecksum` is the exact
uploaded-bytes checksum returned by the object store. They usually differ because the WAL canonical checksum
zeros the `objectChecksum` field while calculating.

Written slice:

```java
public record WrittenStreamSlice(
        StreamId streamId,
        String sliceId,
        long objectOffset,
        long objectLength,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        PayloadFormat payloadFormat,
        EntryIndexRef entryIndexRef,
        Checksum sliceChecksum,
        long minEventTimeMillis,
        long maxEventTimeMillis) {
}
```

`WrittenStreamSlice.objectOffset/objectLength` describe this stream slice's physical byte range inside the
object. `WalWriteResult.objectLength` describes the full object length.

`WrittenStreamSlice.logicalBytes` is copied into `CommitSliceRequest.logicalBytes`. For Phase 1 opaque
payloads it is the sum of the append entry byte lengths. Phase 1 does not apply object-level compression.
`WrittenStreamSlice.schemaRefs` is the canonicalized form of `AppendBatch.schemaRefs` and is then copied
into `CommitSliceRequest.schemaRefs` and the offset index.

## 9. WAL Writer State Machine

```text
NEW
  -> ADD_SLICE
  -> BUILD_LAYOUT
  -> ENCODE_BYTES
  -> PREPARED
  -> PUT_OBJECT
  -> VERIFY_CHECKSUM
  -> WRITTEN
```

Failure handling:

| State | Failure | Result |
| --- | --- | --- |
| `BUILD_LAYOUT` | invalid batch | append future fails, no object |
| `PUT_OBJECT` | store timeout | retriable failure, no stream-head commit |
| `VERIFY_CHECKSUM` | mismatch | non-retriable failure, no stream-head commit |
| `WRITTEN` before manifest | object may exist but is not visible | metadata layer may write manifest or orphan scanner handles TTL |

## 10. WAL Reader API

```java
public interface WalObjectReader {
    CompletableFuture<List<ReadBatch>> read(
            long startOffset,
            List<ResolvedObjectRange> ranges,
            ReadOptions options);
}
```

Algorithm:

1. range-read the full resolved stream slice payload section；
2. read/decode entry index if requested or needed for boundaries；
3. verify slice and entry-index checksums；
4. clip entries to `startOffset`, `maxRecords`, and `maxBytes`；
5. for Phase 1 `OPAQUE_RECORD_BATCH`, return one `ReadBatch` per selected entry with source object
   metadata。

The explicit `startOffset` is required because the first `ResolvedObjectRange` usually covers a larger
committed slice than the caller requested.

Opaque reader rule:

- one returned `ReadBatch` maps to one `EntryIndexItem`；
- adjacent opaque entry payloads must not be concatenated into one `ReadBatch`；
- zero-byte entries are selected by record budget, not byte budget, so they can be returned after
  `maxBytes` has been exactly consumed by previous entries；
- future formats may return larger batches only after their decoder/projection contract defines how entry
  boundaries are preserved。

Phase 1 reads the full resolved slice before clipping so the slice checksum can be verified. Future block
checksums can reduce read amplification without weakening corruption detection.

Phase 1 can perform one range read per resolved object range. Coalescing adjacent ranges is an optimization.

Full-slice read amplification is an accepted Phase 1 compromise, not a license for unbounded allocation.
`WalObjectReader` must reserve memory for the full checksum domain:
`ResolvedObjectRange.objectLength` plus the referenced entry-index byte length, using checked addition,
before the first `ObjectStore.readRange` starts. In M3, `DefaultWalObjectReader` accepts an injected
`ReadResourceGuard`; M5 core will bind that guard to `StreamStorageConfig.maxConcurrentObjectReads` and
`maxReadBufferBytes`. The object-store module keeps the reader API neutral, but the default reader
implementation must:

- reject negative or overflowing resolved range lengths before allocation；
- rely on the core read resource guard, or an equivalent injected guard, to fail with
  `BACKPRESSURE_REJECTED` before object IO when the full checksum-domain bytes cannot be reserved；
- release memory reservations on success, decode failure, checksum failure, timeout, cancellation, and
  close；
- report downloaded full-slice payload and entry-index bytes separately from returned clipped payload
  bytes；
- never claim a clipped subrange is independently checksum-verified unless a future block-level checksum
  format exists。

Read amplification metrics are owned by `nereus-core` metrics, but the M3 reader already exposes
`WalReadObserver.onSliceRead(fullSlicePayloadBytes, entryIndexBytes, returnedPayloadBytes)` so core can
emit:

```text
fullSliceAndIndexBytesDownloaded
returnedPayloadBytes
amplificationBytes = fullSliceAndIndexBytesDownloaded - returnedPayloadBytes
```

Entry index location handling:

- `OBJECT_FOOTER`: read index bytes from the same object unless `EntryIndexRef.objectId/objectKey` are
  both present；
- `INLINE`: reserved in Phase 1; reader fails with `UNSUPPORTED_FORMAT`；
- `INDEX_OBJECT`: reserved in Phase 1; reader fails with `UNSUPPORTED_FORMAT`。

## 11. Multi-Stream WAL Object Policy

Design supports multi-stream objects from day one:

```text
WalWriteRequest.slices = [streamA, streamB, streamC]
```

Visibility remains per slice:

```text
object upload succeeds
manifest state = UPLOADED
commit streamA slice succeeds -> streamA visible
commit streamB slice fails -> streamB invisible
commit streamC slice pending -> streamC invisible
```

Initial `DefaultStreamStorage` can start with one append work item per object if it keeps:

- `WalObjectWriter` support for multi-slice requests；
- manifest slice list；
- commit loop per slice；
- tests for partial visibility using a writer-created or manually constructed multi-slice object。

## 12. Manifest Construction

`WalWriteResult` maps to `ObjectManifestRecord`, but that mapping belongs in `nereus-core` or
`nereus-metadata-oxia`, not inside `nereus-object-store`. The object-store module must not depend on Oxia
metadata record classes.

Manifest fields derived from `WalWriteResult`:

```text
objectId
objectKey
objectType = MULTI_STREAM_WAL_OBJECT
state = UPLOADED
formatMajorVersion
formatMinorVersion
writerVersion
writerId
writerRunIdHash
writerEpoch
createdAtMillis
uploadedAtMillis
objectLength
objectChecksum
storageChecksum
slices
orphanExpiresAtMillis
```

Slice states:

```text
UPLOADED
VISIBLE
ABANDONED
```

Again, slice state is audit information. Offset index is visibility truth.

## 13. Checksum Rules

Required:

- WAL object-level CRC32C with the `objectChecksum` field zeroed；
- storage CRC32C over exact uploaded bytes；
- slice-level CRC32C；
- section-level CRC32C for footer and entry index。

Commit rules:

- WAL object checksum and storage checksum must be known before `putObjectManifest`；
- metadata commit references the WAL object checksum from manifest; storage checksum is used for
  object-store audit/head verification；
- read path verifies at least entry index checksum and range checksum when available。
- for clipped reads, Phase 1 verifies the full resolved slice checksum before returning clipped payload
  bytes. It must not claim a clipped subrange is checksum-verified by the full-slice checksum unless the
  full slice was actually read and checked。

Corruption behavior:

- checksum mismatch before stream-head commit: no reachable commit/index entry, fail append；
- checksum mismatch during read: fail read with non-retriable data integrity error and emit metric；
- future repair may mark object suspect, but Phase 1 only reports。

## 14. Local Filesystem Object Store for Tests

Add a test/local implementation:

```text
io.nereus.objectstore.testing.LocalFileObjectStore
```

Requirements:

- root path injected by test；
- object key is normalized under root and path traversal is rejected；
- reject absolute keys, empty path segments, `.`, `..`, platform separator escapes, and Windows drive
  prefixes before resolving the path；
- normal object keys generated by `WalObjectWriter` must not contain those dangerous segments because
  dynamic components are produced by `KeyComponentCodec.encodeComponent`；
- normalize the configured root once and verify every resolved target remains under that root. Existing
  parent components must not be followed through symlinks outside the root；
- `putObject(ifAbsent=true)` uses create-new semantics and fails if key exists；
- write object bytes to a temporary file in the same directory, fsync when practical, then atomic move to
  the final path；
- if object metadata is persisted by the local implementation, write it as a sidecar with the same
  create-new/atomic-move discipline. Object-store metadata is diagnostic only and must not become read
  visibility truth；
- a failed or cancelled write must not leave a final visible object, though temporary files may remain for
  test cleanup；
- range read follows object-store semantics and rejects negative offset/length or reads past EOF；
- checksum validated and returned using the same helper as WAL checksums；
- missing file maps to `OBJECT_NOT_FOUND`；
- permission/IO failures map to `OBJECT_UPLOAD_FAILED` or `OBJECT_READ_FAILED` depending on operation；
- no reliance on directory listing for correctness；
- expose a test-only cleanup helper such as `deleteAllForTesting()` or fixture-level `cleanupRoot()` that
  recursively clears only the injected root after tests. This helper must live in the testing package and
  must not become a production `ObjectStore.delete` contract。

Directory listing may be used only in tests to assert orphan files exist.
