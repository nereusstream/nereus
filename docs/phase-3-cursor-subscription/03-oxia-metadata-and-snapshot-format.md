# Oxia Metadata and Cursor Snapshot Format

## 1. Correctness Domains

F3 has exactly two kinds of durable coordination root：one `CursorStateRecord` per historical cursor-name key and one
`CursorRetentionRecord` per activated stream：

```text
CursorStateRecord(cursorNameHash)
  authority for one cursor generation, ack truth, properties and snapshot reference

CursorRetentionRecord(streamId)
  authority for conservative stream protection floor and in-flight protection/trim barriers
```

They are separate correctness domains and are not updated atomically。The state machines in document 04 order their
CAS operations so a crash between them is conservative：it may retain too much data or temporarily reject a reset，
but cannot trim data needed by an acknowledged successful cursor operation。

The rejected schema is：

```text
.../state
.../ack-ranges
.../properties
.../snapshot-ref
```

Splitting these fields would allow a broker crash or cross-broker race to publish a mark-delete without the matching
batch/range state or vice versa。No F3 implementation may introduce those keys as durable authorities。

## 2. Keyspace

`CursorKeyspace` belongs in `nereus-metadata-oxia` and composes the existing `OxiaKeyspace`/F2 projection prefix。

`CursorNames` is the sole exact-name/hash helper，parallel to F2 `ManagedLedgerProjectionNames`：

```java
public final class CursorNames {
    public static final int MAX_CURSOR_NAME_BYTES = 16 * 1024;
    public static String requireCursorName(String cursorName);
    public static String cursorNameHash(String cursorName);
}
```

It rejects null/blank/NUL、malformed UTF-16-to-UTF-8 and over-limit names before hashing。

`CursorIds.requireRandomId(value, fieldName)` is the sole validator for owner-session、protection、trim and snapshot
IDs；V1 requires exactly 32 lowercase hexadecimal characters representing 128 random bits。

```java
public final class CursorKeyspace {
    public CursorKeyspace(String cluster);

    public String cursorStateKey(StreamId streamId, String cursorName);
    public String cursorStateScanFrom(StreamId streamId);
    public String cursorStateScanToExclusive(StreamId streamId);
    public String retentionKey(StreamId streamId);
    public PartitionKey streamPartitionKey(StreamId streamId);
}
```

Exact paths：

```text
/nereus/clusters/{clusterComponent}/streams/{streamIdComponent}/facade/managed-ledger/cursors/v1/by-hash/{cursorNameHash}/state
/nereus/clusters/{clusterComponent}/streams/{streamIdComponent}/facade/managed-ledger/cursors/v1/retention
```

Hash formula：

```java
DeterministicIds.stableHashComponent("nereus-cursor-v1\0" + cursorName)
```

Input is strict UTF-8 and must satisfy the configured byte cap。The exact cursor name is also stored in the record；a
hash hit with a different exact name is corruption/collision and never aliases two subscriptions。

All cursor and retention keys use `OxiaKeyspace.streamPartitionKey(streamId)`。Range scans start at the literal
`.../by-hash/` prefix and end at its lexicographic successor；they are paged and reject any unexpected child shape。
No code constructs a path from raw cursor name。

### 2.1 Topic-projection activation / downgrade fence

Broker lookup capability is an admission signal，not durable evidence that an old F2 binary will inspect F3 keys。
Before the first absent/tombstone -> ACTIVE durable cursor for a topic name，F3 CASes this exact internal property into
the existing authoritative F2 `TopicProjectionRecord`：

```text
nereus.cursor-protocol = 1
```

Target helper and store method：

```java
public final class ManagedLedgerCursorProtocol {
    public static final String PROPERTY = "nereus.cursor-protocol";
    public static final String VERSION_1 = "1";

    public static boolean isActivated(TopicProjectionRecord record);
    public static Map<String, String> activate(Map<String, String> durableProperties);
    public static Map<String, String> canonicalDurableProperties(Map<String, String> durableProperties);
    public static Map<String, String> externalProperties(Map<String, String> durableProperties);
    public static Map<String, String> replaceExternalProperties(
            Map<String, String> currentDurableProperties,
            Map<String, String> requestedExternalProperties);
}

CompletableFuture<TopicProjectionRecord> activateCursorProtocol(
        String cluster,
        String managedLedgerName,
        ManagedLedgerProjectionIdentity expectedIdentity,
        long expectedMetadataVersion);
```

Rules：

- `ProjectionCreateRequest.initialProperties` and public ManagedLedger property APIs continue rejecting every
  caller-provided `nereus.*` key；only the internal activation method can add it；
- `ProjectionCreateRequest.canonicalProperties` remains the external canonicalizer and therefore preserves the
  locked F2 golden/rejection behavior；`TopicProjectionRecord` changes only to call
  `ManagedLedgerCursorProtocol.canonicalDurableProperties`，which permits exactly this reserved key/value；unknown
  value or any other internal key fails；
- `ProjectionMetadataStoreCore.updateProperties` canonicalizes the caller map externally and then calls
  `replaceExternalProperties` so a replacement cannot drop or inject the marker；
- every property/lifecycle update preserves the marker；external getters/filtering never expose it as a user
  managed-ledger property；
- once activated，delete/recreate preserves the marker on the topic projection key；recreate builds durable properties
  with `replaceExternalProperties(deleted.properties, request.initialProperties)` rather than `topicCandidate`'s
  external-only map，so it is never downgraded/removed；
- activation is one version-CAS after two stable all-broker capability snapshots and before the first
  PROTECTION_PENDING/cursor root creation；CAS success with later create failure is safe and resumed；
- an F3 cursor record with a missing activation marker is corruption；a retention record without the marker is valid
  only as the ACTIVE/no-pending owner-only preactivation root with no cursor records；
- every F3 writable ledger open creates or claims that owner-only root before publication，even while the marker is
  absent。This durable session fence does not activate cursor semantics and therefore does not bypass the marker's
  minimum-reader ordering；
- the locked F2 reader may ignore a marker-absent owner-only root safely because that shape has no cursor record、no
  pending transition and F2 Nereus trim remains a no-op；the moment cursor semantics may be written，the marker is
  already durable and makes that reader fail closed；
- the locked F2 decoder still calls `ProjectionCreateRequest.canonicalProperties` while constructing the decoded
  record，therefore it rejects the reserved key and fails topic open before exposing an empty cursor view；
- the marker-aware F3 decoder and the final writable cursor claim/hydration path are one deployable release boundary；
  M1/M2 repository milestones cannot be published into a broker runtime on their own，because accepting the marker
  while retaining the F2 empty local cursor facade would defeat the downgrade fence；
- changing the locked F2 decoder/property validation invalidates this downgrade proof and reopens M0R。

The marker does not change stream/projection/Position identity and is not a cursor correctness field。It is a monotonic
minimum-reader fence for the topic projection。

`activateCursorProtocol` requires the exact projection identity、metadata version and a non-DELETING/non-DELETED
facade state。Already-activated state returns idempotently only after identity validation；a condition loss reloads the
projection and retries within the metadata deadline。If activation makes the F2 metadata value exceed its existing
hard cap，first create fails before any retention/cursor write。Activation preserves projection identity、facade
state/stateVersion、created time and all external properties；only durable properties and Oxia metadata version change。

## 3. Metadata Store API

Planned files：

```text
nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/
  CursorMetadataStore.java
  OxiaJavaCursorMetadataStore.java
  CursorMetadataStoreCore.java
  CursorKeyspace.java
  CursorNames.java
  CursorIds.java
  CursorMetadataStoreConfig.java
  ManagedLedgerCursorProtocol.java
  CursorScanPage.java
  CursorScanToken.java
  VersionedCursorState.java
  VersionedCursorRetention.java
  CursorMetadataConditionFailedException.java
  records/CursorRecordLifecycle.java
  records/CursorRetentionLifecycle.java
  records/CursorProtectionKind.java
  records/CursorStateRecord.java
  records/CursorSnapshotReferenceRecord.java
  records/CursorAckRangeRecord.java
  records/CursorPartialBatchAckRecord.java
  records/CursorProtectionIntentRecord.java
  records/CursorRetentionRecord.java
  codec/F3MetadataCodecs.java
  codec/CursorStateRecordCodecV1.java
  codec/CursorRetentionRecordCodecV1.java
```

These are explicit codecs under the existing `NRM1` envelope。They do not delegate to the Phase 1 reflection codec：
F3 has optional fields、`Map<String,Long>`、stable enum IDs and stricter canonical range/word rules that the generic
record codec does not encode。Registry selection is by exact record type；decoder probing is forbidden。

Interface：

```java
public interface CursorMetadataStore extends AutoCloseable {

    static CursorMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            CursorMetadataStoreConfig storeConfig);

    CompletableFuture<Optional<VersionedCursorState>> getCursor(
            String cluster,
            StreamId streamId,
            String cursorName);

    CompletableFuture<VersionedCursorState> createCursor(
            String cluster,
            CursorStateRecord value);

    CompletableFuture<VersionedCursorState> compareAndSetCursor(
            String cluster,
            CursorStateRecord value,
            long expectedMetadataVersion);

    CompletableFuture<CursorScanPage> scanCursors(
            String cluster,
            StreamId streamId,
            Optional<CursorScanToken> continuation,
            int pageSize);

    CompletableFuture<Optional<VersionedCursorRetention>> getRetention(
            String cluster,
            StreamId streamId);

    CompletableFuture<VersionedCursorRetention> createRetention(
            String cluster,
            CursorRetentionRecord value);

    CompletableFuture<VersionedCursorRetention> compareAndSetRetention(
            String cluster,
            CursorRetentionRecord value,
            long expectedMetadataVersion);

    WatchRegistration watchStreamCursors(
            String cluster,
            StreamId streamId,
            Runnable invalidation);

    @Override
    void close();
}
```

Returned value carriers are：

```java
public record VersionedCursorState(
        CursorStateRecord value,
        long metadataVersion) {}

public record VersionedCursorRetention(
        CursorRetentionRecord value,
        long metadataVersion) {}

public record CursorScanPage(
        List<VersionedCursorState> records,
        Optional<CursorScanToken> continuation) {}
```

They require nonnegative Oxia version and immutable defensive copies。`CursorScanToken` is an opaque immutable class
created only by `CursorMetadataStoreCore`；it binds cluster、stream ID、scan prefix and exclusive last key。A token from
another store scope or a token whose key is not an exact cursor-state child fails before IO。

`Versioned*` records contain decoded immutable value and Oxia metadata version。`create*` means absent-only put；
`compareAndSet*` means exact-version replace。Condition failures use one typed exception and do not masquerade as IO
failure。Fake and real stores must use the same codec and `CursorMetadataStoreCore` validation。Like F2，the adapter
uses a caller-owned `SharedOxiaClientRuntime`；closing the cursor store drains its own admitted operations/watch
registrations but never closes that shared runtime。Every method validates the cluster and constructs one
cluster-scoped `CursorKeyspace`，so a stream ID cannot address another cluster's prefix accidentally。

```java
public record CursorMetadataStoreConfig(
        Duration operationTimeout,
        int maxPendingOperations,
        int maxValueBytes,
        int maxScanPageSize) {

    public static final int F3_MAX_VALUE_BYTES = 64 * 1024;

    public static CursorMetadataStoreConfig defaults() {
        return new CursorMetadataStoreConfig(
                Duration.ofSeconds(30), 1_024, F3_MAX_VALUE_BYTES, 256);
    }
}
```

The constructor requires a positive timeout/count/page size and `maxValueBytes == F3_MAX_VALUE_BYTES`。
`NereusRuntimeConfiguration` performs the cross-record check
`cursorMetadata.maxScanPageSize <= cursorStorage.cursorRecordsPerStreamMax` because the metadata module does not
depend on the managed-ledger configuration type。Encoded envelope bytes，not only payload bytes，must fit
`maxValueBytes` before any put/CAS。

Scan contract：

- stable lexical order by cursor-name hash；
- `1 <= pageSize <= configured max`；
- a non-final page contains exactly `pageSize` records；an empty page is final and has no continuation；
- continuation token is opaque, bound to stream/prefix and cannot move backward；
- duplicate keys across pages are rejected by the hydration coordinator；
- scan is not a transaction snapshot。The open coordinator revalidates each hydrated root and retries the full scan
  if a watch/version change can affect the result；bounded retry exhaustion fails topic open。

`watchStreamCursors` watches the complete `.../cursors/v1/` prefix，including retention and by-hash roots，and returns
the existing closeable `WatchRegistration`。Its `Runnable` may be coalesced and carries no value/version。Watch is
cache invalidation only. Every mutation reads/CASes authoritative state even if no watch event arrives。

## 4. CursorStateRecord V1

`F3MetadataCodecs` registers envelope record type `CursorStateRecord`（the exact Java simple name），schema version
`1`，min reader `1` and `MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1` under existing `NRM1` rules。
`MetadataRecordCodecFactory` adds F3 as its fourth explicit registry fallback after Phase 1、L0-target and F2；it does
not probe decoders。The existing factory check `recordType == recordClass.getSimpleName()` remains unchanged。

Logical fields in canonical order：

```java
public record CursorStateRecord(
        long metadataVersion,                        // encoded value must be 0
        ManagedLedgerProjectionIdentity projection,
        String ownerSessionId,
        String cursorName,
        String cursorNameHash,
        long cursorGeneration,
        CursorRecordLifecycle lifecycle,
        long mutationSequence,
        long ackStateEpoch,
        String lastProtectionAttemptId,
        long markDeleteOffset,
        Optional<CursorSnapshotReferenceRecord> snapshotReference,
        List<CursorAckRangeRecord> inlineWholeAckDeltas,
        List<CursorPartialBatchAckRecord> inlinePartialAckOverrides,
        Map<String, Long> positionProperties,
        Map<String, String> cursorProperties,
        long createdAtMillis,
        long updatedAtMillis,
        OptionalLong deletedAtMillis) {
}
```

The record embeds the existing `ManagedLedgerProjectionIdentity` fields：

```text
storageClassBindingGeneration
incarnation
streamId
virtualLedgerId
```

The exact managed-ledger name/hash remains in the authoritative F2 topic projection and the open-time
`CursorLedgerIdentity` context；it is not duplicated into every cursor root。The decoded projection must exactly equal
that context。A matching stream ID with a different binding generation、incarnation or virtual ledger is corruption。

Absent create starts `mutationSequence=1, ackStateEpoch=1`。Every successful cursor-root replacement writes exactly
`previous.mutationSequence+1`；an `ALREADY_APPLIED` read with no CAS does not increment it。Within one generation，
durable reset and clear-backlog write `previous.ackStateEpoch+1`；all other replacements preserve the prior epoch。
Recreate starts the new generation at epoch `1`。A writable-open owner claim changes `ownerSessionId` and increments
`mutationSequence`/updated time while preserving every semantic cursor field and `ackStateEpoch`。

`createdAtMillis` is the creation time of the current cursor generation，not the historical key。Initial create sets
it from the operation's nonnegative `nowMillis`；delete preserves it；recreate sets it to the new operation time while
continuing the key-level `mutationSequence` and sets `updatedAtMillis=max(previous.updatedAtMillis, nowMillis)`。
Owner claim and every same-generation mutation preserve `createdAtMillis`。

### 4.1 ACTIVE invariants

- `cursorGeneration >= 1`；`mutationSequence >= 1`；`ackStateEpoch >= 1`；
- `ownerSessionId` has the exact V1 32-lowercase-hex shape and equals the claiming writable ledger session；
- `lastProtectionAttemptId` is a canonical 32-lowercase-hex 128-bit ID from create/recreate or the latest backward
  reset protection intent；ordinary ack/property/forward-reset mutations preserve it；
- `cursorNameHash` equals the locked hash formula and key suffix；
- `createdAtMillis >= 0` and `updatedAtMillis >= createdAtMillis`；
- `trimOffset <= markDeleteOffset <= committedEndOffset` is revalidated against L0 during hydration/mutation；
- when snapshot ref is absent, inline ranges/partials are the complete effective state；
- when snapshot ref is present, inline ranges are whole-ack union deltas and partial records override the same offset
  in snapshot；whole range always wins；
- inline lists are canonical, sorted and unique；
- properties are canonical sorted maps in encoded bytes；
- `deletedAtMillis` is empty。

### 4.2 DELETED tombstone invariants

- projection、owner-session ID、exact cursor name/hash、generation、last mutation sequence、ack-state epoch、protection attempt and
  timestamps remain；
- snapshot ref, inline ranges, partials, position properties and cursor properties are empty；
- `deletedAtMillis` is present and `>= createdAtMillis`；
- `updatedAtMillis >= createdAtMillis` and `deletedAtMillis >= updatedAtMillis`；
- `markDeleteOffset` retains the final validated value for diagnostics only；it is not a retention reference；
- tombstone is not removed while the stream incarnation exists。

Reopening the same name CASes this same key from DELETED to ACTIVE with `generation + 1`。A stale handle always includes
the old generation in its candidate bytes and therefore cannot mutate the recreated cursor even if its cached Oxia
version were accidentally refreshed。

### 4.3 Binary payload

Numeric primitives are big-endian。Offsets、counts、lengths、generations、sequences and timestamps receive their stated
non-negative/positive validation；a position-property value preserves the complete signed Java `long` domain，and a
`remainingWord` preserves its complete 64-bit bit pattern after the exact final-word mask（so it may be a negative
Java `long`）。Strings use strict UTF-8 with unsigned 32-bit byte length。Optional is encoded as one byte `0/1`。Enums
use explicit stable numeric IDs, never Java ordinal。

Payload field sequence：

```text
u16 payloadVersion = 1
i64 metadataVersion = 0

i64 storageClassBindingGeneration
i64 incarnation
string streamId
i64 virtualLedgerId
string ownerSessionId
string cursorName
string cursorNameHash
i64 cursorGeneration
u8  lifecycleId                 // 1 ACTIVE, 2 DELETED
i64 mutationSequence
i64 ackStateEpoch
string lastProtectionAttemptId
i64 markDeleteOffset

u8  snapshotPresent
[when present:]
  string objectKey
  string snapshotId
  i64 cursorGeneration
  i64 sourceMutationSequence
  i64 baseMarkDeleteOffset
  i64 objectLength
  string storageChecksumType
  string storageChecksumValue
  i32 formatCrc32c
  i32 formatVersion
  i64 createdAtMillis

u32 wholeRangeCount
repeat: i64 startOffset, i64 endOffset

u32 partialCount
repeat:
  i64 entryOffset
  i32 batchSize
  u32 wordCount
  repeat i64 remainingWord

u32 positionPropertyCount
repeat sorted by unsigned UTF-8 key bytes: string key, i64 value

u32 cursorPropertyCount
repeat sorted by unsigned UTF-8 key bytes: string key, string value

i64 createdAtMillis
i64 updatedAtMillis
u8  deletedAtPresent
[i64 deletedAtMillis]
```

Decoder rejects：

- negative/overflowing count or length；
- allocation above configured record cap before allocation；
- invalid UTF-8, duplicate/noncanonical map keys, duplicate partial offsets；
- unknown enum/flag, unsupported schema version or nonzero metadataVersion；
- noncanonical range/word ordering；
- trailing bytes or envelope CRC mismatch。

Encode(decode(bytes)) must reproduce identical bytes for every canonical golden fixture。

## 5. Snapshot Reference V1

Embedded reference fields：

```java
public record CursorSnapshotReferenceRecord(
        String objectKey,
        String snapshotId,
        long cursorGeneration,
        long sourceMutationSequence,
        long baseMarkDeleteOffset,
        long objectLength,
        String storageChecksumType,
        String storageChecksumValue,
        int formatCrc32c,
        int formatVersion,
        long createdAtMillis) {
}
```

Invariants：

- `formatVersion == 1`；generation equals parent root generation；
- source sequence `<=` root mutation sequence and `>= 1`；
- base mark-delete `<=` current root mark-delete；
- `0 < objectLength <= cursorSnapshotMaxBytes`；
- object key exactly equals the deterministic prefix plus recorded random snapshot ID；
- snapshot ID is 32 lowercase hex characters from 128 cryptographically random bits；
- `storageChecksumType == CRC32C` and lowercase 8-hex `storageChecksumValue` cover the complete stored object bytes，
  exactly matching `PutObjectOptions.expectedChecksum` / PUT / HEAD；
- `formatCrc32c` covers the format bytes except its own final field as specified below and equals the footer field；
  root value is the lookup/reference authority。

## 6. Cursor Snapshot Object V1

### 6.1 Logical key

Before the provider-specific `S3ObjectKeyMapper`, logical `ObjectKey.value()` is：

```text
{clusterComponent}/cursor-snapshots/v1/
  {streamIdComponent}/
  {cursorNameHash}/
  {cursorGeneration019}/
  {snapshotId}.ncs
```

Rendered as one line：

```text
{clusterComponent}/cursor-snapshots/v1/{streamIdComponent}/{cursorNameHash}/{cursorGeneration019}/{snapshotId}.ncs
```

Components use `KeyComponentCodec`; generation uses `encodeNonNegativeLong`。The exact cursor name never appears in
the key。Writes use `ObjectType.CURSOR_SNAPSHOT_OBJECT` and `PutObjectOptions.ifAbsent=true`。A collision/precondition
failure creates a fresh random snapshot ID；bytes are never overwritten。

Exact generic ObjectStore options，where `objectCallTimeout` is recomputed immediately before each call as
`min(snapshotDeadline.remaining(), configuredObjectStoreRequestTimeout)`：

```java
new PutObjectOptions(
    "application/vnd.nereus.cursor-snapshot-v1",
    fullStoredBytesCrc32c,
    true,
    Map.of(
        "nereus-format", "NCS1",
        "nereus-object-type", "CURSOR_SNAPSHOT_OBJECT",
        "nereus-snapshot-id", snapshotId),
    objectCallTimeout);

new HeadObjectOptions(objectCallTimeout);

new RangeReadOptions(
    Optional.of(fullStoredBytesCrc32c),
    objectCallTimeout);
```

After PUT，the store requires `PutObjectResult` key、length and checksum to equal the request，then HEAD requires the
same key/length/checksum and the exact three metadata entries。Read first HEAD-validates those values and the root
reference，then calls
`readRange(key, 0, objectLength, new RangeReadOptions(Optional.of(checksum), objectCallTimeout))` exactly once。The returned
`RangeReadResult` must repeat the exact key、offset `0`、length、remaining payload size and present checksum；a mismatch
fails。PUT+HEAD and HEAD+read share their respective one captured snapshot deadline；they are not each granted a fresh
60 seconds。The 64-MiB cap guarantees the full read fits one Java `ByteBuffer`。No S3 SDK type enters the cursor package。

### 6.2 Binary layout

The object is big-endian and contains a full normalized ack-state base only；properties are never placed in object
storage。`ownerSessionId` is intentionally absent：owner claim fences root CAS，while immutable snapshot bytes remain
valid across broker ownership changes and are visible only through the newly claimed root reference。

```text
Header
  4 bytes magic = ASCII "NCS1"
  u16 majorVersion = 1
  u16 minorVersion = 0
  u16 objectTypeCode = 4              // stable F3 code for CURSOR_SNAPSHOT_OBJECT
  u16 flags = 0
  u32 headerLengthBytes
  u64 totalObjectLengthBytes
  16 bytes snapshotId raw
  string streamId
  string managedLedgerNameHash
  string cursorNameHash
  i64 storageClassBindingGeneration
  i64 incarnation
  i64 virtualLedgerId
  i64 cursorGeneration
  i64 sourceMutationSequence
  i64 baseMarkDeleteOffset
  i64 createdAtMillis
  u32 wholeRangeCount
  u32 partialEntryCount
  u32 headerCrc32c

WholeAckRangeSection
  4 bytes magic = ASCII "NCR1"
  u64 sectionLengthBytes
  repeat wholeRangeCount:
    i64 startOffset
    i64 endOffset
  u32 sectionCrc32c

PartialBatchSection
  4 bytes magic = ASCII "NCB1"
  u64 sectionLengthBytes
  repeat partialEntryCount:
    i64 entryOffset
    i32 batchSize
    u32 wordCount
    repeat wordCount:
      i64 remainingWord
  u32 sectionCrc32c

Footer
  4 bytes magic = ASCII "NCF1"
  u16 majorVersion = 1
  u16 flags = 0
  i64 minReferencedOffset
  i64 maxReferencedOffsetExclusive
  u64 totalObjectLengthBytes
  u32 formatCrc32c
```

`string` is `u32 byteLength + strict UTF-8 bytes`。Header CRC covers Header from `NCS1` through
`partialEntryCount`, excluding its own field。Each section CRC covers section magic, section length and payload，
excluding its own field。`formatCrc32c` covers every byte from `NCS1` through footer's
`totalObjectLengthBytes`, excluding the final checksum field。Reference `formatCrc32c` equals this value。After the
footer is complete，the writer computes a second CRC32C over **all** stored bytes；that full-object checksum is used by
the generic ObjectStore PUT/HEAD API and stored in the root reference as type/value。

`headerLengthBytes` ends immediately after header CRC；section length includes magic、length、payload and section CRC。
`totalObjectLengthBytes` includes the final checksum。Every encoded length must equal the actual consumed bytes。

### 6.3 Snapshot canonical state

- ranges obey the document-02 normalized order and are above/equal base mark-delete；
- partial entries are ascending and not covered by ranges；
- remaining words use canonical `BitSet.toLongArray()` length and are neither empty nor the exact masked all-ones
  state for their batch size；
- counts exactly match entries；
- `minReferencedOffset` is the smallest base/range/partial relevant offset, or base mark-delete when no holes；
- `maxReferencedOffsetExclusive` is the maximum range end / partial+1 / base mark-delete；
- empty state is legal but the planner does not create a snapshot for it。

### 6.4 Strict reader

`CursorSnapshotCodecV1.decode(ByteBuffer, CursorSnapshotReference, CursorIdentity, CursorStorageConfig)`：

1. checks length cap before allocating；
2. validates all magic/version/type/flags and exact expected object length；
3. validates every CRC before publishing decoded state；
4. compares snapshot ID、stream ID、projection hashes/generations、cursor hash/generation、source sequence、base
   mark-delete and root reference；
5. bounds count times minimum element size before list allocation；
6. validates canonical order while streaming；
7. recomputes `minReferencedOffset/maxReferencedOffsetExclusive` and requires exact footer equality；
8. rejects extra/trailing bytes；
9. returns immutable state only after full footer validation。

Unknown major/type/flag fails closed。A future minor may be accepted only if current reader explicitly understands
every set flag；V1 accepts only minor 0 and flags 0。

## 7. Effective Ack-state Hydration

For root `R`：

```text
if R.snapshotReference is absent:
    base.markDelete = R.markDeleteOffset
    base.ranges = empty
    base.partials = empty
else:
    S = strictly read referenced full snapshot
    require S.baseMarkDelete <= R.markDeleteOffset
    base = S

effective.markDelete = R.markDeleteOffset
effective.ranges = union(
    snapshot ranges clipped to >= R.markDeleteOffset,
    R.inlineWholeAckDeltas)
effective.partials = snapshot partials clipped to >= R.markDeleteOffset
for override in R.inlinePartialAckOverrides:
    effective.partials[override.offset] = override
remove partials covered by effective.ranges
normalize/fold ranges beginning at effective.markDelete
```

An inline partial record is a full replacement for the same snapshot offset, not an AND delta；the state machine
computes the already-merged remaining words before encoding it。Whole ranges win over both snapshot and inline partial
state。

If hydration normalization changes the semantic state beyond clipping entries below the newer root mark-delete, the
  record/object pair is noncanonical corruption。Reader does not silently repair bytes while opening a topic。

## 8. Snapshot Publish Protocol

Planner selects a replacement snapshot when encoded inline ack fields would exceed `cursorInlineAckMaxBytes`，when
canonical root bytes would cross `cursorMetadataValueMaxBytes - cursorMetadataSafetyMarginBytes`，or when combined
inline range/partial count would exceed `cursorInlineDeltaMaxCount`。

```text
1. Read root R at Oxia version V and hydrate effective state E.
2. Apply requested mutation -> E'.
3. Encode full snapshot S(E') with random snapshotId and
   sourceMutationSequence=Math.addExact(R.mutationSequence,1).
4. Compute the full stored-byte CRC32C and PUT object ifAbsent=true with that `Checksum` plus length/type metadata.
5. HEAD the object; require exact length, full storage checksum/type metadata and key.
6. Build root R' at sequence+1:
     markDelete = E'.markDelete
     snapshotRef = S reference
     inline ranges = empty
     inline partial overrides = empty
     properties = mutation result
7. CAS R' against V.
8. Only CAS success makes S visible and allows cursor callback success.
```

If PUT succeeds and HEAD/CAS fails, the object is an orphan candidate。F3 records metrics/log context but does not
delete it because current `ObjectStore` has no delete and F4 owns physical GC。A CAS retry reloads current root and
creates a new snapshot ID if another snapshot is still needed；it never points to bytes encoded from stale state。

Snapshot publication is not a second callback boundary。The client observes one cursor mutation completed only after
step 7。

## 9. Snapshot Read and Root Revalidation

Open/hydration algorithm：

```text
loop until deadline:
  R1,V1 = get root
  if no ref: return decode-inline(R1)

  HEAD/READ exact referenced object with strict cap
  if object read/head/decode fails:
      R2,V2 = get root
      if generation/ref/version changed: retry
      else: fail corruption/open

  R2,V2 = get root
  if V2 != V1 or generation/ref changed: retry
  return hydrate(R1, snapshot)
```

Any root version change, including property-only update, causes retry so the returned handle is one coherent root
version。There is no fallback to an older snapshot or inline-only state when the referenced object is missing/corrupt。

This retry-on-ref-change makes the F3 cursor object read safe without a long-lived read pin；F4 may later add common
object reference epochs, but cannot weaken this open rule。

## 10. CursorRetentionRecord V1

Envelope record type `CursorRetentionRecord`（exact Java simple name），schema `1`，min reader `1`，existing binary
V1 payload-encoding constant。

```java
public record CursorRetentionRecord(
        long metadataVersion,                 // encoded 0
        ManagedLedgerProjectionIdentity projection,
        String ownerSessionId,
        CursorRetentionLifecycle lifecycle,
        long mutationSequence,
        long protectedFloorOffset,
        long lastCompletedTrimOffset,
        Optional<CursorProtectionIntentRecord> pendingProtectionIntent,
        Optional<String> pendingTrimAttemptId,
        OptionalLong pendingTrimOffset,
        Optional<String> pendingTrimReason,
        long updatedAtMillis) {
}

public enum CursorRetentionLifecycle {
    ACTIVE,
    PROTECTION_PENDING,
    TRIM_PENDING
}

public record CursorProtectionIntentRecord(
        String attemptId,
        CursorProtectionKind kind,
        String cursorName,
        String cursorNameHash,
        long expectedCursorGeneration,
        long targetCursorGeneration,
        long targetMarkDeleteOffset,
        Optional<CursorPartialBatchAckRecord> targetPartialBatch,
        Map<String, Long> initialPositionProperties,
        Map<String, String> initialCursorProperties,
        long createdAtMillis) {
}

public enum CursorProtectionKind {
    CREATE,
    RECREATE,
    BACKWARD_RESET
}
```

ACTIVE invariants：

```text
ownerSessionId is exact 32-lowercase-hex V1 ID
0 <= lastCompletedTrimOffset <= protectedFloorOffset <= committedEndOffset
all pending fields absent
runtime hydration requires L0 trimOffset == lastCompletedTrimOffset
```

PROTECTION_PENDING invariants：

```text
pendingProtectionIntent present；all trim-pending fields absent
lastCompletedTrimOffset <= protectedFloorOffset
lastCompletedTrimOffset <= intent.targetMarkDeleteOffset
protectedFloorOffset <= intent.targetMarkDeleteOffset
runtime L0 trimOffset == lastCompletedTrimOffset

CREATE:         expectedGeneration=0, targetGeneration=1, target partial absent
RECREATE:       expectedGeneration>=1, targetGeneration=expected+1, target partial absent
BACKWARD_RESET: expectedGeneration=targetGeneration>=1,
                initial property maps empty
target partial absent or its entryOffset == targetMarkDeleteOffset
```

The intent contains enough immutable input to finish after process loss。Its encoded bytes are capped by
`cursorProtectionIntentMaxBytes` and the complete retention value remains under the metadata hard cap；an oversize
create/backward reset fails before the pending CAS。The intent is not a second cursor truth：it only authorizes and
recovers one root transition。
`attemptId` uses the same 128-random-bit lowercase-hex shape as snapshot ID，`createdAtMillis >= 0`，and exact
name/hash/projection/generation are revalidated before every recovery CAS。

TRIM_PENDING invariants：

```text
pendingTrimAttemptId present and same 32-lowercase-hex 128-random-bit shape as protection/snapshot IDs
pendingTrimReason present，strict UTF-8/nonblank/no-NUL，bounded by `cursorTrimReasonMaxUtf8Bytes`，and exactly starts
with `nereus-cursor-retention/{pendingTrimAttemptId}:`
lastCompletedTrimOffset <= pendingTrimOffset <= protectedFloorOffset
protectedFloorOffset == pendingTrimOffset for the frozen trim candidate
runtime L0 trimOffset is either lastCompletedTrimOffset or exactly pendingTrimOffset
```

All three lifecycles carry one valid `ownerSessionId`。Only the open-time claim protocol may replace it；every ordinary
retention/cursor candidate must match and preserve the supplied owner session。Claiming a pending root does not clear
or rewrite its intent，and recovery continues it under the new owner。

While `PROTECTION_PENDING`，floor raise、trim and another create/backward reset are blocked。Mutations on the targeted
cursor either carry the same attempt or fail/retry busy；an already-in-flight monotonic ack may win one cursor CAS，and
the durable protection intent then reapplies the reset against the latest same-generation root。While
`TRIM_PENDING`，every new cursor create/recreate and every backward reset fails busy；forward ack/delete/reset may
continue when they cannot lower protection。Recovery never clears either pending state based only on timeout。

The root is initialized by the first F3 writable ledger open，before that ledger is published。When the projection
marker is absent，the only legal shape is lifecycle ACTIVE、no pending fields and no cursor records；the root carries
the current owner session and conservative trim/floor state but does not activate cursor semantics。First durable
create publishes the marker before changing this root to PROTECTION_PENDING。Marker-present + no cursor/root can occur
on a newly recreated F2 topic incarnation that inherited the monotonic managed-ledger-name marker but has a fresh
stream ID；the next F3 writable open absent-creates the root at the exact current L0 trim。Any cursor record with no
root or without the marker fails closed；marker-absent root in a pending lifecycle is corruption。

Retention payload ordering follows CursorState primitive rules。The record does not contain cursor-name lists or
snapshot references；those stay in cursor roots。

Exact payload：

```text
u16 payloadVersion = 1
i64 metadataVersion = 0
i64 storageClassBindingGeneration
i64 incarnation
string streamId
i64 virtualLedgerId
string ownerSessionId
u8 lifecycleId                    // 1 ACTIVE, 2 PROTECTION_PENDING, 3 TRIM_PENDING
i64 mutationSequence
i64 protectedFloorOffset
i64 lastCompletedTrimOffset
when lifecycle == PROTECTION_PENDING:
  string protectionAttemptId
  u8 protectionKindId             // 1 CREATE, 2 RECREATE, 3 BACKWARD_RESET
  string cursorName
  string cursorNameHash
  i64 expectedCursorGeneration
  i64 targetCursorGeneration
  i64 targetMarkDeleteOffset
  u8 targetPartialPresent
  [when present: partial batch payload in CursorState V1 form]
  u32 initialPositionPropertyCount
  repeat sorted key/value payload in CursorState V1 form
  u32 initialCursorPropertyCount
  repeat sorted key/value payload in CursorState V1 form
  i64 intentCreatedAtMillis
when lifecycle == TRIM_PENDING:
  string pendingTrimAttemptId
  i64 pendingTrimOffset
  string pendingTrimReason
i64 updatedAtMillis
```

Lifecycle determines the exact branch，so ACTIVE has no pending payload and the two pending kinds cannot coexist。
Decoder requires `mutationSequence >= 1`、`updatedAtMillis >= 0` and the lifecycle-specific numeric relations above，
then runtime hydration revalidates projection and L0 bounds。

Retention absent-create starts sequence 1；every ACTIVE -> pending、pending -> ACTIVE、floor raise、owner-session claim
or other successful replacement increments by exactly one。An owner claim changes only owner ID、sequence and updated
time while preserving lifecycle/pending bytes/floors。Recovery preserves the pending attempt ID and exact composed
trim reason while retrying and never reuses a sequence from a lost CAS。

## 11. Retention Floor Reconciliation

`protectedFloorOffset` is deliberately conservative。It may be lower than the true minimum active mark-delete；it
must never be higher than an offset newly promised to a successful create/backward reset。

Raising floor：

1. read retention root at V；require ACTIVE（both pending lifecycles block reconciliation）；
2. bounded-scan all cursor roots and strictly hydrate ACTIVE records；
3. compute `candidate = min(active.markDeleteOffset)` or committed end if none；
4. reread retention root and require the same V；forward ack/delete changes seen during the scan can only make the
   scanned minimum more conservative and need no per-cursor version lock；
5. CAS retention floor upward only against unchanged V；
6. every concurrent create/backward operation first CASes the root from ACTIVE to PROTECTION_PENDING，even if the
   numeric floor stays unchanged；if it wins before step 5，the ACTIVE/version check loses，and if step 5 wins first the
   protection operation reloads the raised root before freezing its target；
7. once PROTECTION_PENDING exists，no new raise may begin until the matching cursor attempt is proven and the root is
   finalized back to ACTIVE。

The implementation may use watch epochs/version fingerprints to avoid rereading unchanged snapshot bytes, but they
are optimizations only。No one-shot `min()` scan authorizes trim by itself。

## 12. Tombstone and Object Lifecycle

Cursor tombstones remain until the entire stream incarnation is logically deleted。This bounds stale-writer safety at
the cost of one small key per historically used cursor name；`cursorRecordsPerStreamMax` controls admission。

Object lifecycle：

| Object | Visible/protected when | Eligible orphan/unreferenced when | Physical delete owner |
| --- | --- | --- | --- |
| current cursor snapshot | referenced by ACTIVE root | root generation/ref changes or cursor deleted | F4 |
| CAS-lost uploaded snapshot | never visible | immediately after failed CAS | F4 orphan scanner |
| snapshot of DELETED cursor | never referenced after delete CAS | delete CAS succeeds | F4 |
| old stream-incarnation snapshots | old stream is deleted | stream deletion/reference grace satisfied | F4 |

F3 must expose snapshot key/generation/ref metrics or scan data needed by F4。F4 must reread authoritative cursor root
before deletion and honor its own reader/reference grace；it cannot infer liveness from object age alone。

## 13. Codec and Store Test Contract

M1 must include：

- canonical golden bytes for minimal ACTIVE、ACTIVE with every field、DELETED、ACTIVE with snapshot ref/deltas and
  all three retention lifecycles；
- protection-intent goldens for CREATE、RECREATE、BACKWARD_RESET with/without target partial and exact cap failure；
- `ackStateEpoch` starts at one，is preserved/incremented by the exact mutation classes and fails on checked overflow；
- round-trip and encode-after-decode equality；
- one mutation/fuzz test for every length/count/enum/CRC/trailing-byte failure；
- name-hash collision mismatch and key/record identity mismatch；
- fake and real Oxia absent-create、exact CAS、CAS loss、page scan、watch-disabled polling paths；
- snapshot key golden values, ifAbsent collision, exact HEAD metadata, short/long/range/trailing bytes and all CRCs；
- object upload success + CAS loss orphan behavior；
- root-ref change during read retry and stable missing/corrupt ref fail-safe；
- root value and snapshot cap boundary tests at limit-1/limit/limit+1。

No test may accept noncanonical bytes and rewrite them silently。
