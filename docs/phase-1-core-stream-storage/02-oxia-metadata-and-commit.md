# 02 Oxia Metadata and Commit Design

本文把 Phase 1 需要的 Oxia metadata schema、record 类型和 commit 操作拆到代码级。

M0.5 status: the selected public Oxia Java client API does not expose the multi-key conditional write
primitive assumed by the original `commitStreamSlice` protocol below. Treat sections that describe one
atomic stream commit batch as pre-redesign design input, not as implementation-ready contract. M2 must
replace the append linearization protocol before fake metadata or real adapter work implements
`commitStreamSlice`.

## 1. Metadata Responsibility

Oxia metadata layer owns:

- stream create-or-get；
- stream state；
- append session epoch/token；
- committed end offset；
- offset index；
- object manifest；
- object references；
- trim low-watermark；
- metadata watch/notification boundary。

It does not own:

- payload bytes；
- object range reads；
- Pulsar cursor state；
- Kafka group offsets；
- lakehouse catalog snapshots。

## 2. Keyspace

Use one cluster-scoped key prefix:

```text
/nereus/clusters/{clusterComponent}
```

Phase 1 keys:

```text
/nereus/clusters/{clusterComponent}/streams/by-name/{streamNameHash}
/nereus/clusters/{clusterComponent}/streams/{streamId}/meta
/nereus/clusters/{clusterComponent}/streams/{streamId}/append-session
/nereus/clusters/{clusterComponent}/streams/{streamId}/committed-end-offset
/nereus/clusters/{clusterComponent}/streams/{streamId}/offset-index/{offsetEnd}/{generation}
/nereus/clusters/{clusterComponent}/streams/{streamId}/committed-slices/{objectIdComponent}/{sliceIdComponent}
/nereus/clusters/{clusterComponent}/streams/{streamId}/trim

/nereus/clusters/{clusterComponent}/objects/{objectIdComponent}/manifest
/nereus/clusters/{clusterComponent}/objects/{objectIdComponent}/references
/nereus/clusters/{clusterComponent}/objects/{objectIdComponent}/gc
```

Notes:

- `streamNameHash` is for stable idempotent lookup. The value must contain the original `StreamName`
  to detect hash collisions.
- Offset index key includes `{generation}` from Phase 1. Append WAL entries use `generation=0`.
- `metadataVersion` is the Oxia key version. `commitVersion` is the durable Nereus stream commit
  sequence stored in `CommittedEndOffsetRecord` and copied into each visible slice commit. Neither is
  compaction generation.
- All stream-scoped keys participating in `commitStreamSlice` must use `PartitionKey(streamId)` or an
  equivalent Oxia key-group routing rule so the visible append commit is single-key-group atomic.
  M0.5 confirmed that the selected public Oxia Java client API does not expose the required atomic
  conditional multi-write primitive. This keyspace can stay, but the original `commitStreamSlice`
  linearization design must be replaced before M2 fake metadata semantics are frozen or M4 core append
  starts.
- Stream creation also uses `PartitionKey(streamId)`. Phase 1 derives `streamId` deterministically from
  the exact `StreamName.value()`, so the by-name lookup and the initial stream records can be committed
  in the same key group.
- Object-scoped keys may use `PartitionKey(objectId)`. They are not part of the producer-ack
  linearization point.
- Dynamic path components must be key-safe encoded before entering an Oxia path. In particular,
  `sliceId` may contain `/` in object-local metadata, so `committed-slices` uses encoded
  `{sliceIdComponent}` rather than the raw `sliceId`.

Path component encoding:

```text
clusterComponent = keyComponent(cluster)
streamNameHash = base32lower_nopad(sha256(UTF-8 exact StreamName.value()))
streamId = "s-" + streamNameHash
objectIdComponent = keyComponent(objectId)
sliceIdComponent = base32lower_nopad(sha256(UTF-8 objectId + "\0" + UTF-8 sliceId))
offsetEnd = offsetEndKey
generation = generationKey
```

Key-safe means ASCII letters, digits, `_`, `-`, and `.` only; no `/`, control characters, or empty
components. Exact `.` and `..` are not key-safe, and neither are Windows drive designators such as `C:`.
`keyComponent(x)` is `KeyComponentCodec.encodeComponent(x)` from `nereus-api`: it returns `x` only when it
is already key-safe and does not start with the reserved prefix `b32-`; otherwise it returns `b32-` plus
`base32lower_nopad(UTF-8 x)`. The same shared helper must be used everywhere a dynamic value enters an
Oxia path or object key. The reserved prefix avoids collisions between a raw safe component and the encoded
representation of a different unsafe component.

Phase 1 default numeric key encoding:

```text
offsetEndKey = zero-padded 19 digit decimal Java long
generationKey = zero-padded 19 digit decimal Java long
```

Both values must be non-negative and produced by `KeyComponentCodec.encodeNonNegativeLong`. The width `19`
covers `Long.MAX_VALUE` and preserves lexicographic order for all Phase 1 offsets and generations. A
different numeric key encoding requires an explicit keyspace migration plan.

## 3. Record Types

Package:

```text
io.nereus.metadata.oxia.records
```

The records below are hydrated Java/domain records returned by the adapter. Fields named
`metadataVersion` come from the Oxia key version returned by get/put/commit operations and do not have to
be serialized inside the value payload. Fields named `commitVersion` are durable Nereus stream commit
metadata and may be stored in offset index or committed-slice values.

### `StreamMetadataRecord`

```java
public record StreamMetadataRecord(
        String streamId,
        String streamName,
        String streamNameHash,
        String state,
        String profile,
        Map<String, String> attributes,
        long createdAtMillis,
        long policyVersion,
        long metadataVersion) {
}
```

### `StreamNameRecord`

```java
public record StreamNameRecord(
        String streamName,
        String streamId,
        String streamNameHash,
        long createdAtMillis,
        long metadataVersion) {
}
```

### `AppendSessionRecord`

```java
public record AppendSessionRecord(
        String streamId,
        String writerId,
        long epoch,
        String fencingToken,
        long leaseVersion,
        long expiresAtMillis) {
}
```

### `CommittedEndOffsetRecord`

```java
public record CommittedEndOffsetRecord(
        String streamId,
        long committedEndOffset,
        long cumulativeSize,
        long commitVersion,
        long metadataVersion) {
}
```

### `OffsetIndexRecord`

```java
public record OffsetIndexRecord(
        String streamId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long cumulativeSize,
        String objectId,
        String objectKey,
        String sliceId,
        String objectType,
        String physicalFormat,
        String logicalFormat,
        long objectOffset,
        long objectLength,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        EntryIndexReferenceRecord entryIndexRef,
        String projectionRef,
        String sliceChecksumType,
        String sliceChecksumValue,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        long commitVersion,
        boolean tombstoned,
        long metadataVersion) {
}
```

### `EntryIndexReferenceRecord`

```java
public record EntryIndexReferenceRecord(
        String location,
        String objectId,
        String objectKey,
        byte[] inlineData,
        long offset,
        long length,
        String checksumType,
        String checksumValue) {
}
```

Location rules mirror `EntryIndexRef` in `nereus-api`. Stored records should avoid nullable fields so
codec golden bytes are deterministic:

- absent `objectId` and `objectKey` are encoded as empty strings；
- absent `inlineData` is encoded as an empty byte array；
- absent `OffsetIndexRecord.projectionRef` is encoded as an empty string；
- footer references may omit `objectId/objectKey` when the index is in the same object；
- index-object references must include both `objectId` and `objectKey`；
- inline references must include non-empty `inlineData` and use `offset=0,length=0`。

### `ObjectManifestRecord`

```java
public record ObjectManifestRecord(
        String objectId,
        String objectKey,
        String objectType,
        String state,
        int formatMajorVersion,
        int formatMinorVersion,
        String writerVersion,
        String writerId,
        String writerRunIdHash,
        long writerEpoch,
        long createdAtMillis,
        long uploadedAtMillis,
        long objectLength,
        String objectChecksumType,
        String objectChecksumValue,
        String storageChecksumType,
        String storageChecksumValue,
        List<StreamSliceManifestRecord> slices,
        long orphanExpiresAtMillis,
        long metadataVersion) {
}
```

### `StreamSliceManifestRecord`

```java
public record StreamSliceManifestRecord(
        int sliceOrdinal,
        String streamId,
        String sliceId,
        long writerEpoch,
        long objectOffset,
        long objectLength,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        EntryIndexReferenceRecord entryIndexRef,
        String sliceChecksumType,
        String sliceChecksumValue,
        String payloadFormat,
        String state) {
}
```

`formatMajorVersion/formatMinorVersion` and `writerVersion` are copied from the WAL object header so
tools can choose the right decoder before opening object bytes. `objectChecksum*` is the WAL canonical
checksum stored in the object header. `storageChecksum*` is the checksum over exact uploaded bytes returned
by the object store. `sliceChecksum*` covers `slicePayloadBytes || entryIndexBytes`. `sliceOrdinal` is the
zero-based encoded order in the object and is part of the manifest identity for that slice.

### `ObjectReferenceRecord`

```java
public record ObjectReferenceRecord(
        String objectId,
        List<VisibleSliceReferenceRecord> visibleSlices,
        long updatedAtMillis,
        long metadataVersion) {
}

public record VisibleSliceReferenceRecord(
        String streamId,
        String sliceId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long commitVersion) {
}
```

`ObjectReferenceRecord` is an audit and GC input. It is not read visibility truth; visibility still comes
from `OffsetIndexRecord`. It must be repairable from committed offset index records.

### `CommittedSliceRecord`

```java
public record CommittedSliceRecord(
        String streamId,
        String objectId,
        String sliceId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long commitVersion) {
}
```

`CommittedSliceRecord` is stream-scoped and participates in the same atomic commit as the offset index.
It prevents accidentally committing the same object slice twice for the same stream.

Manifest states:

```text
PREPARED
UPLOADED
PARTIALLY_VISIBLE
VISIBLE
ORPHAN_CANDIDATE
GC_ELIGIBLE
DELETED
```

`VISIBLE` is a convenience/audit state only. Readers still must use offset index.

### `TrimRecord`

```java
public record TrimRecord(
        String streamId,
        long trimOffset,
        String reason,
        long updatedAtMillis,
        long metadataVersion) {
}
```

## 4. `OxiaMetadataStore` Interface

Package:

```text
io.nereus.metadata.oxia
```

Target interface:

```java
public interface OxiaMetadataStore extends AutoCloseable {
    CompletableFuture<StreamMetadataRecord> createOrGetStream(
            String cluster,
            StreamName streamName,
            StreamCreateOptions options);

    CompletableFuture<StreamMetadataRecord> getStream(
            String cluster,
            StreamId streamId);

    CompletableFuture<AppendSessionRecord> acquireAppendSession(
            String cluster,
            StreamId streamId,
            AppendSessionOptions options);

    CompletableFuture<AppendSessionRecord> renewAppendSession(
            String cluster,
            StreamId streamId,
            String writerId,
            long epoch,
            String fencingToken,
            Duration ttl);

    CompletableFuture<Void> putObjectManifest(
            String cluster,
            ObjectManifestRecord manifest);

    CompletableFuture<Optional<ObjectManifestRecord>> getObjectManifest(
            String cluster,
            ObjectId objectId);

    CompletableFuture<Optional<ObjectReferenceRecord>> getObjectReferences(
            String cluster,
            ObjectId objectId);

    CompletableFuture<ObjectReferenceRecord> repairObjectReferences(
            String cluster,
            ObjectId objectId);

    CompletableFuture<CommitSliceResult> commitStreamSlice(
            String cluster,
            CommitSliceRequest request);

    CompletableFuture<List<OffsetIndexRecord>> scanOffsetIndex(
            String cluster,
            StreamId streamId,
            long startOffset,
            int limit);

    CompletableFuture<CommittedEndOffsetRecord> getCommittedEndOffset(
            String cluster,
            StreamId streamId);

    CompletableFuture<TrimRecord> updateTrim(
            String cluster,
            StreamId streamId,
            long beforeOffset,
            String reason);

    CompletableFuture<TrimRecord> getTrim(
            String cluster,
            StreamId streamId);

    WatchRegistration watchStream(
            String cluster,
            StreamId streamId,
            MetadataWatcher watcher);
}
```

Implementation can wrap native Oxia operations behind smaller internal helpers:

```java
interface OxiaClient {
    CompletableFuture<OxiaGetResult> get(String key, PartitionKey partitionKey);
    CompletableFuture<OxiaPutResult> put(
            String key,
            byte[] value,
            PutCondition condition,
            PartitionKey partitionKey);
    CompletableFuture<OxiaCommitResult> commit(OxiaCommitBatch batch);
    CompletableFuture<List<OxiaKeyValue>> scan(
            String fromKeyExclusive,
            String toKeyExclusive,
            int limit,
            PartitionKey partitionKey);
    WatchRegistration watchPrefix(
            String prefix,
            PartitionKey partitionKey,
            MetadataWatcher watcher);
}

record PartitionKey(String value) {
}
```

`OxiaGetResult`, `OxiaPutResult`, `OxiaCommitResult`, `OxiaCommitBatch`, `OxiaKeyValue`, and
`PutCondition` are adapter-private placeholders for the real Oxia client binding. They should not leak
into `nereus-api`. `OxiaCommitBatch` must support conditional writes for a single stream key group.
Phase 1 must not require cross-shard or cross-partition atomicity for the producer-ack path.

Rules:

- every stream-scoped operation must pass `PartitionKey(streamId)`；
- every object-scoped operation must pass `PartitionKey(objectId)`；
- `OxiaCommitBatch` must carry one partition key and reject keys routed with a different partition key；
- the fake store must record supplied partition keys and fail contract tests when production code omits or
  changes them。

## 5. Create-Or-Get Stream

Operation:

```text
createOrGetStream(cluster, streamName)
```

Algorithm:

1. Compute `streamNameHash = base32lower_nopad(sha256(UTF-8 exact StreamName.value()))`.
2. Compute deterministic `streamId = "s-" + streamNameHash`.
3. Use `PartitionKey(streamId)` for all reads/writes in this operation.
4. Read `/streams/by-name/{streamNameHash}`.
5. If found, validate stored `streamName` and `streamId` match the requested `StreamName`.
6. Read `/streams/{streamId}/meta`.
7. If stream metadata exists with a different `streamName`, fail with `METADATA_INVARIANT_VIOLATION`
   because this indicates a deterministic id collision or data corruption.
8. If not found, create:
   - deterministic `StreamId`；
   - `StreamMetadataRecord(streamNameHash, state=ACTIVE, profile=OBJECT_WAL, attributes=options.attributes)`；
   - `CommittedEndOffsetRecord(committedEndOffset=0, cumulativeSize=0, commitVersion=0)`；
   - `TrimRecord(trimOffset=0)`；
   - name lookup record。
9. Commit all records in one stream key group with conditions:
   - by-name key missing；
   - stream meta key missing。

Idempotency:

- concurrent create with the same name returns the same stream；
- hash collision fails with a non-retriable metadata error。
- if the by-name record exists but stream metadata is missing, fail with `METADATA_INVARIANT_VIOLATION`
  and leave repair to an explicit metadata repair workflow。
- Phase 1 does not allocate random stream ids. If a future needs random or sequence stream ids, it must
  introduce an allocation protocol that preserves create-or-get atomicity.

## 6. Append Session Protocol

### Acquire

Input:

```text
streamId
writerId
ttl
allowStealExpiredSession
```

Condition:

```text
/streams/{streamId}/meta.state == ACTIVE
AND (
  append-session missing
  OR append-session expired AND (append-session.writerId == writerId OR allowStealExpiredSession)
  OR append-session.writerId == writerId
)
```

Output:

```text
epoch = previousEpoch + 1 when ownership changes or the old session is expired
epoch = currentEpoch when the same writer renews a live session
fencingToken = current token for same-writer live renew, new opaque token for a new epoch
leaseVersion = Oxia metadata version
expiresAtMillis = now + ttl
```

Implementation detail:

- `acquire` and `renew` may share one CAS loop.
- Use metadata server time if Oxia exposes it. Otherwise use local clock plus conservative TTL.
- Different-writer takeover of an expired session is allowed only when
  `AppendSessionOptions.allowStealExpiredSession=true`.
- Failed acquire should complete with `FENCED_APPEND` for a live conflicting owner or
  `APPEND_SESSION_EXPIRED` when caller intent disallows taking an expired owner。

### Renew

Condition:

```text
append-session.writerId == writerId
AND append-session.epoch == epoch
AND append-session.fencingToken == fencingToken
AND append-session.expiresAtMillis > now
AND /streams/{streamId}/meta.state == ACTIVE
```

Renew only extends expiration and increments `leaseVersion`.

## 7. Object Manifest Commit

After object upload succeeds and checksum is known:

```text
putObjectManifest(manifest.state=UPLOADED)
```

Conditions:

- manifest key missing, or same writer retry with identical WAL canonical object checksum,
  storage checksum, object length, and slice manifest；
- object state is not `DELETED`。

The manifest write is not the producer ack point.

Retry:

- same `objectId` plus same object checksum, storage checksum, object length, and slice manifest is
  idempotent；
- same `objectId` plus different checksum, length, or slice manifest is non-retriable corruption。

`orphanExpiresAtMillis` is an audit/repair hint in Phase 1. It must not authorize deleting an object that
has a manifest. Deletion requires a future GC protocol that proves no offset index entry references the
object.

## 8. Commit Stream Slice

`commitStreamSlice` is the Phase 1 linearization point for append.

Idempotent replay:

1. Before attempting a new atomic commit, read
   `/streams/{streamId}/committed-slices/{objectIdComponent}/{sliceIdComponent}`.
2. If it exists, read the matching offset index entry and return the original `CommitSliceResult`.
3. Do not create a second visible range for the same stream/object/slice.
4. The offset index entry must match the request's object id, slice id, physical range, record count,
   entry count, logical bytes, payload format, schema refs, and slice checksum.
5. If the marker exists but the offset index entry is missing or inconsistent, fail with
   `METADATA_INVARIANT_VIOLATION`.

This handles the crash/RPC-loss case after the stream commit succeeds but before the caller receives the
ack result.

Condition-failure classification:

1. If the atomic commit fails after the initial marker read, re-read the committed-slice marker before
   returning an error.
2. If the marker now exists and validates against the matching offset index, return the original
   `CommitSliceResult`.
3. If the marker exists but does not validate, fail `METADATA_INVARIANT_VIOLATION`.
4. If the marker is still missing, classify the failed condition by the stream-scoped state:
   stream state not `ACTIVE` -> `STREAM_NOT_ACTIVE`; stale epoch/token -> `FENCED_APPEND`; committed end
   mismatch -> `OFFSET_CONFLICT`; other CAS failures -> `METADATA_CONDITION_FAILED` or
   `METADATA_UNAVAILABLE` according to the adapter error.

This avoids misclassifying a concurrent same-slice replay as a new offset conflict.

Request:

```java
public record CommitSliceRequest(
        StreamId streamId,
        String writerId,
        String writerRunIdHash,
        long epoch,
        String fencingToken,
        long expectedStartOffset,
        String sliceId,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        ObjectId objectId,
        ObjectKey objectKey,
        Checksum objectChecksum,
        long objectOffset,
        long objectLength,
        EntryIndexRef entryIndexRef,
        Checksum sliceChecksum,
        PayloadFormat payloadFormat,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        Optional<ProjectionRef> projectionRef) {
}
```

`objectChecksum` in `CommitSliceRequest` is the WAL canonical object checksum, not the object-store
storage checksum over exact uploaded bytes.
`writerRunIdHash` identifies the `DefaultStreamStorage` process incarnation that produced the WAL object.

`logicalBytes` is the caller-visible uncompressed payload bytes represented by the slice. For
`OPAQUE_RECORD_BATCH` Phase 1 tests it is the sum of entry payload lengths. It is used only for
`cumulativeSize`; it is not an offset coordinate.

Atomic conditions:

```text
/streams/{streamId}/meta.state == ACTIVE
/streams/{streamId}/append-session.epoch == request.epoch
/streams/{streamId}/append-session.fencingToken == request.fencingToken
/streams/{streamId}/committed-end-offset.committedEndOffset == request.expectedStartOffset
/streams/{streamId}/committed-slices/{objectIdComponent}/{sliceIdComponent} is missing
```

`expiresAtMillis` is checked before attempting the commit and during acquire/renew. `DefaultStreamStorage`
must renew before upload or before `commitStreamSlice` when the remaining lease is below
`appendSessionMinCommitRemaining`. `expiresAtMillis` is not required as an Oxia value predicate in the
commit batch. Correct fencing comes from the current session record's epoch/token: once another writer
acquires the stream, these values change and stale commits fail.
`leaseVersion` is intentionally not part of `commitStreamSlice` conditions because same-writer live renew
increments it and must not fence that writer's in-flight appends.

Object manifest validation is a required pre-commit read, but not part of the stream atomic commit:

```text
/objects/{objectIdComponent}/manifest.state in [UPLOADED, PARTIALLY_VISIBLE, VISIBLE]
/objects/{objectIdComponent}/manifest.objectKey == request.objectKey
/objects/{objectIdComponent}/manifest.objectType == MULTI_STREAM_WAL_OBJECT
/objects/{objectIdComponent}/manifest.formatMajorVersion == 1
/objects/{objectIdComponent}/manifest.writerId == request.writerId
/objects/{objectIdComponent}/manifest.writerRunIdHash == request.writerRunIdHash
/objects/{objectIdComponent}/manifest.writerEpoch == request.epoch
/objects/{objectIdComponent}/manifest.objectChecksum == request.objectChecksum
/objects/{objectIdComponent}/manifest.slices contains request.sliceId
/objects/{objectIdComponent}/manifest.slices[request.sliceId].state == UPLOADED
/objects/{objectIdComponent}/manifest.slices[request.sliceId].sliceOrdinal is unique in the manifest
/objects/{objectIdComponent}/manifest.slices[request.sliceId].writerEpoch == request.epoch
/objects/{objectIdComponent}/manifest.slices[request.sliceId].sliceChecksum == request.sliceChecksum
/objects/{objectIdComponent}/manifest.slices[request.sliceId].objectOffset == request.objectOffset
/objects/{objectIdComponent}/manifest.slices[request.sliceId].objectLength == request.objectLength
/objects/{objectIdComponent}/manifest.slices[request.sliceId].recordCount == request.recordCount
/objects/{objectIdComponent}/manifest.slices[request.sliceId].entryCount == request.entryCount
/objects/{objectIdComponent}/manifest.slices[request.sliceId].logicalBytes == request.logicalBytes
/objects/{objectIdComponent}/manifest.slices[request.sliceId].payloadFormat == request.payloadFormat
/objects/{objectIdComponent}/manifest.slices[request.sliceId].schemaRefs == request.schemaRefs
```

The requested slice state must be `UPLOADED` for a new commit attempt. A same physical slice replay after a
successful stream commit is handled by the committed-slice marker before this validation path. If the marker
is missing but the manifest slice already says `VISIBLE`, object-scoped audit metadata is inconsistent with
stream-scoped truth and the commit must fail with `METADATA_INVARIANT_VIOLATION` rather than creating a new
offset range.

This split is intentional. Stream visibility must fit in one stream key group; object reference metadata is
repairable and must not force a cross-shard transaction onto producer ack.

Atomic writes:

```text
offsetStart = expectedStartOffset
offsetEnd = Math.addExact(expectedStartOffset, recordCount)
cumulativeSize = Math.addExact(currentCommittedEndOffsetRecord.cumulativeSize, logicalBytes)
generation = 0
commitVersion = Math.addExact(currentCommittedEndOffsetRecord.commitVersion, 1)

put /streams/{streamId}/offset-index/{offsetEnd}/0 = OffsetIndexRecord(commitVersion)
put /streams/{streamId}/committed-end-offset = {offsetEnd, cumulativeSize, commitVersion}
put /streams/{streamId}/committed-slices/{objectIdComponent}/{sliceIdComponent} = CommittedSliceRecord(generation=0, commitVersion)
notify stream-data-available
```

The `OffsetIndexRecord` value must include `logicalBytes` and the slice checksum from the request. The read
resolver must copy that checksum into `ResolvedObjectRange.sliceChecksum`; otherwise the WAL reader cannot
verify the full slice before clipping.
The offset index value also stores canonical slice-level `schemaRefs` derived from the append batch so
`resolve` and `read` do not need object manifest or footer reads to expose schema metadata.

`commitVersion` must be monotonic for visible commits of one stream and must be computable before the
atomic write batch is encoded. Phase 1 stores it in `CommittedEndOffsetRecord`; `commitStreamSlice` reads
the current committed-end record under a version/condition check, computes `current.commitVersion + 1`,
and writes that same value into the new committed-end record, `OffsetIndexRecord`, and
`CommittedSliceRecord`. Do not use the Oxia key version returned after commit as this durable value,
because that version is not available when encoding sibling records in the same atomic batch. Hydrated
`metadataVersion` remains the Oxia key version for each individual key.

Post-commit repairable writes:

```text
put /objects/{objectIdComponent}/references = ObjectReferenceRecord with this visible slice added
update /objects/{objectIdComponent}/manifest.slices[request.sliceId].state = VISIBLE
update /objects/{objectIdComponent}/manifest.state = PARTIALLY_VISIBLE or VISIBLE
```

Failure of post-commit repairable writes must not roll back the append and must not block producer ack after
the stream commit succeeds. GC must treat offset index as the authoritative reference source when object
reference metadata is stale or missing.

Post-commit object-scoped updates should be best-effort CAS merge operations:

- object reference update reads the current reference list, adds the committed slice if absent, and writes
  back with a version condition；
- manifest update changes only the matching slice state and recomputes aggregate object state；
- CAS conflict may be retried a bounded number of times；
- after retries are exhausted, leave the object metadata stale and rely on repair from offset index；
- do not overwrite another stream slice's already-visible reference or manifest state with an older value。

Phase 1 does not implement object deletion. In particular, a WAL object with a manifest must not be
deleted solely because its object reference record is missing or stale.

Object reference repair:

```text
repairObjectReferences(cluster, objectId)
```

Algorithm:

1. Read `ObjectManifestRecord`. If missing, fail with `METADATA_INVARIANT_VIOLATION` for repair callers.
2. For each manifest slice, read the stream-scoped committed-slice marker.
3. If marker exists, read and validate the matching offset index entry.
4. Build `VisibleSliceReferenceRecord` only for validated visible slices.
5. CAS-merge the rebuilt reference list into `/objects/{objectIdComponent}/references`.

Repair must not use object-store list and must not create visibility. It only rebuilds object-scoped audit
metadata from stream-scoped truth.

Result:

```java
public record CommitSliceResult(
        StreamId streamId,
        OffsetRange range,
        long committedEndOffset,
        long generation,
        long commitVersion,
        Optional<ProjectionRef> projectionRef) {
}
```

Dense offset rule:

- `offsetStart` must equal current `committedEndOffset`；
- `offsetEnd = offsetStart + recordCount`；
- no visible gap can be created。

## 9. Offset Index Scan

Resolver receives `targetOffset` from `resolve(streamId, targetOffset, options)` and needs the first
index entries whose `offsetEnd > targetOffset`.

Implementation with lexicographic keys:

```text
fromExclusive = /streams/{streamId}/offset-index/{padded(targetOffset)}/~
to   = /streams/{streamId}/offset-index/~/
scan limit = small page size
```

This avoids skipping `offsetEnd == targetOffset + 1` and avoids overflow at very large offsets. The `~`
suffix sorts after all generations for exactly `targetOffset`, so the next key has
`offsetEnd > targetOffset`.

Encoding requirement:

- numeric offsets in keys must use `offsetEndKey`, the 19-digit zero-padded decimal Java long encoding；
- numeric generations in keys must use `generationKey`, the 19-digit zero-padded decimal Java long encoding；
- plain variable-length decimal strings are not safe for lexicographic ordering。

Example fixed-width:

```text
0000000000001048576
```

Candidate selection:

1. scan entries ordered by `offsetEnd`, then generation；
2. scan a bounded lookahead window, not only the first key, so future larger compacted ranges can be
   considered if they also cover the target offset；
3. group candidates that cover `targetOffset` where `offsetStart <= targetOffset < offsetEnd`；
4. choose max visible generation；
5. in Phase 1 only generation `0` exists, so one non-overlapping append entry is normally sufficient。

The bounded lookahead is an extension point, not the complete Future 4 compaction resolver. Future 4 must
define the exact coverage lookup strategy for large compacted ranges before enabling generation
replacement.

## 10. Trim Update

`updateTrim(beforeOffset)` conditions:

```text
/streams/{streamId}/meta.state in [ACTIVE, SEALED]
beforeOffset >= 0
beforeOffset >= currentTrimOffset
beforeOffset <= committedEndOffset
```

Writes:

```text
/streams/{streamId}/trim = beforeOffset
```

No object delete and no offset index delete in Phase 1.

## 11. Watch and Cache Invalidation Boundary

Phase 1 can run without watch by using read-through cache with short TTL. The API should still leave a
watch boundary:

```java
interface WatchRegistration extends AutoCloseable {
    @Override
    void close();
}

interface MetadataWatcher {
    void onOffsetIndexUpdated(StreamId streamId, long committedEndOffset, long metadataVersion);
    void onTrimUpdated(StreamId streamId, long trimOffset, long metadataVersion);
    void onAppendSessionChanged(StreamId streamId, long epoch, long leaseVersion);
}
```

`OffsetIndexCache` must treat watch events as hints. Correctness still comes from validating generation and
metadata version when resolving.

Watch rules:

- `watchStream` may return a no-op registration when watch is disabled by config; cache correctness must
  still come from TTL/read-through validation；
- watch registration is best-effort and may miss, reorder, or collapse intermediate updates；
- watch callbacks are invalidation hints only. They must not create or update positive offset-index cache
  records without a follow-up scan；
- if a watch callback carries `metadataVersion` lower than a cached range's observed version, the cache may
  ignore it as stale；
- a watch event must never be required for append/read correctness；
- `onOffsetIndexUpdated` should carry at least the stream id and committed end offset observed after the
  update. It may skip intermediate committed ends；
- `onTrimUpdated` invalidates cached ranges below the new trim offset；
- `onAppendSessionChanged` invalidates local append-session cache only, not committed read data；
- reconnect should invalidate affected local caches or force short-TTL read-through until a fresh scan
  succeeds；
- fake watch tests should simulate missed, duplicate, collapsed, and out-of-order events。

## 12. Failure Semantics

| Failure | Metadata outcome | Caller behavior |
| --- | --- | --- |
| Oxia unavailable before object upload | no metadata commit | retry append later |
| object upload succeeds, manifest put fails | orphan by object store naming; no visibility | retry manifest or GC after TTL |
| manifest succeeds, slice commit fails due to stale token | manifest remains uploaded; no visibility | reacquire session and retry or abandon |
| timeout after slice commit RPC is sent | commit may or may not have succeeded | retry same physical slice can use committed-slice marker; caller-level retry may duplicate |
| slice commit succeeds, ack response lost | data visible; committed-slice marker exists | retry can return idempotent commit result |
| slice commit succeeds, object reference update fails | data visible; object reference stale | repair object references from offset index |
| offset conflict | no new index entry | refresh committedEndOffset and retry |
| checksum mismatch | no index entry | fail non-retriably and quarantine object |

## 13. Fake Metadata Store for Tests

Before real Oxia integration, implement an in-memory store with the same operation-level semantics:

```text
io.nereus.metadata.oxia.testing.FakeOxiaMetadataStore
```

It must support:

- linearizable synchronized commit；
- CAS condition failures；
- metadata versions；
- session expiration through injected clock；
- required partition key recording and validation；
- scan ordering identical to production key encoding；
- watch registration and configurable missed/duplicate/collapsed/out-of-order notifications；
- failure injection before and after writes。
- no object deletion behavior; fake GC tests must assert deletion is outside Phase 1。

The fake store should not expose test-only shortcuts to `nereus-core` production code.
Test-only controls may exist on the fake instance used by tests, but production code must interact only
through `OxiaMetadataStore`.

Fake/real parity requirements:

- every successful write increments the affected key's `metadataVersion` monotonically；
- every successful `commitStreamSlice` increments the stream `commitVersion` exactly once and stores the
  same durable value in committed-end, offset-index, and committed-slice records；
- atomic commit either applies all stream-scoped writes and notifications or applies none；
- condition failure must not partially apply writes；
- same physical slice replay must observe the committed-slice marker and return the original result；
- scan ordering must match production key bytes, including 19-digit offset/generation encoding；
- partition key mismatch must fail before applying a write；
- injected post-write response loss must leave durable metadata visible to later reads；
- fake and real adapters must share the same metadata codecs, not separate test-only record encoders。

## 14. Metadata Record Encoding and Versioning

`metadataVersion` fields in hydrated records are Oxia key versions returned by the adapter. They are not
schema versions and should not be confused with durable `commitVersion` fields stored in stream commit
records. Every stored value must use an explicit encoding envelope so the fake store, real Oxia adapter,
and repair tools decode the same bytes.

Adapter-private codec shape:

```java
interface MetadataRecordCodec<T> {
    String recordType();
    int schemaVersion();
    int minReaderSchemaVersion();
    byte[] encode(T record);
    T decode(byte[] bytes);
}

interface MetadataCodecRegistry {
    <T> MetadataRecordCodec<T> codecForType(String recordType);
    <T> MetadataRecordCodec<T> codecForClass(Class<T> recordClass);
}
```

Envelope fields:

```text
magic = "NRM1"
recordType
schemaVersion
minReaderSchemaVersion
payloadEncoding
payloadLength
payloadChecksum
payload
```

Rules:

- Phase 1 should use one schema-first encoding for production records, preferably Protobuf once the build
  adds the dependency. A deterministic JSON codec may be used only before the codec freeze, for early
  fake-store spikes hidden behind the same `MetadataRecordCodec` contract. It must not be accepted as the
  final `phase1Check` codec unless the real adapter, fake store, repair tools, and golden bytes all use
  that exact deterministic JSON codec；
- map fields such as stream attributes and entry attributes must encode entries sorted by UTF-8 key bytes；
- repeated fields preserve the order specified by their owning contract. `schemaRefs` use canonical
  `(namespace,id,version)` order; manifest slice lists preserve encoded object slice order；
- fake store, real Oxia adapter, repair tools, and migration tools must use the same
  `MetadataCodecRegistry`；
- `recordType` names are stable durable identifiers such as `StreamMetadataRecord`,
  `OffsetIndexRecord`, and `ObjectManifestRecord`; Java package renames must not change durable
  `recordType` values；
- decode must reject wrong `recordType`, unknown newer required schema versions, checksum mismatch, and
  truncated payloads with `METADATA_INVARIANT_VIOLATION` or `METADATA_UNAVAILABLE` depending on whether
  bytes are corrupt or the store is unavailable；
- adding optional fields requires defaults and a schema-version test；
- changing field meaning requires a new record type or an explicit migration plan；
- codec tests must include golden bytes for each Phase 1 record type before the implementation is treated
  as frozen；
- key encoding version and value schema version are separate concerns. Fixed-width offset/generation key
  encoding must not change without a keyspace migration document。
