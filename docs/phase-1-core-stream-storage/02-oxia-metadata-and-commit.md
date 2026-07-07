# 02 Oxia Metadata and Commit Design

本文把 Phase 1 需要的 Oxia metadata schema、record 类型和 commit 操作拆到代码级。

M0.5 status: the selected public Oxia Java client API does not expose the multi-key conditional write
primitive assumed by the first draft of `commitStreamSlice`. This file has been rewritten to match the
current public Oxia behavior: Phase 1 uses one authoritative stream-head key CAS as the append
linearization point, and treats offset-index and committed-slice keys as materialized, repairable read
indexes. M2 fake metadata must implement this single-key-CAS protocol, not the old atomic multi-key batch.

M2 foundation implementation status:

- production code now has `OxiaMetadataStore`, `OxiaKeyspace`, `PartitionKey`, watch interfaces, commit
  request/result records, metadata record classes, metadata codec envelope/registry scaffolding, and a
  `Phase1MetadataCodecs` binary-v1 implementation；
- `io.nereus.metadata.oxia.testing.FakeOxiaMetadataStore` exists under test fixtures and implements the
  stream-head single-key CAS protocol for create/get, append session acquire/renew, object manifest
  put/get, `commitStreamSlice`, derived-index repair, offset-index scan, object-reference repair, trim,
  and watch registration；
- fake commit truth is stream-head reachability through immutable `StreamCommitRecord`; offset-index and
  committed-slice records are repairable derived state；
- fake failure injection currently covers the critical head-CAS-success-before-derived-index point,
  post-derived-index object-audit/reference update failure, plus earlier commit phases used by future
  tests；
- fake watch simulation can drop, duplicate, collapse, emit reconnect-before-current, or deliver
  stale-before-current invalidation hints so cache tests do not treat watch as correctness truth；
- metadata envelope and codec tests cover magic/payload/checksum/truncation behavior, strict UTF-8 decode,
  wrong record type, unsupported schema/encoding, invalid payload tags, deterministic map order,
  round-trip for every Phase 1 metadata record, and golden envelope hex for every Phase 1 record type；
- 2026-07-06 hardening pass closed the first review gaps for nested length-prefixed `commitId` identity,
  event-time/projection identity coverage, full canonical replay validation, record-level
  `offset + length` overflow checks on decoded metadata, committed-slice-marker-first replay, and
  post-commit object-audit failure injection；
- 2026-07-07 helper pass added package-private `PartitionedOxiaClient` so future real adapter get/put/list/
  rangeScan/watch/head-CAS calls cannot be built without a `PartitionKey`；
- 2026-07-07 watch pass added collapsed and reconnect-before-current watch simulation；
- 2026-07-07 codec/validation pass closed decoded `EntryIndexReferenceRecord` location-shape validation,
  strict UTF-8 decode, and per-record codec round-trip/golden/error-path tests；
- 2026-07-07 fake-store codec pass changed fake stored metadata maps to keep encoded envelope bytes and
  decode through `Phase1MetadataCodecs` on read；
- still not complete for full M2 exit: broader linearizability tests remain.

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
/nereus/clusters/{clusterComponent}/streams/{streamId}/head
/nereus/clusters/{clusterComponent}/streams/{streamId}/commit-log/{commitIdComponent}
/nereus/clusters/{clusterComponent}/streams/{streamId}/offset-index/{offsetEnd}/{generation}
/nereus/clusters/{clusterComponent}/streams/{streamId}/committed-slices/{objectIdComponent}/{sliceIdComponent}
/nereus/clusters/{clusterComponent}/streams/by-name/{streamNameHash}

/nereus/clusters/{clusterComponent}/objects/{objectIdComponent}/manifest
/nereus/clusters/{clusterComponent}/objects/{objectIdComponent}/references
/nereus/clusters/{clusterComponent}/objects/{objectIdComponent}/gc
```

Notes:

- `/streams/{streamId}/head` is the authoritative stream record. It contains stream metadata, stream
  state, append session snapshot, committed end, trim low-watermark, durable commit version, cumulative
  logical size, and the latest committed commit id.
- `streamId` is deterministic: `s-` plus the full `streamNameHash`. Therefore
  `createOrGetStream(streamName)` can compute the authoritative head key directly. The by-name key is an
  optional lookup/audit cache, not a correctness dependency.
- `streamNameHash` is for stable idempotent lookup. The head value and optional by-name value must contain
  the original `StreamName` to detect hash collisions.
- `/commit-log/{commitIdComponent}` stores immutable append commit records. A commit-log record is not
  visible by itself; it becomes committed only when it is reachable from the stream head's
  `lastCommitId` chain after a successful head CAS.
- Offset index key includes `{generation}` from Phase 1. Append WAL entries use `generation=0`.
- `metadataVersion` is the Oxia key version for the key that was read or written. `commitVersion` is the
  durable Nereus stream commit sequence stored in `StreamHeadRecord`, `StreamCommitRecord`, and derived
  read-index records. Neither is compaction generation.
- Every stream-scoped operation must use `PartitionKey(streamId)`. The only append linearization write is
  a conditional put of `/streams/{streamId}/head` with `IfVersionIdEquals(previousHeadVersion)`.
- Phase 1 must not rely on Oxia multi-key atomicity. The public API shape verified in M0.5 supports
  single-key conditional put/delete and partition-key-aware get/list/rangeScan/sequence APIs; fake
  metadata must not expose a stronger commit primitive.
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
commitId = base32lower_nopad(sha256(canonical CommitSliceRequest identity))
commitIdComponent = keyComponent(commitId)
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

`StreamMetadataRecord` is a hydrated view derived from `StreamHeadRecord`. Phase 1 does not require a
separate authoritative `/meta` key.

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

`StreamNameRecord` is an optional lookup/audit cache. The authoritative name-to-id mapping is still the
deterministic `streamId = "s-" + streamNameHash` plus the `streamName` stored in `StreamHeadRecord`.

### `StreamHeadRecord`

```java
public record StreamHeadRecord(
        String streamId,
        String streamName,
        String streamNameHash,
        String state,
        String profile,
        Map<String, String> attributes,
        long createdAtMillis,
        long policyVersion,
        long committedEndOffset,
        long cumulativeSize,
        long commitVersion,
        long trimOffset,
        String lastCommitId,
        AppendSessionSnapshotRecord appendSession,
        long metadataVersion) {
}

public record AppendSessionSnapshotRecord(
        String writerId,
        long epoch,
        String fencingToken,
        long leaseVersion,
        long expiresAtMillis) {
}
```

`StreamHeadRecord` is the only Phase 1 stream key that participates in append, session, state, committed
end, and trim CAS operations. Empty `lastCommitId` means the stream has no committed append records.
An empty `appendSession.writerId` means no append session is currently owned.

### `StreamCommitRecord`

```java
public record StreamCommitRecord(
        String streamId,
        String commitId,
        String previousCommitId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long cumulativeSize,
        long commitVersion,
        String writerId,
        String writerRunIdHash,
        long writerEpoch,
        String fencingTokenHash,
        String objectId,
        String objectKey,
        String sliceId,
        String objectType,
        String physicalFormat,
        String logicalFormat,
        String payloadFormat,
        String objectChecksumType,
        String objectChecksumValue,
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
        long preparedAtMillis,
        long metadataVersion) {
}
```

`StreamCommitRecord` is written before the stream-head CAS and is immutable. It is only committed if the
current `StreamHeadRecord.lastCommitId` chain reaches `commitId`. Records not reachable from the head are
orphan commit intents and must be ignored by reads, repair, and object reference rebuilds. Same physical
retry must reuse an existing record with the same `commitId` after validating its canonical identity
fields; volatile fields such as `preparedAtMillis` must not make a replay look like a conflicting commit.

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

`AppendSessionRecord` is a public/internal view derived from `StreamHeadRecord.appendSession`. The session
snapshot itself is stored inside the stream head so append commit CAS can validate epoch/token with one
public Oxia conditional put.

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

`CommittedEndOffsetRecord` is a compatibility view derived from `StreamHeadRecord`; Phase 1 does not keep
an authoritative `/committed-end-offset` key.

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

Location rules must mirror `EntryIndexRef` in `nereus-api`. Stored records should avoid nullable fields so
codec golden bytes are deterministic:

- absent `objectId` and `objectKey` are encoded as empty strings；
- absent `inlineData` is encoded as an empty byte array；
- `OffsetIndexRecord.projectionRef` and `StreamCommitRecord.projectionRef` store the canonical projection
  identity string. An absent public `ProjectionRef` is encoded as the canonical absent identity, not as a
  null field. The exact absent/present bytes are covered by codec golden tests；
- footer references may omit `objectId/objectKey` when the index is in the same object. If one is
  present, both must be present. Footer references use empty `inlineData` and positive `length`；
- index-object references must include both `objectId` and `objectKey`, use empty `inlineData`, and use
  positive `length`；
- inline references must include non-empty `inlineData` and use `offset=0,length=0`。

2026-07-07 codec/validation pass: the Java record constructor now also enforces the same
`INLINE`/`OBJECT_FOOTER`/`INDEX_OBJECT` location-specific shape rules as the public `EntryIndexRef` API
contract.

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

`ObjectReferenceRecord` is an audit and GC input. It is not read visibility truth; visibility comes from
`StreamHeadRecord.lastCommitId` reachability. `OffsetIndexRecord` is the normal read-serving materialized
index and can be rebuilt from the committed head chain when object-reference repair needs it.

### `CommittedSliceRecord`

```java
public record CommittedSliceRecord(
        String streamId,
        String objectId,
        String sliceId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long commitVersion,
        long metadataVersion) {
}
```

`CommittedSliceRecord` is a materialized idempotency/read-repair marker derived from a committed
`StreamCommitRecord`. It is not the append linearization key. If the marker is missing, the adapter must
fall back to the stream-head commit chain before deciding whether the same physical slice already
committed.

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

`TrimRecord` is a hydrated view derived from `StreamHeadRecord.trimOffset`. Phase 1 does not keep an
authoritative `/trim` key.

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

    CompletableFuture<DerivedIndexRepairResult> repairDerivedStreamIndexes(
            String cluster,
            StreamId streamId,
            long targetOffset,
            int maxRecordsToRepair);

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

public record DerivedIndexRepairResult(
        StreamId streamId,
        long repairedFromOffset,
        long repairedToOffset,
        int repairedRecords,
        boolean targetCovered,
        boolean repairBudgetExhausted,
        long observedCommitVersion) {
}
```

Implementation can wrap native Oxia operations behind smaller internal helpers:

```java
interface OxiaClient {
    CompletableFuture<OxiaGetResult> get(String key, PartitionKey partitionKey);
    CompletableFuture<OxiaPutResult> putIfAbsent(
            String key,
            byte[] value,
            PartitionKey partitionKey);
    CompletableFuture<OxiaPutResult> putIfVersion(
            String key,
            byte[] value,
            long expectedVersion,
            PartitionKey partitionKey);
    CompletableFuture<OxiaDeleteResult> deleteIfVersion(
            String key,
            long expectedVersion,
            PartitionKey partitionKey);
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

`OxiaGetResult`, `OxiaPutResult`, `OxiaDeleteResult`, and `OxiaKeyValue` are adapter-private placeholders
for the real Oxia client binding. They should not leak into `nereus-api`. The adapter deliberately does
not define an `OxiaCommitBatch` abstraction in Phase 1 because the selected public Java client API does
not expose a supportable multi-key conditional write primitive.

Rules:

- every stream-scoped operation must pass `PartitionKey(streamId)`；
- every object-scoped operation must pass `PartitionKey(objectId)`；
- append/session/trim linearization is always a `putIfVersion` on `/streams/{streamId}/head`；
- create stream and immutable commit-log records use `putIfAbsent`；
- derived offset-index, committed-slice, by-name, object-reference, and manifest-state updates are
  idempotent single-key puts or CAS merges；
- the fake store must record supplied partition keys and fail contract tests when production code omits or
  changes them；
- the fake store must be able to inject failure between head CAS and derived-index materialization, because
  that is the critical recovery path introduced by the Oxia public API constraint。

## 5. Create-Or-Get Stream

Operation:

```text
createOrGetStream(cluster, streamName)
```

Algorithm:

1. Compute `streamNameHash = base32lower_nopad(sha256(UTF-8 exact StreamName.value()))`.
2. Compute deterministic `streamId = "s-" + streamNameHash`.
3. Use `PartitionKey(streamId)` for all reads/writes in this operation.
4. Read `/streams/{streamId}/head`.
5. If found, validate stored `streamName` and `streamId` match the requested `StreamName`.
6. If stream head exists with a different `streamName`, fail with `METADATA_INVARIANT_VIOLATION`
   because this indicates a deterministic id collision or data corruption.
7. If not found, build `StreamHeadRecord` with deterministic `StreamId`, `state=ACTIVE`,
   `profile=OBJECT_WAL`, `attributes=options.attributes`, `committedEndOffset=0`, `cumulativeSize=0`,
   `commitVersion=0`, `trimOffset=0`, empty `lastCommitId`, and empty append session.
8. `putIfAbsent(/streams/{streamId}/head, StreamHeadRecord)`.
9. If the conditional put loses a race, re-read the head and validate it as in step 5.
10. Best-effort write `/streams/by-name/{streamNameHash}` as a derived cache. Failure to write the cache
    must not make the stream creation fail after the head exists.

Idempotency:

- concurrent create with the same name returns the same stream；
- hash collision fails with a non-retriable metadata error。
- if the by-name record exists but stream head is missing, ignore the cache for correctness, re-create or
  re-read the deterministic head, and leave cache repair to an explicit metadata repair workflow。
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
head.state == ACTIVE
AND (
  head.appendSession missing
  OR head.appendSession expired AND (head.appendSession.writerId == writerId OR allowStealExpiredSession)
  OR head.appendSession.writerId == writerId
)
```

Output:

```text
epoch = previousEpoch + 1 when ownership changes or the old session is expired
epoch = currentEpoch when the same writer renews a live session
fencingToken = current token for same-writer live renew, new opaque token for a new epoch
leaseVersion = head.appendSession.leaseVersion + 1 on acquire/renew
expiresAtMillis = now + ttl
```

Implementation detail:

- `acquire` and `renew` are CAS loops over `/streams/{streamId}/head`.
- Use metadata server time if Oxia exposes it. Otherwise use local clock plus conservative TTL.
- Different-writer takeover of an expired session is allowed only when
  `AppendSessionOptions.allowStealExpiredSession=true`.
- Failed acquire should complete with `FENCED_APPEND` for a live conflicting owner or
  `APPEND_SESSION_EXPIRED` when caller intent disallows taking an expired owner。

### Renew

Condition:

```text
head.appendSession.writerId == writerId
AND head.appendSession.epoch == epoch
AND head.appendSession.fencingToken == fencingToken
AND head.appendSession.expiresAtMillis > now
AND head.state == ACTIVE
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

`commitStreamSlice` uses a single Oxia public API linearization point:

```text
putIfVersion(/streams/{streamId}/head, newHead, previousHead.metadataVersion)
```

The old design tried to atomically update committed end, offset index, and committed-slice marker in one
conditional multi-key batch. M0.5 proved the selected public Oxia Java API does not expose that primitive,
so Phase 1 now separates commit truth from read-serving indexes:

- `StreamHeadRecord` is the authoritative stream state and commit head；
- `StreamCommitRecord` is an immutable, pre-written commit intent；
- `OffsetIndexRecord` and `CommittedSliceRecord` are materialized indexes derived from committed
  commit-log records；
- a derived-index write may fail after the head CAS, so read/replay paths must be able to repair it from
  the head commit chain。

Producer ack is allowed only after the head CAS has succeeded and the offset-index plus committed-slice
marker for this slice have been materialized or idempotently confirmed. If the head CAS may have succeeded
but materialization cannot be confirmed before timeout/cancellation, the append result is unknown final
state, not a known failure.

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

Commit identity:

```text
commitId = sha256(canonical streamId, expectedStartOffset, writerId, writerRunIdHash, epoch,
                  fencingTokenHash, objectId, objectKey, objectChecksum, sliceId, objectOffset,
                  objectLength, sliceChecksum, recordCount, entryCount, logicalBytes,
                  payloadFormat, schemaRefs, entryIndexRef, minEventTimeMillis,
                  maxEventTimeMillis, projectionRef)
```

`fencingTokenHash` is `base32lower_nopad(sha256(UTF-8 fencingToken))`. The raw fencing token is used only
to validate the current append session. `StreamCommitRecord` stores the hash so durable metadata and
golden bytes do not persist the opaque token itself. The canonical `commitId` input order uses
length-prefixed UTF-8 fields at every nested level, including `EntryIndexRef` and `projectionRef`, so
legal delimiter characters in API values cannot collide. Text encoding, empty optional-field encoding, and
schema-ref ordering must be covered by golden tests shared by fake and real adapters.

The same physical slice retry uses the same `commitId`. Phase 1 default behavior does not rebase an
uploaded slice to a different `expectedStartOffset`; an offset conflict returns a retriable error to the
caller.

`expiresAtMillis` is checked before attempting the commit and during acquire/renew. `DefaultStreamStorage`
must renew before upload or before `commitStreamSlice` when the remaining lease is below
`appendSessionMinCommitRemaining`. `expiresAtMillis` is not required as an Oxia value predicate in the
head CAS payload. Correct fencing comes from the current head's session epoch/token: once another writer
acquires the stream, these values change and stale commits fail. `leaseVersion` is intentionally not part
of `commitStreamSlice` conditions because same-writer live renew
increments it and must not fence that writer's in-flight appends.

Object manifest validation is a required pre-head-CAS read, but not part of the stream linearization
write:

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
successful stream commit is handled by the committed-slice marker or by the stream-head commit chain before
this validation path creates any new commit. If the marker is missing but the manifest slice already says
`VISIBLE`, object-scoped audit metadata is inconsistent with stream-scoped truth; the adapter must search
the committed head chain for the same `commitId` before deciding whether this is idempotent replay or
`METADATA_INVARIANT_VIOLATION`.

This split is intentional. Stream commit truth must fit in one single-key Oxia CAS; object reference
metadata is repairable and must not force cross-shard or multi-key atomicity onto producer ack.

Commit algorithm:

```text
1. Compute `fencingTokenHash` and `commitId`.
2. Try replay discovery:
   a. read materialized committed-slice marker;
   b. if absent, read stream head and walk the reachable commit chain until commitId is found or the
      configured replay-search bound is reached.
3. If replay is found, validate the committed commit record against the request, materialize any missing
   derived records, and return the original CommitSliceResult.
4. Start a bounded head-CAS loop. Each iteration reads StreamHeadRecord with PartitionKey(streamId).
5. Validate the current head:
   head.state == ACTIVE
   head.appendSession.epoch == request.epoch
   head.appendSession.fencingToken == request.fencingToken
   head.committedEndOffset == request.expectedStartOffset
6. Validate object manifest and slice fields.
7. Build immutable StreamCommitRecord:
   previousCommitId = head.lastCommitId
   offsetStart = request.expectedStartOffset
   offsetEnd = Math.addExact(request.expectedStartOffset, recordCount)
   cumulativeSize = Math.addExact(head.cumulativeSize, logicalBytes)
   generation = 0
   commitVersion = Math.addExact(head.commitVersion, 1)
8. putIfAbsent(/streams/{streamId}/commit-log/{commitIdComponent}, StreamCommitRecord).
   If the record already exists, validate all canonical commit identity fields against the request and the
   current head snapshot, including previousCommitId, offsetStart, offsetEnd, cumulativeSize, and
   commitVersion. If they match, reuse the stored record; if they differ, fail
   METADATA_INVARIANT_VIOLATION unless the current head already reaches the same commitId.
9. CAS /streams/{streamId}/head from the version read in step 4 to:
   committedEndOffset = commitRecord.offsetEnd
   cumulativeSize = commitRecord.cumulativeSize
   commitVersion = commitRecord.commitVersion
   lastCommitId = commitId
   appendSession = current head.appendSession
   trimOffset = current head.trimOffset
   stream metadata/state fields copied from the current head
10. If head CAS loses, re-read the head:
    a. if the current head chain reaches commitId, materialize missing derived records and return success;
    b. if state, epoch/token, or committedEndOffset no longer satisfy step 5, classify the failure;
    c. if state, epoch/token, committedEndOffset, commitVersion, and lastCommitId are still compatible,
       retry the CAS with the current head version. This is the expected path when same-writer renew or
       trim updated the head between read and CAS.
11. If head CAS succeeds, materialize offset-index and committed-slice marker from the committed record.
12. Ack only after the derived records required for normal read and same-slice replay are confirmed.
```

The `OffsetIndexRecord` value must include `logicalBytes` and the slice checksum from the request. The read
resolver must copy that checksum into `ResolvedObjectRange.sliceChecksum`; otherwise the WAL reader cannot
verify the full slice before clipping.
The offset index value also stores canonical slice-level `schemaRefs` derived from the append batch so
`resolve` and `read` do not need object manifest or footer reads to expose schema metadata.

`commitVersion` must be monotonic for visible commits of one stream and must be computable before the
head CAS is attempted. Phase 1 stores it in `StreamHeadRecord` and `StreamCommitRecord`; materialized
`OffsetIndexRecord` and `CommittedSliceRecord` copy the value from the committed record. Do not use the
Oxia key version returned after commit as this durable value. Hydrated `metadataVersion` remains the Oxia
key version for each individual key.

Head-CAS failure classification:

1. Re-read the head.
2. If the current head chain reaches `commitId`, treat the attempt as committed, materialize missing
   derived records, and return the original result.
3. If head state is not `ACTIVE`, return `STREAM_NOT_ACTIVE`.
4. If epoch/token no longer match, return `FENCED_APPEND`.
5. If `head.committedEndOffset != request.expectedStartOffset`, return `OFFSET_CONFLICT`.
6. If head state, epoch/token, committed end, commitVersion, and lastCommitId are still compatible, retry
   the CAS loop because another non-append head update such as same-writer renew or trim only changed the
   Oxia key version.
7. Otherwise return `METADATA_CONDITION_FAILED` or `METADATA_UNAVAILABLE` according to the adapter error.

Materialization failure after successful head CAS:

- the append is already logically committed because the head points to the commit record；
- the adapter must retry materializing offset-index and committed-slice marker while the caller's
  timeout/cancellation budget allows；
- if materialization cannot be confirmed before the budget expires, surface `TIMEOUT`, `CANCELLED`, or
  `METADATA_UNAVAILABLE` with unknown-final-state semantics；
- do not return `OFFSET_CONFLICT` or a known-not-committed error after a successful or possibly successful
  head CAS；
- the next same physical slice retry must discover the commit through the head chain and finish
  materialization instead of creating a second range。

Post-commit repairable writes:

```text
put /objects/{objectIdComponent}/references = ObjectReferenceRecord with this visible slice added
update /objects/{objectIdComponent}/manifest.slices[request.sliceId].state = VISIBLE
update /objects/{objectIdComponent}/manifest.state = PARTIALLY_VISIBLE or VISIBLE
```

Failure of post-commit repairable writes must not roll back the append and must not block producer ack after
the stream commit succeeds. GC/repair must treat the stream-head commit chain as authoritative when object
reference metadata is stale or missing; offset index is the normal materialized lookup source and can be
repaired from that chain if needed.

Post-commit object-scoped updates should be best-effort CAS merge operations:

- object reference update reads the current reference list, adds the committed slice if absent, and writes
  back with a version condition；
- manifest update changes only the matching slice state and recomputes aggregate object state；
- CAS conflict may be retried a bounded number of times；
- after retries are exhausted, leave the object metadata stale and rely on repair from the stream-head
  commit chain and materialized offset index；
- do not overwrite another stream slice's already-visible reference or manifest state with an older value。

Phase 1 does not implement object deletion. In particular, a WAL object with a manifest must not be
deleted solely because its object reference record is missing or stale.

Derived index repair:

```text
repairDerivedStreamIndexes(streamId, targetOffset)
```

Algorithm:

1. Read `StreamHeadRecord`.
2. If `targetOffset >= head.committedEndOffset`, no repair is needed for a normal read.
3. Walk the commit chain from `head.lastCommitId` through `StreamCommitRecord.previousCommitId`, but
   materialize at most `maxRecordsToRepair` records in one call.
4. Stop once the walk has materialized the committed record covering `targetOffset` and all later records
   needed by the caller's bounded resolve window, once it reaches offset `0`, or once the repair budget is
   exhausted.
5. For each reachable commit record, idempotently put:
   - `/offset-index/{offsetEnd}/{generation}`；
   - `/committed-slices/{objectIdComponent}/{sliceIdComponent}`。
6. If `maxRecordsToRepair` is exhausted before the target offset is covered, return
   `DerivedIndexRepairResult(targetCovered=false, repairBudgetExhausted=true)`. The resolver may issue
   another bounded repair within the read timeout, but it must not convert budget exhaustion into
   `METADATA_INVARIANT_VIOLATION`.
7. If the chain is broken, a commit record is corrupt, or a derived record exists with conflicting bytes,
   fail with `METADATA_INVARIANT_VIOLATION`.

Reads should normally use offset-index directly. If an offset-index scan shows a gap below
`head.committedEndOffset`, the resolver must attempt this repair before reporting a metadata invariant
failure. Budget exhaustion is retriable or may be retried by the resolver; broken chain/corrupt record/
conflicting materialized bytes are invariant failures. This is the price of replacing unavailable
multi-key atomic commit with a single-key head CAS.

Object reference repair:

```text
repairObjectReferences(cluster, objectId)
```

Algorithm:

1. Read `ObjectManifestRecord`. If missing, fail with `METADATA_INVARIANT_VIOLATION` for repair callers.
2. For each manifest slice, read the stream head for that slice's stream.
3. Walk the committed head chain until a record matching the manifest slice's stream id, object id,
   slice id, writer epoch, checksum, physical range, record count, entry count, logical bytes, payload
   format, and schema refs is found, or until the chain proves it is not committed.
4. If committed, materialize and validate the matching offset index and committed-slice marker.
5. Build `VisibleSliceReferenceRecord` only for validated visible slices.
6. CAS-merge the rebuilt reference list into `/objects/{objectIdComponent}/references`.

Repair must not use object-store list and must not create visibility. It only rebuilds object-scoped audit
metadata from stream-head commit truth and derived read indexes.

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
head.state in [ACTIVE, SEALED]
beforeOffset >= 0
beforeOffset >= head.trimOffset
beforeOffset <= head.committedEndOffset
```

Writes:

```text
putIfVersion(/streams/{streamId}/head, head with trimOffset=beforeOffset, previousHead.metadataVersion)
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
- fake watch tests should simulate missed, duplicate, collapsed, reconnect, and out-of-order events。

## 12. Failure Semantics

| Failure | Metadata outcome | Caller behavior |
| --- | --- | --- |
| Oxia unavailable before object upload | no metadata commit | retry append later |
| object upload succeeds, manifest put fails | orphan by object store naming; no visibility | retry manifest or GC after TTL |
| manifest succeeds, slice commit fails due to stale token | manifest remains uploaded; no visibility | reacquire session and retry or abandon |
| timeout after head CAS is sent | commit may or may not have succeeded; derived indexes may lag | retry same physical slice can use head chain or committed-slice marker; caller-level retry may duplicate |
| head CAS succeeds but derived index materialization times out | stream head is committed; normal read may need repair | retry same physical slice should finish materialization before ack |
| slice commit succeeds, ack response lost | data visible after derived index materialization; committed-slice marker or head chain exists | retry can return idempotent commit result |
| slice commit succeeds, object reference update fails | data visible; object reference stale | repair object references from head chain and materialized offset index |
| offset conflict | no new index entry | refresh committedEndOffset and retry |
| checksum mismatch | no index entry | fail non-retriably and quarantine object |

## 13. Fake Metadata Store for Tests

Before real Oxia integration, implement an in-memory store with the same operation-level semantics:

```text
io.nereus.metadata.oxia.testing.FakeOxiaMetadataStore
```

It must support:

- linearizable synchronized stream-head CAS；
- CAS condition failures；
- metadata versions；
- session expiration through injected clock；
- required partition key recording and validation；
- scan ordering identical to production key encoding；
- watch registration and configurable missed/duplicate/collapsed/reconnect/out-of-order notifications；
- failure injection before commit-log put, after commit-log put, before head CAS, after head CAS before
  derived-index materialization, and after derived-index materialization before response。
- no object deletion behavior; fake GC tests must assert deletion is outside Phase 1。

The fake store should not expose test-only shortcuts to `nereus-core` production code.
Test-only controls may exist on the fake instance used by tests, but production code must interact only
through `OxiaMetadataStore`.

Fake/real parity requirements:

- every successful write increments the affected key's `metadataVersion` monotonically；
- every successful head CAS increments the stream `commitVersion` exactly once and stores the same durable
  value in `StreamHeadRecord`, `StreamCommitRecord`, `OffsetIndexRecord`, and `CommittedSliceRecord` when
  the derived records are materialized；
- condition failure before head CAS must not advance stream head；
- failure after head CAS must leave a repairable committed stream head and must not be reported as known
  uncommitted；
- same physical slice replay must first use the materialized committed-slice marker when present, then
  fall back to the stream-head commit chain；
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

Current implementation:

- `Phase1MetadataCodecs` registers every Phase 1 metadata record type and wraps record payloads in
  `MetadataRecordEnvelope` with `payloadEncoding=binary-v1`；
- payload encoding is deterministic for currently supported field kinds: strings, longs, ints, booleans,
  byte arrays, lists, `Map<String,String>`, and nested Java records；
- map entries are encoded sorted by UTF-8 key bytes；
- decode rejects malformed UTF-8 for envelope strings and payload string fields；
- Java record simple names and component names are part of the encoded bytes. The current golden-byte
  tests treat those names as schema for the Phase 1 record set.

Rules:

- Phase 1 must use one shared metadata value encoding for production records. `Phase1MetadataCodecs`
  is now the current M2 binary-v1 codec for the Phase 1 record set, but the metadata layer is not M2-exit
  complete until the fake store, future real adapter, and repair tools all use the same
  `MetadataCodecRegistry`；
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
- decode must reject malformed UTF-8 instead of relying on Java replacement-character decoding；
- adding optional fields requires defaults and a schema-version test；
- changing field meaning requires a new record type or an explicit migration plan；
- codec tests must include golden bytes for each Phase 1 record type before the implementation is treated
  as frozen；
- key encoding version and value schema version are separate concerns. Fixed-width offset/generation key
  encoding must not change without a keyspace migration document。
