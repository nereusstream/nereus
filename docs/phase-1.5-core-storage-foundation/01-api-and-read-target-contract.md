# API and Read-target Contract

> 状态：Implemented；P15-M1/P15-M4 API contract final-gated on 2026-07-11

本文冻结 Phase 1.5 的 protocol-neutral public surface。示例是 target Java shape，省略 license、import 和
method body。任何实现差异都必须先更新本文并重新审查 F2/F4 handoff。

## 1. Current Gaps

Current Phase 1 values encode an object slice in common result fields：

- `AppendResult.objectId/objectKey/sliceId/objectOffset/objectLength/...`；
- `ResolveResult.ranges: List<ResolvedObjectRange>`；
- `EntryIndexRef` sits beside the result even though it is part of object-slice decoding；
- error/result validation assumes every physical target is an object。

Putting a BookKeeper ledger ID into `ObjectId` or fabricating an `ObjectKey` is forbidden。The Phase 1.5 target replaces the
common physical fields with one tagged `ReadTarget` while retaining logical range/payload/projection fields outside
the target。

## 2. ReadTarget Model

Target package：`nereus-api/com.nereusstream.api.target`。

P15-M1 adds `ApiLimits.MAX_READ_TARGET_ENCODED_BYTES = 64 * 1024` and
`ApiLimits.MAX_APPEND_ATTEMPT_ID_ENCODED_BYTES = 256`。The first limit covers the complete canonical target payload,
including any inline entry index；the second covers strict UTF-8 attempt-ID bytes。Limits are checked before durable
metadata or buffer allocation。

```java
public enum ReadTargetType {
    OBJECT_SLICE,
    BOOKKEEPER_ENTRY_RANGE
}

public sealed interface ReadTarget
        permits ObjectSliceReadTarget, BookKeeperEntryRangeReadTarget {
    ReadTargetType type();
    int version();
}
```

The sealed Java type set is not the runtime support set。`PrimaryWalRegistry` determines executable adapters；a
valid decoded BookKeeper target with no registered reader fails `UNSUPPORTED_READ_TARGET` and never falls through to
an object reader。

### 2.1 ObjectSliceReadTarget

```java
public record ObjectSliceReadTarget(
        int version,
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectType objectType,
        String physicalFormat,
        String logicalFormat,
        String sliceId,
        long objectOffset,
        long objectLength,
        Checksum sliceChecksum,
        EntryIndexRef entryIndexRef) implements ReadTarget {

    @Override
    public ReadTargetType type() {
        return ReadTargetType.OBJECT_SLICE;
    }
}
```

Validation：

- `version == 1` in Phase 1.5；
- IDs/keys/formats/slice ID are non-null, strict UTF-8, nonblank and bounded by the matching existing API limit；
- `objectOffset >= 0`、`objectLength > 0` and checked addition does not overflow；
- `sliceChecksum` validates the selected bytes and participates in target identity；
- `entryIndexRef` follows its existing `INLINE/OBJECT_FOOTER/INDEX_OBJECT` shape rules；
- an `OBJECT_FOOTER` reference with empty object ID/key means the same object carried by this target；if present,
  both must equal the target object identity。A different index object uses `INDEX_OBJECT`。

The aggregate canonical/storage object checksums remain in `ObjectManifestRecord` and sealed Object-WAL provider
evidence。They are validated before head commit and used for audit/recovery, but are not needed to read one selected
slice and therefore are not mandatory fields of the cross-provider `ReadTarget`。This lets a frozen legacy
`OffsetIndexRecord` hydrate the target without adding a manifest read to every resolve。

Object WAL v1 creates `objectType=MULTI_STREAM_WAL_OBJECT`、`physicalFormat=NEREUS_WAL_V1` and
`logicalFormat=NEREUS_STREAM_RECORD_V1`。Future 4 may use the same target class with
`STREAM_COMPACTED_OBJECT` and its own versioned format；that does not make such objects readable in Phase 1.5。

### 2.2 BookKeeperEntryRangeReadTarget

```java
public enum BookKeeperEntryMapping {
    ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY
}

public record BookKeeperEntryRangeReadTarget(
        int version,
        String clusterAlias,
        long ledgerId,
        long firstEntryId,
        int entryCount,
        BookKeeperEntryMapping entryMapping,
        Checksum rangeChecksum) implements ReadTarget {

    @Override
    public ReadTargetType type() {
        return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
    }

    public long lastEntryIdInclusive();
}
```

Validation：

- `version == 1`；cluster alias is canonical, nonblank and bounded；
- ledger/first-entry IDs are non-negative；`entryCount > 0`；
- `firstEntryId + entryCount - 1` uses checked arithmetic；
- Phase 1.5 reserves only one-to-one entry mapping；a different framing requires target version 2 or a new type；
- the canonical SHA-256 `rangeChecksum` covers the ordered logical payload bytes plus entry boundaries, not
  BookKeeper ensemble metadata or a client digest password。

Ledger ensemble/quorum/digest metadata remains authoritative in the configured BookKeeper metadata plane and is
looked up by the future adapter。Credentials and bookie addresses are never copied into stream commit records。
P15-M1/M2 implement this value and durable codec to freeze the union shape, but do not add BookKeeper libraries or
register a reader/writer。

## 3. Generic AppendResult

`AppendResult` becomes：

```java
public record AppendResult(
        StreamId streamId,
        OffsetRange range,
        long committedEndOffset,
        long generation,
        ReadTarget readTarget,
        PayloadFormat payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        Optional<ProjectionRef> projectionRef,
        long commitVersion) {
}
```

Common validation：

- `committedEndOffset == range.endOffset()`；
- `generation == 0` for an append result；higher generations are never a new append result；
- `readTarget` is the durable primary target recorded by that commit；
- positive record/entry counts and `recordCount == range.recordCount()`；
- payload/count/schema/projection validation remains identical to Phase 1；
- `commitVersion > 0` for every visible append。

The public result deliberately omits object-only convenience fields。Callers that truly need physical details use a
total type switch over `readTarget`。F2 does not switch；it derives Position only from the logical range。

## 4. Generic Resolve Result

```java
public record ResolvedRange(
        OffsetRange offsetRange,
        long generation,
        ReadTarget readTarget,
        PayloadFormat payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        Optional<ProjectionRef> projectionRef,
        long commitVersion) {
}

public record ResolveResult(
        StreamId streamId,
        long requestedOffset,
        List<ResolvedRange> ranges,
        long resolvedEndOffset,
        long metadataVersion) {
}
```

`ResolvedRange` validation requires a dense positive logical range, generation/commit version consistency and
canonical schema/projection values。The target owns physical range/checksum/index details；common resolver code must
not branch on object fields while selecting the highest generation。

`ResolvedObjectRange` remains for one source-compatibility cycle as `@Deprecated(forRemoval=true)` with an explicit
`from(ResolvedRange)` conversion that succeeds only for `ObjectSliceReadTarget`。Core APIs never return it and there
is no conversion from a BookKeeper target。It is removed only after all repository callers and released consumers
have migrated；no fake-object conversion is permitted。

## 5. StreamStorage Additions

```java
public record AppendAttemptId(String value) {
}

public record AppendRecoveryOptions(Duration timeout) {
}

public record SealOptions(Duration timeout, String reason) {
}

public record DeleteOptions(Duration timeout, String reason) {
}

public interface StreamStorage extends AutoCloseable {
    // Existing methods remain.

    CompletableFuture<AppendResult> recoverAppend(
            StreamId streamId,
            AppendAttemptId attemptId,
            AppendRecoveryOptions options);

    CompletableFuture<StreamMetadata> seal(
            StreamId streamId,
            SealOptions options);

    CompletableFuture<StreamMetadata> delete(
            StreamId streamId,
            DeleteOptions options);
}
```

Value rules：

- attempt ID is opaque, nonblank strict UTF-8 and bounded by
  `ApiLimits.MAX_APPEND_ATTEMPT_ID_ENCODED_BYTES`；callers cannot construct one from a
  producer identifier and use it as durable dedup；
- every duration is positive；
- lifecycle reason is diagnostic only, nonblank strict UTF-8 and bounded by
  `ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES`；it is not persisted as state authority or used as idempotency key；
- all operational failures complete the future exceptionally；value-construction errors may throw
  `IllegalArgumentException` as existing API values do。

## 6. NereusException Append Identity

`NereusException` adds：

```java
public Optional<AppendAttemptId> appendAttemptId();
```

Constructor/factory validation enforces：

| Append state | Outcome | Attempt ID |
| --- | --- | --- |
| no head request was submitted | `KNOWN_NOT_COMMITTED` | empty |
| head request submitted, response uncertain | `MAY_HAVE_COMMITTED` | required |
| head known reachable but result/index failed | `KNOWN_COMMITTED` | required |
| recovery completely proves absence | `KNOWN_NOT_COMMITTED` | empty in terminal returned failure |
| non-append failure | empty outcome | empty |

The `StreamStorage` boundary never exposes `MAY_HAVE_COMMITTED`/`KNOWN_COMMITTED` without an ID, or an attempt ID
without an append outcome。Metadata/provider-internal compatibility exceptions may begin without a process-local ID；
`AppendCoordinator` attaches the retained exact ID before the exception reaches the public future。Normalization
retains both fields while unwrapping async exceptions。

The Phase 1.5 target adds provider-neutral codes：

```text
PRIMARY_WAL_WRITE_FAILED
PRIMARY_WAL_READ_FAILED
PRIMARY_WAL_TARGET_NOT_FOUND
PRIMARY_WAL_CHECKSUM_MISMATCH
UNSUPPORTED_READ_TARGET
```

Existing `OBJECT_*` codes remain and Object WAL continues emitting them where the failure is specifically object IO,
preserving Phase 1/F2 mapping。Generic coordinator code uses provider-neutral codes only when no more specific adapter
classification exists。

## 7. F2 Consumer Contract

F2 validates only：

- stream ID、range length、committed end、generation zero and commit version；
- payload format、record/entry counts、logical bytes、schema/projection values；
- current projection incarnation/lifecycle。

F2 never reads `ObjectId`、`ObjectKey`、BookKeeper IDs or entry-index fields from an append/resolve result to create a
`Position`。The first F2 runtime still opens only streams whose profile is `OBJECT_WAL_SYNC_OBJECT`; this admission
rule is independent from its consumption of the generic result。

## 8. API Tests

P15-M1 requires：

- construction/property tests for both target types at zero/max/overflow boundaries；
- exhaustive sealed-type switch tests；
- generic append/resolve validation and immutable list/byte-array tests；
- legacy object-range conversion success and every non-object rejection；
- lifecycle/recovery option UTF-8, duration and size limits；
- a dependency guard proving `nereus-api` contains no ObjectStore, BookKeeper, Pulsar or Kafka client type；
- F2 compile fixtures proving Position mapping does not inspect a physical target。

P15-M4 adds the `NereusException` outcome/attempt-ID combination matrix and method-level async validation for
`recoverAppend/seal/delete` when those methods become executable。
