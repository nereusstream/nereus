# Metadata Schema and Compatibility

> 状态：Implemented；P15-M2/P15-M3 dual-read/new-write contract final-gated on 2026-07-11
> Compatibility rule：legacy Phase 1 golden bytes stay frozen；new commits use generic target records
> F4 evolution：2026-07-15 checkpoint B replaced the combined raw stable-commit call with protected two-stage commit

本文定义 physical target 如何进入 commit identity、generation-zero index 和 replay marker，同时允许同一
stream head 链接 Phase 1 legacy object records 与 Phase 1.5 generic-target records。

## 1. Authority and Unchanged State

The following remain unchanged：

- stream-name key and deterministic stream ID；
- `/streams/{streamId}/head` key and `StreamHeadRecord` binary-v1 value；
- head-CAS append linearization point；
- commit-log key shape and `lastCommitId/previousCommitId` chain；
- offset-index key shape `{offsetEnd19}/{generation19}` and ordering；
- object manifest/reference keys and Object WAL v1 bytes；
- append/session/trim partition keys。

`StreamHeadRecord` contains no object-specific location, so a mixed legacy/new commit chain needs no head migration。
Logical truth remains the head plus the reachable chain；value record type is not a second truth。

## 2. Keyspace Additions

Readable forms：

```text
/nereus/clusters/{cluster}/streams/{streamId}/commit-log/{commitId}
/nereus/clusters/{cluster}/streams/{streamId}/offset-index/{offsetEnd19}/{generation19}
/nereus/clusters/{cluster}/streams/{streamId}/committed-appends/{commitId}
```

The first two keys already exist and may contain either legacy or generic values。The third is the generic replay
marker added by Phase 1.5。It uses `PartitionKey(streamId)` and exact canonical `commitId` bytes。

Legacy marker keys remain readable：

```text
/streams/{streamId}/committed-slices/{objectId}/{sliceHash}
```

New append attempts never write `committed-slices`。Repair of a legacy commit may validate or recreate its legacy
marker；repair of a generic commit writes only `committed-appends`。No correctness path lists both families to guess
commit state；it computes the exact expected key from the decoded commit/request version。

## 3. ReadTargetRecord

Durable generic records do not persist a Java sealed-interface implementation through reflection。They embed one
stable primitive record：

```java
public record ReadTargetRecord(
        String targetType,
        int targetVersion,
        String payloadEncoding,
        byte[] payload,
        String identityChecksumType,
        String identityChecksumValue) {
}
```

Phase 1.5 values：

- `targetType`: `OBJECT_SLICE` or `BOOKKEEPER_ENTRY_RANGE`；
- `targetVersion == 1`；
- `payloadEncoding == "canonical-target-v1"`；
- payload is non-empty and defensively copied；
- payload length is at most `ApiLimits.MAX_READ_TARGET_ENCODED_BYTES`；
- identity checksum is `SHA256` over
  `strictUtf8(targetType) + 0x00 + asciiDecimal(targetVersion) + 0x00 + payload`。

`ReadTargetCodecRegistry` maps `(targetType,targetVersion,payloadEncoding)` to a concrete codec。Unknown target type,
unsupported required version, checksum mismatch, malformed UTF-8, trailing bytes or numeric overflow is
`METADATA_INVARIANT_VIOLATION`。A known valid target without a runtime IO adapter is not corruption；read execution
fails `UNSUPPORTED_READ_TARGET`。

### 3.1 Canonical target payload primitives

`canonical-target-v1` uses big-endian fixed-width numeric values：

| Primitive | Encoding |
| --- | --- |
| magic | four ASCII bytes |
| string | unsigned 32-bit byte length + strict UTF-8 bytes |
| byte array | unsigned 32-bit length + exact bytes |
| long | signed 64-bit big-endian; owning field validation supplies non-negative constraint |
| int | signed 32-bit big-endian; owning field validation supplies positive constraint |
| enum/checksum type | canonical enum name encoded as string |

Lengths greater than `Integer.MAX_VALUE`, truncated input and trailing bytes are rejected。There is no platform
charset, Java serialization, JSON map ordering or reflection-dependent field name in this payload。

Object target payload v1 uses magic `NRO1` and fields in this exact order：

```text
objectId
objectKey
objectType
physicalFormat
logicalFormat
sliceId
objectOffset
objectLength
sliceChecksumType
sliceChecksumValue
entryIndex.location
entryIndex.objectId-or-empty
entryIndex.objectKey-or-empty
entryIndex.inlineData
entryIndex.offset
entryIndex.length
entryIndex.checksumType
entryIndex.checksumValue
```

BookKeeper target payload v1 uses magic `NRB1` and fields：

```text
clusterAlias
ledgerId
firstEntryId
entryCount
entryMapping
rangeChecksumType
rangeChecksumValue
```

Every codec has durable full-payload and checksum golden vectors。Changing field order/meaning requires target
version 2；adding an optional trailing field to v1 is forbidden because trailing bytes fail closed。

## 4. New Record Families

New Java record simple names are durable `MetadataRecordEnvelope.recordType` values。They start at metadata schema
version 1 independently from the legacy record schema version。

### 4.1 StreamCommitTargetRecord

```java
public record StreamCommitTargetRecord(
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
        ReadTargetRecord readTarget,
        String payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        String projectionRef,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        long preparedAtMillis,
        long metadataVersion) {
}
```

It contains every immutable field needed to rebuild a generation-zero index and result。Object-specific manifest
validation happens before this record is proposed, but the record itself only carries `ReadTargetRecord`。

### 4.2 OffsetIndexTargetRecord

```java
public record OffsetIndexTargetRecord(
        String streamId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long cumulativeSize,
        ReadTargetRecord readTarget,
        String payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        String projectionRef,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        long commitVersion,
        boolean tombstoned,
        long metadataVersion) {
}
```

Generation zero is derived from one reachable commit。Future 4 may reuse this family for generation greater than
zero only after freezing publish/overlap/source-identity fields；Phase 1.5 must reject writing generation > 0 rather
than assuming the current record is already sufficient for F4。

### 4.3 CommittedAppendRecord

```java
public record CommittedAppendRecord(
        String streamId,
        String commitId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long commitVersion,
        String readTargetIdentitySha256,
        long metadataVersion) {
}
```

This is a fast replay marker。It is valid only when its commit is reachable from the current or a later append-only
head anchor。Marker presence does not independently make an orphan intent visible；replay validates its fields against
the requested identity and reachable commit rules before returning success。

## 5. Canonical In-memory Metadata Models

Core must not switch on raw durable Java record classes。`nereus-metadata-oxia` hydrates both generations into：

```java
public record CommittedAppend(
        CommitIdentity identity,
        ReadTarget readTarget,
        OffsetRange range,
        long cumulativeSize,
        long commitVersion,
        PayloadFormat payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        Optional<ProjectionRef> projectionRef,
        long minEventTimeMillis,
        long maxEventTimeMillis) {
}

public record OffsetIndexEntry(
        StreamId streamId,
        OffsetRange range,
        long generation,
        long cumulativeSize,
        ReadTarget readTarget,
        PayloadFormat payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        Optional<ProjectionRef> projectionRef,
        long commitVersion,
        boolean tombstoned,
        long metadataVersion) {
}
```

Core append/read/repair imports these models, not `StreamCommitRecord`、`StreamCommitTargetRecord`、
`OffsetIndexRecord` or `OffsetIndexTargetRecord`。

Metadata operations that have actually proved reachability return an opaque wrapper：

```java
public final class ReachableCommittedAppend {
    public CommittedAppend committedAppend();
    public String observedHeadCommitId();
    public long observedHeadOffsetEnd();
    public long observedHeadCumulativeSize();
    public long observedHeadCommitVersion();
    // no public constructor; fake/real adapter factories validate and create it
}
```

The wrapper is process-local proof produced only by successful head CAS/replay/repair chain validation。It is not
encoded, accepted from protocol code or reconstructed from object listing。A reachable commit stays reachable under
the append-only chain even if a later head or lifecycle state is observed。

## 6. New Commit Identity

Generic commits use：

```text
commitId = sha256-base32(
  "nereus-commit-v2\0"
  + canonical length-prefixed logical append identity
  + ReadTargetRecord.targetType
  + ReadTargetRecord.targetVersion
  + ReadTargetRecord.identityChecksumValue)
```

The logical identity includes the same writer/session、expected start、counts、payload/schema/projection and event-time
fields as Phase 1。It does not include previous commit ID、next cumulative size or commit version, preserving
same-physical-attempt identity across compatible renew/trim head changes。The `v2` domain separator prevents a
generic commit from aliasing a legacy object-shaped commit even when all physical values happen to match。

Replay of an existing generic intent requires exact equality of the decoded request identity plus current
head-derived predecessor/cumulative/commit-version fields。A legacy commit is compared with the legacy v1 algorithm；
algorithms are never mixed for one commit ID。

## 7. Metadata Store Surface

Target protocol-neutral metadata boundary：

```java
public record CommitAppendRequest(
        StreamId streamId,
        String writerId,
        String writerRunIdHash,
        long epoch,
        String fencingToken,
        long expectedStartOffset,
        ReadTarget readTarget,
        PayloadFormat payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        Optional<ProjectionRef> projectionRef) {
}

public record StableAppendResult(
        ReachableCommittedAppend reachableAppend,
        boolean headAdvancedByThisCall) {
}

public interface OxiaMetadataStore extends AutoCloseable {
    CompletableFuture<PreparedStableAppend> prepareStableAppend(
            String cluster, CommitAppendRequest request);

    CompletableFuture<StableAppendResult> commitPreparedStableAppend(
            String cluster,
            PreparedStableAppend prepared,
            ObjectProtectionIdentity protectionIdentity,
            long rootMetadataVersion,
            long rootLifecycleEpoch,
            long protectionMetadataVersion,
            Checksum protectionRecordSha256);

    CompletableFuture<MaterializedGenerationZero> materializeGenerationZero(
            String cluster, ReachableCommittedAppend reachableAppend);

    CompletableFuture<Void> revalidateMaterializedGenerationZero(
            String cluster, MaterializedGenerationZero materialized);

    CompletableFuture<AppendReplaySearchResult> searchAppendReplay(
            String cluster,
            CommitAppendRequest request,
            Optional<AppendReplayCursor> continuation,
            int maxCommitsToScan);

    CompletableFuture<DerivedIndexRepairResult> repairDerivedStreamIndexes(...);

    CompletableFuture<List<OffsetIndexEntry>> scanOffsetIndex(...);

    CompletableFuture<StreamMetadataSnapshot> transitionStreamState(
            String cluster, StreamStateTransitionRequest request);
}
```

Phase 1.5 originally exposed one combined stable-commit operation. F4-M4 checkpoint B replaced that raw surface with
`prepareStableAppend` plus `commitPreparedStableAppend` so a permanent physical-object protection can be established
before the head CAS. Prepare writes/reuses the intent and returns its exact key/version/value SHA while leaving the
head unchanged；commit reloads that intent and the exact ACTIVE root/`REACHABLE_APPEND` proof before CAS or replay.
`materializeGenerationZero` accepts only the adapter-produced reachable wrapper and now returns the exact durable
index identity；`revalidateMaterializedGenerationZero` is the owner check used while acquiring index-owned
`VISIBLE_GENERATION`. Neither method starts an unbounded reachability scan from a bare commit ID. The public strict
append path executes all cuts before success；the head remains the only logical append linearization point.

The legacy `commitStreamSlice` method remains deprecated compatibility surface for Phase 1 contract/integration
fixtures。No production core caller uses it；all new `DefaultStreamStorage` appends use intent preparation、protected
head commit、exact generation-zero materialization and visible-index protection. Removing the legacy method is
deferred to a separately announced compatibility break。

## 8. Dual-read / New-write Rules

At any commit-log or offset-index key, adapter decode first reads `MetadataRecordEnvelope.recordType`：

| Key family | Legacy type | New type |
| --- | --- | --- |
| commit log | `StreamCommitRecord` | `StreamCommitTargetRecord` |
| offset index | `OffsetIndexRecord` | `OffsetIndexTargetRecord` |
| replay marker | `CommittedSliceRecord` at legacy key | `CommittedAppendRecord` at new key |

Rules：

1. Phase 1 golden codecs/classes are not modified and remain in `Phase1MetadataCodecs`。
2. `MetadataRecordCodecFactory` creates explicit registries；`L0TargetMetadataCodecs` registers only the new types。
3. Adapter dispatch is by envelope type, not by trying decoders until one succeeds。
4. P15-M2 makes the generic write path testable while the production coordinator remains on the legacy path；the
   P15-M3 coordinator cutover makes every new production append write only generic target records/marker。
5. Existing values are decoded/hydrated in place；there is no startup full scan or required rewrite。
6. Legacy `OffsetIndexRecord` losslessly hydrates `ObjectSliceReadTarget` from its existing object/slice/format/index
   fields without a manifest lookup。Aggregate object-checksum/manifest validation remains in append and object-audit
   paths, not ordinary resolve。
7. A derived-index repair may create a new generic index from a legacy reachable commit when it can reconstruct
   the complete slice target from that commit。An already-present equal legacy index is accepted without
   replacement；a conflicting value is invariant failure。
8. Mixed legacy/new chain walking validates dense `(offsetEnd,cumulativeSize,commitVersion)` independently of value
   record type。
9. Old binaries do not understand new record types and therefore cannot safely write after the first P15-M3 generic
   production commit。
   P15-M5 requires the explicit one-way rollout protocol below；rollback means restore from a snapshot or use a
   purpose-built down-conversion tool, not restart an old binary against new values。

### 8.1 One-way rollout protocol

Phase 1 binaries do not know a durable “minimum reader” record and therefore cannot be fenced safely by adding one。
The first Phase 1.5 release uses a non-rolling data-plane cutover：

1. stop admission and drain every Phase 1 writer/reader process for the target cluster；
2. take and verify the supported Oxia/object metadata backup/snapshot；
3. deploy the complete P15-M5 binary to every process while writes remain stopped；
4. run the legacy fixture/read-repair probe against real cluster metadata；
5. resume admission；the first append may now write generic records；
6. prohibit an old binary from joining that cluster thereafter。

If Phase 1 has never been deployed for the cluster, steps 1/4 reduce to proving absence and running the fixture in
staging。A future rolling protocol needs a pre-existing fence understood by both old and new binaries or a separate
namespace migration；inventing a new capability key that old code ignores is not sufficient。

## 9. Object-specific Audit Boundary

For `ObjectSliceReadTarget`：

- stable commit requires the exact manifest/object/slice identity already validated before head CAS；
- generation-zero materialization creates the generic index/marker；
- object manifest visibility/reference updates remain repairable audit/GC state after head commit；
- failure of audit/reference repair cannot report logical rollback but is surfaced in metrics and repair queues。

`repairObjectReferences` and `MetadataOrphanObjectScanner` must decode both legacy commits and generic
`ObjectSliceReadTarget` commits while walking the same reachable chain。They match exact object ID/key/slice/range and
checksums from the manifest/target；a BookKeeper target is simply not a reference to the scanned object。Mixed-chain
scan budget/density rules remain unchanged, and every Phase 1.5 orphan assessment stays diagnostic only。

For a future BookKeeper target there is no object manifest。Its adapter must provide an equivalent durable-target
validation hook before head CAS and retention reference hook after commit；Phase 1.5 has no fake object record branch。

## 10. Codec and Compatibility Gates

P15-M2 cannot exit until fake and production Oxia tests prove：

- every legacy Phase 1 envelope hex remains byte-for-byte unchanged；
- new record and both target-payload golden vectors are stable；
- legacy-only, new-only and alternating mixed commit chains resolve/repair across restart；
- legacy index hydration is lossless without manifest/list IO；legacy append/audit still validates the exact manifest；
- new writes never emit a legacy commit/index/marker；
- wrong envelope type, target type/version/checksum, malformed UTF-8, trailing bytes and overflow fail closed；
- one missing derived index below head is repaired from either commit version without changing logical truth；
- new generic marker conflict and legacy marker conflict are separately detected；
- object-reference repair/orphan assessment finds generic object targets across a mixed chain and ignores non-object
  targets without classifying them as corruption；
- head/version/commit density validation is identical in fake and real adapters；
- old-binary rollout prohibition is documented in release output and tested with a fixture decoder that rejects new
  record types rather than silently misreading them。
- deployment tests/scripts prove all old processes are drained before the first generic write；P15-M3 code is not a
  separately deployable release before the P15-M5 bundle。
