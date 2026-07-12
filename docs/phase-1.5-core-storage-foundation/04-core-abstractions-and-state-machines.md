# Core Abstractions and State Machines

> 状态：Implemented；P15-M3/P15-M4 core contract final-gated on 2026-07-11

本文定义 Phase 1.5 如何在不改变 Object WAL v1 bytes 和 strict success 语义的前提下，把 current
`DefaultStreamStorage` 从直接依赖 `WalObjectWriter/WalObjectReader` 改成 target-aware L0 core。

## 1. Package and Dependency Boundary

Target production files：

```text
nereus-api/com.nereusstream.api.target/
  ReadTarget.java
  ReadTargetType.java
  ObjectSliceReadTarget.java
  BookKeeperEntryRangeReadTarget.java
  BookKeeperEntryMapping.java

nereus-core/com.nereusstream.core.profile/
  StorageProfileResolver.java
  StorageExecutionPlan.java
  ObjectPublicationMode.java

nereus-core/com.nereusstream.core.wal/
  PrimaryWalAppender.java
  PrimaryWalReader.java
  PrimaryWalRegistry.java
  PrimaryAppendRequest.java
  PreparedPrimaryAppend.java
  DurablePrimaryAppend.java
  ProviderCommitEvidence.java

nereus-core/com.nereusstream.core.wal.object/
  ObjectWalAppenderAdapter.java
  ObjectWalReaderAdapter.java
  ObjectPreparedPrimaryAppend.java
  ObjectWalCommitEvidence.java

nereus-core/com.nereusstream.core.append/
  AppendCoordinator.java
  AppendDeadline.java
  AppendResourceLimiter.java
  AppendSessionManager.java
  StableAppendCommitter.java
  MetadataStableAppendCommitter.java
  GenerationZeroIndexMaterializer.java
  MetadataGenerationZeroIndexMaterializer.java

nereus-core/com.nereusstream.core.lifecycle/
  StreamLifecycleCoordinator.java

nereus-core/com.nereusstream.core.read/
  ReadTargetDispatcher.java
```

`nereus-object-store` keeps the implemented WAL writer/reader and binary format。The object adapters live in core
because they translate between core domain and that module。A later BookKeeper implementation belongs in a separate
module (target name `nereus-bookkeeper-wal`) and registers through the interfaces；Phase 1.5 L0 modules do not add
`org.apache.bookkeeper` dependencies。

The list above is the implemented class layout。`AppendCoordinator` deliberately contains the stream lane、retained
physical attempt、terminal cache and recovery single-flight state as private implementation types；the superseded
design names `StrictAppendCoordinator`、`AppendRecoveryCoordinator`、`RetainedAppendAttempt`、
`RetainedAppendRegistry` and `StreamMutationLaneRegistry` must not be recreated as parallel production owners。

## 2. Profile Execution Plan

```java
public enum ObjectPublicationMode {
    DISABLED,
    SYNCHRONOUS,
    ASYNCHRONOUS
}

public record StorageExecutionPlan(
        StorageProfile profile,
        ReadTargetType primaryTargetType,
        ObjectPublicationMode publicationMode,
        DurabilityLevel allowedDurability) {
}

public interface StorageProfileResolver {
    StorageExecutionPlan requireExecutable(
            StorageProfile profile,
            DurabilityLevel requestedDurability,
            boolean primaryAppenderInstalled,
            boolean primaryReaderInstalled);
}
```

P15-M5 executable table：

| Profile | Primary adapter | Publication | Allowed public durability |
| --- | --- | --- | --- |
| `OBJECT_WAL_SYNC_OBJECT` | object | synchronous generation zero | `WAL_DURABLE_AND_INDEX_COMMITTED` |
| all BookKeeper profiles | none | reserved | reject before resource/WAL IO |
| `OBJECT_WAL_ASYNC_OBJECT` | object reader/writer exists but async coordinator absent | reserved | reject before IO |

Canonicalization of deprecated `OBJECT_WAL` happens before lookup。The presence of a target codec or reader alone does
not enable a profile；profile resolver, appender, reader and completion-policy bindings must all exist。

## 3. Primary WAL Append Boundary

```java
public record PrimaryAppendRequest(
        StreamId streamId,
        AppendBatch batch,
        AppendSession session,
        long expectedStartOffset,
        AppendAttemptId attemptId,
        Duration timeout) {
}

public interface PreparedPrimaryAppend {
    StreamId streamId();
    int recordCount();
    int entryCount();
    long logicalBytes();
    long reservedBytes();
}

public record DurablePrimaryAppend(
        StreamId streamId,
        ReadTarget readTarget,
        PayloadFormat payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        ProviderCommitEvidence providerCommitEvidence) {
}

public interface PrimaryWalAppender<P extends PreparedPrimaryAppend> {
    ReadTargetType targetType();
    P prepare(PrimaryAppendRequest request);
    CompletableFuture<DurablePrimaryAppend> persist(P prepared, Duration timeout);
    CompletableFuture<Void> validateBeforeHeadCommit(
            DurablePrimaryAppend append,
            AppendSession session,
            Duration timeout);
}
```

`providerCommitEvidence` is not serialized or exposed and may only be down-cast by the same registered provider。The
implementation uses the typed `ProviderCommitEvidence` interface and `ObjectWalCommitEvidence`；because the project
currently builds as an unnamed Java module, the interface is not Java-`sealed`, while registry/type checks still fail
closed。A provider cannot change any logical field between prepare and persist。

`validateBeforeHeadCommit` is the provider-specific durable-target validation seam。For the current Object path,
`AppendCoordinator` writes/compares the exact manifest immediately before this call and the adapter then validates
the typed `ObjectWalCommitEvidence`；a future BookKeeper adapter validates its ledger/entry range here。Failure is
`KNOWN_NOT_COMMITTED` because no head request has been sent, although orphan durable bytes may remain。

## 4. Object WAL Adapter

`ObjectWalAppenderAdapter`：

1. maps one current core work item to `WalWriteRequest`；
2. delegates exact size/layout/checksum calculation to `DefaultWalObjectWriter.prepare`；
3. adjusts the generic append-buffer reservation to exact object length；
4. delegates immutable upload；
5. constructs one `ObjectSliceReadTarget` from `WalWriteResult + WrittenStreamSlice`；
6. stores the full `WalWriteResult` as sealed provider evidence；
7. `AppendCoordinator.uploadAndCommit` builds and put/compares the existing `ObjectManifestRecord` before invoking
   the adapter's `validateBeforeHeadCommit` hook。

No WAL magic、section、footer、checksum or object-key change occurs。Existing direct object-store tests and golden
bytes remain authoritative。Manifest/object/slice mismatch remains an object-specific invariant/error and cannot be
normalized into a generic successful target。

## 5. Stable Commit and Strict Materialization Split

```java
public interface StableAppendCommitter {
    CompletableFuture<StableAppendResult> commit(CommitAppendRequest request);
}

public interface GenerationZeroIndexMaterializer {
    CompletableFuture<CommittedAppend> materialize(ReachableCommittedAppend reachableAppend);
}
```

Phase 1.5 strict pipeline：

```text
validate executable plan
  -> reserve retained-attempt and generic buffer capacity
  -> session + expected offset
  -> PrimaryWalAppender.prepare
  -> persist primary bytes
  -> validate provider target/manifest
  -> StableAppendCommitter.commit              // intent + head CAS
  -> GenerationZeroIndexMaterializer.materialize
  -> construct generic AppendResult
  -> return strict success
```

`StableAppendCommitter` never calls a writer/reader and never knows object/BK fields。It builds commit identity from
logical fields plus encoded target identity。`GenerationZeroIndexMaterializer` never advances head and can be retried
from the reachable commit alone。

Failure classification：

| Boundary | Outcome |
| --- | --- |
| before head request | `KNOWN_NOT_COMMITTED`, no public attempt ID |
| head request submitted, response unavailable | `MAY_HAVE_COMMITTED` + attempt ID |
| head reachable, materialization/result fails | `KNOWN_COMMITTED` + attempt ID |
| strict result returned | normal success and terminal attempt cached |

Although the internal stable boundary now exists, `StorageProfileResolver` rejects public
`WAL_DURABLE` before IO。Enabling it requires Future 4/task repair or a separately accepted primary-index repair SLA,
not a one-line early return。

## 6. Shared Stream Mutation Lane

`AppendCoordinator` stores one private `StreamLane` per locally retained stream。A lane owns：

- expected committed end cache；
- FIFO mutation tail for append/seal/delete barriers；
- an optional suspended failure/fence tied to the unresolved physical attempt kept in the coordinator's
  `retainedAttempts` map；
- lifecycle admission barrier and retain count；
- permanent local fence when exact recovery finds corruption/invariant failure。

The lane is removed only when its retain count reaches zero and no suspended failure remains。A bounded resident-lane
test remains part of the Phase 1 post-M8 memory gate。Append/session caches remain distinct；removing a lane cannot
discard an unresolved attempt or make a durable session authoritative locally。

The existing `AppendCoordinator` class name is retained for source compatibility, but its only production path is the
strict Phase 1.5 pipeline through `PrimaryWalRegistry`、`ObjectWalAppenderAdapter`、`StableAppendCommitter` and
`GenerationZeroIndexMaterializer`。Its retained-attempt registry and mutation-lane implementation are encapsulated in
the same coordinator；there is no second legacy production append path。

## 7. Primary Read Boundary

```java
public interface PrimaryWalReader {
    ReadTargetType targetType();
    long reservationBytes(ResolvedRange range);
    CompletableFuture<WalReadResult> readWithStats(
            long startOffset,
            List<ResolvedRange> ranges,
            ReadOptions options);
}
```

`ReadTargetDispatcher` partitions a logical contiguous resolution into maximal adjacent runs handled by the same
reader type, while preserving logical order and one caller record/byte budget across runs。This is required for
future profile migration/mixed historical ranges；it cannot assume one stream has one target type forever。

Phase 1.5 registry contains only `ObjectWalReaderAdapter`。It validates every target is
`ObjectSliceReadTarget`, converts to current reader inputs and delegates checksum/index decoding；the existing
`WalReadResult` carries batches plus per-slice stats back to generic accounting。A BookKeeper target reaches no object
method and fails before resource reservation with `UNSUPPORTED_READ_TARGET`。

## 8. Resolve and Cache Changes

`ReadResolver` consumes `OffsetIndexEntry` and produces `ResolvedRange` without object branching。Selection remains：

```text
cover requested offset
  -> ignore tombstoned/unsupported generation values
  -> choose highest valid visible generation
  -> repair missing generation zero from reachable commit
```

Phase 1.5 accepts only generation zero。A decoded generation > 0 is not silently skipped when it is the highest
published target；until Future 4 reader semantics exist it fails `UNSUPPORTED_READ_TARGET`/format as appropriate so a
newer physical truth is not ignored accidentally。

Positive cache identity includes：

```text
streamId + logical range + generation + commitVersion
+ ReadTargetRecord.identityChecksumValue
```

Target read/not-found/checksum failures from a cached resolution invalidate and perform one fresh metadata resolve,
not only current `OBJECT_*` failures。Fresh equality compares canonical target identity, not Java object reference or
object key alone。

## 9. Generic Resource Accounting

Rename core-wide limits to describe the resource rather than the current provider：

```text
maxConcurrentPrimaryReads
maxPrimaryReadBufferBytes
maxPrimaryAppendBytes
```

Object-specific `maxObjectBytes` remains because Object WAL encoding has that exact hard limit and must be
`<= maxPrimaryAppendBytes <= maxBufferedBytes` and `<= maxPrimaryReadBufferBytes` for the current all-buffered reader。
Deprecated accessor methods for `maxConcurrentObjectReads` and `maxReadBufferBytes` may delegate during one source
compatibility cycle；new construction uses generic names。

The reader estimates reservation from target-specific physical ranges with checked arithmetic。Core enforces the
generic semaphore/byte pool before calling the adapter, then reconciles reported physical bytes。A provider
underestimate beyond its declared bound is invariant failure；metrics-only counters saturate but correctness limits do
not。

## 10. StreamStorageConfig Evolution

Target additions：

```text
processRunId
appendRecoveryAttemptTimeout
appendRecoveryBackoffMin
appendRecoveryBackoffMax
appendRecoveryTerminalTtl
maxRetainedAppendAttempts
maxAppendRecoveryTerminals
maxConcurrentPrimaryReads
maxPrimaryReadBufferBytes
maxPrimaryAppendBytes
```

Rules：

- process ID is nonblank/bounded and supplied by production runtime；tests/default factory generate at least 128
  random bits；
- `writerRunIdHash` is deterministically derived from `processRunId` instead of a second hidden core random value；
- recovery durations are positive, min <= max and each attempt timeout <= append recovery caller budget；
- terminal capacity is at least retained-attempt capacity；
- `shutdownGrace` covers one admitted append plus one caller-visible recovery budget in F2 construction；
- every size/count relation uses checked arithmetic。

An overloaded compatibility factory may generate `processRunId` for Phase 1 tests, but production constructors must
receive the explicit runtime identity。Configuration values and secrets never enter durable target payloads except
the hashed writer-run identity already required by commit/object identity。

## 11. Construction and Ownership

Target constructor shape：

```java
public DefaultStreamStorage(
        StreamStorageConfig config,
        OxiaMetadataStore metadataStore,
        PrimaryWalRegistry primaryWalRegistry,
        ScheduledExecutorService recoveryScheduler,
        Clock clock,
        Executor callbackExecutor,
        ReadMetricsObserver readObserver,
        TrimMetricsObserver trimObserver,
        RecoveryMetricsObserver recoveryObserver) {
}
```

`NereusCore` supplies an Object-WAL convenience builder that wraps the current writer/reader into a registry。The
caller owns injected metadata/WAL clients/scheduler/executor unless an explicit owning runtime factory says otherwise；
`DefaultStreamStorage.close()` stops its coordinators and adapter admission but does not double-close borrowed
resources。F2 `NereusManagedLedgerRuntime` remains the final dependency-order owner。

## 12. Observability

Minimum new dimensions：

- primary target type on WAL write/read stage latency；
- stable-head commit latency separated from generation-zero materialization；
- retained attempt count/capacity rejection/age；
- recovery pages, scanned commits, continuation progress, terminals and backoff；
- lifecycle transitions/conflicts/resumes；
- read bytes/reservation/amplification by target type；
- legacy/new metadata decode counts and target-adapter selection failures。

Object adapter retains current object-WAL metrics。No BookKeeper metric is emitted without a BookKeeper adapter；
generic metrics do not relabel object operations as bookie operations。

## 13. Core Compatibility Tests

P15-M3/M4 require：

- existing Object WAL append/read/trim tests pass through the registry, not direct coordinator dependencies；
- byte-for-byte Object WAL/object-key/checksum golden parity；
- profile/durability rejection occurs before prepare, permit or provider IO；
- stable commit and materializer failure injection at every boundary preserves exact outcomes；
- generic result contains the same logical/object target information as the old result without fake fields；
- mixed target dispatcher never sends a target to the wrong reader and preserves one caller budget；
- cache refresh compares target identity and handles provider-neutral errors；
- resource reservation overflow/underestimate fails closed；
- lane/attempt/cache/scheduler resident counts remain bounded after success, failure, timeout and close；
- dependency guards prove no BookKeeper/Pulsar/Kafka classes enter L0 modules；
- Phase 1 ordinary and Docker final gates remain green unchanged。
