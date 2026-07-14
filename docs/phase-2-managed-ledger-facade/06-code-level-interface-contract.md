# Code-level Interface Contract

本文是 Future 2 对锁定 Pulsar surface 的可实现合同。`04` 负责状态机；本文负责类、字段、方法、
返回值和 unsupported 行为。实现类对外暴露前，本文列出的每个方法都必须有测试；不能依赖未审计的
interface default，也不能用 `null`、空集合、零值或成功 callback 掩盖未实现语义。

## 1. Source Lock and Contract Labels

目标仍是 `nereusstream/pulsar@100d3ef0ff7c7da36d497453b141ddff6f34a9d3`。
除 `01` 已锁定的核心接口外，F2 直接实现或返回的 surface 还包括：

| File | Locked Git blob | Declared surface |
| --- | --- | ---: |
| `ReadOnlyCursor.java` | `016298cb108bb2f3abc3e9b9a48c0c45486dcfef` | 11 methods |
| `ReadOnlyManagedLedger.java` | `91b8f92eb637e6350c659f4a44df24800696c88b` | 4 methods |
| `Entry.java` | `24ea5c17c0d66c324e73b60d3c53bf991cd456fd` | 13 methods including defaults |
| `Position.java` | `d0d6d865c9558a7a02707c48fb19b09d7fc1014b` | 10 methods including defaults/bridge |
| `AsyncCallbacks.java` | `70db427afce4f811670dd018c6dd98d44230d4bc` | callback signatures |
| `ManagedLedgerException.java` | `1fa565d6ec788df22f5fe725afdaea73141e024a` | exception subclasses |
| `ManagedLedgerInfo.java` | `3e4e56187e5eed531a33e1d0cf8d9bab0c94116c` | admin DTO |
| `EntryCacheManager.java` | `816ccd4a3e459f296a7b5775bd3d48bcd163f463` | 11 methods |
| `EntryCache.java` | `b2ebf7430560cb37f420ff5cd21984ba04b00c96` | 8 methods |
| `ManagedLedgerFactoryMXBean.java` | `43e8196daa9ae4cb1e4299abe2b5f3425009445c` | 16 methods |
| `ManagedLedgerMXBean.java` | `1d978e23785690bd17c2f0a9a898397361c933cb` | 37 methods |
| `ManagedCursorMXBean.java` | `7402bd65f793eddab8b34b0ac5ed0c40c24cf6a1` | 13 methods |
| `ManagedLedgerInternalStats.java` | `c109e269202115ada9c032c6168d9f6420a3eeec` | ledger/cursor admin DTO |
| `ManagedLedgerConfig.java` | `9dd5ced2dc787cae110b535fa89e91af40ba03f1` | per-ledger mutable config |
| `ManagedLedgerFactoryConfig.java` | `a915d651bd6b2bf01eb9aadb26d1c82fa6f60a0b` | factory compatibility config |
| `ScanOutcome.java` | `c679c40e85e9174da793cc99ebd73009a55e7dfe` | scan terminal enum |
| `PersistentOfflineTopicStats.java` | `f1cb0dfd088803be1cf4c9b88525d31c0af6ddc8` | offline admin DTO |

The target blob in this table is authoritative. The API probe and all source assertions in this document were
rechecked against the exact clean checkout at
`/Users/liusinan/apps/ideaproject/nereusstream/pulsar@100d3ef0ff7c7da36d497453b141ddff6f34a9d3`；no nearby
checkout or online source is part of the F2 contract.

The following labels are used below:

| Label | Meaning |
| --- | --- |
| `I` | Implemented by the F2 target design |
| `L` | Implemented as broker-local state only; restart does not preserve it |
| `N` | Deliberate neutral behavior because the concept does not exist in F2 |
| `U` | Explicitly unsupported; use the method's failure channel |
| `D` | Locked interface default is audited and intentionally inherited |

`N` is not an accidental no-op. Every `N` row below states why the neutral result is correct. `U` behavior is
standardized by `ManagedLedgerErrorMapper.unsupported(operation)` and includes the stable prefix
`NEREUS_UNSUPPORTED_OPERATION:`.

## 2. Target Packages and Classes

```text
nereus-api/com.nereusstream.api/
  AppendResult.java                            # P15-M6 cumulativeSize handoff
  AppendAttemptId.java
  AppendRecoveryOptions.java
  SealOptions.java
  DeleteOptions.java

nereus-core/com.nereusstream.core/
  append/AppendCoordinator.java                 # implemented P15 owner of append + exact recovery
  lifecycle/StreamLifecycleCoordinator.java

nereus-metadata-oxia/com.nereusstream.metadata.oxia/
  ManagedLedgerProjectionNames.java
  ManagedLedgerProjectionMetadataStore.java
  ManagedLedgerProjectionKeyspace.java
  OxiaJavaManagedLedgerProjectionMetadataStore.java
  AppendReplayCursor.java
  AppendReplayStatus.java
  AppendReplaySearchResult.java
  records/...

nereus-object-store/com.nereusstream.objectstore/
  ObjectStoreProvider.java
  ObjectStoreConfiguration.java
  ObjectStoreSecretResolver.java
  provider/NoopObjectStoreSecretResolver.java
  provider/S3CompatibleObjectStoreProvider.java
  provider/S3CompatibleObjectStore.java
  provider/S3ObjectKeyMapper.java
  provider/S3ObjectErrorMapper.java

nereus-managed-ledger/com.nereusstream.managedledger/
  NereusManagedLedgerFactory.java
  NereusManagedLedger.java
  NereusReadOnlyManagedLedger.java
  NereusManagedLedgerFactoryConfig.java
  NereusManagedLedgerRuntime.java
  NereusDurableStorageState.java
  NereusStorageStateSnapshot.java
  NereusWriteFenceResolution.java
  NereusWriteFenceSnapshot.java
  NereusWriteFenceView.java
  integration/NereusCreationGuard.java
  integration/NereusCreationPermit.java
  config/ManagedLedgerConfigView.java
  config/ManagedLedgerOpenConfigView.java
  config/ManagedLedgerConfigValidator.java
  entry/EncodedAppend.java
  entry/NereusEntry.java
  entry/PulsarEntryCodec.java
  projection/VirtualLedgerProjection.java
  projection/StreamPositionBounds.java
  projection/PositionProjection.java
  projection/F2L0RequestFactory.java
  snapshot/StreamSnapshotView.java
  snapshot/StreamSnapshotTracker.java
  snapshot/PendingReadWaiter.java
  snapshot/TailPollCoordinator.java
  cursor/NereusReadOnlyCursor.java
  cursor/NereusNonDurableCursor.java
  cursor/NereusManagedCursorBoundary.java
  cache/ZeroCapacityEntryCacheManager.java
  cache/ZeroCapacityEntryCache.java
  callbacks/TerminalCallback.java
  callbacks/SerialCallbackLane.java
  errors/ManagedLedgerErrorMapper.java
  stats/NereusManagedLedgerFactoryStats.java
  stats/NereusManagedLedgerStats.java
  stats/NereusManagedCursorStats.java
```

`nereus-managed-ledger` may import Pulsar managed-ledger/common/client-admin-api types. No class added to `nereus-api`,
`nereus-core`, `nereus-metadata-oxia` or `nereus-object-store` imports a Pulsar type.

As of F2-M1，`ManagedLedgerProjectionNames`、all `projection/` targets listed above and
`entry/EncodedAppend`、`entry/NereusEntry`、`entry/PulsarEntryCodec` are implemented and tested。The remaining F2
names in this inventory are later milestone targets and must not be reported as implemented early。

The L0 API/core classes and append-replay types in this inventory are implemented by Phase 1.5 P15-M1-M6，including
the M0R2-discovered `AppendResult.cumulativeSize` handoff。F2 owns only projection metadata and managed-ledger
classes；the combined inventory is retained here
because these are the exact types the facade consumes。

## 3. Required Phase 1.5 L0 Contract

The Phase 1 public `StreamStorage` surface cannot fulfill the F2 append and lifecycle promises。Phase 1.5
`../phase-1.5-core-storage-foundation/` is the implementation authority for the protocol-neutral additions below；
P15-M6 has passed for recovery/lifecycle and the exact cumulative result，and F2-M1 now implements the projection、
Position、entry codec and request foundation；
The F2-M3 writable/get-only ledger、factory/stats、F2-M4 cursor and F2-M5 broker integration consume this
contract；F2-M6 has composed their final acceptance。This section remains the F2 consumer
contract and cannot be implemented independently in the facade。

### 3.0 Complete logical append snapshot

```java
public record AppendResult(
        StreamId streamId,
        OffsetRange range,
        long committedEndOffset,
        long cumulativeSize,
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

`cumulativeSize` is exact lifetime logical bytes at this commit and comes from the existing internal
`CommittedAppend.cumulativeSize` for both normal and recovered results。It is not object physical size。Without it，a
facade snapshot that predates another broker's append cannot combine its stale size with only this append's
`logicalBytes`；a second metadata read may time out after durability is already known and therefore cannot be the
success condition。P15-M6 implemented this field without changing durable bytes or append certainty。

### 3.1 Recoverable append attempt

```java
public record AppendAttemptId(String value) {
}

public record AppendRecoveryOptions(Duration timeout) {
}

public interface StreamStorage extends AutoCloseable {
    // Existing methods omitted.

    CompletableFuture<AppendResult> recoverAppend(
            StreamId streamId,
            AppendAttemptId attemptId,
            AppendRecoveryOptions options);
}
```

`NereusException` gains `Optional<AppendAttemptId> appendAttemptId()` without replacing
`appendOutcome()`。Contract:

- the core generates the opaque ID before WAL submission as `processRunId + "/" + unsignedSequence`; it is nonblank,
  bounded, never reused in one runtime and never accepted from a protocol client；
- a failure before the stream-head request is `KNOWN_NOT_COMMITTED` and has no attempt ID；
- every `MAY_HAVE_COMMITTED` or `KNOWN_COMMITTED` failure after the head request has an attempt ID；
- the core append lane retains the exact generic `CommitAppendRequest`, durable provider result and `ReadTarget`
  needed to replay that ID；
- `recoverAppend` replays that same physical attempt; it never prepares or persists a new primary target；
- successful recovery returns the same `AppendResult` that the original call would have returned and releases the
  suspended lane；
- recovery may instead fail with `KNOWN_NOT_COMMITTED` only after bounded complete commit-identity inspection proves
  the retained request absent; that terminal releases the attempt and suspended lane without fabricating a result；
- an unresolved recovery keeps the lane suspended and returns an exception whose outcome never moves backward；
- an unknown/expired attempt ID is `METADATA_INVARIANT_VIOLATION`, not permission to submit new bytes。

The current Phase 1 replay search restarts at the latest head after each bounded-scan exhaustion. That is insufficient
for an F2 suspended callback because an attempt older than `maxCommitChainScan` would never make progress. P15-M2/M4 add a
protocol-neutral, metadata-internal paged search:

```java
public record AppendReplayCursor(
        StreamId streamId,
        String commitId,
        long expectedStartOffset,
        String observedHeadCommitId,
        long observedHeadOffsetEnd,
        long observedHeadCumulativeSize,
        long observedHeadCommitVersion,
        String nextCommitId,
        long nextOffsetEnd,
        long nextCumulativeSize,
        long nextCommitVersion) {
}

public enum AppendReplayStatus {
    FOUND,
    PROVEN_NOT_COMMITTED,
    CONTINUE
}

public record AppendReplaySearchResult(
        AppendReplayStatus status,
        Optional<ReachableCommittedAppend> committedResult,
        Optional<AppendReplayCursor> continuation,
        int scannedRecords) {
}

public interface OxiaMetadataStore extends AutoCloseable {
    CompletableFuture<AppendReplaySearchResult> searchAppendReplay(
            String cluster,
            CommitAppendRequest request,
            Optional<AppendReplayCursor> continuation,
            int maxCommitsToScan);
}
```

Exactly one field combination is legal per status: `FOUND` has a committed result, `CONTINUE` has a cursor, and
`PROVEN_NOT_COMMITTED` has neither. `scannedRecords` is in `[0,maxCommitsToScan]`. Cursor construction validates the
request identity plus the original observed-head anchor and the exact dense `(offsetEnd,cumulativeSize,commitVersion)`
tuple expected at `nextCommitId`; it is opaque to facade callers.

Recovery first joins the retained original mutation runner until no old task can still submit the head CAS. The
single-flight recovery runner is then the only code allowed to resubmit that exact `CommitAppendRequest`. When the
observed head equals `expectedStartOffset`, it retries that request directly. When the head is later, it freezes the
immutable head anchor and pages backward; later remote appends do not invalidate that anchor because the old physical
attempt can no longer be submitted concurrently. `FOUND` requires full request-identity validation and derived-index
materialization. A different reachable commit crossing `expectedStartOffset` (or a valid genesis boundary) alone may
produce `PROVEN_NOT_COMMITTED`. Page exhaustion returns `CONTINUE`, never a terminal failure; broken anchors/chains are
permanent invariants. The retained attempt stores the continuation so callback-time recovery and later background
recovery continue rather than rescan the newest page.

The retained recovery state is broker-process scoped and bounded to at most one suspended attempt per stream plus a
configured global cap. Capacity is reserved before WAL upload; exhaustion fails with `KNOWN_NOT_COMMITTED`, so an
already-sent head attempt is never discarded to make room. A process crash does not owe an old in-process callback,
but durable replay remains safe through the existing commit identity. F2 does not claim producer-sequence
deduplication across a broker crash.

Recovery is single-flight per suspended attempt. The facade's first `recoverAppend` uses the callback recovery budget.
The returned caller view joins the retained attempt's terminal future across retriable single-flight failures; it does
not expose the first transient recovery-flight exception as a false terminal。If that budget expires, it fails the
callback once and leaves the facade `WRITE_FENCED`; the already implemented core
`AppendCoordinator` remains the only retained-attempt registry and the only component allowed to retry that ID, using
its runtime scheduler with capped exponential backoff and one attempt timeout at a time. F2 must not add a second
`AppendRecoveryCoordinator`, copy the retained `CommitAppendRequest`, or schedule an independent replay loop. Later
committed success or proven `KNOWN_NOT_COMMITTED` releases retained capacity and clears the local write fence but
never emits a second producer callback. A permanent invariant/corruption result releases any remaining local
capacity, stops retrying and leaves the facade fenced for its lifetime. Retryable uncertainty continues until one of
those terminals or runtime close; `readyToCreateNewLedger()` cannot bypass it.

### 3.2 Stream lifecycle

```java
public record SealOptions(Duration timeout, String reason) {
}

public record DeleteOptions(Duration timeout, String reason) {
}

public interface StreamStorage extends AutoCloseable {
    CompletableFuture<StreamMetadata> seal(StreamId streamId, SealOptions options);

    CompletableFuture<StreamMetadata> delete(StreamId streamId, DeleteOptions options);
}
```

Both operations CAS the existing stream head; neither creates a missing head.
Options require a positive timeout and a nonblank strict-UTF-8 reason whose encoded size is at most
`ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES`. The reason is diagnostic context, not lifecycle authority or an
idempotency key.

`seal` state table:

| Observed L0 state | Result |
| --- | --- |
| `CREATING` | retriable `STREAM_NOT_ACTIVE`; F2 never publishes a projection to this state |
| `ACTIVE` | CAS to `SEALED`, retry on version conflict until deadline |
| `SEALED` | idempotently return the current snapshot |
| `DELETING` / `DELETED` | fail non-retriably |
| missing | `STREAM_NOT_FOUND` |

`delete` state table:

| Observed L0 state | Result |
| --- | --- |
| `CREATING` | retriable `STREAM_NOT_ACTIVE`; creation recovery must finish first |
| `ACTIVE` / `SEALED` | CAS to `DELETING`, then CAS to `DELETED` |
| `DELETING` | resume the terminal CAS |
| `DELETED` | idempotently return the tombstone snapshot |
| missing | `STREAM_NOT_FOUND` |

The first successful transition to `DELETING` is the logical delete point: append/read/trim/session acquisition are
rejected from that point. `DELETED` records completion. No object is physically removed. Concurrent append versus
seal/delete is ordered solely by stream-head CAS: an append either precedes the lifecycle transition and appears in
its returned LAC, or loses the CAS and fails.

## 4. Runtime Construction Contract

Target signatures below omit concrete method bodies:

```text
public record NereusManagedLedgerFactoryConfig(
        String storageClassName,
        Duration metadataTimeout,
        Duration appendTimeout,
        Duration appendRecoveryTimeout,
        Duration readTimeout,
        Duration closeTimeout,
        Duration tailPollInterval,
        int maxEntryBytes,
        int maxReadEntries,
        int maxOpenLedgers,
        int maxPendingCallbacks,
        int maxRetainedAppendAttempts,
        int maxScanEntries) {
}

public final class NereusManagedLedgerRuntime implements AutoCloseable {
    public NereusManagedLedgerRuntime(
            StreamStorage streamStorage,
            ManagedLedgerProjectionMetadataStore projectionStore,
            OxiaMetadataStore l0MetadataStore,
            SharedOxiaClientRuntime sharedOxiaRuntime,
            ObjectStore objectStore,
            ObjectStoreProvider objectStoreProvider,
            ScheduledExecutorService scheduler,
            ExecutorService callbackExecutor,
            NereusManagedLedgerFactoryConfig config,
            String cluster,
            String processRunId,
            String writerId);

    public StreamStorage streamStorage();
    public ManagedLedgerProjectionMetadataStore projectionStore();
    public ScheduledExecutorService scheduler();
    public Executor callbackExecutor();
    public NereusManagedLedgerFactoryConfig config();
    public String cluster();
    public String processRunId();
    public String writerId();
    public void close();
}

public enum NereusDurableStorageState {
    MISSING,
    ACTIVE,
    SEALED,
    DELETING,
    DELETED
}

public record NereusStorageStateSnapshot(
        NereusDurableStorageState state,
        Optional<VirtualLedgerProjection> projection,
        Optional<StreamMetadata> streamMetadata) {
}

public enum NereusWriteFenceResolution {
    COMMITTED,
    PROVEN_NOT_COMMITTED
}

public record NereusWriteFenceSnapshot(
        long generation,
        AppendAttemptId attemptId) {
}

public interface NereusWriteFenceView {
    Optional<NereusWriteFenceSnapshot> currentWriteFence();

    CompletableFuture<NereusWriteFenceResolution> awaitWriteFence(long generation);
}

public final class NereusManagedLedgerFactory implements ManagedLedgerFactory {
    public NereusManagedLedgerFactory(
            NereusManagedLedgerRuntime runtime,
            NereusCreationGuard creationGuard,
            ManagedLedgerConfig defaultManagedLedgerConfig,
            ManagedLedgerFactoryConfig compatibilityFactoryConfig,
            boolean ownsRuntime);

    public CompletableFuture<NereusStorageStateSnapshot> inspectStorageState(
            String managedLedgerName);
}

public record StreamSnapshotView(
        StreamMetadata metadata,
        long observedCommitVersion,
        boolean localAppendOverlay) {
}

public final class StreamSnapshotTracker {
    public StreamSnapshotView current();
    public StreamSnapshotView updateFromMetadata(StreamMetadata candidate);
    public StreamSnapshotView advanceFromAppend(AppendResult result);
}

interface PendingReadWaiter {
    long nextOffset();
    OptionalLong inclusiveMaxOffset();
    boolean trySignal(StreamMetadata snapshot);
    boolean tryFail(ManagedLedgerException error);
}

public final class TailPollCoordinator implements AutoCloseable {
    public void register(PendingReadWaiter waiter);
    public boolean remove(PendingReadWaiter waiter);
    public void signalLocalAppend();
    public void close();
}

public final class SerialCallbackLane implements AutoCloseable {
    public long admit();
    public void complete(long sequence, Runnable terminalCallback);
    public CompletableFuture<Void> closeAfterDrain();
    public void close();
}
```

All durations and limits are positive. `storageClassName` is exactly `nereus`. `closeTimeout` is at least
`appendTimeout + appendRecoveryTimeout` (checked without duration overflow), so a normal unload can drain an admitted
append and exact recovery before closing its callback domain. `maxEntryBytes` is at least the broker's maximum
persisted entry size and no larger than the L0/object limits.

Runtime construction does not accept or wait for a Pulsar `brokerId`: the locked `PulsarService` startup sequence
constructs managed-ledger storage before the final broker ID exists. It generates a fresh, URL-safe 128-bit-or-more
`processRunId` with `SecureRandom` and freezes `writerId="pulsar-f2/" + processRunId`；both accessors return those
immutable values. They are diagnostic/session identities only, never durable topic identity. The fork publishes its
broker capability later, after a broker ID exists and before load-manager advertisement, as specified in `07`.

The runtime also freezes the nonblank exact `cluster` passed from `StreamStorageConfig.cluster()`。Projection metadata
methods require that cluster to construct their Oxia keyspace，while `StreamStorage` intentionally exposes no config
accessor；therefore omitting it from the runtime constructor would make the factory open protocol unimplementable or
force an unsafe namespace inference。The runtime accessor is the only facade-side source of this identity，and
changing it requires rebuilding the runtime。

The factory retains the exact constructor-supplied `defaultManagedLedgerConfig` for both no-config `open` methods；it
does not create a new default per call. `compatibilityFactoryConfig` is factory-owned and returned by `getConfig()`.
`ownsRuntime=true` is legal only for the hybrid provider's successful ownership transfer；test factories may pass
false and close the runtime explicitly. Null arguments, a closed runtime, mismatched storage class, or reused owned
runtime fail constructor validation before the factory becomes visible.

`inspectStorageState` is a get-only product API used by the fork's durable binding protocol；it never invokes
create-or-get, projection repair, lifecycle mutation or property mutation. Missing projection returns `MISSING` with
both optionals empty. A present current projection requires an exact-name/identity-valid L0 head and returns the L0
`ACTIVE`、`SEALED`、`DELETING` or `DELETED` state with both optionals present. L0 `CREATING` is a retriable
`STREAM_NOT_ACTIVE`; a present projection with a missing head, an illegal mirror/L0 pair, or mismatched identity is
`METADATA_INVARIANT_VIOLATION`, never `MISSING`. The returned records are immutable snapshots.

`NereusManagedLedger` implements both `ManagedLedger` and `NereusWriteFenceView`. Generation starts at zero and is
incremented before publishing every new `WRITE_FENCED` attempt；generation overflow is a permanent invariant failure.
`currentWriteFence()` is nonempty only while exact recovery is unresolved. `awaitWriteFence(g)` joins the one
generation-specific terminal future: commit yields `COMMITTED`, complete proof of absence yields
`PROVEN_NOT_COMMITTED`, and corruption/permanent failure or ledger close completes exceptionally. A stale generation
never observes or clears a later fence. To close the snapshot/await race，the ledger retains the most recently
completed generation/future until the next generation begins or the ledger closes；`awaitWriteFence` accepts only the
current or that last terminal generation and rejects any other value as an invariant. There is no unbounded history。
For a normal terminal it clears the matching current fence/updates facade state under the ledger lock before
completing the terminal future outside that lock，so the broker completion's fresh read cannot observe a completed
same-generation fence。
This is the only broker/facade handoff that may authorize topic unfencing；a
stock `readyToCreateNewLedger()` result alone is insufficient.

`maxOpenLedgers` is a factory handle-permit count across the separate writable and read-only exact-name registries:
joining the same registry/name consumes no extra permit; a distinct handle reserves before L0/projection IO and
releases on failed open, final writable close, or read-only `factoryClose` during shutdown. Cursor reads require
`maxEntries > 0` and use `min(maxEntries, maxReadEntries)` per operation. Search/replay loops stop with the method's
documented budget failure/rejected-position result after `maxScanEntries`; none of these limits silently truncates a
correctness-sensitive metadata repair.

Constructor arguments transfer ownership atomically only after every null/identity/config relationship validates；on
constructor failure ownership stays with the caller. Scheduler/callback executor and every closeable role must be
identity-distinct（the two metadata adapters intentionally share only `sharedOxiaRuntime`），so reverse close cannot
double-close an aliased object. The runtime owns `StreamStorage`, both Oxia metadata adapters,
the shared Oxia client/runtime, ObjectStore/provider,
scheduler and callback executor. A ledger close never closes these shared resources. Factory shutdown closes writable
and read-only handles first; runtime close then closes projection handles, drains `StreamStorage`, closes its L0
metadata adapter, ObjectStore and provider, shared Oxia client, and finally executors. All close attempts run in
dependency-reverse order and aggregate suppressed failures. The hybrid provider transfers runtime ownership to the
factory constructor and thereafter closes only the factory; partial initialization closes the runtime directly only
before that transfer. Each owner uses an atomic close guard.

Every writable ledger creates one `SerialCallbackLane` backed by `callbackExecutor`. Admission assigns a monotonic
sequence; `complete(sequence, callbackTask)` stores an out-of-order terminal result in a bounded map and drains only
from `nextCallbackSequence`. Thus append, append-recovery, terminate, delete and close callbacks execute in
accepted-operation order even when their futures finish out of order. Each cursor has its own serialized operation /
callback lane for read-position mutation and read/wait callbacks. Factory opens need no cross-name ordering but still
use `callbackExecutor`. One runtime semaphore of `maxPendingCallbacks` bounds accepted non-terminal operations before
input bytes are copied; a rejected operation receives `TooManyRequestsException` and never enters L0.

`StreamSnapshotTracker` is one per facade and retains both the last complete remote `StreamMetadata` and one effective
view. `updateFromMetadata` validates exact stream identity/profile plus monotonic backend metadata version、committed
end、cumulative size、trim and lifecycle transitions. Equal backend version with different **remote** content and a
newer backend version with numeric regression are invariants；an older/equal remote base arriving after a local
overlay is dropped as stale rather than erasing a known commit.

`advanceFromAppend` validates stream/result shape and copies exact `committedEndOffset`、`cumulativeSize` and
`commitVersion` from P15-M6 `AppendResult` into the effective view while retaining trim/lifecycle/created/profile and
the last remote metadata version. It accepts an older recovered result as a validated no-op when the effective tail is
already later；otherwise end、cumulative size and commit version cannot regress. `localAppendOverlay=true` records why
equal backend-version content differs and is cleared only when a later metadata snapshot catches up or passes it. It
never derives size from `logicalBytes` and cannot synthesize trim or lifecycle state. Every Position/count/size read
uses one `current().metadata()` view.

`TailPollCoordinator` is also one per facade: all
cursors share one timer and at most one in-flight metadata read, the first waiter starts it, the last removal stops
it, and close increments a generation so late poll completions cannot invoke removed waiters. `register` performs
register-then-recheck against the tracker before returning. `PendingReadWaiter` owns the cursor lane and exactly-once
terminal guard；the coordinator may only call its `trySignal`/`tryFail` methods and never invokes a broker callback
directly. `OptionalLong.empty()` means no finite max position.

`SerialCallbackLane.admit()` rejects after close and returns a strictly increasing nonnegative sequence (overflow is
a permanent local failure). `complete` accepts each admitted sequence exactly once, stores out-of-order completions
in a map bounded by the same admission permit, and runs ready tasks without holding the lane lock. A duplicate or
unknown sequence is an invariant failure. `closeAfterDrain` prevents new admission and completes after all admitted
terminal tasks have run；`close` is the forced runtime-shutdown form and discards no facade-owned cleanup.

## 5. PositionProjection API

A single `toOffset` method is insufficient because a direct entry, a next-read position and a mark-delete position
have different legal ranges. The target API is role-specific; concrete method bodies are omitted below:

```text
public record StreamPositionBounds(
        long trimOffset,
        long committedEndOffset,
        Position beforeFirstAvailable,
        Position firstAvailable,
        Position lastConfirmed,
        Position onePastLast) {
}

public final class PositionProjection {
    public StreamPositionBounds bounds(
            VirtualLedgerProjection projection,
            StreamMetadata snapshot);

    public Position entryPosition(VirtualLedgerProjection projection, long offset);

    public long requireReadableEntryOffset(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata snapshot);

    public long requireReadPositionOffset(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata snapshot);

    public long cursorReadOffsetAfter(
            VirtualLedgerProjection projection,
            Position cursorStartOrMarkDeletePosition,
            StreamMetadata snapshot);

    public long normalizeResetReadPositionOffset(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata snapshot);

    public long markDeleteOffsetAfter(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata snapshot);

    public Position readPosition(
            VirtualLedgerProjection projection,
            long nextOffset,
            StreamMetadata snapshot);

    public Position normalizeInclusiveMaxPosition(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata snapshot);
}
```

Legal ranges:

| Role | Legal entry ID |
| --- | --- |
| readable entry | `[trimOffset, committedEndOffset)` |
| next-read position | `[trimOffset, committedEndOffset]` |
| concrete cursor-start/mark-delete input used to find next-valid | same-ledger `[-1, committedEndOffset)`; values before trim advance to trim |
| reset read-position input | same-ledger `[-1, Long.MAX_VALUE]`; clamp to retained/tail range |
| mark-delete position | `[trimOffset - 1, committedEndOffset - 1]` |
| inclusive max position input | null, `EARLIEST`, `LATEST`, or current-ledger entry ID `>= -1` |

`PositionFactory.EARLIEST` and `LATEST` are recognized before virtual-ledger validation. Every other position must
carry the current virtual ledger ID. Inclusive max normalization maps null/`LATEST` to `lastConfirmed`, `EARLIEST`
or an already-trimmed same-ledger value to `beforeFirstAvailable`, and a same-ledger value beyond LAC to
`lastConfirmed`. This matches stock's “read no farther than the current LAC” behavior instead of rejecting a harmless
future max bound. `ManagedLedger.getFirstPosition()` returns `beforeFirstAvailable`, not the first readable entry.
This is intentionally different from the `pulsar-storage` prototype.

`cursorReadOffsetAfter` accepts only a concrete current-ledger coordinate and returns
`max(trimOffset, entryId + 1)` with checked arithmetic. Sentinel/default handling stays in `NereusManagedLedger`:
explicit `EARLIEST` selects trim; null、`LATEST` and concrete future positions use `InitialPosition`; the two-argument
overload supplies `Latest`. The returned local mark-delete is always `readOffset - 1`, including after trim clamping.
This method must not replace `requireReadPositionOffset`: direct seek/reset/read coordinates do not receive `+1`.

## 6. ManagedLedgerFactory Surface

All 26 declared members are covered below.

| Label | Exact method(s) | F2 contract |
| --- | --- | --- |
| `I` | `open(String)`; `open(String, ManagedLedgerConfig)` | Stock-shaped latch wrappers over the async core；add no independent timer, restore interruption and throw the mapped `ManagedLedgerException`. The async metadata deadline guarantees ordinary terminal completion. |
| `I` | both `asyncOpen(...)` overloads | Deduplicate by exact managed-ledger name; acquire/validate the fork binding permit, honor `config.isCreateIfMissing()`, and return one live local facade. |
| `I` | `openReadOnlyCursor(...)`; `asyncOpenReadOnlyCursor(...)` | Never create a missing topic regardless of `createIfMissing`; normalize the supplied position as a next-read position. |
| `I` | `asyncOpenReadOnlyManagedLedger(...)` | Get-only open and return `NereusReadOnlyManagedLedger`; no projection/L0 creation, append session or writable facade. |
| `I` | `getManagedLedgerInfo(...)`; `asyncGetManagedLedgerInfo(...)` | Get-only; synthesize one virtual ledger from an `ACTIVE/SEALED` L0 snapshot and an empty durable cursor map. `DELETING/DELETED` is not found. |
| `I` | both `delete(...)` overloads; both `asyncDelete(...)` overloads | Delete the open facade or resolve the current projection without creating it, then perform logical L0 delete. An existing `DELETED` tombstone is idempotent success; a missing projection is not found. `mlConfigFuture` is null-checked but is not used for offload cleanup. |
| `I` | `shutdown()`; `shutdownAsync()` | Idempotent close sequence; async method reports failure through the returned future and never throws due to ordinary closed state. |
| `I` | `asyncExists(String)` | Topic projection missing is false; current L0 `ACTIVE/SEALED` is true; `DELETING/DELETED` is false; a present projection with missing L0 head is corruption, not false. |
| `I` | `getEntryCacheManager()` | Return the F2 zero-capacity manager described in section 11. |
| `I` | `updateCacheEvictionTimeThreshold(long)`; `getCacheEvictionTimeThreshold()` | Validate/store the compatibility setting; it does not create a BookKeeper cache. |
| `I` | both cache TTL extension update defaults | Override and forward to the zero-capacity manager so target drift is visible in tests. |
| `I` | `getManagedLedgerPropertiesAsync(String)` | Return an empty map for genuinely `MISSING` state so broker pre-create probing remains write-free；return a defensive property snapshot for `ACTIVE/SEALED`；treat `DELETING/DELETED` as not found and projection/L0 mismatch as corruption. |
| `I` | `getManagedLedgers()` | Return an immutable snapshot containing only live writable facades. |
| `I` | `getCacheStats()` | Return Nereus counters; BookKeeper hit/miss/eviction counters remain zero. |
| `I` | `estimateUnloadedTopicBacklog(...)` | Blocking admin-only get bounded by metadata timeout. Ignore the BookKeeper-shaped `ctx`; set one virtual-ledger detail only when `accurate=true`; no durable cursor detail in F2; deleted is not found. |
| `I` | `getConfig()` | Return one stable, factory-owned compatibility config with zero cache and the real stats period; never allocate a new default instance per call. |

Open-time `ManagedLedgerConfig` rules:

- `storageClassName` is null or exactly `nereus`；
- `createIfMissing=false` never writes L0 or projection metadata；
- initial `config.getProperties()` are used only for a new projection incarnation；
- a non-null `ManagedLedgerInterceptor` is rejected in F2 because it can transform persisted and callback bytes；
- offload is admissible only when `getLedgerOffloader()` is `NullLedgerOffloader.INSTANCE` and
  `isTriggerOffloadOnTopicLoad()==false`；
- both `getShadowSource()` and Lombok `getShadowSourceName()` must be null；
- read-compacted is not a config field and is rejected at the cursor/subscribe argument boundary；
- `autoSkipNonRecoverableData=true` is rejected; F2 never turns corruption into a silent position skip；
- the target class has no copy constructor/clone contract, so open retains the exact caller-supplied reference as
  stock `ManagedLedgerImpl` does; no reflective or partial clone is allowed；
- `setConfig` null-checks, captures/validates a candidate view, then replaces a `volatile ManagedLedgerConfig`
  reference and applies the supported local throttle update；a rejected candidate never becomes current；
- every operation reads the current reference once, calls `ManagedLedgerConfigValidator.captureForOperation`, and
  uses only the returned immutable view; mutation after capture cannot enable an interceptor/offloader inside that
  operation；
- direct mutation through the object returned by `getConfig()` is observed only by a later capture and can cause that
  later operation to fail validation; callers needing an atomic multi-field change construct a candidate and call
  `setConfig`。

The code-level view is deliberately smaller than the BookKeeper configuration object:

```java
public record ManagedLedgerConfigView(
        String storageClassName,
        double throttleMarkDelete) {
}

public record ManagedLedgerOpenConfigView(
        ManagedLedgerConfigView operationView,
        boolean createIfMissing,
        Map<String, String> initialProperties) {
}

public final class ManagedLedgerConfigValidator {
    public static ManagedLedgerConfigView captureForOperation(ManagedLedgerConfig source);
    public static ManagedLedgerOpenConfigView captureForOpen(ManagedLedgerConfig source);
}
```

Both captures require `getRetentionTimeMillis()==0`,
`getRetentionSizeInMB()==0`, null interceptor, `NullLedgerOffloader.INSTANCE`, trigger-on-load false, both shadow
source views null, auto-skip false and storage class null/`nereus` before returning. Only `captureForOpen` copies and
canonicalizes properties with `MetadataCanonicalizer.canonicalStringMap(...,
ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,"managedLedgerProperties")` (`null` becomes the immutable empty map);
appends do not recopy that map. Its
`createIfMissing` and `initialProperties` are consumed
only during open/new-incarnation creation. `throttleMarkDelete` is the only F2 dynamic field applied to live local
cursors. BookKeeper quorum, digest/password, rollover, ledger-delete and entry-cache tuning remain accepted
compatibility values but are never read by Nereus; Nereus metadata/append/read/close timeouts and cache bounds come
from `NereusManagedLedgerFactoryConfig`.

## 7. ManagedLedger Surface

All 87 declared members, including defaults that could be semantically unsafe, are assigned below.

### 7.1 Identity, append and cursors

| Label | Exact method(s) | F2 contract |
| --- | --- | --- |
| `I` | `getName()` | Exact broker-supplied persistence name. |
| `I` | all four synchronous `addEntry(byte[]...)` overloads | Validate slice/count, copy exact bytes, call the same async core and wait for its single terminal callback. Like locked stock code, the wrapper adds no second timeout race；the admitted async operation already owns append/recovery deadlines. Interruption is restored and propagated. |
| `I` | all five `asyncAddEntry(...)` overloads | Funnel into `asyncAddEntry(ByteBuf,int,...)`; one Pulsar Entry consumes one L0 offset. |
| `I` | three `openCursor(...)` overloads; three `asyncOpenCursor(...)` overloads | Create/join an F2 durable-boundary cursor. Its initial state is local; durable progress mutation is unsupported. |
| `I` | three `newNonDurableCursor(...)` overloads | Create/join a broker-local cursor; reject `isReadCompacted=true`。A concrete Position is the already-consumed coordinate and reads next-valid; explicit `EARLIEST` maps to trim; null、`LATEST` and concrete future Positions consult `InitialPosition`; the two-argument overload defaults Latest. Anonymous cursors use collision-resistant unique names. |
| `L` | `deleteCursor(String)`; `asyncDeleteCursor(...)` | Close/remove the local cursor. Missing name is `CursorNotFoundException`; no durable cursor record is claimed. |
| `I` | `removeWaitingCursor(ManagedCursor)` | Remove only that cursor's registered waiter; identity mismatch is ignored after validation. |
| `I` | `getCursors()`; `getActiveCursors()` | Immutable snapshots; active variant filters the cursor-local active flag. |

### 7.2 Counts, size and time

| Label | Exact method(s) | F2 contract |
| --- | --- | --- |
| `I` | `getNumberOfEntries()` | `committedEndOffset - trimOffset` from the latest complete local snapshot. |
| `I` | `getNumberOfEntries(Range<Position>)` | Exact open/closed bound arithmetic after role-aware projection and clipping to the retained range. |
| `I` | `getNumberOfActiveEntries()` | Zero when there is no durable-boundary cursor; otherwise entries after the slowest local mark-delete. |
| `I` | `getTotalSize()` | L0 `cumulativeSize`: exact lifetime logical payload still protected by F2 because F2 performs no physical GC. It is not object-store physical bytes and not post-trim logical size. |
| `I` | `getEstimatedBacklogSize()`; `getEstimatedBacklogSize(Position)` | No cursor means zero for the no-arg form; otherwise use retained/lifetime average-entry size from the supplied or slowest local mark-delete position. These methods are explicitly estimates. |
| `I` | `getEarliestMessagePublishTimeInBacklog()` | Return `0` without a durable-boundary cursor, `-1` when that cursor has no backlog, otherwise read the first backlog entry and parse its persisted Pulsar timestamp. |
| `N` | `getOffloadedSize()`; `getLastOffloadedLedgerId()`; both offload timestamp getters | Return `0`; F2 has no Pulsar offload domain and never reports a virtual ledger as offloaded. |
| `I` | `getLastAddEntryTime()` | Local timestamp of the last successful append callback; `0` before the first local success. |
| `I` | `getMetadataCreationTimestamp()` | Current projection incarnation `createdAtMillis`. |

### 7.3 Lifecycle and compatibility state

| Label | Exact method(s) | F2 contract |
| --- | --- | --- |
| `N` | `unfenceForInterceptorException()` | Inherited no-op is safe because F2 rejects interceptors at open. |
| `I` | `terminate()`; `asyncTerminate(...)`; `isTerminated()` | Ordered append barrier, L0 `seal`, final LAC. Idempotent. |
| `U` | `asyncMigrate()`; `isMigrated()` | Future fails; `isMigrated()` is false. Migration is not silently treated as termination. |
| `I` | `close()`; `asyncClose(...)` | Local close only; accepted operations drain before resources are released. |
| `I` | `delete()`; `asyncDelete(...)` | Logical L0 delete and local removal; no object deletion. |
| `U` | `offloadPrefix(...)`; `asyncOffloadPrefix(...)` | Checked throw or failure callback. Virtual IDs never reach an offloader. |
| `I` | `getConfig()`; `setConfig(...)` | Retain/return the exact current config reference like stock. `setConfig` validates then atomically replaces the volatile reference. Each operation captures one immutable validated view before admission and never rereads the mutable object during that operation. |
| `I` | `getLastConfirmedEntry()` | `(virtualLedgerId,-1)` only for a never-appended stream; otherwise `(virtualLedgerId, committedEndOffset-1)`, even after full trim. |
| `I` | `readyToCreateNewLedger()` | No rollover. If there is no suspended/non-known attempt, atomically discard only the cached append session so the next append auto-acquires a fresh one. The void method does no remote IO and never clears `WRITE_FENCED`. |
| `N` | `trimConsumedLedgersInBackground(CompletableFuture<?>)` | Complete the promise normally and increment a deferred-retention metric; F2 has no cursor-driven trim/physical GC. |
| `N` | `rolloverCursorsInBackground()` | No durable cursor ledger exists in F2. |
| `U` | `skipNonRecoverableLedger(long)` | Throw stable unsupported runtime exception; data loss is never converted into a position skip. |
| `N` | `rollCurrentLedgerIfFull()`; `checkInactiveLedgerAndRollOver()` | No physical/virtual rollover; void method records a metric and check returns false. |

### 7.4 Properties, search, stats and navigation

| Label | Exact method(s) | F2 contract |
| --- | --- | --- |
| `I` | `getProperties()`; all set/delete property sync/async methods | Topic projection property map；sync methods wait for the async metadata-deadline terminal without adding a second timer. `setProperties` replaces the entire user map. |
| `U` | `asyncAddLedgerProperty`; `asyncRemoveLedgerProperty`; `asyncGetLedgerProperty` | Per-BookKeeper-ledger/offload properties are unsupported even when the ID matches the virtual ledger. |
| `I` | `asyncFindPosition(Predicate<Entry>)` | Bounded newest-match scan used for sequence lookup: empty stream returns null, no match returns first available, a match returns its next valid position, and budget/read failure fails the future rather than fabricating null. |
| `N` | `getManagedLedgerInterceptor()` | Return null because non-null configuration is rejected. |
| `I` | `getLedgerInfo(long)`; `getOptionalLedgerInfo(long)` | Matching virtual ID returns the synthetic protobuf info; wrong ID returns `null`/empty exactly like stock lookup. |
| `U` | `asyncTruncate()` | Failed future. Topic truncate requires cursor/retention semantics beyond F2. |
| `I` | `getManagedLedgerInternalStats(boolean)` | Populate Nereus values and one virtual ledger; BookKeeper ensemble metadata is null and under-replicated is false. |
| `N` | `checkCursorsToCacheEntries()` | Deliberate no-op with metric because F2 cache capacity is zero. |
| `D` | `getManagedLedgerAttributes()` | Locked default is safe and creates only metrics attributes. |
| `I` | `asyncReadEntry(...)` | Exact one-entry L0 read; sentinels/future/trimmed positions fail through callback. |
| `I` | `getLedgersInfo()` | Immutable sorted map with exactly the current virtual ledger. |
| `I` | `getNextValidPosition(...)`; `getPreviousPosition(...)` | Dense one-ledger formula, clipped to `beforeFirstAvailable` and `onePastLast`. |
| `I` | `getPositionAfterN(...)` | Checked arithmetic, correct `PositionBound`, clamp to LAC. |
| `I` | `getPendingAddEntriesCount()` | Accepted but not terminal append operations. |
| `N` | `getCacheSize()` | Zero under the F2 cache policy. |
| `I` | `getLastDispatchablePosition(...)` | LAC for an unconditional predicate; otherwise bounded backward scan. Never inherit the default `EARLIEST`. |
| `I` | `getFirstPosition()` | `beforeFirstAvailable == (virtualLedgerId, trimOffset-1)`. |
| `I` | `getStats()` | Nereus `ManagedLedgerMXBean`; no BookKeeper pending-op claims. |
| `I` | `getSlowestConsumer()` | Lowest local mark-delete among durable-boundary cursors; null when none. |

`getLedgerInfo`、`getOptionalLedgerInfo` and `getLedgersInfo` return the generated protobuf
`MLDataFormats.ManagedLedgerInfo.LedgerInfo`，whose exact builder surface includes `properties`:

```text
ledgerId  = current virtualLedgerId
entries   = committedEndOffset
size      = cumulativeSize
timestamp = absent/0 while the never-rolling virtual ledger is live
offloadContext = absent
properties = empty
```

`entries` is the stable entry-id extent, not the retained count. Admin-facing
`org.apache.bookkeeper.mledger.ManagedLedgerInfo` is a different DTO；its nested `LedgerInfo` has only
`ledgerId/entries/size/timestamp/isOffloaded/offloadedContextUuid` and must never be given a nonexistent
`properties` field. It separately exposes the terminated position when sealed and current topic properties. Its F2
`cursors` map is empty because F2 has no durable cursor authority.

The DTO builders assign fields exactly:

```text
ManagedLedgerInfo.version          = topicProjection.metadataVersion
creationDate                       = DateFormatter.format(createdAtMillis)
modificationDate                   = null (F2 metadata API has no authoritative mtime)
ledgers                            = one ManagedLedgerInfo.LedgerInfo with extent/size, timestamp=null,
                                     isOffloaded=false, offloadedContextUuid=null
terminatedPosition                 = final LAC only when SEALED; otherwise null
cursors                            = empty map
properties                         = null when empty; otherwise a sorted copy of topic-projection properties

PersistentOfflineTopicStats.totalMessages   = committedEndOffset - trimOffset
PersistentOfflineTopicStats.storageSize     = cumulativeSize
PersistentOfflineTopicStats.messageBacklog  = 0
cursorDetails                               = empty
dataLedgerDetails                           = empty unless accurate=true; then one retained-count virtual detail
```

`PersistentOfflineTopicStats.totalMessages` follows the stock method's entry-count behavior despite its field name;
F2 does not expand `pulsar.numberOfMessages`. `ctx` may contain BookKeeper digest/password values from stock broker
callers and is deliberately ignored, never cast and never forwarded to BookKeeper.

`getManagedLedgerInternalStats(includeLedgerMetadata)` always returns a completed DTO asynchronously on the callback
executor and never opens BookKeeper. Its fields are:

```text
entriesAddedCounter       = successful entry appends since this facade opened
numberOfEntries           = committedEndOffset - trimOffset
totalSize                 = cumulativeSize
currentLedgerEntries      = committedEndOffset
currentLedgerSize         = cumulativeSize
lastLedgerCreatedTimestamp= DateFormatter.format(projection.createdAtMillis)
lastLedgerCreationFailureTimestamp = null
waitingCursorsCount       = installed local waiters
pendingAddEntriesCount    = accepted non-terminal appends
lastConfirmedEntry        = projected LAC.toString()
state                     = LedgerOpened / Terminated / Closing / Closed / Fenced compatibility name
properties                = mutable defensive copy of topic properties
ledgers                   = one virtual-ledger DTO
cursors                   = one defensive DTO per live local cursor
```

The virtual-ledger DTO uses extent `entries=committedEndOffset`, `size=cumulativeSize`, `offloaded=false`,
`underReplicated=false` and empty properties. With `includeLedgerMetadata=false`, `metadata=null`; with true, metadata
is a safe Nereus summary containing only stream ID, incarnation and profile—never an ensemble or credential.

Each `CursorStats` reports projected mark-delete/read positions, waiter/pending-read state, local consumed counter,
`cursorLedger=-1`, `cursorLedgerLastEntry=-1`, `individuallyDeletedMessages="[]"`, projection creation time as the
only ledger-switch timestamp, local state/active flag, exact entry backlog, zero non-contiguous ranges, explicit
pending-read/replay booleans and a defensive copy of local long properties. The durable-boundary DTO does not imply
that these local values survived restart.

## 8. ReadOnlyManagedLedger, ReadOnlyCursor and Entry

### 8.1 ReadOnlyManagedLedger

| Label | Exact method | F2 contract |
| --- | --- | --- |
| `I` | `asyncReadEntry(...)` | Same direct-read core as writable facade. |
| `I` | `getNumberOfEntries()` | Retained entry count from refreshed snapshot. |
| `I` | `createReadOnlyCursor(Position)` | Position is the next entry to read; `EARLIEST` becomes `trimOffset`, `LATEST` becomes `committedEndOffset`. |
| `I` | `getProperties()` | Immutable snapshot from the latest validated projection-property cache. |

### 8.2 ReadOnlyCursor

All 11 methods are implemented: both sync/async read overloads, read position, `hasMoreEntries`, count,
`skipEntries`, newest-match search, range count, sync close and async close. It has no waiter API; EOF returns an
empty list. `getNumberOfEntries(Range)` uses the same exact bound arithmetic as the ledger.

### 8.3 NereusEntry

`NereusEntry` implements all abstract `Entry` methods. It owns one read-only `ByteBuf` reference:

- `getData()` copies without moving indices；
- `getDataAndRelease()` copies and then calls `release()` exactly once；
- `getLength()` is the readable byte count；
- `getDataBuffer()` returns the owned buffer without retaining it, matching `EntryImpl` usage；
- position/ledger/entry accessors return the immutable projected coordinate；
- `release()` releases the owned reference and returns Netty's deallocation result；
- an atomic released guard makes a second `release()` return false without touching the buffer again; data access
  after release is illegal, matching a deallocated stock entry；
- `getReadCountHandler()` returns null, so audited interface defaults report no expected reads；
- `getMessageMetadata()` returns a lazily parsed/cached metadata object or null after a parse failure; callers may
  still parse from bytes as stock broker paths do；
- audited defaults `hasExpectedReads()` and `matchesPosition(...)` are inherited；`getEntryTimestamp()` is also
  inherited and parses the exact entry bytes through locked `Commands`，so a malformed Pulsar entry fails as stock
  rather than fabricating timestamp zero. Parsing uses a duplicate buffer and never moves the owned indices。

The facade never returns an `Entry` backed by caller-owned append memory.

## 9. ManagedCursor Surface by Concrete Type

Three classes intentionally expose different durability:

| Type | Interface | Durable truth |
| --- | --- | --- |
| `NereusReadOnlyCursor` | `ReadOnlyCursor` | none; local next-read position |
| `NereusNonDurableCursor` | `ManagedCursor` | none; local read/mark-delete/properties |
| `NereusManagedCursorBoundary` | `ManagedCursor` | initial identity only; no successful durable mutation until F3 |

Together the two `ManagedCursor` implementations assign all 89 locked methods plus the interface constant; no
default is inherited without an `I/L/N/U/D` decision.

### 9.1 Identity, properties and reads

| Exact method group | Non-durable | Durable boundary |
| --- | --- | --- |
| `getName`, `getLastActive`, `updateLastActive` | `L`: local clock/state | `L`: local clock/state |
| `getProperties`, `getCursorProperties` | `L`: immutable snapshots | initial snapshots only |
| `put/removeProperty` | local update, boolean success | return false |
| `put/set/removeCursorProperty` | local completed future | failed future (`U`) |
| all `readEntries` overloads | `I` | `I` |
| `asyncReadEntriesWithSkip` | evaluate the exact `Predicate<Position>` before L0 IO；a skipped coordinate advances locally without allocating or releasing an `Entry` | same |
| `getNthEntry` / `asyncGetNthEntry` | require `n > 0`; non-mutating 1-based read after local mark-delete; return success with null when fewer than `n` entries remain | same; Include/Exclude individual-deleted modes are equal because F2 has no ack holes |
| all `readEntriesOrWait` overloads | `I` | `I` |
| both `asyncReadEntriesWithSkipOrWait` overloads | same pre-IO `Predicate<Position>` rule；if every currently visible coordinate is skipped, install/recheck the waiter at the resulting next offset | same |
| `cancelPendingReadRequest` | atomically remove without invoking callback; return true only when this call removed a live waiter | same |
| `hasMoreEntries`, count/backlog methods | exact dense-offset formulas | exact dense-offset formulas |

`readEntriesOrWait` allows at most one pending waiter. A second request fails with
`ConcurrentWaitCallbackException`. Cursor close fails the installed waiter once with
`CursorAlreadyClosedException`; explicit `cancelPendingReadRequest()` removes/recycles the request and deliberately
does not invoke it.

### 9.2 Progress and position mutation

| Exact method group | Non-durable | Durable boundary |
| --- | --- | --- |
| all `markDelete` overloads | local cumulative mark-delete, reject ack-set/partial-batch extensions | failure (`U`) |
| `delete(Position/Iterable)` and async variants | failure (`U`); individual ack holes are F3 | failure (`U`) |
| `getReadPosition`, `getMarkDeletedPosition` | local values | local values |
| `getPersistentMarkDeletedPosition` | null | null; F2 has persisted no cursor progress |
| `rewind`, `rewind(boolean)` | reset to next retained offset after local mark-delete; reject `readCompacted=true` | same local read-position behavior |
| `seek(Position[,force])` | direct read-position update with no `+1`; when force is false clamp to next after local mark-delete | same; does not change ack truth |
| `clearBacklog` / async | local mark-delete to current LAC | failure (`U`) |
| `skipEntries` / async | local next-read advance | same |
| `resetCursor` / async | Position is direct read target; normalize `EARLIEST`/`LATEST` and trimmed/future targets, set mark-delete to the previous retained coordinate; force cannot resurrect trimmed F2 bytes | failure (`U`) |
| replay overloads | synchronously return positions at/below local mark-delete as skipped, asynchronously reread every other retained position, honor `sortEntries`, and fail callback on a wrong/trimmed/future position | same; no individual ack-hole inference |

### 9.3 Search, lifecycle and stats

| Exact method group | F2 contract |
| --- | --- |
| all `findNewestMatching` overloads | Bounded scan. `isFindFromLedger` has identical behavior because the Pulsar entry cache is zero-capacity. Range overload honors both endpoints. |
| default `scan(...)` | Override with bounded forward scan: `COMPLETED` at end, `ABORTED` on timeout/entry budget, `USER_INTERRUPTED` when predicate stops; only validation/read/storage errors fail the future. |
| `close`, `asyncClose`, `isClosed` | Idempotent local close; close fails an installed waiter once. |
| `getFirstPosition` | Ledger `beforeFirstAvailable`. |
| active/inactive methods | Local atomic flag; `setAlwaysInactive` is terminal inactive. |
| `isDurable` | false for non-durable, true for durable boundary. |
| ack-range counts/serialized size | zero; F2 creates no individual ranges. |
| `getEstimatedSizeSinceMarkDeletePosition` | Stock-style average-size estimate. |
| `skipNonRecoverableLedger` | Explicit unsupported runtime failure. |
| throttle getters/setter | Local validated value; it does not make durable mark-delete supported. |
| `getManagedLedger` | Owning facade identity. |
| `getLastIndividualDeletedRange` | null. |
| `trimDeletedEntries` | Release/remove only entries at or below local mark-delete; no individual holes. |
| batch-index ack getters | null; empty arrays would incorrectly claim a present empty ack set. |
| `getStats`, `getCursorStats`, attributes | Nereus/local stats; cursor-ledger IDs are `-1`. |
| `checkAndUpdateReadPositionChanged` | Atomic compare/update of the stats snapshot. |
| `isCursorDataFullyPersistable` | true for non-durable; false for durable boundary until F3. |
| `periodicRollover` | false. |
| `isMessageDeleted` | true at/below local mark-delete, false above it. |
| `duplicateNonDurableCursor` | New local cursor with copied positions/properties, no shared waiter. |
| `applyMaxSizeCap` | Estimate a count but return at least one when `maxEntries > 0`. |
| `updateReadStats` | Add to local counters with overflow saturation for metrics only. |

## 10. One-entry Read Loop

F2 uses a correctness-first one-entry L0 loop because the current `ReadBatch` does not expose a generic Pulsar
entry-splitting surface for arbitrary multi-entry batches.

Read arguments require `maxEntries > 0`; `maxSizeBytes == -1` means no byte limit, values `>= 0` are bounded, and
values `< -1` fail through the method's normal error channel. A zero byte budget still returns the first available
entry for progress. A non-null skip predicate is the locked `Predicate<Position>`，not `Predicate<Entry>`。It is
evaluated against the projected coordinate before any L0 read；a true result advances past that coordinate without
allocating an `Entry` or charging the returned entry/byte budget. Predicate evaluation and skipped positions are
still bounded by `maxScanEntries`；a predicate exception fails through the callback lane.

For each offset:

1. use one L0 `StreamMetadata` snapshot to validate trim/end and maxPosition；
2. evaluate `skipCondition.test(entryPosition(offset))` when present and, on true, advance to the next offset without
   object IO；
3. otherwise call `StreamStorage.read(offset, F2L0RequestFactory.singleEntryReadOptions(...))`；
4. require matching result stream/requested offset, one `ReadBatch`, range `[offset, offset+1)`, opaque payload,
   empty schema/projection refs, payload within `maxEntryBytes`, and `nextOffset=offset+1`；
5. wrap exact bytes in one `NereusEntry`；
6. include the first available entry even when its size exceeds the caller's `maxSizeBytes`, matching the stock
   progress behavior; subsequent entries stop before crossing the byte limit；
7. advance the cursor read position only after the entry has been accepted into the success result；
8. on any failure release every accumulated entry before the failure callback。

A zero-byte entry still counts against `maxEntries`, is returned when within `maxPosition`, and advances the cursor
to `offset + 1`; byte-budget logic must never use remaining bytes as its only loop-progress condition.

The L0 `maxEntryBytes` request must be large enough for the broker's allowed persisted entry, so first-entry progress
does not depend on `READ_LIMIT_TOO_SMALL`.

## 11. Waiters, Cache and Stats

### 11.1 Tail waiters

Installing a waiter uses register-then-recheck to close the lost-wakeup window:

```text
read snapshot -> no data
CAS install waiter
read fresh snapshot
if data now exists: remove same waiter and dispatch read
otherwise: await local append, optional watch hint, or periodic poll
```

Periodic metadata polling at `tailPollInterval` is the correctness fallback for appends performed by another broker.
An Oxia watch may wake the poll early but is never the proof of visibility. `maxPosition` exhaustion completes with an
empty list rather than waiting for an impossible bound. A fresh `SEALED` snapshot at final LAC instead fails the
pending/read-or-wait callback with `NoMoreEntriesToReadException`; `DELETING/DELETED` maps to the lifecycle error and
never remains registered.

### 11.2 F2 cache decision

F2 initially uses a zero-capacity Pulsar `EntryCacheManager`. L0 already has resolver/object read controls, and adding
a second entry cache is not required for the correctness gate. The manager returns a per-ledger zero-capacity
`EntryCache` whose insert is false, size is zero, invalidation is safe, and BookKeeper `ReadHandle` read methods fail
explicitly because no Nereus path may call them.

All locked cache methods have concrete behavior:

| Type / method group | Contract |
| --- | --- |
| manager `getEntryCache(ml)` | Null-check; return one stable zero cache by exact ledger name while that facade is live. |
| manager `removeEntryCache(name)` / `clear()` | Remove local compatibility objects only; never close a ledger or L0 cache. |
| manager `getSize()` / `getMaxSize()` | Always `0`. |
| manager `updateCacheSizeAndThreshold(maxSize)` | Reject negative; record requested value for diagnostics but keep effective max at zero. |
| manager watermark update/get | Require finite `0.0 < value <= 1.0`, store/return compatibility value; it cannot enable caching. |
| manager `doCacheEviction()` | No-op plus invocation metric; there are no entries. |
| both manager TTL-extension updates | Validate nonnegative max-times/store boolean for config observability; no cache behavior changes. |
| cache `getName()` / `getSize()` | Exact managed-ledger name / `0`. |
| cache `insert(entry)` | Null-check and return false without retaining or releasing caller-owned entry. |
| cache invalidation / `clear()` | Validate non-null position where applicable, otherwise no-op. |
| both cache `asyncReadEntry(ReadHandle,...)` methods | Do not inspect/call the handle; fail the supplied callback once with `NEREUS_UNSUPPORTED_OPERATION:cache-bookkeeper-read`. |

The manager's per-ledger map is bounded by the factory handle limit and entries are removed on writable close /
factory shutdown. A positive dynamic BookKeeper cache setting never changes F2's effective zero capacity.

### 11.3 Stats meaning

All 16 `ManagedLedgerFactoryMXBean` methods are implemented. `getNumberOfManagedLedgers` counts completed, live
writable facades. Every cache size/hit/miss/throughput/insert/eviction getter is zero under the locked cache policy;
none is repurposed for L0 resolver-cache metrics.

All 37 `ManagedLedgerMXBean` methods are implemented with these groups:

- name is the exact persistence name；stored logical size is L0 `cumulativeSize`；the legacy replicated-size getter
  returns the same logical value with replica factor one, while actual ObjectStore physical bytes/replication are
  Nereus-specific metrics；
- backlog is the saturating sum of local cursor entry backlogs; production F2 admits no durable subscription；
- add/read success, error, entry count, message count and byte totals are `LongAdder`-style local counters since this
  facade opened; rates are deltas divided by the configured stats period, never cumulative totals mislabeled as rates；
- add message counters use the supplied `numberOfMessages`; position allocation still advances once per entry；
- mark-delete counters include successful local non-durable operations only；rejected durable mutation increments an
  error metric outside the success rate；
- entry-size and end-to-end add-latency buckets use Pulsar's locked `StatsBuckets` boundaries and return defensive
  array copies；Object-WAL stage latency is exposed separately in Nereus telemetry；
- ledger-switch latency/rate is zero with correctly shaped zero buckets because v1 never rolls；
- `PendingBookieOpsStats` is a fresh all-zero value; no object/Oxia operation is labeled a pending Bookie op；
- legacy ledger-add latency mirrors end-to-end append latency only as a documented compatibility value, while the
  Nereus WAL/Oxia stage histograms remain authoritative for diagnosis.

All 13 `ManagedCursorMXBean` methods are implemented. Name/ledger name are exact. `persistToLedger`,
`persistToZookeeper`, cursor-ledger byte update methods and every corresponding getter remain zero/no-op because F2
does not persist a cursor ledger or cursor metadata; rejected durable operations are counted only in Nereus-specific
unsupported-operation metrics. Synthetic virtual-ledger info is labeled Nereus in logs/metrics and never contains
ensemble/bookie metadata.

## 12. Callback and Buffer Contract

`TerminalCallback<T>` owns one `AtomicBoolean terminal` and the operation's cleanup. Every timeout, close,
cancellation, L0 completion and recovery completion calls `tryComplete`/`tryFail`; only the winner invokes a Pulsar
callback. Cleanup runs even when the callback throws. Terminal invocation is enqueued on the ledger/cursor
`SerialCallbackLane`; completion on an Oxia/object thread never calls broker code directly.

An admitted append has no facade-side timer that can fire before L0 has classified the attempt. The facade passes
`appendTimeout` to `AppendOptions`; L0 guarantees terminal completion with `AppendOutcome` and, after a head request,
`AppendAttemptId`. Only then may the facade start `recoverAppend` with `appendRecoveryTimeout`. While its producer
callback is still non-terminal, local close drains that chain instead of emitting an ID-less failure. After the
callback has failed and background exact recovery owns the suspended attempt, local close may detach without stopping
the core recovery; terminate/delete must wait for committed/proven-uncommitted recovery or fail before lifecycle CAS.
A process-level forced shutdown may end the callback domain, but it does not claim a safely retryable producer outcome.

The implemented handoff uses two views of the same retained core attempt：the callback-budget view uses
`appendRecoveryTimeout`；after that view fails retryably uncertain，one saturated-timeout terminal observer joins the
same core terminal future。It owns no retained request、retry scheduler or physical replay，and ledger close completes
only the local generation future exceptionally；the P15 `AppendCoordinator` remains the sole recovery owner。

Append ownership:

1. validate arguments/config, reject unsupported interceptor and reserve capacity before copying/submitting；
2. copy the input readable slice without changing indices/refcount；
3. L0 owns the copied `byte[]` through immutable API values；
4. on success create a read-only callback `ByteBuf` over the exact persisted bytes；
5. invoke `addComplete(position, entryData, ctx)` outside locks；
6. release the facade-owned callback buffer after callback return。

Read ownership:

- each success `Entry` transfers one owned reference to the callback；
- after successful callback return the caller still owns and must release entries；
- failure before callback releases all entries；
- ownership transfers immediately before callback invocation; a throwing callback does not return ownership, so the
  facade must not risk a double release；
- callback failure never changes durable append outcome or triggers a second callback。

## 13. Append Outcome Mapping

| L0 result | Facade behavior |
| --- | --- |
| normal `AppendResult` | validate one-entry result and `addComplete` |
| `KNOWN_NOT_COMMITTED` | `addFailed` with mapped exception |
| `KNOWN_COMMITTED` + attempt ID | call `recoverAppend`; success becomes `addComplete` |
| `MAY_HAVE_COMMITTED` + attempt ID | call `recoverAppend`; do not submit another physical append |
| recovery proves `KNOWN_NOT_COMMITTED` | fail the callback if it is still pending; release the exact attempt/lane and clear the local fence without a second callback |
| recovery deadline expires retryably uncertain | one failure callback and transition facade to `WRITE_FENCED`; background single-flight exact recovery continues |
| permanent recovery invariant/corruption | fail once if still pending, stop retrying and leave this facade permanently `WRITE_FENCED` |
| non-known outcome without attempt ID | invariant violation and permanent `WRITE_FENCED` |

The facade never derives success merely from `committedEndOffset` advancing, because another fenced/renewed writer may
have committed that range.

## 14. Error Channels

`ManagedLedgerErrorMapper` provides four forms from one classification table:

Target mapper signatures below omit implementation bodies:

```text
public record OperationContext(
        String operation,
        boolean factoryOperation,
        boolean directRead,
        Optional<StreamState> observedStreamState) {
}

ManagedLedgerException map(Throwable error, OperationContext context);
ManagedLedgerException unsupported(String operation);
<T> CompletableFuture<T> failedFuture(ManagedLedgerException error);
void invokeFailureCallback(...);
```

The pipeline recursively unwraps `CompletionException`/`ExecutionException` but retains the original Nereus cause.
Phase 1.5 provider-neutral primary-WAL availability errors preserve their retriable flag in a generic
`ManagedLedgerException`；target-not-found/checksum mismatch maps non-recoverably, and
`UNSUPPORTED_READ_TARGET` follows the explicit unsupported path。The current Object-WAL-specific mappings remain
unchanged。
When `STREAM_NOT_ACTIVE` arrives without a trustworthy state snapshot, the async operation refreshes
`StreamStorage.getStreamMetadata` before calling the mapper: `SEALED` maps to
`ManagedLedgerTerminatedException`, `DELETING/DELETED` to closed/not-found by operation, and `CREATING` to a
retriable generic managed-ledger exception. `OFFSET_NOT_AVAILABLE` similarly uses `directRead` versus cursor-tail
context. No mapping branch inspects exception-message text.

Async overloads validate arguments but report validation through their callback/future. Sync overloads throw checked
`ManagedLedgerException` where the interface permits it. Getter/void methods without a checked channel use only a
documented neutral result (`N`) or a stable runtime unsupported exception (`U`); they never fabricate BookKeeper state.

## 15. Contract Gates

The facade contract passed F2-M6 after tests proved the list below；`05` assigns each unit、contract or broker
integration case to its owning milestone and final composition：

1. the winning open acquires one binding permit; generation mismatch fails before L0 IO and publish-time permit
   invalidation leaves no topic projection；
2. a reflection/Javap snapshot accounts for every locked factory/ledger/read-only/cursor abstract/default method；
3. every `U` method uses the correct checked, callback, future or runtime failure channel；
4. `getFirstPosition`, LAC, one-past-tail and trim boundary match stock one-ledger behavior；
5. first-entry-over-byte-limit progress and subsequent byte clipping match broker dispatcher expectations, while
   zero-byte entries still consume entry budget and advance position；
6. cancel does not invoke a pending read callback, while close fails it exactly once；
7. non-local append wakeup succeeds through polling with watch delivery disabled；
8. interceptor/offload/shadow/auto-skip configurations and read-compacted cursor arguments fail before storage IO；
9. `MAY_HAVE_COMMITTED` and `KNOWN_COMMITTED` recover the same physical attempt or fence writes；a later proven
   `KNOWN_NOT_COMMITTED` releases the suspended lane without issuing a second callback, while a permanent invariant
   leaves that facade fenced；
10. replay recovery older than one commit-scan page advances an anchored continuation, waits for the original mutation
   runner to quiesce and never falsely proves non-commit while a late original CAS can still land；
11. zero-capacity cache paths never receive a virtual ID as a BookKeeper handle/ledger ID；
12. all callback and `ByteBuf` race tests finish with one terminal signal and zero facade-owned leaked references；
    tests whose callback throws must explicitly release any entry ownership they accepted；
13. append callbacks preserve accepted-operation order even when L0 futures and timeouts complete on different
    threads；pending-operation admission is bounded before byte copying；
14. `inspectStorageState` is write-free for every state, returns missing only when the projection is absent, and
    reports projection-without-L0 plus identity/mirror mismatches as invariants；
15. fence generations cannot be cleared by stale completion or `readyToCreateNewLedger()`，and a broker integration
    test keeps the topic fenced until `NereusWriteFenceView.awaitWriteFence` reaches an exact terminal；
16. position-skip predicates run before L0 read/allocation, predicate failure uses the callback channel, and an
    all-skipped visible range installs a lost-wakeup-safe waiter；
17. both protobuf and admin `LedgerInfo` DTOs are compiled and asserted independently so no field from one type is
    accidentally assigned to the other；
18. the no-config factory opens reuse the constructor-supplied default `ManagedLedgerConfig`，and runtime construction
    succeeds before a Pulsar broker ID exists while producing a unique writer ID per process run；
19. a stale facade snapshot advances end and cumulative size from one normal/recovered `AppendResult` without a
    second metadata read；local overlay versus remote metadata races never regress either value and never confuse
    commit version with backend metadata version。
20. committed-response loss after the real L0 head CAS recovers the retained attempt through the facade and produces
    exactly one successful callback、one Position and exact readable bytes；
21. both derived projection keys may be removed from real Oxia，followed by complete client-runtime close/reconnect and
    authority-only idempotent repair；
22. local close followed by harness-driven L0 trim and reopen preserves the virtual ledger/incarnation，rejects old
    Positions and leaves retained entry IDs unchanged；
23. terminate returns one stable final LAC and rejects append，while logical delete leaves the committed Object-WAL
    object physically present；
24. same-name recreation after delete uses the next incarnation、new stream and virtual-ledger ID，rejects an old
    Position and restarts entry numbering only inside the new Position namespace。
