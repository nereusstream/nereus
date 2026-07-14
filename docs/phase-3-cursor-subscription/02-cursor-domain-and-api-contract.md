# Cursor Domain and Code-level API Contract

## 1. Coordinate Model

F3 只使用 F2 已锁定的一个坐标空间：

```text
CursorLedgerIdentity
  managedLedgerName (exact opaque UTF-8)
  managedLedgerNameHash
  ManagedLedgerProjectionIdentity
    storageClassBindingGeneration
    incarnation
    streamId
    virtualLedgerId

Position(virtualLedgerId, entryId)
  entryId == persisted Pulsar Entry's stream offset
```

一个 Pulsar batch 仍是一个 persisted Entry 和一个 offset。`batchIndex` 只存在于 Entry payload 和
`Position` extension；F3 不创建 `(offset,batchIndex)` sub-offset。

### 1.1 Durable and local coordinates

```text
markDeleteOffset
  first persisted-entry offset not cumulatively acknowledged

wholeAckRanges
  fully acknowledged offsets >= markDeleteOffset
  sorted + disjoint + non-adjacent half-open [start,end)

partialBatchAcks[offset]
  batchSize + remaining-message bitset
  only for offsets >= markDeleteOffset that are not in wholeAckRanges

localReadOffset
  next broker-local read candidate
  never encoded in CursorStateRecord or snapshot
```

API conversion：

```text
getMarkDeletedPosition() =
    PositionFactory.create(virtualLedgerId, Math.subtractExact(markDeleteOffset, 1))

getReadPosition() =
    positionProjection.readPosition(virtualProjection, localReadOffset, validatedL0Snapshot)
```

`markDeleteOffset` is nonnegative，so its Position projection is at least legal entry ID `-1`。Concrete Entry reads
use the existing `PositionProjection.entryPosition/requireReadableEntryOffset` methods；no new conversion helper or
second coordinate formula is introduced。

`PositionFactory.EARLIEST/LATEST` 只能在 API ingress 解析，不能进入 durable record。

### 1.2 Ack normalization

Every state accepted by constructors/codecs/mutations satisfies：

1. `trimOffset <= markDeleteOffset <= committedEndOffset`；
2. every whole range has `markDeleteOffset <= start < end <= committedEndOffset`；
3. ranges are sorted by start, disjoint and non-adjacent；adjacent ranges are merged；
4. no partial offset is below mark-delete or covered by a whole range；
5. partial map offsets are strictly increasing and unique；
6. `batchSize > 0` and every set bit is `< batchSize`；
7. empty remaining bitset is promoted to `[offset,offset+1)` and removed from partial map；
8. a remaining bitset equal to the exact masked all-ones state means no index was acknowledged and is removed from
   the partial map；for cumulative ack it may still advance mark-delete to that offset；
9. a whole range beginning at mark-delete is folded into mark-delete repeatedly；
10. state beyond the current committed end is corruption, not a future-position reservation。

## 2. Package and File Boundary

The M1 value/domain/snapshot subset in this section now exists and is final-gated。The M2 storage/state-machine and
M3 facade types remain planned under the same boundary；a literal Java name collision may change spelling，but
semantic ownership must not change。

```text
nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/
  CursorStorage.java
  DefaultCursorStorage.java
  CursorProtocolActivationGuard.java
  CursorLedgerIdentity.java
  CursorOwnerSession.java
  CursorIdentity.java
  CursorHandle.java
  CursorState.java
  CursorLifecycle.java
  CursorAckState.java
  AckNormalizer.java
  AckWords.java
  CursorOpenRequest.java
  InitialCursorPosition.java
  CursorAckRequest.java
  CursorResetRequest.java
  CursorMutationResult.java
  CursorMutationOutcome.java
  CursorPropertyMutation.java
  BatchAckState.java
  OffsetRange.java
  CursorStateMachine.java
  CursorStatePersistencePlanner.java
  CursorSnapshotStore.java
  DefaultCursorSnapshotStore.java
  CursorSnapshotCodecV1.java
  CursorSnapshotReference.java
  CursorSnapshotWriteRequest.java
  CursorRetentionView.java
  CursorMutationLane.java
  CursorRetentionCoordinator.java
  DefaultCursorRetentionCoordinator.java
  CursorStorageConfig.java
```

Facade files added/modified in M3：

```text
NereusManagedCursor.java
NereusManagedCursorStats.java
NereusManagedLedger.java
NereusLedgerOpenResult.java
NereusWritableLedgerOpenResult.java
NereusManagedLedgerOwnershipGuard.java
NereusManagedLedgerOpenCoordinator.java
NereusManagedLedgerFactory.java
NereusManagedLedgerRuntime.java
NereusManagedLedgerFactoryConfig.java
ManagedLedgerModule.java
errors/ManagedLedgerErrorMapper.java
```

## 3. Immutable Domain Types

The following signatures are pseudocode precise enough to drive implementation. Public constructors perform all
validation and defensively copy arrays/maps/lists。

```java
public record CursorLedgerIdentity(
        String managedLedgerName,
        String managedLedgerNameHash,
        ManagedLedgerProjectionIdentity projection) {

    public CursorLedgerIdentity {
        managedLedgerName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        if (!ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName)
                .equals(managedLedgerNameHash)) {
            throw new IllegalArgumentException("managed-ledger name/hash mismatch");
        }
        Objects.requireNonNull(projection);
    }
}

public record CursorOwnerSession(
        CursorLedgerIdentity ledger,
        String ownerSessionId) {

    public CursorOwnerSession {
        Objects.requireNonNull(ledger);
        ownerSessionId = CursorIds.requireRandomId(ownerSessionId, "ownerSessionId");
    }
}

public record CursorIdentity(
        CursorLedgerIdentity ledger,
        String cursorName,
        String cursorNameHash,
        long cursorGeneration) {

    public CursorIdentity {
        Objects.requireNonNull(ledger);
        cursorName = CursorNames.requireCursorName(cursorName);
        if (!CursorNames.cursorNameHash(cursorName).equals(cursorNameHash)) {
            throw new IllegalArgumentException("cursor name/hash mismatch");
        }
        if (cursorGeneration <= 0) throw new IllegalArgumentException(...);
    }
}

public record OffsetRange(long startOffset, long endOffset) {
    public OffsetRange {
        if (startOffset < 0 || endOffset <= startOffset) {
            throw new IllegalArgumentException(...);
        }
    }
}

public record BatchAckState(int batchSize, long[] remainingWords) {
    public BatchAckState {
        if (batchSize <= 0) throw new IllegalArgumentException(...);
        remainingWords = AckWords.canonicalCopy(batchSize, remainingWords);
    }

    @Override
    public long[] remainingWords() {
        return remainingWords.clone();
    }

    public boolean isWholeEntryAcknowledged() {
        return remainingWords.length == 0;
    }
}

public record CursorAckState(
        long markDeleteOffset,
        List<OffsetRange> wholeAckRanges,
        NavigableMap<Long, BatchAckState> partialBatchAcks) {

    public CursorAckState {
        wholeAckRanges = AckNormalizer.copyAndValidateRanges(markDeleteOffset, wholeAckRanges);
        partialBatchAcks = AckNormalizer.copyAndValidatePartial(...);
    }
}
```

`AckWords.canonicalCopy` uses Java `BitSet.valueOf(words).toLongArray()` canonical trimming. It rejects a negative
logical word count, words above the configured limit, and any set bit at or beyond `batchSize`。Arrays returned to
Pulsar are always clones。`BatchAckState` may temporarily represent empty/all-ones request shapes，but
`CursorAckState` durable normalization stores neither：empty becomes whole-acked and exact all-ones becomes absent。

The effective hydrated state is：

```java
public record CursorState(
        CursorIdentity identity,
        String ownerSessionId,
        CursorLifecycle lifecycle,
        long mutationSequence,
        long ackStateEpoch,
        String lastProtectionAttemptId,
        CursorAckState acknowledgements,
        Map<String, Long> positionProperties,
        Map<String, String> cursorProperties,
        Optional<CursorSnapshotReference> snapshotReference,
        long createdAtMillis,
        long updatedAtMillis,
        long metadataVersion) {

    public CursorState {
        // Immutable copies; lifecycle ACTIVE for a usable handle.
        // metadataVersion is hydrated from Oxia and is never encoded into record bytes.
    }
}

public enum CursorLifecycle {
    ACTIVE,
    DELETED
}
```

`ownerSessionId` is a random 128-bit lowercase-hex writable-open fence。It changes only during the open-time owner
claim or when a new/recreated cursor is born under the current owner；it is not part of MessageId/cursor identity or
snapshot bytes。Every durable mutation requires the handle session to match the authoritative root。

`ackStateEpoch` starts at `1` for each new cursor generation and increments only when the ack state is destructively
replaced：durable reset (forward or backward) and clear-backlog。Cumulative/individual ack、skip、snapshot spill and
property-only mutation preserve it。A CAS retry may rebase a monotonic ack only while this epoch is unchanged；this is
the durable proof that a concurrent reset did not erase/redefine the base state。Delete retains it for diagnostics；
recreate starts a new generation at epoch `1`。

`metadataVersion` is the Oxia version returned with the value。The encoded
`CursorStateRecord.metadataVersion` field is always zero and is rejected when nonzero on encode；it is not a second
CAS token。

Snapshot domain values use generic ObjectStore types rather than provider-specific metadata：

```java
public record CursorSnapshotReference(
        ObjectKey objectKey,
        String snapshotId,
        long cursorGeneration,
        long sourceMutationSequence,
        long baseMarkDeleteOffset,
        long objectLength,
        Checksum storageChecksum,
        int formatCrc32c,
        int formatVersion,
        long createdAtMillis) {
}

public record CursorSnapshotWriteRequest(
        CursorIdentity identity,
        long sourceMutationSequence,
        CursorAckState fullState,
        long createdAtMillis) {
}

public interface CursorSnapshotStore extends AutoCloseable {
    CompletableFuture<CursorSnapshotReference> write(CursorSnapshotWriteRequest request);

    CompletableFuture<CursorAckState> read(
            CursorSnapshotReference reference,
            CursorIdentity expectedIdentity);

    @Override
    void close();
}
```

Production construction is explicit：

```java
public DefaultCursorSnapshotStore(
        String cluster,
        ObjectStore objectStore,
        CursorStorageConfig cursorConfig,
        Duration objectStoreRequestTimeout,
        Clock clock);
```

The store captures one `cursorSnapshotOperationTimeout` deadline per `write`/`read`。Each PUT/HEAD/range-read receives
`min(remaining snapshot deadline, objectStoreRequestTimeout)`；timeout is recomputed before every subcall and a
nonpositive remainder fails without issuing IO。Production random IDs use `SecureRandom` internally；package-private
tests inject the deterministic ID source。`cluster` is canonicalized exactly as the owning runtime cluster and the
object-store request timeout must be positive。

Constructors validate positive generation/sequence，exact identity/hash/checksum/version and defensive copies。
`write` owns random snapshot-ID generation、encode、immutable PUT and HEAD verification；`read` owns HEAD、one exact
full-object range read and strict decode。Neither method publishes visibility；only the later cursor-root CAS does。

## 4. CursorStorage API

F3 keeps Pulsar types out of metadata/storage APIs. `NereusManagedCursor` converts `Position` before calling them。

`DefaultCursorStorage` is constructed with the shared F2 `ManagedLedgerProjectionMetadataStore` and a
protocol-neutral activation guard：

```java
@FunctionalInterface
public interface CursorProtocolActivationGuard {
    CompletableFuture<Void> acquireFirstActivationPermit(CursorLedgerIdentity ledger);

    static CursorProtocolActivationGuard unavailable() {
        return ledger -> CompletableFuture.failedFuture(
                new IllegalStateException("NEREUS_CURSOR_CAPABILITY_NOT_READY"));
    }
}
```

`unavailable()` is the named fail-closed F2 source-compatibility bridge，not a production activation permit；it is
used only until the M4 fork supplies the real capability coordinator。

Production constructor ownership is explicit：

```java
public DefaultCursorRetentionCoordinator(
        String cluster,
        StreamStorage streamStorage,
        ManagedLedgerProjectionMetadataStore projectionStore,
        CursorMetadataStore metadataStore,
        CursorSnapshotStore snapshotStore,
        CursorProtocolActivationGuard activationGuard,
        CursorStateMachine stateMachine,
        CursorStorageConfig config,
        Clock clock,
        ScheduledExecutorService scheduler);

public DefaultCursorStorage(
        String cluster,
        StreamStorage streamStorage,
        ManagedLedgerProjectionMetadataStore projectionStore,
        CursorMetadataStore metadataStore,
        CursorSnapshotStore snapshotStore,
        CursorRetentionCoordinator retentionCoordinator,
        CursorProtocolActivationGuard activationGuard,
        CursorStateMachine stateMachine,
        CursorStatePersistencePlanner persistencePlanner,
        CursorStorageConfig config,
        Clock clock,
        ScheduledExecutorService scheduler);
```

All dependencies are non-null and caller-owned；these two close methods initiate drain/cancel only for their own
lanes/tasks and do
not double-close shared `StreamStorage`、metadata、snapshot or scheduler instances。`cluster` must exactly equal the
owning `NereusManagedLedgerRuntime.cluster()`。Tests inject deterministic clocks/ID sources through package-private
constructors；the public production path uses cryptographically random 128-bit owner-session、attempt and snapshot
IDs。

`NereusManagedLedgerOpenCoordinator.openWritable` creates a fresh cryptographically random `CursorOwnerSession` for
every writable ledger instance and never reuses it across close/reopen。The existing base `open` path remains
projection/L0-only for get-only/read-only inspection and never claims cursor roots。`claimAndLoadActiveCursors(owner)` first claims the retention root，
then bounded-parallel CAS-claims every ACTIVE cursor root to that same session，stabilizes/rescans，strictly hydrates
snapshots and only then returns handles。A claim preserves ack state、generation、`ackStateEpoch`、properties、snapshot
ref and protection proof；it checked-increments only root mutation sequence and updated time。DELETED tombstones need
not be rewritten because recreate is already serialized by the claimed retention root。

The real M4 guard performs two consecutive all-persistent-broker capability snapshots；the second is the broker-set/
version stability recheck，and the returned future completes only after both pass。M2 tests inject an explicit fake；
there is no implicit allow-all production default。For an
ABSENT/DELETED -> ACTIVE transition，`DefaultCursorStorage` first reads the authoritative topic projection。It calls
the guard only when the monotonic marker is absent，then CAS-activates the marker before creating retention/cursor
state。If the exact marker already exists，including recreate or crash-resume，no cluster-wide snapshot is required。
Opening an existing ACTIVE root likewise never depends on cluster-wide capability availability。

```java
public interface CursorStorage extends AutoCloseable {

    CompletableFuture<CursorHandle> open(
            CursorOwnerSession owner,
            String cursorName,
            CursorOpenRequest request);

    CompletableFuture<List<CursorHandle>> claimAndLoadActiveCursors(
            CursorOwnerSession owner);

    CompletableFuture<CursorMutationResult> cumulativeAck(
            CursorHandle handle,
            CursorAckRequest request);

    CompletableFuture<CursorMutationResult> individualAck(
            CursorHandle handle,
            List<CursorAckRequest> requests);

    CompletableFuture<CursorMutationResult> reset(
            CursorHandle handle,
            CursorResetRequest request);

    CompletableFuture<CursorMutationResult> clearBacklog(
            CursorHandle handle,
            long observedCommittedEndOffset);

    CompletableFuture<CursorMutationResult> mutateCursorProperties(
            CursorHandle handle,
            CursorPropertyMutation mutation);

    CompletableFuture<CursorMutationResult> flushPositionProperties(
            CursorHandle handle,
            Map<String, Long> stagedProperties);

    CompletableFuture<Void> delete(
            CursorOwnerSession owner,
            String cursorName);

    CompletableFuture<CursorRetentionView> retentionView(
            CursorOwnerSession owner);

    @Override
    void close();
}
```

Every returned future is non-null. Methods never throw a domain/IO error synchronously；only programmer errors such
as a null argument may throw before a future is created。`close()` is idempotent and rejects new operations with a
completed-exceptionally future。

Every method carrying a `CursorOwnerSession` verifies exact ledger identity and owner ID。A cursor/retention root held
by a different session is claimed only by the open-time protocol；ordinary operations fail fenced and never steal
ownership。A handle mutation whose root session changed fails `ManagedLedgerFencedException` even if generation and
Oxia version would otherwise permit a retry。Durable delete first requires the current retention owner，so a stale
broker cannot report an authoritative delete after takeover。Within that owner session，an absent key or an
already-DELETED tombstone returns idempotent success after exact key/name/projection validation；ACTIVE additionally
requires the cursor root to carry that same owner before CAS。

`CursorStorage.retentionView(owner)` is an owner-scoped topic-runtime read and is not the F4 planner API。A read-only
F4 planner/GC worker reads `VersionedCursorRetention` plus paged `VersionedCursorState` directly through
`CursorMetadataStore`，builds its own immutable snapshot and revalidates all observed versions/session IDs before its
action boundary。It must not fabricate a `CursorOwnerSession` or weaken this interface's owner check for reuse。

`CursorStorage.delete` is exclusively the durable-record operation。The ManagedLedger facade must resolve a
registered non-durable cursor locally and never pass its name to this method；otherwise analyze-backlog cleanup would
create permanent tombstones for temporary cursors。

Cursor-property request shape is explicit rather than a mode plus ambiguous map：

```java
public sealed interface CursorPropertyMutation {
    record Put(String key, String value) implements CursorPropertyMutation {}
    record Remove(String key) implements CursorPropertyMutation {}
    record ReplaceExternal(Map<String, String> properties) implements CursorPropertyMutation {}
}
```

Every variant validates/copies at construction；`Remove` of an absent key is idempotent。

The retention-facing managed-ledger values are exact and contain no mutable metadata record：

```java
public record CursorRetentionView(
        CursorLedgerIdentity ledger,
        String ownerSessionId,
        Lifecycle lifecycle,
        long mutationSequence,
        long metadataVersion,
        long protectedFloorOffset,
        long lastCompletedTrimOffset,
        Optional<PendingProtection> pendingProtection,
        Optional<PendingTrim> pendingTrim) {

    public enum Lifecycle { ACTIVE, PROTECTION_PENDING, TRIM_PENDING }

    public record PendingProtection(
            String attemptId,
            Kind kind,
            String cursorNameHash,
            long targetCursorGeneration,
            long targetMarkDeleteOffset) {
        public enum Kind { CREATE, RECREATE, BACKWARD_RESET }
    }

    public record PendingTrim(
            String attemptId,
            long targetTrimOffset,
            String composedReason) {}
}

public interface CursorRetentionCoordinator extends AutoCloseable {
    CompletableFuture<CursorRetentionView> claimAndRecover(
            CursorOwnerSession owner);

    CompletableFuture<ProtectionLease> beginProtection(
            CursorOwnerSession owner,
            ProtectionRequest request);

    CompletableFuture<CursorRetentionView> completeProtection(
            ProtectionLease lease);

    CompletableFuture<CursorRetentionView> reconcileFloor(
            CursorOwnerSession owner);

    CompletableFuture<CursorRetentionView> requestTrim(
            CursorOwnerSession owner,
            long candidateOffset,
            String reason);

    @Override
    void close();

    record ProtectionRequest(
            CursorRetentionView.PendingProtection.Kind kind,
            String cursorName,
            String cursorNameHash,
            long expectedCursorGeneration,
            long targetCursorGeneration,
            long targetMarkDeleteOffset,
            Optional<BatchAckState> targetPartialBatch,
            Map<String, Long> initialPositionProperties,
            Map<String, String> initialCursorProperties) {}

    record ProtectionLease(
            CursorOwnerSession owner,
            String attemptId,
            long retentionMetadataVersion) {}
}
```

Constructors validate/copy every field and enforce the lifecycle-specific shape from document 03。
`CursorRetentionView.metadataVersion` is the nonnegative Oxia version hydrated beside the encoded record；like
`CursorState.metadataVersion`，it is never written into the payload's required-zero metadataVersion field。
`claimAndRecover` first stabilizes the marker/cursor scan。If the root is absent and there are no cursor records，it
absent-creates an ACTIVE owner-only root at current L0 trim under the supplied session；this is allowed whether the
projection marker is absent or present。If the root exists，it CAS-claims it to the supplied owner session before
pending recovery，and pending recovery happens before completion。It therefore always returns a non-null view for a
successful writable open。`beginProtection` persists the complete intent and returns only after
ACTIVE -> PROTECTION_PENDING CAS。
`completeProtection` rereads the pending root and exact cursor key，proves the attempt from the cursor root (or the
documented delete-winner case)，then CASes the same intent to ACTIVE。`requestTrim` is an internal handoff API only；
F3 does not route Pulsar policy/admin housekeeping to it。Completion is idempotent：an already-ACTIVE root succeeds
only when the cursor/delete-winner evidence proves this lease's attempt；a different pending attempt is busy/conflict，
never success-by-absence。`PendingTrim.composedReason` is the exact bounded string durably supplied to L0；it starts
with `nereus-cursor-retention/{attemptId}:` and is retained so crash recovery never invents a replacement reason。

For the future F4 caller，`requestTrim` owns the unactivated-marker race：the current writable open has already
created/claimed the owner-only retention root。If the projection marker is absent，`requestTrim` first uses the
injected activation guard and CASes the same monotonic topic marker before attempting TRIM_PENDING。It rechecks
projection identity/state、root owner and L0 trim after each boundary；an absent root at this point is an invariant
failure that forces a fresh writable open，not permission for direct trim。Thus F4 never trims an uncoordinated topic
while a first cursor is concurrently establishing protection。This path is dormant in F3 because no broker/admin
policy calls `requestTrim`。

### 4.1 Open request

```java
public record CursorOpenRequest(
        InitialCursorPosition initialPosition,
        Map<String, Long> initialPositionProperties,
        Map<String, String> initialCursorProperties,
        long observedTrimOffset,
        long observedCommittedEndOffset) {
}

public sealed interface InitialCursorPosition {
    record Earliest() implements InitialCursorPosition {}
    record Latest() implements InitialCursorPosition {}
    record AtOffset(long nextReadOffset) implements InitialCursorPosition {}
}
```

The public ManagedLedger facade normalizes null `InitialPosition` to Latest and null initial position/cursor property
maps to empty maps before constructing this internal record。The internal record itself has no nullable fields。

Creation computes first-unacked offset：

| Initial value | `markDeleteOffset` |
| --- | --- |
| Earliest | `observedTrimOffset` |
| Latest / null default | `observedCommittedEndOffset` |
| explicit durable cursor target | `clamp(nextReadOffset, trimOffset, committedEndOffset)` |

The F2 `newNonDurableCursor(Position startCursorPosition)` contract remains different: an explicit position is
already consumed, so its local next read is `entryId + 1` clamped to trim。F3 durable reset/seek explicit Position is
a direct next-read target and does not apply that increment。

Opening an existing ACTIVE cursor ignores all initial fields and returns current durable state. Opening a DELETED
tombstone starts a new generation only after the retention coordinator protects the new initial offset。

`claimAndLoadActiveCursors` never activates an unactivated topic，but every successful writable open creates or claims
the owner-only ACTIVE retention root before returning an empty cursor list。A missing-name `delete` returns success
without marker/cursor mutation after that stabilized context。Only an ABSENT/DELETED -> ACTIVE create path may invoke
`CursorProtocolActivationGuard` and add the marker within F3's broker-visible surface；the reserved future
`requestTrim` activation path above is the only other owner。The preactivation root is a fencing record，not evidence
that a durable cursor protocol has been activated。

### 4.2 Ack request

```java
public record CursorAckRequest(
        long entryOffset,
        Optional<BatchAckState> batchAck,
        Map<String, Long> positionProperties) {
}
```

- `batchAck.empty()` means whole persisted Entry；
- present nonempty words mean partial batch remaining bits；
- present empty words normalize immediately to whole Entry；
- for cumulative `markDelete`，`positionProperties` is the already-resolved complete map (possibly empty) and commits
  atomically；individual requests require it empty；
- the request carries no Pulsar `Position`, ledger ID or subscription type。

Before building a partial request, facade validation must：

1. require the Position ledger ID equals the projection virtual ledger ID；
2. require offset lies in `[trimOffset, committedEndOffset)`；
3. read the exact committed Entry through F1/F2；
4. parse Pulsar metadata and require it is a batch；
5. require exact `batchSize` and canonical remaining words；
6. release the Entry/ByteBuf on every success/failure path。

The storage rechecks committed bounds immediately before CAS. A trim that overtakes the ack maps to an already-gone
or invalid-position outcome; it never recreates a reference to trimmed data。

### 4.3 Reset request

```java
public record CursorResetRequest(
        long nextReadOffset,
        Optional<BatchAckState> targetBatchAck,
        boolean force,
        long observedTrimOffset,
        long observedCommittedEndOffset) {
}
```

For an ordinary reset, `nextReadOffset` becomes `markDeleteOffset` and all old whole/partial holes are cleared。If the
target Position carries an ack set, only that exact offset's validated partial state is retained。Cursor properties
survive；mark-delete position properties become empty, matching non-compaction stock reset behavior。

Facade ingress normalizes the public Position before constructing this request：

| Public reset input | `force=false` | `force=true` before F4 |
| --- | --- | --- |
| `PositionFactory.EARLIEST` | current L0 `trimOffset` | current L0 `trimOffset` |
| `PositionFactory.LATEST` | current `committedEndOffset` | current `committedEndOffset` |
| current-ledger offset in `[trim,end]` | exact direct next-read offset | exact direct next-read offset |
| current-ledger offset `< trim` | advance to `trimOffset` | fail `InvalidCursorPositionException` |
| current-ledger offset `> end` | normalize to `committedEndOffset` | fail `InvalidCursorPositionException` |
| foreign ledger/projection | fail | fail |

An ack-set extension is admitted only on a retained Entry target `< committedEndOffset` and is decoded/validated as
in section 4.2；EARLIEST/LATEST or one-past-tail cannot carry it。The normalized Position returned as reset callback
context is the projected direct target，not the caller's trimmed/future input。

`force=true` does not invent bytes. Before F4 compacted storage exists, a target `< trimOffset` fails with
`InvalidCursorPositionException`；it is not silently accepted merely because force was requested。

A backward reset must encode its full recovery intent，including target partial words，within
`cursorProtectionIntentMaxBytes` before changing retention。The configured batch-index/name caps are validated so any
admitted target fits；an Entry above the batch-index cap rejects partial ack/reset at ingress，while whole-entry ack
remains valid。No path truncates the target or leaves an incomplete pending record。Forward reset does not need a
protection intent。

## 5. CursorHandle and Mutation Lane

```java
public final class CursorHandle {
    CursorIdentity identity();
    CursorOwnerSession owner();
    CursorState state();
    CompletableFuture<Void> closeAsync();
}
```

Each handle owns one bounded `CursorMutationLane`：

```java
final class CursorMutationLane {
    <T> CompletableFuture<T> submit(Supplier<CompletableFuture<T>> operation);
    int pendingOperations();
    void close(Throwable cause);
}
```

Rules：

- FIFO within one local cursor handle；no monitor is held across IO/CAS/callback；
- queue admission is bounded；overflow returns `TooManyRequestsException` before state mutation；
- cross-broker serialization is Oxia version-CAS plus the open-time owner-session claim；the lane is not a distributed
  lock and a stale session cannot mutate a newly claimed root；
- a successful result replaces handle state only if identity generation and mutation sequence match the submitted op；
- stale completions cannot overwrite a newer local state；
- M2 `CursorHandle.closeAsync()` marks the handle terminal、rejects new and queued mutations，and completes only after
  the already-running admitted mutation finishes；the later M3 ledger/managed-cursor shutdown owns the outer close
  deadline and waiter/callback cancellation；
- callback dispatch is on the ManagedLedger callback executor, outside the lane and exactly once。

## 6. Mutation Result and Retry Contract

```java
public enum CursorMutationOutcome {
    APPLIED,
    ALREADY_APPLIED
}

public record CursorMutationResult(
        CursorMutationOutcome outcome,
        CursorState state) {
}
```

Ack retries are monotonic and may reload/recompute on CAS conflict：

- if the current effective state already subsumes the original cumulative/individual ack, return
  `ALREADY_APPLIED`；
- if the same cursor generation still has the operation's original `ackStateEpoch`，recompute against the latest
  state until deadline or retry budget；
- if `ackStateEpoch` changed and the request is not already subsumed，fail conflict rather than recreating an ack that
  a reset deliberately erased；
- CAS success followed by response loss is therefore safe for a monotonic ack to retry；destructive operations use
  the stricter semantic-result/epoch rules below rather than ack subsumption。

Non-monotonic/destructive updates do not blindly rebase：

- forward reset CAS conflict/uncertain response -> success only when latest has the requested mark-delete、empty
  holes/ref、target partial、empty position properties and `capturedAckStateEpoch+1`；a later cursor-property-only
  change may be preserved and does not invalidate that proof。Otherwise reapply only when `ackStateEpoch` is still
  the captured value，and fail `ConcurrentFindCursorPositionException` when another destructive transition advanced
  it；
- backward reset first durably freezes a complete `PROTECTION_PENDING` intent。Conflict before acquiring that intent
  is busy；after acquisition，same-generation CAS loss is recovered by reapplying that exact intent，not by inventing a
  new target；
- full cursor-property replacement CAS conflict -> `BadVersionException` unless result is already exact；
- delete is idempotent: missing or DELETED -> success；
- create after tombstone always changes generation and cannot be mistaken for retry of old generation。

No retry loop is infinite. It is bounded by `cursorMetadataOperationTimeout` and `cursorMaxCasAttempts`；deadline
expiry maps to a storage/timeout failure without callback success。

## 7. ManagedCursor Exact Behavior

### 7.1 Identity and lifecycle

| Method | Durable cursor behavior |
| --- | --- |
| `getName()` | exact encoded name passed by ManagedLedger；never decode/normalize |
| `getManagedLedger()` | owning `NereusManagedLedger` |
| `isDurable()` | true |
| `getLastActive/updateLastActive` | broker-local epoch-millis wall-clock field；update uses current time and performs no metadata write |
| `setActive/setInactive/setAlwaysInactive/isActive` | local dispatcher/cache hint |
| `isClosed()` | local lifecycle |
| `close/asyncClose` | enter terminal CLOSING, reject new work, drain accepted lane, flush staged position properties, close waiter/lane, keep durable state |
| `duplicateNonDurableCursor` | create an F2 non-durable cursor at the durable mark-delete boundary and copy effective whole ranges + partial-batch state；never copy dispatch-ahead local read offset |

### 7.2 Reads

Read operations start at `localReadOffset`, use F2 committed bounds and skip every wholly acknowledged offset。A
partial batch Entry is returned with the current remaining ack words available to the dispatcher；the Entry payload is
not rewritten。

After a successful read callback, `localReadOffset` advances to the first offset after the returned range. No Oxia
write occurs. If the callback fails or returned entries are released due to cancellation, local advance follows the
same locked F2 read contract and must be covered by tests。

On durable open/failover：

```text
localReadOffset = first offset >= markDeleteOffset
                  not covered by a whole ack range
```

A partial batch entry remains that first offset. This may redeliver unacked messages and is required for safety。

Position-valued getters are exact：

```text
getReadPosition()
  = Position(virtualLedgerId, localReadOffset)

getMarkDeletedPosition()
getPersistentMarkDeletedPosition()
  = Position(virtualLedgerId, markDeleteOffset - 1)

getFirstPosition()
  = owning ManagedLedger.getFirstPosition()
  = Position(virtualLedgerId, current trimOffset - 1)
```

For a durable cursor，`getPersistentMarkDeletedPosition()` never returns `null` and equals
`getMarkDeletedPosition()` because every published state is already backed by the cursor root。Entry ID `-1` is the
legal before-origin coordinate when trim/mark-delete is zero；it is never passed to an Entry read。

| Method | Contract |
| --- | --- |
| sync/async `readEntries` | bounded by entry count, byte size and `maxPosition`; filter whole ack ranges |
| `readEntriesOrWait` variants | same read, then F2 coalesced tail waiter when no dispatchable committed Entry |
| `asyncReadEntriesWithSkip*` | combine caller predicate with cursor whole-delete predicate; release skipped Entry |
| `cancelPendingReadRequest` | local waiter cancellation only |
| `hasMoreEntries` | committed dispatchable Entry exists at/after local read offset |
| `getNthEntry` | resolve Nth from first unacked; Include/Exclude controls whole individual ranges |
| find/scan | F1 bounded resolver; SearchActive starts at durable first-unacked, SearchAll at retained trim |
| replay | foreign/trimmed/whole-deleted positions returned in skipped set; partial Entry is returned |

Every sync read/find/replay wrapper waits on the same async core and adds no second mutation path。The explicit
start/end `asyncFindNewestMatching` overload validates both bounds in the same projection and searches only that
inclusive range。`isFindFromLedger` does not select a cursor ledger in Nereus；both values route to the same F1
committed-entry resolver。

`scan(start, predicate, batchSize, maxEntries, timeoutMs)` validates positive bounds，uses the supplied direct start
or first-unacked when absent，does not move local/durable cursor state，and releases every Entry after predicate
evaluation。It returns `COMPLETED` at committed end、`USER_INTERRUPTED` when the predicate returns false and
`ABORTED` on the entry/deadline cap；read/predicate exceptions complete the future exceptionally。

`readCompacted=true` remains rejected in broker admission；`rewind(boolean true)` cannot route to compacted bytes in
F3。

Read count/byte limits are validated before asynchronous suspension。`getNthEntry/asyncGetNthEntry` requires `n > 0`
and returns a successful `null` Entry when fewer than `n` eligible retained Entries exist。A cursor close failure while
flushing properties leaves the handle terminal CLOSING and reports failure；concurrent/repeated close callers attach to
the same close future，so no later close can falsely claim that the failed flush persisted。

### 7.3 Cumulative ack

Whole-entry cumulative ack at `o` proposes：

```text
newMarkDeleteOffset = o + 1
drop whole ranges/partial states below newMarkDeleteOffset
fold ranges beginning at newMarkDeleteOffset
```

If `o + 1 < current.markDeleteOffset`, it is already applied and succeeds idempotently. A future/foreign position
fails。

Partial-batch cumulative ack at `o` proposes：

```text
ack every whole offset < o
markDeleteOffset = o
partialBatchAcks[o] = existing AND requestRemainingWords
if empty: markDeleteOffset = o + 1
fold following whole ranges
```

The mutation persists provided position properties in the same root CAS。Callback success means the root version
already covers the mark-delete, holes and properties。

### 7.4 Individual ack

Whole entry `o` adds `[o,o+1)`。Partial ack merges remaining words by bitwise AND。A request list：

- is defensively copied and capped；
- rejects duplicate offsets with incompatible batch sizes；
- coalesces duplicate compatible acks by AND；
- normalizes once before CAS；
- either applies entirely in one root CAS or fails entirely。

An individual ack for an offset below mark-delete or already in a whole range returns success。`isMessageDeleted`
returns true only for those whole states；it returns false for a partial entry。

### 7.5 Batch helpers

```text
getBatchPositionAckSet(position)
getDeletedBatchIndexesAsLongArray(position)
```

Both return a defensive clone of the effective **remaining/unacknowledged** word array for a partial Entry；return
`null` when no partial state exists, matching the locked broker expectation。They do not return the complement。

`trimDeletedEntries(List<Entry>)` releases/removes Entries that are wholly acknowledged or below mark-delete；it
keeps partial entries and leaves batch filtering to dispatcher metadata。

### 7.6 Local seek and rewind

`rewind()` sets local read to first unacked and performs no CAS。

`seek(position,false)` sets：

```text
localReadOffset = max(validated direct target, firstUnackedOffset)
```

`seek(position,true)` may read an acknowledged but retained Entry; it still rejects foreign projection and clamps or
fails at trim according to the public method contract. It changes no ack truth and contributes no retention floor。

### 7.7 Durable reset

`resetCursor` is not local seek. It atomically replaces ack truth：

```text
markDeleteOffset = validated direct target
wholeAckRanges = empty
partialBatchAcks = optional validated target partial state
snapshotReference = empty
inline snapshot deltas = empty
positionProperties = empty
cursorProperties = preserved
mutationSequence += 1
ackStateEpoch += 1
lastProtectionAttemptId = preserved for forward reset；pending intent ID for backward reset
```

Before a backward reset，the retention coordinator CASes its stream root ACTIVE -> PROTECTION_PENDING with the full
reset intent，lowering the protection floor when needed；the pending transition is still required if the numeric floor
is already lower。Both pending lifecycles reject another backward transition。Conflict before intent acquisition maps
to `ConcurrentFindCursorPositionException` so `PersistentSubscription` can expose `SubscriptionBusyException`；once
the intent is durable，recovery must complete or prove it before returning the retention root to ACTIVE。

The locked `asyncResetCursor` signature has no caller `ctx` parameter。F3 invokes
`resetComplete(Object)` / `resetFailed(ManagedLedgerException,Object)` with the canonical projected reset-read
`Position` as that Object，matching `ManagedCursorImpl`；success is still delayed until protection finalization。

### 7.8 Clear backlog and skip

`clearBacklog` first reads one committed-end snapshot `E`, then CASes `markDeleteOffset=E` and clears all holes/ref。
Appends committed after E remain backlog。

`skipEntries(n, Include/Exclude)` follows the locked stock behavior: it is durable cumulative progress, not a local
read-position move. It resolves the Nth target from mark-delete, accounting for the Include/Exclude treatment of
whole individual ack ranges, then mark-deletes through that target。`n < 0` fails；`n == 0` succeeds without CAS。

### 7.9 Backlog and size methods

Exact entry backlog：

```text
committedEndOffset
  - markDeleteOffset
  - cardinality(wholeAckRanges within [markDeleteOffset,end))
```

Partial batches count as one outstanding Entry. `getNumberOfEntriesInBacklog(true)` returns this exact entry count。
Message-level count requires decoding batch metadata and is not promised by this API。Estimated byte methods use F2
cumulative logical size where endpoints are resolvable; otherwise they return a documented conservative estimate,
never a negative value。

`isCursorDataFullyPersistable()` is true for every valid usable state. F3 never reports false after silently dropping
ranges；a mutation that cannot fit root + snapshot limits fails before CAS。

### 7.10 Remaining public method contract

The less visible `ManagedCursor` methods are still part of the locked API and are not left to implementation guesswork：

| Method | Exact F3 behavior |
| --- | --- |
| `getNumberOfEntries()` | count offsets in `[localReadOffset, committedEndOffset)` excluding whole-ack ranges；partial Entry counts as one |
| `hasBacklog()` / `hasBacklog(precise)` | `getNumberOfEntriesInBacklog(precise) > 0`；both are exact at F3 scale |
| `getNumberOfEntriesSinceFirstNotAckedMessage()` | `max(0, min(localReadOffset,end)-firstUnacked)` in the dense one-Entry/one-offset coordinate |
| `getTotalNonContiguousDeletedMessagesRange()` | effective normalized whole-ack range count；partials are not reported as whole ranges |
| `getNonContiguousDeletedMessagesRangeSerializedSize()` | exact `16 * wholeRangeCount` V1 range-payload bytes，checked before narrowing to `int` |
| `getEstimatedSizeSinceMarkDeletePosition()` | nonnegative stream-average estimate `round(cumulativeSize * exactBacklogEntries / max(1,committedEndOffset))`，saturated on overflow |
| `getLastIndividualDeletedRange()` | `null` when no whole range；otherwise project the last `[s,e)` as `Range.openClosed(Position(s-1),Position(e-1))` |
| `getThrottleMarkDelete/setThrottleMarkDelete` | broker-local finite nonnegative rate；setter rejects NaN/negative and never changes durable bytes or callback boundary |
| `skipNonRecoverableLedger(long)` | no-op plus unsupported-operation metric；F3 admission rejects auto-skip, and missing committed bytes remain corruption |
| `periodicRollover()` | `false`；F3 has no cursor BookKeeper ledger to roll |
| `getManagedLedger()` | exact owning `NereusManagedLedger` |
| `getStats()/getCursorStats()` | immutable/local snapshot with projected durable mark-delete, local read position, ack-range counts and `cursorLedger=-1` |
| `checkAndUpdateReadPositionChanged()` | atomically compares local read position with the prior stats sample and returns true when changed or currently at tail |
| `getManagedCursorAttributes()` | memoized stock `ManagedCursorAttributes(this)`；attributes are not durable cursor state |
| `applyMaxSizeCap(maxEntries,maxBytes)` | rejects negative arguments except the locked `NO_MAX_SIZE_LIMIT` sentinel；returns zero for a zero bound，otherwise min of caller count, configured read cap and local observed-size estimate；the sentinel omits only the byte cap |
| `updateReadStats(count,size)` | validates nonnegative inputs and updates only local counters/entry-size estimate |

`getCursorStats()` renders `individuallyDeletedMessages` from canonical effective whole ranges，never from root-delta
bytes alone。Snapshot-backed and inline-equivalent states therefore expose identical stats。Throttle/read counters、
last-active and stats samples are discarded on failover。

## 8. Property Contract

### 8.1 Position properties

`getProperties()` returns one immutable local full-map view。When there is no staging it is the hydrated persisted map；
the first `putProperty/removeProperty` copies that map and subsequent calls replace/remove one key in the staged copy。

```text
putProperty/removeProperty
  -> synchronous local staging; true means an initialized OPEN stage accepted the call，not that map bytes changed

markDelete(position) without explicit map
  -> commits the current visible full map in the same ack CAS

markDelete(position, explicitMap)
  -> non-null explicitMap is the full replacement and supersedes staged local contents

markDelete(position, null)
  -> same as the no-map overload：capture the current visible full map；null never means clear

reset
  -> commits an empty position-property map and clears staging only after reset CAS

flush-on-close
  -> commits the current staged full map with no ack change

successful property-carrying CAS
  -> clears staging only after CAS success
```

Matching locked `ManagedCursorImpl` behavior，putting the same value or removing an absent key still returns `true`
when the OPEN stage accepts the call；a cursor with no usable initialized stage returns `false`。Only an actual map
change advances `positionPropertyStageRevision`。

Thus a non-null empty explicit map is the only mark-delete form that deliberately clears the map。Facade ingress
always resolves these overloads to a non-null complete `CursorAckRequest.positionProperties` map before async
suspension。

The facade keeps a broker-local `positionPropertyStageRevision`。An ack/reset captures `(revision, fullMap)` before
async suspension。On CAS success it clears staging only if the revision still matches；if a later `put/remove` advanced
the revision，that newer full-map view remains dirty and is rebased on the newly persisted result for the next
ack/close flush。An `ALREADY_APPLIED` ack clears the captured stage only when the authoritative root also equals that
captured property map；ack subsumption alone is insufficient。CAS failure clears nothing。This prevents a synchronous
property update racing an async ack，or an older ack racing a newer mark-delete property map，from losing local staged
state。

If close-time flush fails, `asyncClose` fails and sync close throws；the cursor does not claim durable persistence。

### 8.2 Cursor properties

`putCursorProperty` and `removeCursorProperty` CAS one key against the latest root。`setCursorProperties` replaces all
external keys while preserving existing keys beginning exactly `#pulsar.internal.`；the caller is forbidden to set or
remove that prefix through the replace operation。

At the public `ManagedCursor` boundary，a null `setCursorProperties` map is normalized to an empty external map，
matching the locked implementation；the internal `ReplaceExternal` record itself is always non-null and immutable。

All returned maps are immutable copies. Null keys/values, invalid UTF-8 or configured byte-limit overflow fail before
CAS。

## 9. Error Mapping

| Domain/storage failure | ManagedLedger-facing error |
| --- | --- |
| foreign virtual ledger/incarnation, future offset, malformed ack set | `InvalidCursorPositionException` |
| force-reset target outside retained/tail range without F4 compacted bytes | `InvalidCursorPositionException` |
| reset/property destructive CAS conflict | `ConcurrentFindCursorPositionException` / `BadVersionException` as method requires |
| another protection/trim intent owns the required stream transition | `ConcurrentFindCursorPositionException` / subscription-busy |
| cursor/retention owner session differs from the writable ledger instance | `ManagedLedgerFencedException` |
| cursor-record or mutation-queue admission cap | `TooManyRequestsException` |
| deleted generation used by stale handle | `CursorAlreadyClosedException` |
| missing/corrupt referenced snapshot or invalid record | `ManagedLedgerException` with corruption cause; topic open fails |
| metadata/object timeout or transient object read failure | `ManagedLedgerException` with the original retriable error and stable operation context |
| mutation queue full | `ManagedLedgerException.TooManyRequestsException` |
| cursor close | `CursorAlreadyClosedException` |
| delete missing cursor | success |

Error messages include operation, stream ID, cursor-name hash and generation；they do not log raw cursor properties,
object credentials or snapshot bytes。

## 10. Limits and Configuration

F3 freezes these defaults so codecs, stores and tests share the same bounds：

```java
public record CursorStorageConfig(
        int cursorMetadataValueMaxBytes,
        int cursorMetadataSafetyMarginBytes,
        int cursorInlineAckMaxBytes,
        int cursorInlineDeltaMaxCount,
        int cursorNameMaxUtf8Bytes,
        int cursorPositionPropertiesMaxBytes,
        int cursorPropertiesMaxBytes,
        long cursorSnapshotMaxBytes,
        int cursorAckPositionsPerRequestMax,
        int cursorBatchIndexesMax,
        int cursorProtectionIntentMaxBytes,
        int cursorTrimReasonMaxUtf8Bytes,
        int cursorScanPageSize,
        int cursorRecordsPerStreamMax,
        int cursorOwnerClaimConcurrency,
        int cursorMutationQueueMax,
        int cursorMaxCasAttempts,
        int cursorHydrationMaxAttempts,
        int cursorSnapshotIdMaxAttempts,
        Duration cursorMetadataOperationTimeout,
        Duration cursorSnapshotOperationTimeout) {

    public static CursorStorageConfig defaults();
}
```

The canonical constructor enforces every cross-field relation stated below and `defaults()` returns exactly the
listed values。Configuration objects are immutable and one operation captures one reference before admission；it does
not reread mutable broker configuration midway through a CAS/snapshot chain。

| Key | Default | Validation / purpose |
| --- | ---: | --- |
| `cursorMetadataValueMaxBytes` | 65,536 | hard maximum encoded Oxia value |
| `cursorMetadataSafetyMarginBytes` | 4,096 | root planner reserves space below hard maximum |
| `cursorInlineAckMaxBytes` | 8,192 | hard encoded budget for inline range/partial fields before snapshot |
| `cursorInlineDeltaMaxCount` | 256 | replacement snapshot when total inline range + partial records exceeds this count |
| `cursorNameMaxUtf8Bytes` | 16,384 | exact name retained in record |
| `cursorPositionPropertiesMaxBytes` | 8,192 | canonical encoded key/value budget |
| `cursorPropertiesMaxBytes` | 16,384 | canonical encoded key/value budget |
| `cursorSnapshotMaxBytes` | 67,108,864 | strict head/read/decode cap, 64 MiB |
| `cursorAckPositionsPerRequestMax` | 1,000 | individual ack list admission |
| `cursorBatchIndexesMax` | 131,072 | allocation/bitset bound for one persisted batch Entry；keeps worst-case reset intent within cap |
| `cursorProtectionIntentMaxBytes` | 49,152 | hard encoded cap for one recoverable create/backward-reset intent |
| `cursorTrimReasonMaxUtf8Bytes` | 1,024 | hard cap for the complete internal prefix + attempt ID + caller reason sent to L0 trim |
| `cursorScanPageSize` | 256 | bounded Oxia cursor enumeration |
| `cursorRecordsPerStreamMax` | 10,000 | active + retained tombstones admission |
| `cursorOwnerClaimConcurrency` | 32 | bounded ACTIVE-root CAS claims before writable topic open |
| `cursorMutationQueueMax` | 1,024 | per local durable cursor |
| `cursorMaxCasAttempts` | 32 | additionally bounded by operation deadline |
| `cursorHydrationMaxAttempts` | 8 | full scan/root-ref stabilization retry cap |
| `cursorSnapshotIdMaxAttempts` | 8 | fresh random-ID retries after immutable-key collision |
| `cursorMetadataOperationTimeout` | 30 s | CAS/get/scan total deadline |
| `cursorSnapshotOperationTimeout` | 60 s | put/head/read total deadline |

The safety margin must be `< hard max`；inline budget must fit below the margin-adjusted root cap；batch-index cap plus
the maximum cursor name must fit the protection-intent cap and snapshot cap after fixed overhead。Every mutation also
encodes the complete candidate，so the combination of exact cursor name、properties and snapshot reference must fit
even when each individual field is within its own cap；the largest composed trim reason plus fixed TRIM_PENDING
payload must likewise fit the metadata hard cap。A production operator may lower limits. Raising hard
wire/object limits is a protocol/config rollout and must have decode compatibility tests。

No configuration may enable ack truncation, persist normal read position, remove tombstones, or make snapshot
existence authoritative。

Every offset、range endpoint、generation、mutation sequence、ack-state epoch、byte count and element-count arithmetic
uses checked add/multiply before allocation or candidate construction。Exhausted `cursorGeneration`、
`mutationSequence` or `ackStateEpoch` fails closed before CAS；it never wraps to a valid-looking value。For timestamps，
create uses nonnegative `nowMillis`，and replacements use
`updatedAtMillis=max(previous.updatedAtMillis, nowMillis)`；delete sets `deletedAtMillis=updatedAtMillis`。A backward
wall-clock step therefore cannot make a valid record unencodable。Recreate is the one replacement that also starts a
new generation：it sets `createdAtMillis=nowMillis` while keeping the monotonic updated-time rule above。

## 11. Async Safety Invariants

1. Every Pulsar callback is invoked exactly once on the designated callback executor。
2. No callback is invoked while holding cursor, ledger, waiter, metadata-client or Netty buffer locks。
3. Callback success occurs only after the authoritative root CAS succeeds or the latest root proves the mutation
   already applied；protected create/backward-reset additionally requires matching retention pending -> ACTIVE
   finalization。
4. Input `Position`, arrays, maps and iterables are copied before async suspension if the caller can mutate them。
5. Every Entry/ByteBuf acquired for batch validation or scan is released on all terminal paths。
6. Close/cancel races choose one terminal callback; late IO completion is ignored except for metrics/orphan tracking。
7. Watch events only invalidate caches. They never complete an ack or replace CAS revalidation。
8. `localReadOffset` may move backward on rewind/seek and resets after successful durable reset; it is never used as
   a protection or commit coordinate。
