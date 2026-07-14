/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.TrimOptions;
import com.nereusstream.metadata.oxia.CursorIds;
import com.nereusstream.metadata.oxia.CursorMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorScanPage;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.ManagedLedgerCursorProtocol;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.codec.F3MetadataCodecs;
import com.nereusstream.metadata.oxia.records.CursorPartialBatchAckRecord;
import com.nereusstream.metadata.oxia.records.CursorProtectionIntentRecord;
import com.nereusstream.metadata.oxia.records.CursorProtectionKind;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/** Default single-root retention coordinator with recoverable protection and trim barriers. */
public final class DefaultCursorRetentionCoordinator implements CursorRetentionCoordinator {
    private static final String TRIM_REASON_PREFIX = "nereus-cursor-retention/";

    private final String cluster;
    private final StreamStorage streamStorage;
    private final ManagedLedgerProjectionMetadataStore projectionStore;
    private final CursorMetadataStore metadataStore;
    private final CursorSnapshotStore snapshotStore;
    private final CursorProtocolActivationGuard activationGuard;
    private final CursorStateMachine stateMachine;
    private final CursorStorageConfig config;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final Supplier<String> randomIdSupplier;
    private final LongSupplier nanoTime;
    private final CursorStatePersistencePlanner persistencePlanner;
    private final CursorStateHydrator hydrator;
    private final ConcurrentHashMap<String, CursorMutationLane> lanes = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

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
            ScheduledExecutorService scheduler) {
        this(
                cluster,
                streamStorage,
                projectionStore,
                metadataStore,
                snapshotStore,
                activationGuard,
                stateMachine,
                config,
                clock,
                scheduler,
                secureRandomIdSupplier(),
                System::nanoTime);
    }

    DefaultCursorRetentionCoordinator(
            String cluster,
            StreamStorage streamStorage,
            ManagedLedgerProjectionMetadataStore projectionStore,
            CursorMetadataStore metadataStore,
            CursorSnapshotStore snapshotStore,
            CursorProtocolActivationGuard activationGuard,
            CursorStateMachine stateMachine,
            CursorStorageConfig config,
            Clock clock,
            ScheduledExecutorService scheduler,
            Supplier<String> randomIdSupplier,
            LongSupplier nanoTime) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.streamStorage = Objects.requireNonNull(streamStorage, "streamStorage");
        this.projectionStore = Objects.requireNonNull(projectionStore, "projectionStore");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.snapshotStore = Objects.requireNonNull(snapshotStore, "snapshotStore");
        this.activationGuard = Objects.requireNonNull(activationGuard, "activationGuard");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.randomIdSupplier = Objects.requireNonNull(randomIdSupplier, "randomIdSupplier");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.persistencePlanner = new CursorStatePersistencePlanner(this.cluster, config);
        this.hydrator = new CursorStateHydrator(
                this.cluster, metadataStore, snapshotStore, persistencePlanner, config);
    }

    @Override
    public CompletableFuture<CursorRetentionView> claimAndRecover(CursorOwnerSession owner) {
        Objects.requireNonNull(owner, "owner");
        return submit(owner, () -> claimAndRecoverAttempt(
                owner, Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime), 0));
    }

    @Override
    public CompletableFuture<ProtectionLease> beginProtection(
            CursorOwnerSession owner, ProtectionRequest request) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(request, "request");
        return submit(owner, () -> beginProtectionAttempt(
                owner,
                request,
                Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime),
                0));
    }

    @Override
    public CompletableFuture<CursorRetentionView> completeProtection(ProtectionLease lease) {
        Objects.requireNonNull(lease, "lease");
        return submit(lease.owner(), () -> completeProtectionAttempt(
                lease,
                Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime),
                0));
    }

    @Override
    public CompletableFuture<CursorRetentionView> reconcileFloor(CursorOwnerSession owner) {
        Objects.requireNonNull(owner, "owner");
        return submit(owner, () -> reconcileFloorAttempt(
                owner, Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime), 0));
    }

    @Override
    public CompletableFuture<CursorRetentionView> requestTrim(
            CursorOwnerSession owner, long candidateOffset, String reason) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(reason, "reason");
        return submit(owner, () -> requestTrimAttempt(
                owner,
                candidateOffset,
                reason,
                Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime),
                0));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            ManagedLedgerException.ManagedLedgerAlreadyClosedException failure =
                    new ManagedLedgerException.ManagedLedgerAlreadyClosedException(
                            "cursor retention coordinator is closed");
            lanes.values().forEach(lane -> lane.close(failure));
            lanes.clear();
        }
    }

    private <T> CompletableFuture<T> submit(
            CursorOwnerSession owner, Supplier<CompletableFuture<T>> operation) {
        if (closed.get()) {
            return closedFuture();
        }
        CursorMutationLane lane = lanes.computeIfAbsent(
                owner.ledger().projection().streamId(),
                ignored -> new CursorMutationLane(config.cursorMutationQueueMax(), scheduler));
        if (closed.get()) {
            lane.close(new ManagedLedgerException.ManagedLedgerAlreadyClosedException(
                    "cursor retention coordinator is closed"));
            return closedFuture();
        }
        return lane.submit(operation);
    }

    private CompletableFuture<CursorRetentionView> claimAndRecoverAttempt(
            CursorOwnerSession owner, Deadline deadline, int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "claim retention root");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return loadOpenContext(owner).thenCompose(context -> {
            try {
                validateOpenMatrix(owner, context);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            if (context.retention().isEmpty()) {
                CursorRetentionRecord initial = new CursorRetentionRecord(
                        0,
                        owner.ledger().projection(),
                        owner.ownerSessionId(),
                        CursorRetentionLifecycle.ACTIVE,
                        1,
                        context.stream().trimOffset(),
                        context.stream().trimOffset(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty(),
                        Optional.empty(),
                        nowMillis());
                return retryCondition(
                        metadataStore.createRetention(cluster, initial)
                                .thenApply(value -> toView(owner.ledger(), value)),
                        () -> claimAndRecoverAttempt(owner, deadline, attempt + 1));
            }
            VersionedCursorRetention current = context.retention().orElseThrow();
            try {
                validateRetention(owner.ledger(), current.value(), context.stream());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            if (!current.value().ownerSessionId().equals(owner.ownerSessionId())) {
                CursorRetentionRecord claimed = copyRetention(
                        current.value(),
                        owner.ownerSessionId(),
                        Math.addExact(current.value().mutationSequence(), 1),
                        nowMillis());
                return retryCondition(
                        metadataStore.compareAndSetRetention(
                                        cluster, claimed, current.metadataVersion())
                                .thenCompose(value -> recoverPending(owner, value, deadline, 0)),
                        () -> claimAndRecoverAttempt(owner, deadline, attempt + 1));
            }
            return recoverPending(owner, current, deadline, 0);
        });
    }

    private CompletableFuture<ProtectionLease> beginProtectionAttempt(
            CursorOwnerSession owner,
            ProtectionRequest request,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "begin cursor protection");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        if (request.targetPartialBatch().isPresent()
                && request.targetPartialBatch().orElseThrow().batchSize()
                        > config.cursorBatchIndexesMax()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "protection partial batch exceeds the configured batch-index bound"));
        }
        return loadProjection(owner).thenCompose(projection -> {
            if (!ManagedLedgerCursorProtocol.isActivated(projection)) {
                return CompletableFuture.failedFuture(invariant(
                        "cursor protection cannot begin before protocol activation"));
            }
            return streamStorage.getStreamMetadata(streamId(owner)).thenCompose(stream ->
                    metadataStore.getRetention(cluster, streamId(owner)).thenCompose(retention -> {
                        if (retention.isEmpty()) {
                            return CompletableFuture.failedFuture(invariant(
                                    "cursor protection requires an existing retention root"));
                        }
                        VersionedCursorRetention current = retention.orElseThrow();
                        try {
                            validateRetention(owner.ledger(), current.value(), stream);
                            requireOwner(owner, current.value());
                            if (request.targetMarkDeleteOffset() < stream.trimOffset()
                                    || request.targetMarkDeleteOffset() > stream.committedEndOffset()) {
                                throw new ManagedLedgerException.InvalidCursorPositionException(
                                        "cursor protection target is outside retained committed bounds");
                            }
                            if (current.value().lifecycle()
                                    == CursorRetentionLifecycle.PROTECTION_PENDING) {
                                CursorProtectionIntentRecord pending = current.value()
                                        .pendingProtectionIntent()
                                        .orElseThrow();
                                if (matches(pending, request)) {
                                    return CompletableFuture.completedFuture(new ProtectionLease(
                                            owner, pending.attemptId(), current.metadataVersion()));
                                }
                                return CompletableFuture.failedFuture(busy(
                                        "another cursor protection intent is pending"));
                            }
                            if (current.value().lifecycle() != CursorRetentionLifecycle.ACTIVE) {
                                return CompletableFuture.failedFuture(busy(
                                        "cursor retention trim is pending"));
                            }
                        } catch (Throwable error) {
                            return CompletableFuture.failedFuture(error);
                        }
                        CompletableFuture<Void> countCheck = request.kind()
                                        == CursorRetentionView.PendingProtection.Kind.CREATE
                                ? scanAll(owner.ledger()).thenCompose(records -> {
                                    if (records.size() >= config.cursorRecordsPerStreamMax()) {
                                        return CompletableFuture.failedFuture(
                                                new ManagedLedgerException.TooManyRequestsException(
                                                        "durable cursor record limit is exhausted"));
                                    }
                                    return CompletableFuture.completedFuture(null);
                                })
                                : CompletableFuture.completedFuture(null);
                        return countCheck.thenCompose(ignored -> {
                            final String attemptId;
                            final CursorProtectionIntentRecord intent;
                            final CursorRetentionRecord candidate;
                            try {
                                attemptId = randomId("protectionAttemptId");
                                intent = toIntent(request, attemptId, nowMillis());
                                requireProtectionIntentSize(intent);
                                candidate = new CursorRetentionRecord(
                                        0,
                                        current.value().projection(),
                                        current.value().ownerSessionId(),
                                        CursorRetentionLifecycle.PROTECTION_PENDING,
                                        Math.addExact(current.value().mutationSequence(), 1),
                                        Math.min(
                                                current.value().protectedFloorOffset(),
                                                request.targetMarkDeleteOffset()),
                                        current.value().lastCompletedTrimOffset(),
                                        Optional.of(intent),
                                        Optional.empty(),
                                        OptionalLong.empty(),
                                        Optional.empty(),
                                        nowMillis());
                                requireMetadataFits(candidate);
                            } catch (Throwable error) {
                                return CompletableFuture.failedFuture(error);
                            }
                            return retryCondition(
                                    metadataStore.compareAndSetRetention(
                                                    cluster,
                                                    candidate,
                                                    current.metadataVersion())
                                            .thenApply(updated -> new ProtectionLease(
                                                    owner, attemptId, updated.metadataVersion())),
                                    () -> beginProtectionAttempt(
                                            owner, request, deadline, attempt + 1));
                        });
                    }));
        });
    }

    private CompletableFuture<CursorRetentionView> completeProtectionAttempt(
            ProtectionLease lease, Deadline deadline, int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "complete cursor protection");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        CursorOwnerSession owner = lease.owner();
        return metadataStore.getRetention(cluster, streamId(owner)).thenCompose(retention -> {
            if (retention.isEmpty()) {
                return CompletableFuture.failedFuture(invariant(
                        "retention root disappeared while completing cursor protection"));
            }
            VersionedCursorRetention current = retention.orElseThrow();
            try {
                requireRetentionIdentity(owner.ledger(), current.value());
                requireOwner(owner, current.value());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            if (current.value().lifecycle() == CursorRetentionLifecycle.ACTIVE) {
                return proveActiveAttempt(owner, lease.attemptId(), current);
            }
            if (current.value().lifecycle() != CursorRetentionLifecycle.PROTECTION_PENDING
                    || !current.value().pendingProtectionIntent().orElseThrow().attemptId()
                            .equals(lease.attemptId())) {
                return CompletableFuture.failedFuture(busy(
                        "retention root carries a different pending operation"));
            }
            return recoverProtection(owner, current, deadline, attempt);
        });
    }

    private CompletableFuture<CursorRetentionView> recoverPending(
            CursorOwnerSession owner,
            VersionedCursorRetention current,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "recover cursor retention");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        try {
            requireRetentionIdentity(owner.ledger(), current.value());
            requireOwner(owner, current.value());
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return switch (current.value().lifecycle()) {
            case ACTIVE -> CompletableFuture.completedFuture(toView(owner.ledger(), current));
            case PROTECTION_PENDING -> recoverProtection(owner, current, deadline, attempt);
            case TRIM_PENDING -> recoverTrim(owner, current, deadline, attempt);
        };
    }

    private CompletableFuture<CursorRetentionView> recoverProtection(
            CursorOwnerSession owner,
            VersionedCursorRetention retention,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "recover protection intent");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        CursorProtectionIntentRecord intent;
        try {
            requireOwner(owner, retention.value());
            if (retention.value().lifecycle() != CursorRetentionLifecycle.PROTECTION_PENDING) {
                throw invariant("protection recovery received a non-pending retention root");
            }
            intent = retention.value().pendingProtectionIntent().orElseThrow();
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return metadataStore
                .getCursor(cluster, streamId(owner), intent.cursorName())
                .thenCompose(cursor -> recoverProtectionTarget(
                        owner, retention, intent, cursor, deadline, attempt));
    }

    private CompletableFuture<CursorRetentionView> recoverProtectionTarget(
            CursorOwnerSession owner,
            VersionedCursorRetention retention,
            CursorProtectionIntentRecord intent,
            Optional<VersionedCursorState> cursor,
            Deadline deadline,
            int attempt) {
        if (cursor.isPresent() && provesAttempt(cursor.orElseThrow().value(), intent)) {
            VersionedCursorState proved = cursor.orElseThrow();
            if (proved.value().lifecycle() == CursorRecordLifecycle.ACTIVE
                    && !proved.value().ownerSessionId().equals(owner.ownerSessionId())) {
                CursorStateRecord claimed;
                try {
                    claimed = claimCursorRecord(proved.value(), owner.ownerSessionId(), nowMillis());
                } catch (Throwable error) {
                    return CompletableFuture.failedFuture(error);
                }
                return retryCondition(
                        metadataStore.compareAndSetCursor(
                                        cluster, claimed, proved.metadataVersion())
                                .thenCompose(ignored -> resumeProtection(
                                        owner, intent.attemptId(), deadline, attempt + 1)),
                        () -> resumeProtection(owner, intent.attemptId(), deadline, attempt + 1));
            }
            return finalizeProtection(owner, retention, intent, deadline, attempt);
        }

        return switch (intent.kind()) {
            case CREATE -> recoverCreate(owner, retention, intent, cursor, deadline, attempt);
            case RECREATE -> recoverRecreate(owner, retention, intent, cursor, deadline, attempt);
            case BACKWARD_RESET -> recoverBackwardReset(
                    owner, retention, intent, cursor, deadline, attempt);
        };
    }

    private CompletableFuture<CursorRetentionView> recoverCreate(
            CursorOwnerSession owner,
            VersionedCursorRetention retention,
            CursorProtectionIntentRecord intent,
            Optional<VersionedCursorState> cursor,
            Deadline deadline,
            int attempt) {
        if (cursor.isPresent()) {
            return CompletableFuture.failedFuture(invariant(
                    "CREATE protection found a cursor root without its attempt proof"));
        }
        final CursorStateRecord candidate;
        try {
            CursorState state = stateMachine.create(
                    owner,
                    intent.cursorName(),
                    intent.targetCursorGeneration(),
                    1,
                    intent.attemptId(),
                    intent.targetMarkDeleteOffset(),
                    intent.initialPositionProperties(),
                    intent.initialCursorProperties(),
                    intent.createdAtMillis());
            candidate = persistencePlanner.recordWithoutSnapshot(state);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return retryCondition(
                metadataStore.createCursor(cluster, candidate)
                        .thenCompose(ignored -> resumeProtection(
                                owner, intent.attemptId(), deadline, attempt + 1)),
                () -> resumeProtection(owner, intent.attemptId(), deadline, attempt + 1));
    }

    private CompletableFuture<CursorRetentionView> recoverRecreate(
            CursorOwnerSession owner,
            VersionedCursorRetention retention,
            CursorProtectionIntentRecord intent,
            Optional<VersionedCursorState> cursor,
            Deadline deadline,
            int attempt) {
        if (cursor.isEmpty()
                || cursor.orElseThrow().value().lifecycle() != CursorRecordLifecycle.DELETED
                || cursor.orElseThrow().value().cursorGeneration()
                        != intent.expectedCursorGeneration()) {
            return CompletableFuture.failedFuture(invariant(
                    "RECREATE protection found an incompatible cursor generation"));
        }
        VersionedCursorState tombstone = cursor.orElseThrow();
        final CursorStateRecord candidate;
        try {
            long createdAt = intent.createdAtMillis();
            CursorState state = new CursorState(
                    new CursorIdentity(
                            owner.ledger(),
                            intent.cursorName(),
                            intent.cursorNameHash(),
                            intent.targetCursorGeneration()),
                    owner.ownerSessionId(),
                    CursorLifecycle.ACTIVE,
                    Math.addExact(tombstone.value().mutationSequence(), 1),
                    1,
                    intent.attemptId(),
                    CursorAckState.empty(intent.targetMarkDeleteOffset()),
                    intent.initialPositionProperties(),
                    intent.initialCursorProperties(),
                    Optional.empty(),
                    createdAt,
                    Math.max(createdAt, tombstone.value().updatedAtMillis()),
                    tombstone.metadataVersion());
            candidate = persistencePlanner.recordWithoutSnapshot(state);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return retryCondition(
                metadataStore.compareAndSetCursor(
                                cluster, candidate, tombstone.metadataVersion())
                        .thenCompose(ignored -> resumeProtection(
                                owner, intent.attemptId(), deadline, attempt + 1)),
                () -> resumeProtection(owner, intent.attemptId(), deadline, attempt + 1));
    }

    private CompletableFuture<CursorRetentionView> recoverBackwardReset(
            CursorOwnerSession owner,
            VersionedCursorRetention retention,
            CursorProtectionIntentRecord intent,
            Optional<VersionedCursorState> cursor,
            Deadline deadline,
            int attempt) {
        if (cursor.isEmpty()
                || cursor.orElseThrow().value().cursorGeneration()
                        != intent.expectedCursorGeneration()) {
            return CompletableFuture.failedFuture(invariant(
                    "BACKWARD_RESET protection found an incompatible cursor generation"));
        }
        VersionedCursorState current = cursor.orElseThrow();
        if (current.value().lifecycle() == CursorRecordLifecycle.DELETED) {
            final CursorStateRecord provedDelete;
            try {
                provedDelete = new CursorStateRecord(
                        0,
                        current.value().projection(),
                        owner.ownerSessionId(),
                        current.value().cursorName(),
                        current.value().cursorNameHash(),
                        current.value().cursorGeneration(),
                        CursorRecordLifecycle.DELETED,
                        Math.addExact(current.value().mutationSequence(), 1),
                        current.value().ackStateEpoch(),
                        intent.attemptId(),
                        current.value().markDeleteOffset(),
                        current.value().snapshotReference(),
                        current.value().inlineWholeAckDeltas(),
                        current.value().inlinePartialAckOverrides(),
                        current.value().positionProperties(),
                        current.value().cursorProperties(),
                        current.value().createdAtMillis(),
                        Math.max(current.value().updatedAtMillis(), nowMillis()),
                        current.value().deletedAtMillis());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return retryCondition(
                    metadataStore.compareAndSetCursor(
                                    cluster, provedDelete, current.metadataVersion())
                            .thenCompose(ignored -> resumeProtection(
                                    owner, intent.attemptId(), deadline, attempt + 1)),
                    () -> resumeProtection(
                            owner, intent.attemptId(), deadline, attempt + 1));
        }
        if (!current.value().ownerSessionId().equals(owner.ownerSessionId())) {
            CursorStateRecord claimed;
            try {
                claimed = claimCursorRecord(current.value(), owner.ownerSessionId(), nowMillis());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return retryCondition(
                    metadataStore.compareAndSetCursor(
                                    cluster, claimed, current.metadataVersion())
                            .thenCompose(ignored -> resumeProtection(
                                    owner, intent.attemptId(), deadline, attempt + 1)),
                    () -> resumeProtection(owner, intent.attemptId(), deadline, attempt + 1));
        }
        return hydrator.hydrate(owner.ledger(), current).thenCompose(hydrated ->
                streamStorage.getStreamMetadata(streamId(owner)).thenCompose(stream -> {
                    if (intent.targetMarkDeleteOffset() < stream.trimOffset()
                            || intent.targetMarkDeleteOffset() > stream.committedEndOffset()) {
                        return CompletableFuture.failedFuture(
                                new ManagedLedgerException.InvalidCursorPositionException(
                                        "protected reset target is no longer retained"));
                    }
                    final CursorStateRecord candidate;
                    try {
                        Optional<BatchAckState> partial = intent.targetPartialBatch().map(
                                value -> new BatchAckState(value.batchSize(), value.remainingWords()));
                        CursorResetRequest request = new CursorResetRequest(
                                intent.targetMarkDeleteOffset(),
                                partial,
                                true,
                                stream.trimOffset(),
                                stream.committedEndOffset());
                        CursorState reset = stateMachine.reset(
                                        hydrated.state(), request, intent.attemptId(), nowMillis())
                                .state();
                        candidate = persistencePlanner.recordWithoutSnapshot(reset);
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                    return retryCondition(
                            metadataStore.compareAndSetCursor(
                                            cluster, candidate, current.metadataVersion())
                                    .thenCompose(ignored -> resumeProtection(
                                            owner, intent.attemptId(), deadline, attempt + 1)),
                            () -> resumeProtection(
                                    owner, intent.attemptId(), deadline, attempt + 1));
                }));
    }

    private CompletableFuture<CursorRetentionView> finalizeProtection(
            CursorOwnerSession owner,
            VersionedCursorRetention retention,
            CursorProtectionIntentRecord intent,
            Deadline deadline,
            int attempt) {
        final CursorRetentionRecord candidate;
        try {
            requireOwner(owner, retention.value());
            CursorProtectionIntentRecord currentIntent = retention.value()
                    .pendingProtectionIntent()
                    .orElseThrow();
            if (retention.value().lifecycle() != CursorRetentionLifecycle.PROTECTION_PENDING
                    || !currentIntent.equals(intent)) {
                throw busy("cursor protection intent changed before completion");
            }
            candidate = new CursorRetentionRecord(
                    0,
                    retention.value().projection(),
                    retention.value().ownerSessionId(),
                    CursorRetentionLifecycle.ACTIVE,
                    Math.addExact(retention.value().mutationSequence(), 1),
                    retention.value().protectedFloorOffset(),
                    retention.value().lastCompletedTrimOffset(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    Optional.empty(),
                    nowMillis());
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return retryCondition(
                metadataStore.compareAndSetRetention(
                                cluster, candidate, retention.metadataVersion())
                        .thenApply(updated -> toView(owner.ledger(), updated)),
                () -> resumeProtection(owner, intent.attemptId(), deadline, attempt + 1));
    }

    private CompletableFuture<CursorRetentionView> resumeProtection(
            CursorOwnerSession owner, String attemptId, Deadline deadline, int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "resume cursor protection");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return metadataStore.getRetention(cluster, streamId(owner)).thenCompose(retention -> {
            if (retention.isEmpty()) {
                return CompletableFuture.failedFuture(invariant(
                        "retention root disappeared during protection recovery"));
            }
            VersionedCursorRetention current = retention.orElseThrow();
            try {
                requireRetentionIdentity(owner.ledger(), current.value());
                requireOwner(owner, current.value());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            if (current.value().lifecycle() == CursorRetentionLifecycle.ACTIVE) {
                return proveActiveAttempt(owner, attemptId, current);
            }
            if (current.value().lifecycle() != CursorRetentionLifecycle.PROTECTION_PENDING
                    || !current.value().pendingProtectionIntent().orElseThrow().attemptId()
                            .equals(attemptId)) {
                return CompletableFuture.failedFuture(busy(
                        "a different retention operation replaced the protection intent"));
            }
            return recoverProtection(owner, current, deadline, attempt + 1);
        });
    }

    private CompletableFuture<CursorRetentionView> proveActiveAttempt(
            CursorOwnerSession owner,
            String attemptId,
            VersionedCursorRetention activeRetention) {
        return scanAll(owner.ledger()).thenCompose(records -> {
            List<VersionedCursorState> proofs = records.stream()
                    .filter(record -> record.value().lastProtectionAttemptId().equals(attemptId))
                    .toList();
            if (proofs.size() != 1) {
                return CompletableFuture.failedFuture(new ManagedLedgerException.BadVersionException(
                        "ACTIVE retention does not have one exact cursor attempt proof"));
            }
            CursorStateRecord proof = proofs.get(0).value();
            if (proof.lifecycle() == CursorRecordLifecycle.ACTIVE
                    && !proof.ownerSessionId().equals(owner.ownerSessionId())) {
                return CompletableFuture.failedFuture(fenced(
                        "cursor attempt proof belongs to a stale owner session"));
            }
            return CompletableFuture.completedFuture(toView(owner.ledger(), activeRetention));
        });
    }

    private CompletableFuture<CursorRetentionView> reconcileFloorAttempt(
            CursorOwnerSession owner, Deadline deadline, int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "reconcile cursor floor");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return streamStorage.getStreamMetadata(streamId(owner)).thenCompose(stream ->
                metadataStore.getRetention(cluster, streamId(owner)).thenCompose(retention -> {
                    if (retention.isEmpty()) {
                        return CompletableFuture.failedFuture(invariant(
                                "retention root is missing during floor reconciliation"));
                    }
                    VersionedCursorRetention current = retention.orElseThrow();
                    try {
                        validateRetention(owner.ledger(), current.value(), stream);
                        requireOwner(owner, current.value());
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                    if (current.value().lifecycle() != CursorRetentionLifecycle.ACTIVE) {
                        return CompletableFuture.completedFuture(toView(owner.ledger(), current));
                    }
                    return scanAll(owner.ledger()).thenCompose(first ->
                            hydrateActive(owner, first, stream, 0, new ArrayList<>()).thenCompose(states ->
                                    scanAll(owner.ledger()).thenCompose(second -> {
                                        if (!fingerprint(first).equals(fingerprint(second))) {
                                            return reconcileFloorAttempt(owner, deadline, attempt + 1);
                                        }
                                        return metadataStore.getRetention(
                                                        cluster, streamId(owner))
                                                .thenCompose(revalidated -> {
                                                    if (revalidated.isEmpty()) {
                                                        return CompletableFuture.failedFuture(invariant(
                                                                "retention root disappeared during floor revalidation"));
                                                    }
                                                    VersionedCursorRetention stable =
                                                            revalidated.orElseThrow();
                                                    if (stable.metadataVersion()
                                                            != current.metadataVersion()) {
                                                        return reconcileFloorAttempt(
                                                                owner, deadline, attempt + 1);
                                                    }
                                                    final long candidateFloor;
                                                    try {
                                                        requireOwner(owner, stable.value());
                                                        if (stable.value().lifecycle()
                                                                != CursorRetentionLifecycle.ACTIVE) {
                                                            return CompletableFuture.completedFuture(
                                                                    toView(owner.ledger(), stable));
                                                        }
                                                        candidateFloor = states.stream()
                                                                .mapToLong(state -> state
                                                                        .acknowledgements()
                                                                        .markDeleteOffset())
                                                                .min()
                                                                .orElse(stream.committedEndOffset());
                                                        if (candidateFloor < stream.trimOffset()
                                                                || stable.value().protectedFloorOffset()
                                                                        > candidateFloor) {
                                                            throw invariant(
                                                                    "retention floor is ahead of an ACTIVE cursor");
                                                        }
                                                        if (candidateFloor
                                                                == stable.value().protectedFloorOffset()) {
                                                            return CompletableFuture.completedFuture(
                                                                    toView(owner.ledger(), stable));
                                                        }
                                                    } catch (Throwable error) {
                                                        return CompletableFuture.failedFuture(error);
                                                    }
                                                    CursorRetentionRecord raised;
                                                    try {
                                                        raised = new CursorRetentionRecord(
                                                                0,
                                                                stable.value().projection(),
                                                                stable.value().ownerSessionId(),
                                                                CursorRetentionLifecycle.ACTIVE,
                                                                Math.addExact(
                                                                        stable.value().mutationSequence(), 1),
                                                                candidateFloor,
                                                                stable.value().lastCompletedTrimOffset(),
                                                                Optional.empty(),
                                                                Optional.empty(),
                                                                OptionalLong.empty(),
                                                                Optional.empty(),
                                                                nowMillis());
                                                    } catch (Throwable error) {
                                                        return CompletableFuture.failedFuture(error);
                                                    }
                                                    return retryCondition(
                                                            metadataStore.compareAndSetRetention(
                                                                            cluster,
                                                                            raised,
                                                                            stable.metadataVersion())
                                                                    .thenApply(value ->
                                                                            toView(owner.ledger(), value)),
                                                            () -> reconcileFloorAttempt(
                                                                    owner,
                                                                    deadline,
                                                                    attempt + 1));
                                                });
                                    })));
                }));
    }

    private CompletableFuture<List<CursorState>> hydrateActive(
            CursorOwnerSession owner,
            List<VersionedCursorState> records,
            StreamMetadata stream,
            int index,
            List<CursorState> states) {
        CompletableFuture<Void> hydration = CompletableFuture.completedFuture(null);
        for (int cursor = index; cursor < records.size(); cursor++) {
            VersionedCursorState record = records.get(cursor);
            if (record.value().lifecycle() == CursorRecordLifecycle.DELETED) {
                continue;
            }
            if (!record.value().ownerSessionId().equals(owner.ownerSessionId())) {
                return CompletableFuture.failedFuture(fenced(
                        "ACTIVE cursor root is not claimed by the current retention owner"));
            }
            hydration = hydration.thenCompose(ignored ->
                    hydrator.hydrate(owner.ledger(), record).thenAccept(hydrated -> {
                        if (hydrated.state().acknowledgements().markDeleteOffset()
                                < stream.trimOffset()) {
                            throw invariant(
                                    "ACTIVE cursor mark-delete is behind the L0 trim offset");
                        }
                        states.add(hydrated.state());
                    }));
        }
        return hydration.thenApply(ignored -> List.copyOf(states));
    }

    private CompletableFuture<CursorRetentionView> requestTrimAttempt(
            CursorOwnerSession owner,
            long candidateOffset,
            String reason,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "request cursor trim");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        try {
            validateTrimReasonInput(candidateOffset, reason);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return loadProjection(owner).thenCompose(projection ->
                metadataStore.getRetention(cluster, streamId(owner)).thenCompose(initialRetention -> {
                    if (initialRetention.isEmpty()) {
                        return CompletableFuture.failedFuture(invariant(
                                "logical trim requires an existing owner retention root"));
                    }
                    try {
                        requireRetentionIdentity(owner.ledger(), initialRetention.orElseThrow().value());
                        requireOwner(owner, initialRetention.orElseThrow().value());
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                    if (!ManagedLedgerCursorProtocol.isActivated(projection)) {
                        return ensureActivated(owner, projection)
                                .thenCompose(ignored -> requestTrimAttempt(
                                        owner, candidateOffset, reason, deadline, attempt + 1));
                    }
                    return streamStorage.getStreamMetadata(streamId(owner)).thenCompose(stream ->
                            metadataStore.getRetention(cluster, streamId(owner)).thenCompose(retention -> {
                                if (retention.isEmpty()) {
                                    return CompletableFuture.failedFuture(invariant(
                                            "retention root disappeared after trim activation"));
                                }
                                VersionedCursorRetention current = retention.orElseThrow();
                                try {
                                    validateRetention(owner.ledger(), current.value(), stream);
                                    requireOwner(owner, current.value());
                                } catch (Throwable error) {
                                    return CompletableFuture.failedFuture(error);
                                }
                                if (current.value().lifecycle()
                                        == CursorRetentionLifecycle.PROTECTION_PENDING) {
                                    return CompletableFuture.failedFuture(busy(
                                            "cursor protection blocks logical trim"));
                                }
                                if (current.value().lifecycle()
                                        == CursorRetentionLifecycle.TRIM_PENDING) {
                                    return recoverTrim(owner, current, deadline, attempt + 1)
                                            .thenCompose(ignored -> requestTrimAttempt(
                                                    owner,
                                                    candidateOffset,
                                                    reason,
                                                    deadline,
                                                    attempt + 1));
                                }
                                if (candidateOffset <= current.value().lastCompletedTrimOffset()) {
                                    return CompletableFuture.completedFuture(
                                            toView(owner.ledger(), current));
                                }
                                if (candidateOffset > current.value().protectedFloorOffset()
                                        || candidateOffset > stream.committedEndOffset()) {
                                    return CompletableFuture.failedFuture(
                                            new ManagedLedgerException.InvalidCursorPositionException(
                                                    "logical trim target exceeds the protected floor or committed end"));
                                }
                                final String attemptId;
                                final String composedReason;
                                final CursorRetentionRecord pending;
                                try {
                                    attemptId = randomId("trimAttemptId");
                                    composedReason = composeTrimReason(attemptId, reason);
                                    pending = new CursorRetentionRecord(
                                            0,
                                            current.value().projection(),
                                            current.value().ownerSessionId(),
                                            CursorRetentionLifecycle.TRIM_PENDING,
                                            Math.addExact(current.value().mutationSequence(), 1),
                                            candidateOffset,
                                            current.value().lastCompletedTrimOffset(),
                                            Optional.empty(),
                                            Optional.of(attemptId),
                                            OptionalLong.of(candidateOffset),
                                            Optional.of(composedReason),
                                            nowMillis());
                                    requireMetadataFits(pending);
                                } catch (Throwable error) {
                                    return CompletableFuture.failedFuture(error);
                                }
                                return retryCondition(
                                        metadataStore.compareAndSetRetention(
                                                        cluster,
                                                        pending,
                                                        current.metadataVersion())
                                                .thenCompose(updated -> recoverTrim(
                                                        owner, updated, deadline, attempt + 1)),
                                        () -> requestTrimAttempt(
                                                owner,
                                                candidateOffset,
                                                reason,
                                                deadline,
                                                attempt + 1));
                            }));
                }));
    }

    private CompletableFuture<TopicProjectionRecord> ensureActivated(
            CursorOwnerSession owner, TopicProjectionRecord projection) {
        if (ManagedLedgerCursorProtocol.isActivated(projection)) {
            return CompletableFuture.completedFuture(requireActivatedProjection(owner, projection));
        }
        final CompletableFuture<TopicProjectionRecord> activation;
        try {
            activation = activationGuard.acquireFirstActivationPermit(owner.ledger())
                    .thenCompose(ignored -> projectionStore.activateCursorProtocol(
                            cluster,
                            owner.ledger().managedLedgerName(),
                            owner.ledger().projection(),
                            projection.metadataVersion()))
                    .handle((activated, error) -> {
                        if (error == null) {
                            return CompletableFuture.completedFuture(
                                    requireActivatedProjection(owner, activated));
                        }
                        Throwable cause = unwrap(error);
                        return loadProjection(owner).thenCompose(reloaded -> {
                            if (ManagedLedgerCursorProtocol.isActivated(reloaded)) {
                                return CompletableFuture.completedFuture(
                                        requireActivatedProjection(owner, reloaded));
                            }
                            return CompletableFuture.failedFuture(cause);
                        });
                    })
                    .thenCompose(future -> future);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return activation;
    }

    private static TopicProjectionRecord requireActivatedProjection(
            CursorOwnerSession owner, TopicProjectionRecord activated) {
        if (!activated.managedLedgerName().equals(owner.ledger().managedLedgerName())
                || !activated.managedLedgerNameHash()
                        .equals(owner.ledger().managedLedgerNameHash())
                || !activated.projectionIdentity().equals(owner.ledger().projection())
                || !ManagedLedgerCursorProtocol.isActivated(activated)) {
            throw invariant("cursor activation did not preserve the exact projection");
        }
        return activated;
    }

    private CompletableFuture<CursorRetentionView> recoverTrim(
            CursorOwnerSession owner,
            VersionedCursorRetention retention,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "recover cursor trim");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        final long target;
        final String attemptId;
        final String reason;
        try {
            requireOwner(owner, retention.value());
            if (retention.value().lifecycle() != CursorRetentionLifecycle.TRIM_PENDING) {
                throw invariant("trim recovery received a non-pending retention root");
            }
            target = retention.value().pendingTrimOffset().orElseThrow();
            attemptId = retention.value().pendingTrimAttemptId().orElseThrow();
            reason = retention.value().pendingTrimReason().orElseThrow();
            if (!reason.startsWith(TRIM_REASON_PREFIX + attemptId + ":")) {
                throw invariant("persisted trim reason does not match its attempt ID");
            }
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return streamStorage.getStreamMetadata(streamId(owner)).thenCompose(stream -> {
            CompletableFuture<StreamMetadata> afterTrim;
            if (stream.trimOffset() == retention.value().lastCompletedTrimOffset()) {
                afterTrim = streamStorage.trim(
                                streamId(owner),
                                target,
                                new TrimOptions(config.cursorMetadataOperationTimeout(), reason))
                        .thenCompose(ignored -> streamStorage.getStreamMetadata(streamId(owner)));
            } else if (stream.trimOffset() == target) {
                afterTrim = CompletableFuture.completedFuture(stream);
            } else {
                return CompletableFuture.failedFuture(invariant(
                        "L0 trim offset does not match either side of TRIM_PENDING"));
            }
            return afterTrim.thenCompose(updatedStream -> {
                if (updatedStream.trimOffset() != target) {
                    return CompletableFuture.failedFuture(invariant(
                            "L0 trim did not reach the exact pending target"));
                }
                return finalizeTrim(owner, attemptId, target, deadline, attempt + 1);
            });
        });
    }

    private CompletableFuture<CursorRetentionView> finalizeTrim(
            CursorOwnerSession owner,
            String attemptId,
            long target,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "finalize cursor trim");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return metadataStore.getRetention(cluster, streamId(owner)).thenCompose(retention -> {
            if (retention.isEmpty()) {
                return CompletableFuture.failedFuture(invariant(
                        "retention root disappeared after L0 trim"));
            }
            VersionedCursorRetention current = retention.orElseThrow();
            try {
                requireRetentionIdentity(owner.ledger(), current.value());
                requireOwner(owner, current.value());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            if (current.value().lifecycle() == CursorRetentionLifecycle.ACTIVE) {
                if (current.value().lastCompletedTrimOffset() == target) {
                    CursorRetentionView view = toView(owner.ledger(), current);
                    scheduleReconcile(owner);
                    return CompletableFuture.completedFuture(view);
                }
                return CompletableFuture.failedFuture(invariant(
                        "ACTIVE retention does not prove the completed trim target"));
            }
            if (current.value().lifecycle() != CursorRetentionLifecycle.TRIM_PENDING
                    || !current.value().pendingTrimAttemptId().orElseThrow().equals(attemptId)
                    || current.value().pendingTrimOffset().orElseThrow() != target) {
                return CompletableFuture.failedFuture(busy(
                        "a different retention operation replaced the pending trim"));
            }
            final CursorRetentionRecord completed;
            try {
                completed = new CursorRetentionRecord(
                        0,
                        current.value().projection(),
                        current.value().ownerSessionId(),
                        CursorRetentionLifecycle.ACTIVE,
                        Math.addExact(current.value().mutationSequence(), 1),
                        target,
                        target,
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty(),
                        Optional.empty(),
                        nowMillis());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return retryCondition(
                    metadataStore.compareAndSetRetention(
                                    cluster, completed, current.metadataVersion())
                            .thenApply(updated -> {
                                CursorRetentionView view = toView(owner.ledger(), updated);
                                scheduleReconcile(owner);
                                return view;
                            }),
                    () -> finalizeTrim(owner, attemptId, target, deadline, attempt + 1));
        });
    }

    private CompletableFuture<OpenContext> loadOpenContext(CursorOwnerSession owner) {
        return loadProjection(owner).thenCompose(projection ->
                streamStorage.getStreamMetadata(streamId(owner)).thenCompose(stream -> {
                    if (!stream.streamId().equals(streamId(owner))) {
                        return CompletableFuture.failedFuture(invariant(
                                "L0 stream metadata does not match the cursor projection"));
                    }
                    return scanAll(owner.ledger()).thenCompose(cursors ->
                            metadataStore.getRetention(cluster, streamId(owner)).thenApply(retention ->
                                    new OpenContext(projection, stream, cursors, retention)));
                }));
    }

    private CompletableFuture<TopicProjectionRecord> loadProjection(CursorOwnerSession owner) {
        return projectionStore
                .getProjection(cluster, owner.ledger().managedLedgerName())
                .thenCompose(projection -> {
                    if (projection.isEmpty()) {
                        return CompletableFuture.failedFuture(invariant(
                                "managed-ledger projection is missing for cursor operation"));
                    }
                    TopicProjectionRecord value = projection.orElseThrow();
                    if (!value.managedLedgerName().equals(owner.ledger().managedLedgerName())
                            || !value.managedLedgerNameHash()
                                    .equals(owner.ledger().managedLedgerNameHash())
                            || !value.projectionIdentity().equals(owner.ledger().projection())) {
                        return CompletableFuture.failedFuture(invariant(
                                "managed-ledger projection identity changed during cursor operation"));
                    }
                    return CompletableFuture.completedFuture(value);
                });
    }

    private CompletableFuture<List<VersionedCursorState>> scanAll(CursorLedgerIdentity ledger) {
        ArrayList<VersionedCursorState> records = new ArrayList<>();
        return scanPage(ledger, Optional.empty(), records);
    }

    private CompletableFuture<List<VersionedCursorState>> scanPage(
            CursorLedgerIdentity ledger,
            Optional<CursorScanToken> continuation,
            ArrayList<VersionedCursorState> records) {
        return metadataStore.scanCursors(
                        cluster,
                        new StreamId(ledger.projection().streamId()),
                        continuation,
                        config.cursorScanPageSize())
                .thenCompose(page -> {
                    try {
                        for (VersionedCursorState record : page.records()) {
                            if (!record.value().projection().equals(ledger.projection())) {
                                throw invariant("cursor scan contains a foreign projection identity");
                            }
                            records.add(record);
                            if (records.size() > config.cursorRecordsPerStreamMax()) {
                                throw invariant("cursor scan exceeds cursorRecordsPerStreamMax");
                            }
                        }
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                    if (page.continuation().isPresent()) {
                        return scanPage(ledger, page.continuation(), records);
                    }
                    return CompletableFuture.completedFuture(List.copyOf(records));
                });
    }

    private void validateOpenMatrix(CursorOwnerSession owner, OpenContext context) {
        boolean activated = ManagedLedgerCursorProtocol.isActivated(context.projection());
        boolean hasCursors = !context.cursors().isEmpty();
        if (!activated && hasCursors) {
            throw invariant("cursor records exist before cursor protocol activation");
        }
        if (context.retention().isEmpty() && hasCursors) {
            throw invariant("cursor records exist without a retention root");
        }
        if (!activated && context.retention().isPresent()) {
            CursorRetentionRecord retention = context.retention().orElseThrow().value();
            if (retention.lifecycle() != CursorRetentionLifecycle.ACTIVE) {
                throw invariant("preactivation retention root cannot carry pending state");
            }
        }
        if (!context.stream().streamId().equals(streamId(owner))) {
            throw invariant("cursor owner projection does not match L0 stream metadata");
        }
    }

    private static void validateRetention(
            CursorLedgerIdentity ledger,
            CursorRetentionRecord retention,
            StreamMetadata stream) {
        requireRetentionIdentity(ledger, retention);
        if (!stream.streamId().value().equals(retention.projection().streamId())) {
            throw invariant("retention root does not match L0 stream identity");
        }
        if (retention.protectedFloorOffset() > stream.committedEndOffset()) {
            throw invariant("retention protected floor exceeds committed end");
        }
        if (retention.lifecycle() == CursorRetentionLifecycle.TRIM_PENDING) {
            long target = retention.pendingTrimOffset().orElseThrow();
            if (stream.trimOffset() != retention.lastCompletedTrimOffset()
                    && stream.trimOffset() != target) {
                throw invariant("TRIM_PENDING does not bracket the L0 trim offset");
            }
            if (target > stream.committedEndOffset()) {
                throw invariant("TRIM_PENDING target exceeds committed end");
            }
        } else if (stream.trimOffset() != retention.lastCompletedTrimOffset()) {
            throw invariant("retention completed trim does not equal L0 trim truth");
        }
        if (retention.lifecycle() == CursorRetentionLifecycle.PROTECTION_PENDING) {
            long target = retention.pendingProtectionIntent().orElseThrow().targetMarkDeleteOffset();
            if (target < stream.trimOffset() || target > stream.committedEndOffset()) {
                throw invariant("pending cursor protection target is not retained");
            }
        }
    }

    private static void requireRetentionIdentity(
            CursorLedgerIdentity ledger, CursorRetentionRecord retention) {
        if (!retention.projection().equals(ledger.projection())) {
            throw invariant("retention projection identity mismatch");
        }
    }

    private static void requireOwner(
            CursorOwnerSession owner, CursorRetentionRecord retention)
            throws ManagedLedgerException.ManagedLedgerFencedException {
        requireRetentionIdentity(owner.ledger(), retention);
        if (!retention.ownerSessionId().equals(owner.ownerSessionId())) {
            throw fenced("cursor retention root is owned by another writable session");
        }
    }

    private static CursorRetentionRecord copyRetention(
            CursorRetentionRecord current,
            String ownerSessionId,
            long mutationSequence,
            long nowMillis) {
        return new CursorRetentionRecord(
                0,
                current.projection(),
                ownerSessionId,
                current.lifecycle(),
                mutationSequence,
                current.protectedFloorOffset(),
                current.lastCompletedTrimOffset(),
                current.pendingProtectionIntent(),
                current.pendingTrimAttemptId(),
                current.pendingTrimOffset(),
                current.pendingTrimReason(),
                Math.max(current.updatedAtMillis(), nowMillis));
    }

    private static CursorStateRecord claimCursorRecord(
            CursorStateRecord current, String ownerSessionId, long nowMillis) {
        if (current.lifecycle() != CursorRecordLifecycle.ACTIVE) {
            throw new IllegalArgumentException("only ACTIVE cursor roots are owner-claimed");
        }
        return new CursorStateRecord(
                0,
                current.projection(),
                ownerSessionId,
                current.cursorName(),
                current.cursorNameHash(),
                current.cursorGeneration(),
                current.lifecycle(),
                Math.addExact(current.mutationSequence(), 1),
                current.ackStateEpoch(),
                current.lastProtectionAttemptId(),
                current.markDeleteOffset(),
                current.snapshotReference(),
                current.inlineWholeAckDeltas(),
                current.inlinePartialAckOverrides(),
                current.positionProperties(),
                current.cursorProperties(),
                current.createdAtMillis(),
                Math.max(current.updatedAtMillis(), nowMillis),
                current.deletedAtMillis());
    }

    private static boolean provesAttempt(
            CursorStateRecord cursor, CursorProtectionIntentRecord intent) {
        return cursor.cursorName().equals(intent.cursorName())
                && cursor.cursorNameHash().equals(intent.cursorNameHash())
                && cursor.cursorGeneration() == intent.targetCursorGeneration()
                && cursor.lastProtectionAttemptId().equals(intent.attemptId());
    }

    private CursorProtectionIntentRecord toIntent(
            ProtectionRequest request, String attemptId, long createdAtMillis) {
        Optional<CursorPartialBatchAckRecord> partial = request.targetPartialBatch().map(state -> {
            if (state.isWholeEntryAcknowledged() || state.isAllRemaining()) {
                throw new IllegalArgumentException(
                        "protection target partial must be normalized durable partial state");
            }
            return new CursorPartialBatchAckRecord(
                    request.targetMarkDeleteOffset(), state.batchSize(), state.remainingWords());
        });
        return new CursorProtectionIntentRecord(
                attemptId,
                CursorProtectionKind.valueOf(request.kind().name()),
                request.cursorName(),
                request.cursorNameHash(),
                request.expectedCursorGeneration(),
                request.targetCursorGeneration(),
                request.targetMarkDeleteOffset(),
                partial,
                request.initialPositionProperties(),
                request.initialCursorProperties(),
                createdAtMillis);
    }

    private static boolean matches(
            CursorProtectionIntentRecord intent, ProtectionRequest request) {
        Optional<BatchAckState> partial = intent.targetPartialBatch().map(value ->
                new BatchAckState(value.batchSize(), value.remainingWords()));
        return intent.kind().name().equals(request.kind().name())
                && intent.cursorName().equals(request.cursorName())
                && intent.cursorNameHash().equals(request.cursorNameHash())
                && intent.expectedCursorGeneration() == request.expectedCursorGeneration()
                && intent.targetCursorGeneration() == request.targetCursorGeneration()
                && intent.targetMarkDeleteOffset() == request.targetMarkDeleteOffset()
                && partial.equals(request.targetPartialBatch())
                && intent.initialPositionProperties().equals(request.initialPositionProperties())
                && intent.initialCursorProperties().equals(request.initialCursorProperties());
    }

    private CursorRetentionView toView(
            CursorLedgerIdentity ledger, VersionedCursorRetention versioned) {
        CursorRetentionRecord value = versioned.value();
        requireRetentionIdentity(ledger, value);
        Optional<CursorRetentionView.PendingProtection> protection =
                value.pendingProtectionIntent().map(intent ->
                        new CursorRetentionView.PendingProtection(
                                intent.attemptId(),
                                CursorRetentionView.PendingProtection.Kind.valueOf(
                                        intent.kind().name()),
                                intent.cursorNameHash(),
                                intent.targetCursorGeneration(),
                                intent.targetMarkDeleteOffset()));
        Optional<CursorRetentionView.PendingTrim> trim =
                value.pendingTrimAttemptId().map(attempt ->
                        new CursorRetentionView.PendingTrim(
                                attempt,
                                value.pendingTrimOffset().orElseThrow(),
                                value.pendingTrimReason().orElseThrow()));
        return new CursorRetentionView(
                ledger,
                value.ownerSessionId(),
                CursorRetentionView.Lifecycle.valueOf(value.lifecycle().name()),
                value.mutationSequence(),
                versioned.metadataVersion(),
                value.protectedFloorOffset(),
                value.lastCompletedTrimOffset(),
                protection,
                trim);
    }

    private void requireProtectionIntentSize(CursorProtectionIntentRecord intent) {
        int nameBytes = strictUtf8(intent.cursorName(), "cursorName").length;
        if (nameBytes > config.cursorNameMaxUtf8Bytes()) {
            throw new IllegalArgumentException("cursorName exceeds cursorNameMaxUtf8Bytes");
        }
        long positionBytes = encodedLongMapBytes(intent.initialPositionProperties());
        long cursorBytes = encodedStringMapBytes(intent.initialCursorProperties());
        if (positionBytes > config.cursorPositionPropertiesMaxBytes()
                || cursorBytes > config.cursorPropertiesMaxBytes()) {
            throw new IllegalArgumentException("cursor protection properties exceed configured bounds");
        }
        long bytes = stringBytes(intent.attemptId());
        bytes = Math.addExact(bytes, 1);
        bytes = Math.addExact(bytes, stringBytes(intent.cursorName()));
        bytes = Math.addExact(bytes, stringBytes(intent.cursorNameHash()));
        bytes = Math.addExact(bytes, Long.BYTES * 3L + 1);
        if (intent.targetPartialBatch().isPresent()) {
            CursorPartialBatchAckRecord partial = intent.targetPartialBatch().orElseThrow();
            bytes = Math.addExact(
                    bytes,
                    Long.BYTES + Integer.BYTES * 2L
                            + Math.multiplyExact(
                                    (long) partial.remainingWords().length, Long.BYTES));
        }
        bytes = Math.addExact(bytes, positionBytes);
        bytes = Math.addExact(bytes, cursorBytes);
        bytes = Math.addExact(bytes, Long.BYTES);
        if (bytes > config.cursorProtectionIntentMaxBytes()) {
            throw new IllegalArgumentException(
                    "cursor protection intent exceeds cursorProtectionIntentMaxBytes");
        }
    }

    private void requireMetadataFits(CursorRetentionRecord record) {
        int bytes = F3MetadataCodecs.encodeEnvelope(record, CursorRetentionRecord.class).length;
        if (bytes > config.cursorMetadataValueMaxBytes()) {
            throw new IllegalArgumentException("cursor retention root exceeds metadata value max");
        }
    }

    private static long encodedLongMapBytes(Map<String, Long> values) {
        long bytes = Integer.BYTES;
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            bytes = Math.addExact(bytes, stringBytes(entry.getKey()) + Long.BYTES);
        }
        return bytes;
    }

    private static long encodedStringMapBytes(Map<String, String> values) {
        long bytes = Integer.BYTES;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            bytes = Math.addExact(bytes, stringBytes(entry.getKey()));
            bytes = Math.addExact(bytes, stringBytes(entry.getValue()));
        }
        return bytes;
    }

    private static long stringBytes(String value) {
        return Math.addExact(
                Integer.BYTES, strictUtf8(value, "metadata string").length);
    }

    private void validateTrimReasonInput(long candidateOffset, String reason) {
        if (candidateOffset < 0) {
            throw new IllegalArgumentException("candidateOffset must be non-negative");
        }
        if (reason.isBlank() || reason.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("trim reason must be nonblank and cannot contain NUL");
        }
        int reasonBytes = strictUtf8(reason, "trim reason").length;
        int fixedBytes = strictUtf8(
                        TRIM_REASON_PREFIX + "00000000000000000000000000000000:",
                        "trim reason prefix")
                .length;
        if ((long) fixedBytes + reasonBytes > config.cursorTrimReasonMaxUtf8Bytes()) {
            throw new IllegalArgumentException("composed trim reason exceeds cursorTrimReasonMaxUtf8Bytes");
        }
    }

    private String composeTrimReason(String attemptId, String reason) {
        String composed = TRIM_REASON_PREFIX + attemptId + ":" + reason;
        if (strictUtf8(composed, "composed trim reason").length
                > config.cursorTrimReasonMaxUtf8Bytes()) {
            throw new IllegalArgumentException("composed trim reason exceeds cursorTrimReasonMaxUtf8Bytes");
        }
        return composed;
    }

    private static byte[] strictUtf8(String value, String fieldName) {
        try {
            ByteBuffer bytes = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(Objects.requireNonNull(value, fieldName)));
            byte[] result = new byte[bytes.remaining()];
            bytes.get(result);
            return result;
        } catch (CharacterCodingException error) {
            throw new IllegalArgumentException(fieldName + " must be strict UTF-8", error);
        }
    }

    private static List<CursorFingerprint> fingerprint(List<VersionedCursorState> records) {
        return records.stream()
                .map(record -> new CursorFingerprint(
                        record.value().cursorName(),
                        record.value().cursorGeneration(),
                        record.value().lifecycle(),
                        record.value().ownerSessionId(),
                        record.metadataVersion()))
                .toList();
    }

    private void scheduleReconcile(CursorOwnerSession owner) {
        if (closed.get()) {
            return;
        }
        try {
            scheduler.execute(() -> reconcileFloor(owner).exceptionally(error -> null));
        } catch (RejectedExecutionException ignored) {
            // The trim is already durable; a later open/reconciliation safely raises the floor.
        }
    }

    private long nowMillis() {
        long now = clock.millis();
        if (now < 0) {
            throw new IllegalStateException("cursor clock returned a negative timestamp");
        }
        return now;
    }

    private String randomId(String fieldName) {
        return CursorIds.requireRandomId(randomIdSupplier.get(), fieldName);
    }

    private StreamId streamId(CursorOwnerSession owner) {
        return new StreamId(owner.ledger().projection().streamId());
    }

    private CompletableFuture<Void> requireAttempt(
            Deadline deadline, int attempt, String operation) {
        if (closed.get()) {
            return closedFuture();
        }
        if (attempt >= config.cursorMaxCasAttempts() || deadline.expired()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.TIMEOUT,
                    true,
                    operation + " exhausted its bounded CAS/deadline budget"));
        }
        return null;
    }

    private static <T> CompletableFuture<T> retryCondition(
            CompletableFuture<T> operation,
            Supplier<CompletableFuture<T>> retry) {
        return operation.handle((value, error) -> {
            if (error == null) {
                return CompletableFuture.completedFuture(value);
            }
            Throwable cause = unwrap(error);
            if (cause instanceof CursorMetadataConditionFailedException) {
                try {
                    return Objects.requireNonNull(retry.get(), "retry returned null future");
                } catch (Throwable retryError) {
                    return CompletableFuture.<T>failedFuture(retryError);
                }
            }
            return CompletableFuture.<T>failedFuture(cause);
        }).thenCompose(future -> future);
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = Objects.requireNonNull(error, "error");
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static ManagedLedgerException.ManagedLedgerFencedException fenced(String message) {
        return new ManagedLedgerException.ManagedLedgerFencedException(message);
    }

    private static ManagedLedgerException.ConcurrentFindCursorPositionException busy(String message) {
        return new ManagedLedgerException.ConcurrentFindCursorPositionException(message);
    }

    private static <T> CompletableFuture<T> closedFuture() {
        return CompletableFuture.failedFuture(
                new ManagedLedgerException.ManagedLedgerAlreadyClosedException(
                        "cursor retention coordinator is closed"));
    }

    private static Supplier<String> secureRandomIdSupplier() {
        SecureRandom random = new SecureRandom();
        return () -> {
            byte[] bytes = new byte[16];
            random.nextBytes(bytes);
            return HexFormat.of().formatHex(bytes);
        };
    }

    private record OpenContext(
            TopicProjectionRecord projection,
            StreamMetadata stream,
            List<VersionedCursorState> cursors,
            Optional<VersionedCursorRetention> retention) {
    }

    private record CursorFingerprint(
            String cursorName,
            long cursorGeneration,
            CursorRecordLifecycle lifecycle,
            String ownerSessionId,
            long metadataVersion) {
    }

    private static final class Deadline {
        private final long deadlineNanos;
        private final LongSupplier nanoTime;

        private Deadline(long deadlineNanos, LongSupplier nanoTime) {
            this.deadlineNanos = deadlineNanos;
            this.nanoTime = nanoTime;
        }

        private static Deadline start(java.time.Duration timeout, LongSupplier nanoTime) {
            long now = nanoTime.getAsLong();
            long timeoutNanos;
            try {
                timeoutNanos = timeout.toNanos();
            } catch (ArithmeticException ignored) {
                timeoutNanos = Long.MAX_VALUE;
            }
            long deadline;
            try {
                deadline = Math.addExact(now, timeoutNanos);
            } catch (ArithmeticException ignored) {
                deadline = Long.MAX_VALUE;
            }
            return new Deadline(deadline, nanoTime);
        }

        private boolean expired() {
            return nanoTime.getAsLong() - deadlineNanos >= 0;
        }
    }
}
