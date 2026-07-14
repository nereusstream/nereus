/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.metadata.oxia.CursorMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.ManagedLedgerCursorProtocol;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/** Default F3 durable cursor storage with owner fencing and single-root CAS authority. */
public final class DefaultCursorStorage implements CursorStorage {
    private final String cluster;
    private final StreamStorage streamStorage;
    private final ManagedLedgerProjectionMetadataStore projectionStore;
    private final CursorMetadataStore metadataStore;
    private final CursorSnapshotStore snapshotStore;
    private final CursorRetentionCoordinator retentionCoordinator;
    private final CursorProtocolActivationGuard activationGuard;
    private final CursorStateMachine stateMachine;
    private final CursorStatePersistencePlanner persistencePlanner;
    private final CursorStorageConfig config;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final LongSupplier nanoTime;
    private final CursorStateHydrator hydrator;
    private final ConcurrentHashMap<CursorIdentity, CursorHandle> handles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<CursorLedgerIdentity, CompletableFuture<TopicProjectionRecord>>
            activationFlights =
            new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

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
            ScheduledExecutorService scheduler) {
        this(
                cluster,
                streamStorage,
                projectionStore,
                metadataStore,
                snapshotStore,
                retentionCoordinator,
                activationGuard,
                stateMachine,
                persistencePlanner,
                config,
                clock,
                scheduler,
                System::nanoTime);
    }

    DefaultCursorStorage(
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
            ScheduledExecutorService scheduler,
            LongSupplier nanoTime) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.streamStorage = Objects.requireNonNull(streamStorage, "streamStorage");
        this.projectionStore = Objects.requireNonNull(projectionStore, "projectionStore");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.snapshotStore = Objects.requireNonNull(snapshotStore, "snapshotStore");
        this.retentionCoordinator = Objects.requireNonNull(
                retentionCoordinator, "retentionCoordinator");
        this.activationGuard = Objects.requireNonNull(activationGuard, "activationGuard");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
        this.persistencePlanner = Objects.requireNonNull(
                persistencePlanner, "persistencePlanner");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.hydrator = new CursorStateHydrator(
                this.cluster, metadataStore, snapshotStore, persistencePlanner, config);
    }

    @Override
    public CompletableFuture<CursorHandle> open(
            CursorOwnerSession owner, String cursorName, CursorOpenRequest request) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(cursorName, "cursorName");
        Objects.requireNonNull(request, "request");
        if (closed.get()) {
            return closedFuture();
        }
        final String exactName;
        try {
            exactName = CursorNames.requireCursorName(cursorName);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        Deadline deadline = Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime);
        return retentionCoordinator.claimAndRecover(owner)
                .thenCompose(ignored -> openAttempt(owner, exactName, request, deadline, 0));
    }

    @Override
    public CompletableFuture<List<CursorHandle>> claimAndLoadActiveCursors(
            CursorOwnerSession owner) {
        Objects.requireNonNull(owner, "owner");
        if (closed.get()) {
            return closedFuture();
        }
        Deadline deadline = Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime);
        return retentionCoordinator.claimAndRecover(owner)
                .thenCompose(ignored -> claimAndLoadAttempt(owner, deadline, 0));
    }

    @Override
    public CompletableFuture<CursorMutationResult> cumulativeAck(
            CursorHandle handle, CursorAckRequest request) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(request, "request");
        return submit(handle, () -> {
            long capturedEpoch = handle.state().ackStateEpoch();
            Deadline deadline = Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime);
            return cumulativeAckAttempt(handle, request, capturedEpoch, deadline, 0);
        });
    }

    @Override
    public CompletableFuture<CursorMutationResult> individualAck(
            CursorHandle handle, List<CursorAckRequest> requests) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(requests, "requests");
        final List<CursorAckRequest> copied;
        try {
            copied = List.copyOf(requests);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return submit(handle, () -> {
            long capturedEpoch = handle.state().ackStateEpoch();
            Deadline deadline = Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime);
            return individualAckAttempt(handle, copied, capturedEpoch, deadline, 0);
        });
    }

    @Override
    public CompletableFuture<CursorMutationResult> reset(
            CursorHandle handle, CursorResetRequest request) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(request, "request");
        return submit(handle, () -> {
            long capturedEpoch = handle.state().ackStateEpoch();
            Deadline deadline = Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime);
            return resetAttempt(handle, request, capturedEpoch, deadline, 0);
        });
    }

    @Override
    public CompletableFuture<CursorMutationResult> clearBacklog(
            CursorHandle handle, long observedCommittedEndOffset) {
        Objects.requireNonNull(handle, "handle");
        return submit(handle, () -> {
            long capturedEpoch = handle.state().ackStateEpoch();
            Deadline deadline = Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime);
            return clearBacklogAttempt(
                    handle, observedCommittedEndOffset, capturedEpoch, deadline, 0);
        });
    }

    @Override
    public CompletableFuture<CursorMutationResult> mutateCursorProperties(
            CursorHandle handle, CursorPropertyMutation mutation) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(mutation, "mutation");
        return submit(handle, () -> mutatePropertiesAttempt(
                handle,
                mutation,
                Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime),
                0));
    }

    @Override
    public CompletableFuture<CursorMutationResult> flushPositionProperties(
            CursorHandle handle, Map<String, Long> stagedProperties) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(stagedProperties, "stagedProperties");
        final Map<String, Long> copied;
        try {
            copied = Map.copyOf(stagedProperties);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return submit(handle, () -> flushPropertiesAttempt(
                handle,
                copied,
                Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime),
                0));
    }

    @Override
    public CompletableFuture<Void> delete(CursorOwnerSession owner, String cursorName) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(cursorName, "cursorName");
        if (closed.get()) {
            return closedFuture();
        }
        final String exactName;
        try {
            exactName = CursorNames.requireCursorName(cursorName);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return deleteAttempt(
                owner,
                exactName,
                Deadline.start(config.cursorMetadataOperationTimeout(), nanoTime),
                0);
    }

    @Override
    public CompletableFuture<CursorRetentionView> retentionView(CursorOwnerSession owner) {
        Objects.requireNonNull(owner, "owner");
        if (closed.get()) {
            return closedFuture();
        }
        return metadataStore.getRetention(cluster, streamId(owner)).thenCompose(retention -> {
            if (retention.isEmpty()) {
                return CompletableFuture.failedFuture(invariant("cursor retention root is missing"));
            }
            try {
                return CompletableFuture.completedFuture(toRetentionView(
                        owner, retention.orElseThrow()));
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
        });
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            handles.values().forEach(CursorHandle::closeAsync);
            handles.clear();
            activationFlights.clear();
        }
    }

    private <T> CompletableFuture<T> submit(
            CursorHandle handle, Supplier<CompletableFuture<T>> operation) {
        if (closed.get()) {
            return closedFuture();
        }
        if (handle.isClosed()) {
            return CompletableFuture.failedFuture(
                    new ManagedLedgerException.CursorAlreadyClosedException(
                            "durable cursor handle is closed"));
        }
        return handle.mutationLane().submit(operation);
    }

    private CompletableFuture<CursorHandle> openAttempt(
            CursorOwnerSession owner,
            String cursorName,
            CursorOpenRequest request,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "open durable cursor");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return loadProjection(owner).thenCompose(projection ->
                metadataStore.getCursor(cluster, streamId(owner), cursorName).thenCompose(root -> {
                    if (root.isPresent()
                            && root.orElseThrow().value().lifecycle()
                                    == CursorRecordLifecycle.ACTIVE) {
                        if (!ManagedLedgerCursorProtocol.isActivated(projection)) {
                            return CompletableFuture.failedFuture(invariant(
                                    "ACTIVE cursor exists before cursor protocol activation"));
                        }
                        try {
                            validateCursorRoot(owner.ledger(), cursorName, root.orElseThrow().value());
                            requireCursorOwner(owner, root.orElseThrow().value());
                        } catch (Throwable error) {
                            return CompletableFuture.failedFuture(error);
                        }
                        return hydrateOpenHandle(owner, root.orElseThrow());
                    }
                    return ensureActivated(owner, projection).thenCompose(activated ->
                            streamStorage.getStreamMetadata(streamId(owner)).thenCompose(stream ->
                                    metadataStore.getCursor(
                                                    cluster, streamId(owner), cursorName)
                                            .thenCompose(revalidated -> {
                                                if (!sameRoot(root, revalidated)) {
                                                    return openAttempt(
                                                            owner,
                                                            cursorName,
                                                            request,
                                                            deadline,
                                                            attempt + 1);
                                                }
                                                CursorOpenRequest actual = new CursorOpenRequest(
                                                        request.initialPosition(),
                                                        request.initialPositionProperties(),
                                                        request.initialCursorProperties(),
                                                        stream.trimOffset(),
                                                        stream.committedEndOffset());
                                                boolean recreate = revalidated.isPresent();
                                                long expectedGeneration = recreate
                                                        ? revalidated.orElseThrow()
                                                                .value()
                                                                .cursorGeneration()
                                                        : 0;
                                                long targetGeneration = recreate
                                                        ? Math.addExact(expectedGeneration, 1)
                                                        : 1;
                                                CursorRetentionCoordinator.ProtectionRequest protection =
                                                        new CursorRetentionCoordinator.ProtectionRequest(
                                                                recreate
                                                                        ? CursorRetentionView.PendingProtection.Kind
                                                                                .RECREATE
                                                                        : CursorRetentionView.PendingProtection.Kind
                                                                                .CREATE,
                                                                cursorName,
                                                                CursorNames.cursorNameHash(cursorName),
                                                                expectedGeneration,
                                                                targetGeneration,
                                                                actual.initialMarkDeleteOffset(),
                                                                Optional.empty(),
                                                                actual.initialPositionProperties(),
                                                                actual.initialCursorProperties());
                                                return retentionCoordinator
                                                        .beginProtection(owner, protection)
                                                        .thenCompose(lease -> retentionCoordinator
                                                                .completeProtection(lease)
                                                                .thenCompose(ignored -> hydrator.load(
                                                                        owner.ledger(), cursorName))
                                                                .thenCompose(hydrated -> {
                                                                    CursorState state = hydrated.state();
                                                                    if (state.lifecycle()
                                                                                    != CursorLifecycle.ACTIVE
                                                                            || state.identity()
                                                                                            .cursorGeneration()
                                                                                    != targetGeneration
                                                                            || !state.lastProtectionAttemptId()
                                                                                    .equals(lease.attemptId())
                                                                            || !state.ownerSessionId()
                                                                                    .equals(owner.ownerSessionId())) {
                                                                        return CompletableFuture.failedFuture(
                                                                                invariant(
                                                                                        "protected cursor create is not proved by the root"));
                                                                    }
                                                                    try {
                                                                        validateStateBounds(state, stream);
                                                                        return retentionCoordinator
                                                                                .reconcileFloor(owner)
                                                                                .thenApply(view -> {
                                                                                    requireRetentionViewOwner(
                                                                                            owner, view);
                                                                                    return newHandle(
                                                                                            owner, state);
                                                                                });
                                                                    } catch (Throwable error) {
                                                                        return CompletableFuture.failedFuture(error);
                                                                    }
                                                                }))
                                                        .handle((value, error) -> {
                                                            if (error == null) {
                                                                return CompletableFuture.completedFuture(value);
                                                            }
                                                            Throwable cause = unwrap(error);
                                                            if (cause instanceof ManagedLedgerException
                                                                            .ConcurrentFindCursorPositionException
                                                                    || cause
                                                                            instanceof CursorMetadataConditionFailedException) {
                                                                return retentionCoordinator
                                                                        .claimAndRecover(owner)
                                                                        .thenCompose(ignored -> openAttempt(
                                                                                owner,
                                                                                cursorName,
                                                                                request,
                                                                                deadline,
                                                                                attempt + 1));
                                                            }
                                                            return CompletableFuture
                                                                    .<CursorHandle>failedFuture(cause);
                                                        })
                                                        .thenCompose(future -> future);
                                            })));
                }));
    }

    private CompletableFuture<CursorHandle> hydrateOpenHandle(
            CursorOwnerSession owner, VersionedCursorState root) {
        return hydrator.hydrate(owner.ledger(), root).thenCompose(hydrated ->
                streamStorage.getStreamMetadata(streamId(owner)).thenCompose(stream -> {
                    try {
                        validateStateBounds(hydrated.state(), stream);
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                    return retentionView(owner).thenApply(view -> {
                        requireRetentionViewOwner(owner, view);
                        return newHandle(owner, hydrated.state());
                    });
                }));
    }

    private CompletableFuture<TopicProjectionRecord> ensureActivated(
            CursorOwnerSession owner, TopicProjectionRecord projection) {
        if (ManagedLedgerCursorProtocol.isActivated(projection)) {
            return CompletableFuture.completedFuture(projection);
        }
        CursorLedgerIdentity flightKey = owner.ledger();
        CompletableFuture<TopicProjectionRecord> existing = activationFlights.get(flightKey);
        if (existing != null) {
            return existing;
        }
        CompletableFuture<TopicProjectionRecord> candidate = new CompletableFuture<>();
        existing = activationFlights.putIfAbsent(flightKey, candidate);
        if (existing != null) {
            return existing;
        }
        CompletableFuture<TopicProjectionRecord> activation;
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
            activation = CompletableFuture.failedFuture(error);
        }
        activation.whenComplete((activated, error) -> {
            if (error == null) {
                candidate.complete(activated);
            } else {
                activationFlights.remove(flightKey, candidate);
                candidate.completeExceptionally(unwrap(error));
            }
        });
        return candidate;
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

    private CompletableFuture<List<CursorHandle>> claimAndLoadAttempt(
            CursorOwnerSession owner, Deadline deadline, int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "claim durable cursors");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return loadProjection(owner).thenCompose(projection ->
                scanAll(owner.ledger()).thenCompose(first -> {
                    if (!ManagedLedgerCursorProtocol.isActivated(projection) && !first.isEmpty()) {
                        return CompletableFuture.failedFuture(invariant(
                                "cursor records exist before protocol activation"));
                    }
                    return claimActiveRoots(owner, first, 0).handle((ignored, claimError) -> {
                        if (claimError == null) {
                            return scanAll(owner.ledger()).thenCompose(second -> {
                                if (!fingerprint(first).equals(fingerprint(second))) {
                                    return claimAndLoadAttempt(owner, deadline, attempt + 1);
                                }
                                for (VersionedCursorState record : second) {
                                    if (record.value().lifecycle()
                                                    == CursorRecordLifecycle.ACTIVE
                                            && !record.value().ownerSessionId()
                                                    .equals(owner.ownerSessionId())) {
                                        return claimAndLoadAttempt(owner, deadline, attempt + 1);
                                    }
                                }
                                return streamStorage.getStreamMetadata(streamId(owner))
                                        .thenCompose(stream -> hydrateActiveRoots(
                                                        owner,
                                                        second,
                                                        stream,
                                                        0,
                                                        new ArrayList<>())
                                                .thenCompose(states -> scanAll(owner.ledger())
                                                        .thenCompose(third -> {
                                                            if (!fingerprint(second)
                                                                    .equals(fingerprint(third))) {
                                                                return claimAndLoadAttempt(
                                                                        owner,
                                                                        deadline,
                                                                        attempt + 1);
                                                            }
                                                            return retentionView(owner)
                                                                    .thenCompose(view -> {
                                                                        if (view.lifecycle()
                                                                                != CursorRetentionView.Lifecycle
                                                                                        .ACTIVE) {
                                                                            return CompletableFuture.failedFuture(
                                                                                    invariant(
                                                                                            "pending retention survived open recovery"));
                                                                        }
                                                                        return retentionCoordinator
                                                                                .reconcileFloor(owner)
                                                                                .thenApply(floor -> {
                                                                                    requireRetentionViewOwner(
                                                                                            owner, floor);
                                                                                    return states.stream()
                                                                                            .map(state -> newHandle(
                                                                                                    owner, state))
                                                                                            .toList();
                                                                                });
                                                                    });
                                                        })));
                            });
                        }
                        Throwable cause = unwrap(claimError);
                        if (cause instanceof CursorMetadataConditionFailedException) {
                            return claimAndLoadAttempt(owner, deadline, attempt + 1);
                        }
                        return CompletableFuture.<List<CursorHandle>>failedFuture(cause);
                    }).thenCompose(future -> future);
                }));
    }

    private CompletableFuture<Void> claimActiveRoots(
            CursorOwnerSession owner, List<VersionedCursorState> records, int index) {
        if (index >= records.size()) {
            return CompletableFuture.completedFuture(null);
        }
        int end = Math.min(
                records.size(), Math.addExact(index, config.cursorOwnerClaimConcurrency()));
        List<CompletableFuture<?>> writes = new ArrayList<>();
        try {
            for (int cursor = index; cursor < end; cursor++) {
                VersionedCursorState current = records.get(cursor);
                validateCursorRoot(
                        owner.ledger(), current.value().cursorName(), current.value());
                if (current.value().lifecycle() == CursorRecordLifecycle.DELETED
                        || current.value().ownerSessionId().equals(owner.ownerSessionId())) {
                    continue;
                }
                CursorStateRecord claimed = claimCursorRecord(
                        current.value(), owner.ownerSessionId(), nowMillis());
                writes.add(metadataStore.compareAndSetCursor(
                        cluster, claimed, current.metadataVersion()));
            }
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new))
                .thenCompose(ignored -> claimActiveRoots(owner, records, end));
    }

    private CompletableFuture<List<CursorState>> hydrateActiveRoots(
            CursorOwnerSession owner,
            List<VersionedCursorState> records,
            StreamMetadata stream,
            int index,
            List<CursorState> states) {
        if (index >= records.size()) {
            return CompletableFuture.completedFuture(List.copyOf(states));
        }
        VersionedCursorState root = records.get(index);
        if (root.value().lifecycle() == CursorRecordLifecycle.DELETED) {
            return hydrateActiveRoots(owner, records, stream, index + 1, states);
        }
        return hydrator.hydrate(owner.ledger(), root).thenCompose(hydrated -> {
            try {
                requireStateOwner(owner, hydrated.state());
                validateStateBounds(hydrated.state(), stream);
                states.add(hydrated.state());
                return hydrateActiveRoots(owner, records, stream, index + 1, states);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
        });
    }

    private CompletableFuture<CursorMutationResult> cumulativeAckAttempt(
            CursorHandle handle,
            CursorAckRequest request,
            long capturedEpoch,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "cumulative cursor ack");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return loadForMutation(handle).thenCompose(current -> {
            try {
                if (stateMachine.isCumulativeAckSubsumed(current.state(), request)) {
                    return completedMutation(handle, CursorMutationOutcome.ALREADY_APPLIED, current.state());
                }
                requireAckEpoch(current.state(), capturedEpoch);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return streamStorage.getStreamMetadata(streamId(handle.owner())).thenCompose(stream -> {
                final CursorMutationResult transition;
                try {
                    transition = stateMachine.cumulativeAck(
                            current.state(),
                            request,
                            stream.trimOffset(),
                            stream.committedEndOffset(),
                            nowMillis());
                    if (transition.outcome() == CursorMutationOutcome.ALREADY_APPLIED) {
                        return completedMutation(
                                handle, CursorMutationOutcome.ALREADY_APPLIED, current.state());
                    }
                } catch (Throwable error) {
                    return CompletableFuture.failedFuture(error);
                }
                return persistMutation(current, transition.state())
                        .thenCompose(state -> completedMutation(
                                handle, CursorMutationOutcome.APPLIED, state))
                        .handle((value, error) -> retryMutation(
                                value,
                                error,
                                () -> cumulativeAckAttempt(
                                        handle,
                                        request,
                                        capturedEpoch,
                                        deadline,
                                        attempt + 1)))
                        .thenCompose(future -> future);
            });
        });
    }

    private CompletableFuture<CursorMutationResult> individualAckAttempt(
            CursorHandle handle,
            List<CursorAckRequest> requests,
            long capturedEpoch,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "individual cursor ack");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return loadForMutation(handle).thenCompose(current -> {
            try {
                if (stateMachine.isIndividualAckSubsumed(current.state(), requests)) {
                    return completedMutation(handle, CursorMutationOutcome.ALREADY_APPLIED, current.state());
                }
                requireAckEpoch(current.state(), capturedEpoch);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return streamStorage.getStreamMetadata(streamId(handle.owner())).thenCompose(stream -> {
                final CursorMutationResult transition;
                try {
                    transition = stateMachine.individualAck(
                            current.state(),
                            requests,
                            stream.trimOffset(),
                            stream.committedEndOffset(),
                            nowMillis());
                    if (transition.outcome() == CursorMutationOutcome.ALREADY_APPLIED) {
                        return completedMutation(
                                handle, CursorMutationOutcome.ALREADY_APPLIED, current.state());
                    }
                } catch (Throwable error) {
                    return CompletableFuture.failedFuture(error);
                }
                return persistMutation(current, transition.state())
                        .thenCompose(state -> completedMutation(
                                handle, CursorMutationOutcome.APPLIED, state))
                        .handle((value, error) -> retryMutation(
                                value,
                                error,
                                () -> individualAckAttempt(
                                        handle,
                                        requests,
                                        capturedEpoch,
                                        deadline,
                                        attempt + 1)))
                        .thenCompose(future -> future);
            });
        });
    }

    private CompletableFuture<CursorMutationResult> resetAttempt(
            CursorHandle handle,
            CursorResetRequest request,
            long capturedEpoch,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "reset durable cursor");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return loadForMutation(handle).thenCompose(current ->
                streamStorage.getStreamMetadata(streamId(handle.owner())).thenCompose(stream -> {
                    final CursorResetRequest actual;
                    final CursorAckState normalizedTarget;
                    try {
                        if (request.nextReadOffset() < stream.trimOffset()
                                || request.nextReadOffset() > stream.committedEndOffset()) {
                            throw new ManagedLedgerException.InvalidCursorPositionException(
                                    "reset target is outside current retained committed bounds");
                        }
                        actual = new CursorResetRequest(
                                request.nextReadOffset(),
                                request.targetBatchAck(),
                                request.force(),
                                stream.trimOffset(),
                                stream.committedEndOffset());
                        TreeMap<Long, BatchAckState> partial = new TreeMap<>();
                        actual.targetBatchAck().ifPresent(state ->
                                partial.put(actual.nextReadOffset(), state));
                        normalizedTarget = new CursorAckState(
                                actual.nextReadOffset(), List.of(), partial);
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                    if (normalizedTarget.markDeleteOffset()
                            < current.state().acknowledgements().markDeleteOffset()) {
                        return protectedReset(
                                handle,
                                current,
                                normalizedTarget,
                                actual,
                                deadline,
                                attempt);
                    }
                    try {
                        if (stateMachine.isExactResetResult(
                                current.state(), actual, capturedEpoch)) {
                            return completedMutation(
                                    handle, CursorMutationOutcome.ALREADY_APPLIED, current.state());
                        }
                        requireAckEpoch(current.state(), capturedEpoch);
                        CursorMutationResult transition = stateMachine.reset(
                                current.state(),
                                actual,
                                current.state().lastProtectionAttemptId(),
                                nowMillis());
                        return persistMutation(current, transition.state())
                                .thenCompose(state -> completedMutation(
                                        handle, CursorMutationOutcome.APPLIED, state))
                                .handle((value, error) -> retryMutation(
                                        value,
                                        error,
                                        () -> resetAttempt(
                                                handle,
                                                request,
                                                capturedEpoch,
                                                deadline,
                                                attempt + 1)))
                                .thenCompose(future -> future);
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                }));
    }

    private CompletableFuture<CursorMutationResult> protectedReset(
            CursorHandle handle,
            CursorStatePersistencePlanner.HydratedState current,
            CursorAckState normalizedTarget,
            CursorResetRequest actual,
            Deadline deadline,
            int attempt) {
        Optional<BatchAckState> partial = Optional.ofNullable(
                normalizedTarget.partialBatchAcks().get(normalizedTarget.markDeleteOffset()));
        CursorRetentionCoordinator.ProtectionRequest protection =
                new CursorRetentionCoordinator.ProtectionRequest(
                        CursorRetentionView.PendingProtection.Kind.BACKWARD_RESET,
                        current.state().identity().cursorName(),
                        current.state().identity().cursorNameHash(),
                        current.state().identity().cursorGeneration(),
                        current.state().identity().cursorGeneration(),
                        normalizedTarget.markDeleteOffset(),
                        partial,
                        Map.of(),
                        Map.of());
        return retentionCoordinator.beginProtection(handle.owner(), protection)
                .thenCompose(lease -> retentionCoordinator.completeProtection(lease)
                        .handle((view, completionError) -> new ProtectionCompletion(
                                lease, unwrapNullable(completionError)))
                        .thenCompose(completion -> hydrator
                                .load(handle.owner().ledger(), handle.identity().cursorName())
                                .thenCompose(hydrated -> {
                                    CursorState state = hydrated.state();
                                    if (state.lifecycle() == CursorLifecycle.DELETED) {
                                        return CompletableFuture.failedFuture(
                                                new ManagedLedgerException.CursorAlreadyClosedException(
                                                        "cursor was deleted during backward reset"));
                                    }
                                    if (completion.error() != null) {
                                        return CompletableFuture.failedFuture(completion.error());
                                    }
                                    if (!state.identity().equals(handle.identity())
                                            || !state.ownerSessionId()
                                                    .equals(handle.owner().ownerSessionId())
                                            || !state.lastProtectionAttemptId()
                                                    .equals(completion.lease().attemptId())) {
                                        return CompletableFuture.failedFuture(invariant(
                                                "backward reset completion lacks durable attempt proof"));
                                    }
                                    return completedMutation(
                                            handle, CursorMutationOutcome.APPLIED, state);
                                })))
                .handle((value, error) -> {
                    if (error == null) {
                        return CompletableFuture.completedFuture(value);
                    }
                    Throwable cause = unwrap(error);
                    if (cause instanceof CursorMetadataConditionFailedException) {
                        return retentionCoordinator.claimAndRecover(handle.owner())
                                .thenCompose(ignored -> resetAttempt(
                                        handle,
                                        actual,
                                        handle.state().ackStateEpoch(),
                                        deadline,
                                        attempt + 1));
                    }
                    return CompletableFuture.<CursorMutationResult>failedFuture(cause);
                })
                .thenCompose(future -> future);
    }

    private CompletableFuture<CursorMutationResult> clearBacklogAttempt(
            CursorHandle handle,
            long capturedEnd,
            long capturedEpoch,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "clear durable cursor backlog");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        if (capturedEnd < 0) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("observedCommittedEndOffset must be non-negative"));
        }
        return loadForMutation(handle).thenCompose(current ->
                streamStorage.getStreamMetadata(streamId(handle.owner())).thenCompose(stream -> {
                    try {
                        if (capturedEnd < stream.trimOffset()
                                || capturedEnd > stream.committedEndOffset()) {
                            throw new ManagedLedgerException.InvalidCursorPositionException(
                                    "captured clear-backlog end is outside current L0 bounds");
                        }
                        if (stateMachine.isExactClearResult(
                                current.state(), capturedEnd, capturedEpoch)) {
                            return completedMutation(
                                    handle, CursorMutationOutcome.ALREADY_APPLIED, current.state());
                        }
                        requireAckEpoch(current.state(), capturedEpoch);
                        CursorMutationResult transition = stateMachine.clearBacklog(
                                current.state(), capturedEnd, nowMillis());
                        return persistMutation(current, transition.state())
                                .thenCompose(state -> completedMutation(
                                        handle, CursorMutationOutcome.APPLIED, state))
                                .handle((value, error) -> retryMutation(
                                        value,
                                        error,
                                        () -> clearBacklogAttempt(
                                                handle,
                                                capturedEnd,
                                                capturedEpoch,
                                                deadline,
                                                attempt + 1)))
                                .thenCompose(future -> future);
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                }));
    }

    private CompletableFuture<CursorMutationResult> mutatePropertiesAttempt(
            CursorHandle handle,
            CursorPropertyMutation mutation,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "mutate cursor properties");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return loadForMutation(handle).thenCompose(current -> {
            final CursorMutationResult transition;
            try {
                transition = stateMachine.mutateCursorProperties(
                        current.state(), mutation, nowMillis());
                if (transition.outcome() == CursorMutationOutcome.ALREADY_APPLIED) {
                    return completedMutation(
                            handle, CursorMutationOutcome.ALREADY_APPLIED, current.state());
                }
                if (attempt > 0 && mutation instanceof CursorPropertyMutation.ReplaceExternal) {
                    return CompletableFuture.failedFuture(
                            new ManagedLedgerException.BadVersionException(
                                    "concurrent cursor root change conflicts with property replacement"));
                }
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return persistMutation(current, transition.state())
                    .thenCompose(state -> completedMutation(
                            handle, CursorMutationOutcome.APPLIED, state))
                    .handle((value, error) -> retryMutation(
                            value,
                            error,
                            () -> mutatePropertiesAttempt(
                                    handle, mutation, deadline, attempt + 1)))
                    .thenCompose(future -> future);
        });
    }

    private CompletableFuture<CursorMutationResult> flushPropertiesAttempt(
            CursorHandle handle,
            Map<String, Long> properties,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "flush cursor position properties");
        if (admitted != null) {
            return admitted.thenApply(ignored -> null);
        }
        return loadForMutation(handle).thenCompose(current -> {
            if (current.state().positionProperties().equals(properties)) {
                return completedMutation(
                        handle, CursorMutationOutcome.ALREADY_APPLIED, current.state());
            }
            if (attempt > 0) {
                return CompletableFuture.failedFuture(new ManagedLedgerException.BadVersionException(
                        "concurrent cursor root change conflicts with position-property flush"));
            }
            final CursorMutationResult transition;
            try {
                transition = stateMachine.flushPositionProperties(
                        current.state(), properties, nowMillis());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return persistMutation(current, transition.state())
                    .thenCompose(state -> completedMutation(
                            handle, CursorMutationOutcome.APPLIED, state))
                    .handle((value, error) -> retryMutation(
                            value,
                            error,
                            () -> flushPropertiesAttempt(
                                    handle, properties, deadline, attempt + 1)))
                    .thenCompose(future -> future);
        });
    }

    private CompletableFuture<CursorStatePersistencePlanner.HydratedState> loadForMutation(
            CursorHandle handle) {
        return hydrator.load(handle.owner().ledger(), handle.identity().cursorName())
                .thenCompose(current -> {
                    try {
                        requireHandleState(handle, current.state());
                    } catch (Throwable error) {
                        return CompletableFuture.failedFuture(error);
                    }
                    return metadataStore.getRetention(
                                    cluster, streamId(handle.owner()))
                            .thenCompose(retention -> {
                                if (retention.isEmpty()) {
                                    return CompletableFuture.failedFuture(invariant(
                                            "cursor mutation found no retention root"));
                                }
                                CursorRetentionRecord value = retention.orElseThrow().value();
                                try {
                                    requireRetentionOwner(handle.owner(), value);
                                    if (value.lifecycle()
                                                    == CursorRetentionLifecycle.PROTECTION_PENDING
                                            && value.pendingProtectionIntent()
                                                    .orElseThrow()
                                                    .cursorName()
                                                    .equals(handle.identity().cursorName())
                                            && !value.pendingProtectionIntent()
                                                    .orElseThrow()
                                                    .attemptId()
                                                    .equals(current.state()
                                                            .lastProtectionAttemptId())) {
                                        throw new ManagedLedgerException
                                                .ConcurrentFindCursorPositionException(
                                                "cursor is the target of a pending protected transition");
                                    }
                                    return CompletableFuture.completedFuture(current);
                                } catch (Throwable error) {
                                    return CompletableFuture.failedFuture(error);
                                }
                            });
                });
    }

    private CompletableFuture<CursorState> persistMutation(
            CursorStatePersistencePlanner.HydratedState current,
            CursorState candidate) {
        final CursorStatePersistencePlanner.PersistencePlan plan;
        try {
            plan = persistencePlanner.plan(current, candidate);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        if (plan instanceof CursorStatePersistencePlanner.InlinePlan inline) {
            return compareAndPublish(
                    inline.record(),
                    candidate,
                    candidate.snapshotReference(),
                    current.root().metadataVersion());
        }
        CursorStatePersistencePlanner.SnapshotPlan snapshot =
                (CursorStatePersistencePlanner.SnapshotPlan) plan;
        return snapshotStore.write(snapshot.request()).thenCompose(reference -> {
            final CursorStateRecord root;
            try {
                root = persistencePlanner.afterSnapshot(candidate, reference);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return compareAndPublish(
                    root,
                    candidate,
                    Optional.of(reference),
                    current.root().metadataVersion());
        });
    }

    private CompletableFuture<CursorState> compareAndPublish(
            CursorStateRecord root,
            CursorState candidate,
            Optional<CursorSnapshotReference> reference,
            long expectedVersion) {
        return metadataStore.compareAndSetCursor(cluster, root, expectedVersion)
                .thenApply(updated -> persistencePlanner.persisted(
                        candidate, reference, updated.metadataVersion()));
    }

    private CompletableFuture<Void> deleteAttempt(
            CursorOwnerSession owner,
            String cursorName,
            Deadline deadline,
            int attempt) {
        CompletableFuture<Void> admitted = requireAttempt(deadline, attempt, "delete durable cursor");
        if (admitted != null) {
            return admitted;
        }
        return metadataStore.getRetention(cluster, streamId(owner)).thenCompose(retention -> {
            if (retention.isEmpty()) {
                return CompletableFuture.failedFuture(invariant(
                        "cursor delete found no retention root"));
            }
            try {
                requireRetentionOwner(owner, retention.orElseThrow().value());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return metadataStore.getCursor(cluster, streamId(owner), cursorName).thenCompose(root -> {
                if (root.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                VersionedCursorState current = root.orElseThrow();
                try {
                    validateCursorRoot(owner.ledger(), cursorName, current.value());
                    if (current.value().lifecycle() == CursorRecordLifecycle.DELETED) {
                        closeHandle(owner, current.value());
                        return CompletableFuture.completedFuture(null);
                    }
                    requireCursorOwner(owner, current.value());
                } catch (Throwable error) {
                    return CompletableFuture.failedFuture(error);
                }
                final CursorStateRecord tombstone;
                try {
                    long updatedAt = Math.max(current.value().updatedAtMillis(), nowMillis());
                    tombstone = new CursorStateRecord(
                            0,
                            current.value().projection(),
                            current.value().ownerSessionId(),
                            current.value().cursorName(),
                            current.value().cursorNameHash(),
                            current.value().cursorGeneration(),
                            CursorRecordLifecycle.DELETED,
                            Math.addExact(current.value().mutationSequence(), 1),
                            current.value().ackStateEpoch(),
                            current.value().lastProtectionAttemptId(),
                            current.value().markDeleteOffset(),
                            Optional.empty(),
                            List.of(),
                            List.of(),
                            Map.of(),
                            Map.of(),
                            current.value().createdAtMillis(),
                            updatedAt,
                            OptionalLong.of(updatedAt));
                } catch (Throwable error) {
                    return CompletableFuture.failedFuture(error);
                }
                return metadataStore.compareAndSetCursor(
                                cluster, tombstone, current.metadataVersion())
                        .thenCompose(updated -> {
                            closeHandle(owner, updated.value());
                            retentionCoordinator.reconcileFloor(owner).exceptionally(error -> null);
                            return CompletableFuture.completedFuture(null);
                        })
                        .handle((value, error) -> {
                            if (error == null) {
                                return CompletableFuture.<Void>completedFuture(null);
                            }
                            Throwable cause = unwrap(error);
                            if (isRetryableCas(cause)) {
                                return deleteAttempt(owner, cursorName, deadline, attempt + 1);
                            }
                            return CompletableFuture.<Void>failedFuture(cause);
                        })
                        .thenCompose(future -> future);
            });
        });
    }

    private void closeHandle(CursorOwnerSession owner, CursorStateRecord root) {
        CursorIdentity identity = new CursorIdentity(
                owner.ledger(),
                root.cursorName(),
                root.cursorNameHash(),
                root.cursorGeneration());
        CursorHandle removed = handles.remove(identity);
        if (removed != null) {
            removed.closeAsync();
        }
    }

    private CompletableFuture<TopicProjectionRecord> loadProjection(CursorOwnerSession owner) {
        return projectionStore
                .getProjection(cluster, owner.ledger().managedLedgerName())
                .thenCompose(projection -> {
                    if (projection.isEmpty()) {
                        return CompletableFuture.failedFuture(invariant(
                                "managed-ledger projection is missing for cursor storage"));
                    }
                    TopicProjectionRecord value = projection.orElseThrow();
                    if (!value.managedLedgerName().equals(owner.ledger().managedLedgerName())
                            || !value.managedLedgerNameHash()
                                    .equals(owner.ledger().managedLedgerNameHash())
                            || !value.projectionIdentity().equals(owner.ledger().projection())) {
                        return CompletableFuture.failedFuture(invariant(
                                "managed-ledger projection identity changed during cursor storage operation"));
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
                            validateCursorRoot(ledger, record.value().cursorName(), record.value());
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

    private synchronized CursorHandle newHandle(CursorOwnerSession owner, CursorState state) {
        if (closed.get()) {
            throw new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "cursor storage closed before handle publication");
        }
        return handles.compute(state.identity(), (identity, existing) -> {
            if (existing != null
                    && !existing.isClosed()
                    && existing.owner().equals(owner)) {
                existing.publish(state);
                return existing;
            }
            if (existing != null) {
                existing.closeAsync();
            }
            return new CursorHandle(
                    state, owner, config.cursorMutationQueueMax(), scheduler);
        });
    }

    private static void validateCursorRoot(
            CursorLedgerIdentity ledger, String cursorName, CursorStateRecord root) {
        if (!root.projection().equals(ledger.projection())
                || !root.cursorName().equals(cursorName)
                || !root.cursorNameHash().equals(CursorNames.cursorNameHash(cursorName))) {
            throw invariant("cursor root identity does not match its requested ledger/name");
        }
    }

    private static void requireCursorOwner(
            CursorOwnerSession owner, CursorStateRecord root)
            throws ManagedLedgerException.ManagedLedgerFencedException {
        if (!root.ownerSessionId().equals(owner.ownerSessionId())) {
            throw fenced("cursor root is owned by another writable session");
        }
    }

    private static void requireStateOwner(
            CursorOwnerSession owner, CursorState state)
            throws ManagedLedgerException.ManagedLedgerFencedException {
        if (!state.identity().ledger().equals(owner.ledger())
                || !state.ownerSessionId().equals(owner.ownerSessionId())) {
            throw fenced("hydrated cursor state belongs to another writable session");
        }
    }

    private static void requireHandleState(CursorHandle handle, CursorState state)
            throws ManagedLedgerException {
        if (!state.identity().equals(handle.identity())) {
            throw new ManagedLedgerException.CursorAlreadyClosedException(
                    "cursor generation changed under a stale handle");
        }
        if (state.lifecycle() != CursorLifecycle.ACTIVE) {
            throw new ManagedLedgerException.CursorAlreadyClosedException(
                    "durable cursor was deleted");
        }
        if (!state.ownerSessionId().equals(handle.owner().ownerSessionId())) {
            throw fenced("durable cursor handle was fenced by a newer owner");
        }
    }

    private static void requireRetentionOwner(
            CursorOwnerSession owner, CursorRetentionRecord retention)
            throws ManagedLedgerException.ManagedLedgerFencedException {
        if (!retention.projection().equals(owner.ledger().projection())) {
            throw invariant("cursor retention projection identity mismatch");
        }
        if (!retention.ownerSessionId().equals(owner.ownerSessionId())) {
            throw fenced("cursor retention root is owned by another writable session");
        }
    }

    private static void requireRetentionViewOwner(
            CursorOwnerSession owner, CursorRetentionView retention) {
        if (!retention.ledger().equals(owner.ledger())
                || !retention.ownerSessionId().equals(owner.ownerSessionId())) {
            throw new CompletionException(fenced(
                    "cursor retention view changed before handle publication"));
        }
    }

    private static void validateStateBounds(CursorState state, StreamMetadata stream) {
        CursorAckState ack = state.acknowledgements();
        if (!stream.streamId().value().equals(state.identity().ledger().projection().streamId())
                || ack.markDeleteOffset() < stream.trimOffset()
                || ack.markDeleteOffset() > stream.committedEndOffset()) {
            throw invariant("cursor mark-delete is outside authoritative L0 bounds");
        }
        for (OffsetRange range : ack.wholeAckRanges()) {
            if (range.endOffset() > stream.committedEndOffset()) {
                throw invariant("cursor whole-ack range exceeds committed end");
            }
        }
        if (!ack.partialBatchAcks().isEmpty()
                && ack.partialBatchAcks().lastKey() >= stream.committedEndOffset()) {
            throw invariant("cursor partial batch offset exceeds committed end");
        }
    }

    private static void requireAckEpoch(CursorState state, long capturedEpoch)
            throws ManagedLedgerException.ConcurrentFindCursorPositionException {
        if (state.ackStateEpoch() != capturedEpoch) {
            throw new ManagedLedgerException.ConcurrentFindCursorPositionException(
                    "cursor ack state was destructively replaced during mutation retry");
        }
    }

    private CompletableFuture<CursorMutationResult> completedMutation(
            CursorHandle handle, CursorMutationOutcome outcome, CursorState state) {
        try {
            handle.publish(state);
            return CompletableFuture.completedFuture(new CursorMutationResult(outcome, state));
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    private static CompletableFuture<CursorMutationResult> retryMutation(
            CursorMutationResult value,
            Throwable error,
            Supplier<CompletableFuture<CursorMutationResult>> retry) {
        if (error == null) {
            return CompletableFuture.completedFuture(value);
        }
        Throwable cause = unwrap(error);
        if (isRetryableCas(cause)) {
            try {
                return Objects.requireNonNull(retry.get(), "mutation retry returned null future");
            } catch (Throwable retryError) {
                return CompletableFuture.failedFuture(retryError);
            }
        }
        return CompletableFuture.failedFuture(cause);
    }

    private static boolean isRetryableCas(Throwable error) {
        if (error instanceof CursorMetadataConditionFailedException) {
            return true;
        }
        if (error instanceof NereusException nereus) {
            return nereus.code() == ErrorCode.TIMEOUT
                    || nereus.code() == ErrorCode.METADATA_UNAVAILABLE
                    || nereus.code() == ErrorCode.METADATA_CONDITION_FAILED
                    || nereus.code() == ErrorCode.CANCELLED;
        }
        return false;
    }

    private CursorRetentionView toRetentionView(
            CursorOwnerSession owner, VersionedCursorRetention versioned)
            throws ManagedLedgerException.ManagedLedgerFencedException {
        CursorRetentionRecord value = versioned.value();
        requireRetentionOwner(owner, value);
        Optional<CursorRetentionView.PendingProtection> protection =
                value.pendingProtectionIntent().map(intent ->
                        new CursorRetentionView.PendingProtection(
                                intent.attemptId(),
                                CursorRetentionView.PendingProtection.Kind.valueOf(
                                        intent.kind().name()),
                                intent.cursorNameHash(),
                                intent.targetCursorGeneration(),
                                intent.targetMarkDeleteOffset()));
        Optional<CursorRetentionView.PendingTrim> trim = value.pendingTrimAttemptId().map(attempt ->
                new CursorRetentionView.PendingTrim(
                        attempt,
                        value.pendingTrimOffset().orElseThrow(),
                        value.pendingTrimReason().orElseThrow()));
        return new CursorRetentionView(
                owner.ledger(),
                value.ownerSessionId(),
                CursorRetentionView.Lifecycle.valueOf(value.lifecycle().name()),
                value.mutationSequence(),
                versioned.metadataVersion(),
                value.protectedFloorOffset(),
                value.lastCompletedTrimOffset(),
                protection,
                trim);
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

    private static boolean sameRoot(
            Optional<VersionedCursorState> left, Optional<VersionedCursorState> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return left.isEmpty() && right.isEmpty();
        }
        return left.orElseThrow().metadataVersion() == right.orElseThrow().metadataVersion()
                && left.orElseThrow().value().equals(right.orElseThrow().value());
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

    private long nowMillis() {
        long now = clock.millis();
        if (now < 0) {
            throw new IllegalStateException("cursor clock returned a negative timestamp");
        }
        return now;
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

    private static Throwable unwrapNullable(Throwable error) {
        return error == null ? null : unwrap(error);
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

    private static <T> CompletableFuture<T> closedFuture() {
        return CompletableFuture.failedFuture(
                new ManagedLedgerException.ManagedLedgerAlreadyClosedException(
                        "cursor storage is closed"));
    }

    private record CursorFingerprint(
            String cursorName,
            long cursorGeneration,
            CursorRecordLifecycle lifecycle,
            String ownerSessionId,
            long metadataVersion) {
    }

    private record ProtectionCompletion(
            CursorRetentionCoordinator.ProtectionLease lease,
            Throwable error) {
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
