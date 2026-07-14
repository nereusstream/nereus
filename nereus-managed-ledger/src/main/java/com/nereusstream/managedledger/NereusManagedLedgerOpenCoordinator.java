/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.managedledger.config.ManagedLedgerOpenConfigView;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorOwnerSession;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.managedledger.integration.NereusCreationPermit;
import com.nereusstream.managedledger.projection.F2L0RequestFactory;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.CursorIds;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.ProjectionCreateRequest;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/** Recoverable first/open/recreate protocol shared by factory open and durable-state inspection. */
public final class NereusManagedLedgerOpenCoordinator {
    private static final Map<String, String> PAYLOAD_ATTRIBUTES = Map.of(
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1);

    private final NereusManagedLedgerRuntime runtime;
    private final NereusCreationGuard creationGuard;
    private final Supplier<String> ownerSessionIdSupplier;
    private final F2L0RequestFactory requests = new F2L0RequestFactory();

    public NereusManagedLedgerOpenCoordinator(
            NereusManagedLedgerRuntime runtime,
            NereusCreationGuard creationGuard) {
        this(runtime, creationGuard, secureRandomIdSupplier());
    }

    NereusManagedLedgerOpenCoordinator(
            NereusManagedLedgerRuntime runtime,
            NereusCreationGuard creationGuard,
            Supplier<String> ownerSessionIdSupplier) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.creationGuard = Objects.requireNonNull(creationGuard, "creationGuard");
        this.ownerSessionIdSupplier = Objects.requireNonNull(
                ownerSessionIdSupplier, "ownerSessionIdSupplier");
    }

    public CompletableFuture<NereusLedgerOpenResult> open(
            String managedLedgerName,
            ManagedLedgerOpenConfigView config) {
        String exactName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(config, "config");
        return creationGuard.acquire(exactName)
                .thenCompose(permit -> {
                    requirePermit(exactName, permit);
                    return runtime.projectionStore().getProjection(runtime.cluster(), exactName)
                            .thenCompose(existing -> existing
                                    .map(topic -> openExisting(topic, permit, config))
                                    .orElseGet(() -> createFirst(exactName, permit, config)));
                });
    }

    public CompletableFuture<NereusWritableLedgerOpenResult> openWritable(
            String managedLedgerName,
            ManagedLedgerOpenConfigView config,
            NereusManagedLedgerOwnershipGuard ownershipGuard) {
        Objects.requireNonNull(ownershipGuard, "ownershipGuard");
        return ownershipGuard.requireOwned("writable open before cursor claim")
                .thenCompose(ignored -> open(managedLedgerName, config))
                .thenCompose(ledger -> hydrateWritable(ledger, ownershipGuard));
    }

    private CompletableFuture<NereusWritableLedgerOpenResult> hydrateWritable(
            NereusLedgerOpenResult ledger,
            NereusManagedLedgerOwnershipGuard ownershipGuard) {
        final CursorOwnerSession owner;
        try {
            String managedLedgerName = ledger.topicProjection().managedLedgerName();
            CursorLedgerIdentity cursorLedger = new CursorLedgerIdentity(
                    managedLedgerName,
                    ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName),
                    ledger.topicProjection().projectionIdentity());
            owner = new CursorOwnerSession(
                    cursorLedger,
                    CursorIds.requireRandomId(ownerSessionIdSupplier.get(), "ownerSessionId"));
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return runtime.cursorStorage().claimAndLoadActiveCursors(owner)
                .thenCompose(cursors -> runtime.cursorStorage().retentionView(owner)
                        .thenCompose(retention -> ownershipGuard
                                .requireOwned("writable open final publication")
                                .thenApply(ignored -> new NereusWritableLedgerOpenResult(
                                        ledger, owner, cursors, retention))));
    }

    public CompletableFuture<NereusStorageStateSnapshot> inspectStorageState(String managedLedgerName) {
        String exactName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        return runtime.projectionStore().getProjection(runtime.cluster(), exactName)
                .thenCompose(existing -> {
                    if (existing.isEmpty()) {
                        return CompletableFuture.completedFuture(NereusStorageStateSnapshot.missing());
                    }
                    TopicProjectionRecord topic = existing.orElseThrow();
                    return loadExact(topic).thenApply(metadata -> {
                        validateMirrorDoesNotLeadL0(topic, metadata);
                        VirtualLedgerProjection projection = toProjection(topic);
                        return new NereusStorageStateSnapshot(
                                durableState(metadata.state()),
                                Optional.of(projection),
                                Optional.of(metadata));
                    });
                });
    }

    private CompletableFuture<NereusLedgerOpenResult> openExisting(
            TopicProjectionRecord topic,
            NereusCreationPermit permit,
            ManagedLedgerOpenConfigView config) {
        requireBinding(topic, permit);
        return loadExact(topic).thenCompose(metadata -> reconcile(topic, metadata))
                .thenCompose(opened -> {
                    StreamState state = opened.streamMetadata().state();
                    if (state == StreamState.DELETING) {
                        return runtime.streamStorage().delete(
                                        opened.streamMetadata().streamId(),
                                        requests.deleteOptions(runtime.config().metadataTimeout()))
                                .thenCompose(deleted -> reconcile(opened.topicProjection(), deleted));
                    }
                    return CompletableFuture.completedFuture(opened);
                })
                .thenCompose(opened -> {
                    if (opened.streamMetadata().state() != StreamState.DELETED) {
                        return CompletableFuture.completedFuture(opened);
                    }
                    if (!config.createIfMissing()) {
                        return failed(ErrorCode.STREAM_NOT_FOUND, false, "managed ledger is deleted");
                    }
                    return recreate(opened.topicProjection(), permit, config);
                });
    }

    private CompletableFuture<NereusLedgerOpenResult> createFirst(
            String managedLedgerName,
            NereusCreationPermit permit,
            ManagedLedgerOpenConfigView config) {
        if (!config.createIfMissing()) {
            return failed(ErrorCode.STREAM_NOT_FOUND, false, "managed ledger does not exist");
        }
        return runtime.streamStorage().createOrGetStream(
                        ManagedLedgerProjectionNames.streamName(managedLedgerName, 1),
                        requests.createOptions())
                .thenCompose(candidate -> publishFirstOrFollowWinner(
                        managedLedgerName, permit, config, candidate));
    }

    private CompletableFuture<NereusLedgerOpenResult> publishFirstOrFollowWinner(
            String managedLedgerName,
            NereusCreationPermit permit,
            ManagedLedgerOpenConfigView config,
            StreamMetadata candidate) {
        ProjectionCreateRequest request;
        try {
            request = new ProjectionCreateRequest(
                    managedLedgerName,
                    permit.bindingGeneration(),
                    1,
                    candidate,
                    config.initialProperties());
        } catch (RuntimeException invalidCandidate) {
            return followPublishedWinnerOrFail(managedLedgerName, permit, config, invalidCandidate);
        }
        return runtime.projectionStore().createFirstProjection(
                        runtime.cluster(), request, permit::validateBeforeProjectionPublish)
                .thenCompose(topic -> {
                    requireBinding(topic, permit);
                    return loadReconcileAndRepair(topic);
                });
    }

    private CompletableFuture<NereusLedgerOpenResult> recreate(
            TopicProjectionRecord deleted,
            NereusCreationPermit permit,
            ManagedLedgerOpenConfigView config) {
        long nextIncarnation;
        try {
            nextIncarnation = Math.addExact(deleted.incarnation(), 1);
        } catch (ArithmeticException e) {
            return failed(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "topic incarnation is exhausted", e);
        }
        if (permit.bindingGeneration() <= deleted.storageClassBindingGeneration()) {
            return failed(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "recreation permit binding generation did not advance");
        }
        long incarnation = nextIncarnation;
        return runtime.streamStorage().createOrGetStream(
                        ManagedLedgerProjectionNames.streamName(deleted.managedLedgerName(), incarnation),
                        requests.createOptions())
                .thenCompose(candidate -> {
                    ProjectionCreateRequest request;
                    try {
                        request = new ProjectionCreateRequest(
                                deleted.managedLedgerName(),
                                permit.bindingGeneration(),
                                incarnation,
                                candidate,
                                config.initialProperties());
                    } catch (RuntimeException invalidCandidate) {
                        return followPublishedWinnerOrFail(
                                deleted.managedLedgerName(), permit, config, invalidCandidate);
                    }
                    return runtime.projectionStore().recreateDeletedProjection(
                                    runtime.cluster(),
                                    deleted.projectionIdentity(),
                                    deleted.metadataVersion(),
                                    request,
                                    permit::validateBeforeProjectionPublish)
                            .thenCompose(topic -> {
                                requireBinding(topic, permit);
                                return loadReconcileAndRepair(topic);
                            });
                });
    }

    private CompletableFuture<NereusLedgerOpenResult> followPublishedWinnerOrFail(
            String managedLedgerName,
            NereusCreationPermit permit,
            ManagedLedgerOpenConfigView config,
            RuntimeException invalidCandidate) {
        return runtime.projectionStore().getProjection(runtime.cluster(), managedLedgerName)
                .thenCompose(existing -> existing
                        .map(topic -> openExisting(topic, permit, config))
                        .orElseGet(() -> failed(
                                ErrorCode.METADATA_INVARIANT_VIOLATION,
                                false,
                                "deterministic candidate is not a canonical empty F2 stream",
                                invalidCandidate)));
    }

    private CompletableFuture<NereusLedgerOpenResult> loadReconcileAndRepair(TopicProjectionRecord topic) {
        return loadExact(topic)
                .thenCompose(metadata -> reconcile(topic, metadata))
                .thenCompose(opened -> runtime.projectionStore()
                        .repairProjectionIndexes(runtime.cluster(), opened.topicProjection())
                        .thenApply(ignored -> opened));
    }

    private CompletableFuture<StreamMetadata> loadExact(TopicProjectionRecord topic) {
        return runtime.streamStorage().getStreamMetadata(new StreamId(topic.streamId()))
                .handle((metadata, error) -> {
                    if (error == null) {
                        validateExact(topic, metadata);
                        return metadata;
                    }
                    Throwable cause = unwrap(error);
                    if (cause instanceof NereusException nereus
                            && nereus.code() == ErrorCode.STREAM_NOT_FOUND) {
                        throw new CompletionException(new NereusException(
                                ErrorCode.METADATA_INVARIANT_VIOLATION,
                                false,
                                "published projection points to a missing L0 stream",
                                nereus));
                    }
                    throw new CompletionException(cause);
                });
    }

    private CompletableFuture<NereusLedgerOpenResult> reconcile(
            TopicProjectionRecord initial,
            StreamMetadata metadata) {
        validateMirrorDoesNotLeadL0(initial, metadata);
        CompletableFuture<TopicProjectionRecord> topic = CompletableFuture.completedFuture(initial);
        if (metadata.state() == StreamState.SEALED
                && initial.parsedFacadeState() == ManagedLedgerFacadeState.OPEN) {
            topic = mirror(initial, ManagedLedgerFacadeState.SEALED);
        } else if ((metadata.state() == StreamState.DELETING || metadata.state() == StreamState.DELETED)
                && (initial.parsedFacadeState() == ManagedLedgerFacadeState.OPEN
                        || initial.parsedFacadeState() == ManagedLedgerFacadeState.SEALED)) {
            topic = mirror(initial, ManagedLedgerFacadeState.DELETING);
        }
        if (metadata.state() == StreamState.DELETED) {
            topic = topic.thenCompose(current -> current.parsedFacadeState() == ManagedLedgerFacadeState.DELETED
                    ? CompletableFuture.completedFuture(current)
                    : mirror(current, ManagedLedgerFacadeState.DELETED));
        }
        return topic.thenApply(current -> new NereusLedgerOpenResult(
                current, toProjection(current), metadata));
    }

    private CompletableFuture<TopicProjectionRecord> mirror(
            TopicProjectionRecord current,
            ManagedLedgerFacadeState target) {
        return runtime.projectionStore().mirrorFacadeState(
                runtime.cluster(),
                current.managedLedgerName(),
                current.projectionIdentity(),
                current.metadataVersion(),
                target);
    }

    private static void validateExact(TopicProjectionRecord topic, StreamMetadata metadata) {
        if (!metadata.streamId().value().equals(topic.streamId())
                || !metadata.streamName().value().equals(topic.streamName())
                || metadata.profile() != StorageProfile.OBJECT_WAL_SYNC_OBJECT
                || !metadata.attributes().equals(PAYLOAD_ATTRIBUTES)
                || metadata.createdAtMillis() != topic.createdAtMillis()) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "L0 stream identity/profile/payload mapping differs from topic projection");
        }
        if (metadata.state() == StreamState.CREATING) {
            throw new NereusException(
                    ErrorCode.STREAM_NOT_ACTIVE, true, "published F2 stream is still CREATING");
        }
    }

    private static void validateMirrorDoesNotLeadL0(
            TopicProjectionRecord topic,
            StreamMetadata metadata) {
        int mirror = facadeRank(topic.parsedFacadeState());
        int l0 = streamRank(metadata.state());
        if (mirror > l0) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "managed-ledger facade lifecycle mirror leads L0 truth");
        }
    }

    private static VirtualLedgerProjection toProjection(TopicProjectionRecord topic) {
        return new VirtualLedgerProjection(
                new StreamId(topic.streamId()),
                topic.managedLedgerName(),
                topic.storageClassBindingGeneration(),
                topic.incarnation(),
                topic.virtualLedgerId(),
                topic.positionMappingVersion(),
                topic.payloadMapping(),
                topic.createdAtMillis(),
                topic.metadataVersion());
    }

    private static void requirePermit(String name, NereusCreationPermit permit) {
        Objects.requireNonNull(permit, "creation permit");
        if (!name.equals(permit.persistenceName()) || permit.bindingGeneration() < 1) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION, false, "creation permit identity is invalid");
        }
    }

    private static void requireBinding(TopicProjectionRecord topic, NereusCreationPermit permit) {
        if (topic.parsedFacadeState() == ManagedLedgerFacadeState.DELETED
                && permit.bindingGeneration() > topic.storageClassBindingGeneration()) {
            return;
        }
        if (topic.storageClassBindingGeneration() != permit.bindingGeneration()) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "projection storage-class binding generation differs from the open permit");
        }
    }

    private static int facadeRank(ManagedLedgerFacadeState state) {
        return switch (state) {
            case OPEN -> 0;
            case SEALED -> 1;
            case DELETING -> 2;
            case DELETED -> 3;
        };
    }

    private static int streamRank(StreamState state) {
        return switch (state) {
            case CREATING -> -1;
            case ACTIVE -> 0;
            case SEALED -> 1;
            case DELETING -> 2;
            case DELETED -> 3;
        };
    }

    private static NereusDurableStorageState durableState(StreamState state) {
        return switch (state) {
            case ACTIVE -> NereusDurableStorageState.ACTIVE;
            case SEALED -> NereusDurableStorageState.SEALED;
            case DELETING -> NereusDurableStorageState.DELETING;
            case DELETED -> NereusDurableStorageState.DELETED;
            case CREATING -> throw new NereusException(
                    ErrorCode.STREAM_NOT_ACTIVE, true, "published F2 stream is still CREATING");
        };
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> CompletableFuture<T> failed(
            ErrorCode code,
            boolean retriable,
            String message) {
        return CompletableFuture.failedFuture(new NereusException(code, retriable, message));
    }

    private static <T> CompletableFuture<T> failed(
            ErrorCode code,
            boolean retriable,
            String message,
            Throwable cause) {
        return CompletableFuture.failedFuture(new NereusException(code, retriable, message, cause));
    }

    private static Supplier<String> secureRandomIdSupplier() {
        SecureRandom random = new SecureRandom();
        return () -> {
            byte[] bytes = new byte[16];
            random.nextBytes(bytes);
            return HexFormat.of().formatHex(bytes);
        };
    }
}
