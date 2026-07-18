/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorSnapshotKeys;
import com.nereusstream.materialization.gc.OwnerlessObjectGcExecutor;
import com.nereusstream.materialization.gc.PhysicalObjectRootVisitor;
import com.nereusstream.materialization.gc.PhysicalRootTombstoneRetirementCoordinator;
import com.nereusstream.materialization.gc.SourceRetirementCoordinator;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerStreamProjection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Product router for one complete metadata-root pass; a fresh instance owns per-pass cursor deduplication. */
final class Phase4PhysicalRootLifecycleRouter
        implements PhysicalObjectRootVisitor {
    private final String cluster;
    private final ManagedLedgerProjectionMetadataStore projections;
    private final Function<CursorLedgerIdentity, CompletableFuture<Void>> scanCursor;
    private final BiFunction<CursorLedgerIdentity, VersionedPhysicalObjectRoot,
            CompletableFuture<Void>> recoverCursor;
    private final Function<VersionedPhysicalObjectRoot, CompletableFuture<Void>> executeOwnerless;
    private final Function<VersionedPhysicalObjectRoot, CompletableFuture<Void>> recoverOwnerless;
    private final Function<VersionedPhysicalObjectRoot, CompletableFuture<Void>> recoverDeleting;
    private final Function<VersionedPhysicalObjectRoot, CompletableFuture<Void>> retireTombstone;
    private final Set<StreamId> activeCursorStreams = new HashSet<>();

    Phase4PhysicalRootLifecycleRouter(
            String cluster,
            ManagedLedgerProjectionMetadataStore projections,
            CursorSnapshotGcExecutor cursorGc,
            OwnerlessObjectGcExecutor ownerlessGc,
            SourceRetirementCoordinator sourceRetirement,
            PhysicalRootTombstoneRetirementCoordinator tombstones) {
        this.cluster = requireText(cluster, "cluster");
        this.projections = Objects.requireNonNull(projections, "projections");
        CursorSnapshotGcExecutor exactCursor = Objects.requireNonNull(cursorGc, "cursorGc");
        OwnerlessObjectGcExecutor exactOwnerless = Objects.requireNonNull(
                ownerlessGc, "ownerlessGc");
        SourceRetirementCoordinator exactRetirement = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        PhysicalRootTombstoneRetirementCoordinator exactTombstones = Objects.requireNonNull(
                tombstones, "tombstones");
        this.scanCursor = ledger -> exactCursor.scan(ledger).thenApply(ignored -> null);
        this.recoverCursor = (ledger, root) -> exactCursor.recoverMarked(ledger, root)
                .thenApply(ignored -> null);
        this.executeOwnerless = root -> exactOwnerless.executeActive(root)
                .thenApply(ignored -> null);
        this.recoverOwnerless = root -> exactOwnerless.recoverMarked(root)
                .thenApply(ignored -> null);
        this.recoverDeleting = root -> exactRetirement.resume(root).thenApply(ignored -> null);
        this.retireTombstone = root -> exactTombstones.retire(root).thenApply(ignored -> null);
    }

    Phase4PhysicalRootLifecycleRouter(
            String cluster,
            ManagedLedgerProjectionMetadataStore projections,
            Function<CursorLedgerIdentity, CompletableFuture<Void>> scanCursor,
            BiFunction<CursorLedgerIdentity, VersionedPhysicalObjectRoot,
                    CompletableFuture<Void>> recoverCursor,
            Function<VersionedPhysicalObjectRoot, CompletableFuture<Void>> executeOwnerless,
            Function<VersionedPhysicalObjectRoot, CompletableFuture<Void>> recoverOwnerless,
            Function<VersionedPhysicalObjectRoot, CompletableFuture<Void>> recoverDeleting,
            Function<VersionedPhysicalObjectRoot, CompletableFuture<Void>> retireTombstone) {
        this.cluster = requireText(cluster, "cluster");
        this.projections = Objects.requireNonNull(projections, "projections");
        this.scanCursor = Objects.requireNonNull(scanCursor, "scanCursor");
        this.recoverCursor = Objects.requireNonNull(recoverCursor, "recoverCursor");
        this.executeOwnerless = Objects.requireNonNull(executeOwnerless, "executeOwnerless");
        this.recoverOwnerless = Objects.requireNonNull(recoverOwnerless, "recoverOwnerless");
        this.recoverDeleting = Objects.requireNonNull(recoverDeleting, "recoverDeleting");
        this.retireTombstone = Objects.requireNonNull(retireTombstone, "retireTombstone");
    }

    @Override
    public CompletableFuture<Void> visit(VersionedPhysicalObjectRoot root) {
        VersionedPhysicalObjectRoot exact = Objects.requireNonNull(root, "root");
        return switch (exact.value().lifecycle()) {
            case ACTIVE -> visitActive(exact);
            case MARKED -> visitMarked(exact);
            case DELETING -> require(recoverDeleting, exact, "DELETING recovery");
            case DELETED -> require(retireTombstone, exact, "DELETED tombstone retirement");
            case QUARANTINED -> CompletableFuture.completedFuture(null);
        };
    }

    private CompletableFuture<Void> visitActive(VersionedPhysicalObjectRoot root) {
        return resolveCursorLedger(root).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return require(executeOwnerless, root, "ownerless ACTIVE execution");
            }
            CursorLedgerIdentity ledger = optional.orElseThrow();
            StreamId stream = new StreamId(ledger.projection().streamId());
            if (!activeCursorStreams.add(stream)) {
                return CompletableFuture.completedFuture(null);
            }
            return require(scanCursor, ledger, "cursor ACTIVE scan");
        });
    }

    private CompletableFuture<Void> visitMarked(VersionedPhysicalObjectRoot root) {
        return resolveCursorLedger(root).thenCompose(optional -> optional
                .<CompletableFuture<Void>>map(ledger ->
                        require(recoverCursor, ledger, root, "cursor MARKED recovery"))
                .orElseGet(() -> require(
                        recoverOwnerless, root, "ownerless MARKED recovery")));
    }

    private CompletableFuture<Optional<CursorLedgerIdentity>> resolveCursorLedger(
            VersionedPhysicalObjectRoot root) {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
        if (object.kind() != PhysicalObjectKind.CURSOR_SNAPSHOT) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        final CursorSnapshotKeys.ParsedOwnerlessSnapshotKey parsed;
        try {
            parsed = CursorSnapshotKeys.parseOwnerless(
                    cluster, new ObjectKey(root.value().objectKey()));
        } catch (IllegalArgumentException malformed) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return projections.getProjectionByStream(cluster, parsed.streamId())
                .thenApply(view -> ledger(parsed.streamId(), view));
    }

    private static Optional<CursorLedgerIdentity> ledger(
            StreamId stream, ManagedLedgerStreamProjection view) {
        if (!view.streamId().equals(stream) || view.streamBinding().isEmpty()) {
            return Optional.empty();
        }
        var binding = view.streamBinding().orElseThrow().value();
        if (!binding.identity().streamId().equals(stream.value())) {
            throw new IllegalArgumentException(
                    "cursor physical-root projection binding belongs to another stream");
        }
        return Optional.of(new CursorLedgerIdentity(
                binding.managedLedgerName(),
                binding.managedLedgerNameHash(),
                binding.identity()));
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static <T> CompletableFuture<Void> require(
            Function<T, CompletableFuture<Void>> operation, T value, String stage) {
        try {
            return Objects.requireNonNull(operation.apply(value), stage + " returned null");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static <T, U> CompletableFuture<Void> require(
            BiFunction<T, U, CompletableFuture<Void>> operation,
            T first,
            U second,
            String stage) {
        try {
            return Objects.requireNonNull(
                    operation.apply(first, second), stage + " returned null");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
