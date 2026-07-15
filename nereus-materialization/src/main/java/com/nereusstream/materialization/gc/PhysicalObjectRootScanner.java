/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectRootScanPage;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Complete 256-shard metadata-root enumerator; object-store listing is never recovery authority. */
public final class PhysicalObjectRootScanner implements AutoCloseable {
    public static final int ROOT_SHARDS = 256;

    private final String cluster;
    private final PhysicalGcConfig config;
    private final PhysicalObjectMetadataStore metadataStore;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean scanning = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public PhysicalObjectRootScanner(
            String cluster,
            PhysicalGcConfig config,
            PhysicalObjectMetadataStore metadataStore,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    public CompletableFuture<PhysicalObjectRootScanResult> scan(
            PhysicalObjectRootVisitor visitor) {
        Objects.requireNonNull(visitor, "visitor");
        if (closed.get()) {
            return CompletableFuture.failedFuture(closed("physical-root scan rejected after close"));
        }
        if (!scanning.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("a physical-root scan is already running"));
        }
        if (closed.get()) {
            scanning.set(false);
            return CompletableFuture.failedFuture(closed("physical-root scan raced close"));
        }
        Counts counts = new Counts();
        CompletableFuture<PhysicalObjectRootScanResult> result = scanShard(
                0, Optional.empty(), null, visitor, counts);
        result.whenComplete((ignored, failure) -> scanning.set(false));
        return result;
    }

    private CompletableFuture<PhysicalObjectRootScanResult> scanShard(
            int shard,
            Optional<F4ScanToken> continuation,
            String lastKey,
            PhysicalObjectRootVisitor visitor,
            Counts counts) {
        if (shard == ROOT_SHARDS) {
            return CompletableFuture.completedFuture(counts.result());
        }
        return bound(
                        () -> metadataStore.scanRoots(
                                cluster,
                                shard,
                                continuation,
                                config.metadataScanPageSize()),
                        "scan physical-root shard " + shard)
                .thenCompose(page -> {
                    requireIncreasingPage(page, lastKey);
                    return visitPage(page, 0, visitor, counts).thenCompose(ignored -> {
                        if (page.continuation().isPresent()) {
                            String nextLast = page.values().get(page.values().size() - 1).key();
                            return scanShard(
                                    shard,
                                    page.continuation(),
                                    nextLast,
                                    visitor,
                                    counts);
                        }
                        return scanShard(
                                shard + 1,
                                Optional.empty(),
                                null,
                                visitor,
                                counts);
                    });
                });
    }

    private CompletableFuture<Void> visitPage(
            PhysicalObjectRootScanPage page,
            int index,
            PhysicalObjectRootVisitor visitor,
            Counts counts) {
        if (index == page.values().size()) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedPhysicalObjectRoot root = page.values().get(index);
        counts.add(root);
        return bound(() -> visitor.visit(root), "visit physical root " + root.key())
                .thenCompose(ignored -> visitPage(page, index + 1, visitor, counts));
    }

    private <T> CompletableFuture<T> bound(
            Supplier<CompletableFuture<T>> operation, String stage) {
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<T> result = deadline.bound(operation, stage);
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private static void requireIncreasingPage(
            PhysicalObjectRootScanPage page, String lastKey) {
        if (lastKey != null && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(lastKey) <= 0) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "physical-root scan did not advance monotonically");
        }
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static NereusException closed(String message) {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, message);
    }

    private static final class Counts {
        private long active;
        private long marked;
        private long deleting;
        private long deleted;
        private long quarantined;

        private void add(VersionedPhysicalObjectRoot root) {
            switch (root.value().lifecycle()) {
                case ACTIVE -> active = Math.addExact(active, 1);
                case MARKED -> marked = Math.addExact(marked, 1);
                case DELETING -> deleting = Math.addExact(deleting, 1);
                case DELETED -> deleted = Math.addExact(deleted, 1);
                case QUARANTINED -> quarantined = Math.addExact(quarantined, 1);
            }
        }

        private PhysicalObjectRootScanResult result() {
            return new PhysicalObjectRootScanResult(
                    active, marked, deleting, deleted, quarantined);
        }
    }
}
