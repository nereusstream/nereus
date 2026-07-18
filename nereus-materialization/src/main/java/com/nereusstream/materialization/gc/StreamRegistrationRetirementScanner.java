/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.StreamRegistrationScanPage;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import java.util.EnumMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/** Complete 64-shard driver for proof-driven, registration-last stream retirement. */
public final class StreamRegistrationRetirementScanner implements AutoCloseable {
    private final String cluster;
    private final GenerationMetadataStore generations;
    private final Function<StreamId, CompletableFuture<StreamRegistrationRetirementResult>> retire;
    private final PhysicalGcConfig config;
    private final ScheduledExecutorService scheduler;
    private final F4Keyspace keys;
    private final AtomicBoolean scanning = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public StreamRegistrationRetirementScanner(
            String cluster,
            GenerationMetadataStore generations,
            StreamRegistrationRetirementCoordinator coordinator,
            PhysicalGcConfig config,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.retire = Objects.requireNonNull(coordinator, "coordinator")::retire;
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.keys = new F4Keyspace(cluster);
    }

    StreamRegistrationRetirementScanner(
            String cluster,
            GenerationMetadataStore generations,
            Function<StreamId, CompletableFuture<StreamRegistrationRetirementResult>> retire,
            PhysicalGcConfig config,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.retire = Objects.requireNonNull(retire, "retire");
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.keys = new F4Keyspace(cluster);
    }

    public CompletableFuture<StreamRegistrationRetirementScanResult> scan() {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closed(
                    "registration-retirement scan rejected after close"));
        }
        if (!scanning.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "a registration-retirement scan is already running"));
        }
        if (closed.get()) {
            scanning.set(false);
            return CompletableFuture.failedFuture(closed(
                    "registration-retirement scan raced close"));
        }
        Counts counts = new Counts();
        CompletableFuture<StreamRegistrationRetirementScanResult> result =
                scanShard(0, counts);
        result.whenComplete((ignored, failure) -> scanning.set(false));
        return result;
    }

    private CompletableFuture<StreamRegistrationRetirementScanResult> scanShard(
            int shard, Counts counts) {
        if (shard == F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS) {
            return CompletableFuture.completedFuture(counts.result(shard));
        }
        return scanPage(shard, Optional.empty(), null, counts).thenCompose(ignored ->
                scanShard(shard + 1, counts));
    }

    private CompletableFuture<Void> scanPage(
            int shard,
            Optional<F4ScanToken> continuation,
            String previousKey,
            Counts counts) {
        return bound(
                        () -> generations.scanStreamRegistrations(
                                cluster,
                                shard,
                                continuation,
                                config.metadataScanPageSize()),
                        "scan stream-registration retirement shard " + shard)
                .thenCompose(page -> {
                    requireProgress(page, previousKey);
                    return visitPage(shard, page, 0, counts).thenCompose(ignored -> {
                        if (page.continuation().isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        String lastKey = page.values()
                                .get(page.values().size() - 1)
                                .key();
                        return scanPage(
                                shard,
                                page.continuation(),
                                lastKey,
                                counts);
                    });
                });
    }

    private CompletableFuture<Void> visitPage(
            int shard,
            StreamRegistrationScanPage page,
            int index,
            Counts counts) {
        if (index == page.values().size()) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedMaterializationStreamRegistration registration = page.values().get(index);
        StreamId stream = requireExactRegistration(shard, registration);
        counts.scanned = Math.addExact(counts.scanned, 1);
        return bound(
                        () -> retire.apply(stream),
                        "retire deleted stream registration " + stream.value())
                .thenAccept(result -> {
                    if (!result.streamId().equals(stream)) {
                        throw invariant(
                                "registration-retirement coordinator returned another stream");
                    }
                    counts.add(result.status());
                })
                .thenCompose(ignored -> visitPage(shard, page, index + 1, counts));
    }

    private StreamId requireExactRegistration(
            int shard, VersionedMaterializationStreamRegistration registration) {
        StreamId stream = new StreamId(registration.value().streamId());
        if (keys.materializationRegistryShard(stream) != shard
                || !registration.key().equals(keys.materializationRegistryKey(stream))) {
            throw invariant("registration scan returned a key/value outside its exact shard");
        }
        return stream;
    }

    private <T> CompletableFuture<T> bound(
            Supplier<CompletableFuture<T>> operation, String stage) {
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<T> result = deadline.bound(operation, stage);
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private static void requireProgress(
            StreamRegistrationScanPage page, String previousKey) {
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("registration-retirement scan did not advance monotonically");
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

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static final class Counts {
        private final EnumMap<StreamRegistrationRetirementStatus, Long> statuses =
                new EnumMap<>(StreamRegistrationRetirementStatus.class);
        private long scanned;

        private Counts() {
            for (StreamRegistrationRetirementStatus status :
                    StreamRegistrationRetirementStatus.values()) {
                statuses.put(status, 0L);
            }
        }

        private void add(StreamRegistrationRetirementStatus status) {
            statuses.compute(Objects.requireNonNull(status, "status"),
                    (ignored, count) -> Math.addExact(count, 1));
        }

        private StreamRegistrationRetirementScanResult result(int shards) {
            return new StreamRegistrationRetirementScanResult(
                    shards, scanned, statuses);
        }
    }
}
