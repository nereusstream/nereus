/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.retirement.RetirementMetadataClient;
import com.nereusstream.metadata.oxia.retirement.RetirementMetadataKey;
import com.nereusstream.metadata.oxia.retirement.RetirementMetadataValue;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.OxiaException;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** One owned Oxia client plus request/watch executors shared by metadata adapters. */
public final class SharedOxiaClientRuntime implements AutoCloseable {
    private final OxiaClientConfiguration configuration;
    private final SyncOxiaClient oxiaClient;
    private final ExecutorService clientExecutor;
    private final ExecutorService watchExecutor;
    private final PartitionedOxiaClient client;
    private final AtomicBoolean closed = new AtomicBoolean();

    public static SharedOxiaClientRuntime connect(
            OxiaClientConfiguration configuration,
            Clock clock) {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(clock, "clock");
        try {
            SyncOxiaClient oxiaClient = OxiaClientBuilder.create(configuration.serviceAddress())
                    .namespace(configuration.namespace())
                    .requestTimeout(configuration.requestTimeout())
                    .sessionTimeout(configuration.sessionTimeout())
                    .syncClient();
            return new SharedOxiaClientRuntime(configuration, oxiaClient);
        } catch (OxiaException e) {
            throw new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE, true, "failed to create shared Oxia client runtime", e);
        }
    }

    static SharedOxiaClientRuntime usingClient(
            OxiaClientConfiguration configuration,
            SyncOxiaClient oxiaClient,
            Clock clock) {
        Objects.requireNonNull(clock, "clock");
        return new SharedOxiaClientRuntime(configuration, oxiaClient);
    }

    private SharedOxiaClientRuntime(
            OxiaClientConfiguration configuration,
            SyncOxiaClient oxiaClient) {
        OxiaClientConfiguration checked = Objects.requireNonNull(configuration, "configuration");
        this.configuration = checked;
        this.oxiaClient = Objects.requireNonNull(oxiaClient, "oxiaClient");
        this.clientExecutor = Executors.newFixedThreadPool(8, daemonFactory("nereus-oxia-client"));
        this.watchExecutor = boundedExecutor(
                2, checked.maxPendingOperations(), "nereus-oxia-watch");
        try {
            this.client = new PartitionedOxiaClient(
                    new OxiaJavaClientBackend(oxiaClient, clientExecutor, watchExecutor));
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    PartitionedOxiaClient client() {
        ensureOpen();
        return client;
    }

    Executor clientExecutor() {
        ensureOpen();
        return clientExecutor;
    }

    Executor watchExecutor() {
        ensureOpen();
        return watchExecutor;
    }

    boolean isClosed() {
        return closed.get();
    }

    void requireCompatible(OxiaClientConfiguration candidate) {
        if (!configuration.equals(Objects.requireNonNull(candidate, "candidate"))) {
            throw new IllegalArgumentException("Oxia adapter configuration does not match the shared runtime");
        }
        ensureOpen();
    }

    /** Returns a borrowed read/delete-only view used by exact Phase 4 retirement adapters. */
    public RetirementMetadataClient retirementMetadataClient(OxiaClientConfiguration candidate) {
        requireCompatible(candidate);
        return new RetirementMetadataClient() {
            @Override
            public java.util.concurrent.CompletableFuture<java.util.Optional<RetirementMetadataValue>> get(
                    RetirementMetadataKey key) {
                ensureOpen();
                return client.get(key.key(), key.partitionKey()).thenApply(optional -> optional.map(value ->
                        new RetirementMetadataValue(value.key(), value.value(), value.version())));
            }

            @Override
            public java.util.concurrent.CompletableFuture<Void> deleteIfVersion(
                    RetirementMetadataKey key, long expectedVersion) {
                ensureOpen();
                return client.deleteIfVersion(key.key(), expectedVersion, key.partitionKey());
            }
        };
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        watchExecutor.shutdown();
        clientExecutor.shutdown();
        try {
            oxiaClient.close();
        } catch (Exception ignored) {
            // Runtime close is best effort after both adapter admission paths have stopped.
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new NereusException(ErrorCode.STORAGE_CLOSED, false, "shared Oxia client runtime is closed");
        }
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong ids = new AtomicLong();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static ExecutorService boundedExecutor(int threads, int queueCapacity, String prefix) {
        return new ThreadPoolExecutor(
                threads,
                threads,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                daemonFactory(prefix),
                new ThreadPoolExecutor.AbortPolicy());
    }
}
