/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.bookkeeper.BookKeeperDeletionActivationCoordinator;
import com.nereusstream.bookkeeper.BookKeeperDeletionActivationRequest;
import com.nereusstream.bookkeeper.BookKeeperLedgerGcConfiguration;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalRuntime;
import com.nereusstream.bookkeeper.BookKeeperStreamCoverageProofProvider;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * One-shot startup owner that installs or rebinds producer-owned BookKeeper deletion proofs before
 * the retention scanner starts.
 */
public final class KafkaBookKeeperDeletionActivationService
        implements KafkaRuntimeBackgroundService {
    private final BookKeeperPrimaryWalRuntime runtime;
    private final BookKeeperDeletionActivationCoordinator coordinator;
    private final String runId;
    private final Duration timeout;
    private final Object monitor = new Object();
    private CompletableFuture<Void> startOperation;

    public KafkaBookKeeperDeletionActivationService(
            BookKeeperPrimaryWalRuntime runtime,
            BookKeeperLedgerGcConfiguration gcConfiguration,
            BookKeeperStreamCoverageProofProvider streamCoverage,
            String runId,
            Duration timeout) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.coordinator =
                runtime.createDeletionActivationCoordinator(
                        Objects.requireNonNull(
                                gcConfiguration,
                                "gcConfiguration"),
                        Objects.requireNonNull(
                                streamCoverage,
                                "streamCoverage"));
        this.runId = text(runId, "runId");
        this.timeout = positive(timeout, "timeout");
    }

    @Override
    public CompletionStage<Void> start() {
        synchronized (monitor) {
            if (startOperation != null) {
                return startOperation.copy();
            }
            startOperation = activate();
            return startOperation.copy();
        }
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> activate() {
        BookKeeperOperationDeadline deadline =
                new BookKeeperOperationDeadline(timeout);
        return deadline.bound(
                        runtime.activationStore()
                                .read(
                                        runtime.configuration(),
                                        runtime.namespace(),
                                        deadline.remaining()))
                .thenCompose(
                        optional -> {
                            long expectedVersion =
                                    optional.orElseThrow(
                                                    () ->
                                                            new IllegalStateException(
                                                                    "BookKeeper publication activation is absent"))
                                            .metadataVersion();
                            return coordinator.activate(
                                    new BookKeeperDeletionActivationRequest(
                                            runId,
                                            expectedVersion,
                                            deadline.remaining()));
                        })
                .thenApply(ignored -> null);
    }

    private static Duration positive(Duration value, String name) {
        Duration exact = Objects.requireNonNull(value, name);
        if (exact.isZero()
                || exact.isNegative()
                || exact.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    name + " must be positive and millisecond-representable");
        }
        return exact;
    }

    private static String text(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(
                    name + " cannot be blank");
        }
        return exact;
    }
}
