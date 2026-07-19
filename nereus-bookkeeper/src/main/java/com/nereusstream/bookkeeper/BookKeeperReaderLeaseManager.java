/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Fixed-slot whole-ledger reader pins; each foreground call owns one unique bounded lease. */
public final class BookKeeperReaderLeaseManager {
    public final class Lease implements AutoCloseable {
        private final BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> value;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> value) {
            this.value = Objects.requireNonNull(value, "value");
        }

        public BookKeeperLedgerReaderLeaseRecord value() {
            return value.value();
        }

        public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> revalidate() {
            if (released.get()) {
                return CompletableFuture.failedFuture(invariant("BookKeeper reader lease was already released"));
            }
            var leaseFuture = metadata.getReaderLease(
                    cluster,
                    configuration.providerScopeSha256(),
                    value.value().ledgerId(),
                    value.value().readerSlot());
            var rootFuture = metadata.getRoot(
                    cluster,
                    configuration.providerScopeSha256(),
                    value.value().ledgerId());
            return CompletableFuture.allOf(leaseFuture, rootFuture).thenApply(ignored -> {
                BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> current = leaseFuture.join()
                        .orElseThrow(() -> invariant("BookKeeper reader lease disappeared during provider read"));
                if (current.metadataVersion() != value.metadataVersion()
                        || !current.durableValueSha256().equals(value.durableValueSha256())
                        || current.value().expiresAtMillis() <= clock.millis()) {
                    throw invariant("BookKeeper reader lease changed or expired during provider read");
                }
                BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root = rootFuture.join()
                        .orElseThrow(() -> invariant("BookKeeper ledger root disappeared during provider read"));
                requirePinnedReadableRoot(root.value(), current.value());
                return root;
            });
        }

        public CompletableFuture<Void> release() {
            if (!released.compareAndSet(false, true)) {
                return CompletableFuture.completedFuture(null);
            }
            return metadata.deleteReaderLease(
                    cluster,
                    configuration.providerScopeSha256(),
                    value.value().ledgerId(),
                    value.value().readerSlot(),
                    value.metadataVersion());
        }

        @Override
        public void close() {
            release().exceptionally(ignored -> null);
        }
    }

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerMetadataStore metadata;
    private final Clock clock;
    private final String processRunId;
    private final AtomicLong leaseSequence = new AtomicLong();

    public BookKeeperReaderLeaseManager(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerMetadataStore metadata,
            Clock clock,
            String processRunId) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.processRunId = text(processRunId, "processRunId");
    }

    public CompletableFuture<Lease> claim(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Duration timeout) {
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> exact = Objects.requireNonNull(root, "root");
        requireNewReadableRoot(exact.value());
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(min(
                Objects.requireNonNull(timeout, "timeout"),
                configuration.operationTimeout()));
        long sequence = leaseSequence.getAndIncrement();
        if (sequence == -1L) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_LIMIT_EXCEEDED, false, "BookKeeper reader lease sequence exhausted"));
        }
        String owner = processRunId + "/" + Long.toUnsignedString(sequence);
        return claimSlot(exact, owner, deadline, 0);
    }

    private CompletableFuture<Lease> claimSlot(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            String owner,
            BookKeeperOperationDeadline deadline,
            int slot) {
        if (slot >= configuration.maxReaderLeasesPerLedger()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED, true,
                    "BookKeeper ledger reader lease slots are exhausted"));
        }
        return deadline.bound(metadata.getReaderLease(
                        cluster,
                        configuration.providerScopeSha256(),
                        root.value().ledgerId(),
                        slot))
                .thenCompose(optional -> {
                    if (optional.isEmpty()) {
                        return create(root, owner, slot).handle((created, failure) -> {
                            if (failure == null) return CompletableFuture.completedFuture(new Lease(created));
                            Throwable cause = unwrap(failure);
                            if (cause instanceof BookKeeperMetadataConditionFailedException) {
                                return claimSlot(root, owner, deadline, slot + 1);
                            }
                            return CompletableFuture.<Lease>failedFuture(cause);
                        }).thenCompose(java.util.function.Function.identity());
                    }
                    BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> current = optional.orElseThrow();
                    if (current.value().processRunId().equals(owner)) {
                        return CompletableFuture.completedFuture(new Lease(current));
                    }
                    if (current.value().expiresAtMillis() > clock.millis()) {
                        return claimSlot(root, owner, deadline, slot + 1);
                    }
                    return metadata.deleteReaderLease(
                                    cluster,
                                    configuration.providerScopeSha256(),
                                    root.value().ledgerId(),
                                    slot,
                                    current.metadataVersion())
                            .handle((ignored, failure) -> {
                                if (failure == null) {
                                    return claimSlot(root, owner, deadline, slot);
                                }
                                Throwable cause = unwrap(failure);
                                if (cause instanceof BookKeeperMetadataConditionFailedException) {
                                    return claimSlot(root, owner, deadline, slot + 1);
                                }
                                return CompletableFuture.<Lease>failedFuture(cause);
                            })
                            .thenCompose(java.util.function.Function.identity());
                });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> create(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            String owner,
            int slot) {
        long now = clock.millis();
        long expiresAt;
        try {
            expiresAt = Math.addExact(now, configuration.readerLeaseTtl().toMillis());
        } catch (ArithmeticException overflow) {
            expiresAt = Long.MAX_VALUE;
        }
        BookKeeperLedgerReaderLeaseRecord desired = new BookKeeperLedgerReaderLeaseRecord(
                1,
                root.value().ledgerIdentitySha256(),
                root.value().ledgerId(),
                root.value().lifecycleEpoch(),
                slot,
                owner,
                1,
                now,
                expiresAt,
                0);
        return metadata.createReaderLease(
                cluster,
                configuration.providerScopeSha256(),
                desired);
    }

    private static void requireNewReadableRoot(BookKeeperLedgerRootRecord root) {
        if (root.lifecycle() != BookKeeperLedgerLifecycle.ACTIVE
                && root.lifecycle() != BookKeeperLedgerLifecycle.SEALING
                && root.lifecycle() != BookKeeperLedgerLifecycle.SEALED) {
            throw new NereusException(
                    ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND,
                    false,
                    "BookKeeper ledger does not admit a new reader pin in lifecycle " + root.lifecycle());
        }
    }

    private static void requirePinnedReadableRoot(
            BookKeeperLedgerRootRecord root,
            BookKeeperLedgerReaderLeaseRecord lease) {
        boolean readable = root.lifecycle() == BookKeeperLedgerLifecycle.ACTIVE
                || root.lifecycle() == BookKeeperLedgerLifecycle.SEALING
                || root.lifecycle() == BookKeeperLedgerLifecycle.SEALED
                || root.lifecycle() == BookKeeperLedgerLifecycle.MARKED;
        if (!readable
                || root.ledgerId() != lease.ledgerId()
                || !root.ledgerIdentitySha256().equals(lease.ledgerIdentitySha256())
                || root.lifecycleEpoch() < lease.rootLifecycleEpoch()) {
            throw invariant("BookKeeper ledger root is no longer readable under the acquired pin");
        }
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
