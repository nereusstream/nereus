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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** Fixed-slot whole-ledger reader pins; each foreground call owns one unique bounded lease. */
public final class BookKeeperReaderLeaseManager {
    public final class Lease implements AutoCloseable {
        private final SharedLease shared;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(SharedLease shared) {
            this.shared = Objects.requireNonNull(shared, "shared");
        }

        public BookKeeperLedgerReaderLeaseRecord value() {
            return current(shared).value();
        }

        public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> revalidate() {
            if (released.get()) {
                return CompletableFuture.failedFuture(invariant("BookKeeper reader lease was already released"));
            }
            BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> expected = current(shared);
            var leaseFuture = metadata.getReaderLease(
                    cluster,
                    configuration.providerScopeSha256(),
                    expected.value().ledgerId(),
                    expected.value().readerSlot());
            var rootFuture = metadata.getRoot(
                    cluster,
                    configuration.providerScopeSha256(),
                    expected.value().ledgerId());
            return CompletableFuture.allOf(leaseFuture, rootFuture).thenApply(ignored -> {
                BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> current = leaseFuture.join()
                        .orElseThrow(() -> invariant("BookKeeper reader lease disappeared during provider read"));
                if (!sameOwner(expected.value(), current.value())
                        || current.value().leaseEpoch() < expected.value().leaseEpoch()
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
            return releaseReference(shared);
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
    private final Map<Long, SharedLease> localLeases = new HashMap<>();

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
        return claimLocal(exact, deadline);
    }

    private CompletableFuture<Lease> claimLocal(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperOperationDeadline deadline) {
        SharedLease shared;
        CompletableFuture<Void> releaseInProgress;
        CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> claim = null;
        synchronized (this) {
            shared = localLeases.get(root.value().ledgerId());
            releaseInProgress = shared == null ? null : shared.releasing;
            if (releaseInProgress == null) {
                if (shared == null) {
                    shared = new SharedLease(root.value().ledgerId());
                    localLeases.put(root.value().ledgerId(), shared);
                    shared.serial = claimSlot(root, deadline, 0);
                } else {
                    shared.serial = recoverSerial(shared)
                            .thenCompose(value -> renewIfRequired(value, deadline));
                }
                shared.references++;
                SharedLease exactShared = shared;
                claim = shared.serial.thenApply(value -> remember(exactShared, value));
                shared.serial = claim;
            }
        }
        if (releaseInProgress != null) {
            return releaseInProgress.handle((ignored, failure) -> null)
                    .thenCompose(ignored -> claimLocal(root, deadline));
        }
        SharedLease exactShared = shared;
        return Objects.requireNonNull(claim, "BookKeeper local reader claim")
                .thenApply(ignored -> new Lease(exactShared))
                .whenComplete((ignored, failure) -> {
                    if (failure != null) releaseFailedClaim(exactShared);
                });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> claimSlot(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperOperationDeadline deadline,
            int attempt) {
        if (attempt >= configuration.maxReaderLeasesPerLedger()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED, true,
                    "BookKeeper ledger reader lease slots are exhausted"));
        }
        int slot = Math.floorMod(
                Objects.hash(processRunId, root.value().ledgerId()) + attempt,
                configuration.maxReaderLeasesPerLedger());
        return deadline.bound(metadata.getReaderLease(
                        cluster,
                        configuration.providerScopeSha256(),
                        root.value().ledgerId(),
                        slot))
                .thenCompose(optional -> {
                    if (optional.isEmpty()) {
                        return create(root, slot).handle((created, failure) -> {
                            if (failure == null) return CompletableFuture.completedFuture(created);
                            Throwable cause = unwrap(failure);
                            if (cause instanceof BookKeeperMetadataConditionFailedException) {
                                return claimSlot(root, deadline, attempt);
                            }
                            return CompletableFuture
                                    .<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>>failedFuture(cause);
                        }).thenCompose(java.util.function.Function.identity());
                    }
                    BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> current = optional.orElseThrow();
                    if (current.value().processRunId().equals(processRunId)) {
                        if (!sameRoot(root.value(), current.value())) {
                            return CompletableFuture.failedFuture(invariant(
                                    "BookKeeper reader slot owned by this process has incompatible identity"));
                        }
                        if (current.value().expiresAtMillis() <= clock.millis()) {
                            return deadline.bound(metadata.deleteReaderLease(
                                            cluster,
                                            configuration.providerScopeSha256(),
                                            current.value().ledgerId(),
                                            current.value().readerSlot(),
                                            current.metadataVersion()))
                                    .thenCompose(ignored -> claimSlot(root, deadline, attempt));
                        }
                        return renewIfRequired(current, deadline);
                    }
                    return claimSlot(root, deadline, attempt + 1);
                });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> create(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            int slot) {
        long now = clock.millis();
        BookKeeperLedgerReaderLeaseRecord desired = new BookKeeperLedgerReaderLeaseRecord(
                1,
                root.value().ledgerIdentitySha256(),
                root.value().ledgerId(),
                root.value().lifecycleEpoch(),
                slot,
                processRunId,
                1,
                now,
                add(now, configuration.readerLeaseTtl()),
                0);
        return metadata.createReaderLease(
                cluster,
                configuration.providerScopeSha256(),
                desired);
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> renewIfRequired(
            BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> current,
            BookKeeperOperationDeadline deadline) {
        if (!current.value().processRunId().equals(processRunId)) {
            return CompletableFuture.failedFuture(invariant("BookKeeper reader lease ownership changed"));
        }
        long requiredUntil = add(clock.millis(), configuration.operationTimeout());
        if (current.value().expiresAtMillis() > requiredUntil) {
            return CompletableFuture.completedFuture(current);
        }
        BookKeeperLedgerReaderLeaseRecord before = current.value();
        BookKeeperLedgerReaderLeaseRecord renewed = new BookKeeperLedgerReaderLeaseRecord(
                before.schemaVersion(),
                before.ledgerIdentitySha256(),
                before.ledgerId(),
                before.rootLifecycleEpoch(),
                before.readerSlot(),
                before.processRunId(),
                Math.addExact(before.leaseEpoch(), 1),
                before.acquiredAtMillis(),
                add(clock.millis(), configuration.readerLeaseTtl()),
                0);
        return deadline.bound(metadata.compareAndSetReaderLease(
                cluster,
                configuration.providerScopeSha256(),
                renewed,
                current.metadataVersion()));
    }

    private CompletableFuture<Void> releaseReference(SharedLease shared) {
        synchronized (this) {
            if (shared.references <= 0) {
                return CompletableFuture.failedFuture(invariant("BookKeeper local reader reference underflow"));
            }
            shared.references--;
            if (shared.references > 0) return CompletableFuture.completedFuture(null);
            if (shared.releasing != null) return shared.releasing;
            shared.releasing = recoverSerial(shared)
                    .thenCompose(value -> deleteOwned(value, 0))
                    .whenComplete((ignored, failure) -> removeLocal(shared));
            return shared.releasing;
        }
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> recoverSerial(
            SharedLease shared) {
        return shared.serial.handle((value, failure) -> {
                    if (failure == null) return CompletableFuture.completedFuture(value);
                    BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> remembered = currentOrNull(shared);
                    return remembered == null
                            ? CompletableFuture
                                    .<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>>failedFuture(
                                            unwrap(failure))
                            : CompletableFuture.completedFuture(remembered);
                })
                .thenCompose(java.util.function.Function.identity());
    }

    private CompletableFuture<Void> deleteOwned(
            BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> expected,
            int attempts) {
        return metadata.deleteReaderLease(
                        cluster,
                        configuration.providerScopeSha256(),
                        expected.value().ledgerId(),
                        expected.value().readerSlot(),
                        expected.metadataVersion())
                .handle((ignored, failure) -> metadata.getReaderLease(
                        cluster,
                        configuration.providerScopeSha256(),
                        expected.value().ledgerId(),
                        expected.value().readerSlot()))
                .thenCompose(java.util.function.Function.identity())
                .thenCompose(current -> {
                    if (current.isEmpty()) return CompletableFuture.completedFuture(null);
                    BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> present = current.orElseThrow();
                    if (!sameOwner(expected.value(), present.value())) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (present.metadataVersion() == expected.metadataVersion() && attempts < 1) {
                        return deleteOwned(present, attempts + 1);
                    }
                    return CompletableFuture.failedFuture(invariant(
                            "BookKeeper reader lease release did not converge"));
                });
    }

    private void releaseFailedClaim(SharedLease shared) {
        synchronized (this) {
            if (shared.references > 0) shared.references--;
            if (shared.references == 0) removeLocal(shared);
        }
    }

    private synchronized BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> remember(
            SharedLease shared,
            BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> value) {
        shared.current = value;
        return value;
    }

    private synchronized BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> current(SharedLease shared) {
        return Objects.requireNonNull(shared.current, "BookKeeper shared reader lease is not ready");
    }

    private synchronized BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> currentOrNull(
            SharedLease shared) {
        return shared.current;
    }

    private synchronized void removeLocal(SharedLease shared) {
        localLeases.remove(shared.ledgerId, shared);
    }

    private boolean sameOwner(
            BookKeeperLedgerReaderLeaseRecord expected,
            BookKeeperLedgerReaderLeaseRecord current) {
        return expected.ledgerId() == current.ledgerId()
                && expected.readerSlot() == current.readerSlot()
                && expected.ledgerIdentitySha256().equals(current.ledgerIdentitySha256())
                && expected.processRunId().equals(processRunId)
                && current.processRunId().equals(processRunId)
                && expected.rootLifecycleEpoch() == current.rootLifecycleEpoch()
                && expected.acquiredAtMillis() == current.acquiredAtMillis();
    }

    private static boolean sameRoot(
            BookKeeperLedgerRootRecord root,
            BookKeeperLedgerReaderLeaseRecord lease) {
        return root.ledgerId() == lease.ledgerId()
                && root.ledgerIdentitySha256().equals(lease.ledgerIdentitySha256())
                && root.lifecycleEpoch() >= lease.rootLifecycleEpoch();
    }

    private static long add(long millis, Duration duration) {
        try {
            return Math.addExact(millis, duration.toMillis());
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static final class SharedLease {
        private final long ledgerId;
        private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> serial;
        private BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> current;
        private int references;
        private CompletableFuture<Void> releasing;

        private SharedLease(long ledgerId) {
            this.ledgerId = ledgerId;
        }
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
