/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ReaderLeaseScanPage;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedReaderLease;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Durable create/revalidate/release handshake for physical-object reads. */
public final class DefaultObjectReadPinManager implements ObjectReadPinManager {
    private static final int SCAN_PAGE_SIZE = 1_000;

    private final String cluster;
    private final String processRunId;
    private final PhysicalObjectMetadataStore store;
    private final long leaseDurationMillis;
    private final long maximumClockSkewMillis;
    private final long orphanGraceMillis;
    private final Clock clock;
    private final Supplier<String> leaseIdSupplier;
    private final ConcurrentHashMap<com.nereusstream.api.ObjectKeyHash, LocalLeaseState> local =
            new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public DefaultObjectReadPinManager(
            String cluster,
            String processRunId,
            PhysicalObjectMetadataStore store,
            Duration leaseDuration,
            Duration maximumClockSkew,
            Duration orphanGrace,
            Clock clock) {
        this(
                cluster,
                processRunId,
                store,
                leaseDuration,
                maximumClockSkew,
                orphanGrace,
                clock,
                DefaultObjectReadPinManager::randomId);
    }

    DefaultObjectReadPinManager(
            String cluster,
            String processRunId,
            PhysicalObjectMetadataStore store,
            Duration leaseDuration,
            Duration maximumClockSkew,
            Duration orphanGrace,
            Clock clock,
            Supplier<String> leaseIdSupplier) {
        this.cluster = requireText(cluster, "cluster");
        this.processRunId = new PublicationId(requireText(processRunId, "processRunId")).value();
        this.store = Objects.requireNonNull(store, "store");
        this.leaseDurationMillis = requirePositiveMillis(leaseDuration, "leaseDuration");
        this.maximumClockSkewMillis = requireNonNegativeMillis(maximumClockSkew, "maximumClockSkew");
        this.orphanGraceMillis = requirePositiveMillis(orphanGrace, "orphanGrace");
        if (leaseDurationMillis <= maximumClockSkewMillis) {
            throw new IllegalArgumentException("leaseDuration must exceed maximumClockSkew");
        }
        this.clock = Objects.requireNonNull(clock, "clock");
        this.leaseIdSupplier = Objects.requireNonNull(leaseIdSupplier, "leaseIdSupplier");
    }

    @Override
    public CompletableFuture<ObjectReadLease> acquire(
            PhysicalObjectIdentity object,
            long maximumReadDeadlineMillis,
            SelectionRevalidator selectionRevalidator) {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(selectionRevalidator, "selectionRevalidator");
        if (closed.get()) {
            return failed(new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "object read pin manager is closed"));
        }
        LocalLeaseState state = local.computeIfAbsent(object.objectKeyHash(), ignored -> new LocalLeaseState());
        synchronized (state) {
            CompletableFuture<ObjectReadLease> operation = state.tail
                    .handle((ignored, previousFailure) -> null)
                    .thenCompose(ignored -> acquireSerialized(
                            state, object, maximumReadDeadlineMillis, selectionRevalidator));
            state.tail = operation.handle((ignored, failure) -> null);
            return operation;
        }
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private CompletableFuture<ObjectReadLease> acquireSerialized(
            LocalLeaseState state,
            PhysicalObjectIdentity object,
            long maximumReadDeadlineMillis,
            SelectionRevalidator selectionRevalidator) {
        if (closed.get()) {
            return failed(new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "object read pin manager is closed"));
        }
        long now = clock.millis();
        LeaseWindow window;
        try {
            window = window(now, maximumReadDeadlineMillis);
        } catch (RuntimeException failure) {
            return failed(failure);
        }
        if (state.refCount == 0 || state.lease == null) {
            return acquireFirst(state, object, maximumReadDeadlineMillis, selectionRevalidator, now, window);
        }
        if (!object.equals(state.object)) {
            return failed(invariant("one object hash resolved to a different immutable identity"));
        }
        return findProcessLease(object.objectKeyHash(), Optional.empty()).thenCompose(currentOptional -> {
            VersionedReaderLease current = currentOptional.orElseThrow(
                    () -> condition("durable reader lease disappeared during local reuse"));
            if (!sameLeaseIdentity(current, state.lease)
                    || current.value().expiresAtMillis() <= Math.addExact(now, maximumClockSkewMillis)) {
                return failed(condition("durable reader lease changed or expired during local reuse"));
            }
            return getExactActiveRoot(object).thenCompose(root -> {
                if (!PhysicalObjectRecords.sameActiveRoot(state.root, root, object)) {
                    return failed(condition("physical root changed before local reader reuse"));
                }
                CompletableFuture<VersionedReaderLease> durable;
                boolean extendExpiry = maximumReadDeadlineMillis
                        > current.value().expiresAtMillis() - maximumClockSkewMillis;
                boolean extendDeadline = maximumReadDeadlineMillis
                        > current.value().maximumReadDeadlineMillis();
                if (!extendExpiry && !extendDeadline) {
                    durable = CompletableFuture.completedFuture(current);
                } else {
                    ObjectReaderLeaseRecord renewed = new ObjectReaderLeaseRecord(
                            1,
                            object.objectKeyHash().value(),
                            processRunId,
                            current.value().leaseId(),
                            root.value().lifecycleEpoch(),
                            current.value().acquiredAtMillis(),
                            extendExpiry ? window.expiresAtMillis() : current.value().expiresAtMillis(),
                            Math.max(current.value().maximumReadDeadlineMillis(), maximumReadDeadlineMillis),
                            Math.addExact(current.value().renewalSequence(), 1),
                            0);
                    durable = store.compareAndSetReaderLease(
                            cluster, renewed, current.metadataVersion());
                }
                return durable.thenCompose(updated -> postCheck(
                                object, root, updated, selectionRevalidator)
                        .thenApply(ignored -> {
                            state.root = root;
                            state.lease = updated;
                            state.refCount++;
                            return new LeaseHandle(
                                    this, state, object, updated.value().leaseId(), maximumReadDeadlineMillis);
                        }));
            });
        });
    }

    private CompletableFuture<ObjectReadLease> acquireFirst(
            LocalLeaseState state,
            PhysicalObjectIdentity object,
            long maximumReadDeadlineMillis,
            SelectionRevalidator selectionRevalidator,
            long now,
            LeaseWindow window) {
        return ensureActiveRoot(object, now).thenCompose(root ->
                findProcessLease(object.objectKeyHash(), Optional.empty()).thenCompose(existing -> {
                    String leaseId = new PublicationId(leaseIdSupplier.get()).value();
                    long renewalSequence = existing
                            .map(value -> Math.addExact(value.value().renewalSequence(), 1))
                            .orElse(0L);
                    ObjectReaderLeaseRecord record = new ObjectReaderLeaseRecord(
                            1,
                            object.objectKeyHash().value(),
                            processRunId,
                            leaseId,
                            root.value().lifecycleEpoch(),
                            now,
                            window.expiresAtMillis(),
                            maximumReadDeadlineMillis,
                            renewalSequence,
                            0);
                    CompletableFuture<VersionedReaderLease> write = existing.isPresent()
                            ? store.compareAndSetReaderLease(
                                    cluster, record, existing.orElseThrow().metadataVersion())
                            : store.createOrCompareReaderLease(cluster, record);
                    return write.thenCompose(lease -> completeFirstAcquisition(
                            state,
                            object,
                            root,
                            lease,
                            leaseId,
                            maximumReadDeadlineMillis,
                            selectionRevalidator));
                }));
    }

    private CompletableFuture<ObjectReadLease> completeFirstAcquisition(
            LocalLeaseState state,
            PhysicalObjectIdentity object,
            VersionedPhysicalObjectRoot root,
            VersionedReaderLease lease,
            String leaseId,
            long maximumReadDeadlineMillis,
            SelectionRevalidator selectionRevalidator) {
        CompletableFuture<ObjectReadLease> result = new CompletableFuture<>();
        postCheck(object, root, lease, selectionRevalidator).whenComplete((ignored, failure) -> {
            if (failure == null) {
                state.object = object;
                state.root = root;
                state.lease = lease;
                state.refCount = 1;
                result.complete(new LeaseHandle(
                        this, state, object, leaseId, maximumReadDeadlineMillis));
                return;
            }
            Throwable exact = unwrap(failure);
            cleanupLease(lease).whenComplete((cleanup, cleanupFailure) -> {
                if (cleanupFailure != null) {
                    exact.addSuppressed(unwrap(cleanupFailure));
                }
                result.completeExceptionally(exact);
            });
        });
        return result;
    }

    private CompletableFuture<Void> postCheck(
            PhysicalObjectIdentity object,
            VersionedPhysicalObjectRoot expectedRoot,
            VersionedReaderLease lease,
            SelectionRevalidator selectionRevalidator) {
        return getExactActiveRoot(object).thenCompose(currentRoot -> {
            if (!PhysicalObjectRecords.sameActiveRoot(expectedRoot, currentRoot, object)
                    || lease.value().rootLifecycleEpoch() != currentRoot.value().lifecycleEpoch()) {
                return failed(condition("physical root changed after durable reader lease write"));
            }
            try {
                return Objects.requireNonNull(
                        selectionRevalidator.revalidate(), "selection revalidator result");
            } catch (Throwable failure) {
                return failed(failure);
            }
        });
    }

    private CompletableFuture<VersionedPhysicalObjectRoot> ensureActiveRoot(
            PhysicalObjectIdentity object, long now) {
        return store.getRoot(cluster, object.objectKeyHash()).thenCompose(optional -> {
            if (optional.isPresent()) {
                return verifyActiveRoot(object, optional.orElseThrow());
            }
            long orphanNotBefore = Math.addExact(now, orphanGraceMillis);
            return store.createRoot(
                            cluster,
                            PhysicalObjectRecords.active(object, now, orphanNotBefore))
                    .thenCompose(root -> verifyActiveRoot(object, root));
        });
    }

    private CompletableFuture<VersionedPhysicalObjectRoot> getExactActiveRoot(
            PhysicalObjectIdentity object) {
        return store.getRoot(cluster, object.objectKeyHash()).thenCompose(optional ->
                optional.<CompletableFuture<VersionedPhysicalObjectRoot>>map(root -> verifyActiveRoot(object, root))
                        .orElseGet(() -> failed(condition("physical object root is absent"))));
    }

    private CompletableFuture<VersionedPhysicalObjectRoot> verifyActiveRoot(
            PhysicalObjectIdentity object, VersionedPhysicalObjectRoot root) {
        if (root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                || !PhysicalObjectRecords.exactIdentity(object, root.value())) {
            return failed(condition("physical object root is not the exact ACTIVE identity"));
        }
        return CompletableFuture.completedFuture(root);
    }

    private CompletableFuture<Optional<VersionedReaderLease>> findProcessLease(
            com.nereusstream.api.ObjectKeyHash object,
            Optional<F4ScanToken> continuation) {
        return store.scanReaderLeases(cluster, object, continuation, SCAN_PAGE_SIZE)
                .thenCompose(page -> {
                    Optional<VersionedReaderLease> found = page.values().stream()
                            .filter(value -> value.value().processRunId().equals(processRunId))
                            .findFirst();
                    if (found.isPresent() || page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(found);
                    }
                    return findProcessLease(object, page.continuation());
                });
    }

    private CompletableFuture<Void> release(LocalLeaseState state, String leaseId) {
        synchronized (state) {
            CompletableFuture<Void> operation = state.tail
                    .handle((ignored, previousFailure) -> null)
                    .thenCompose(ignored -> releaseSerialized(state, leaseId));
            state.tail = operation.handle((ignored, failure) -> null);
            return operation;
        }
    }

    private CompletableFuture<Void> releaseSerialized(LocalLeaseState state, String leaseId) {
        if (state.refCount <= 0 || state.lease == null || !state.lease.value().leaseId().equals(leaseId)) {
            return failed(invariant("reader lease release does not match local durable lease"));
        }
        state.refCount--;
        if (state.refCount > 0) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedReaderLease releasing = state.lease;
        return cleanupLease(releasing).whenComplete((ignored, failure) -> {
            if (failure == null) {
                state.object = null;
                state.root = null;
                state.lease = null;
            }
        });
    }

    private CompletableFuture<Void> cleanupLease(VersionedReaderLease lease) {
        return store.deleteReaderLease(
                        cluster,
                        new com.nereusstream.api.ObjectKeyHash(lease.value().objectKeyHash()),
                        processRunId,
                        lease.metadataVersion())
                .handle((ignored, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    return findProcessLease(
                                    new com.nereusstream.api.ObjectKeyHash(lease.value().objectKeyHash()),
                                    Optional.empty())
                            .thenCompose(current -> {
                                if (current.isEmpty()) {
                                    return CompletableFuture.completedFuture(null);
                                }
                                VersionedReaderLease exact = current.orElseThrow();
                                if (!sameLeaseIdentity(exact, lease)) {
                                    return failed(invariant(
                                            "reader lease delete response loss found a different lease"));
                                }
                                return store.deleteReaderLease(
                                        cluster,
                                        new com.nereusstream.api.ObjectKeyHash(lease.value().objectKeyHash()),
                                        processRunId,
                                        exact.metadataVersion());
                            });
                }).thenCompose(value -> value);
    }

    private LeaseWindow window(long now, long maximumReadDeadlineMillis) {
        if (maximumReadDeadlineMillis < now) {
            throw new IllegalArgumentException("maximumReadDeadlineMillis cannot precede acquisition");
        }
        long expiresAt = Math.addExact(now, leaseDurationMillis);
        long safeDeadline = Math.subtractExact(expiresAt, maximumClockSkewMillis);
        if (maximumReadDeadlineMillis > safeDeadline) {
            throw new IllegalArgumentException("read deadline exceeds the skew-safe durable lease window");
        }
        return new LeaseWindow(expiresAt);
    }

    private static boolean sameLeaseIdentity(
            VersionedReaderLease actual, VersionedReaderLease expected) {
        return actual.metadataVersion() == expected.metadataVersion()
                && actual.value().equals(expected.value())
                && actual.durableValueSha256().equals(expected.durableValueSha256());
    }

    private static String randomId() {
        byte[] random = new byte[16];
        new SecureRandom().nextBytes(random);
        return DeterministicIds.randomRunIdHash(random);
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    private static long requirePositiveMillis(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value.toMillis();
    }

    private static long requireNonNegativeMillis(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " cannot be negative");
        }
        return value.toMillis();
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static <T> CompletableFuture<T> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }

    private record LeaseWindow(long expiresAtMillis) {
    }

    private static final class LocalLeaseState {
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
        private PhysicalObjectIdentity object;
        private VersionedPhysicalObjectRoot root;
        private VersionedReaderLease lease;
        private int refCount;
    }

    private static final class LeaseHandle implements ObjectReadLease {
        private final DefaultObjectReadPinManager owner;
        private final LocalLeaseState state;
        private final PhysicalObjectIdentity object;
        private final String leaseId;
        private final long maximumReadDeadlineMillis;
        private CompletableFuture<Void> release;

        private LeaseHandle(
                DefaultObjectReadPinManager owner,
                LocalLeaseState state,
                PhysicalObjectIdentity object,
                String leaseId,
                long maximumReadDeadlineMillis) {
            this.owner = owner;
            this.state = state;
            this.object = object;
            this.leaseId = leaseId;
            this.maximumReadDeadlineMillis = maximumReadDeadlineMillis;
        }

        @Override
        public PhysicalObjectIdentity object() {
            return object;
        }

        @Override
        public String leaseId() {
            return leaseId;
        }

        @Override
        public long maximumReadDeadlineMillis() {
            return maximumReadDeadlineMillis;
        }

        @Override
        public synchronized CompletableFuture<Void> release() {
            if (release == null) {
                release = owner.release(state, leaseId);
            }
            return release;
        }

        @Override
        public synchronized boolean isReleased() {
            return release != null;
        }
    }
}
