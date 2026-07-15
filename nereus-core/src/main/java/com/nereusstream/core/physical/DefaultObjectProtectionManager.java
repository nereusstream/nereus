/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Default cross-key handshake with bounded process-local serialization and no transfer gap. */
public final class DefaultObjectProtectionManager implements ObjectProtectionManager {
    private static final int SCAN_PAGE_SIZE = 1_000;
    private static final int SERIALIZATION_STRIPES = 256;

    private final String cluster;
    private final PhysicalObjectMetadataStore store;
    private final long pendingProtectionDurationMillis;
    private final long maximumClockSkewMillis;
    private final long orphanGraceMillis;
    private final Clock clock;
    private final SerialState[] serialStates = new SerialState[SERIALIZATION_STRIPES];
    private final AtomicBoolean closed = new AtomicBoolean();

    public DefaultObjectProtectionManager(
            String cluster,
            PhysicalObjectMetadataStore store,
            Duration pendingProtectionDuration,
            Duration maximumClockSkew,
            Duration orphanGrace,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.store = Objects.requireNonNull(store, "store");
        this.pendingProtectionDurationMillis = requirePositiveMillis(
                pendingProtectionDuration, "pendingProtectionDuration");
        this.maximumClockSkewMillis = requireNonNegativeMillis(
                maximumClockSkew, "maximumClockSkew");
        this.orphanGraceMillis = requirePositiveMillis(orphanGrace, "orphanGrace");
        if (pendingProtectionDurationMillis <= maximumClockSkewMillis) {
            throw new IllegalArgumentException(
                    "pendingProtectionDuration must exceed maximumClockSkew");
        }
        this.clock = Objects.requireNonNull(clock, "clock");
        for (int index = 0; index < serialStates.length; index++) {
            serialStates[index] = new SerialState();
        }
    }

    @Override
    public CompletableFuture<ObjectProtection> acquire(
            ObjectProtectionRequest request,
            OwnerRevalidator ownerRevalidator) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ownerRevalidator, "ownerRevalidator");
        if (closed.get()) {
            return failed(closedFailure());
        }
        return serialized(request.identity(), () -> acquireSerialized(request, ownerRevalidator));
    }

    @Override
    public CompletableFuture<ObjectProtection> acquireOrTransfer(
            ObjectProtectionRequest request,
            OwnerRevalidator ownerRevalidator) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ownerRevalidator, "ownerRevalidator");
        if (closed.get()) {
            return failed(closedFailure());
        }
        return serialized(request.identity(), () -> acquireOrTransferSerialized(
                request, ownerRevalidator));
    }

    @Override
    public CompletableFuture<ObjectProtection> revalidate(
            ObjectProtection protection,
            OwnerRevalidator ownerRevalidator) {
        Objects.requireNonNull(protection, "protection");
        Objects.requireNonNull(ownerRevalidator, "ownerRevalidator");
        if (closed.get()) {
            return failed(closedFailure());
        }
        return serialized(protection.identity(), () -> revalidateSerialized(protection, ownerRevalidator));
    }

    @Override
    public CompletableFuture<ObjectProtection> transfer(
            ObjectProtection protection,
            ObjectProtectionOwner newOwner,
            OwnerRevalidator newOwnerRevalidator) {
        Objects.requireNonNull(protection, "protection");
        Objects.requireNonNull(newOwner, "newOwner");
        Objects.requireNonNull(newOwnerRevalidator, "newOwnerRevalidator");
        if (closed.get()) {
            return failed(closedFailure());
        }
        return serialized(protection.identity(), () -> transferSerialized(
                protection, newOwner, newOwnerRevalidator));
    }

    @Override
    public CompletableFuture<Void> release(
            ObjectProtection protection,
            RemovalAuthorizer removalAuthorizer) {
        Objects.requireNonNull(protection, "protection");
        Objects.requireNonNull(removalAuthorizer, "removalAuthorizer");
        return serialized(protection.identity(), () -> releaseSerialized(
                protection, removalAuthorizer));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private CompletableFuture<ObjectProtection> acquireSerialized(
            ObjectProtectionRequest request,
            OwnerRevalidator ownerRevalidator) {
        if (closed.get()) {
            return failed(closedFailure());
        }
        long now = clock.millis();
        try {
            validateRequestedExpiry(request.type(), request.expiresAtMillis(), now);
        } catch (RuntimeException failure) {
            return failed(failure);
        }
        return ensureActiveRoot(request.object(), now).thenCompose(root ->
                findProtection(request.identity(), Optional.empty()).thenCompose(existing ->
                        createOrReuse(request, root, existing, now).thenCompose(acquisition -> {
                            ObjectProtection protection = fromVersioned(request.object(), acquisition.value());
                            CompletableFuture<ObjectProtection> checked = postCheck(
                                    protection, root, ownerRevalidator);
                            if (!acquisition.createdByThisCall()) {
                                return checked;
                            }
                            CompletableFuture<ObjectProtection> result = new CompletableFuture<>();
                            checked.whenComplete((value, failure) -> {
                                if (failure == null) {
                                    result.complete(value);
                                    return;
                                }
                                Throwable exact = unwrap(failure);
                                deleteExact(acquisition.value()).whenComplete((ignored, cleanupFailure) -> {
                                    if (cleanupFailure != null) {
                                        exact.addSuppressed(unwrap(cleanupFailure));
                                    }
                                    result.completeExceptionally(exact);
                                });
                            });
                            return result;
                        })));
    }

    private CompletableFuture<ObjectProtection> acquireOrTransferSerialized(
            ObjectProtectionRequest request,
            OwnerRevalidator ownerRevalidator) {
        if (closed.get()) {
            return failed(closedFailure());
        }
        long now = clock.millis();
        try {
            validateRequestedExpiry(request.type(), request.expiresAtMillis(), now);
        } catch (RuntimeException failure) {
            return failed(failure);
        }
        return ensureActiveRoot(request.object(), now).thenCompose(root ->
                findProtection(request.identity(), Optional.empty()).thenCompose(existing ->
                        existing.isPresent()
                                ? reconcileExisting(
                                        request, root, existing.orElseThrow(), ownerRevalidator)
                                : createForReconciliation(
                                        request, root, ownerRevalidator, now)));
    }

    private CompletableFuture<ObjectProtection> createForReconciliation(
            ObjectProtectionRequest request,
            VersionedPhysicalObjectRoot root,
            OwnerRevalidator ownerRevalidator,
            long now) {
        ObjectProtectionRecord record = requestedRecord(request, root, now);
        return store.createProtection(cluster, record).handle((created, failure) -> {
            if (failure == null) {
                ObjectProtection protection = fromVersioned(request.object(), created);
                CompletableFuture<ObjectProtection> checked = postCheck(
                        protection, root, ownerRevalidator);
                CompletableFuture<ObjectProtection> result = new CompletableFuture<>();
                checked.whenComplete((value, checkFailure) -> {
                    if (checkFailure == null) {
                        result.complete(value);
                        return;
                    }
                    Throwable exact = unwrap(checkFailure);
                    deleteExact(created).whenComplete((ignored, cleanupFailure) -> {
                        if (cleanupFailure != null) {
                            exact.addSuppressed(unwrap(cleanupFailure));
                        }
                        result.completeExceptionally(exact);
                    });
                });
                return result;
            }
            Throwable original = unwrap(failure);
            return findProtection(request.identity(), Optional.empty()).thenCompose(recovered -> {
                if (recovered.isEmpty()) {
                    return failed(original);
                }
                return reconcileExisting(
                                request,
                                root,
                                recovered.orElseThrow(),
                                ownerRevalidator)
                        .exceptionallyCompose(reconciliationFailure -> {
                            Throwable exact = unwrap(reconciliationFailure);
                            exact.addSuppressed(original);
                            return failed(exact);
                        });
            });
        }).thenCompose(value -> value);
    }

    private CompletableFuture<ObjectProtection> reconcileExisting(
            ObjectProtectionRequest request,
            VersionedPhysicalObjectRoot root,
            VersionedObjectProtection current,
            OwnerRevalidator ownerRevalidator) {
        try {
            requireReconciliationIdentity(request, root, current);
        } catch (RuntimeException failure) {
            return failed(failure);
        }
        ObjectProtection existing = fromVersioned(request.object(), current);
        if (existing.owner().equals(request.owner())) {
            return postCheck(existing, root, ownerRevalidator);
        }
        return invokeOwnerRevalidator(ownerRevalidator, request.owner()).thenCompose(ignored -> {
            ObjectProtectionRecord desired = transferRecord(
                    current.value(), request.owner(), root.value().lifecycleEpoch());
            return compareAndSetOrRecover(existing, current, desired)
                    .thenCompose(updated -> postCheck(
                            fromVersioned(request.object(), updated), root, ownerRevalidator));
        });
    }

    private CompletableFuture<ObjectProtection> revalidateSerialized(
            ObjectProtection expected,
            OwnerRevalidator ownerRevalidator) {
        if (closed.get()) {
            return failed(closedFailure());
        }
        return findProtection(expected.identity(), Optional.empty()).thenCompose(optional -> {
            VersionedObjectProtection current = optional.orElseThrow(
                    () -> condition("object protection is absent during revalidation"));
            requireSameHandle(expected, current);
            return getExactActiveRoot(expected.object()).thenCompose(root ->
                    postCheck(fromVersioned(expected.object(), current), root, ownerRevalidator));
        });
    }

    private CompletableFuture<ObjectProtection> transferSerialized(
            ObjectProtection expected,
            ObjectProtectionOwner newOwner,
            OwnerRevalidator newOwnerRevalidator) {
        if (closed.get()) {
            return failed(closedFailure());
        }
        return invokeOwnerRevalidator(newOwnerRevalidator, newOwner).thenCompose(ignored ->
                findProtection(expected.identity(), Optional.empty()).thenCompose(optional -> {
                    VersionedObjectProtection current = optional.orElseThrow(
                            () -> condition("object protection is absent during owner transfer"));
                    requireRecordIdentity(expected.identity(), current.value());
                    if (!matchesHandle(expected, current)
                            && !matchesTransferredOutcome(expected, newOwner, current.value())) {
                        return failed(condition(
                                "object protection changed before owner transfer"));
                    }
                    return getExactActiveRoot(expected.object()).thenCompose(root -> {
                        ObjectProtectionRecord desired = transferRecord(
                                current.value(), newOwner, root.value().lifecycleEpoch());
                        CompletableFuture<VersionedObjectProtection> write;
                        if (sameRecordIgnoringMetadataVersion(current.value(), desired)) {
                            write = CompletableFuture.completedFuture(current);
                        } else {
                            write = compareAndSetOrRecover(expected, current, desired);
                        }
                        return write.thenCompose(updated -> postCheck(
                                fromVersioned(expected.object(), updated), root, newOwnerRevalidator));
                    });
                }));
    }

    private CompletableFuture<Void> releaseSerialized(
            ObjectProtection expected,
            RemovalAuthorizer removalAuthorizer) {
        return findProtection(expected.identity(), Optional.empty()).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return invokeRemovalAuthorizer(removalAuthorizer, expected);
            }
            VersionedObjectProtection current = optional.orElseThrow();
            requireSameHandle(expected, current);
            ObjectProtection exact = fromVersioned(expected.object(), current);
            return invokeRemovalAuthorizer(removalAuthorizer, exact).thenCompose(ignored ->
                    findProtection(expected.identity(), Optional.empty()).thenCompose(afterAuthorization -> {
                        if (afterAuthorization.isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        VersionedObjectProtection stillCurrent = afterAuthorization.orElseThrow();
                        if (!sameVersioned(current, stillCurrent)) {
                            return failed(condition(
                                    "object protection changed while removal was authorized"));
                        }
                        return deleteExact(stillCurrent);
                    }));
        });
    }

    private CompletableFuture<Acquisition> createOrReuse(
            ObjectProtectionRequest request,
            VersionedPhysicalObjectRoot root,
            Optional<VersionedObjectProtection> existing,
            long now) {
        if (existing.isPresent()) {
            VersionedObjectProtection current = existing.orElseThrow();
            requireReusable(request, root, current);
            return CompletableFuture.completedFuture(new Acquisition(current, false));
        }
        ObjectProtectionRecord record = requestedRecord(request, root, now);
        return store.createProtection(cluster, record).handle((created, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(new Acquisition(created, true));
            }
            Throwable exact = unwrap(failure);
            return findProtection(request.identity(), Optional.empty()).thenCompose(recovered -> {
                if (recovered.isEmpty()) {
                    return failed(exact);
                }
                VersionedObjectProtection current = recovered.orElseThrow();
                try {
                    requireReusable(request, root, current);
                    // A lost create response cannot prove that this process alone owns cleanup.
                    return CompletableFuture.completedFuture(new Acquisition(current, false));
                } catch (RuntimeException mismatch) {
                    exact.addSuppressed(mismatch);
                    return failed(exact);
                }
            });
        }).thenCompose(value -> value);
    }

    private CompletableFuture<VersionedObjectProtection> compareAndSetOrRecover(
            ObjectProtection expected,
            VersionedObjectProtection current,
            ObjectProtectionRecord desired) {
        return store.compareAndSetProtection(cluster, desired, current.metadataVersion())
                .handle((updated, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(updated);
                    }
                    Throwable exact = unwrap(failure);
                    return findProtection(expected.identity(), Optional.empty()).thenCompose(recovered -> {
                        if (recovered.isPresent()
                                && sameRecordIgnoringMetadataVersion(
                                        recovered.orElseThrow().value(), desired)) {
                            return CompletableFuture.completedFuture(recovered.orElseThrow());
                        }
                        return failed(exact);
                    });
                }).thenCompose(value -> value);
    }

    private CompletableFuture<ObjectProtection> postCheck(
            ObjectProtection protection,
            VersionedPhysicalObjectRoot expectedRoot,
            OwnerRevalidator ownerRevalidator) {
        return getExactActiveRoot(protection.object()).thenCompose(currentRoot -> {
            if (!PhysicalObjectRecords.sameActiveRoot(
                            expectedRoot, currentRoot, protection.object())
                    || protection.rootLifecycleEpoch()
                            != currentRoot.value().lifecycleEpoch()) {
                return failed(condition(
                        "physical root changed during object protection handshake"));
            }
            try {
                validateDurableExpiry(
                        protection.identity().type(),
                        protection.createdAtMillis(),
                        protection.expiresAtMillis(),
                        clock.millis());
            } catch (RuntimeException failure) {
                return failed(failure);
            }
            return invokeOwnerRevalidator(ownerRevalidator, protection.owner())
                    .thenApply(ignored -> protection);
        });
    }

    private CompletableFuture<VersionedPhysicalObjectRoot> ensureActiveRoot(
            PhysicalObjectIdentity object,
            long now) {
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
                optional.<CompletableFuture<VersionedPhysicalObjectRoot>>map(
                                root -> verifyActiveRoot(object, root))
                        .orElseGet(() -> failed(condition("physical object root is absent"))));
    }

    private CompletableFuture<VersionedPhysicalObjectRoot> verifyActiveRoot(
            PhysicalObjectIdentity object,
            VersionedPhysicalObjectRoot root) {
        if (root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                || !PhysicalObjectRecords.exactIdentity(object, root.value())) {
            return failed(condition("physical object root is not the exact ACTIVE identity"));
        }
        return CompletableFuture.completedFuture(root);
    }

    private CompletableFuture<Optional<VersionedObjectProtection>> findProtection(
            ObjectProtectionIdentity identity,
            Optional<F4ScanToken> continuation) {
        return store.scanProtections(
                        cluster, identity.object(), continuation, SCAN_PAGE_SIZE)
                .thenCompose(page -> findInPage(identity, page));
    }

    private CompletableFuture<Optional<VersionedObjectProtection>> findInPage(
            ObjectProtectionIdentity identity,
            ObjectProtectionScanPage page) {
        Optional<VersionedObjectProtection> found = page.values().stream()
                .filter(value -> recordMatchesIdentity(identity, value.value()))
                .findFirst();
        if (found.isPresent() || page.continuation().isEmpty()) {
            return CompletableFuture.completedFuture(found);
        }
        return findProtection(identity, page.continuation());
    }

    private CompletableFuture<Void> deleteExact(VersionedObjectProtection expected) {
        ObjectProtectionIdentity identity = identity(expected.value());
        return store.deleteProtection(cluster, identity, expected.metadataVersion())
                .handle((ignored, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    return findProtection(identity, Optional.empty()).thenCompose(current -> {
                        if (current.isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        VersionedObjectProtection exact = current.orElseThrow();
                        if (!sameVersioned(expected, exact)) {
                            return failed(invariant(
                                    "protection delete response loss found a different value"));
                        }
                        return store.deleteProtection(
                                cluster, identity, exact.metadataVersion());
                    });
                }).thenCompose(value -> value);
    }

    private void requireReusable(
            ObjectProtectionRequest request,
            VersionedPhysicalObjectRoot root,
            VersionedObjectProtection current) {
        requireRecordIdentity(request.identity(), current.value());
        ObjectProtectionRecord value = current.value();
        if (!owner(value).equals(request.owner())
                || value.expiresAtMillis() != request.expiresAtMillis()
                || value.rootLifecycleEpoch() != root.value().lifecycleEpoch()) {
            throw condition("existing protection requires explicit owner/root reconciliation");
        }
    }

    private void requireReconciliationIdentity(
            ObjectProtectionRequest request,
            VersionedPhysicalObjectRoot root,
            VersionedObjectProtection current) {
        requireRecordIdentity(request.identity(), current.value());
        ObjectProtectionRecord value = current.value();
        ObjectProtectionOwner currentOwner = owner(value);
        if (!currentOwner.ownerKey().equals(request.owner().ownerKey())) {
            throw condition("existing protection belongs to a different logical owner");
        }
        if (value.expiresAtMillis() != request.expiresAtMillis()
                || value.rootLifecycleEpoch() != root.value().lifecycleEpoch()) {
            throw condition("existing protection expiry/root cannot be reconciled");
        }
        if (request.owner().metadataVersion() < currentOwner.metadataVersion()) {
            throw condition("object protection owner reconciliation cannot roll back");
        }
        if (request.owner().metadataVersion() == currentOwner.metadataVersion()
                && !request.owner().equals(currentOwner)) {
            throw invariant("one logical owner version has conflicting durable identities");
        }
    }

    private void validateRequestedExpiry(
            com.nereusstream.metadata.oxia.records.ObjectProtectionType type,
            long expiresAtMillis,
            long now) {
        if (!ObjectProtectionRequest.isPending(type)) {
            if (expiresAtMillis != 0) {
                throw new IllegalArgumentException("permanent protection cannot expire");
            }
            return;
        }
        long skewSafeNow = Math.addExact(now, maximumClockSkewMillis);
        long maximumExpiry = Math.addExact(now, pendingProtectionDurationMillis);
        if (expiresAtMillis <= skewSafeNow || expiresAtMillis > maximumExpiry) {
            throw new IllegalArgumentException(
                    "pending protection expiry is outside its skew-safe configured window");
        }
    }

    private void validateDurableExpiry(
            com.nereusstream.metadata.oxia.records.ObjectProtectionType type,
            long createdAtMillis,
            long expiresAtMillis,
            long now) {
        validateRequestedExpiry(type, expiresAtMillis, now);
        if (ObjectProtectionRequest.isPending(type)
                && expiresAtMillis
                        > Math.addExact(createdAtMillis, pendingProtectionDurationMillis)) {
            throw invariant("pending protection exceeds its durable creation-time bound");
        }
    }

    private static ObjectProtectionRecord transferRecord(
            ObjectProtectionRecord current,
            ObjectProtectionOwner newOwner,
            long rootLifecycleEpoch) {
        return new ObjectProtectionRecord(
                current.schemaVersion(),
                current.objectKeyHash(),
                current.protectionTypeId(),
                current.referenceId(),
                newOwner.ownerKey(),
                newOwner.metadataVersion(),
                newOwner.identitySha256().value(),
                rootLifecycleEpoch,
                current.createdAtMillis(),
                current.expiresAtMillis(),
                0);
    }

    private static ObjectProtectionRecord requestedRecord(
            ObjectProtectionRequest request,
            VersionedPhysicalObjectRoot root,
            long now) {
        return new ObjectProtectionRecord(
                1,
                request.object().objectKeyHash().value(),
                request.type().wireId(),
                request.referenceId(),
                request.owner().ownerKey(),
                request.owner().metadataVersion(),
                request.owner().identitySha256().value(),
                root.value().lifecycleEpoch(),
                now,
                request.expiresAtMillis(),
                0);
    }

    private static ObjectProtection fromVersioned(
            PhysicalObjectIdentity object,
            VersionedObjectProtection value) {
        ObjectProtectionRecord record = value.value();
        return new ObjectProtection(
                object,
                identity(record),
                owner(record),
                record.rootLifecycleEpoch(),
                record.createdAtMillis(),
                record.expiresAtMillis(),
                value.metadataVersion(),
                value.durableValueSha256());
    }

    private static ObjectProtectionIdentity identity(ObjectProtectionRecord value) {
        return new ObjectProtectionIdentity(
                new ObjectKeyHash(value.objectKeyHash()),
                com.nereusstream.metadata.oxia.records.ObjectProtectionType.fromWireId(
                        value.protectionTypeId()),
                value.referenceId());
    }

    private static ObjectProtectionOwner owner(ObjectProtectionRecord value) {
        return new ObjectProtectionOwner(
                value.ownerKey(),
                value.ownerMetadataVersion(),
                new com.nereusstream.api.Checksum(
                        com.nereusstream.api.ChecksumType.SHA256,
                        value.ownerIdentitySha256()));
    }

    private static void requireRecordIdentity(
            ObjectProtectionIdentity expected,
            ObjectProtectionRecord actual) {
        if (!recordMatchesIdentity(expected, actual)) {
            throw invariant("protection record does not match its exact identity");
        }
    }

    private static boolean recordMatchesIdentity(
            ObjectProtectionIdentity identity,
            ObjectProtectionRecord value) {
        return value.objectKeyHash().equals(identity.object().value())
                && value.protectionTypeId() == identity.type().wireId()
                && value.referenceId().equals(identity.referenceId());
    }

    private static void requireSameHandle(
            ObjectProtection expected,
            VersionedObjectProtection actual) {
        requireRecordIdentity(expected.identity(), actual.value());
        if (!matchesHandle(expected, actual)) {
            throw condition("object protection handle is stale");
        }
    }

    private static boolean matchesHandle(
            ObjectProtection expected,
            VersionedObjectProtection actual) {
        ObjectProtectionRecord value = actual.value();
        return actual.metadataVersion() == expected.metadataVersion()
                && actual.durableValueSha256().equals(expected.durableValueSha256())
                && owner(value).equals(expected.owner())
                && value.rootLifecycleEpoch() == expected.rootLifecycleEpoch()
                && value.createdAtMillis() == expected.createdAtMillis()
                && value.expiresAtMillis() == expected.expiresAtMillis();
    }

    private static boolean matchesTransferredOutcome(
            ObjectProtection previous,
            ObjectProtectionOwner newOwner,
            ObjectProtectionRecord current) {
        return recordMatchesIdentity(previous.identity(), current)
                && owner(current).equals(newOwner)
                && current.createdAtMillis() == previous.createdAtMillis()
                && current.expiresAtMillis() == previous.expiresAtMillis();
    }

    private static boolean sameRecordIgnoringMetadataVersion(
            ObjectProtectionRecord left,
            ObjectProtectionRecord right) {
        return left.withMetadataVersion(0).equals(right.withMetadataVersion(0));
    }

    private static boolean sameVersioned(
            VersionedObjectProtection left,
            VersionedObjectProtection right) {
        return left.metadataVersion() == right.metadataVersion()
                && left.value().equals(right.value())
                && left.durableValueSha256().equals(right.durableValueSha256());
    }

    private static CompletableFuture<Void> invokeOwnerRevalidator(
            OwnerRevalidator revalidator,
            ObjectProtectionOwner owner) {
        try {
            return Objects.requireNonNull(
                    revalidator.revalidate(owner), "owner revalidator result");
        } catch (Throwable failure) {
            return failed(failure);
        }
    }

    private static CompletableFuture<Void> invokeRemovalAuthorizer(
            RemovalAuthorizer authorizer,
            ObjectProtection protection) {
        try {
            return Objects.requireNonNull(
                    authorizer.authorizeRemoval(protection),
                    "removal authorizer result");
        } catch (Throwable failure) {
            return failed(failure);
        }
    }

    private <T> CompletableFuture<T> serialized(
            ObjectProtectionIdentity identity,
            Supplier<CompletableFuture<T>> operation) {
        SerialState state = serialStates[Math.floorMod(identity.hashCode(), serialStates.length)];
        synchronized (state) {
            CompletableFuture<T> result = state.tail
                    .handle((ignored, previousFailure) -> null)
                    .thenCompose(ignored -> {
                        try {
                            return Objects.requireNonNull(
                                    operation.get(), "serialized operation result");
                        } catch (Throwable failure) {
                            return failed(failure);
                        }
                    });
            state.tail = result.handle((ignored, failure) -> null);
            return result;
        }
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
            throw new IllegalArgumentException(
                    name + " must be positive and millisecond-representable");
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

    private static NereusException closedFailure() {
        return new NereusException(
                ErrorCode.STORAGE_CLOSED,
                false,
                "object protection manager is closed");
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> CompletableFuture<T> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }

    private record Acquisition(
            VersionedObjectProtection value,
            boolean createdByThisCall) {
    }

    private static final class SerialState {
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
    }
}
