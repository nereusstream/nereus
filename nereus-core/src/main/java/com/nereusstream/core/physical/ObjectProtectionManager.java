/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Durable create/revalidate/transfer/release handshake for non-reader object references. */
public interface ObjectProtectionManager extends AutoCloseable {
    @FunctionalInterface
    interface OwnerRevalidator {
        CompletableFuture<Void> revalidate(ObjectProtectionOwner expectedOwner);
    }

    @FunctionalInterface
    interface RemovalAuthorizer {
        CompletableFuture<Void> authorizeRemoval(ObjectProtection protection);
    }

    CompletableFuture<ObjectProtection> acquire(
            ObjectProtectionRequest request,
            OwnerRevalidator ownerRevalidator);

    /**
     * Acquires an absent protection or monotonically reconciles an existing protection owned by the same logical
     * owner key to the requested durable owner version and current ACTIVE root lifecycle epoch.
     */
    CompletableFuture<ObjectProtection> acquireOrTransfer(
            ObjectProtectionRequest request,
            OwnerRevalidator ownerRevalidator);

    /**
     * Finds one exact durable protection without acquiring or transferring it.
     *
     * <p>Terminal workflow cleanup uses this read-only lookup so an already released protection is
     * distinguishable from a live task-owned reference.
     */
    default CompletableFuture<Optional<ObjectProtection>> findExisting(
            PhysicalObjectIdentity object,
            ObjectProtectionIdentity identity) {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("exact object-protection lookup is unsupported"));
    }

    CompletableFuture<ObjectProtection> revalidate(
            ObjectProtection protection,
            OwnerRevalidator ownerRevalidator);

    CompletableFuture<ObjectProtection> transfer(
            ObjectProtection protection,
            ObjectProtectionOwner newOwner,
            OwnerRevalidator newOwnerRevalidator);

    CompletableFuture<Void> release(
            ObjectProtection protection,
            RemovalAuthorizer removalAuthorizer);

    @Override
    void close();
}
