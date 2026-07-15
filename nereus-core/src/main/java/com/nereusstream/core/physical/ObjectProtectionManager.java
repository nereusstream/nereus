/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

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
     * owner key to the requested durable owner version.
     */
    CompletableFuture<ObjectProtection> acquireOrTransfer(
            ObjectProtectionRequest request,
            OwnerRevalidator ownerRevalidator);

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
