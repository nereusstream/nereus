/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import java.util.concurrent.CompletableFuture;

/** Provider-owned durable protection protocol for one task-frozen source target. */
public interface MaterializationSourceProtectionAdapter<T extends ReadTarget> {
    @FunctionalInterface
    interface OwnerRevalidator {
        CompletableFuture<Void> revalidate(ObjectProtectionOwner expectedOwner);
    }

    @FunctionalInterface
    interface RemovalAuthorizer {
        CompletableFuture<Void> authorize(MaterializationSourceProtection exactProtection);
    }

    ReadTargetType targetType();

    Class<T> targetClass();

    CompletableFuture<MaterializationSourceProtection> acquireOrTransfer(
            StreamId streamId,
            SourceGeneration source,
            String referenceId,
            ObjectProtectionOwner owner,
            OwnerRevalidator ownerRevalidator);

    CompletableFuture<MaterializationSourceProtection> revalidate(
            MaterializationSourceProtection protection,
            OwnerRevalidator ownerRevalidator);

    CompletableFuture<MaterializationSourceProtection> transfer(
            MaterializationSourceProtection protection,
            ObjectProtectionOwner newOwner,
            OwnerRevalidator newOwnerRevalidator);

    CompletableFuture<Void> release(
            MaterializationSourceProtection protection,
            RemovalAuthorizer removalAuthorizer);
}
