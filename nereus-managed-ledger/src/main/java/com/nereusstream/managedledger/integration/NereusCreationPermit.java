/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.integration;

import com.nereusstream.api.StorageProfile;
import java.util.concurrent.CompletableFuture;

public interface NereusCreationPermit {
    String persistenceName();

    long bindingGeneration();

    /** Rollout barrier for a new durable profile; existing projections do not call this hook. */
    default CompletableFuture<Void> validateStorageProfileBeforeCreate(StorageProfile profile) {
        java.util.Objects.requireNonNull(profile, "profile");
        return CompletableFuture.completedFuture(null);
    }

    /** Local broker admission for an existing durable profile before writable hydration. */
    default CompletableFuture<Void> validateStorageProfileBeforeWritableOpen(StorageProfile profile) {
        java.util.Objects.requireNonNull(profile, "profile");
        return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> validateBeforeProjectionPublish();
}
