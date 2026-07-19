/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.append.AppendAdmissionGuard;
import com.nereusstream.core.append.AppendAdmissionRequest;
import com.nereusstream.core.backpressure.MaterializationLagGate;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Applies the shared F4 lag gate to BK-async appends before primary BookKeeper preparation or IO. */
public final class BookKeeperAsyncAppendAdmissionGuard implements AppendAdmissionGuard {
    private final MaterializationLagGate lagGate;

    public BookKeeperAsyncAppendAdmissionGuard(MaterializationLagGate lagGate) {
        this.lagGate = Objects.requireNonNull(lagGate, "lagGate");
    }

    @Override
    public CompletableFuture<Void> admit(AppendAdmissionRequest request) {
        final AppendAdmissionRequest exact;
        try {
            exact = Objects.requireNonNull(request, "request");
            if (exact.storageProfile() != StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT) {
                return CompletableFuture.completedFuture(null);
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return lagGate.admit(exact.streamId(), exact.timeout()).thenApply(ignored -> null);
    }
}
