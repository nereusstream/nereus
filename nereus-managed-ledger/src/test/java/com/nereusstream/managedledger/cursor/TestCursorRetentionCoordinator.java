/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Minimal retention role kept distinct from TestCursorStorage for runtime lifecycle tests. */
public final class TestCursorRetentionCoordinator implements CursorRetentionCoordinator {
    @Override
    public CompletableFuture<CursorRetentionView> claimAndRecover(CursorOwnerSession owner) {
        return CompletableFuture.completedFuture(active(owner));
    }

    @Override
    public CompletableFuture<ProtectionLease> beginProtection(
            CursorOwnerSession owner,
            ProtectionRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("beginProtection"));
    }

    @Override
    public CompletableFuture<CursorRetentionView> completeProtection(ProtectionLease lease) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("completeProtection"));
    }

    @Override
    public CompletableFuture<CursorRetentionView> reconcileFloor(CursorOwnerSession owner) {
        return CompletableFuture.completedFuture(active(owner));
    }

    @Override
    public CompletableFuture<CursorRetentionView> requestTrim(
            CursorOwnerSession owner,
            long candidateOffset,
            String reason) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("requestTrim"));
    }

    @Override
    public void close() {
    }

    private static CursorRetentionView active(CursorOwnerSession owner) {
        return new CursorRetentionView(
                owner.ledger(),
                owner.ownerSessionId(),
                CursorRetentionView.Lifecycle.ACTIVE,
                1,
                0,
                0,
                0,
                Optional.empty(),
                Optional.empty());
    }
}
