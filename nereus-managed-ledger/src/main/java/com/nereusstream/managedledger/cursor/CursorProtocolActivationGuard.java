/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Broker-capability gate invoked before the first monotonic cursor-protocol activation. */
@FunctionalInterface
public interface CursorProtocolActivationGuard {
    CompletableFuture<Void> acquireFirstActivationPermit(CursorLedgerIdentity ledger);

    static CursorProtocolActivationGuard unavailable() {
        return ledger -> {
            Objects.requireNonNull(ledger, "ledger");
            return CompletableFuture.failedFuture(
                    new IllegalStateException("NEREUS_CURSOR_CAPABILITY_NOT_READY"));
        };
    }
}
