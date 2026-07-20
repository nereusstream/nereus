/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Product-neutral live broker capability barrier used by the BookKeeper deletion gate. */
public interface BookKeeperBrokerReadinessProvider {
    CompletableFuture<BookKeeperBrokerReadiness> requireBookKeeperPrimaryWalReadiness();

    Optional<BookKeeperBrokerReadiness> currentBookKeeperPrimaryWalReadiness();

    static BookKeeperBrokerReadinessProvider unavailable() {
        return new BookKeeperBrokerReadinessProvider() {
            @Override
            public CompletableFuture<BookKeeperBrokerReadiness>
                    requireBookKeeperPrimaryWalReadiness() {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "NEREUS_BOOKKEEPER_CAPABILITY_NOT_READY"));
            }

            @Override
            public Optional<BookKeeperBrokerReadiness>
                    currentBookKeeperPrimaryWalReadiness() {
                return Optional.empty();
            }
        };
    }
}
