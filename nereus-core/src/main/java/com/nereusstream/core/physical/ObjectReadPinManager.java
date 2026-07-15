/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import java.util.concurrent.CompletableFuture;

public interface ObjectReadPinManager extends AutoCloseable {
    @FunctionalInterface
    interface SelectionRevalidator {
        CompletableFuture<Void> revalidate();
    }

    CompletableFuture<ObjectReadLease> acquire(
            PhysicalObjectIdentity object,
            long maximumReadDeadlineMillis,
            SelectionRevalidator selectionRevalidator);

    @Override
    void close();
}
