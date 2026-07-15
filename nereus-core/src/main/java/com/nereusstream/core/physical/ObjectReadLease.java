/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import java.util.concurrent.CompletableFuture;

/** Ref-counted logical read admission backed by one process/object durable lease. */
public interface ObjectReadLease extends AutoCloseable {
    PhysicalObjectIdentity object();

    String leaseId();

    long maximumReadDeadlineMillis();

    CompletableFuture<Void> release();

    boolean isReleased();

    @Override
    default void close() {
        release();
    }
}
