/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Narrow read/delete-only bridge from retirement adapters to one shared Oxia runtime. */
public interface RetirementMetadataClient {
    CompletableFuture<Optional<RetirementMetadataValue>> get(RetirementMetadataKey key);

    CompletableFuture<Void> deleteIfVersion(
            RetirementMetadataKey key, long expectedVersion);
}
