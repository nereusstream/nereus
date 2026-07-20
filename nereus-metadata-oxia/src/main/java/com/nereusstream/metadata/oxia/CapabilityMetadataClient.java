/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact-key capability metadata surface used only by explicit cluster administration. */
public interface CapabilityMetadataClient {
    CompletableFuture<Optional<CapabilityMetadataValue>> get(
            String key, String partitionKey);

    CompletableFuture<CapabilityMetadataValue> putIfAbsent(
            String key, byte[] value, String partitionKey);

    CompletableFuture<CapabilityMetadataValue> putIfVersion(
            String key, byte[] value, long expectedVersion, String partitionKey);
}
