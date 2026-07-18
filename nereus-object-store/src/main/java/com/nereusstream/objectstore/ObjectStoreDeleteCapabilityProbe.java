/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.util.concurrent.CompletableFuture;

/** Proves the exact configured object-store scope supports the V1 destructive protocol. */
public interface ObjectStoreDeleteCapabilityProbe {
    /** Deterministic identity of provider/scope plus the exact semantics exercised by {@link #probe}. */
    String expectedCapabilitySha256();

    CompletableFuture<ObjectStoreDeleteCapabilityProof> probe(
            ObjectStoreDeleteCapabilityRequest request);
}
