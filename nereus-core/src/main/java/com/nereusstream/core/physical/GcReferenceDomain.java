/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import java.util.concurrent.CompletableFuture;

public interface GcReferenceDomain {
    String domainId();

    int protocolVersion();

    CompletableFuture<GcReferenceSnapshot> snapshot(GcReferenceQuery query);

    CompletableFuture<Boolean> stillMatches(
            GcReferenceQuery query, GcReferenceSnapshot snapshot);
}
