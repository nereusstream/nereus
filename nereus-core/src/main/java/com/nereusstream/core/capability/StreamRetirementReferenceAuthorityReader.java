/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import java.util.concurrent.CompletableFuture;

/**
 * Captures protocol-owned per-stream references that would leave the global GC scope when a
 * materialization registration is retired.
 *
 * <p>The returned value is an exact, re-capturable authority snapshot. Implementations must fail
 * closed on pagination limits, transitional lifecycle states, or an unsupported projection.
 */
@FunctionalInterface
public interface StreamRetirementReferenceAuthorityReader {
    CompletableFuture<StreamRetirementReferenceAuthoritySnapshot> capture(
            LiveProjectionSubject subject);
}
