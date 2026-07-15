/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.StreamId;
import java.util.concurrent.CompletableFuture;

/** Best-effort durable health propagation before a same-view physical fallback. */
@FunctionalInterface
public interface GenerationReadFailureHandler {
    CompletableFuture<Void> handle(
            StreamId streamId, GenerationReadCandidate candidate, Throwable failure);

    static GenerationReadFailureHandler noOp() {
        return (streamId, candidate, failure) -> CompletableFuture.completedFuture(null);
    }
}
