/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Optional higher-generation completion seam implemented by the shared F4 materialization runtime. */
@FunctionalInterface
public interface RequiredObjectGenerationCompletion {
    CompletableFuture<RequiredObjectGenerationProof> complete(
            RequiredObjectGenerationRequest request,
            Duration timeout);
}
