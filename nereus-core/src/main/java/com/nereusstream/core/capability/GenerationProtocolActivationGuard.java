/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import java.util.concurrent.CompletableFuture;

public interface GenerationProtocolActivationGuard {
    CompletableFuture<GenerationActivationProof> requireReady(
            GenerationOperation operation,
            GenerationActivationSubject subject,
            boolean activateLiveProjectionIfAbsent);

    CompletableFuture<Void> revalidate(GenerationActivationProof proof);
}
