/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Live normal-read-path authority required in addition to durable committed-generation metadata. */
@FunctionalInterface
public interface CommittedObjectGenerationReadVerifier {
    CompletableFuture<Boolean> verify(
            CommittedObjectGenerationProof proof,
            Duration timeout);
}
