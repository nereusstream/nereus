/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.concurrent.CompletableFuture;

/** Caller-owned storage-class guard revalidated inside the projection operation immediately before topic publish. */
@FunctionalInterface
public interface ProjectionPublishGuard {
    CompletableFuture<Void> validateBeforePublish();
}
