/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.concurrent.CompletableFuture;

/** Ephemeral capability revalidation invoked immediately before one durable task mutation. */
@FunctionalInterface
public interface MaterializationTaskMutationGuard {
    CompletableFuture<Void> revalidate();

    static MaterializationTaskMutationGuard noOp() {
        return () -> CompletableFuture.completedFuture(null);
    }
}
