/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.concurrent.CompletableFuture;

/** Claims, executes, verifies and durably freezes one task output without publishing visibility. */
public interface MaterializationWorker {
    CompletableFuture<MaterializationOutput> execute(MaterializationTask task);

    /** Best-effort local cancellation; durable claim recovery remains metadata-driven. */
    default void cancel(MaterializationTask task) {
    }
}
