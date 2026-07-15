/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.concurrent.CompletableFuture;

/** Process-shared, multi-stream materialization scan and worker lifecycle. */
public interface MaterializationService extends AutoCloseable {
    CompletableFuture<Void> start();

    /** Coalesces an immediate full registry pass with any pass already in flight. */
    CompletableFuture<RegisteredMaterializationScanResult> scanNow();

    CompletableFuture<Void> closeAsync();

    boolean isRunning();

    @Override
    void close();
}
