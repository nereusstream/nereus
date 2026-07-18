/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import java.util.concurrent.CompletableFuture;

/** Process-wide non-overlapping lifecycle for complete physical-GC passes. */
public interface PhysicalGcLifecycleService extends AutoCloseable {
    CompletableFuture<Void> start();

    /** Coalesces one immediate complete pass with a pass already in flight. */
    CompletableFuture<PhysicalGcLifecyclePassResult> scanNow();

    CompletableFuture<Void> closeAsync();

    boolean isRunning();

    @Override
    void close();
}
