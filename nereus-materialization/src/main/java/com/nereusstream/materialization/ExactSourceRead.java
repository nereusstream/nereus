/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ReadBatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/** Close-owned, single-subscription exact source stream backed by one durable reader pin. */
public interface ExactSourceRead extends AutoCloseable {
    SourceGeneration source();

    Flow.Publisher<ReadBatch> batches();

    CompletableFuture<ExactSourceReadSummary> completion();

    @Override
    void close();
}
