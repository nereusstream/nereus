/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.materialization;

import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;

/**
 * Public cold stream over one frozen exact source set, with one subscriber and owned cancellation.
 */
public final class ExactSourceSetBatchPublisher implements Flow.Publisher<ReadBatch>, AutoCloseable {
    private final ExactSourceBatchPublisher delegate;

    public ExactSourceSetBatchPublisher(
            ExactSourceSet sourceSet,
            ExactSourceRangeReader reader,
            ReadOptions options,
            Executor callbackExecutor,
            boolean rangedEntries) {
        delegate = new ExactSourceBatchPublisher(
                Objects.requireNonNull(sourceSet, "sourceSet"),
                Objects.requireNonNull(reader, "reader"),
                Objects.requireNonNull(options, "options"),
                Objects.requireNonNull(callbackExecutor, "callbackExecutor"),
                rangedEntries);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ReadBatch> subscriber) {
        delegate.subscribe(subscriber);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
