/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Cold sequential exact-source concat publisher used by both topic-compaction passes. */
final class ExactSourceBatchPublisher implements Flow.Publisher<ReadBatch>, AutoCloseable {
    private final List<SourceGeneration> sources;
    private final ExactSourceRangeReader reader;
    private final ReadOptions options;
    private final SerialExecutor serial;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicBoolean closeRequested = new AtomicBoolean();

    private Flow.Subscriber<? super ReadBatch> downstream;
    private CompletableFuture<ExactSourceRead> opening;
    private ExactSourceRead current;
    private Flow.Subscription upstream;
    private int sourceIndex;
    private long expectedOffset;
    private long demand;
    private boolean upstreamRequested;
    private boolean terminal;

    ExactSourceBatchPublisher(
            MaterializationTask task,
            ExactSourceRangeReader reader,
            ReadOptions options,
            Executor callbackExecutor) {
        MaterializationTask exactTask = Objects.requireNonNull(task, "task");
        this.sources = exactTask.sources();
        this.reader = Objects.requireNonNull(reader, "reader");
        this.options = Objects.requireNonNull(options, "options");
        this.serial = new SerialExecutor(Objects.requireNonNull(callbackExecutor, "callbackExecutor"));
        this.expectedOffset = exactTask.coverage().startOffset();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ReadBatch> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");
        if (!subscribed.compareAndSet(false, true)) {
            reject(subscriber);
            return;
        }
        submit(() -> {
            if (terminal || closeRequested.get()) {
                reject(subscriber);
                return;
            }
            downstream = subscriber;
            try {
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long count) {
                        submit(() -> requestOnSerial(count));
                    }

                    @Override
                    public void cancel() {
                        closeRequested.set(true);
                        submit(() -> fail(new NereusException(
                                ErrorCode.CANCELLED,
                                false,
                                "exact source batch subscriber cancelled"), false));
                    }
                });
            } catch (Throwable failure) {
                fail(failure, false);
            }
        });
    }

    @Override
    public void close() {
        closeRequested.set(true);
        submit(() -> fail(new NereusException(
                ErrorCode.CANCELLED, false, "exact source batch publisher closed"), false));
    }

    private void requestOnSerial(long count) {
        if (terminal) {
            return;
        }
        if (count <= 0) {
            fail(new IllegalArgumentException("Flow request count must be positive"), true);
            return;
        }
        demand = addDemand(demand, count);
        advance();
    }

    private void advance() {
        if (closeRequested.get() && !terminal) {
            fail(new NereusException(
                    ErrorCode.CANCELLED, false, "exact source batch publisher closed"), false);
            return;
        }
        if (terminal || demand == 0 || opening != null) {
            return;
        }
        if (current == null) {
            if (sourceIndex == sources.size()) {
                complete();
            } else {
                openSource();
            }
            return;
        }
        requestUpstream();
    }

    private void openSource() {
        SourceGeneration source = sources.get(sourceIndex);
        expectedOffset = source.range().startOffset();
        try {
            opening = Objects.requireNonNull(reader.read(source, options), "exact source future");
        } catch (Throwable failure) {
            fail(failure, true);
            return;
        }
        CompletableFuture<ExactSourceRead> pending = opening;
        pending.whenComplete((opened, failure) -> submit(() -> {
            if (terminal || opening != pending) {
                if (opened != null) {
                    opened.close();
                }
                return;
            }
            opening = null;
            if (failure != null) {
                fail(unwrap(failure), true);
                return;
            }
            current = Objects.requireNonNull(opened, "exact source read");
            if (!current.source().equals(source)) {
                fail(invariant("exact reader returned another source identity"), true);
                return;
            }
            try {
                current.batches().subscribe(new SourceSubscriber(source));
            } catch (Throwable subscribeFailure) {
                fail(subscribeFailure, true);
            }
        }));
    }

    private void requestUpstream() {
        if (upstream == null || upstreamRequested || demand == 0 || terminal) {
            return;
        }
        upstreamRequested = true;
        try {
            upstream.request(1);
        } catch (Throwable failure) {
            upstreamRequested = false;
            fail(failure, true);
        }
    }

    private void sourceNext(SourceGeneration source, ReadBatch batch) {
        if (terminal) {
            return;
        }
        if (!source.equals(sources.get(sourceIndex)) || !upstreamRequested || demand == 0) {
            fail(invariant("exact source emitted outside downstream demand"), true);
            return;
        }
        upstreamRequested = false;
        try {
            ReadBatch exact = Objects.requireNonNull(batch, "batch");
            if (exact.range().recordCount() != 1
                    || exact.range().startOffset() != expectedOffset
                    || !source.range().contains(expectedOffset)
                    || exact.payloadFormat() != source.payloadFormat()
                    || !exact.schemaRefs().equals(source.schemaRefs())
                    || !exact.projectionRef().equals(source.projectionRef())) {
                throw invariant("exact source batch is not one dense task record");
            }
            expectedOffset = Math.addExact(expectedOffset, 1);
            demand--;
            downstream.onNext(exact);
        } catch (Throwable failure) {
            fail(failure, true);
            return;
        }
        advance();
    }

    private void sourceComplete(SourceGeneration source) {
        if (terminal || current == null || !source.equals(sources.get(sourceIndex))) {
            return;
        }
        ExactSourceRead completed = current;
        completed.completion().whenComplete((summary, failure) -> submit(() -> {
            if (terminal || current != completed) {
                completed.close();
                return;
            }
            if (failure != null) {
                fail(unwrap(failure), true);
                return;
            }
            try {
                requireSummary(source, summary);
                if (expectedOffset != source.range().endOffset()) {
                    throw invariant("exact source ended before dense task coverage");
                }
            } catch (Throwable validationFailure) {
                fail(validationFailure, true);
                return;
            }
            completed.close();
            current = null;
            upstream = null;
            upstreamRequested = false;
            sourceIndex++;
            advance();
        }));
    }

    private void complete() {
        if (terminal) {
            return;
        }
        terminal = true;
        try {
            downstream.onComplete();
        } catch (Throwable ignored) {
        }
    }

    private void fail(Throwable failure, boolean signal) {
        if (terminal) {
            return;
        }
        terminal = true;
        Throwable exact = unwrap(failure);
        CompletableFuture<ExactSourceRead> pending = opening;
        opening = null;
        if (pending != null) {
            pending.cancel(true);
        }
        Flow.Subscription subscription = upstream;
        upstream = null;
        if (subscription != null) {
            subscription.cancel();
        }
        ExactSourceRead active = current;
        current = null;
        if (active != null) {
            active.close();
        }
        if (signal && downstream != null) {
            try {
                downstream.onError(exact);
            } catch (Throwable ignored) {
            }
        }
    }

    private void submit(Runnable action) {
        try {
            serial.execute(action);
        } catch (RejectedExecutionException failure) {
            fail(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "exact source callback executor rejected admitted work",
                    failure), true);
        }
    }

    private static void requireSummary(SourceGeneration source, ExactSourceReadSummary summary) {
        if (!summary.coverage().equals(source.range())
                || summary.recordCount() != source.recordCount()
                || summary.entryCount() != source.entryCount()
                || summary.logicalBytes() != source.logicalBytes()) {
            throw invariant("exact source summary differs from the materialization task");
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static void reject(Flow.Subscriber<?> subscriber) {
        try {
            subscriber.onSubscribe(NoopSubscription.INSTANCE);
            subscriber.onError(new IllegalStateException(
                    "exact source batch publisher permits exactly one subscriber"));
        } catch (Throwable ignored) {
        }
    }

    private static long addDemand(long current, long requested) {
        long result = current + requested;
        return result < 0 ? Long.MAX_VALUE : result;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private enum NoopSubscription implements Flow.Subscription {
        INSTANCE;

        @Override
        public void request(long count) {
        }

        @Override
        public void cancel() {
        }
    }

    private final class SourceSubscriber implements Flow.Subscriber<ReadBatch> {
        private final SourceGeneration source;

        private SourceSubscriber(SourceGeneration source) {
            this.source = source;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            submit(() -> {
                if (terminal || current == null || !source.equals(sources.get(sourceIndex))) {
                    subscription.cancel();
                    return;
                }
                if (upstream != null) {
                    subscription.cancel();
                    fail(new IllegalStateException("exact source subscribed more than once"), true);
                    return;
                }
                upstream = subscription;
                requestUpstream();
            });
        }

        @Override
        public void onNext(ReadBatch item) {
            submit(() -> sourceNext(source, item));
        }

        @Override
        public void onError(Throwable failure) {
            submit(() -> fail(unwrap(failure), true));
        }

        @Override
        public void onComplete() {
            submit(() -> sourceComplete(source));
        }
    }

    private static final class SerialExecutor implements Executor {
        private final Executor delegate;
        private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
        private Runnable active;

        private SerialExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void execute(Runnable command) {
            queue.addLast(() -> {
                try {
                    command.run();
                } finally {
                    scheduleNext();
                }
            });
            if (active == null) {
                scheduleNext();
            }
        }

        private synchronized void scheduleNext() {
            active = queue.pollFirst();
            if (active != null) {
                delegate.execute(active);
            }
        }
    }
}
