/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.compacted.CompactedObjectRow;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Sequential concat-map from exact source batches to opaque NCP1 rows with one-source-at-a-time memory. */
final class LosslessMaterializationRowPublisher
        implements Flow.Publisher<CompactedObjectRow>, AutoCloseable {
    private final List<SourceGeneration> sources;
    private final ExactSourceRangeReader reader;
    private final ReadOptions readOptions;
    private final SerialExecutor serial;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final CompletableFuture<Void> completion = new CompletableFuture<>();

    private Flow.Subscriber<? super CompactedObjectRow> downstream;
    private CompletableFuture<ExactSourceRead> opening;
    private ExactSourceRead current;
    private Flow.Subscription upstream;
    private int sourceIndex;
    private long demand;
    private boolean upstreamRequested;
    private boolean terminal;

    LosslessMaterializationRowPublisher(
            MaterializationTask task,
            ExactSourceRangeReader reader,
            ReadOptions readOptions,
            Executor callbackExecutor) {
        MaterializationTask exactTask = Objects.requireNonNull(task, "task");
        if (exactTask.taskKind() != TaskKind.LOSSLESS_REWRITE) {
            throw new IllegalArgumentException("lossless row publisher requires LOSSLESS_REWRITE");
        }
        this.sources = exactTask.sources();
        this.reader = Objects.requireNonNull(reader, "reader");
        this.readOptions = Objects.requireNonNull(readOptions, "readOptions");
        this.serial = new SerialExecutor(Objects.requireNonNull(callbackExecutor, "callbackExecutor"));
    }

    CompletableFuture<Void> completion() {
        return completion;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super CompactedObjectRow> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");
        if (!subscribed.compareAndSet(false, true)) {
            reject(subscriber);
            return;
        }
        submit(() -> {
            if (terminal) {
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
                        cancellationRequested.set(true);
                        submit(() -> LosslessMaterializationRowPublisher.this.cancel(new NereusException(
                                ErrorCode.CANCELLED,
                                false,
                                "materialization row subscriber cancelled")));
                    }
                });
            } catch (Throwable failure) {
                fail(failure, false);
            }
        });
    }

    @Override
    public void close() {
        cancellationRequested.set(true);
        submit(() -> cancel(new NereusException(
                ErrorCode.CANCELLED,
                false,
                "materialization row publisher closed")));
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
        if (cancellationRequested.get() && !terminal) {
            cancel(new NereusException(
                    ErrorCode.CANCELLED, false, "materialization row subscriber cancelled"));
            return;
        }
        if (terminal || demand == 0 || opening != null) {
            return;
        }
        if (current == null) {
            if (sourceIndex == sources.size()) {
                complete();
                return;
            }
            openSource();
            return;
        }
        requestUpstream();
    }

    private void openSource() {
        SourceGeneration source = sources.get(sourceIndex);
        try {
            opening = Objects.requireNonNull(reader.read(source, readOptions), "exact source future");
        } catch (Throwable failure) {
            fail(failure, true);
            return;
        }
        CompletableFuture<ExactSourceRead> exactOpening = opening;
        exactOpening.whenComplete((opened, failure) -> submit(() -> {
            if (terminal || opening != exactOpening) {
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
                fail(new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "exact reader returned another source identity"), true);
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
            fail(new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "exact source publisher emitted outside downstream demand"), true);
            return;
        }
        upstreamRequested = false;
        CompactedObjectRow row;
        try {
            row = row(batch);
            demand--;
            downstream.onNext(row);
        } catch (Throwable failure) {
            fail(failure, false);
            return;
        }
        if (cancellationRequested.get()) {
            cancel(new NereusException(
                    ErrorCode.CANCELLED, false, "materialization row subscriber cancelled"));
            return;
        }
        advance();
    }

    private void sourceComplete(SourceGeneration source) {
        if (terminal || !source.equals(sources.get(sourceIndex)) || current == null) {
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
            } catch (Throwable validationFailure) {
                fail(validationFailure, true);
                return;
            }
            completed.close();
            current = null;
            upstream = null;
            upstreamRequested = false;
            sourceIndex++;
            if (sourceIndex == sources.size()) {
                complete();
            } else {
                advance();
            }
        }));
    }

    private void sourceError(Throwable failure) {
        submit(() -> fail(unwrap(failure), true));
    }

    private void complete() {
        if (terminal) {
            return;
        }
        terminal = true;
        completion.complete(null);
        try {
            downstream.onComplete();
        } catch (Throwable ignored) {
            // The worker observes completion independently through writer/completion futures.
        }
    }

    private void cancel(Throwable failure) {
        fail(failure, false);
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
        Flow.Subscription activeUpstream = upstream;
        upstream = null;
        if (activeUpstream != null) {
            activeUpstream.cancel();
        }
        ExactSourceRead active = current;
        current = null;
        if (active != null) {
            active.close();
        }
        completion.completeExceptionally(exact);
        if (signal && downstream != null) {
            try {
                downstream.onError(exact);
            } catch (Throwable ignored) {
                // Completion remains authoritative for the worker.
            }
        }
    }

    private static CompactedObjectRow row(ReadBatch batch) {
        Objects.requireNonNull(batch, "batch");
        if (batch.range().recordCount() != 1) {
            throw new MaterializationExecutionException(
                    TaskFailureClass.UNSUPPORTED_MAPPING,
                    ErrorCode.UNSUPPORTED_FORMAT,
                    false,
                    "NCP1 lossless rewrite requires one exact entry per stream offset");
        }
        byte[] payload = batch.payload();
        return new CompactedObjectRow(
                batch.range().startOffset(),
                ByteBuffer.wrap(payload),
                Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty());
    }

    private static void requireSummary(
            SourceGeneration source,
            ExactSourceReadSummary summary) {
        if (!summary.coverage().equals(source.range())
                || summary.recordCount() != source.recordCount()
                || summary.entryCount() != source.entryCount()
                || summary.logicalBytes() != source.logicalBytes()) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "exact source summary differs from the materialization task");
        }
    }

    private void submit(Runnable action) {
        try {
            serial.execute(action);
        } catch (RejectedExecutionException failure) {
            if (!terminal) {
                fail(new NereusException(
                        ErrorCode.STORAGE_CLOSED,
                        false,
                        "materialization row executor rejected admitted work",
                        failure), true);
            }
        }
    }

    private static void reject(Flow.Subscriber<? super CompactedObjectRow> subscriber) {
        try {
            subscriber.onSubscribe(NoopSubscription.INSTANCE);
            subscriber.onError(new IllegalStateException(
                    "materialization row publisher permits exactly one subscriber"));
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
            sourceError(failure);
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
