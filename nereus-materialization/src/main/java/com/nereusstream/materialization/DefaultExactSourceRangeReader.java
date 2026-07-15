/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.read.PhysicalObjectIdentityResolver;
import com.nereusstream.core.read.ReadTargetDispatcher;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Task-stream-scoped exact reader that pins one physical target and never performs generation selection.
 *
 * <p>{@link ReadOptions#maxRecords()} and {@link ReadOptions#maxBytes()} bound each physical page. A successful
 * completion always consumes and verifies the entire frozen source range.
 */
public final class DefaultExactSourceRangeReader implements ExactSourceRangeReader {
    private final String cluster;
    private final StreamId streamId;
    private final GenerationMetadataStore generations;
    private final PhysicalObjectIdentityResolver identities;
    private final ObjectReadPinManager pins;
    private final ReadTargetDispatcher dispatcher;
    private final int sourceReadPageRecords;
    private final int sourceReadPageBytes;
    private final Clock clock;
    private final Executor callbackExecutor;

    public DefaultExactSourceRangeReader(
            String cluster,
            StreamId streamId,
            GenerationMetadataStore generations,
            PhysicalObjectIdentityResolver identities,
            ObjectReadPinManager pins,
            ReadTargetDispatcher dispatcher,
            int sourceReadPageRecords,
            int sourceReadPageBytes,
            Clock clock,
            Executor callbackExecutor) {
        this.cluster = requireText(cluster, "cluster");
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.identities = Objects.requireNonNull(identities, "identities");
        this.pins = Objects.requireNonNull(pins, "pins");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        if (sourceReadPageRecords <= 0 || sourceReadPageRecords > 65_536) {
            throw new IllegalArgumentException("sourceReadPageRecords must be in [1, 65536]");
        }
        if (sourceReadPageBytes < 64 * 1024 || sourceReadPageBytes > 64 * 1024 * 1024) {
            throw new IllegalArgumentException("sourceReadPageBytes must be in [64 KiB, 64 MiB]");
        }
        this.sourceReadPageRecords = sourceReadPageRecords;
        this.sourceReadPageBytes = sourceReadPageBytes;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
    }

    @Override
    public CompletableFuture<ExactSourceRead> read(
            SourceGeneration expected,
            ReadOptions options) {
        try {
            SourceGeneration source = Objects.requireNonNull(expected, "expected");
            ReadOptions exactOptions = Objects.requireNonNull(options, "options");
            if (!(source.readTarget() instanceof ObjectSliceReadTarget target)) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.UNSUPPORTED_READ_TARGET,
                        false,
                        "F4 exact source reads require an object-slice target"));
            }
            ResolvedRange resolved = resolved(source);
            // Fails before pin admission when the exact target has no registered physical reader.
            dispatcher.reservationBytes(List.of(resolved));
            long maximumReadDeadlineMillis = deadlineMillis(clock.millis(), exactOptions.timeout());
            long deadlineNanos = deadlineNanos(exactOptions.timeout());
            return revalidate(source)
                    .thenCompose(ignored -> identities.resolve(target, source.view()))
                    .thenCompose(identity -> pins.acquire(
                            identity,
                            maximumReadDeadlineMillis,
                            () -> revalidate(source)))
                    .thenCompose(lease -> createRead(
                            source, resolved, exactOptions, deadlineNanos, lease));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<ExactSourceRead> createRead(
            SourceGeneration source,
            ResolvedRange resolved,
            ReadOptions options,
            long deadlineNanos,
            ObjectReadLease lease) {
        try {
            return CompletableFuture.completedFuture(new Session(
                    source, resolved, options, deadlineNanos, lease));
        } catch (Throwable failure) {
            return lease.release().handle((ignored, releaseFailure) -> {
                if (releaseFailure != null) {
                    failure.addSuppressed(unwrap(releaseFailure));
                }
                throw new CompletionException(failure);
            });
        }
    }

    private CompletableFuture<Void> revalidate(SourceGeneration expected) {
        return generations.getCandidate(
                        cluster,
                        streamId,
                        expected.view(),
                        expected.range().endOffset(),
                        expected.generation())
                .thenAccept(actual -> {
                    if (actual.isEmpty()
                            || !MaterializationSourceMapper.matchesExactSource(
                                    actual.orElseThrow(), streamId, expected)) {
                        throw sourceChanged("task-frozen materialization source changed");
                    }
                });
    }

    private static ResolvedRange resolved(SourceGeneration source) {
        return new ResolvedRange(
                source.range(),
                source.generation(),
                source.readTarget(),
                source.payloadFormat(),
                source.recordCount(),
                source.entryCount(),
                source.logicalBytes(),
                source.schemaRefs(),
                source.projectionRef(),
                source.commitVersion());
    }

    private final class Session implements ExactSourceRead {
        private final SourceGeneration source;
        private final ResolvedRange resolved;
        private final ReadOptions options;
        private final long deadlineNanos;
        private final ObjectReadLease lease;
        private final SerialExecutor serial = new SerialExecutor(callbackExecutor);
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private final AtomicBoolean cancellationRequested = new AtomicBoolean();
        private final CompletableFuture<ExactSourceReadSummary> completion = new CompletableFuture<>();
        private final ArrayDeque<ReadBatch> page = new ArrayDeque<>();
        private final MessageDigest payloadDigest = sha256();

        private Flow.Subscriber<? super ReadBatch> subscriber;
        private CompletableFuture<WalReadResult> inFlight;
        private long demand;
        private long cursor;
        private int records;
        private int entries;
        private long logicalBytes;
        private boolean terminal;

        private Session(
                SourceGeneration source,
                ResolvedRange resolved,
                ReadOptions options,
                long deadlineNanos,
                ObjectReadLease lease) {
            this.source = source;
            this.resolved = resolved;
            this.options = options;
            this.deadlineNanos = deadlineNanos;
            this.lease = Objects.requireNonNull(lease, "lease");
            this.cursor = source.range().startOffset();
        }

        @Override
        public SourceGeneration source() {
            return source;
        }

        @Override
        public Flow.Publisher<ReadBatch> batches() {
            return this::subscribe;
        }

        @Override
        public CompletableFuture<ExactSourceReadSummary> completion() {
            return completion;
        }

        @Override
        public void close() {
            cancellationRequested.set(true);
            submit(() -> cancel(new NereusException(
                    ErrorCode.CANCELLED,
                    false,
                    "exact source read was closed")));
        }

        private void subscribe(Flow.Subscriber<? super ReadBatch> value) {
            Objects.requireNonNull(value, "subscriber");
            if (!subscribed.compareAndSet(false, true)) {
                rejectSecondSubscriber(value);
                return;
            }
            submit(() -> {
                if (terminal) {
                    rejectSecondSubscriber(value);
                    return;
                }
                subscriber = value;
                try {
                    value.onSubscribe(new Flow.Subscription() {
                        @Override
                        public void request(long count) {
                            submit(() -> requestOnSerial(count));
                        }

                        @Override
                        public void cancel() {
                            cancellationRequested.set(true);
                            submit(() -> Session.this.cancel(new NereusException(
                                    ErrorCode.CANCELLED,
                                    false,
                                    "exact source subscriber cancelled")));
                        }
                    });
                } catch (Throwable failure) {
                    fail(failure, false);
                }
            });
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
            drain();
        }

        private void drain() {
            if (terminal || subscriber == null) {
                return;
            }
            if (cancellationRequested.get()) {
                cancel(new NereusException(
                        ErrorCode.CANCELLED, false, "exact source subscriber cancelled"));
                return;
            }
            while (demand > 0
                    && !page.isEmpty()
                    && !terminal
                    && !cancellationRequested.get()) {
                ReadBatch batch = page.removeFirst();
                try {
                    account(batch);
                    demand--;
                    subscriber.onNext(batch);
                } catch (Throwable failure) {
                    fail(failure, false);
                    return;
                }
            }
            if (cancellationRequested.get() && !terminal) {
                cancel(new NereusException(
                        ErrorCode.CANCELLED, false, "exact source subscriber cancelled"));
                return;
            }
            if (terminal || !page.isEmpty() || inFlight != null) {
                return;
            }
            if (cursor == source.range().endOffset()) {
                succeed();
                return;
            }
            if (demand > 0) {
                readPage();
            }
        }

        private void readPage() {
            int remainingRecords = Math.toIntExact(source.range().endOffset() - cursor);
            int pageRecords = Math.min(
                    Math.min(options.maxRecords(), sourceReadPageRecords), remainingRecords);
            int pageBytes = Math.min(options.maxBytes(), sourceReadPageBytes);
            ReadOptions pageOptions;
            try {
                pageOptions = new ReadOptions(
                        pageRecords,
                        pageBytes,
                        options.isolation(),
                        remaining());
                inFlight = dispatcher.read(
                        streamId,
                        cursor,
                        List.of(resolved),
                        pageOptions);
            } catch (Throwable failure) {
                fail(failure, true);
                return;
            }
            CompletableFuture<WalReadResult> exactFuture = inFlight;
            exactFuture.whenComplete((result, failure) -> submit(() -> {
                if (terminal || inFlight != exactFuture) {
                    return;
                }
                inFlight = null;
                if (failure != null) {
                    fail(unwrap(failure), true);
                    return;
                }
                try {
                    List<ReadBatch> batches = Objects.requireNonNull(result, "read result").batches();
                    validatePage(batches, pageOptions);
                    page.addAll(batches);
                    drain();
                } catch (Throwable validationFailure) {
                    fail(validationFailure, true);
                }
            }));
        }

        private void validatePage(List<ReadBatch> batches, ReadOptions pageOptions) {
            if (batches.isEmpty()) {
                throw sourceChanged("exact physical target ended before its frozen coverage");
            }
            long pageCursor = cursor;
            long pageRecords = 0;
            long pageBytes = 0;
            ObjectSliceReadTarget target = (ObjectSliceReadTarget) source.readTarget();
            for (ReadBatch batch : batches) {
                Objects.requireNonNull(batch, "read batch");
                if (batch.range().startOffset() != pageCursor
                        || batch.range().endOffset() > source.range().endOffset()
                        || batch.payloadFormat() != source.payloadFormat()
                        || !batch.schemaRefs().equals(source.schemaRefs())
                        || !batch.projectionRef().equals(source.projectionRef())
                        || !batch.sourceObjectId().equals(target.objectId())
                        || !batch.entryIndexRef().equals(target.entryIndexRef())) {
                    throw outputInvariant("exact source reader returned a non-canonical batch");
                }
                pageCursor = batch.range().endOffset();
                pageRecords = Math.addExact(pageRecords, batch.range().recordCount());
                pageBytes = Math.addExact(pageBytes, batch.payload().length);
            }
            if (pageRecords > pageOptions.maxRecords() || pageBytes > pageOptions.maxBytes()) {
                throw outputInvariant("exact source reader exceeded its physical page limits");
            }
        }

        private void account(ReadBatch batch) {
            if (batch.range().startOffset() != cursor) {
                throw outputInvariant("exact source publisher lost dense offset ordering");
            }
            byte[] payload = batch.payload();
            byte[] header = ByteBuffer.allocate(Long.BYTES * 2 + Integer.BYTES)
                    .putLong(batch.range().startOffset())
                    .putLong(batch.range().endOffset())
                    .putInt(payload.length)
                    .array();
            payloadDigest.update(header);
            payloadDigest.update(payload);
            cursor = batch.range().endOffset();
            records = Math.addExact(records, Math.toIntExact(batch.range().recordCount()));
            entries = Math.addExact(entries, 1);
            logicalBytes = Math.addExact(logicalBytes, payload.length);
        }

        private void succeed() {
            if (records != source.recordCount()
                    || entries != source.entryCount()
                    || logicalBytes != source.logicalBytes()) {
                fail(outputInvariant("exact source completion accounting differs from the task snapshot"), true);
                return;
            }
            terminal = true;
            ExactSourceReadSummary summary = new ExactSourceReadSummary(
                    source.range(),
                    records,
                    entries,
                    logicalBytes,
                    new Checksum(ChecksumType.SHA256, hex(payloadDigest.digest())));
            release().whenComplete((ignored, failure) -> {
                if (failure != null) {
                    Throwable exact = unwrap(failure);
                    completion.completeExceptionally(exact);
                    signalError(exact);
                    return;
                }
                completion.complete(summary);
                Flow.Subscriber<? super ReadBatch> value = subscriber;
                if (value != null) {
                    try {
                        value.onComplete();
                    } catch (Throwable ignoredSignalFailure) {
                        // Subscriber callback failures do not change an already verified/released source read.
                    }
                }
            });
        }

        private void fail(Throwable failure, boolean signal) {
            if (terminal) {
                return;
            }
            terminal = true;
            CompletableFuture<WalReadResult> current = inFlight;
            inFlight = null;
            if (current != null) {
                current.cancel(true);
            }
            Throwable exact = unwrap(failure);
            release().whenComplete((ignored, releaseFailure) -> {
                if (releaseFailure != null) {
                    exact.addSuppressed(unwrap(releaseFailure));
                }
                completion.completeExceptionally(exact);
                if (signal) {
                    signalError(exact);
                }
            });
        }

        private void cancel(Throwable failure) {
            fail(failure, false);
        }

        private CompletableFuture<Void> release() {
            try {
                return Objects.requireNonNull(lease.release(), "reader lease release future");
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }

        private void signalError(Throwable failure) {
            Flow.Subscriber<? super ReadBatch> value = subscriber;
            if (value == null) {
                return;
            }
            try {
                value.onError(failure);
            } catch (Throwable ignoredSignalFailure) {
                // The completion future remains the authoritative terminal result.
            }
        }

        private Duration remaining() {
            long nanos = deadlineNanos == Long.MAX_VALUE
                    ? Long.MAX_VALUE
                    : deadlineNanos - System.nanoTime();
            if (nanos <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "exact source read deadline expired");
            }
            return Duration.ofNanos(nanos);
        }

        private void submit(Runnable action) {
            try {
                serial.execute(action);
            } catch (RejectedExecutionException failure) {
                if (!terminal) {
                    fail(new NereusException(
                            ErrorCode.STORAGE_CLOSED,
                            false,
                            "exact source callback executor rejected admitted work",
                            failure), true);
                }
            }
        }
    }

    private static void rejectSecondSubscriber(Flow.Subscriber<? super ReadBatch> subscriber) {
        try {
            subscriber.onSubscribe(NoopSubscription.INSTANCE);
            subscriber.onError(new IllegalStateException(
                    "exact source publisher permits exactly one subscriber"));
        } catch (Throwable ignored) {
            // No state is owned by a rejected subscriber.
        }
    }

    private static long addDemand(long current, long requested) {
        long result = current + requested;
        return result < 0 ? Long.MAX_VALUE : result;
    }

    private static long deadlineMillis(long nowMillis, Duration timeout) {
        long timeoutMillis;
        try {
            timeoutMillis = Math.max(1, timeout.toMillis());
        } catch (ArithmeticException overflow) {
            timeoutMillis = Long.MAX_VALUE;
        }
        return timeoutMillis == Long.MAX_VALUE || nowMillis > Long.MAX_VALUE - timeoutMillis
                ? Long.MAX_VALUE
                : nowMillis + timeoutMillis;
    }

    private static long deadlineNanos(Duration timeout) {
        long now = System.nanoTime();
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        return timeoutNanos == Long.MAX_VALUE || now > Long.MAX_VALUE - timeoutNanos
                ? Long.MAX_VALUE
                : now + timeoutNanos;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String hex(byte[] bytes) {
        return java.util.HexFormat.of().formatHex(bytes);
    }

    private static NereusException sourceChanged(String message) {
        return new MaterializationExecutionException(
                TaskFailureClass.SOURCE_CHANGED,
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                message);
    }

    private static NereusException outputInvariant(String message) {
        return new MaterializationExecutionException(
                TaskFailureClass.OUTPUT_INVARIANT,
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message);
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

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
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

    private static final class SerialExecutor implements Executor {
        private final Executor delegate;
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        private Runnable active;

        private SerialExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void execute(Runnable command) {
            tasks.addLast(() -> {
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
            active = tasks.pollFirst();
            if (active != null) {
                delegate.execute(active);
            }
        }
    }
}
