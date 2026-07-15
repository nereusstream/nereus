/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolveResult;
import com.nereusstream.api.ResolvedObjectRange;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.wal.object.ObjectWalReaderAdapter;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import com.nereusstream.objectstore.wal.WalObjectReader;
import com.nereusstream.objectstore.wal.WalReadResult;
import com.nereusstream.objectstore.wal.WalSliceReadStats;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ReadCoordinator implements StreamViewReader {
    private final StreamStorageConfig config;
    private final ReadResolver resolver;
    private final PinnedGenerationResolver generationResolver;
    private final ReadTargetDispatcher targetDispatcher;
    private final GenerationReadFailureHandler generationFailureHandler;
    private final GenerationReadRetryPolicy generationRetryPolicy;
    private final ReadResourceLimiter resourceLimiter;
    private final ReadMetricsObserver observer;
    private final Executor callbackExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ReadCoordinator(
            StreamStorageConfig config,
            ReadResolver resolver,
            WalObjectReader walObjectReader,
            ReadMetricsObserver observer,
            Executor callbackExecutor) {
        this.config = Objects.requireNonNull(config, "config");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        ObjectWalReaderAdapter objectReader = new ObjectWalReaderAdapter(
                Objects.requireNonNull(walObjectReader, "walObjectReader"));
        PrimaryWalRegistry registry = new PrimaryWalRegistry(List.of(), List.of(objectReader));
        registry.readerRegistry().require(ObjectWalReaderAdapter.KEY);
        this.generationResolver = null;
        this.targetDispatcher = new ReadTargetDispatcher(registry);
        this.generationFailureHandler = GenerationReadFailureHandler.noOp();
        this.generationRetryPolicy = GenerationReadRetryPolicy.defaults();
        this.observer = Objects.requireNonNull(observer, "observer");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.resourceLimiter = new ReadResourceLimiter(
                config.maxConcurrentObjectReads(), config.maxReadBufferBytes());
    }

    /** F4 constructor using authoritative generation resolution and exact target readers. */
    public ReadCoordinator(
            StreamStorageConfig config,
            ReadResolver resolver,
            GenerationReadResolver generationResolver,
            ReadTargetReaderRegistry readers,
            GenerationReadFailureHandler generationFailureHandler,
            ReadMetricsObserver observer,
            Executor callbackExecutor) {
        this(
                config,
                resolver,
                generationResolver,
                readers,
                generationFailureHandler,
                GenerationReadRetryPolicy.defaults(),
                observer,
                callbackExecutor);
    }

    /** F4 constructor with an explicit transient-read retry bound before same-view fallback. */
    public ReadCoordinator(
            StreamStorageConfig config,
            ReadResolver resolver,
            GenerationReadResolver generationResolver,
            ReadTargetReaderRegistry readers,
            GenerationReadFailureHandler generationFailureHandler,
            GenerationReadRetryPolicy generationRetryPolicy,
            ReadMetricsObserver observer,
            Executor callbackExecutor) {
        this(
                config,
                resolver,
                Objects.requireNonNull(generationResolver, "generationResolver")::resolve,
                readers,
                generationFailureHandler,
                generationRetryPolicy,
                observer,
                callbackExecutor);
    }

    ReadCoordinator(
            StreamStorageConfig config,
            ReadResolver resolver,
            PinnedGenerationResolver generationResolver,
            ReadTargetReaderRegistry readers,
            GenerationReadFailureHandler generationFailureHandler,
            ReadMetricsObserver observer,
            Executor callbackExecutor) {
        this(
                config,
                resolver,
                generationResolver,
                readers,
                generationFailureHandler,
                GenerationReadRetryPolicy.defaults(),
                observer,
                callbackExecutor);
    }

    ReadCoordinator(
            StreamStorageConfig config,
            ReadResolver resolver,
            PinnedGenerationResolver generationResolver,
            ReadTargetReaderRegistry readers,
            GenerationReadFailureHandler generationFailureHandler,
            GenerationReadRetryPolicy generationRetryPolicy,
            ReadMetricsObserver observer,
            Executor callbackExecutor) {
        this.config = Objects.requireNonNull(config, "config");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.generationResolver = Objects.requireNonNull(generationResolver, "generationResolver");
        this.targetDispatcher = new ReadTargetDispatcher(
                Objects.requireNonNull(readers, "readers"));
        this.generationFailureHandler = Objects.requireNonNull(
                generationFailureHandler, "generationFailureHandler");
        this.generationRetryPolicy = Objects.requireNonNull(
                generationRetryPolicy, "generationRetryPolicy");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.resourceLimiter = new ReadResourceLimiter(
                config.maxConcurrentObjectReads(), config.maxReadBufferBytes());
    }

    public CompletableFuture<ResolveResult> resolve(
            StreamId streamId,
            long startOffset,
            ResolveOptions options) {
        CancelAwareFuture<ResolveResult> result = new CancelAwareFuture<>();
        if (closed.get()) {
            result.completeExceptionally(
                    new NereusException(ErrorCode.STORAGE_CLOSED, false, "stream storage is closed"));
            return result;
        }
        if (streamId == null || options == null) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "streamId and resolve options are required"));
            return result;
        }
        ReadOperationDeadline deadline = new ReadOperationDeadline(config.readTimeout());
        result.onCancel(deadline::cancel);
        CompletableFuture<ResolveResult> pipeline;
        try {
            pipeline = resolver.resolve(streamId, startOffset, options, deadline)
                    .thenApply(ReadResolver.Resolution::result);
        } catch (RuntimeException e) {
            pipeline = CompletableFuture.failedFuture(e);
        }
        completeFrom(result, pipeline);
        return result;
    }

    public CompletableFuture<ReadResult> read(
            StreamId streamId,
            long startOffset,
            ReadOptions options) {
        if (generationResolver != null) {
            CancelAwareFuture<ReadResult> result = new CancelAwareFuture<>();
            CompletableFuture<ViewReadResult> viewRead = read(
                    streamId, startOffset, ReadView.COMMITTED, options);
            result.onCancel(() -> viewRead.cancel(true));
            completeFrom(result, viewRead.thenApply(ViewReadResult::result));
            return result;
        }
        CancelAwareFuture<ReadResult> result = new CancelAwareFuture<>();
        if (closed.get()) {
            result.completeExceptionally(
                    new NereusException(ErrorCode.STORAGE_CLOSED, false, "stream storage is closed"));
            return result;
        }
        if (options == null || streamId == null) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "streamId and read options are required"));
            return result;
        }
        if (startOffset < 0) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "startOffset must be non-negative"));
            return result;
        }
        if (options.isolation() != ReadIsolation.COMMITTED) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "Phase 1 supports only committed reads"));
            return result;
        }
        Duration effectiveTimeout = options.timeout().compareTo(config.readTimeout()) <= 0
                ? options.timeout()
                : config.readTimeout();
        ReadOperationDeadline deadline = new ReadOperationDeadline(effectiveTimeout);
        result.onCancel(deadline::cancel);
        ResolveOptions resolveOptions = new ResolveOptions(
                config.maxResolveRanges(), true, true);
        CompletableFuture<ReadResult> pipeline = resolver
                .resolve(streamId, startOffset, resolveOptions, deadline)
                .thenComposeAsync(resolution -> readResolution(
                        streamId, startOffset, options, resolution, deadline, true), callbackExecutor);
        completeFrom(result, pipeline);
        return result;
    }

    @Override
    public CompletableFuture<ViewReadResult> read(
            StreamId streamId,
            long startOffset,
            ReadView view,
            ReadOptions options) {
        CancelAwareFuture<ViewReadResult> result = new CancelAwareFuture<>();
        if (generationResolver == null) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.UNSUPPORTED_READ_TARGET,
                    false,
                    "semantic-view reads require the F4 generation resolver"));
            return result;
        }
        if (closed.get()) {
            result.completeExceptionally(
                    new NereusException(ErrorCode.STORAGE_CLOSED, false, "stream storage is closed"));
            return result;
        }
        if (streamId == null || view == null || options == null || startOffset < 0) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "streamId, view, valid startOffset and read options are required"));
            return result;
        }
        if (options.isolation() != ReadIsolation.COMMITTED) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "semantic-view reads require committed isolation"));
            return result;
        }
        Duration effectiveTimeout = options.timeout().compareTo(config.readTimeout()) <= 0
                ? options.timeout()
                : config.readTimeout();
        ReadOperationDeadline deadline = new ReadOperationDeadline(effectiveTimeout);
        result.onCancel(deadline::cancel);
        CompletableFuture<ViewReadResult> pipeline = readGeneration(
                streamId,
                startOffset,
                view,
                options,
                deadline,
                new LinkedHashSet<>(),
                new HashMap<>());
        completeFrom(result, pipeline);
        return result;
    }

    private CompletableFuture<ViewReadResult> readGeneration(
            StreamId streamId,
            long startOffset,
            ReadView view,
            ReadOptions options,
            ReadOperationDeadline deadline,
            Set<GenerationReadCandidate> excludedCandidates,
            Map<GenerationReadCandidate, Integer> transientRetries) {
        return generationResolver.resolve(
                        streamId,
                        startOffset,
                        view,
                        deadline,
                        true,
                        excludedCandidates)
                .thenCompose(optional -> optional
                        .map(pinned -> readPinned(
                                streamId,
                                startOffset,
                                view,
                                options,
                                deadline,
                                pinned,
                                excludedCandidates,
                                transientRetries))
                        .orElseGet(() -> CompletableFuture.completedFuture(new ViewReadResult(
                                view,
                                new ReadResult(streamId, startOffset, startOffset, List.of(), true),
                                startOffset))));
    }

    private CompletableFuture<ViewReadResult> readPinned(
            StreamId streamId,
            long startOffset,
            ReadView view,
            ReadOptions options,
            ReadOperationDeadline deadline,
            PinnedResolvedRange pinned,
            Set<GenerationReadCandidate> excludedCandidates,
            Map<GenerationReadCandidate, Integer> transientRetries) {
        CompletableFuture<ReadResult> read = readRanges(
                streamId,
                startOffset,
                options,
                List.of(pinned.resolvedRange()),
                deadline);
        return releaseAfter(read, pinned).handle((value, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(new ViewReadResult(
                        view,
                        value,
                        view == ReadView.COMMITTED
                                ? value.nextOffset()
                                : pinned.resolvedRange().offsetRange().endOffset()));
            }
            Throwable cause = unwrap(failure);
            if (!isObjectReadFailure(cause)) {
                return CompletableFuture.<ViewReadResult>failedFuture(cause);
            }
            if (isRetriableTransientRead(cause)) {
                int completedRetries = transientRetries.getOrDefault(pinned.candidate(), 0);
                if (generationRetryPolicy.retryAfter(completedRetries)) {
                    Map<GenerationReadCandidate, Integer> nextRetries =
                            new HashMap<>(transientRetries);
                    nextRetries.put(pinned.candidate(), completedRetries + 1);
                    return readGeneration(
                            streamId,
                            startOffset,
                            view,
                            options,
                            deadline,
                            excludedCandidates,
                            nextRetries);
                }
            }
            Set<GenerationReadCandidate> nextExclusions = new LinkedHashSet<>(excludedCandidates);
            nextExclusions.add(pinned.candidate());
            return generationFailureHandler.handle(streamId, pinned.candidate(), cause)
                    .exceptionally(ignored -> null)
                    .thenCompose(ignored -> readGeneration(
                            streamId,
                            startOffset,
                            view,
                            options,
                            deadline,
                            nextExclusions,
                            transientRetries));
        }).thenCompose(value -> value);
    }

    private static <T> CompletableFuture<T> releaseAfter(
            CompletableFuture<T> operation,
            PinnedResolvedRange pinned) {
        return operation.handle((value, failure) -> pinned.release().handle((ignored, releaseFailure) -> {
            if (failure == null && releaseFailure == null) {
                return value;
            }
            Throwable cause = failure == null ? unwrap(releaseFailure) : unwrap(failure);
            if (failure != null && releaseFailure != null) {
                cause.addSuppressed(unwrap(releaseFailure));
            }
            throw new CompletionException(cause);
        })).thenCompose(value -> value);
    }

    /** Invalidates every cached offset-index range for one stream. */
    public void invalidate(StreamId streamId) {
        resolver.invalidate(Objects.requireNonNull(streamId, "streamId"));
    }

    private CompletableFuture<ReadResult> readResolution(
            StreamId streamId,
            long startOffset,
            ReadOptions options,
            ReadResolver.Resolution resolution,
            ReadOperationDeadline deadline,
            boolean allowCacheRefresh) {
        if (resolution.result().ranges().isEmpty()) {
            return CompletableFuture.completedFuture(
                    new ReadResult(streamId, startOffset, startOffset, List.of(), true));
        }
        CompletableFuture<ReadResult> attempted = readRanges(
                streamId, startOffset, options, resolution.result().ranges(), deadline);
        if (!allowCacheRefresh || !resolution.cacheUsed()) {
            return attempted;
        }
        return attempted.exceptionallyCompose(error -> {
            Throwable cause = unwrap(error);
            if (!isObjectReadFailure(cause)) {
                return CompletableFuture.failedFuture(cause);
            }
            resolver.invalidate(streamId);
            ResolveOptions noCache = new ResolveOptions(config.maxResolveRanges(), false, true);
            return resolver.resolve(streamId, startOffset, noCache, deadline)
                    .thenCompose(fresh -> {
                        if (fresh.result().ranges().equals(resolution.result().ranges())) {
                            return CompletableFuture.failedFuture(cause);
                        }
                        return readResolution(
                                streamId, startOffset, options, fresh, deadline, false);
                    });
        });
    }

    private CompletableFuture<ReadResult> readRanges(
            StreamId streamId,
            long startOffset,
            ReadOptions options,
            List<ResolvedRange> ranges,
            ReadOperationDeadline deadline) {
        long reservationBytes;
        try {
            reservationBytes = targetDispatcher.reservationBytes(ranges);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        ReadResourceLimiter.Reservation reservation;
        try {
            deadline.check("reserve read resources");
            reservation = resourceLimiter.reserve(reservationBytes);
        } catch (NereusException e) {
            if (e.code() == ErrorCode.BACKPRESSURE_REJECTED) {
                observe(() -> observer.onBackpressureRejected(reservationBytes));
            }
            return CompletableFuture.failedFuture(e);
        }
        ReadOptions boundedOptions;
        try {
            boundedOptions = new ReadOptions(
                    options.maxRecords(),
                    options.maxBytes(),
                    options.isolation(),
                    deadline.remaining());
        } catch (RuntimeException e) {
            reservation.close();
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<WalReadResult> walRead = deadline.bound(
                () -> targetDispatcher.read(streamId, startOffset, ranges, boundedOptions),
                "read resolved WAL ranges");
        return walRead.whenComplete((ignored, error) -> reservation.close())
                .thenApplyAsync(result -> buildReadResult(
                        streamId, startOffset, options, ranges, result), callbackExecutor);
    }

    private ReadResult buildReadResult(
            StreamId streamId,
            long startOffset,
            ReadOptions options,
            List<ResolvedRange> ranges,
            WalReadResult result) {
        validateReadAccounting(ranges, result);
        result.sliceStats().forEach(stats -> observe(() -> observer.onSliceRead(
                stats.fullSlicePayloadBytes(),
                stats.entryIndexBytes(),
                stats.returnedPayloadBytes())));
        if (result.batches().isEmpty()) {
            throw new NereusException(
                    ErrorCode.READ_RESOLUTION_FAILED,
                    false,
                    "resolved WAL ranges produced no readable entry");
        }
        long expectedOffset = startOffset;
        long payloadBytes = 0;
        long records = 0;
        try {
            for (ReadBatch batch : result.batches()) {
                if (batch.range().startOffset() != expectedOffset) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "WAL reader returned a non-dense logical range");
                }
                expectedOffset = batch.range().endOffset();
                payloadBytes = Math.addExact(payloadBytes, batch.payload().length);
                records = Math.addExact(records, batch.range().recordCount());
            }
        } catch (ArithmeticException e) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "WAL reader result accounting overflows",
                    e);
        }
        if (payloadBytes > options.maxBytes() || records > options.maxRecords()) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "WAL reader exceeded caller read limits");
        }
        return new ReadResult(streamId, startOffset, expectedOffset, result.batches(), false);
    }

    private static void validateReadAccounting(
            List<ResolvedRange> ranges,
            WalReadResult result) {
        Set<RangeIdentity> expectedRanges = new HashSet<>();
        ranges.stream().map(ResolvedObjectRange::from).forEach(range -> expectedRanges.add(new RangeIdentity(
                range.objectId(), range.objectOffset(), range.objectLength(), range.entryIndexRef().length())));
        Set<RangeIdentity> observedRanges = new HashSet<>();
        Set<com.nereusstream.api.ObjectId> observedObjects = new HashSet<>();
        long statsReturned = 0;
        long batchBytes = 0;
        try {
            for (WalSliceReadStats stats : result.sliceStats()) {
                RangeIdentity identity = new RangeIdentity(
                        stats.objectId(),
                        stats.objectOffset(),
                        stats.fullSlicePayloadBytes(),
                        stats.entryIndexBytes());
                if (!expectedRanges.contains(identity)) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "WAL reader reported accounting for an unknown resolved range");
                }
                if (!observedRanges.add(identity)) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "WAL reader reported duplicate accounting for one resolved range");
                }
                observedObjects.add(stats.objectId());
                stats.amplificationBytes();
                statsReturned = Math.addExact(statsReturned, stats.returnedPayloadBytes());
            }
            for (ReadBatch batch : result.batches()) {
                if (!observedObjects.contains(batch.sourceObjectId())) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "WAL reader omitted read accounting for a returned batch");
                }
                batchBytes = Math.addExact(batchBytes, batch.payload().length);
            }
        } catch (ArithmeticException e) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "WAL reader accounting overflows",
                    e);
        }
        if (statsReturned != batchBytes) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "WAL reader returned-byte accounting does not match batches");
        }
    }

    private static boolean isObjectReadFailure(Throwable cause) {
        return cause instanceof NereusException nereus
                && (nereus.code() == ErrorCode.OBJECT_NOT_FOUND
                        || nereus.code() == ErrorCode.OBJECT_READ_FAILED
                        || nereus.code() == ErrorCode.OBJECT_CHECKSUM_MISMATCH);
    }

    private static boolean isRetriableTransientRead(Throwable cause) {
        return cause instanceof NereusException nereus
                && nereus.code() == ErrorCode.OBJECT_READ_FAILED
                && nereus.retriable();
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> void completeFrom(
            CompletableFuture<T> target,
            CompletableFuture<T> source) {
        source.whenComplete((value, error) -> {
            if (error == null) {
                target.complete(value);
            } else {
                target.completeExceptionally(unwrap(error));
            }
        });
    }

    private static void observe(Runnable callback) {
        try {
            callback.run();
        } catch (RuntimeException ignored) {
            // Metrics callbacks cannot alter read correctness.
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            resolver.close();
        }
    }

    private static final class CancelAwareFuture<T> extends CompletableFuture<T> {
        private Runnable cancellation = () -> {
        };

        synchronized void onCancel(Runnable action) {
            cancellation = Objects.requireNonNull(action, "action");
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (isDone()) {
                return false;
            }
            Runnable action;
            synchronized (this) {
                action = cancellation;
            }
            action.run();
            return true;
        }
    }

    private record RangeIdentity(
            com.nereusstream.api.ObjectId objectId,
            long objectOffset,
            long objectLength,
            long entryIndexLength) {
    }

    @FunctionalInterface
    interface PinnedGenerationResolver {
        CompletableFuture<java.util.Optional<PinnedResolvedRange>> resolve(
                StreamId streamId,
                long offset,
                ReadView view,
                ReadOperationDeadline deadline,
                boolean allowRepair,
                Set<GenerationReadCandidate> excludedCandidates);
    }
}
