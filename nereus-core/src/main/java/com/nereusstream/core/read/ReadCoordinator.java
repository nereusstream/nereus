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
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolveResult;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.wal.object.ObjectWalReaderAdapter;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import com.nereusstream.objectstore.wal.WalObjectReader;
import java.time.Duration;
import java.util.HashMap;
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

    /** Provider-neutral generation-zero constructor used by non-Object primary WAL profiles. */
    public ReadCoordinator(
            StreamStorageConfig config,
            ReadResolver resolver,
            ReadTargetReaderRegistry readers,
            ReadMetricsObserver observer,
            Executor callbackExecutor) {
        this.config = Objects.requireNonNull(config, "config");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.generationResolver = null;
        this.targetDispatcher = new ReadTargetDispatcher(
                Objects.requireNonNull(readers, "readers"));
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

    /** Executes the public ranged-entry and semantic-view read contract. */
    public CompletableFuture<SemanticReadResult> read(
            StreamId streamId,
            ReadRequest request) {
        if (request != null && request.isLegacyEquivalent() && generationResolver == null) {
            return read(streamId, request.startOffset(), request.options())
                    .thenApply(value -> SemanticReadResult.forRequest(
                            request, value, value.nextOffset()));
        }
        CancelAwareFuture<SemanticReadResult> result = new CancelAwareFuture<>();
        if (closed.get()) {
            result.completeExceptionally(
                    new NereusException(ErrorCode.STORAGE_CLOSED, false, "stream storage is closed"));
            return result;
        }
        if (streamId == null || request == null) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "streamId and read request are required"));
            return result;
        }
        if (request.options().isolation() != ReadIsolation.COMMITTED) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "semantic reads require committed isolation"));
            return result;
        }
        if (generationResolver == null && request.view() != ReadView.COMMITTED) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                    false,
                    "the configured storage does not expose higher-generation semantic views"));
            return result;
        }
        Duration effectiveTimeout = request.options().timeout().compareTo(config.readTimeout()) <= 0
                ? request.options().timeout()
                : config.readTimeout();
        ReadOperationDeadline deadline = new ReadOperationDeadline(effectiveTimeout);
        result.onCancel(deadline::cancel);
        CompletableFuture<SemanticReadResult> pipeline;
        if (generationResolver == null) {
            ResolveOptions resolveOptions = new ResolveOptions(
                    config.maxResolveRanges(), true, true);
            pipeline = resolver.resolve(streamId, request.startOffset(), resolveOptions, deadline)
                    .thenComposeAsync(resolution -> readSemanticResolution(
                            streamId, request, resolution, deadline, true), callbackExecutor);
        } else {
            pipeline = readSemanticGeneration(
                    streamId,
                    request,
                    deadline,
                    new LinkedHashSet<>(),
                    new HashMap<>());
        }
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
        if (streamId == null || view == null || options == null || startOffset < 0) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "streamId, view, valid startOffset and read options are required"));
            return result;
        }
        CompletableFuture<SemanticReadResult> semantic = read(
                streamId,
                new ReadRequest(
                        startOffset,
                        view,
                        ReadBoundaryMode.EXACT_START,
                        FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                        options));
        result.onCancel(() -> semantic.cancel(true));
        CompletableFuture<ViewReadResult> pipeline = semantic.thenApply(value ->
                new ViewReadResult(
                        value.view(),
                        value.result(),
                        value.sourceCoverageEndOffset()));
        completeFrom(result, pipeline);
        return result;
    }

    private CompletableFuture<SemanticReadResult> readSemanticGeneration(
            StreamId streamId,
            ReadRequest request,
            ReadOperationDeadline deadline,
            Set<GenerationReadCandidate> excludedCandidates,
            Map<GenerationReadCandidate, Integer> transientRetries) {
        return generationResolver.resolve(
                        streamId,
                        request.startOffset(),
                        request.view(),
                        deadline,
                        true,
                        excludedCandidates)
                .thenCompose(optional -> optional
                        .map(pinned -> readSemanticPinned(
                                streamId,
                                request,
                                deadline,
                                pinned,
                                excludedCandidates,
                                transientRetries))
                        .orElseGet(() -> {
                            ReadResult empty = new ReadResult(
                                    streamId,
                                    request.startOffset(),
                                    request.startOffset(),
                                    List.of(),
                                    true);
                            return CompletableFuture.completedFuture(
                                    SemanticReadResult.forRequest(
                                            request, empty, request.startOffset()));
                        }));
    }

    private CompletableFuture<SemanticReadResult> readSemanticPinned(
            StreamId streamId,
            ReadRequest request,
            ReadOperationDeadline deadline,
            PinnedResolvedRange pinned,
            Set<GenerationReadCandidate> excludedCandidates,
            Map<GenerationReadCandidate, Integer> transientRetries) {
        CompletableFuture<ReadResult> read = readSemanticRanges(
                streamId,
                request,
                List.of(pinned.resolvedRange()),
                deadline);
        return releaseAfter(read, pinned).handle((value, failure) -> {
            if (failure == null) {
                long coverage = request.view() == ReadView.COMMITTED
                        ? value.nextOffset()
                        : pinned.resolvedRange().offsetRange().endOffset();
                return CompletableFuture.completedFuture(
                        SemanticReadResult.forRequest(request, value, coverage));
            }
            Throwable cause = unwrap(failure);
            java.util.Optional<PhysicalReadFailureKind> physicalFailure = PhysicalReadFailures.classify(cause);
            if (physicalFailure.isEmpty()) {
                return CompletableFuture.<SemanticReadResult>failedFuture(cause);
            }
            if (physicalFailure.orElseThrow() == PhysicalReadFailureKind.TRANSIENT_IO
                    && cause instanceof NereusException nereus
                    && nereus.retriable()) {
                int completedRetries = transientRetries.getOrDefault(pinned.candidate(), 0);
                if (generationRetryPolicy.retryAfter(completedRetries)) {
                    Map<GenerationReadCandidate, Integer> nextRetries =
                            new HashMap<>(transientRetries);
                    nextRetries.put(pinned.candidate(), completedRetries + 1);
                    return readSemanticGeneration(
                            streamId,
                            request,
                            deadline,
                            excludedCandidates,
                            nextRetries);
                }
            }
            Set<GenerationReadCandidate> nextExclusions = new LinkedHashSet<>(excludedCandidates);
            nextExclusions.add(pinned.candidate());
            return generationFailureHandler.handle(streamId, pinned.candidate(), cause)
                    .exceptionally(ignored -> null)
                    .thenCompose(ignored -> readSemanticGeneration(
                            streamId,
                            request,
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
            if (PhysicalReadFailures.classify(cause).isEmpty()) {
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

    private CompletableFuture<SemanticReadResult> readSemanticResolution(
            StreamId streamId,
            ReadRequest request,
            ReadResolver.Resolution resolution,
            ReadOperationDeadline deadline,
            boolean allowCacheRefresh) {
        if (resolution.result().ranges().isEmpty()) {
            ReadResult empty = new ReadResult(
                    streamId,
                    request.startOffset(),
                    request.startOffset(),
                    List.of(),
                    true);
            return CompletableFuture.completedFuture(
                    SemanticReadResult.forRequest(
                            request, empty, request.startOffset()));
        }
        CompletableFuture<SemanticReadResult> attempted = readSemanticRanges(
                        streamId,
                        request,
                        resolution.result().ranges(),
                        deadline)
                .thenApply(value -> SemanticReadResult.forRequest(
                        request, value, value.nextOffset()));
        if (!allowCacheRefresh || !resolution.cacheUsed()) {
            return attempted;
        }
        return attempted.exceptionallyCompose(error -> {
            Throwable cause = unwrap(error);
            if (PhysicalReadFailures.classify(cause).isEmpty()) {
                return CompletableFuture.failedFuture(cause);
            }
            resolver.invalidate(streamId);
            ResolveOptions noCache = new ResolveOptions(config.maxResolveRanges(), false, true);
            return resolver.resolve(streamId, request.startOffset(), noCache, deadline)
                    .thenCompose(fresh -> {
                        if (fresh.result().ranges().equals(resolution.result().ranges())) {
                            return CompletableFuture.failedFuture(cause);
                        }
                        return readSemanticResolution(
                                streamId, request, fresh, deadline, false);
                    });
        });
    }

    private CompletableFuture<ReadResult> readSemanticRanges(
            StreamId streamId,
            ReadRequest request,
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
            deadline.check("reserve semantic read resources");
            reservation = resourceLimiter.reserve(reservationBytes);
        } catch (NereusException e) {
            if (e.code() == ErrorCode.BACKPRESSURE_REJECTED) {
                observe(() -> observer.onBackpressureRejected(reservationBytes));
            }
            return CompletableFuture.failedFuture(e);
        }
        ReadRequest boundedRequest;
        try {
            ReadOptions options = request.options();
            boundedRequest = new ReadRequest(
                    request.startOffset(),
                    request.view(),
                    request.boundaryMode(),
                    request.firstEntryPolicy(),
                    new ReadOptions(
                            options.maxRecords(),
                            options.maxBytes(),
                            options.isolation(),
                            deadline.remaining()));
        } catch (RuntimeException e) {
            reservation.close();
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<PhysicalReadResult> physicalRead = deadline.bound(
                () -> targetDispatcher.read(streamId, boundedRequest, ranges),
                "read semantic resolved ranges");
        return physicalRead.whenComplete((ignored, error) -> reservation.close())
                .thenApplyAsync(result -> buildSemanticReadResult(
                        streamId, request, ranges, result), callbackExecutor);
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
        CompletableFuture<PhysicalReadResult> walRead = deadline.bound(
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
            PhysicalReadResult result) {
        ProviderNeutralReadAccounting.validate(ranges, result);
        result.rangeStats().forEach(stats -> observe(() -> observer.onSliceRead(
                stats.physicalPayloadBytesRead(),
                stats.physicalAuxiliaryBytesRead(),
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

    private ReadResult buildSemanticReadResult(
            StreamId streamId,
            ReadRequest request,
            List<ResolvedRange> ranges,
            PhysicalReadResult result) {
        ProviderNeutralReadAccounting.validate(ranges, result);
        requireExactLogicalSources(ranges, result.batches());
        result.rangeStats().forEach(stats -> observe(() -> observer.onSliceRead(
                stats.physicalPayloadBytesRead(),
                stats.physicalAuxiliaryBytesRead(),
                stats.returnedPayloadBytes())));
        if (result.batches().isEmpty()) {
            throw new NereusException(
                    ErrorCode.READ_RESOLUTION_FAILED,
                    false,
                    "resolved ranges produced no readable entry for the semantic request");
        }
        long expectedOffset = result.batches().get(0).range().startOffset();
        long payloadBytes = 0;
        long records = 0;
        try {
            for (ReadBatch batch : result.batches()) {
                if (request.view() == ReadView.COMMITTED
                        && batch.range().startOffset() != expectedOffset) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "physical reader returned a non-dense committed range");
                }
                if (request.view() == ReadView.TOPIC_COMPACTED
                        && batch.range().startOffset() < expectedOffset) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "physical reader returned overlapping compacted ranges");
                }
                expectedOffset = batch.range().endOffset();
                payloadBytes = Math.addExact(payloadBytes, batch.payload().length);
                records = Math.addExact(records, batch.range().recordCount());
            }
        } catch (ArithmeticException e) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "semantic read result accounting overflows",
                    e);
        }
        boolean exceedsLimits = payloadBytes > request.options().maxBytes()
                || records > request.options().maxRecords();
        boolean permittedFirstOverflow = request.firstEntryPolicy()
                == FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW
                && result.batches().size() == 1;
        if (exceedsLimits && !permittedFirstOverflow) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "physical reader exceeded semantic read limits");
        }
        ReadResult readResult = new ReadResult(
                streamId,
                request.startOffset(),
                expectedOffset,
                result.batches(),
                false);
        try {
            SemanticReadResult.forRequest(request, readResult, expectedOffset);
        } catch (IllegalArgumentException invalid) {
            throw new NereusException(
                    request.boundaryMode() == ReadBoundaryMode.EXACT_START
                            ? ErrorCode.OFFSET_NOT_AVAILABLE
                            : ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "physical reader violated semantic boundary requirements",
                    invalid);
        }
        return readResult;
    }

    private static void requireExactLogicalSources(
            List<ResolvedRange> ranges,
            List<ReadBatch> batches) {
        for (ReadBatch batch : batches) {
            boolean exactSource = ranges.stream().anyMatch(range ->
                    range.offsetRange().equals(batch.source().resolvedRange())
                            && range.generation() == batch.source().generation()
                            && range.commitVersion() == batch.source().commitVersion()
                            && range.readTarget().equals(batch.source().target())
                            && range.payloadFormat() == batch.payloadFormat()
                            && range.schemaRefs().equals(batch.schemaRefs())
                            && range.projectionRef().equals(batch.projectionRef())
                            && range.offsetRange().startOffset() <= batch.range().startOffset()
                            && batch.range().endOffset() <= range.offsetRange().endOffset());
            if (!exactSource) {
                throw new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "physical reader returned logical facts outside its exact resolved source");
            }
        }
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
