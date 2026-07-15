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

package com.nereusstream.core.append;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.profile.Phase15StorageProfileResolver;
import com.nereusstream.core.profile.StorageProfileResolver;
import com.nereusstream.core.recovery.AppendRecoverySearcher;
import com.nereusstream.core.recovery.AppendReplayEvidenceSource;
import com.nereusstream.core.recovery.MetadataAppendRecoverySearcher;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import com.nereusstream.core.wal.object.ObjectPreparedPrimaryAppend;
import com.nereusstream.core.wal.object.ObjectWalAppenderAdapter;
import com.nereusstream.core.wal.object.ObjectWalCommitEvidence;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StableAppendResult;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import com.nereusstream.objectstore.wal.CompressionType;
import com.nereusstream.objectstore.wal.PreparedWalObject;
import com.nereusstream.objectstore.wal.WalObjectWriter;
import com.nereusstream.objectstore.wal.WalStreamSliceInput;
import com.nereusstream.objectstore.wal.WalWriteOptions;
import com.nereusstream.objectstore.wal.WalWriteRequest;
import com.nereusstream.objectstore.wal.WalWriteResult;
import com.nereusstream.objectstore.wal.WrittenStreamSlice;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class AppendCoordinator implements AutoCloseable {
    private static final Duration ORPHAN_AUDIT_TTL = Duration.ofHours(24);

    private final StreamStorageConfig config;
    private final OxiaMetadataStore metadataStore;
    private final ObjectWalAppenderAdapter primaryAppender;
    private final AppendSessionManager sessionManager;
    private final Clock clock;
    private final Executor callbackExecutor;
    private final String writerRunIdHash;
    private final AppendResourceLimiter resourceLimiter;
    private final StableAppendCommitter stableCommitter;
    private final GenerationZeroIndexMaterializer indexMaterializer;
    private final GenerationZeroPhysicalReferencePublisher physicalReferences;
    private final AppendRecoverySearcher recoverySearcher;
    private final StorageProfileResolver profileResolver = new Phase15StorageProfileResolver();
    private final ConcurrentHashMap<StreamId, StreamLane> lanes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<AppendAttemptId, Attempt> retainedAttempts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<AppendAttemptId, TerminalAttempt> terminalAttempts = new ConcurrentHashMap<>();
    private final Semaphore retainedAttemptPermits;
    private final AtomicLong attemptSequence = new AtomicLong();
    private final ScheduledExecutorService recoveryScheduler;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycleMonitor = new Object();
    private int activeAppends;

    public AppendCoordinator(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            AppendSessionManager sessionManager,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            Clock clock,
            Executor callbackExecutor) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                sessionManager,
                physicalReferences,
                new MetadataAppendRecoverySearcher(config.cluster(), metadataStore),
                clock,
                callbackExecutor);
    }

    public AppendCoordinator(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            AppendSessionManager sessionManager,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            AppendRecoverySearcher recoverySearcher,
            Clock clock,
            Executor callbackExecutor) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        Objects.requireNonNull(walObjectWriter, "walObjectWriter");
        this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
        this.physicalReferences = Objects.requireNonNull(physicalReferences, "physicalReferences");
        this.recoverySearcher = Objects.requireNonNull(recoverySearcher, "recoverySearcher");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.writerRunIdHash = DeterministicIds.stableHashComponent(config.processRunId());
        ObjectWalAppenderAdapter objectAppender = new ObjectWalAppenderAdapter(
                config.cluster(), config.writerId(), writerRunIdHash, config.maxObjectBytes(), walObjectWriter);
        PrimaryWalRegistry registry = new PrimaryWalRegistry(List.of(objectAppender), List.of());
        this.primaryAppender = (ObjectWalAppenderAdapter) registry.requireAppender(
                com.nereusstream.api.target.ReadTargetType.OBJECT_SLICE);
        this.resourceLimiter = new AppendResourceLimiter(
                config.maxInFlightAppends(), config.maxBufferedBytes());
        this.retainedAttemptPermits = new Semaphore(config.maxRetainedAppendAttempts());
        this.stableCommitter = new MetadataStableAppendCommitter(config.cluster(), metadataStore);
        this.indexMaterializer = new MetadataGenerationZeroIndexMaterializer(config.cluster(), metadataStore);
        this.recoveryScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "nereus-append-recovery");
            thread.setDaemon(true);
            return thread;
        });
    }

    public CompletableFuture<AppendResult> append(
            StreamId streamId,
            AppendBatch batch,
            AppendOptions options) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(options, "options");
        AppendFuture result = new AppendFuture();
        Duration effectiveTimeout = options.timeout().compareTo(config.appendTimeout()) <= 0
                ? options.timeout()
                : config.appendTimeout();
        AppendDeadline deadline = new AppendDeadline(effectiveTimeout);
        result.onCancel(deadline::cancel);

        if (closed.get()) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "stream storage is closed",
                    AppendOutcome.KNOWN_NOT_COMMITTED));
            return result;
        }
        if (batch.recordCount() > config.maxAppendBatchRecords()) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "append batch exceeds maxAppendBatchRecords",
                    AppendOutcome.KNOWN_NOT_COMMITTED));
            return result;
        }
        if (!retainedAttemptPermits.tryAcquire()) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED, true,
                    "retained append-attempt capacity is exhausted", AppendOutcome.KNOWN_NOT_COMMITTED));
            return result;
        }
        AppendAttemptId attemptId;
        try {
            long sequence = attemptSequence.getAndIncrement();
            if (sequence == -1L) throw new IllegalStateException("append attempt sequence exhausted");
            attemptId = new AppendAttemptId(config.processRunId() + "/" + Long.toUnsignedString(sequence));
        } catch (RuntimeException e) {
            retainedAttemptPermits.release();
            result.completeExceptionally(new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED, false, "cannot allocate append attempt ID", e,
                    AppendOutcome.KNOWN_NOT_COMMITTED));
            return result;
        }
        Attempt attempt = new Attempt(attemptId, streamId, batch, options, deadline);
        CompletableFuture<AppendResult> pipeline = deadline.bound(
                        () -> metadataStore.getStream(config.cluster(), streamId),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "load stream profile")
                .thenApplyAsync(metadata -> validateProfile(metadata, options), callbackExecutor)
                .thenComposeAsync(ignored -> acceptAndEnqueue(attempt), callbackExecutor);
        pipeline.whenComplete((value, error) -> {
            if (error == null) {
                attempt.completeQuiescedIfNoHeadSource();
                attempt.releasePermit();
                result.complete(value);
            } else {
                NereusException failure = normalizeAppendFailure(error, attempt.outcome());
                attempt.completeQuiescedIfNoHeadSource();
                if (failure.appendAttemptId().isPresent()
                        && !failure.appendAttemptId().orElseThrow().equals(attempt.id())) {
                    attempt.releasePermit();
                    result.completeExceptionally(failure);
                    return;
                }
                if (failure.appendOutcome().orElse(AppendOutcome.KNOWN_NOT_COMMITTED)
                        == AppendOutcome.KNOWN_NOT_COMMITTED) {
                    attempt.releasePermit();
                    result.completeExceptionally(failure);
                } else {
                    retainedAttempts.put(attempt.id(), attempt);
                    NereusException exposed = failure.appendAttemptId().isPresent()
                            ? failure
                            : failure.withAppendAttemptId(attempt.id());
                    attempt.lane().suspend(exposed);
                    scheduleRecovery(attempt, config.appendRecoveryBackoffMin());
                    result.completeExceptionally(exposed);
                }
            }
        });
        return result;
    }

    private CompletableFuture<AppendResult> acceptAndEnqueue(Attempt attempt) {
        attempt.deadline().check(AppendOutcome.KNOWN_NOT_COMMITTED, "accept append");
        if (closed.get()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "stream storage is closed",
                    AppendOutcome.KNOWN_NOT_COMMITTED));
        }
        appendAccepted();
        AppendResourceLimiter.Reservation reservation;
        try {
            reservation = resourceLimiter.accept();
        } catch (RuntimeException e) {
            appendCompleted();
            throw e;
        }
        StreamLane lane = retainLane(attempt.streamId());
        try {
            if (!lane.tryAdmitAppend()) {
                reservation.close();
                appendCompleted();
                releaseLane(attempt.streamId(), lane);
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.STREAM_NOT_ACTIVE, false,
                        "stream lifecycle barrier rejects new append admission",
                        AppendOutcome.KNOWN_NOT_COMMITTED));
            }
            CompletableFuture<AppendResult> queued = lane.enqueue(
                    () -> executeAppend(lane, attempt, reservation), callbackExecutor);
            return queued.whenComplete((ignored, error) -> {
                reservation.close();
                appendCompleted();
                releaseLane(attempt.streamId(), lane);
            });
        } catch (RuntimeException e) {
            reservation.close();
            appendCompleted();
            releaseLane(attempt.streamId(), lane);
            throw e;
        }
    }

    private StreamLane retainLane(StreamId streamId) {
        return lanes.compute(streamId, (ignored, existing) -> {
            StreamLane retained = existing == null ? new StreamLane() : existing;
            retained.retain();
            return retained;
        });
    }

    private void releaseLane(StreamId streamId, StreamLane lane) {
        lanes.compute(streamId, (ignored, existing) -> {
            lane.release();
            return existing == lane && lane.removable() ? null : existing;
        });
    }

    int retainedLaneCount() {
        return lanes.size();
    }

    /** Orders a lifecycle barrier after all previously admitted local appends for the stream. */
    public <T> CompletableFuture<T> enqueueLifecycleMutation(
            StreamId streamId, Supplier<CompletableFuture<T>> operation) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(operation, "operation");
        if (closed.get()) {
            return NereusException.failedFuture(ErrorCode.STORAGE_CLOSED, false, "stream storage is closed");
        }
        StreamLane lane = retainLane(streamId);
        try {
            lane.beginLifecycleBarrier();
            CompletableFuture<T> result = lane.enqueue(() -> {
                NereusException suspended = lane.suspendedFailure();
                return suspended == null ? operation.get() : CompletableFuture.failedFuture(suspended);
            }, callbackExecutor);
            return result.whenComplete((ignored, error) -> releaseLane(streamId, lane));
        } catch (RuntimeException e) {
            releaseLane(streamId, lane);
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<AppendResult> recoverAppend(
            StreamId streamId, AppendAttemptId attemptId, AppendRecoveryOptions options) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(attemptId, "attemptId");
        Objects.requireNonNull(options, "options");
        if (closed.get()) return NereusException.failedFuture(
                ErrorCode.STORAGE_CLOSED, false, "stream storage is closed");
        evictExpiredTerminals();
        TerminalAttempt terminal = terminalAttempts.get(attemptId);
        if (terminal != null) {
            if (!terminal.streamId().equals(streamId)) return unknownAttempt();
            return terminal.result() != null
                    ? CompletableFuture.completedFuture(terminal.result())
                    : CompletableFuture.failedFuture(terminal.failure());
        }
        Attempt attempt = retainedAttempts.get(attemptId);
        if (attempt == null || !attempt.streamId().equals(streamId)) return unknownAttempt();
        attempt.markRecoveryUsed();
        startRecovery(attempt).exceptionally(error -> null);
        return callerView(attempt, attempt.terminalFuture(), options.timeout());
    }

    private CompletableFuture<AppendResult> startRecovery(Attempt attempt) {
        return attempt.singleFlight(() -> attempt.originalRunnerQuiesced()
                .thenCompose(ignored -> metadataStore.getCommittedEndOffset(config.cluster(), attempt.streamId()))
                .thenCompose(head -> {
                    long expected = attempt.commitRequest().expectedStartOffset();
                    if (head.committedEndOffset() < expected) {
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                "stream head moved behind the retained append start"));
                    }
                    if (head.committedEndOffset() == expected) {
                        return recoverProtectedAppend(attempt);
                    }
                    return searchRecovery(attempt);
                })
                .thenApply(commit -> toAppendResult(commit, attempt.slice()))
                .handle((result, error) -> finishRecovery(attempt, result, error)));
    }

    private CompletableFuture<CommittedAppend> searchRecovery(Attempt attempt) {
        int pageSize = Math.min(
                config.maxDerivedIndexRepairCommitsPerCall(),
                config.maxCommitChainScan());
        return recoverySearcher.search(
                        attempt.commitRequest(),
                        config.maxCommitChainScan(),
                        pageSize,
                        config.appendRecoveryAttemptTimeout())
                .thenCompose(search -> switch (search.status()) {
                    case FOUND -> {
                        if (search.evidenceSource().orElseThrow()
                                == AppendReplayEvidenceSource.RECOVERY_CHECKPOINT) {
                            attempt.markHeadKnownCommitted();
                            yield CompletableFuture.completedFuture(
                                    search.committedAppend().orElseThrow().committedAppend());
                        }
                        yield recoverProtectedAppend(attempt);
                    }
                    case PROVEN_NOT_COMMITTED -> CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.OFFSET_CONFLICT, false,
                            "exact append recovery proved the attempt was not committed",
                            AppendOutcome.KNOWN_NOT_COMMITTED));
                    case CONTINUE -> CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "terminal append recovery search returned a continuation",
                            AppendOutcome.MAY_HAVE_COMMITTED));
                });
    }

    private CompletableFuture<CommittedAppend> recoverProtectedAppend(Attempt attempt) {
        AppendDeadline deadline = new AppendDeadline(config.appendRecoveryAttemptTimeout());
        return deadline.bound(
                        () -> stableCommitter.prepare(attempt.commitRequest()),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "prepare stable append recovery intent")
                .thenCompose(prepared -> deadline.bound(
                        () -> physicalReferences.protectBeforeHead(
                                prepared,
                                deadline.remaining()),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "protect recovered append before head"))
                .thenCompose(protectedAppend -> {
                    attempt.markHeadSent();
                    CompletableFuture<StableAppendResult> head = stableCommitter.commit(protectedAppend);
                    attempt.trackHeadSource(head);
                    return deadline.bound(
                            () -> head,
                            AppendOutcome.KNOWN_NOT_COMMITTED,
                            AppendOutcome.MAY_HAVE_COMMITTED,
                            "commit recovered protected stream head");
                })
                .thenCompose(stable -> {
                    attempt.markHeadKnownCommitted();
                    return deadline.bound(
                            () -> indexMaterializer.materialize(stable.reachableAppend()),
                            AppendOutcome.KNOWN_COMMITTED,
                            "materialize recovered generation-zero index");
                })
                .thenCompose(materialized -> deadline.bound(
                        () -> physicalReferences.protectVisibleIndex(
                                materialized,
                                deadline.remaining()),
                        AppendOutcome.KNOWN_COMMITTED,
                        "protect recovered visible generation-zero index"))
                .thenApply(protectedIndex -> protectedIndex.materialized().committedAppend());
    }

    private AppendResult finishRecovery(Attempt attempt, AppendResult result, Throwable error) {
        if (error == null) {
            attempt.lane().advanceExpectedOffset(result.committedEndOffset());
            attempt.lane().unsuspend();
            removeLaneIfIdle(attempt.streamId(), attempt.lane());
            completeAttempt(attempt, new TerminalAttempt(attempt.streamId(), result, null, terminalExpiry()));
            return result;
        }
        Throwable cause = unwrap(error);
        NereusException failure = cause instanceof NereusException nereus
                ? nereus
                : new NereusException(ErrorCode.METADATA_UNAVAILABLE, true,
                        "append recovery failed", cause);
        AppendOutcome outcome = failure.appendOutcome().orElse(AppendOutcome.MAY_HAVE_COMMITTED);
        if (outcome == AppendOutcome.KNOWN_NOT_COMMITTED) {
            attempt.lane().invalidateExpectedOffset();
            attempt.lane().unsuspend();
            removeLaneIfIdle(attempt.streamId(), attempt.lane());
            completeAttempt(attempt, new TerminalAttempt(attempt.streamId(), null, failure, terminalExpiry()));
            throw failure;
        }
        NereusException exposed = failure.appendAttemptId().isPresent()
                ? failure
                : new NereusException(failure.code(), failure.retriable(), failure.getMessage(), failure,
                        outcome == AppendOutcome.KNOWN_COMMITTED ? outcome : AppendOutcome.MAY_HAVE_COMMITTED,
                        attempt.id());
        if (failure.retriable()) {
            scheduleRecovery(attempt, attempt.nextBackoff(config.appendRecoveryBackoffMax()));
        } else {
            completeAttempt(attempt, new TerminalAttempt(attempt.streamId(), null, exposed, terminalExpiry()));
        }
        throw exposed;
    }

    private CompletableFuture<AppendResult> callerView(
            Attempt attempt, CompletableFuture<AppendResult> source, Duration timeout) {
        CompletableFuture<AppendResult> view = new CompletableFuture<>();
        source.whenComplete((value, error) -> {
            if (error == null) view.complete(value); else view.completeExceptionally(unwrap(error));
        });
        recoveryScheduler.schedule(() -> view.completeExceptionally(new NereusException(
                ErrorCode.TIMEOUT, true, "append recovery caller wait timed out", null,
                AppendOutcome.MAY_HAVE_COMMITTED, attempt.id())),
                timeout.toNanos(), TimeUnit.NANOSECONDS);
        return view;
    }

    private void scheduleRecovery(Attempt attempt, Duration delay) {
        if (closed.get() || !retainedAttempts.containsKey(attempt.id())) return;
        recoveryScheduler.schedule(() -> {
            if (!closed.get() && retainedAttempts.containsKey(attempt.id())) {
                startRecovery(attempt).exceptionally(error -> null);
            }
        }, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void completeAttempt(Attempt attempt, TerminalAttempt terminal) {
        if (retainedAttempts.remove(attempt.id(), attempt)) {
            attempt.releasePermit();
        }
        terminalAttempts.put(attempt.id(), terminal);
        attempt.completeTerminal(terminal);
        while (terminalAttempts.size() > config.maxAppendRecoveryTerminals()) {
            terminalAttempts.entrySet().stream()
                    .min(java.util.Comparator.comparingLong(entry -> entry.getValue().expiresAtMillis()))
                    .ifPresent(entry -> terminalAttempts.remove(entry.getKey(), entry.getValue()));
        }
    }

    private long terminalExpiry() {
        long ttl;
        try { ttl = config.appendRecoveryTerminalTtl().toMillis(); }
        catch (ArithmeticException e) { ttl = Long.MAX_VALUE; }
        long now = clock.millis();
        return ttl >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + ttl;
    }

    private void evictExpiredTerminals() {
        long now = clock.millis();
        terminalAttempts.entrySet().removeIf(entry -> entry.getValue().expiresAtMillis() <= now);
    }

    private void removeLaneIfIdle(StreamId streamId, StreamLane lane) {
        lanes.compute(streamId, (ignored, existing) ->
                existing == lane && lane.removable() ? null : existing);
    }

    private static <T> CompletableFuture<T> unknownAttempt() {
        return NereusException.failedFuture(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                "append attempt is unknown, expired, or belongs to another stream");
    }

    private CompletableFuture<AppendResult> executeAppend(
            StreamLane lane,
            Attempt attempt,
            AppendResourceLimiter.Reservation reservation) {
        attempt.attachLane(lane);
        NereusException suspended = lane.suspendedFailure();
        if (suspended != null) {
            AppendOutcome suspendedOutcome = suspended.appendOutcome().orElse(AppendOutcome.MAY_HAVE_COMMITTED);
            NereusException failure = suspended.appendAttemptId().isPresent()
                    ? new NereusException(
                            ErrorCode.METADATA_UNAVAILABLE, true,
                            "stream append lane is suspended until the original physical attempt is resolved",
                            suspended, suspendedOutcome, suspended.appendAttemptId().orElseThrow())
                    : new NereusException(
                            ErrorCode.METADATA_UNAVAILABLE, true,
                            "stream append lane is suspended until the original physical attempt is resolved",
                            suspended, suspendedOutcome);
            return CompletableFuture.failedFuture(failure);
        }
        rejectClosedBeforeUpload();
        attempt.deadline().check(AppendOutcome.KNOWN_NOT_COMMITTED, "start append");
        CompletableFuture<Long> expectedOffset = lane.expectedOffset() == null
                ? attempt.deadline().bound(
                                () -> metadataStore.getCommittedEndOffset(config.cluster(), attempt.streamId()),
                                AppendOutcome.KNOWN_NOT_COMMITTED,
                                "load committed end offset")
                        .thenApply(record -> {
                            lane.initializeExpectedOffset(record.committedEndOffset());
                            return record.committedEndOffset();
                        })
                : CompletableFuture.completedFuture(lane.expectedOffset());
        return expectedOffset
                .thenCompose(offset -> sessionManager.ensureSession(
                        attempt.streamId(),
                        attempt.options().appendSession(),
                        attempt.options().autoAcquireSession(),
                        attempt.deadline()).thenApply(session -> new SessionAndOffset(session, offset)))
                .thenApplyAsync(state -> prepareAttempt(attempt, state, reservation), callbackExecutor)
                .thenCompose(prepared -> uploadAndCommit(lane, attempt, prepared));
    }

    private PreparedAttempt prepareAttempt(
            Attempt attempt,
            SessionAndOffset state,
            AppendResourceLimiter.Reservation reservation) {
        rejectClosedBeforeUpload();
        attempt.deadline().check(AppendOutcome.KNOWN_NOT_COMMITTED, "prepare WAL object");
        reservation.reserveBuffer(config.maxObjectBytes());
        ObjectPreparedPrimaryAppend prepared = primaryAppender.prepare(new PrimaryAppendRequest(
                attempt.streamId(), attempt.batch(), state.session(), state.expectedOffset(), attempt.id(),
                attempt.deadline().remaining()));
        if (prepared.reservedBytes() > config.maxObjectBytes()) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "prepared WAL object exceeds maxObjectBytes",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        reservation.adjustToExactBytes(prepared.reservedBytes());
        WalWriteResult writeResult = prepared.preparedObject().result();
        if (writeResult.slices().size() != 1
                || !writeResult.slices().get(0).streamId().equals(attempt.streamId())) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "single-work-item planner produced an unexpected WAL slice set",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        validateWrittenSlice(attempt.batch(), writeResult.slices().get(0));
        return new PreparedAttempt(state.session(), state.expectedOffset(), prepared);
    }

    private static void validateWrittenSlice(AppendBatch batch, WrittenStreamSlice slice) {
        long logicalBytes = 0;
        try {
            for (com.nereusstream.api.AppendEntry entry : batch.entries()) {
                logicalBytes = Math.addExact(logicalBytes, entry.payload().length);
            }
        } catch (ArithmeticException e) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "append logical byte count overflows",
                    e,
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        if (slice.recordCount() != batch.recordCount()
                || slice.entryCount() != batch.entryCount()
                || slice.logicalBytes() != logicalBytes
                || slice.payloadFormat() != batch.payloadFormat()
                || !slice.schemaRefs().equals(batch.schemaRefs())
                || slice.minEventTimeMillis() != batch.minEventTimeMillis()
                || slice.maxEventTimeMillis() != batch.maxEventTimeMillis()) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "prepared WAL slice does not match the append batch",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
    }

    private void rejectClosedBeforeUpload() {
        if (closed.get()) {
            throw new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "stream storage closed before WAL upload",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
    }

    private CompletableFuture<AppendResult> uploadAndCommit(
            StreamLane lane,
            Attempt attempt,
            PreparedAttempt prepared) {
        attempt.deadline().check(AppendOutcome.KNOWN_NOT_COMMITTED, "start WAL upload");
        return attempt.deadline().bound(
                        () -> primaryAppender.persist(
                                prepared.primaryAppend(), attempt.deadline().remaining()),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "upload WAL object")
                .thenCompose(durable -> {
                    if (!(durable.providerCommitEvidence() instanceof ObjectWalCommitEvidence evidence)) {
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_INVARIANT_VIOLATION,
                                false,
                                "Object WAL adapter returned incompatible commit evidence",
                                AppendOutcome.KNOWN_NOT_COMMITTED));
                    }
                    WalWriteResult writeResult = evidence.writeResult();
                    if (!writeResult.equals(prepared.primaryAppend().preparedObject().result())) {
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                "WAL upload result does not match the prepared object",
                                AppendOutcome.KNOWN_NOT_COMMITTED));
                    }
                    ObjectManifestRecord manifest = toManifest(writeResult, prepared.session());
                    return attempt.deadline().bound(
                                    () -> metadataStore.putObjectManifest(config.cluster(), manifest),
                                    AppendOutcome.KNOWN_NOT_COMMITTED,
                                    "put WAL object manifest")
                            .thenCompose(ignored -> primaryAppender.validateBeforeHeadCommit(
                                    durable, prepared.session(), attempt.deadline().remaining()))
                            .thenApply(ignored -> writeResult);
                })
                .thenCompose(writeResult -> sessionManager.ensureCommitWindow(
                                prepared.session(), attempt.deadline())
                        .thenApply(session -> new UploadedAttempt(session, prepared.expectedOffset(), writeResult)))
                .thenCompose(uploaded -> commit(lane, attempt, uploaded));
    }

    private CompletableFuture<AppendResult> commit(
            StreamLane lane,
            Attempt attempt,
            UploadedAttempt uploaded) {
        attempt.deadline().check(AppendOutcome.KNOWN_NOT_COMMITTED, "start stream-head commit");
        WrittenStreamSlice slice = uploaded.writeResult().slices().get(0);
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                uploaded.writeResult().objectId(),
                uploaded.writeResult().objectKey(),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                slice.sliceId(),
                slice.objectOffset(),
                slice.objectLength(),
                slice.sliceChecksum(),
                slice.entryIndexRef());
        CommitAppendRequest request = new CommitAppendRequest(
                attempt.streamId(),
                config.writerId(),
                writerRunIdHash,
                uploaded.session().epoch(),
                uploaded.session().fencingToken(),
                uploaded.expectedOffset(),
                target,
                slice.payloadFormat(),
                slice.recordCount(),
                slice.entryCount(),
                slice.logicalBytes(),
                slice.schemaRefs(),
                slice.minEventTimeMillis(),
                slice.maxEventTimeMillis(),
                Optional.empty());
        attempt.retainPhysical(request, uploaded.writeResult(), slice);
        CompletableFuture<StableAppendResult> commitFuture = attempt.deadline().bound(
                        () -> stableCommitter.prepare(request),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "prepare stable append intent")
                .thenCompose(prepared -> attempt.deadline().bound(
                        () -> physicalReferences.protectBeforeHead(
                                prepared,
                                attempt.deadline().remaining()),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "protect stable append before head"))
                .thenCompose(protectedAppend -> {
                    attempt.markHeadSent();
                    CompletableFuture<StableAppendResult> rawCommit = stableCommitter.commit(protectedAppend);
                    attempt.trackHeadSource(rawCommit);
                    return attempt.deadline().bound(
                            () -> rawCommit,
                            AppendOutcome.KNOWN_NOT_COMMITTED,
                            AppendOutcome.MAY_HAVE_COMMITTED,
                            "commit protected stream head");
                });
        CompletableFuture<CommittedAppend> strictCommit = commitFuture
                .thenCompose(stable -> {
                    attempt.markHeadKnownCommitted();
                    return attempt.deadline().bound(
                            () -> indexMaterializer.materialize(stable.reachableAppend()),
                            AppendOutcome.KNOWN_COMMITTED,
                            "materialize generation-zero index");
                })
                .thenCompose(materialized -> attempt.deadline().bound(
                        () -> physicalReferences.protectVisibleIndex(
                                materialized,
                                attempt.deadline().remaining()),
                        AppendOutcome.KNOWN_COMMITTED,
                        "protect visible generation-zero index"))
                .thenApply(protectedIndex -> protectedIndex.materialized().committedAppend());
        return strictCommit.handleAsync((commitResult, error) -> {
            if (error != null) {
                NereusException failure = normalizeAppendFailure(error, attempt.outcome());
                if (failure.code() == ErrorCode.FENCED_APPEND) {
                    sessionManager.invalidate(attempt.streamId());
                }
                if (failure.code() == ErrorCode.OFFSET_CONFLICT
                        && failure.appendOutcome().orElse(AppendOutcome.KNOWN_NOT_COMMITTED)
                        == AppendOutcome.KNOWN_NOT_COMMITTED) {
                    lane.invalidateExpectedOffset();
                }
                AppendOutcome outcome = failure.appendOutcome().orElse(attempt.outcome());
                if (outcome != AppendOutcome.KNOWN_NOT_COMMITTED) {
                    NereusException exposed = failure.appendAttemptId().isPresent()
                            ? failure
                            : failure.withAppendAttemptId(attempt.id());
                    lane.suspend(exposed);
                    throw exposed;
                }
                throw failure;
            }
            attempt.markHeadKnownCommitted();
            lane.advanceExpectedOffset(commitResult.range().endOffset());
            attempt.deadline().check(AppendOutcome.KNOWN_COMMITTED, "ack append result");
            return toAppendResult(commitResult, slice);
        }, callbackExecutor);
    }

    private StreamMetadataRecord validateProfile(StreamMetadataRecord metadata, AppendOptions options) {
        StreamState state;
        StorageProfile profile;
        try {
            state = StreamState.valueOf(metadata.state());
            profile = StorageProfile.valueOf(metadata.profile()).canonical();
        } catch (IllegalArgumentException e) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "stream metadata contains an unknown state or profile",
                    e,
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        if (state != StreamState.ACTIVE) {
            throw new NereusException(
                    ErrorCode.STREAM_NOT_ACTIVE,
                    false,
                    "stream is not active",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        profileResolver.requireExecutable(profile, options.durabilityLevel(), true, true);
        return metadata;
    }

    private ObjectManifestRecord toManifest(WalWriteResult result, AppendSession session) {
        long uploadedAtMillis = clock.millis();
        long orphanExpiresAtMillis;
        try {
            orphanExpiresAtMillis = Math.addExact(uploadedAtMillis, ORPHAN_AUDIT_TTL.toMillis());
        } catch (ArithmeticException e) {
            orphanExpiresAtMillis = Long.MAX_VALUE;
        }
        List<StreamSliceManifestRecord> slices = java.util.stream.IntStream.range(0, result.slices().size())
                .mapToObj(index -> {
                    WrittenStreamSlice slice = result.slices().get(index);
                    return new StreamSliceManifestRecord(
                            index,
                            slice.streamId().value(),
                            slice.sliceId(),
                            session.epoch(),
                            slice.objectOffset(),
                            slice.objectLength(),
                            slice.recordCount(),
                            slice.entryCount(),
                            slice.logicalBytes(),
                            slice.schemaRefs(),
                            EntryIndexReferenceRecord.fromApi(slice.entryIndexRef()),
                            slice.sliceChecksum().type().name(),
                            slice.sliceChecksum().value(),
                            slice.payloadFormat().name(),
                            "UPLOADED");
                })
                .toList();
        return new ObjectManifestRecord(
                result.objectId().value(),
                result.objectKey().value(),
                ObjectType.MULTI_STREAM_WAL_OBJECT.name(),
                "UPLOADED",
                result.formatMajorVersion(),
                result.formatMinorVersion(),
                result.writerVersion(),
                config.writerId(),
                writerRunIdHash,
                session.epoch(),
                result.createdAtMillis(),
                uploadedAtMillis,
                result.objectLength(),
                result.objectChecksum().type().name(),
                result.objectChecksum().value(),
                result.storageChecksum().type().name(),
                result.storageChecksum().value(),
                slices,
                orphanExpiresAtMillis,
                0);
    }

    private static AppendResult toAppendResult(
            CommittedAppend commit,
            WrittenStreamSlice slice) {
        return new AppendResult(
                commit.streamId(),
                commit.range(),
                commit.range().endOffset(),
                commit.cumulativeSize(),
                commit.generation(),
                commit.readTarget(),
                slice.payloadFormat(),
                slice.recordCount(),
                slice.entryCount(),
                slice.logicalBytes(),
                slice.schemaRefs(),
                commit.projectionRef(),
                commit.commitVersion());
    }

    private static NereusException normalizeAppendFailure(Throwable throwable, AppendOutcome fallback) {
        Throwable cause = unwrap(throwable);
        if (cause instanceof NereusException nereus) {
            if (nereus.appendOutcome().isPresent()) {
                return nereus;
            }
            return new NereusException(
                    nereus.code(), nereus.retriable(), nereus.getMessage(), nereus, fallback);
        }
        if (cause instanceof java.util.concurrent.CancellationException) {
            return new NereusException(ErrorCode.CANCELLED, true, "append was cancelled", cause, fallback);
        }
        if (cause instanceof IllegalArgumentException) {
            return new NereusException(ErrorCode.INVALID_ARGUMENT, false, cause.getMessage(), cause, fallback);
        }
        return new NereusException(
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                "append failed with an unexpected asynchronous error",
                cause,
                fallback);
    }

    private static Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        while ((current instanceof CompletionException
                || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    @Override
    public void close() {
        beginClose();
        awaitClose(config.shutdownGrace());
    }

    /** Stops admission without waiting for already accepted appends. */
    public void beginClose() {
        if (closed.compareAndSet(false, true)) {
            recoveryScheduler.shutdown();
        }
    }

    /** Waits for accepted appends using the caller's remaining global shutdown budget. */
    public void awaitClose(Duration grace) {
        Objects.requireNonNull(grace, "grace");
        long graceNanos;
        try {
            graceNanos = grace.toNanos();
        } catch (ArithmeticException e) {
            graceNanos = Long.MAX_VALUE;
        }
        long start = System.nanoTime();
        synchronized (lifecycleMonitor) {
            while (activeAppends > 0) {
                long elapsed = System.nanoTime() - start;
                long remaining = graceNanos - elapsed;
                if (remaining <= 0) {
                    break;
                }
                try {
                    long millis = remaining / 1_000_000;
                    int nanos = (int) (remaining % 1_000_000);
                    lifecycleMonitor.wait(millis, nanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void appendAccepted() {
        synchronized (lifecycleMonitor) {
            if (closed.get()) {
                throw new NereusException(
                        ErrorCode.STORAGE_CLOSED,
                        false,
                        "stream storage is closed",
                        AppendOutcome.KNOWN_NOT_COMMITTED);
            }
            activeAppends++;
        }
    }

    private void appendCompleted() {
        synchronized (lifecycleMonitor) {
            activeAppends--;
            lifecycleMonitor.notifyAll();
        }
    }

    private final class Attempt {
        private final AppendAttemptId id;
        private final StreamId streamId;
        private final AppendBatch batch;
        private final AppendOptions options;
        private final AppendDeadline deadline;
        private final AtomicBoolean headSent = new AtomicBoolean();
        private final AtomicBoolean headKnownCommitted = new AtomicBoolean();
        private final AtomicBoolean permitReleased = new AtomicBoolean();
        private final CompletableFuture<Void> originalRunnerQuiesced = new CompletableFuture<>();
        private final CompletableFuture<AppendResult> terminalFuture = new CompletableFuture<>();
        private volatile CommitAppendRequest commitRequest;
        private volatile WalWriteResult writeResult;
        private volatile WrittenStreamSlice slice;
        private volatile StreamLane lane;
        private volatile CompletableFuture<StableAppendResult> headSource;
        private volatile CompletableFuture<AppendResult> recoveryFlight;
        private volatile com.nereusstream.metadata.oxia.AppendReplayCursor replayCursor;
        private long nextBackoffNanos;
        private boolean recoveryUsed;

        Attempt(AppendAttemptId id, StreamId streamId, AppendBatch batch,
                AppendOptions options, AppendDeadline deadline) {
            this.id = id;
            this.streamId = streamId;
            this.batch = batch;
            this.options = options;
            this.deadline = deadline;
            this.nextBackoffNanos = config.appendRecoveryBackoffMin().toNanos();
        }

        AppendAttemptId id() { return id; }
        StreamId streamId() { return streamId; }
        AppendBatch batch() { return batch; }
        AppendOptions options() { return options; }
        AppendDeadline deadline() { return deadline; }

        void attachLane(StreamLane value) { lane = value; }
        StreamLane lane() { return Objects.requireNonNull(lane, "attempt lane"); }

        void retainPhysical(CommitAppendRequest request, WalWriteResult result, WrittenStreamSlice writtenSlice) {
            commitRequest = Objects.requireNonNull(request);
            writeResult = Objects.requireNonNull(result);
            slice = Objects.requireNonNull(writtenSlice);
        }
        CommitAppendRequest commitRequest() { return Objects.requireNonNull(commitRequest, "commitRequest"); }
        WrittenStreamSlice slice() { return Objects.requireNonNull(slice, "slice"); }

        void trackHeadSource(CompletableFuture<StableAppendResult> source) {
            headSource = source;
            source.whenComplete((ignored, error) -> originalRunnerQuiesced.complete(null));
        }

        void completeQuiescedIfNoHeadSource() {
            if (headSource == null) originalRunnerQuiesced.complete(null);
        }

        CompletableFuture<Void> originalRunnerQuiesced() { return originalRunnerQuiesced; }

        CompletableFuture<AppendResult> terminalFuture() { return terminalFuture; }

        void completeTerminal(TerminalAttempt terminal) {
            if (terminal.result() != null) {
                terminalFuture.complete(terminal.result());
            } else {
                terminalFuture.completeExceptionally(terminal.failure());
            }
        }

        synchronized CompletableFuture<AppendResult> singleFlight(
                Supplier<CompletableFuture<AppendResult>> operation) {
            if (recoveryFlight == null || recoveryFlight.isDone()) recoveryFlight = operation.get();
            return recoveryFlight;
        }

        synchronized Duration nextBackoff(Duration maximum) {
            long current = nextBackoffNanos;
            long max;
            try { max = maximum.toNanos(); } catch (ArithmeticException e) { max = Long.MAX_VALUE; }
            nextBackoffNanos = current >= max / 2 ? max : Math.min(max, current * 2);
            return Duration.ofNanos(current);
        }

        synchronized void markRecoveryUsed() { recoveryUsed = true; }
        synchronized void updateReplayCursor(com.nereusstream.metadata.oxia.AppendReplayCursor cursor) {
            replayCursor = cursor;
        }

        void releasePermit() {
            if (permitReleased.compareAndSet(false, true)) retainedAttemptPermits.release();
        }

        void markHeadSent() {
            headSent.set(true);
        }

        void markHeadKnownCommitted() {
            headKnownCommitted.set(true);
        }

        AppendOutcome outcome() {
            if (headKnownCommitted.get()) {
                return AppendOutcome.KNOWN_COMMITTED;
            }
            return headSent.get() ? AppendOutcome.MAY_HAVE_COMMITTED : AppendOutcome.KNOWN_NOT_COMMITTED;
        }
    }

    private record TerminalAttempt(
            StreamId streamId,
            AppendResult result,
            NereusException failure,
            long expiresAtMillis) {
        private TerminalAttempt {
            Objects.requireNonNull(streamId, "streamId");
            if ((result == null) == (failure == null)) {
                throw new IllegalArgumentException("terminal attempt must contain exactly one outcome");
            }
        }
    }

    private record SessionAndOffset(AppendSession session, long expectedOffset) {
    }

    private record PreparedAttempt(
            AppendSession session,
            long expectedOffset,
            ObjectPreparedPrimaryAppend primaryAppend) {
    }

    private record UploadedAttempt(
            AppendSession session,
            long expectedOffset,
            WalWriteResult writeResult) {
    }

    private static final class StreamLane {
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
        private Long expectedOffset;
        private NereusException suspendedFailure;
        private boolean lifecycleBarrier;
        private int retainedOperations;

        synchronized void retain() {
            retainedOperations++;
        }

        synchronized void release() {
            if (retainedOperations <= 0) {
                throw new IllegalStateException("stream lane retain count underflow");
            }
            retainedOperations--;
        }

        synchronized boolean removable() {
            return retainedOperations == 0 && suspendedFailure == null;
        }

        synchronized <T> CompletableFuture<T> enqueue(
                Supplier<CompletableFuture<T>> operation,
                Executor executor) {
            CompletableFuture<T> result = tail.thenComposeAsync(ignored -> operation.get(), executor);
            tail = result.handle((ignored, error) -> null);
            return result;
        }

        synchronized boolean tryAdmitAppend() {
            return !lifecycleBarrier;
        }

        synchronized void beginLifecycleBarrier() {
            lifecycleBarrier = true;
        }

        synchronized Long expectedOffset() {
            return expectedOffset;
        }

        synchronized void initializeExpectedOffset(long value) {
            if (expectedOffset == null) {
                expectedOffset = value;
            }
        }

        synchronized void advanceExpectedOffset(long value) {
            expectedOffset = value;
        }

        synchronized void invalidateExpectedOffset() {
            expectedOffset = null;
        }

        synchronized void suspend(NereusException failure) {
            suspendedFailure = failure;
        }

        synchronized void unsuspend() {
            suspendedFailure = null;
        }

        synchronized NereusException suspendedFailure() {
            return suspendedFailure;
        }
    }

    private static final class AppendFuture extends CompletableFuture<AppendResult> {
        private Runnable cancellation = () -> { };

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
}
