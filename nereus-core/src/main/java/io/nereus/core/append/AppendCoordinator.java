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

package io.nereus.core.append;

import io.nereus.api.AppendBatch;
import io.nereus.api.AppendOptions;
import io.nereus.api.AppendOutcome;
import io.nereus.api.AppendResult;
import io.nereus.api.AppendSession;
import io.nereus.api.DurabilityLevel;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.ObjectType;
import io.nereus.api.StorageProfile;
import io.nereus.api.StreamId;
import io.nereus.api.StreamState;
import io.nereus.api.keys.DeterministicIds;
import io.nereus.core.StreamStorageConfig;
import io.nereus.metadata.oxia.CommitSliceRequest;
import io.nereus.metadata.oxia.CommitSliceResult;
import io.nereus.metadata.oxia.OxiaMetadataStore;
import io.nereus.metadata.oxia.records.EntryIndexReferenceRecord;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.metadata.oxia.records.StreamMetadataRecord;
import io.nereus.metadata.oxia.records.StreamSliceManifestRecord;
import io.nereus.objectstore.wal.CompressionType;
import io.nereus.objectstore.wal.PreparedWalObject;
import io.nereus.objectstore.wal.WalObjectWriter;
import io.nereus.objectstore.wal.WalStreamSliceInput;
import io.nereus.objectstore.wal.WalWriteOptions;
import io.nereus.objectstore.wal.WalWriteRequest;
import io.nereus.objectstore.wal.WalWriteResult;
import io.nereus.objectstore.wal.WrittenStreamSlice;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public final class AppendCoordinator implements AutoCloseable {
    private static final Duration ORPHAN_AUDIT_TTL = Duration.ofHours(24);

    private final StreamStorageConfig config;
    private final OxiaMetadataStore metadataStore;
    private final WalObjectWriter walObjectWriter;
    private final AppendSessionManager sessionManager;
    private final Clock clock;
    private final Executor callbackExecutor;
    private final String writerRunIdHash;
    private final AppendResourceLimiter resourceLimiter;
    private final ConcurrentHashMap<StreamId, StreamLane> lanes = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycleMonitor = new Object();
    private int activeAppends;

    public AppendCoordinator(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            AppendSessionManager sessionManager,
            Clock clock,
            Executor callbackExecutor) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.walObjectWriter = Objects.requireNonNull(walObjectWriter, "walObjectWriter");
        this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.writerRunIdHash = newWriterRunIdHash();
        this.resourceLimiter = new AppendResourceLimiter(
                config.maxInFlightAppends(), config.maxBufferedBytes());
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
        Attempt attempt = new Attempt(streamId, batch, options, deadline);
        CompletableFuture<AppendResult> pipeline = deadline.bound(
                        () -> metadataStore.getStream(config.cluster(), streamId),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "load stream profile")
                .thenApplyAsync(metadata -> validateProfile(metadata, options), callbackExecutor)
                .thenComposeAsync(ignored -> acceptAndEnqueue(attempt), callbackExecutor);
        pipeline.whenComplete((value, error) -> {
            if (error == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(normalizeAppendFailure(error, attempt.outcome()));
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
        StreamLane lane = lanes.computeIfAbsent(attempt.streamId(), ignored -> new StreamLane());
        try {
            CompletableFuture<AppendResult> queued = lane.enqueue(
                    () -> executeAppend(lane, attempt, reservation), callbackExecutor);
            return queued.whenComplete((ignored, error) -> {
                reservation.close();
                appendCompleted();
            });
        } catch (RuntimeException e) {
            reservation.close();
            appendCompleted();
            throw e;
        }
    }

    private CompletableFuture<AppendResult> executeAppend(
            StreamLane lane,
            Attempt attempt,
            AppendResourceLimiter.Reservation reservation) {
        NereusException suspended = lane.suspendedFailure();
        if (suspended != null) {
            AppendOutcome suspendedOutcome = suspended.appendOutcome().orElse(AppendOutcome.MAY_HAVE_COMMITTED);
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "stream append lane is suspended until the original physical attempt is resolved",
                    suspended,
                    suspendedOutcome));
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
        Duration uploadTimeout = attempt.deadline().remaining();
        WalWriteRequest request = new WalWriteRequest(
                config.cluster(),
                config.writerId(),
                writerRunIdHash,
                state.session().epoch(),
                List.of(new WalStreamSliceInput(attempt.streamId(), attempt.batch())),
                new WalWriteOptions(
                        CompressionType.NONE,
                        config.maxObjectBytes(),
                        config.maxObjectBytes(),
                        uploadTimeout,
                        true));
        PreparedWalObject preparedObject = walObjectWriter.prepare(request);
        if (preparedObject.objectLength() > config.maxObjectBytes()) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "prepared WAL object exceeds maxObjectBytes",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        reservation.adjustToExactBytes(preparedObject.objectLength());
        WalWriteResult writeResult = preparedObject.result();
        if (writeResult.slices().size() != 1
                || !writeResult.slices().getFirst().streamId().equals(attempt.streamId())) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "single-work-item planner produced an unexpected WAL slice set",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        validateWrittenSlice(attempt.batch(), writeResult.slices().getFirst());
        return new PreparedAttempt(state.session(), state.expectedOffset(), preparedObject);
    }

    private static void validateWrittenSlice(AppendBatch batch, WrittenStreamSlice slice) {
        long logicalBytes = 0;
        try {
            for (io.nereus.api.AppendEntry entry : batch.entries()) {
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
                        () -> walObjectWriter.upload(prepared.preparedObject()),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "upload WAL object")
                .thenCompose(writeResult -> {
                    if (!writeResult.equals(prepared.preparedObject().result())) {
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_INVARIANT_VIOLATION,
                                false,
                                "WAL upload result does not match the prepared object",
                                AppendOutcome.KNOWN_NOT_COMMITTED));
                    }
                    ObjectManifestRecord manifest = toManifest(writeResult, prepared.session());
                    return attempt.deadline().bound(
                                    () -> metadataStore.putObjectManifest(config.cluster(), manifest),
                                    AppendOutcome.KNOWN_NOT_COMMITTED,
                                    "put WAL object manifest")
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
        WrittenStreamSlice slice = uploaded.writeResult().slices().getFirst();
        CommitSliceRequest request = new CommitSliceRequest(
                attempt.streamId(),
                config.writerId(),
                writerRunIdHash,
                uploaded.session().epoch(),
                uploaded.session().fencingToken(),
                uploaded.expectedOffset(),
                slice.sliceId(),
                slice.recordCount(),
                slice.entryCount(),
                slice.logicalBytes(),
                slice.schemaRefs(),
                uploaded.writeResult().objectId(),
                uploaded.writeResult().objectKey(),
                uploaded.writeResult().objectChecksum(),
                slice.objectOffset(),
                slice.objectLength(),
                slice.entryIndexRef(),
                slice.sliceChecksum(),
                slice.payloadFormat(),
                slice.minEventTimeMillis(),
                slice.maxEventTimeMillis(),
                Optional.empty());
        CompletableFuture<CommitSliceResult> commitFuture = attempt.deadline().bound(
                () -> {
                    attempt.markHeadSent();
                    return metadataStore.commitStreamSlice(config.cluster(), request);
                },
                AppendOutcome.KNOWN_NOT_COMMITTED,
                AppendOutcome.MAY_HAVE_COMMITTED,
                "commit stream head");
        return commitFuture.handleAsync((commitResult, error) -> {
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
                    lane.suspend(failure);
                }
                throw failure;
            }
            attempt.markHeadKnownCommitted();
            lane.advanceExpectedOffset(commitResult.committedEndOffset());
            attempt.deadline().check(AppendOutcome.KNOWN_COMMITTED, "ack append result");
            return toAppendResult(commitResult, uploaded.writeResult(), slice);
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
        if (profile != StorageProfile.OBJECT_WAL_SYNC_OBJECT) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "Phase 1 core append supports only OBJECT_WAL_SYNC_OBJECT",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        if (options.durabilityLevel() != DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_DURABILITY_LEVEL,
                    false,
                    "Phase 1 core append requires WAL_DURABLE_AND_INDEX_COMMITTED",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
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
            CommitSliceResult commit,
            WalWriteResult writeResult,
            WrittenStreamSlice slice) {
        return new AppendResult(
                commit.streamId(),
                commit.range(),
                commit.committedEndOffset(),
                commit.generation(),
                writeResult.objectId(),
                writeResult.objectKey(),
                slice.sliceId(),
                slice.objectOffset(),
                slice.objectLength(),
                slice.payloadFormat(),
                slice.recordCount(),
                slice.entryCount(),
                slice.logicalBytes(),
                slice.schemaRefs(),
                slice.entryIndexRef(),
                writeResult.objectChecksum(),
                slice.sliceChecksum(),
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

    private static String newWriterRunIdHash() {
        byte[] entropy = new byte[16];
        new SecureRandom().nextBytes(entropy);
        return DeterministicIds.randomRunIdHash(entropy);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        long graceNanos;
        try {
            graceNanos = config.shutdownGrace().toNanos();
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

    private record Attempt(
            StreamId streamId,
            AppendBatch batch,
            AppendOptions options,
            AppendDeadline deadline,
            AtomicBoolean headSent,
            AtomicBoolean headKnownCommitted) {
        Attempt(StreamId streamId, AppendBatch batch, AppendOptions options, AppendDeadline deadline) {
            this(streamId, batch, options, deadline, new AtomicBoolean(), new AtomicBoolean());
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

    private record SessionAndOffset(AppendSession session, long expectedOffset) {
    }

    private record PreparedAttempt(
            AppendSession session,
            long expectedOffset,
            PreparedWalObject preparedObject) {
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

        synchronized <T> CompletableFuture<T> enqueue(
                Supplier<CompletableFuture<T>> operation,
                Executor executor) {
            CompletableFuture<T> result = tail.thenComposeAsync(ignored -> operation.get(), executor);
            tail = result.handle((ignored, error) -> null);
            return result;
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
