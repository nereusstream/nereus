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

package com.nereusstream.kafka.partition;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendPrecondition;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.codec.EncodedKafkaAppend;
import com.nereusstream.kafka.codec.KafkaAppendBatchEncoder;
import com.nereusstream.kafka.codec.KafkaAppendResultValidator;
import com.nereusstream.kafka.codec.KafkaFetchAssembler;
import com.nereusstream.kafka.codec.KafkaFetchAssembly;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Serialized stable append and bounded committed-read implementation for one recovered Kafka leader. */
public final class DefaultKafkaPartitionStorage implements KafkaPartitionStorage {
    private static final String AUTHORITY_TYPE = "kafka-partition-leader-v1";

    private final Object guard = new Object();
    private final KafkaPartitionIdentity identity;
    private final StreamStorage streams;
    private final StreamId streamId;
    private volatile AppendSession appendSession;
    private final int leaderEpoch;
    private final KafkaStorageProfilePolicy profilePolicy;
    private final KafkaAppendBatchEncoder appendEncoder;
    private final KafkaFetchAssembler fetchAssembler;
    private final ScheduledExecutorService renewalScheduler;
    private final Duration sessionTtl;
    private final long renewalIntervalMillis;
    private final ArrayDeque<AppendOperation> appendQueue = new ArrayDeque<>();
    private final Set<ListenerRegistration> eventListeners = new HashSet<>();
    private final CompletableFuture<Void> resigned = new CompletableFuture<>();

    private KafkaPartitionState state = KafkaPartitionState.LEADER_WRITABLE;
    private KafkaStableSnapshot stableSnapshot;
    private long admittedEndOffset;
    private boolean appendRunning;
    private boolean dispatching;
    private boolean dispatchRequested;
    private boolean renewalInFlight;
    private ScheduledFuture<?> renewalTask;

    public DefaultKafkaPartitionStorage(
            KafkaPartitionIdentity identity,
            StreamStorage streams,
            StreamId streamId,
            AcquiredAppendSession acquiredSession,
            KafkaCheckpointSourceState recoveredSource,
            KafkaStorageProfilePolicy profilePolicy,
            KafkaAppendBatchEncoder appendEncoder,
            KafkaFetchAssembler fetchAssembler) {
        this(
                identity,
                streams,
                streamId,
                acquiredSession,
                recoveredSource,
                profilePolicy,
                appendEncoder,
                fetchAssembler,
                null,
                null,
                0);
    }

    public DefaultKafkaPartitionStorage(
            KafkaPartitionIdentity identity,
            StreamStorage streams,
            StreamId streamId,
            AcquiredAppendSession acquiredSession,
            KafkaCheckpointSourceState recoveredSource,
            KafkaStorageProfilePolicy profilePolicy,
            KafkaAppendBatchEncoder appendEncoder,
            KafkaFetchAssembler fetchAssembler,
            ScheduledExecutorService renewalScheduler,
            Duration sessionTtl,
            Duration renewalInterval) {
        this(
                identity,
                streams,
                streamId,
                acquiredSession,
                recoveredSource,
                profilePolicy,
                appendEncoder,
                fetchAssembler,
                Objects.requireNonNull(renewalScheduler, "renewalScheduler"),
                positive(sessionTtl, "sessionTtl"),
                positive(renewalInterval, "renewalInterval").toMillis());
        if (renewalInterval.compareTo(sessionTtl) >= 0) {
            throw new IllegalArgumentException("renewalInterval must be shorter than sessionTtl");
        }
        scheduleRenewal(true);
    }

    private DefaultKafkaPartitionStorage(
            KafkaPartitionIdentity identity,
            StreamStorage streams,
            StreamId streamId,
            AcquiredAppendSession acquiredSession,
            KafkaCheckpointSourceState recoveredSource,
            KafkaStorageProfilePolicy profilePolicy,
            KafkaAppendBatchEncoder appendEncoder,
            KafkaFetchAssembler fetchAssembler,
            ScheduledExecutorService renewalScheduler,
            Duration sessionTtl,
            long renewalIntervalMillis) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.streams = Objects.requireNonNull(streams, "streams");
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(acquiredSession, "acquiredSession");
        Objects.requireNonNull(recoveredSource, "recoveredSource");
        this.profilePolicy = Objects.requireNonNull(profilePolicy, "profilePolicy");
        this.appendEncoder = Objects.requireNonNull(appendEncoder, "appendEncoder");
        this.fetchAssembler = Objects.requireNonNull(fetchAssembler, "fetchAssembler");
        this.renewalScheduler = renewalScheduler;
        this.sessionTtl = sessionTtl;
        this.renewalIntervalMillis = renewalIntervalMillis;
        this.appendSession = acquiredSession.session();
        validateRecoveredSession(acquiredSession, recoveredSource);
        this.leaderEpoch = Math.toIntExact(recoveredSource.authority().authorityEpoch());
        this.stableSnapshot = KafkaStableSnapshot.nonTransactional(
                recoveredSource.trimOffset(), recoveredSource.endOffset(), recoveredSource.commitVersion());
        this.admittedEndOffset = recoveredSource.endOffset();
    }

    @Override
    public KafkaPartitionIdentity identity() {
        return identity;
    }

    @Override
    public int leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public StorageProfile storageProfile() {
        return profilePolicy.storageProfile();
    }

    @Override
    public KafkaPartitionState state() {
        synchronized (guard) {
            return state;
        }
    }

    @Override
    public KafkaStableSnapshot stableSnapshot() {
        synchronized (guard) {
            return stableSnapshot;
        }
    }

    @Override
    public CompletableFuture<KafkaStableAppendResult> append(
            ByteBuffer validatedRecords, KafkaAppendContext context) {
        Objects.requireNonNull(context, "context");
        EncodedKafkaAppend encoded;
        try {
            encoded = appendEncoder.encode(validatedRecords, context.expectedStartOffset());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        AppendOperation operation = new AppendOperation(encoded, context);
        boolean start;
        synchronized (guard) {
            if (state != KafkaPartitionState.LEADER_WRITABLE) {
                return CompletableFuture.failedFuture(fenced("Kafka partition is not writable: " + state));
            }
            if (context.leaderEpoch() != leaderEpoch) {
                return CompletableFuture.failedFuture(fenced("Kafka append leader epoch is stale"));
            }
            if (context.expectedStartOffset() != admittedEndOffset) {
                return CompletableFuture.failedFuture(offsetConflict(
                        "Kafka append expected " + context.expectedStartOffset()
                                + " but admitted end is " + admittedEndOffset));
            }
            admittedEndOffset = encoded.range().endOffset();
            appendQueue.addLast(operation);
            start = !appendRunning;
            if (start) appendRunning = true;
        }
        if (start) requestAppendDispatch();
        return operation.result;
    }

    @Override
    public CompletableFuture<KafkaStorageReadResult> read(KafkaStorageReadRequest request) {
        Objects.requireNonNull(request, "request");
        KafkaStableSnapshot snapshot;
        synchronized (guard) {
            if (state != KafkaPartitionState.LEADER_WRITABLE
                    && state != KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED) {
                return CompletableFuture.failedFuture(
                        new NereusException(
                                ErrorCode.STORAGE_CLOSED,
                                false,
                                "Kafka partition is not readable: " + state));
            }
            snapshot = stableSnapshot;
        }
        if (request.startOffset() < snapshot.logStartOffset()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.OFFSET_TRIMMED, false, "Kafka fetch offset precedes log start"));
        }
        long upperBound = Math.min(request.maxOffsetExclusive(), snapshot.stableEndOffset());
        if (request.startOffset() >= upperBound) {
            return CompletableFuture.completedFuture(emptyRead(request, snapshot));
        }
        ReadRequest nereusRequest = new ReadRequest(
                request.startOffset(),
                ReadView.COMMITTED,
                ReadBoundaryMode.CONTAINING_ENTRY,
                request.minOneMessage()
                        ? FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW
                        : FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                new ReadOptions(
                        request.maxRecords(),
                        request.maxPartitionBytes(),
                        ReadIsolation.COMMITTED,
                        request.timeout()));
        return streams.read(streamId, nereusRequest)
                .thenApply(result -> assembleRead(request, snapshot, upperBound, result));
    }

    @Override
    public KafkaPartitionEventSubscription subscribe(KafkaPartitionEventListener listener) {
        Objects.requireNonNull(listener, "listener");
        synchronized (guard) {
            if (state != KafkaPartitionState.LEADER_WRITABLE
                    && state != KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED) {
                throw new NereusException(
                        ErrorCode.STORAGE_CLOSED,
                        false,
                        "Kafka partition cannot accept Fetch listeners: " + state);
            }
            ListenerRegistration registration = new ListenerRegistration(listener);
            eventListeners.add(registration);
            return registration;
        }
    }

    @Override
    public CompletableFuture<Void> resign() {
        boolean complete;
        boolean leadershipChanged = false;
        KafkaStableSnapshot snapshot;
        ScheduledFuture<?> renewal;
        synchronized (guard) {
            if (state == KafkaPartitionState.CLOSED) return resigned;
            if (state != KafkaPartitionState.RESIGNING) {
                state = KafkaPartitionState.RESIGNING;
                leadershipChanged = true;
            }
            snapshot = stableSnapshot;
            renewal = renewalTask;
            renewalTask = null;
            complete = !appendRunning && appendQueue.isEmpty();
            if (complete) state = KafkaPartitionState.CLOSED;
        }
        if (renewal != null) renewal.cancel(false);
        if (leadershipChanged) publishEvent(KafkaPartitionEventType.LEADERSHIP_LOST, snapshot);
        if (complete) resigned.complete(null);
        return resigned;
    }

    @Override
    public void close() {
        resign();
    }

    private void requestAppendDispatch() {
        synchronized (guard) {
            dispatchRequested = true;
            if (dispatching) return;
            dispatching = true;
        }
        while (true) {
            synchronized (guard) {
                if (!dispatchRequested) {
                    dispatching = false;
                    return;
                }
                dispatchRequested = false;
            }
            dispatchHeadAppend();
        }
    }

    private void dispatchHeadAppend() {
        AppendOperation operation;
        boolean completeResign = false;
        synchronized (guard) {
            operation = appendQueue.peekFirst();
            if (operation == null) {
                appendRunning = false;
                completeResign = finishResignIfDrained();
            }
        }
        if (completeResign) resigned.complete(null);
        if (operation == null) return;
        AppendOptions options = new AppendOptions(
                Optional.of(appendSession),
                profilePolicy.durabilityLevel(),
                profilePolicy.completionPolicy(),
                operation.context.timeout(),
                false,
                operation.context.tags());
        CompletableFuture<AppendResult> append;
        try {
            append = streams.append(
                    streamId,
                    operation.encoded.appendBatch(),
                    options,
                    AppendPrecondition.expectedStartOffset(operation.context.expectedStartOffset()));
        } catch (Throwable failure) {
            append = CompletableFuture.failedFuture(failure);
        }
        append.whenComplete((result, failure) -> completeHeadAppend(operation, result, failure));
    }

    private void completeHeadAppend(
            AppendOperation operation, AppendResult appendResult, Throwable appendFailure) {
        Throwable failure = appendFailure == null ? null : unwrap(appendFailure);
        KafkaStableAppendResult success = null;
        if (failure == null) {
            try {
                AppendResult exact = KafkaAppendResultValidator.validate(streamId, operation.encoded, appendResult);
                synchronized (guard) {
                    if (exact.commitVersion() <= stableSnapshot.commitVersion()
                            || exact.range().startOffset() != stableSnapshot.stableEndOffset()) {
                        throw new IllegalStateException("Kafka stable append result is stale or non-contiguous");
                    }
                    stableSnapshot = KafkaStableSnapshot.nonTransactional(
                            stableSnapshot.logStartOffset(), exact.committedEndOffset(), exact.commitVersion());
                    success = new KafkaStableAppendResult(
                            exact, operation.encoded, stableSnapshot, operation.context.requiredAcks());
                }
            } catch (Throwable invalidResult) {
                failure = invalidResult;
            }
        }

        List<AppendOperation> rejected = List.of();
        boolean startNext = false;
        boolean completeResign = false;
        boolean rejectBecauseFenced = false;
        synchronized (guard) {
            if (appendQueue.peekFirst() != operation) {
                throw new IllegalStateException("Kafka append lane completion is out of order");
            }
            appendQueue.removeFirst();
            if (failure == null) {
                if (!appendQueue.isEmpty()
                        && state == KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED) {
                    rejected = new ArrayList<>(appendQueue);
                    appendQueue.clear();
                    admittedEndOffset = stableSnapshot.stableEndOffset();
                    appendRunning = false;
                    completeResign = finishResignIfDrained();
                } else if (appendQueue.isEmpty()) {
                    appendRunning = false;
                    completeResign = finishResignIfDrained();
                } else {
                    startNext = true;
                }
            } else {
                boolean knownNotCommitted = isKnownNotCommitted(failure);
                rejected = new ArrayList<>(appendQueue);
                appendQueue.clear();
                admittedEndOffset = stableSnapshot.stableEndOffset();
                appendRunning = false;
                if (!knownNotCommitted && state == KafkaPartitionState.LEADER_WRITABLE) {
                    state = KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED;
                }
                rejectBecauseFenced = state == KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED;
                completeResign = finishResignIfDrained();
            }
        }

        if (failure == null) {
            publishEvent(KafkaPartitionEventType.STABLE_APPEND, success.stableSnapshot());
            operation.result.complete(success);
            NereusException rejectedFailure = fenced(
                    "Kafka append was rejected because partition authority renewal failed");
            rejected.forEach(value -> value.result.completeExceptionally(rejectedFailure));
        } else {
            operation.result.completeExceptionally(failure);
            NereusException rejectedFailure = rejectBecauseFenced
                    ? fenced("Kafka append was rejected because partition authority was lost")
                    : offsetConflict("Kafka append was rejected because an earlier lane append did not complete");
            rejected.forEach(value -> value.result.completeExceptionally(rejectedFailure));
        }
        if (completeResign) resigned.complete(null);
        if (startNext) requestAppendDispatch();
    }

    private KafkaStorageReadResult assembleRead(
            KafkaStorageReadRequest request,
            KafkaStableSnapshot snapshot,
            long upperBound,
            SemanticReadResult source) {
        if (!source.result().streamId().equals(streamId)
                || source.result().requestedOffset() != request.startOffset()
                || source.view() != ReadView.COMMITTED) {
            throw new IllegalStateException("Nereus read result does not match the Kafka partition request");
        }
        List<ReadBatch> selected = new ArrayList<>();
        int selectedBytes = 0;
        for (ReadBatch batch : source.result().batches()) {
            if (batch.range().endOffset() > upperBound) break;
            selectedBytes = Math.addExact(selectedBytes, batch.payload().length);
            selected.add(batch);
        }
        long nextOffset = selected.isEmpty()
                ? request.startOffset()
                : selected.get(selected.size() - 1).range().endOffset();
        boolean firstOverflow = !selected.isEmpty()
                && selected.get(0).payload().length > request.maxPartitionBytes();
        if (firstOverflow && (!request.minOneMessage() || selected.size() != 1)) {
            throw new IllegalStateException("Nereus read violated Kafka first-entry overflow semantics");
        }
        if (!firstOverflow && selectedBytes > request.maxPartitionBytes()) {
            throw new IllegalStateException("Nereus read exceeded the Kafka partition byte limit");
        }
        ReadResult clipped = new ReadResult(
                streamId,
                request.startOffset(),
                nextOffset,
                selected,
                source.result().endOfStream());
        SemanticReadResult semantic = new SemanticReadResult(ReadView.COMMITTED, clipped, nextOffset);
        KafkaFetchAssembly assembly = fetchAssembler.assemble(
                semantic,
                request.hardMaxResponseBytes(),
                firstOverflow,
                request.virtualSegmentBaseOffset(),
                request.relativeLogicalBytePosition(),
                List.of());
        return new KafkaStorageReadResult(assembly, snapshot);
    }

    private KafkaStorageReadResult emptyRead(
            KafkaStorageReadRequest request, KafkaStableSnapshot snapshot) {
        ReadResult empty = new ReadResult(
                streamId,
                request.startOffset(),
                request.startOffset(),
                List.of(),
                request.startOffset() >= snapshot.stableEndOffset());
        KafkaFetchAssembly assembly = fetchAssembler.assemble(
                new SemanticReadResult(ReadView.COMMITTED, empty, request.startOffset()),
                request.hardMaxResponseBytes(),
                false,
                request.virtualSegmentBaseOffset(),
                request.relativeLogicalBytePosition(),
                List.of());
        return new KafkaStorageReadResult(assembly, snapshot);
    }

    private void validateRecoveredSession(
            AcquiredAppendSession acquired, KafkaCheckpointSourceState recovered) {
        if (!appendSession.streamId().equals(streamId)
                || acquired.authority().isEmpty()
                || !acquired.authority().orElseThrow().equals(recovered.authority())
                || !recovered.authority().authorityType().equals(AUTHORITY_TYPE)
                || !recovered.authority().authorityId().equals(identity.durableId().canonicalIdentity())
                || !appendSession.writerId().equals(recovered.writerId())
                || appendSession.epoch() != recovered.sessionEpoch()
                || !appendSession.fencingToken().equals(recovered.fencingToken())
                || appendSession.leaseVersion() < recovered.leaseVersion()
                || recovered.appendInFlight()
                || recovered.stateMapEndOffset() != recovered.endOffset()) {
            throw new IllegalArgumentException(
                    "Kafka partition storage requires the exact recovered authority session");
        }
    }

    private void scheduleRenewal(boolean initial) {
        if (renewalScheduler == null) return;
        try {
            synchronized (guard) {
                if (state != KafkaPartitionState.LEADER_WRITABLE || renewalTask != null || renewalInFlight) {
                    return;
                }
                renewalTask = renewalScheduler.schedule(
                        this::beginRenewal,
                        renewalIntervalMillis,
                        TimeUnit.MILLISECONDS);
            }
        } catch (RejectedExecutionException failure) {
            if (initial) {
                throw new NereusException(
                        ErrorCode.BACKPRESSURE_REJECTED,
                        true,
                        "Kafka append-session renewal scheduler rejected leader storage",
                        failure);
            }
            fenceRenewalFailure();
        }
    }

    private void beginRenewal() {
        AppendSession current;
        synchronized (guard) {
            renewalTask = null;
            if (state != KafkaPartitionState.LEADER_WRITABLE || renewalInFlight) return;
            renewalInFlight = true;
            current = appendSession;
        }
        CompletableFuture<AppendSession> renewal;
        try {
            renewal = streams.renewAppendSession(current, sessionTtl);
            if (renewal == null) {
                throw new IllegalStateException("StreamStorage returned a null append-session renewal future");
            }
        } catch (Throwable failure) {
            renewal = CompletableFuture.failedFuture(failure);
        }
        renewal.whenComplete((renewed, failure) -> completeRenewal(current, renewed, failure));
    }

    private void completeRenewal(
            AppendSession previous,
            AppendSession renewed,
            Throwable failure) {
        boolean failed = failure != null || !validRenewal(previous, renewed);
        synchronized (guard) {
            renewalInFlight = false;
            if (state != KafkaPartitionState.LEADER_WRITABLE) return;
            if (failed) {
                state = KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED;
                admittedEndOffset = stableSnapshot.stableEndOffset();
            } else {
                appendSession = renewed;
            }
        }
        if (failed) {
            publishEvent(KafkaPartitionEventType.LEADERSHIP_LOST, stableSnapshot());
        } else {
            scheduleRenewal(false);
        }
    }

    private void fenceRenewalFailure() {
        boolean changed;
        KafkaStableSnapshot snapshot;
        synchronized (guard) {
            changed = state == KafkaPartitionState.LEADER_WRITABLE;
            if (changed) {
                state = KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED;
                admittedEndOffset = stableSnapshot.stableEndOffset();
            }
            snapshot = stableSnapshot;
        }
        if (changed) publishEvent(KafkaPartitionEventType.LEADERSHIP_LOST, snapshot);
    }

    private static boolean validRenewal(AppendSession previous, AppendSession renewed) {
        return renewed != null
                && renewed.streamId().equals(previous.streamId())
                && renewed.writerId().equals(previous.writerId())
                && renewed.epoch() == previous.epoch()
                && renewed.fencingToken().equals(previous.fencingToken())
                && renewed.leaseVersion() > previous.leaseVersion()
                && renewed.expiresAtMillis() > previous.expiresAtMillis();
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        boolean valid;
        try {
            valid = !value.isZero() && !value.isNegative() && value.toMillis() > 0;
        } catch (ArithmeticException failure) {
            valid = false;
        }
        if (!valid) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private boolean finishResignIfDrained() {
        if (state == KafkaPartitionState.RESIGNING && !appendRunning && appendQueue.isEmpty()) {
            state = KafkaPartitionState.CLOSED;
            return true;
        }
        return false;
    }

    private void publishEvent(KafkaPartitionEventType type, KafkaStableSnapshot snapshot) {
        KafkaPartitionEvent event = new KafkaPartitionEvent(identity, type, snapshot);
        List<ListenerRegistration> listeners;
        synchronized (guard) {
            listeners = List.copyOf(eventListeners);
            if (type == KafkaPartitionEventType.LEADERSHIP_LOST
                    || type == KafkaPartitionEventType.CORRUPT_OFFLINE) {
                eventListeners.clear();
            }
        }
        for (ListenerRegistration listener : listeners) {
            try {
                listener.listener.onPartitionEvent(event);
            } catch (Throwable ignored) {
                // Listener code is observation-only and cannot change a completed storage outcome.
            }
        }
    }

    private static boolean isKnownNotCommitted(Throwable failure) {
        return failure instanceof NereusException nereus
                && nereus.appendOutcome().orElse(AppendOutcome.MAY_HAVE_COMMITTED)
                        == AppendOutcome.KNOWN_NOT_COMMITTED;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException fenced(String message) {
        return new NereusException(ErrorCode.FENCED_APPEND, false, message);
    }

    private static NereusException offsetConflict(String message) {
        return new NereusException(
                ErrorCode.OFFSET_CONFLICT, false, message, AppendOutcome.KNOWN_NOT_COMMITTED);
    }

    private static final class AppendOperation {
        private final EncodedKafkaAppend encoded;
        private final KafkaAppendContext context;
        private final CompletableFuture<KafkaStableAppendResult> result = new CompletableFuture<>();

        private AppendOperation(EncodedKafkaAppend encoded, KafkaAppendContext context) {
            this.encoded = encoded;
            this.context = context;
        }
    }

    private final class ListenerRegistration implements KafkaPartitionEventSubscription {
        private final KafkaPartitionEventListener listener;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ListenerRegistration(KafkaPartitionEventListener listener) {
            this.listener = listener;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                synchronized (guard) {
                    eventListeners.remove(this);
                }
            }
        }
    }
}
