/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.registry.allocator.AllocatorEvidenceAdmissionPolicyV3;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * ADR-0108 asynchronous evidence runner. Dispatch order is deterministic per actor, callback completion may be
 * unordered, and terminal inventory is canonicalized by request ordinal. No Java lock provides allocator correctness.
 */
final class M3V3AsyncActorLaneRunner<T> {
    static final int ACTOR_COUNT = AllocatorEvidenceAdmissionPolicyV3.ACTOR_COUNT;
    static final int MAX_OUTSTANDING_PER_ACTOR =
            AllocatorEvidenceAdmissionPolicyV3.MAX_ASYNC_OUTSTANDING_PER_ACTOR;
    static final int MAX_GLOBAL_OUTSTANDING = AllocatorEvidenceAdmissionPolicyV3.MAX_GLOBAL_OUTSTANDING;
    static final Duration FORMAL_WARMUP = Duration.ofSeconds(10);
    static final Duration FORMAL_MEASUREMENT = Duration.ofSeconds(30);
    static final Duration FORMAL_CLEANUP_GRACE = Duration.ofSeconds(5);

    private final Duration warmup;
    private final Duration measurement;
    private final Duration cleanupGrace;

    M3V3AsyncActorLaneRunner(Duration warmup, Duration measurement, Duration cleanupGrace) {
        this.warmup = requirePositiveOrZero(warmup, "warmup");
        this.measurement = requirePositive(measurement, "measurement");
        this.cleanupGrace = requirePositive(cleanupGrace, "cleanup grace");
    }

    static <T> M3V3AsyncActorLaneRunner<T> formal() {
        return new M3V3AsyncActorLaneRunner<>(FORMAL_WARMUP, FORMAL_MEASUREMENT, FORMAL_CLEANUP_GRACE);
    }

    IntervalResult run(
            int offeredRate,
            List<ScheduledOffer<T>> schedule,
            AdmissionMode admissionMode,
            ActorOperation<T> operation)
            throws InterruptedException {
        Objects.requireNonNull(admissionMode, "admissionMode");
        Objects.requireNonNull(operation, "operation");
        List<ScheduledOffer<T>> exactSchedule = validateSchedule(schedule);
        int queueCapacity = AllocatorEvidenceAdmissionPolicyV3.preAdmissionQueueCapacity(offeredRate);
        long warmupNanos = warmup.toNanos();
        long intervalNanos = Math.addExact(warmupNanos, measurement.toNanos());
        long startNanos = System.nanoTime();
        long measurementStartNanos = Math.addExact(startNanos, warmupNanos);
        long cutoffNanos = Math.addExact(startNanos, intervalNanos);
        long cleanupDeadlineNanos = Math.addExact(cutoffNanos, cleanupGrace.toNanos());
        RunState<T> state = new RunState<>(queueCapacity, cutoffNanos, cleanupDeadlineNanos, admissionMode);
        CountDownLatch laneStops = new CountDownLatch(ACTOR_COUNT);
        List<Thread> lanes = startLanes(state, operation, laneStops);

        try {
            boolean measurementStarted = warmupNanos == 0;
            if (measurementStarted) {
                state.beginMeasurement();
            }
            for (ScheduledOffer<T> offer : exactSchedule) {
                long targetNanos = Math.addExact(startNanos, offer.arrivalOffsetNanos());
                waitUntil(targetNanos);
                if (!measurementStarted && offer.measured()) {
                    state.beginMeasurement();
                    measurementStarted = true;
                }
                state.offer(offer, targetNanos);
            }
            if (!measurementStarted) {
                waitUntil(measurementStartNanos);
                state.beginMeasurement();
            }
            waitUntil(cutoffNanos);
            state.cutoff();
            state.awaitQuiescence(cleanupDeadlineNanos);
            state.timeoutActiveAtCleanupDeadline();
            long remaining = Math.max(0, cleanupDeadlineNanos - System.nanoTime());
            laneStops.await(remaining, TimeUnit.NANOSECONDS);
            lanes.forEach(Thread::interrupt);
            return state.result(laneStops.getCount() == 0);
        } catch (InterruptedException failure) {
            state.cutoff();
            state.timeoutActiveAtCleanupDeadline();
            lanes.forEach(Thread::interrupt);
            throw failure;
        }
    }

    IntervalResult run(int offeredRate, List<ScheduledOffer<T>> schedule, ActorOperation<T> operation)
            throws InterruptedException {
        return run(offeredRate, schedule, AdmissionMode.NORMAL_SINGLE_FLIGHT, operation);
    }

    private List<Thread> startLanes(RunState<T> state, ActorOperation<T> operation, CountDownLatch laneStops) {
        List<Thread> threads = new ArrayList<>(ACTOR_COUNT);
        for (int actorId = 0; actorId < ACTOR_COUNT; actorId++) {
            int exactActorId = actorId;
            Thread thread = new Thread(
                    () -> {
                        try {
                            runLane(state, exactActorId, operation);
                        } finally {
                            laneStops.countDown();
                        }
                    },
                    "m3-v3-allocator-async-actor-" + actorId);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }
        return List.copyOf(threads);
    }

    private void runLane(RunState<T> state, int actorId, ActorOperation<T> operation) {
        while (!Thread.currentThread().isInterrupted()) {
            AdmittedRequest<T> admitted;
            try {
                admitted = state.admitNext(actorId);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                return;
            }
            if (admitted == null) {
                return;
            }
            state.firstDispatch(admitted);
            try {
                CompletionStage<?> stage = Objects.requireNonNull(
                        operation.execute(actorId, admitted.offer().request(), admitted.context()),
                        "allocator V3 actor operation stage");
                stage.whenComplete((ignored, failure) -> {
                    admitted.context().markOperationCompleted();
                    state.finish(
                            admitted,
                            failure == null ? TerminalOutcome.COMPLETED : TerminalOutcome.FAILED_AFTER_ADMISSION,
                            failure == null ? null : unwrap(failure));
                });
            } catch (RuntimeException failure) {
                state.finish(admitted, TerminalOutcome.FAILED_AFTER_ADMISSION, unwrap(failure));
            } catch (Error failure) {
                state.finish(admitted, TerminalOutcome.FAILED_AFTER_ADMISSION, failure);
            }
        }
    }

    List<ScheduledOffer<T>> validateSchedule(List<ScheduledOffer<T>> schedule) {
        List<ScheduledOffer<T>> exact = List.copyOf(Objects.requireNonNull(schedule, "schedule"));
        long intervalNanos = Math.addExact(warmup.toNanos(), measurement.toNanos());
        long previousOffset = -1;
        Set<Long> ordinals = new HashSet<>();
        for (ScheduledOffer<T> offer : exact) {
            Objects.requireNonNull(offer, "scheduled offer");
            if (offer.actorId() < 0
                    || offer.actorId() >= ACTOR_COUNT
                    || offer.arrivalOffsetNanos() < 0
                    || offer.arrivalOffsetNanos() >= intervalNanos
                    || offer.arrivalOffsetNanos() < previousOffset
                    || !ordinals.add(offer.ordinal())) {
                throw new IllegalArgumentException("allocator V3 schedule is not ordered, unique, and in bounds");
            }
            previousOffset = offer.arrivalOffsetNanos();
        }
        validatePhaseTransitions(exact, warmup.isZero());
        return exact;
    }

    static void validatePhaseTransitions(
            List<? extends ScheduledOffer<?>> schedule, boolean measurementStartsImmediately) {
        boolean measuredSeen = measurementStartsImmediately;
        for (ScheduledOffer<?> offer : Objects.requireNonNull(schedule, "schedule")) {
            Objects.requireNonNull(offer, "scheduled offer");
            if (!offer.measured() && measuredSeen) {
                throw new IllegalArgumentException("allocator V3 schedule returned to warmup after measurement began");
            }
            measuredSeen |= offer.measured();
        }
    }

    private static void waitUntil(long targetNanos) throws InterruptedException {
        while (true) {
            long remaining = targetNanos - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(2)));
            if (Thread.interrupted()) {
                throw new InterruptedException("allocator V3 interval coordinator was interrupted");
            }
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Duration requirePositive(Duration value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("allocator V3 " + label + " must be positive");
        }
        return value;
    }

    private static Duration requirePositiveOrZero(Duration value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isNegative()) {
            throw new IllegalArgumentException("allocator V3 " + label + " cannot be negative");
        }
        return value;
    }

    enum AdmissionMode {
        NORMAL_SINGLE_FLIGHT,
        CONFLICT_PROOF
    }

    record ScheduledOffer<T>(
            long ordinal,
            int actorId,
            long bindingOrdinal,
            long arrivalOffsetNanos,
            boolean measured,
            T request) {
        ScheduledOffer {
            if (ordinal < 0 || bindingOrdinal < 0) {
                throw new IllegalArgumentException("allocator V3 request or binding ordinal cannot be negative");
            }
            Objects.requireNonNull(request, "request");
        }
    }

    @FunctionalInterface
    interface ActorOperation<T> {
        CompletionStage<?> execute(int actorId, T request, OperationContext context);
    }

    static final class OperationContext {
        private final long cleanupDeadlineNanos;
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final AtomicLong operationCompletedNanos = new AtomicLong();

        private OperationContext(long cleanupDeadlineNanos) {
            this.cleanupDeadlineNanos = cleanupDeadlineNanos;
        }

        boolean allowsNextMetadataOperation() {
            return active.get() && System.nanoTime() < cleanupDeadlineNanos;
        }

        void markOperationCompleted() {
            operationCompletedNanos.compareAndSet(0, System.nanoTime());
        }

        private void close() {
            active.set(false);
        }

        private long operationCompletedNanos(long fallback) {
            long exact = operationCompletedNanos.get();
            return exact == 0 ? fallback : exact;
        }
    }

    enum TerminalOutcome {
        OVERLOAD_DROPPED_BEFORE_ADMISSION,
        COMPLETED,
        FAILED_AFTER_ADMISSION,
        TIMED_OUT_AFTER_ADMISSION
    }

    record TerminalRecord(
            long ordinal, int actorId, long bindingOrdinal, TerminalOutcome outcome, String failureSummary) {
        TerminalRecord {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(failureSummary, "failureSummary");
        }
    }

    record RequestTelemetry(
            long ordinal,
            int actorId,
            long bindingOrdinal,
            long schedulerFiringLagMicros,
            long queueWaitMicros,
            long operationMicros,
            long callbackLagMicros,
            TerminalOutcome outcome,
            String failureSummary) {
        RequestTelemetry {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(failureSummary, "failureSummary");
        }
    }

    record IntervalResult(
            int queueCapacity,
            List<Integer> laneQueueCapacities,
            long offered,
            long admitted,
            long overloadDroppedBeforeAdmission,
            long completed,
            long failedAfterAdmission,
            long timedOutAfterAdmission,
            long terminal,
            long queueDepthMaximum,
            long queueDepthAtEnd,
            long globalOutstandingP50,
            long globalOutstandingP99,
            long globalOutstandingMaximum,
            long globalOutstandingAtEnd,
            List<Integer> perActorOutstandingMaximum,
            long bindingBusyMaximum,
            long bindingBusyAtEnd,
            long pendingPermitMaximum,
            long pendingPermitAtEnd,
            long queueWaitP99Micros,
            long queueWaitMaximumMicros,
            long schedulerFiringLagP99Micros,
            long callbackLagP99Micros,
            long rolloverP99Micros,
            long warmupOffered,
            long warmupDroppedBeforeAdmission,
            long warmupCompleted,
            long warmupFailedAfterAdmission,
            long warmupTimedOutAfterAdmission,
            String warmupFirstFailure,
            boolean actorLanesStoppedAtCleanupDeadline,
            List<TerminalRecord> measuredTerminals,
            List<RequestTelemetry> measuredTelemetry) {
        IntervalResult {
            laneQueueCapacities = List.copyOf(laneQueueCapacities);
            perActorOutstandingMaximum = List.copyOf(perActorOutstandingMaximum);
            measuredTerminals = List.copyOf(measuredTerminals);
            measuredTelemetry = List.copyOf(measuredTelemetry);
            Objects.requireNonNull(warmupFirstFailure, "warmupFirstFailure");
            long measuredTerminal = Math.addExact(
                    completed, Math.addExact(failedAfterAdmission, timedOutAfterAdmission));
            long warmupTerminal = Math.addExact(
                    warmupCompleted, Math.addExact(warmupFailedAfterAdmission, warmupTimedOutAfterAdmission));
            if (laneQueueCapacities.size() != ACTOR_COUNT
                    || perActorOutstandingMaximum.size() != ACTOR_COUNT
                    || laneQueueCapacities.stream().mapToInt(Integer::intValue).sum() != queueCapacity
                    || offered != Math.addExact(overloadDroppedBeforeAdmission, measuredTerminal)
                    || admitted != measuredTerminal
                    || terminal != measuredTerminal
                    || measuredTerminals.size() != Math.toIntExact(offered)
                    || measuredTelemetry.size() != Math.toIntExact(offered)
                    || warmupOffered != Math.addExact(warmupDroppedBeforeAdmission, warmupTerminal)
                    || (warmupFailedAfterAdmission == 0) != warmupFirstFailure.isEmpty()) {
                throw new IllegalArgumentException("allocator V3 asynchronous inventory differs");
            }
        }
    }

    private static final class RunState<T> {
        private final int queueCapacity;
        private final long cutoffNanos;
        private final long cleanupDeadlineNanos;
        private final AdmissionMode admissionMode;
        private final List<Integer> laneCapacities;
        private final List<ArrayDeque<QueuedRequest<T>>> queues = new ArrayList<>(ACTOR_COUNT);
        private final Map<Long, AdmittedRequest<T>> active = new LinkedHashMap<>();
        private final Map<Long, Integer> bindingOutstanding = new HashMap<>();
        private final int[] actorOutstanding = new int[ACTOR_COUNT];
        private final boolean[] actorWaiting = new boolean[ACTOR_COUNT];
        private final Metrics metrics = new Metrics();
        private boolean accepting = true;

        private RunState(
                int queueCapacity, long cutoffNanos, long cleanupDeadlineNanos, AdmissionMode admissionMode) {
            this.queueCapacity = queueCapacity;
            this.cutoffNanos = cutoffNanos;
            this.cleanupDeadlineNanos = cleanupDeadlineNanos;
            this.admissionMode = admissionMode;
            List<Integer> capacities = new ArrayList<>(ACTOR_COUNT);
            int base = queueCapacity / ACTOR_COUNT;
            int remainder = queueCapacity % ACTOR_COUNT;
            for (int actorId = 0; actorId < ACTOR_COUNT; actorId++) {
                int capacity = base + (actorId < remainder ? 1 : 0);
                capacities.add(capacity);
                queues.add(new ArrayDeque<>(capacity));
            }
            laneCapacities = List.copyOf(capacities);
        }

        private synchronized void beginMeasurement() {
            for (ArrayDeque<QueuedRequest<T>> queue : queues) {
                while (!queue.isEmpty()) {
                    QueuedRequest<T> queued = queue.removeFirst();
                    if (queued.offer().measured()) {
                        throw new IllegalStateException("allocator V3 measured request was queued before its phase");
                    }
                    metrics.dequeued();
                    metrics.dropped(queued.offer(), queued.schedulerFiringLagNanos());
                }
            }
            metrics.beginMeasurement(active.size(), actorOutstanding, bindingOutstanding.size());
            notifyAll();
        }

        private synchronized void offer(ScheduledOffer<T> offer, long targetNanos) {
            long now = System.nanoTime();
            long schedulerLag = Math.max(0, now - targetNanos);
            metrics.offered(offer.measured(), schedulerLag);
            ArrayDeque<QueuedRequest<T>> queue = queues.get(offer.actorId());
            if (!accepting || now >= cutoffNanos || queue.size() >= laneCapacities.get(offer.actorId())) {
                metrics.dropped(offer, schedulerLag);
                return;
            }
            queue.addLast(new QueuedRequest<>(offer, now, schedulerLag));
            metrics.enqueued();
            notifyAll();
        }

        private synchronized AdmittedRequest<T> admitNext(int actorId) throws InterruptedException {
            ArrayDeque<QueuedRequest<T>> queue = queues.get(actorId);
            try {
                while (true) {
                    while (queue.isEmpty() && accepting && System.nanoTime() < cutoffNanos) {
                        waitBounded(cutoffNanos);
                    }
                    if (queue.isEmpty()) {
                        return null;
                    }
                    QueuedRequest<T> queued = queue.peekFirst();
                    if (!accepting || System.nanoTime() >= cutoffNanos) {
                        queue.removeFirst();
                        metrics.dequeued();
                        metrics.dropped(queued.offer(), queued.schedulerFiringLagNanos());
                        continue;
                    }
                    if (!canAdmit(queued.offer())) {
                        setWaiting(actorId, true);
                        waitBounded(cutoffNanos);
                        continue;
                    }
                    setWaiting(actorId, false);
                    queue.removeFirst();
                    metrics.dequeued();
                    long now = System.nanoTime();
                    OperationContext context = new OperationContext(cleanupDeadlineNanos);
                    AdmittedRequest<T> admitted = new AdmittedRequest<>(queued, now, context);
                    if (active.putIfAbsent(queued.offer().ordinal(), admitted) != null) {
                        throw new IllegalStateException("allocator V3 active ordinal aliases another request");
                    }
                    actorOutstanding[actorId]++;
                    bindingOutstanding.merge(queued.offer().bindingOrdinal(), 1, Math::addExact);
                    metrics.admitted(admitted, active.size(), actorOutstanding, bindingOutstanding.size());
                    return admitted;
                }
            } finally {
                setWaiting(actorId, false);
            }
        }

        private boolean canAdmit(ScheduledOffer<T> offer) {
            boolean bindingAvailable = admissionMode == AdmissionMode.CONFLICT_PROOF
                    || !bindingOutstanding.containsKey(offer.bindingOrdinal());
            return actorOutstanding[offer.actorId()] < MAX_OUTSTANDING_PER_ACTOR
                    && active.size() < MAX_GLOBAL_OUTSTANDING
                    && bindingAvailable;
        }

        private synchronized void firstDispatch(AdmittedRequest<T> admitted) {
            admitted.firstDispatchNanos().compareAndSet(0, System.nanoTime());
        }

        private synchronized void finish(
                AdmittedRequest<T> admitted, TerminalOutcome requestedOutcome, Throwable terminalFailure) {
            long now = System.nanoTime();
            TerminalOutcome outcome = now <= cleanupDeadlineNanos
                    ? requestedOutcome
                    : TerminalOutcome.TIMED_OUT_AFTER_ADMISSION;
            if (!admitted.terminal().compareAndSet(false, true)) {
                return;
            }
            admitted.context().close();
            ScheduledOffer<T> offer = admitted.offer();
            if (active.remove(offer.ordinal()) != admitted) {
                throw new IllegalStateException("allocator V3 terminal does not match its active request");
            }
            actorOutstanding[offer.actorId()]--;
            bindingOutstanding.compute(offer.bindingOrdinal(), (ignored, count) -> {
                if (count == null || count <= 0) {
                    throw new IllegalStateException("allocator V3 binding outstanding gauge became invalid");
                }
                return count == 1 ? null : count - 1;
            });
            metrics.finished(
                    admitted,
                    outcome,
                    terminalFailure,
                    now,
                    active.size(),
                    actorOutstanding,
                    bindingOutstanding.size());
            notifyAll();
        }

        private synchronized void cutoff() {
            if (!accepting) {
                return;
            }
            accepting = false;
            for (ArrayDeque<QueuedRequest<T>> queue : queues) {
                while (!queue.isEmpty()) {
                    QueuedRequest<T> queued = queue.removeFirst();
                    metrics.dequeued();
                    metrics.dropped(queued.offer(), queued.schedulerFiringLagNanos());
                }
            }
            notifyAll();
        }

        private synchronized void awaitQuiescence(long deadlineNanos) throws InterruptedException {
            while (!active.isEmpty() && System.nanoTime() < deadlineNanos) {
                waitBounded(deadlineNanos);
            }
        }

        private synchronized void timeoutActiveAtCleanupDeadline() {
            for (AdmittedRequest<T> admitted : List.copyOf(active.values())) {
                finish(admitted, TerminalOutcome.TIMED_OUT_AFTER_ADMISSION, null);
            }
            notifyAll();
        }

        private synchronized IntervalResult result(boolean lanesStopped) {
            return metrics.result(queueCapacity, laneCapacities, lanesStopped);
        }

        private void setWaiting(int actorId, boolean waiting) {
            if (actorWaiting[actorId] != waiting) {
                actorWaiting[actorId] = waiting;
                metrics.pendingPermits(waitingCount());
            }
        }

        private int waitingCount() {
            int count = 0;
            for (boolean waiting : actorWaiting) {
                if (waiting) {
                    count++;
                }
            }
            return count;
        }

        private void waitBounded(long deadlineNanos) throws InterruptedException {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining > 0) {
                long millis = Math.max(1, Math.min(TimeUnit.NANOSECONDS.toMillis(remaining), 10));
                wait(millis);
            }
        }
    }

    private record QueuedRequest<T>(
            ScheduledOffer<T> offer, long enqueuedNanos, long schedulerFiringLagNanos) {}

    private record AdmittedRequest<T>(
            QueuedRequest<T> queued,
            long admittedNanos,
            OperationContext context,
            AtomicLong firstDispatchNanos,
            AtomicBoolean terminal) {
        private AdmittedRequest(QueuedRequest<T> queued, long admittedNanos, OperationContext context) {
            this(queued, admittedNanos, context, new AtomicLong(), new AtomicBoolean());
        }

        private ScheduledOffer<T> offer() {
            return queued.offer();
        }
    }

    private static final class Metrics {
        private boolean measuring;
        private long offered;
        private long admitted;
        private long dropped;
        private long completed;
        private long failed;
        private long timedOut;
        private long queueDepth;
        private long queueDepthMaximum;
        private long globalOutstandingMaximum;
        private final int[] perActorOutstandingMaximum = new int[ACTOR_COUNT];
        private long bindingBusyMaximum;
        private long pendingPermit;
        private long pendingPermitMaximum;
        private final List<Long> globalOutstandingSamples = new ArrayList<>();
        private final List<Long> queueWaitMicros = new ArrayList<>();
        private final List<Long> schedulerLagMicros = new ArrayList<>();
        private final List<Long> callbackLagMicros = new ArrayList<>();
        private final List<Long> rolloverMicros = new ArrayList<>();
        private final List<TerminalRecord> measuredTerminals = new ArrayList<>();
        private final List<RequestTelemetry> measuredTelemetry = new ArrayList<>();
        private long warmupOffered;
        private long warmupDropped;
        private long warmupCompleted;
        private long warmupFailed;
        private long warmupTimedOut;
        private String warmupFirstFailure = "";

        private void beginMeasurement(int globalOutstanding, int[] actorOutstanding, int bindingBusy) {
            if (measuring) {
                throw new IllegalStateException("allocator V3 measurement boundary repeated");
            }
            measuring = true;
            queueDepthMaximum = queueDepth;
            sampleOutstanding(globalOutstanding, actorOutstanding, bindingBusy);
        }

        private void offered(boolean measured, long schedulerLagNanos) {
            if (measured) {
                requireMeasuring();
                offered++;
                schedulerLagMicros.add(TimeUnit.NANOSECONDS.toMicros(schedulerLagNanos));
            } else {
                warmupOffered++;
            }
        }

        private void enqueued() {
            queueDepth++;
            if (measuring) {
                queueDepthMaximum = Math.max(queueDepthMaximum, queueDepth);
            }
        }

        private void dequeued() {
            queueDepth--;
            if (queueDepth < 0) {
                throw new IllegalStateException("allocator V3 physical queue depth became negative");
            }
        }

        private void admitted(
                AdmittedRequest<?> request, int globalOutstanding, int[] actorOutstanding, int bindingBusy) {
            if (request.offer().measured()) {
                admitted++;
                queueWaitMicros.add(
                        TimeUnit.NANOSECONDS.toMicros(request.admittedNanos() - request.queued().enqueuedNanos()));
            }
            if (actorOutstanding[request.offer().actorId()] > MAX_OUTSTANDING_PER_ACTOR
                    || globalOutstanding > MAX_GLOBAL_OUTSTANDING) {
                throw new IllegalStateException("allocator V3 asynchronous outstanding bound was exceeded");
            }
            if (measuring) {
                sampleOutstanding(globalOutstanding, actorOutstanding, bindingBusy);
            }
        }

        private void dropped(ScheduledOffer<?> offer, long schedulerLagNanos) {
            if (offer.measured()) {
                dropped++;
                long schedulerMicros = TimeUnit.NANOSECONDS.toMicros(schedulerLagNanos);
                measuredTerminals.add(new TerminalRecord(
                        offer.ordinal(),
                        offer.actorId(),
                        offer.bindingOrdinal(),
                        TerminalOutcome.OVERLOAD_DROPPED_BEFORE_ADMISSION,
                        "PRE_ADMISSION_CUTOFF"));
                measuredTelemetry.add(new RequestTelemetry(
                        offer.ordinal(),
                        offer.actorId(),
                        offer.bindingOrdinal(),
                        schedulerMicros,
                        0,
                        0,
                        0,
                        TerminalOutcome.OVERLOAD_DROPPED_BEFORE_ADMISSION,
                        "PRE_ADMISSION_CUTOFF"));
            } else {
                warmupDropped++;
            }
        }

        private void finished(
                AdmittedRequest<?> request,
                TerminalOutcome outcome,
                Throwable terminalFailure,
                long terminalNanos,
                int globalOutstanding,
                int[] actorOutstanding,
                int bindingBusy) {
            if (measuring) {
                sampleOutstanding(globalOutstanding, actorOutstanding, bindingBusy);
            }
            if (request.offer().measured()) {
                long operationCompleted = request.context().operationCompletedNanos(terminalNanos);
                long callbackLag = Math.max(0, terminalNanos - operationCompleted);
                long operationNanos = Math.max(0, operationCompleted - request.firstDispatchNanos().get());
                long rolloverNanos = Math.max(0, terminalNanos - request.admittedNanos());
                callbackLagMicros.add(TimeUnit.NANOSECONDS.toMicros(callbackLag));
                rolloverMicros.add(TimeUnit.NANOSECONDS.toMicros(rolloverNanos));
                switch (outcome) {
                    case COMPLETED -> completed++;
                    case FAILED_AFTER_ADMISSION -> failed++;
                    case TIMED_OUT_AFTER_ADMISSION -> timedOut++;
                    case OVERLOAD_DROPPED_BEFORE_ADMISSION ->
                        throw new IllegalArgumentException("admitted request cannot receive a pre-admission drop");
                }
                String failure = switch (outcome) {
                    case COMPLETED -> "";
                    case FAILED_AFTER_ADMISSION -> failureSummary(terminalFailure);
                    case TIMED_OUT_AFTER_ADMISSION -> "CLEANUP_DEADLINE";
                    case OVERLOAD_DROPPED_BEFORE_ADMISSION ->
                        throw new IllegalArgumentException("admitted request cannot receive a pre-admission drop");
                };
                measuredTerminals.add(new TerminalRecord(
                        request.offer().ordinal(),
                        request.offer().actorId(),
                        request.offer().bindingOrdinal(),
                        outcome,
                        failure));
                measuredTelemetry.add(new RequestTelemetry(
                        request.offer().ordinal(),
                        request.offer().actorId(),
                        request.offer().bindingOrdinal(),
                        TimeUnit.NANOSECONDS.toMicros(request.queued().schedulerFiringLagNanos()),
                        TimeUnit.NANOSECONDS.toMicros(request.admittedNanos() - request.queued().enqueuedNanos()),
                        TimeUnit.NANOSECONDS.toMicros(operationNanos),
                        TimeUnit.NANOSECONDS.toMicros(callbackLag),
                        outcome,
                        failure));
            } else {
                switch (outcome) {
                    case COMPLETED -> warmupCompleted++;
                    case FAILED_AFTER_ADMISSION -> {
                        warmupFailed++;
                        if (warmupFirstFailure.isEmpty()) {
                            warmupFirstFailure = failureSummary(terminalFailure);
                        }
                    }
                    case TIMED_OUT_AFTER_ADMISSION -> warmupTimedOut++;
                    case OVERLOAD_DROPPED_BEFORE_ADMISSION ->
                        throw new IllegalArgumentException("admitted warmup cannot receive a pre-admission drop");
                }
            }
        }

        private void pendingPermits(int value) {
            pendingPermit = value;
            if (measuring) {
                pendingPermitMaximum = Math.max(pendingPermitMaximum, value);
            }
        }

        private void sampleOutstanding(int global, int[] actors, int bindingBusy) {
            globalOutstandingSamples.add((long) global);
            globalOutstandingMaximum = Math.max(globalOutstandingMaximum, global);
            bindingBusyMaximum = Math.max(bindingBusyMaximum, bindingBusy);
            for (int actorId = 0; actorId < ACTOR_COUNT; actorId++) {
                perActorOutstandingMaximum[actorId] =
                        Math.max(perActorOutstandingMaximum[actorId], actors[actorId]);
            }
        }

        private IntervalResult result(int queueCapacity, List<Integer> laneCapacities, boolean lanesStopped) {
            List<TerminalRecord> terminals = measuredTerminals.stream()
                    .sorted(Comparator.comparingLong(TerminalRecord::ordinal))
                    .toList();
            List<RequestTelemetry> telemetry = measuredTelemetry.stream()
                    .sorted(Comparator.comparingLong(RequestTelemetry::ordinal))
                    .toList();
            long terminal = Math.addExact(completed, Math.addExact(failed, timedOut));
            return new IntervalResult(
                    queueCapacity,
                    laneCapacities,
                    offered,
                    admitted,
                    dropped,
                    completed,
                    failed,
                    timedOut,
                    terminal,
                    queueDepthMaximum,
                    queueDepth,
                    percentile(globalOutstandingSamples, 0.50),
                    percentile(globalOutstandingSamples, 0.99),
                    globalOutstandingMaximum,
                    0,
                    List.of(
                            perActorOutstandingMaximum[0],
                            perActorOutstandingMaximum[1],
                            perActorOutstandingMaximum[2],
                            perActorOutstandingMaximum[3]),
                    bindingBusyMaximum,
                    0,
                    pendingPermitMaximum,
                    pendingPermit,
                    percentile(queueWaitMicros, 0.99),
                    queueWaitMicros.stream().mapToLong(Long::longValue).max().orElse(0),
                    percentile(schedulerLagMicros, 0.99),
                    percentile(callbackLagMicros, 0.99),
                    percentile(rolloverMicros, 0.99),
                    warmupOffered,
                    warmupDropped,
                    warmupCompleted,
                    warmupFailed,
                    warmupTimedOut,
                    warmupFirstFailure,
                    lanesStopped,
                    terminals,
                    telemetry);
        }

        private static String failureSummary(Throwable failure) {
            Throwable exact = unwrap(Objects.requireNonNull(failure, "terminalFailure"));
            String type = exact.getClass().getSimpleName();
            if (exact instanceof AllocatorProtocolException protocolFailure) {
                type += '[' + protocolFailure.code().name() + ']';
            }
            String message = exact.getMessage();
            String summary = message == null || message.isBlank() ? type : type + ':' + message;
            String printable = summary.replaceAll("[^\\x20-\\x7E]", "?");
            return printable.substring(0, Math.min(printable.length(), 512));
        }

        private void requireMeasuring() {
            if (!measuring) {
                throw new IllegalStateException("allocator V3 measured offer preceded its measurement boundary");
            }
        }

        private static long percentile(List<Long> values, double percentile) {
            if (values.isEmpty()) {
                return 0;
            }
            List<Long> sorted = values.stream().sorted().toList();
            int index = Math.max(0, (int) Math.ceil(sorted.size() * percentile) - 1);
            return sorted.get(index);
        }
    }
}
