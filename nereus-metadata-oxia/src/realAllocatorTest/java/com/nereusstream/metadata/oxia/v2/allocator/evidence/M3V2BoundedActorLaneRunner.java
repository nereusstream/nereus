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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * ADR-0104 physical admission boundary: four actor lanes, one in-flight request per lane, and no correctness lock.
 * The queue is bounded before the run begins and a request that has not been admitted at cutoff receives only the
 * overload-drop terminal. Admitted requests share one immutable cleanup deadline.
 */
final class M3V2BoundedActorLaneRunner<T> {
    static final int ACTOR_COUNT = 4;
    static final Duration FORMAL_WARMUP = Duration.ofSeconds(10);
    static final Duration FORMAL_MEASUREMENT = Duration.ofSeconds(30);
    static final Duration FORMAL_CLEANUP_GRACE = Duration.ofSeconds(5);

    private final Duration warmup;
    private final Duration measurement;
    private final Duration cleanupGrace;

    M3V2BoundedActorLaneRunner(Duration warmup, Duration measurement, Duration cleanupGrace) {
        this.warmup = requirePositiveOrZero(warmup, "warmup");
        this.measurement = requirePositive(measurement, "measurement");
        this.cleanupGrace = requirePositive(cleanupGrace, "cleanup grace");
    }

    static <T> M3V2BoundedActorLaneRunner<T> formal() {
        return new M3V2BoundedActorLaneRunner<>(FORMAL_WARMUP, FORMAL_MEASUREMENT, FORMAL_CLEANUP_GRACE);
    }

    IntervalResult run(int offeredRate, List<ScheduledOffer<T>> schedule, ActorOperation<T> operation)
            throws InterruptedException {
        if (offeredRate <= 0) {
            throw new IllegalArgumentException("allocator V2 offered rate must be positive");
        }
        Objects.requireNonNull(operation, "operation");
        List<ScheduledOffer<T>> exactSchedule = validateSchedule(schedule);
        int queueCapacity = Math.multiplyExact(2, offeredRate);
        long warmupNanos = warmup.toNanos();
        long intervalNanos = Math.addExact(warmupNanos, measurement.toNanos());
        long cleanupNanos = cleanupGrace.toNanos();
        long startNanos = System.nanoTime();
        long measurementStartNanos = Math.addExact(startNanos, warmupNanos);
        long cutoffNanos = Math.addExact(startNanos, intervalNanos);
        long cleanupDeadlineNanos = Math.addExact(cutoffNanos, cleanupNanos);
        RunState<T> state = new RunState<>(queueCapacity, cutoffNanos);
        CountDownLatch laneStops = new CountDownLatch(ACTOR_COUNT);
        List<Thread> lanes = startLanes(state, cleanupDeadlineNanos, operation, laneStops);

        try {
            boolean measurementStarted = warmupNanos == 0;
            if (measurementStarted) {
                state.beginMeasurement();
            }
            for (ScheduledOffer<T> offer : exactSchedule) {
                long target = Math.addExact(startNanos, offer.arrivalOffsetNanos());
                waitUntil(target);
                if (!measurementStarted && offer.measured()) {
                    state.beginMeasurement();
                    measurementStarted = true;
                }
                state.offer(offer);
            }
            if (!measurementStarted) {
                waitUntil(measurementStartNanos);
                state.beginMeasurement();
            }
            waitUntil(cutoffNanos);
            state.cutoff();
            long remainingCleanup = Math.max(0, cleanupDeadlineNanos - System.nanoTime());
            laneStops.await(remainingCleanup, TimeUnit.NANOSECONDS);
            state.timeoutActiveAtCleanupDeadline();
            lanes.forEach(Thread::interrupt);
            return state.result(laneStops.getCount() == 0);
        } catch (InterruptedException failure) {
            state.cutoff();
            state.timeoutActiveAtCleanupDeadline();
            lanes.forEach(Thread::interrupt);
            throw failure;
        }
    }

    private List<Thread> startLanes(
            RunState<T> state,
            long cleanupDeadlineNanos,
            ActorOperation<T> operation,
            CountDownLatch laneStops) {
        List<Thread> threads = new ArrayList<>(ACTOR_COUNT);
        for (int actorId = 0; actorId < ACTOR_COUNT; actorId++) {
            int exactActorId = actorId;
            Thread thread = new Thread(
                    () -> {
                        try {
                            runLane(state, exactActorId, cleanupDeadlineNanos, operation);
                        } finally {
                            laneStops.countDown();
                        }
                    },
                    "m3-v2-allocator-actor-lane-" + actorId);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }
        return List.copyOf(threads);
    }

    private void runLane(
            RunState<T> state,
            int actorId,
            long cleanupDeadlineNanos,
            ActorOperation<T> operation) {
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
            TerminalOutcome outcome;
            try {
                CompletionStage<?> stage = Objects.requireNonNull(
                        operation.execute(actorId, admitted.offer().request()),
                        "allocator actor operation stage");
                CompletableFuture<Object> completion = new CompletableFuture<>();
                stage.whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        completion.complete(null);
                    } else {
                        completion.completeExceptionally(unwrap(failure));
                    }
                });
                long remaining = cleanupDeadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    outcome = TerminalOutcome.TIMED_OUT_AFTER_ADMISSION;
                } else {
                    completion.get(remaining, TimeUnit.NANOSECONDS);
                    outcome = System.nanoTime() <= cleanupDeadlineNanos
                            ? TerminalOutcome.COMPLETED
                            : TerminalOutcome.TIMED_OUT_AFTER_ADMISSION;
                }
            } catch (TimeoutException failure) {
                outcome = TerminalOutcome.TIMED_OUT_AFTER_ADMISSION;
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                outcome = TerminalOutcome.TIMED_OUT_AFTER_ADMISSION;
            } catch (ExecutionException | RuntimeException failure) {
                outcome = TerminalOutcome.FAILED_AFTER_ADMISSION;
            } catch (Error failure) {
                outcome = TerminalOutcome.FAILED_AFTER_ADMISSION;
            }
            state.finish(actorId, admitted, outcome);
        }
    }

    List<ScheduledOffer<T>> validateSchedule(List<ScheduledOffer<T>> schedule) {
        List<ScheduledOffer<T>> exact = List.copyOf(Objects.requireNonNull(schedule, "schedule"));
        long intervalNanos = Math.addExact(warmup.toNanos(), measurement.toNanos());
        long previousOffset = -1;
        Set<Long> ordinals = new HashSet<>();
        for (ScheduledOffer<T> offer : exact) {
            Objects.requireNonNull(offer, "scheduled offer");
            if (offer.actorId() < 0 || offer.actorId() >= ACTOR_COUNT
                    || offer.arrivalOffsetNanos() < 0
                    || offer.arrivalOffsetNanos() >= intervalNanos
                    || offer.arrivalOffsetNanos() < previousOffset
                    || !ordinals.add(offer.ordinal())) {
                throw new IllegalArgumentException("allocator V2 schedule is not ordered, unique, and in bounds");
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
                throw new IllegalArgumentException(
                        "allocator V2 schedule returned to warmup after measurement began");
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
                throw new InterruptedException("allocator V2 interval coordinator was interrupted");
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
            throw new IllegalArgumentException("allocator V2 " + label + " must be positive");
        }
        return value;
    }

    private static Duration requirePositiveOrZero(Duration value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isNegative()) {
            throw new IllegalArgumentException("allocator V2 " + label + " cannot be negative");
        }
        return value;
    }

    record ScheduledOffer<T>(long ordinal, int actorId, long arrivalOffsetNanos, boolean measured, T request) {
        ScheduledOffer {
            if (ordinal < 0) {
                throw new IllegalArgumentException("allocator V2 request ordinal cannot be negative");
            }
            Objects.requireNonNull(request, "request");
        }
    }

    @FunctionalInterface
    interface ActorOperation<T> {
        CompletionStage<?> execute(int actorId, T request);
    }

    enum TerminalOutcome {
        OVERLOAD_DROPPED_BEFORE_ADMISSION,
        COMPLETED,
        FAILED_AFTER_ADMISSION,
        TIMED_OUT_AFTER_ADMISSION
    }

    record TerminalRecord(long ordinal, int actorId, TerminalOutcome outcome) {}

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
            long backlogMaximum,
            long backlogAtEnd,
            long inFlightMaximum,
            long inFlightAtEnd,
            long waiterMaximum,
            long waiterAtEnd,
            List<Integer> perActorInFlightMaximum,
            long queueAgeP99Micros,
            long starvationMaximumMicros,
            long rolloverP99Micros,
            long warmupOffered,
            long warmupDroppedBeforeAdmission,
            long warmupCompleted,
            long warmupFailedAfterAdmission,
            long warmupTimedOutAfterAdmission,
            boolean actorLanesStoppedAtCleanupDeadline,
            List<TerminalRecord> measuredTerminals) {
        IntervalResult {
            laneQueueCapacities = List.copyOf(laneQueueCapacities);
            perActorInFlightMaximum = List.copyOf(perActorInFlightMaximum);
            measuredTerminals = List.copyOf(measuredTerminals);
            if (laneQueueCapacities.size() != ACTOR_COUNT
                    || perActorInFlightMaximum.size() != ACTOR_COUNT
                    || laneQueueCapacities.stream().mapToInt(Integer::intValue).sum() != queueCapacity) {
                throw new IllegalArgumentException("allocator V2 physical lane inventory differs");
            }
        }
    }

    private static final class RunState<T> {
        private final int queueCapacity;
        private final long cutoffNanos;
        private final List<Integer> laneCapacities;
        private final List<ArrayDeque<QueuedRequest<T>>> queues = new ArrayList<>(ACTOR_COUNT);
        private final List<AdmittedRequest<T>> active = new ArrayList<>(ACTOR_COUNT);
        private final Metrics metrics = new Metrics();
        private boolean accepting = true;

        private RunState(int queueCapacity, long cutoffNanos) {
            this.queueCapacity = queueCapacity;
            this.cutoffNanos = cutoffNanos;
            List<Integer> capacities = new ArrayList<>(ACTOR_COUNT);
            int base = queueCapacity / ACTOR_COUNT;
            int remainder = queueCapacity % ACTOR_COUNT;
            for (int actorId = 0; actorId < ACTOR_COUNT; actorId++) {
                int capacity = base + (actorId < remainder ? 1 : 0);
                capacities.add(capacity);
                queues.add(new ArrayDeque<>(capacity));
                active.add(null);
            }
            laneCapacities = List.copyOf(capacities);
        }

        private synchronized void beginMeasurement() {
            metrics.beginMeasurement();
        }

        private synchronized void offer(ScheduledOffer<T> offer) {
            metrics.offered(offer.measured());
            ArrayDeque<QueuedRequest<T>> queue = queues.get(offer.actorId());
            long now = System.nanoTime();
            if (!accepting || now >= cutoffNanos || queue.size() >= laneCapacities.get(offer.actorId())) {
                metrics.dropped(offer);
                return;
            }
            queue.addLast(new QueuedRequest<>(offer, now));
            metrics.enqueued();
            notifyAll();
        }

        private synchronized AdmittedRequest<T> admitNext(int actorId) throws InterruptedException {
            ArrayDeque<QueuedRequest<T>> queue = queues.get(actorId);
            while (true) {
                while (queue.isEmpty() && accepting && System.nanoTime() < cutoffNanos) {
                    long remaining = cutoffNanos - System.nanoTime();
                    long millis = Math.max(1, Math.min(TimeUnit.NANOSECONDS.toMillis(remaining), 10));
                    wait(millis);
                }
                if (queue.isEmpty()) {
                    return null;
                }
                QueuedRequest<T> queued = queue.removeFirst();
                metrics.dequeued();
                if (!accepting || System.nanoTime() >= cutoffNanos) {
                    metrics.dropped(queued.offer());
                    continue;
                }
                if (active.get(actorId) != null) {
                    throw new IllegalStateException("allocator V2 actor lane admitted more than one request");
                }
                AdmittedRequest<T> admitted =
                        new AdmittedRequest<>(queued.offer(), queued.enqueuedNanos(), System.nanoTime());
                active.set(actorId, admitted);
                metrics.admitted(admitted);
                return admitted;
            }
        }

        private synchronized void finish(int actorId, AdmittedRequest<T> admitted, TerminalOutcome outcome) {
            if (admitted.terminal().compareAndSet(false, true)) {
                if (active.get(actorId) != admitted) {
                    throw new IllegalStateException("allocator V2 actor terminal does not match its active request");
                }
                active.set(actorId, null);
                metrics.finished(admitted, outcome, System.nanoTime());
            }
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
                    metrics.dropped(queued.offer());
                }
            }
            notifyAll();
        }

        private synchronized void timeoutActiveAtCleanupDeadline() {
            for (int actorId = 0; actorId < active.size(); actorId++) {
                AdmittedRequest<T> admitted = active.get(actorId);
                if (admitted != null && admitted.terminal().compareAndSet(false, true)) {
                    active.set(actorId, null);
                    metrics.finished(admitted, TerminalOutcome.TIMED_OUT_AFTER_ADMISSION, System.nanoTime());
                }
            }
            notifyAll();
        }

        private synchronized IntervalResult result(boolean lanesStopped) {
            return metrics.result(queueCapacity, laneCapacities, lanesStopped);
        }
    }

    private record QueuedRequest<T>(ScheduledOffer<T> offer, long enqueuedNanos) {}

    private record AdmittedRequest<T>(
            ScheduledOffer<T> offer, long enqueuedNanos, long admittedNanos, AtomicBoolean terminal) {
        private AdmittedRequest(ScheduledOffer<T> offer, long enqueuedNanos, long admittedNanos) {
            this(offer, enqueuedNanos, admittedNanos, new AtomicBoolean());
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
        private long backlog;
        private long backlogMaximum;
        private long inFlight;
        private long inFlightMaximum;
        private final int[] perActorInFlight = new int[ACTOR_COUNT];
        private final int[] perActorInFlightMaximum = new int[ACTOR_COUNT];
        private final List<Long> queueAgesMicros = new ArrayList<>();
        private final List<Long> rolloverMicros = new ArrayList<>();
        private final List<TerminalRecord> measuredTerminals = new ArrayList<>();
        private long warmupOffered;
        private long warmupDropped;
        private long warmupCompleted;
        private long warmupFailed;
        private long warmupTimedOut;

        private void beginMeasurement() {
            if (measuring) {
                throw new IllegalStateException("allocator V2 measurement boundary repeated");
            }
            measuring = true;
            backlogMaximum = backlog;
            inFlightMaximum = inFlight;
            for (int actorId = 0; actorId < ACTOR_COUNT; actorId++) {
                perActorInFlightMaximum[actorId] = perActorInFlight[actorId];
            }
        }

        private void offered(boolean measured) {
            if (measured) {
                requireMeasuring();
                offered++;
            } else {
                warmupOffered++;
            }
        }

        private void enqueued() {
            backlog++;
            if (measuring) {
                backlogMaximum = Math.max(backlogMaximum, backlog);
            }
        }

        private void dequeued() {
            backlog--;
            if (backlog < 0) {
                throw new IllegalStateException("allocator V2 physical backlog became negative");
            }
        }

        private void admitted(AdmittedRequest<?> request) {
            int actorId = request.offer().actorId();
            inFlight++;
            perActorInFlight[actorId]++;
            if (perActorInFlight[actorId] > 1) {
                throw new IllegalStateException("allocator V2 actor lane exceeded one in-flight request");
            }
            if (measuring) {
                inFlightMaximum = Math.max(inFlightMaximum, inFlight);
                perActorInFlightMaximum[actorId] = Math.max(
                        perActorInFlightMaximum[actorId], perActorInFlight[actorId]);
            }
            if (request.offer().measured()) {
                admitted++;
                queueAgesMicros.add(TimeUnit.NANOSECONDS.toMicros(request.admittedNanos() - request.enqueuedNanos()));
            }
        }

        private void dropped(ScheduledOffer<?> offer) {
            if (offer.measured()) {
                dropped++;
                measuredTerminals.add(new TerminalRecord(
                        offer.ordinal(), offer.actorId(), TerminalOutcome.OVERLOAD_DROPPED_BEFORE_ADMISSION));
            } else {
                warmupDropped++;
            }
        }

        private void finished(AdmittedRequest<?> request, TerminalOutcome outcome, long terminalNanos) {
            int actorId = request.offer().actorId();
            inFlight--;
            perActorInFlight[actorId]--;
            if (inFlight < 0 || perActorInFlight[actorId] < 0) {
                throw new IllegalStateException("allocator V2 physical in-flight gauge became negative");
            }
            if (request.offer().measured()) {
                rolloverMicros.add(TimeUnit.NANOSECONDS.toMicros(terminalNanos - request.admittedNanos()));
                switch (outcome) {
                    case COMPLETED -> completed++;
                    case FAILED_AFTER_ADMISSION -> failed++;
                    case TIMED_OUT_AFTER_ADMISSION -> timedOut++;
                    case OVERLOAD_DROPPED_BEFORE_ADMISSION ->
                        throw new IllegalArgumentException("an admitted request cannot receive a pre-admission drop");
                }
                measuredTerminals.add(new TerminalRecord(request.offer().ordinal(), actorId, outcome));
            } else {
                switch (outcome) {
                    case COMPLETED -> warmupCompleted++;
                    case FAILED_AFTER_ADMISSION -> warmupFailed++;
                    case TIMED_OUT_AFTER_ADMISSION -> warmupTimedOut++;
                    case OVERLOAD_DROPPED_BEFORE_ADMISSION ->
                        throw new IllegalArgumentException("an admitted warmup cannot receive a pre-admission drop");
                }
            }
        }

        private IntervalResult result(int queueCapacity, List<Integer> laneCapacities, boolean lanesStopped) {
            List<TerminalRecord> terminals = measuredTerminals.stream()
                    .sorted(Comparator.comparingLong(TerminalRecord::ordinal))
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
                    backlogMaximum,
                    backlog,
                    inFlightMaximum,
                    inFlight,
                    0,
                    0,
                    List.of(
                            perActorInFlightMaximum[0],
                            perActorInFlightMaximum[1],
                            perActorInFlightMaximum[2],
                            perActorInFlightMaximum[3]),
                    percentile99(queueAgesMicros),
                    queueAgesMicros.stream().mapToLong(Long::longValue).max().orElse(0),
                    percentile99(rolloverMicros),
                    warmupOffered,
                    warmupDropped,
                    warmupCompleted,
                    warmupFailed,
                    warmupTimedOut,
                    lanesStopped,
                    terminals);
        }

        private void requireMeasuring() {
            if (!measuring) {
                throw new IllegalStateException("allocator V2 measured offer preceded its measurement boundary");
            }
        }

        private static long percentile99(List<Long> values) {
            if (values.isEmpty()) {
                return 0;
            }
            List<Long> sorted = values.stream().sorted().toList();
            int index = Math.max(0, (int) Math.ceil(sorted.size() * 0.99) - 1);
            return sorted.get(index);
        }
    }
}
