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

package com.nereusstream.kafka.fetch;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * Event-driven whole-request Fetch operation around a stock-owned read wave.
 *
 * <p>All state transitions and reads run on the supplied bounded read executor. At most one wave is in flight. Signals
 * arriving during a wave are coalesced, the deadline always gets one final read after the initial wave, and terminal
 * cleanup happens before the operation-owned completion is dispatched on the callback executor.
 */
public final class KafkaFetchWaveOperation<T> implements AutoCloseable {
    private final KafkaFetchWaveSource<T> source;
    private final int minBytes;
    private final Duration maxWait;
    private final int maxEventRereads;
    private final ToIntFunction<T> responseBytes;
    private final Predicate<T> forceComplete;
    private final Executor readExecutor;
    private final Executor callbackExecutor;
    private final ScheduledExecutorService deadlineScheduler;
    private final OperationFuture<KafkaFetchWaveResult<T>> completion = new OperationFuture<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean terminal = new AtomicBoolean();
    private final Object controlGuard = new Object();
    private final ArrayDeque<Runnable> controlTasks = new ArrayDeque<>();

    private volatile KafkaFetchOperationState state = KafkaFetchOperationState.NEW;
    private volatile ScheduledFuture<?> deadlineTask;
    private volatile CompletableFuture<T> inFlightFuture;
    private AutoCloseable subscription;
    private boolean controlScheduled;
    private volatile boolean inFlight;
    private boolean dirty;
    private boolean deadlineExpired;
    private boolean deadlineReadStarted;
    private int eventRereads;
    private volatile int readAttempts;

    public KafkaFetchWaveOperation(
            KafkaFetchWaveSource<T> source,
            int minBytes,
            Duration maxWait,
            int maxEventRereads,
            ToIntFunction<T> responseBytes,
            Predicate<T> forceComplete,
            Executor readExecutor,
            Executor callbackExecutor,
            ScheduledExecutorService deadlineScheduler) {
        this.source = Objects.requireNonNull(source, "source");
        this.maxWait = Objects.requireNonNull(maxWait, "maxWait");
        this.responseBytes = Objects.requireNonNull(responseBytes, "responseBytes");
        this.forceComplete = Objects.requireNonNull(forceComplete, "forceComplete");
        this.readExecutor = Objects.requireNonNull(readExecutor, "readExecutor");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.deadlineScheduler = Objects.requireNonNull(deadlineScheduler, "deadlineScheduler");
        if (minBytes < 0 || maxEventRereads <= 0 || maxWait.isNegative()) {
            throw new IllegalArgumentException("invalid Kafka Fetch wave limits");
        }
        try {
            maxWait.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Kafka Fetch maxWait is not nanosecond-representable", overflow);
        }
        this.minBytes = minBytes;
        this.maxEventRereads = maxEventRereads;
    }

    /** Registers all request signals before scheduling the initial wave. */
    public CompletableFuture<KafkaFetchWaveResult<T>> start() {
        if (!started.compareAndSet(false, true)) return completion;
        try {
            subscription = Objects.requireNonNull(
                    source.subscribe(this::signal), "Kafka Fetch wave subscription");
            if (terminal.get()) {
                closeSubscription();
                return completion;
            }
            if (!maxWait.isZero()) {
                deadlineTask = deadlineScheduler.schedule(
                        () -> dispatchControl(this::handleDeadline),
                        maxWait.toNanos(),
                        TimeUnit.NANOSECONDS);
            }
            dispatchControl(() -> beginRead(ReadKind.INITIAL));
        } catch (Throwable failure) {
            completeFailure(failure, KafkaFetchOperationState.COMPLETE);
        }
        return completion;
    }

    public KafkaFetchOperationState state() {
        return state;
    }

    public int readAttempts() {
        return readAttempts;
    }

    public boolean inFlight() {
        return inFlight;
    }

    /** Only broker lifecycle teardown calls this; cancelling the returned future is deliberately disabled. */
    public void cancel() {
        completeFailure(
                new NereusException(
                        ErrorCode.CANCELLED,
                        false,
                        "Kafka Fetch wave operation was cancelled"),
                KafkaFetchOperationState.CANCELLED);
    }

    @Override
    public void close() {
        cancel();
    }

    private void signal() {
        dispatchControl(this::handleSignal);
    }

    private void handleSignal() {
        if (terminal.get() || deadlineExpired) return;
        dirty = true;
        if (!inFlight && state == KafkaFetchOperationState.WAITING) {
            beginEventReadIfAllowed();
        }
    }

    private void handleDeadline() {
        if (terminal.get()) return;
        deadlineExpired = true;
        dirty = false;
        state = KafkaFetchOperationState.TIMED_READING;
        if (!inFlight) beginDeadlineRead();
    }

    private void beginEventReadIfAllowed() {
        if (eventRereads >= maxEventRereads) {
            state = KafkaFetchOperationState.WAITING;
            return;
        }
        eventRereads++;
        dirty = false;
        beginRead(ReadKind.EVENT);
    }

    private void beginDeadlineRead() {
        if (deadlineReadStarted) return;
        deadlineReadStarted = true;
        beginRead(ReadKind.DEADLINE);
    }

    private void beginRead(ReadKind kind) {
        if (terminal.get() || inFlight) return;
        inFlight = true;
        state = kind == ReadKind.DEADLINE
                ? KafkaFetchOperationState.TIMED_READING
                : KafkaFetchOperationState.READING;
        readAttempts++;
        CompletionStage<T> stage;
        try {
            stage = Objects.requireNonNull(
                    source.read(kind == ReadKind.INITIAL),
                    "Kafka Fetch wave read future");
        } catch (Throwable failure) {
            stage = CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<T> read = stage.toCompletableFuture();
        inFlightFuture = read;
        read.whenComplete((result, failure) ->
                dispatchControl(() -> finishRead(kind, result, failure)));
    }

    private void finishRead(ReadKind kind, T result, Throwable suppliedFailure) {
        if (terminal.get() || !inFlight) return;
        inFlight = false;
        inFlightFuture = null;
        if (suppliedFailure != null) {
            completeFailure(unwrap(suppliedFailure), KafkaFetchOperationState.COMPLETE);
            return;
        }
        T exactResult = Objects.requireNonNull(result, "Kafka Fetch wave result");
        int exactBytes;
        try {
            exactBytes = responseBytes.applyAsInt(exactResult);
        } catch (Throwable failure) {
            completeFailure(failure, KafkaFetchOperationState.COMPLETE);
            return;
        }
        if (exactBytes < 0) {
            completeFailure(
                    new IllegalArgumentException("Kafka Fetch wave returned negative response bytes"),
                    KafkaFetchOperationState.COMPLETE);
            return;
        }
        boolean mustComplete;
        try {
            mustComplete = forceComplete.test(exactResult);
        } catch (Throwable failure) {
            completeFailure(failure, KafkaFetchOperationState.COMPLETE);
            return;
        }
        if (mustComplete
                || kind == ReadKind.DEADLINE
                || maxWait.isZero()) {
            completeSuccess(new KafkaFetchWaveResult<>(
                    exactResult,
                    exactBytes,
                    kind == ReadKind.DEADLINE,
                    readAttempts));
            return;
        }
        if (deadlineExpired) {
            beginDeadlineRead();
        } else if (exactBytes >= minBytes) {
            completeSuccess(new KafkaFetchWaveResult<>(
                    exactResult,
                    exactBytes,
                    false,
                    readAttempts));
        } else if (dirty) {
            beginEventReadIfAllowed();
        } else {
            state = KafkaFetchOperationState.WAITING;
        }
    }

    private void completeSuccess(KafkaFetchWaveResult<T> result) {
        if (!terminal.compareAndSet(false, true)) return;
        state = KafkaFetchOperationState.COMPLETE;
        cleanup();
        dispatchCompletion(() -> completion.completeFromOperation(result));
    }

    private void completeFailure(Throwable failure, KafkaFetchOperationState terminalState) {
        Objects.requireNonNull(failure, "failure");
        if (!terminal.compareAndSet(false, true)) return;
        state = terminalState;
        cleanup();
        dispatchCompletion(() -> completion.failFromOperation(failure));
    }

    private void cleanup() {
        ScheduledFuture<?> deadline = deadlineTask;
        if (deadline != null) deadline.cancel(false);
        closeSubscription();
        CompletableFuture<T> read = inFlightFuture;
        if (read != null) read.cancel(true);
        inFlightFuture = null;
        inFlight = false;
        synchronized (controlGuard) {
            controlTasks.clear();
        }
    }

    private void closeSubscription() {
        AutoCloseable exactSubscription = subscription;
        subscription = null;
        if (exactSubscription == null) return;
        try {
            exactSubscription.close();
        } catch (Throwable ignored) {
            // Cleanup stays best effort after the terminal result is frozen.
        }
    }

    private void dispatchCompletion(Runnable action) {
        AtomicBoolean invoked = new AtomicBoolean();
        Runnable exactlyOnce = () -> {
            if (invoked.compareAndSet(false, true)) action.run();
        };
        try {
            callbackExecutor.execute(exactlyOnce);
        } catch (Throwable rejected) {
            exactlyOnce.run();
        }
    }

    private void dispatchControl(Runnable task) {
        if (terminal.get()) return;
        boolean submit;
        synchronized (controlGuard) {
            if (terminal.get()) return;
            controlTasks.addLast(task);
            submit = !controlScheduled;
            if (submit) controlScheduled = true;
        }
        if (!submit) return;
        try {
            readExecutor.execute(this::drainControlTasks);
        } catch (RejectedExecutionException rejected) {
            synchronized (controlGuard) {
                controlScheduled = false;
                controlTasks.clear();
            }
            completeFailure(
                    new NereusException(
                            ErrorCode.BACKPRESSURE_REJECTED,
                            true,
                            "Kafka Fetch executor rejected operation control",
                            rejected),
                    KafkaFetchOperationState.COMPLETE);
        } catch (Throwable failure) {
            completeFailure(failure, KafkaFetchOperationState.COMPLETE);
        }
    }

    private void drainControlTasks() {
        while (true) {
            Runnable task;
            synchronized (controlGuard) {
                task = controlTasks.pollFirst();
                if (task == null) {
                    controlScheduled = false;
                    return;
                }
            }
            try {
                task.run();
            } catch (Throwable failure) {
                completeFailure(failure, KafkaFetchOperationState.COMPLETE);
            }
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private enum ReadKind {
        INITIAL,
        EVENT,
        DEADLINE
    }

    /** Prevents caller cancellation or completion from bypassing operation-owned cleanup. */
    private static final class OperationFuture<T> extends CompletableFuture<T> {
        private boolean completeFromOperation(T value) {
            return super.complete(value);
        }

        private boolean failFromOperation(Throwable failure) {
            return super.completeExceptionally(failure);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean complete(T value) {
            return false;
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            return false;
        }

        @Override
        public void obtrudeValue(T value) {
            throw new UnsupportedOperationException("Kafka Fetch wave completion is operation-owned");
        }

        @Override
        public void obtrudeException(Throwable failure) {
            throw new UnsupportedOperationException("Kafka Fetch wave completion is operation-owned");
        }
    }
}
