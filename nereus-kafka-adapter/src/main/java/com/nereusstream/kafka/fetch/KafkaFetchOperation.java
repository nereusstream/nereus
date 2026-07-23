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
import com.nereusstream.kafka.partition.KafkaPartitionEvent;
import com.nereusstream.kafka.partition.KafkaPartitionEventSubscription;
import com.nereusstream.kafka.partition.KafkaPartitionEventType;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaStorageReadResult;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Event-driven multi-partition Fetch operation. State transitions are serialized on the read executor while each
 * partition has at most one storage read in flight.
 */
public final class KafkaFetchOperation implements AutoCloseable {
    private final KafkaFetchOperationRequest request;
    private final Executor readExecutor;
    private final Executor callbackExecutor;
    private final ScheduledExecutorService deadlineScheduler;
    private final List<PartitionSlot> slots;
    private final Map<KafkaPartitionIdentity, PartitionSlot> slotsByIdentity;
    private final CopyOnWriteArrayList<KafkaPartitionEventSubscription> subscriptions =
            new CopyOnWriteArrayList<>();
    private final OperationFuture<KafkaFetchOperationResult> completion = new OperationFuture<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean terminal = new AtomicBoolean();
    private final Object controlGuard = new Object();
    private final ArrayDeque<Runnable> controlTasks = new ArrayDeque<>();

    private volatile KafkaFetchOperationState state = KafkaFetchOperationState.NEW;
    private volatile ScheduledFuture<?> deadlineTask;
    private boolean controlScheduled;
    private boolean deadlineExpired;
    private volatile int pendingReads;
    private int rereadRounds;
    private int readAttempts;

    public KafkaFetchOperation(
            KafkaFetchOperationRequest request,
            Executor readExecutor,
            Executor callbackExecutor,
            ScheduledExecutorService deadlineScheduler) {
        this.request = Objects.requireNonNull(request, "request");
        this.readExecutor = Objects.requireNonNull(readExecutor, "readExecutor");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.deadlineScheduler = Objects.requireNonNull(deadlineScheduler, "deadlineScheduler");
        List<PartitionSlot> ordered = new ArrayList<>(request.partitions().size());
        Map<KafkaPartitionIdentity, PartitionSlot> indexed = new LinkedHashMap<>();
        for (KafkaFetchPartitionRequest partition : request.partitions()) {
            PartitionSlot slot = new PartitionSlot(partition);
            ordered.add(slot);
            indexed.put(partition.identity(), slot);
        }
        this.slots = List.copyOf(ordered);
        this.slotsByIdentity = Map.copyOf(indexed);
    }

    /** Starts listener registration, the initial read wave and the max-wait timer exactly once. */
    public CompletableFuture<KafkaFetchOperationResult> start() {
        if (!started.compareAndSet(false, true)) return completion;
        try {
            registerListeners();
            if (terminal.get()) return completion;
            state = KafkaFetchOperationState.READING;
            if (!request.maxWait().isZero()) {
                deadlineTask = deadlineScheduler.schedule(
                        () -> dispatchControl(this::handleDeadline),
                        request.maxWait().toNanos(),
                        TimeUnit.NANOSECONDS);
            }
            dispatchControl(() -> beginReadWave(slots, false));
        } catch (Throwable failure) {
            completeFailure(failure, KafkaFetchOperationState.COMPLETE);
        }
        return completion;
    }

    public KafkaFetchOperationState state() {
        return state;
    }

    public int inFlightReads() {
        return pendingReads;
    }

    /** Only runtime shutdown/leadership teardown calls this; client future cancellation is deliberately disabled. */
    public void cancel() {
        completeFailure(
                new NereusException(ErrorCode.CANCELLED, false, "Kafka Fetch operation was cancelled"),
                KafkaFetchOperationState.CANCELLED);
    }

    @Override
    public void close() {
        cancel();
    }

    private void registerListeners() {
        for (PartitionSlot slot : slots) {
            KafkaPartitionEventSubscription subscription = slot.request.storage().subscribe(
                    event -> dispatchControl(() -> handleEvent(event)));
            subscriptions.add(subscription);
            if (terminal.get() && subscriptions.remove(subscription)) subscription.close();
        }
    }

    private void handleEvent(KafkaPartitionEvent event) {
        if (terminal.get()) return;
        PartitionSlot slot = slotsByIdentity.get(event.identity());
        if (slot == null) return;
        if (event.type() == KafkaPartitionEventType.LEADERSHIP_LOST) {
            completeFailure(
                    new NereusException(
                            ErrorCode.FENCED_APPEND,
                            false,
                            "Kafka Fetch partition leadership was lost: " + event.identity()),
                    KafkaFetchOperationState.COMPLETE);
            return;
        }
        if (event.type() == KafkaPartitionEventType.CORRUPT_OFFLINE) {
            completeFailure(
                    new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "Kafka Fetch partition became corrupt/offline: " + event.identity()),
                    KafkaFetchOperationState.COMPLETE);
            return;
        }
        if (deadlineExpired) return;
        slot.dirty = true;
        if (state == KafkaFetchOperationState.WAITING) {
            beginReadWave(dirtySlots(), true);
        }
    }

    private void handleDeadline() {
        if (terminal.get()) return;
        deadlineExpired = true;
        state = KafkaFetchOperationState.TIMED_READING;
        if (pendingReads == 0) {
            if (!beginReadWave(slots, true)) {
                completeSuccess(buildResult(true));
            }
        }
    }

    /** Returns false only when the reread safety budget is exhausted. */
    private boolean beginReadWave(Collection<PartitionSlot> requestedSlots, boolean reread) {
        if (terminal.get()) return false;
        if (reread && rereadRounds >= request.maxRereads()) {
            if (!deadlineExpired) state = KafkaFetchOperationState.WAITING;
            return false;
        }
        List<PartitionSlot> eligible = requestedSlots.stream()
                .filter(slot -> !slot.inFlight)
                .toList();
        if (eligible.isEmpty()) return true;
        if (reread) rereadRounds++;
        state = deadlineExpired
                ? KafkaFetchOperationState.TIMED_READING
                : KafkaFetchOperationState.READING;
        for (PartitionSlot slot : eligible) beginRead(slot);
        return true;
    }

    private void beginRead(PartitionSlot slot) {
        slot.dirty = false;
        slot.inFlight = true;
        pendingReads++;
        readAttempts++;
        CompletableFuture<KafkaStorageReadResult> read;
        try {
            read = Objects.requireNonNull(
                    slot.request.storage().read(slot.request.readRequest()),
                    "Kafka partition read future");
        } catch (Throwable failure) {
            read = CompletableFuture.failedFuture(failure);
        }
        slot.inFlightFuture = read;
        read.whenComplete((result, failure) ->
                dispatchControl(() -> finishRead(slot, result, failure)));
    }

    private void finishRead(
            PartitionSlot slot,
            KafkaStorageReadResult result,
            Throwable failure) {
        if (terminal.get() || !slot.inFlight) return;
        slot.inFlight = false;
        slot.inFlightFuture = null;
        pendingReads--;
        if (failure != null) {
            completeFailure(unwrap(failure), KafkaFetchOperationState.COMPLETE);
            return;
        }
        slot.latest = Objects.requireNonNull(result, "Kafka partition read result");
        if (pendingReads != 0) return;

        KafkaFetchOperationResult assembled = buildResult(deadlineExpired);
        if (deadlineExpired
                || request.maxWait().isZero()
                || assembled.responseBytes() >= request.minBytes()
                || assembled.responseBudgetExhausted()) {
            completeSuccess(assembled);
            return;
        }
        List<PartitionSlot> dirty = dirtySlots();
        if (dirty.isEmpty() || !beginReadWave(dirty, true)) {
            state = KafkaFetchOperationState.WAITING;
        }
    }

    private List<PartitionSlot> dirtySlots() {
        return slots.stream().filter(slot -> slot.dirty && !slot.inFlight).toList();
    }

    private KafkaFetchOperationResult buildResult(boolean timedOut) {
        List<KafkaFetchPartitionResult> partitionResults = new ArrayList<>(slots.size());
        int responseBytes = 0;
        boolean exhausted = false;
        for (PartitionSlot slot : slots) {
            KafkaStorageReadResult latest = Objects.requireNonNull(
                    slot.latest, "Fetch result requested before every partition completed an initial read");
            int bytes = latest.fetchAssembly().sizeInBytes();
            if ((exhausted && bytes > 0)
                    || bytes > request.maxResponseBytes() - responseBytes) {
                exhausted = true;
                partitionResults.add(new KafkaFetchPartitionResult(
                        slot.request.identity(), Optional.empty(), true));
            } else {
                responseBytes = Math.addExact(responseBytes, bytes);
                partitionResults.add(new KafkaFetchPartitionResult(
                        slot.request.identity(), Optional.of(latest), false));
            }
        }
        return new KafkaFetchOperationResult(
                partitionResults, responseBytes, exhausted, timedOut, readAttempts);
    }

    private void completeSuccess(KafkaFetchOperationResult result) {
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
        for (KafkaPartitionEventSubscription subscription : subscriptions) {
            try {
                subscription.close();
            } catch (Throwable ignored) {
                // Cleanup remains best effort after the terminal outcome is already frozen.
            }
        }
        subscriptions.clear();
        for (PartitionSlot slot : slots) {
            CompletableFuture<KafkaStorageReadResult> read = slot.inFlightFuture;
            if (read != null) read.cancel(true);
            slot.inFlightFuture = null;
            slot.inFlight = false;
        }
        pendingReads = 0;
        synchronized (controlGuard) {
            controlTasks.clear();
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

    private static final class PartitionSlot {
        private final KafkaFetchPartitionRequest request;
        private boolean dirty;
        private boolean inFlight;
        private CompletableFuture<KafkaStorageReadResult> inFlightFuture;
        private KafkaStorageReadResult latest;

        private PartitionSlot(KafkaFetchPartitionRequest request) {
            this.request = request;
        }
    }

    /** Prevents a network/client future cancellation from bypassing operation terminal cleanup. */
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
            throw new UnsupportedOperationException("Kafka Fetch completion is operation-owned");
        }

        @Override
        public void obtrudeException(Throwable failure) {
            throw new UnsupportedOperationException("Kafka Fetch completion is operation-owned");
        }
    }
}
