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

package com.nereusstream.kafka.runtime;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Bounded executor that owns Produce buffers until blocking stock-log append work reaches a terminal result. */
public final class KafkaBoundedAppendExecutor implements AutoCloseable {
    private final KafkaByteBudget byteBudget;
    private final TrackingThreadPoolExecutor executor;
    private final int maxOutstandingTasks;
    private final Object guard = new Object();
    private final Map<Object, Lane> lanes = new HashMap<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private int outstandingTasks;
    private int activeTasks;

    public KafkaBoundedAppendExecutor(
            int threads,
            int maxQueuedTasks,
            KafkaByteBudget byteBudget,
            String threadNamePrefix) {
        if (threads <= 0 || maxQueuedTasks <= 0) {
            throw new IllegalArgumentException("Kafka append executor threads and queue must be positive");
        }
        this.byteBudget = Objects.requireNonNull(byteBudget, "byteBudget");
        this.maxOutstandingTasks = Math.addExact(threads, maxQueuedTasks);
        if (threadNamePrefix == null || threadNamePrefix.isBlank()) {
            throw new IllegalArgumentException("threadNamePrefix must be nonblank");
        }
        AtomicInteger threadId = new AtomicInteger();
        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(task, threadNamePrefix + "-" + threadId.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.executor = new TrackingThreadPoolExecutor(
                threads,
                threads,
                new ArrayBlockingQueue<>(maxQueuedTasks),
                threadFactory);
    }

    /**
     * Captures bytes before queue admission. Cancelling the returned future never cancels admitted append work.
     */
    public <T> CompletableFuture<T> submit(ByteBuffer requestBytes, KafkaAppendTask<T> task) {
        return submit(new Object(), requestBytes, task);
    }

    /**
     * Captures bytes before queue admission and executes work with the same ordering key in strict submission order.
     * Different keys may execute concurrently. Cancelling the returned future never cancels admitted append work.
     */
    public <T> CompletableFuture<T> submit(
            Object orderingKey,
            ByteBuffer requestBytes,
            KafkaAppendTask<T> task) {
        Objects.requireNonNull(orderingKey, "orderingKey");
        Objects.requireNonNull(task, "task");
        if (closed.get()) return failedClosed();
        KafkaProduceBufferSnapshot snapshot;
        try {
            snapshot = KafkaProduceBufferSnapshot.capture(requestBytes, byteBudget);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        Work<T> work = new Work<>(snapshot, task, result);
        synchronized (guard) {
            if (closed.get()) {
                reject(work, ErrorCode.STORAGE_CLOSED, null);
                return result;
            }
            if (outstandingTasks >= maxOutstandingTasks) {
                reject(work, ErrorCode.BACKPRESSURE_REJECTED, null);
                return result;
            }
            Lane lane = lanes.computeIfAbsent(orderingKey, ignored -> new Lane());
            lane.work.addLast(work);
            outstandingTasks++;
            if (!lane.scheduled) {
                lane.scheduled = true;
                try {
                    executor.execute(() -> runLane(orderingKey, lane));
                } catch (RejectedExecutionException rejected) {
                    lane.work.removeLast();
                    outstandingTasks--;
                    if (lane.work.isEmpty()) lanes.remove(orderingKey, lane);
                    reject(work, closed.get() ? ErrorCode.STORAGE_CLOSED : ErrorCode.BACKPRESSURE_REJECTED,
                            rejected);
                }
            }
        }
        return result;
    }

    public int queuedTasks() {
        synchronized (guard) {
            return outstandingTasks - activeTasks;
        }
    }

    public int activeTasks() {
        synchronized (guard) {
            return activeTasks;
        }
    }

    public long ownedBufferBytes() {
        return byteBudget.usedBytes();
    }

    /** A non-mutating view that completes only after close and all admitted work have terminated. */
    public CompletableFuture<Void> drainedFuture() {
        return drained.thenApply(ignored -> null);
    }

    @Override
    public void close() {
        synchronized (guard) {
            if (closed.compareAndSet(false, true) && outstandingTasks == 0) executor.shutdown();
        }
    }

    private void runLane(Object orderingKey, Lane lane) {
        Work<?> work;
        synchronized (guard) {
            work = lane.work.pollFirst();
            if (work == null) {
                lanes.remove(orderingKey, lane);
                lane.scheduled = false;
                shutdownIfDrained();
                return;
            }
            activeTasks++;
        }
        work.execute();
        synchronized (guard) {
            activeTasks--;
            outstandingTasks--;
            if (lane.work.isEmpty()) {
                lanes.remove(orderingKey, lane);
                lane.scheduled = false;
            } else {
                try {
                    // One work item per runner gives queued partitions a chance even with a single worker thread.
                    executor.execute(() -> runLane(orderingKey, lane));
                } catch (RejectedExecutionException rejected) {
                    rejectLane(lane, rejected);
                    lanes.remove(orderingKey, lane);
                    lane.scheduled = false;
                }
            }
            shutdownIfDrained();
        }
    }

    private void rejectLane(Lane lane, RejectedExecutionException rejected) {
        Work<?> remaining;
        while ((remaining = lane.work.pollFirst()) != null) {
            outstandingTasks--;
            reject(remaining, closed.get() ? ErrorCode.STORAGE_CLOSED : ErrorCode.BACKPRESSURE_REJECTED, rejected);
        }
    }

    private void shutdownIfDrained() {
        if (closed.get() && outstandingTasks == 0) executor.shutdown();
    }

    private static void reject(Work<?> work, ErrorCode code, Throwable cause) {
        work.snapshot.close();
        work.result.completeExceptionally(new NereusException(
                code,
                code == ErrorCode.BACKPRESSURE_REJECTED,
                "Kafka append executor rejected work before append I/O",
                cause,
                AppendOutcome.KNOWN_NOT_COMMITTED));
    }

    private static <T> CompletableFuture<T> failedClosed() {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.STORAGE_CLOSED,
                false,
                "Kafka append executor is closed",
                AppendOutcome.KNOWN_NOT_COMMITTED));
    }

    private final class TrackingThreadPoolExecutor extends ThreadPoolExecutor {
        private TrackingThreadPoolExecutor(
                int threads,
                int maximumPoolSize,
                ArrayBlockingQueue<Runnable> queue,
                ThreadFactory threadFactory) {
            super(
                    threads,
                    maximumPoolSize,
                    0,
                    TimeUnit.MILLISECONDS,
                    queue,
                    threadFactory,
                    new ThreadPoolExecutor.AbortPolicy());
        }

        @Override
        protected void terminated() {
            drained.complete(null);
        }
    }

    private static final class Lane {
        private final ArrayDeque<Work<?>> work = new ArrayDeque<>();
        private boolean scheduled;
    }

    private static final class Work<T> {
        private final KafkaProduceBufferSnapshot snapshot;
        private final KafkaAppendTask<T> task;
        private final CompletableFuture<T> result;

        private Work(
                KafkaProduceBufferSnapshot snapshot,
                KafkaAppendTask<T> task,
                CompletableFuture<T> result) {
            this.snapshot = snapshot;
            this.task = task;
            this.result = result;
        }

        private void execute() {
            try {
                result.complete(task.execute(snapshot.buffer()));
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            } finally {
                snapshot.close();
            }
        }
    }
}
