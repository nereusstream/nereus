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
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean closed = new AtomicBoolean();

    public KafkaBoundedAppendExecutor(
            int threads,
            int maxQueuedTasks,
            KafkaByteBudget byteBudget,
            String threadNamePrefix) {
        if (threads <= 0 || maxQueuedTasks <= 0) {
            throw new IllegalArgumentException("Kafka append executor threads and queue must be positive");
        }
        this.byteBudget = Objects.requireNonNull(byteBudget, "byteBudget");
        if (threadNamePrefix == null || threadNamePrefix.isBlank()) {
            throw new IllegalArgumentException("threadNamePrefix must be nonblank");
        }
        AtomicInteger threadId = new AtomicInteger();
        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(task, threadNamePrefix + "-" + threadId.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.executor = new ThreadPoolExecutor(
                threads,
                threads,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(maxQueuedTasks),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Captures bytes before queue admission. Cancelling the returned future never cancels admitted append work.
     */
    public <T> CompletableFuture<T> submit(ByteBuffer requestBytes, KafkaAppendTask<T> task) {
        Objects.requireNonNull(task, "task");
        if (closed.get()) return failedClosed();
        KafkaProduceBufferSnapshot snapshot;
        try {
            snapshot = KafkaProduceBufferSnapshot.capture(requestBytes, byteBudget);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        try {
            executor.execute(() -> execute(snapshot, task, result));
        } catch (RejectedExecutionException rejected) {
            snapshot.close();
            ErrorCode code = closed.get() ? ErrorCode.STORAGE_CLOSED : ErrorCode.BACKPRESSURE_REJECTED;
            result.completeExceptionally(new NereusException(
                    code,
                    code == ErrorCode.BACKPRESSURE_REJECTED,
                    "Kafka append executor rejected work before append I/O",
                    rejected,
                    AppendOutcome.KNOWN_NOT_COMMITTED));
        }
        return result;
    }

    public int queuedTasks() {
        return executor.getQueue().size();
    }

    public int activeTasks() {
        return executor.getActiveCount();
    }

    public long ownedBufferBytes() {
        return byteBudget.usedBytes();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) executor.shutdown();
    }

    private static <T> void execute(
            KafkaProduceBufferSnapshot snapshot,
            KafkaAppendTask<T> task,
            CompletableFuture<T> result) {
        try {
            T value = task.execute(snapshot.buffer());
            snapshot.close();
            result.complete(value);
        } catch (Throwable failure) {
            snapshot.close();
            result.completeExceptionally(failure);
        }
    }

    private static <T> CompletableFuture<T> failedClosed() {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.STORAGE_CLOSED,
                false,
                "Kafka append executor is closed",
                AppendOutcome.KNOWN_NOT_COMMITTED));
    }
}
