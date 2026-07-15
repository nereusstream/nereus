/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Fair, process-local global/per-stream worker admission with duplicate coalescing and bounded close. */
public final class DefaultMaterializationTaskDispatcher implements MaterializationTaskDispatcher {
    private final MaterializationWorker worker;
    private final GenerationCommitter committer;
    private final Executor workerExecutor;
    private final int maxConcurrentWorkers;
    private final int maxConcurrentWorkersPerStream;
    private final Object monitor = new Object();
    private final ArrayDeque<WorkItem> queued = new ArrayDeque<>();
    private final Map<TaskKey, WorkItem> admitted = new HashMap<>();
    private final Map<StreamId, Integer> activeByStream = new HashMap<>();

    private int active;
    private boolean closed;
    private CompletableFuture<Void> closeFuture;
    private ScheduledFuture<?> closeTimeout;

    public DefaultMaterializationTaskDispatcher(
            MaterializationWorker worker,
            GenerationCommitter committer,
            Executor workerExecutor,
            int maxConcurrentWorkers,
            int maxConcurrentWorkersPerStream) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.committer = Objects.requireNonNull(committer, "committer");
        this.workerExecutor = Objects.requireNonNull(workerExecutor, "workerExecutor");
        if (maxConcurrentWorkers <= 0
                || maxConcurrentWorkersPerStream <= 0
                || maxConcurrentWorkersPerStream > maxConcurrentWorkers) {
            throw new IllegalArgumentException(
                    "worker concurrency must be positive and per-stream concurrency must not exceed global concurrency");
        }
        this.maxConcurrentWorkers = maxConcurrentWorkers;
        this.maxConcurrentWorkersPerStream = maxConcurrentWorkersPerStream;
    }

    /** Compatibility constructor with one globally active worker. */
    public DefaultMaterializationTaskDispatcher(
            MaterializationWorker worker,
            GenerationCommitter committer,
            Executor workerExecutor) {
        this(worker, committer, workerExecutor, 1, 1);
    }

    @Override
    public CompletableFuture<Void> dispatch(
            VersionedMaterializationTask durable,
            MaterializationTask task) {
        try {
            Objects.requireNonNull(durable, "durable");
            Objects.requireNonNull(task, "task");
            if (!MaterializationRecordMapper.domainTask(durable).equals(task)) {
                throw new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "dispatcher durable task differs from the requested task");
            }
            WorkItem item;
            List<WorkItem> starters;
            synchronized (monitor) {
                if (closed) {
                    return CompletableFuture.failedFuture(closedFailure(
                            "materialization dispatcher is closed"));
                }
                TaskKey key = new TaskKey(task.streamId(), task.taskId());
                item = admitted.get(key);
                if (item == null) {
                    item = new WorkItem(key, task);
                    admitted.put(key, item);
                    queued.addLast(item);
                }
                starters = drainLocked();
            }
            schedule(starters);
            return item.completion.thenApply(ignored -> null);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync(
            Duration timeout,
            ScheduledExecutorService scheduler) {
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(scheduler, "scheduler");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        List<WorkItem> abandoned;
        CompletableFuture<Void> result;
        boolean scheduleDeadline;
        synchronized (monitor) {
            if (closeFuture != null) {
                return closeFuture;
            }
            closed = true;
            closeFuture = new CompletableFuture<>();
            result = closeFuture;
            abandoned = new ArrayList<>(queued);
            queued.clear();
            for (WorkItem item : abandoned) {
                admitted.remove(item.key, item);
            }
            if (active == 0) {
                closeFuture.complete(null);
            }
            scheduleDeadline = !closeFuture.isDone();
        }
        NereusException rejection = closedFailure(
                "materialization task was queued when the dispatcher closed");
        for (WorkItem item : abandoned) {
            item.completion.completeExceptionally(rejection);
        }
        if (scheduleDeadline) {
            try {
                ScheduledFuture<?> scheduled = scheduler.schedule(
                        this::forceClose,
                        timeoutNanos,
                        TimeUnit.NANOSECONDS);
                synchronized (monitor) {
                    closeTimeout = scheduled;
                    if (closeFuture.isDone()) {
                        scheduled.cancel(false);
                    }
                }
            } catch (RejectedExecutionException failure) {
                forceClose();
            }
        }
        return result;
    }

    @Override
    public int activeTaskCount() {
        synchronized (monitor) {
            return active;
        }
    }

    @Override
    public int queuedTaskCount() {
        synchronized (monitor) {
            return queued.size();
        }
    }

    private List<WorkItem> drainLocked() {
        ArrayList<WorkItem> starters = new ArrayList<>();
        if (closed) {
            return starters;
        }
        while (active < maxConcurrentWorkers && !queued.isEmpty()) {
            WorkItem selected = null;
            Iterator<WorkItem> iterator = queued.iterator();
            while (iterator.hasNext()) {
                WorkItem candidate = iterator.next();
                int streamActive = activeByStream.getOrDefault(candidate.key.streamId(), 0);
                if (streamActive < maxConcurrentWorkersPerStream) {
                    selected = candidate;
                    iterator.remove();
                    break;
                }
            }
            if (selected == null) {
                break;
            }
            selected.active = true;
            active = Math.addExact(active, 1);
            activeByStream.merge(selected.key.streamId(), 1, Math::addExact);
            starters.add(selected);
        }
        return starters;
    }

    private void schedule(List<WorkItem> starters) {
        for (WorkItem item : starters) {
            try {
                workerExecutor.execute(() -> start(item));
            } catch (Throwable failure) {
                finish(item, failure instanceof RejectedExecutionException
                        ? closedFailure("worker executor rejected admitted materialization work", failure)
                        : failure);
            }
        }
    }

    private void start(WorkItem item) {
        if (item.finished.get()) {
            return;
        }
        CompletableFuture<Void> operation;
        try {
            CompletableFuture<MaterializationOutput> execution = Objects.requireNonNull(
                    worker.execute(item.task), "materialization worker future");
            operation = execution
                    .thenCompose(output -> Objects.requireNonNull(
                            committer.publish(item.task, output),
                            "generation commit future"))
                    .thenApply(ignored -> null);
        } catch (Throwable failure) {
            finish(item, failure);
            return;
        }
        boolean cancel;
        synchronized (monitor) {
            item.operation = operation;
            cancel = item.cancelRequested;
        }
        operation.whenComplete((ignored, failure) -> finish(item, failure));
        if (cancel) {
            operation.cancel(true);
        }
    }

    private void finish(WorkItem item, Throwable failure) {
        if (!item.finished.compareAndSet(false, true)) {
            return;
        }
        List<WorkItem> starters;
        CompletableFuture<Void> closeToComplete = null;
        synchronized (monitor) {
            admitted.remove(item.key, item);
            if (item.active) {
                item.active = false;
                active--;
                int remaining = activeByStream.getOrDefault(item.key.streamId(), 0) - 1;
                if (remaining == 0) {
                    activeByStream.remove(item.key.streamId());
                } else if (remaining > 0) {
                    activeByStream.put(item.key.streamId(), remaining);
                } else {
                    throw new IllegalStateException("per-stream materialization admission underflow");
                }
            }
            starters = drainLocked();
            if (closed && active == 0 && closeFuture != null && !closeFuture.isDone()) {
                closeToComplete = closeFuture;
                if (closeTimeout != null) {
                    closeTimeout.cancel(false);
                }
            }
        }
        if (failure == null) {
            item.completion.complete(null);
        } else {
            item.completion.completeExceptionally(failure);
        }
        if (closeToComplete != null) {
            closeToComplete.complete(null);
        }
        schedule(starters);
    }

    private void forceClose() {
        List<WorkItem> running;
        synchronized (monitor) {
            if (closeFuture == null || closeFuture.isDone()) {
                return;
            }
            running = admitted.values().stream().filter(item -> item.active).toList();
            for (WorkItem item : running) {
                item.cancelRequested = true;
            }
        }
        for (WorkItem item : running) {
            try {
                worker.cancel(item.task);
            } catch (Throwable ignored) {
                // Local cancellation is best-effort; the durable claim remains recoverable.
            }
            CompletableFuture<Void> operation = item.operation;
            if (operation != null) {
                operation.cancel(true);
            }
            finish(item, new CancellationException(
                    "materialization task cancelled at dispatcher close deadline"));
        }
        synchronized (monitor) {
            if (closeFuture != null && !closeFuture.isDone()) {
                closeFuture.complete(null);
            }
        }
    }

    private static NereusException closedFailure(String message) {
        return closedFailure(message, null);
    }

    private static NereusException closedFailure(String message, Throwable cause) {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, message, cause);
    }

    private record TaskKey(StreamId streamId, String taskId) {
        private TaskKey {
            Objects.requireNonNull(streamId, "streamId");
            Objects.requireNonNull(taskId, "taskId");
        }
    }

    private static final class WorkItem {
        private final TaskKey key;
        private final MaterializationTask task;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        private final AtomicBoolean finished = new AtomicBoolean();
        private volatile CompletableFuture<Void> operation;
        private boolean active;
        private boolean cancelRequested;

        private WorkItem(TaskKey key, MaterializationTask task) {
            this.key = key;
            this.task = task;
        }
    }
}
