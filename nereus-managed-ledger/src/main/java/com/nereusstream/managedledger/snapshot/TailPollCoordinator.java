/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.snapshot;

import com.nereusstream.api.StreamMetadata;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/** Coalesces all local cursor tail waits into one timer and one metadata refresh. */
public final class TailPollCoordinator implements AutoCloseable {
    private final ScheduledExecutorService scheduler;
    private final Duration interval;
    private final Supplier<CompletableFuture<StreamMetadata>> refresh;
    private final Supplier<StreamMetadata> current;
    private final Set<PendingReadWaiter> waiters = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public TailPollCoordinator(
            ScheduledExecutorService scheduler,
            Duration interval,
            Supplier<CompletableFuture<StreamMetadata>> refresh,
            Supplier<StreamMetadata> current) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.refresh = Objects.requireNonNull(refresh, "refresh");
        this.current = Objects.requireNonNull(current, "current");
    }

    public void register(PendingReadWaiter waiter) {
        Objects.requireNonNull(waiter, "waiter");
        if (closed.get()) {
            waiter.tryFail(new ManagedLedgerException.ManagedLedgerAlreadyClosedException(
                    "tail poll coordinator is closed"));
            return;
        }
        waiters.add(waiter);
        if (waiter.trySignal(current.get())) {
            waiters.remove(waiter);
        }
        schedule();
    }

    public boolean remove(PendingReadWaiter waiter) {
        return waiters.remove(waiter);
    }

    public void signalLocalAppend() {
        signal(current.get());
    }

    private void schedule() {
        if (waiters.isEmpty() || closed.get() || !scheduled.compareAndSet(false, true)) return;
        scheduler.schedule(this::poll, interval.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void poll() {
        if (closed.get()) return;
        refresh.get().whenComplete((metadata, error) -> {
            scheduled.set(false);
            if (error == null) {
                signal(metadata);
            } else {
                ManagedLedgerException failure = new ManagedLedgerException(
                        "Nereus tail metadata refresh failed", error);
                waiters.removeIf(waiter -> waiter.tryFail(failure));
            }
            schedule();
        });
    }

    private void signal(StreamMetadata metadata) {
        waiters.removeIf(waiter -> waiter.trySignal(metadata));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        ManagedLedgerException failure = new ManagedLedgerException.ManagedLedgerAlreadyClosedException(
                "managed ledger closed while waiting at tail");
        waiters.removeIf(waiter -> waiter.tryFail(failure));
    }
}
