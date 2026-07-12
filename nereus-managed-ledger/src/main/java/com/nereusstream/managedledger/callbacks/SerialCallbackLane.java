/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.callbacks;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Executes terminal callbacks in admission order even when operation futures finish out of order. */
public final class SerialCallbackLane implements AutoCloseable {
    private final Executor executor;
    private final int maxPending;
    private final Set<Long> admitted = new HashSet<>();
    private final Map<Long, Runnable> completed = new HashMap<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private long nextAdmission;
    private long nextCallback;
    private boolean accepting = true;
    private boolean draining;

    public SerialCallbackLane(Executor executor, int maxPending) {
        this.executor = Objects.requireNonNull(executor, "executor");
        if (maxPending <= 0) {
            throw new IllegalArgumentException("maxPending must be positive");
        }
        this.maxPending = maxPending;
    }

    public synchronized long admit() {
        if (!accepting) {
            throw new NereusException(ErrorCode.STORAGE_CLOSED, false, "callback lane is closed");
        }
        if (admitted.size() >= maxPending) {
            throw new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED, true, "callback lane pending bound is exhausted");
        }
        if (nextAdmission == Long.MAX_VALUE) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION, false, "callback sequence exhausted");
        }
        long sequence = nextAdmission++;
        admitted.add(sequence);
        return sequence;
    }

    public void complete(long sequence, Runnable terminalCallback) {
        Objects.requireNonNull(terminalCallback, "terminalCallback");
        boolean schedule = false;
        synchronized (this) {
            if (!admitted.contains(sequence) || completed.putIfAbsent(sequence, terminalCallback) != null) {
                throw new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION, false, "unknown or duplicate callback sequence");
            }
            if (!draining && completed.containsKey(nextCallback)) {
                draining = true;
                schedule = true;
            }
        }
        if (schedule) {
            executor.execute(this::drain);
        }
    }

    public synchronized CompletableFuture<Void> closeAfterDrain() {
        accepting = false;
        completeDrainedIfReady();
        return drained;
    }

    @Override
    public void close() {
        closeAfterDrain();
    }

    private void drain() {
        while (true) {
            Runnable callback;
            synchronized (this) {
                callback = completed.remove(nextCallback);
                if (callback == null) {
                    draining = false;
                    completeDrainedIfReady();
                    return;
                }
                admitted.remove(nextCallback);
                nextCallback++;
            }
            try {
                callback.run();
            } catch (Throwable ignored) {
                // A broker callback exception cannot block later admitted terminal callbacks.
            }
        }
    }

    private void completeDrainedIfReady() {
        if (!accepting && admitted.isEmpty() && !draining) {
            drained.complete(null);
        }
    }
}
