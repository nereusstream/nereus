/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.callbacks;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Exactly-once terminal callback guard whose cleanup always runs after the winning callback. */
public final class TerminalCallback<T> {
    private final AtomicBoolean terminal = new AtomicBoolean();
    private final Consumer<T> success;
    private final Consumer<Throwable> failure;
    private final Runnable cleanup;

    public TerminalCallback(
            Consumer<T> success,
            Consumer<Throwable> failure,
            Runnable cleanup) {
        this.success = Objects.requireNonNull(success, "success");
        this.failure = Objects.requireNonNull(failure, "failure");
        this.cleanup = Objects.requireNonNull(cleanup, "cleanup");
    }

    public boolean tryComplete(T value) {
        return tryTerminal(() -> success.accept(value));
    }

    public boolean tryFail(Throwable error) {
        Objects.requireNonNull(error, "error");
        return tryTerminal(() -> failure.accept(error));
    }

    public boolean isTerminal() {
        return terminal.get();
    }

    private boolean tryTerminal(Runnable invocation) {
        if (!terminal.compareAndSet(false, true)) {
            return false;
        }
        try {
            invocation.run();
        } finally {
            cleanup.run();
        }
        return true;
    }
}
