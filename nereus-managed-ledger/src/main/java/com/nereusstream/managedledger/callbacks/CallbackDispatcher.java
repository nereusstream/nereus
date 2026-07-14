/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.callbacks;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Dispatches one terminal broker callback without allowing executor shutdown to
 * strand the callback or its associated resources.
 */
public final class CallbackDispatcher {
    private CallbackDispatcher() {
    }

    public static void execute(Executor executor, Runnable callback) {
        Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(callback, "callback");
        try {
            executor.execute(callback);
        } catch (RejectedExecutionException rejected) {
            runTerminal(callback);
        }
    }

    private static void runTerminal(Runnable callback) {
        try {
            callback.run();
        } catch (Throwable ignored) {
            // A broker callback exception cannot strand cleanup or escape an IO completion thread.
        }
    }
}
