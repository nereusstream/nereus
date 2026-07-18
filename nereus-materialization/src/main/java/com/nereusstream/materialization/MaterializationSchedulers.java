/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/** Scheduler factories whose cancelled deadline tasks cannot accumulate until their original due time. */
public final class MaterializationSchedulers {
    private MaterializationSchedulers() {
    }

    public static ScheduledThreadPoolExecutor newSingleThreadScheduler(
            ThreadFactory threadFactory) {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(
                1, Objects.requireNonNull(threadFactory, "threadFactory"));
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        return scheduler;
    }
}
