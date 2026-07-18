/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MaterializationSchedulersTest {
    @Test
    void cancelledDeadlinesAreRemovedInsteadOfRetainedUntilDueTime() {
        ScheduledThreadPoolExecutor scheduler =
                MaterializationSchedulers.newSingleThreadScheduler(Thread::new);
        try {
            ScheduledFuture<?> deadline = scheduler.schedule(
                    () -> { }, 1, TimeUnit.DAYS);

            assertThat(scheduler.getQueue()).hasSize(1);
            assertThat(deadline.cancel(false)).isTrue();

            assertThat(scheduler.getRemoveOnCancelPolicy()).isTrue();
            assertThat(scheduler.getExecuteExistingDelayedTasksAfterShutdownPolicy()).isFalse();
            assertThat(scheduler.getContinueExistingPeriodicTasksAfterShutdownPolicy()).isFalse();
            assertThat(scheduler.getQueue()).isEmpty();
        } finally {
            scheduler.shutdownNow();
        }
    }
}
