/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BookKeeperAppenderDeadlineTest {
    @Test
    void propagatesRemainingBudget() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            Duration timeout = Duration.ofMillis(250);
            runtime.operations.createDelayNanos = Duration.ofMillis(40).toNanos();
            try (var prepared = runtime.appender.prepare(BookKeeperPrimaryWalAppenderTest.request(
                    BookKeeperPrimaryWalAppenderTest.session(), "deadline", 0, new byte[] {1}))) {
                runtime.appender.persist(prepared, timeout).join();
            }

            assertThat(runtime.operations.observedDeadlineNanos).hasSizeGreaterThanOrEqualTo(2);
            long createBudget = runtime.operations.observedDeadlineNanos.get(0);
            long writeBudget = runtime.operations.observedDeadlineNanos.get(1);
            assertThat(createBudget).isPositive().isLessThanOrEqualTo(timeout.toNanos());
            assertThat(writeBudget).isPositive().isLessThan(createBudget - Duration.ofMillis(20).toNanos());
        }
    }
}
