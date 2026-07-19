/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class BookKeeperAppenderResourceTest {
    @Test
    void releasesEveryOwnedResource() {
        successAndProviderFailureReleaseTheWritePermit();
        timeoutReleasesTheWritePermitAfterLedgerReconciliation();
        cancellationKeepsThePermitUntilTheProviderAttemptConverges();
        closeRejectsNewAdmissionWhileTheAcceptedAttemptConverges();
    }

    private static void successAndProviderFailureReleaseTheWritePermit() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            var session = BookKeeperPrimaryWalAppenderTest.session();
            try (var prepared = runtime.appender.prepare(BookKeeperPrimaryWalAppenderTest.request(
                    session, "resource-success", 0, new byte[] {1}))) {
                runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
            }
            assertThat(runtime.appender.inFlightWriteCount()).isZero();

            runtime.operations.failWriteCall = runtime.operations.writeCalls() + 1;
            assertThatThrownBy(() -> {
                try (var prepared = runtime.appender.prepare(BookKeeperPrimaryWalAppenderTest.request(
                        session, "resource-failure", 1, new byte[] {2}))) {
                    runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
                }
            }).hasRootCauseInstanceOf(NereusException.class);
            assertThat(runtime.appender.inFlightWriteCount()).isZero();
        }
    }

    private static void timeoutReleasesTheWritePermitAfterLedgerReconciliation() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            runtime.operations.hangWriteCall = 1;
            assertThatThrownBy(() -> {
                try (var prepared = runtime.appender.prepare(BookKeeperPrimaryWalAppenderTest.request(
                        BookKeeperPrimaryWalAppenderTest.session(), "resource-timeout", 0, new byte[] {1}))) {
                    runtime.appender.persist(prepared, Duration.ofMillis(25)).join();
                }
            }).hasCauseInstanceOf(NereusException.class)
                    .satisfies(error -> assertThat(((NereusException) error.getCause()).code())
                            .isEqualTo(ErrorCode.TIMEOUT));
            assertThat(runtime.appender.inFlightWriteCount()).isZero();
        }
    }

    private static void cancellationKeepsThePermitUntilTheProviderAttemptConverges() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            runtime.operations.hangWriteCall = 1;
            try (var first = runtime.appender.prepare(BookKeeperPrimaryWalAppenderTest.request(
                    BookKeeperPrimaryWalAppenderTest.session(), "resource-cancel", 0, new byte[] {1}));
                    var rejected = runtime.appender.prepare(BookKeeperPrimaryWalAppenderTest.request(
                            BookKeeperPrimaryWalAppenderTest.session(), "resource-rejected", 0, new byte[] {2}))) {
                var pending = runtime.appender.persist(first, Duration.ofSeconds(10));
                assertThat(pending.cancel(false)).isTrue();
                assertThat(runtime.appender.inFlightWriteCount()).isOne();
                assertThatThrownBy(() -> runtime.appender.persist(rejected, Duration.ofSeconds(10)))
                        .isInstanceOf(NereusException.class)
                        .extracting(error -> ((NereusException) error).code())
                        .isEqualTo(ErrorCode.BACKPRESSURE_REJECTED);

                runtime.operations.failHungWrite();
                assertThat(runtime.appender.inFlightWriteCount()).isZero();
            }
        }
    }

    private static void closeRejectsNewAdmissionWhileTheAcceptedAttemptConverges() {
        BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime();
        try {
            runtime.operations.hangWriteCall = 1;
            try (var first = runtime.appender.prepare(BookKeeperPrimaryWalAppenderTest.request(
                    BookKeeperPrimaryWalAppenderTest.session(), "resource-close", 0, new byte[] {1}));
                    var rejected = runtime.appender.prepare(BookKeeperPrimaryWalAppenderTest.request(
                            BookKeeperPrimaryWalAppenderTest.session(), "resource-after-close", 0, new byte[] {2}))) {
                runtime.appender.persist(first, Duration.ofSeconds(10));
                runtime.appender.close();
                assertThatThrownBy(() -> runtime.appender.persist(rejected, Duration.ofSeconds(10)))
                        .isInstanceOf(NereusException.class)
                        .extracting(error -> ((NereusException) error).code())
                        .isEqualTo(ErrorCode.STORAGE_CLOSED);
                runtime.operations.failHungWrite();
                assertThat(runtime.appender.inFlightWriteCount()).isZero();
            }
        } finally {
            runtime.close();
        }
    }
}
