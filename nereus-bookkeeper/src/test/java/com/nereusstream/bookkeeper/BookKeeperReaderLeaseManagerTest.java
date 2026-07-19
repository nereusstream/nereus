/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class BookKeeperReaderLeaseManagerTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void oneProcessSharesOneRenewableSlotUntilItsFinalLocalRelease() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            var root = appendAndRoot(runtime);
            MutableClock clock = new MutableClock(1_000);
            BookKeeperReaderLeaseManager manager = new BookKeeperReaderLeaseManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    clock,
                    "reader-shared");

            var firstFuture = manager.claim(root, TIMEOUT);
            var secondFuture = manager.claim(root, TIMEOUT);
            BookKeeperReaderLeaseManager.Lease first = firstFuture.join();
            BookKeeperReaderLeaseManager.Lease second = secondFuture.join();
            assertThat(first.value().readerSlot()).isEqualTo(second.value().readerSlot());
            assertThat(first.value().processRunId()).isEqualTo("reader-shared");
            assertThat(readerLeases(runtime, root.value().ledgerId())).hasSize(1);

            clock.setMillis(100_000);
            BookKeeperReaderLeaseManager.Lease renewed = manager.claim(root, TIMEOUT).join();
            assertThat(renewed.value().readerSlot()).isEqualTo(first.value().readerSlot());
            assertThat(renewed.value().leaseEpoch()).isEqualTo(2);
            assertThat(first.revalidate().join().value().ledgerId()).isEqualTo(root.value().ledgerId());

            first.release().join();
            second.release().join();
            assertThat(readerLeases(runtime, root.value().ledgerId())).hasSize(1);
            renewed.release().join();
            assertThat(readerLeases(runtime, root.value().ledgerId())).isEmpty();
        }
    }

    @Test
    void fixedSlotsBoundIndependentProcessesWithoutDeletingForeignOccupants() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            var root = appendAndRoot(runtime);
            List<BookKeeperReaderLeaseManager.Lease> leases = new ArrayList<>();
            for (int process = 0; process < runtime.configuration.maxReaderLeasesPerLedger(); process++) {
                BookKeeperReaderLeaseManager manager = new BookKeeperReaderLeaseManager(
                        BookKeeperPrimaryWalAppenderTest.CLUSTER,
                        runtime.configuration,
                        runtime.metadata,
                        BookKeeperPrimaryWalAppenderTest.CLOCK,
                        "reader-process-" + process);
                leases.add(manager.claim(root, TIMEOUT).join());
            }

            var durable = readerLeases(runtime, root.value().ledgerId());
            assertThat(durable).hasSize(runtime.configuration.maxReaderLeasesPerLedger());
            assertThat(durable).extracting(value -> value.value().readerSlot()).doesNotHaveDuplicates();
            assertThat(durable).extracting(value -> value.value().processRunId()).doesNotHaveDuplicates();

            BookKeeperReaderLeaseManager overflow = new BookKeeperReaderLeaseManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    BookKeeperPrimaryWalAppenderTest.CLOCK,
                    "reader-process-overflow");
            assertThatThrownBy(() -> overflow.claim(root, TIMEOUT).join())
                    .hasCauseInstanceOf(NereusException.class)
                    .satisfies(error -> assertThat(((NereusException) error.getCause()).code())
                            .isEqualTo(ErrorCode.BACKPRESSURE_REJECTED));
            assertThat(readerLeases(runtime, root.value().ledgerId()))
                    .hasSize(runtime.configuration.maxReaderLeasesPerLedger());

            leases.forEach(lease -> lease.release().join());
            assertThat(readerLeases(runtime, root.value().ledgerId())).isEmpty();
        }
    }

    @Test
    void finalRevalidationFailsWhenTheExactDurableLeaseDisappears() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            var root = appendAndRoot(runtime);
            BookKeeperReaderLeaseManager manager = new BookKeeperReaderLeaseManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    BookKeeperPrimaryWalAppenderTest.CLOCK,
                    "reader-revalidation");
            BookKeeperReaderLeaseManager.Lease lease = manager.claim(root, TIMEOUT).join();
            var durable = runtime.metadata.getReaderLease(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            root.value().ledgerId(),
                            lease.value().readerSlot())
                    .join()
                    .orElseThrow();
            runtime.metadata.deleteReaderLease(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            root.value().ledgerId(),
                            durable.value().readerSlot(),
                            durable.metadataVersion())
                    .join();

            assertThatThrownBy(() -> lease.revalidate().join())
                    .hasCauseInstanceOf(NereusException.class)
                    .satisfies(error -> assertThat(((NereusException) error.getCause()).code())
                            .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
            lease.release().join();
        }
    }

    @Test
    void renewalFailureDoesNotLeakTheRememberedDurableSlot() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            var root = appendAndRoot(runtime);
            MutableClock clock = new MutableClock(1_000);
            AtomicBoolean failRenewal = new AtomicBoolean();
            BookKeeperLedgerMetadataStore metadata = (BookKeeperLedgerMetadataStore) Proxy.newProxyInstance(
                    BookKeeperLedgerMetadataStore.class.getClassLoader(),
                    new Class<?>[] {BookKeeperLedgerMetadataStore.class},
                    (ignored, method, arguments) -> {
                        if (method.getName().equals("compareAndSetReaderLease")
                                && failRenewal.compareAndSet(true, false)) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("injected reader lease renewal failure"));
                        }
                        try {
                            return method.invoke(runtime.metadata, arguments);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
            BookKeeperReaderLeaseManager manager = new BookKeeperReaderLeaseManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    metadata,
                    clock,
                    "reader-renewal-failure");
            BookKeeperReaderLeaseManager.Lease first = manager.claim(root, TIMEOUT).join();
            clock.setMillis(100_000);
            failRenewal.set(true);

            assertThatThrownBy(() -> manager.claim(root, TIMEOUT).join())
                    .hasRootCauseMessage("injected reader lease renewal failure");
            first.release().join();
            assertThat(readerLeases(runtime, root.value().ledgerId())).isEmpty();
        }
    }

    private static com.nereusstream.metadata.oxia.BookKeeperVersionedValue<
                    com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord>
            appendAndRoot(BookKeeperPrimaryWalAppenderTest.Runtime runtime) {
        DurablePrimaryAppend durable;
        try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                BookKeeperPrimaryWalAppenderTest.request(
                        BookKeeperPrimaryWalAppenderTest.session(),
                        "reader-lease-manager",
                        10,
                        new byte[] {1}))) {
            durable = runtime.appender.persist(prepared, TIMEOUT).join();
        }
        long ledgerId = ((BookKeeperEntryRangeReadTarget) durable.readTarget()).ledgerId();
        return runtime.metadata.getRoot(
                        BookKeeperPrimaryWalAppenderTest.CLUSTER,
                        runtime.configuration.providerScopeSha256(),
                        ledgerId)
                .join()
                .orElseThrow();
    }

    private static List<com.nereusstream.metadata.oxia.BookKeeperVersionedValue<
                    com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord>>
            readerLeases(BookKeeperPrimaryWalAppenderTest.Runtime runtime, long ledgerId) {
        return runtime.metadata.scanReaderLeases(
                        BookKeeperPrimaryWalAppenderTest.CLUSTER,
                        runtime.configuration.providerScopeSha256(),
                        ledgerId,
                        Optional.empty(),
                        runtime.configuration.maxReaderLeasesPerLedger())
                .join()
                .values();
    }

    private static final class MutableClock extends Clock {
        private long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void setMillis(long millis) {
            this.millis = millis;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) throw new IllegalArgumentException("test clock is UTC");
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
