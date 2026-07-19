/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BookKeeperWriterStatePropertyTest {
    @Test
    void isMonotonicAcrossCasSchedules() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime =
                new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            Random random = new Random(29);
            long nextOffset = 0;
            long nextEntryId = 0;
            long physicalBytes = 0;
            long activeLedgerId = 0;
            long activeSegment = -1;
            Set<String> physicalEntries = new LinkedHashSet<>();

            for (int append = 0; append < 64; append++) {
                int entryCount = 1 + random.nextInt(4);
                byte[][] payloads = new byte[entryCount][];
                long appendBytes = 0;
                for (int entry = 0; entry < entryCount; entry++) {
                    payloads[entry] = new byte[1 + random.nextInt(32)];
                    random.nextBytes(payloads[entry]);
                    appendBytes += payloads[entry].length;
                }
                BookKeeperEntryRangeReadTarget target;
                try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                        BookKeeperPrimaryWalAppenderTest.request(
                                BookKeeperPrimaryWalAppenderTest.session(),
                                "property-attempt-" + append,
                                nextOffset,
                                payloads))) {
                    target = (BookKeeperEntryRangeReadTarget) runtime.appender
                            .persist(prepared, Duration.ofSeconds(10))
                            .join()
                            .readTarget();
                }
                if (append == 0) {
                    activeLedgerId = target.ledgerId();
                }
                assertThat(target.ledgerId()).isEqualTo(activeLedgerId);
                assertThat(target.firstEntryId()).isEqualTo(nextEntryId);
                assertThat(target.entryCount()).isEqualTo(entryCount);
                for (long entryId = target.firstEntryId();
                        entryId < target.firstEntryId() + target.entryCount();
                        entryId++) {
                    assertThat(physicalEntries.add(target.ledgerId() + ":" + entryId)).isTrue();
                }
                nextEntryId += entryCount;
                physicalBytes += appendBytes;
                nextOffset += entryCount;

                var writer = runtime.metadata.getWriter(
                                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                                BookKeeperPrimaryWalAppenderTest.STREAM)
                        .join()
                        .orElseThrow()
                        .value();
                if (activeSegment < 0) {
                    activeSegment = writer.activeSegmentSequence();
                }
                assertThat(writer.activeSegmentSequence()).isEqualTo(activeSegment);
                assertThat(writer.activeLedgerId()).isEqualTo(activeLedgerId);
                assertThat(writer.nextEntryId()).isEqualTo(nextEntryId);
                assertThat(writer.activePhysicalBytes()).isEqualTo(physicalBytes);
                assertThat(writer.activeAppendRangeCount()).isEqualTo(append + 1);
                assertThat(writer.activeReservationId()).isEmpty();
            }

            runtime.operations.failWriteCall = Math.toIntExact(nextEntryId + 2);
            long failureOffset = nextOffset;
            assertThatThrownBy(() -> {
                try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                        BookKeeperPrimaryWalAppenderTest.request(
                                BookKeeperPrimaryWalAppenderTest.session(),
                                "property-partial-failure",
                                failureOffset,
                                new byte[] {1},
                                new byte[] {2},
                                new byte[] {3}))) {
                    runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
                }
            }).hasRootCauseInstanceOf(NereusException.class)
                    .rootCause()
                    .extracting(error -> ((NereusException) error).code())
                    .isEqualTo(ErrorCode.PRIMARY_WAL_WRITE_FAILED);
            assertThat(runtime.metadata.getRoot(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            activeLedgerId)
                    .join())
                    .get()
                    .extracting(root -> root.value().lifecycle())
                    .isEqualTo(BookKeeperLedgerLifecycle.SEALED);

            BookKeeperEntryRangeReadTarget replacement;
            try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                    BookKeeperPrimaryWalAppenderTest.request(
                            BookKeeperPrimaryWalAppenderTest.session(),
                            "property-replacement",
                            nextOffset,
                            new byte[] {9}))) {
                replacement = (BookKeeperEntryRangeReadTarget) runtime.appender
                        .persist(prepared, Duration.ofSeconds(10))
                        .join()
                        .readTarget();
            }
            assertThat(replacement.ledgerId()).isNotEqualTo(activeLedgerId);
            assertThat(replacement.firstEntryId()).isZero();
            assertThat(physicalEntries.add(replacement.ledgerId() + ":0")).isTrue();
            var writer = runtime.metadata.getWriter(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            BookKeeperPrimaryWalAppenderTest.STREAM)
                    .join()
                    .orElseThrow()
                    .value();
            assertThat(writer.activeSegmentSequence()).isEqualTo(activeSegment + 1);
            assertThat(writer.nextSegmentSequence()).isGreaterThan(writer.activeSegmentSequence());
            assertThat(writer.nextEntryId()).isOne();
            assertThat(writer.activeAppendRangeCount()).isOne();
        }
    }
}
