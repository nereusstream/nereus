/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BookKeeperPrimaryWalReaderTest {
    @Test
    void nonRecoveryOpenVerifiesWholeRangeBeforeReturningClippedExactEntries() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime =
                     new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            DurablePrimaryAppend durable;
            try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                    BookKeeperPrimaryWalAppenderTest.request(
                            BookKeeperPrimaryWalAppenderTest.session(),
                            "reader-attempt",
                            10,
                            new byte[] {1, 2},
                            new byte[] {3}))) {
                durable = runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
            }
            BookKeeperEntryRangeReadTarget target = (BookKeeperEntryRangeReadTarget) durable.readTarget();
            ResolvedRange range = resolved(target);

            var result = runtime.reader.readPhysicalWithStats(
                    BookKeeperPrimaryWalAppenderTest.STREAM,
                    11,
                    List.of(range),
                    new ReadOptions(10, 100, ReadIsolation.COMMITTED, Duration.ofSeconds(10))).join();

            assertThat(result.batches()).singleElement().satisfies(batch -> {
                assertThat(batch.range()).isEqualTo(new OffsetRange(11, 12));
                assertThat(batch.payload()).containsExactly(3);
                assertThat(batch.source().target()).isEqualTo(target);
                assertThat(batch.source().resolvedRange()).isEqualTo(range.offsetRange());
            });
            assertThat(result.rangeStats()).singleElement().satisfies(stats -> {
                assertThat(stats.resolvedPayloadBytes()).isEqualTo(3);
                assertThat(stats.physicalPayloadBytesRead()).isEqualTo(3);
                assertThat(stats.returnedPayloadBytes()).isOne();
            });
            assertThat(runtime.operations.normalOpenCalls).isOne();
            assertThat(runtime.operations.recoveryOpenCalls).isZero();
            assertThat(runtime.metadata.scanReaderLeases(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration.providerScopeSha256(),
                    target.ledgerId(),
                    Optional.empty(),
                    10).join().values()).isEmpty();
        }
    }

    @Test
    void checksumMismatchFailsClosedWithoutReturningPartialBytes() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime =
                     new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            DurablePrimaryAppend durable;
            try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                    BookKeeperPrimaryWalAppenderTest.request(
                            BookKeeperPrimaryWalAppenderTest.session(),
                            "reader-corruption",
                            10,
                            new byte[] {1},
                            new byte[] {2}))) {
                durable = runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
            }
            BookKeeperEntryRangeReadTarget exact = (BookKeeperEntryRangeReadTarget) durable.readTarget();
            BookKeeperEntryRangeReadTarget corrupted = new BookKeeperEntryRangeReadTarget(
                    exact.version(), exact.clusterAlias(), exact.ledgerId(), exact.firstEntryId(), exact.entryCount(),
                    exact.entryMapping(), new Checksum(ChecksumType.SHA256, "0".repeat(64)));

            assertThatThrownBy(() -> runtime.reader.readPhysicalWithStats(
                            BookKeeperPrimaryWalAppenderTest.STREAM,
                            10,
                            List.of(resolved(corrupted)),
                            new ReadOptions(10, 100, ReadIsolation.COMMITTED, Duration.ofSeconds(10))).join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .rootCause().extracting(error -> ((NereusException) error).code())
                    .isEqualTo(ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH);
            assertThat(runtime.operations.recoveryOpenCalls).isZero();
        }
    }

    private static ResolvedRange resolved(BookKeeperEntryRangeReadTarget target) {
        long start = 10;
        long end = start + target.entryCount();
        long bytes = target.entryCount() == 2 ? 3 : target.entryCount();
        return new ResolvedRange(new OffsetRange(start, end), 0, target,
                PayloadFormat.OPAQUE_RECORD_BATCH, target.entryCount(), target.entryCount(), bytes,
                List.of(), Optional.empty(), 1);
    }
}
