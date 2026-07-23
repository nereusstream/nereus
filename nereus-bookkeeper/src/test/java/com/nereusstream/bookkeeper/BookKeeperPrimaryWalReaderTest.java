/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import java.nio.charset.StandardCharsets;
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

    @Test
    void rangedMappingPreservesPayloadAndMatchesObjectBoundaryAndOverflowSemantics() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime =
                     new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            DurablePrimaryAppend durable;
            try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                    rangedRequest())) {
                durable = runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
            }
            BookKeeperEntryRangeReadTarget target =
                    (BookKeeperEntryRangeReadTarget) durable.readTarget();
            assertThat(target.entryMapping()).isEqualTo(BookKeeperEntryMapping.RANGED_NEREUS_ENTRY_V1);
            ResolvedRange range = new ResolvedRange(
                    new OffsetRange(10, 15),
                    0,
                    target,
                    PayloadFormat.KAFKA_RECORD_BATCH,
                    5,
                    2,
                    5,
                    List.of(),
                    Optional.empty(),
                    1);

            assertThatThrownBy(() -> runtime.reader.readPhysicalWithStats(
                            BookKeeperPrimaryWalAppenderTest.STREAM,
                            request(11, ReadBoundaryMode.EXACT_START,
                                    FirstEntryPolicy.LEGACY_STRICT_LIMIT, 10, 100),
                            List.of(range)).join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .rootCause().extracting(error -> ((NereusException) error).code())
                    .isEqualTo(ErrorCode.OFFSET_NOT_AVAILABLE);

            PhysicalReadResult containing = runtime.reader.readPhysicalWithStats(
                    BookKeeperPrimaryWalAppenderTest.STREAM,
                    request(11, ReadBoundaryMode.CONTAINING_ENTRY,
                            FirstEntryPolicy.LEGACY_STRICT_LIMIT, 10, 100),
                    List.of(range)).join();
            assertThat(containing.batches()).extracting(batch -> batch.range())
                    .containsExactly(new OffsetRange(10, 13), new OffsetRange(13, 15));
            assertThat(containing.batches()).extracting(batch ->
                            new String(batch.payload(), StandardCharsets.UTF_8))
                    .containsExactly("aaa", "bb");

            PhysicalReadResult overflow = runtime.reader.readPhysicalWithStats(
                    BookKeeperPrimaryWalAppenderTest.STREAM,
                    request(11, ReadBoundaryMode.CONTAINING_ENTRY,
                            FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW, 1, 1),
                    List.of(range)).join();
            assertThat(overflow.batches()).singleElement().satisfies(batch -> {
                assertThat(batch.range()).isEqualTo(new OffsetRange(10, 13));
                assertThat(new String(batch.payload(), StandardCharsets.UTF_8)).isEqualTo("aaa");
            });
            assertThat(overflow.rangeStats()).singleElement().satisfies(stats -> {
                assertThat(stats.physicalPayloadBytesRead()).isGreaterThan(5);
                assertThat(stats.returnedPayloadBytes()).isEqualTo(3);
            });
        }
    }

    private static PrimaryAppendRequest rangedRequest() {
        List<AppendEntry> entries = List.of(
                new AppendEntry("aaa".getBytes(StandardCharsets.UTF_8), 3, 1, Map.of()),
                new AppendEntry("bb".getBytes(StandardCharsets.UTF_8), 2, 2, Map.of()));
        AppendBatch batch = new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                entries,
                5,
                2,
                1,
                2,
                List.of(),
                Map.of(),
                Optional.empty());
        return new PrimaryAppendRequest(
                BookKeeperPrimaryWalAppenderTest.STREAM,
                batch,
                BookKeeperPrimaryWalAppenderTest.session(),
                10,
                new AppendAttemptId("reader-ranged"),
                Duration.ofSeconds(10));
    }

    private static ReadRequest request(
            long startOffset,
            ReadBoundaryMode boundaryMode,
            FirstEntryPolicy firstEntryPolicy,
            int maxRecords,
            int maxBytes) {
        return new ReadRequest(
                startOffset,
                ReadView.COMMITTED,
                boundaryMode,
                firstEntryPolicy,
                new ReadOptions(
                        maxRecords,
                        maxBytes,
                        ReadIsolation.COMMITTED,
                        Duration.ofSeconds(10)));
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
