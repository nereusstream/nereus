/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopicCompactedSparseFormatTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void sparseValuesAndTombstonesPreserveDenseSourceCoverageInNtc1Only() throws Exception {
        CompactedObjectWriteRequest request =
                CompactedParquetTestSupport.topicRequest(8, 3, 128, 2, "ZSTD");
        List<CompactedObjectRow> rows = List.of(
                CompactedParquetTestSupport.sparseValue(20, utf8("key-a"), utf8("value-a")),
                CompactedParquetTestSupport.sparseTombstone(23, utf8("key-b")),
                CompactedParquetTestSupport.sparseValue(27, utf8("key-c"), utf8("value-c")));

        try (StagingFileManager staging =
                        CompactedParquetTestSupport.staging(temporaryDirectory, 32L << 20);
                LocalFileObjectStore store =
                        new LocalFileObjectStore(temporaryDirectory.resolve("objects"));
                CompactedObjectWriteResult result = new ParquetCompactedObjectWriter(staging, Runnable::run)
                        .write(request, CompactedParquetTestSupport.publisher(rows))
                        .join()) {
            CompactedParquetTestSupport.upload(store, result);
            CompactedObjectVerificationRequest verification = CompactedObjectVerificationRequest.from(
                    request,
                    result,
                    Duration.ofSeconds(10));
            ParquetCompactedObjectReader storageReader =
                    new ParquetCompactedObjectReader(store, Runnable::run);
            new CompactedObjectVerifier(store, storageReader)
                    .verifyExact(verification, request)
                    .join();

            TopicCompactedObjectReader reader = new TopicCompactedObjectReader(storageReader);
            CompactedObjectReadResult full = reader.read(readRequest(request, verification, 20, 10)).join();
            assertThat(full.rows()).extracting(CompactedObjectRow::streamOffset)
                    .containsExactly(20L, 23L, 27L);
            assertThat(full.rows()).extracting(row -> row.sparseDisposition().orElseThrow())
                    .containsExactly(1, 2, 1);
            assertThat(full.rows().get(1).exactPayload().remaining()).isZero();
            assertThat(full.sourceCoverageEndOffset()).isEqualTo(28);
            assertThat(full.metadata().topicCompaction()).contains(request.topicCompaction().orElseThrow());

            CompactedObjectReadResult suffix = reader.read(readRequest(request, verification, 21, 1)).join();
            assertThat(suffix.rows()).extracting(CompactedObjectRow::streamOffset)
                    .containsExactly(23L);
            assertThat(suffix.sourceCoverageEndOffset()).isEqualTo(24);
        }
    }

    private static CompactedObjectReadRequest readRequest(
            CompactedObjectWriteRequest request,
            CompactedObjectVerificationRequest verification,
            long startOffset,
            int maxRecords) {
        return new CompactedObjectReadRequest(
                request.streamId(),
                request.view(),
                request.sourceCoverage(),
                startOffset,
                verification.target(),
                request.payloadFormat(),
                maxRecords,
                1 << 20,
                Duration.ofSeconds(10));
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
