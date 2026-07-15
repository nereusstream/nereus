/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ObjectType;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactedParquetStrictReaderTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void readsExactDenseRowsAndHonorsPrefixLimitsFromAnImmutableObject() throws Exception {
        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second".getBytes(StandardCharsets.UTF_8);
        byte[] third = "third".getBytes(StandardCharsets.UTF_8);
        CompactedObjectWriteRequest writeRequest = CompactedParquetTestSupport.committedRequest(
                3,
                first.length + second.length + third.length,
                2,
                "ZSTD");

        try (StagingFileManager staging =
                        CompactedParquetTestSupport.staging(temporaryDirectory, 32L << 20);
                LocalFileObjectStore store =
                        new LocalFileObjectStore(temporaryDirectory.resolve("objects"));
                CompactedObjectWriteResult written = new ParquetCompactedObjectWriter(staging, Runnable::run)
                        .write(writeRequest, CompactedParquetTestSupport.publisher(List.of(
                                CompactedParquetTestSupport.denseRow(10, first),
                                CompactedParquetTestSupport.denseRow(11, second),
                                CompactedParquetTestSupport.denseRow(12, third))))
                        .join()) {
            store.putObject(
                            written.objectKey(),
                            written.stagingFile(),
                            new PutObjectOptions(
                                    "application/vnd.apache.parquet",
                                    written.storageCrc32c(),
                                    true,
                                    Map.of(),
                                    Duration.ofSeconds(10)))
                    .join();

            ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                    1,
                    written.objectId(),
                    written.objectKey(),
                    ObjectType.STREAM_COMPACTED_OBJECT,
                    written.physicalFormat(),
                    writeRequest.logicalFormat(),
                    "10-13",
                    0,
                    written.objectLength(),
                    written.storageCrc32c(),
                    written.entryIndexRef());
            ParquetCompactedObjectReader reader =
                    new ParquetCompactedObjectReader(store, Runnable::run);

            CompactedObjectReadResult full = reader.read(new CompactedObjectReadRequest(
                            writeRequest.streamId(),
                            writeRequest.view(),
                            writeRequest.sourceCoverage(),
                            10,
                            target,
                            writeRequest.payloadFormat(),
                            10,
                            1 << 20,
                            Duration.ofSeconds(10)))
                    .join();
            assertThat(full.metadata().sourceSetSha256()).isEqualTo(writeRequest.sourceSetSha256());
            assertThat(full.rows()).extracting(CompactedObjectRow::streamOffset)
                    .containsExactly(10L, 11L, 12L);
            assertThat(full.rows()).extracting(CompactedParquetStrictReaderTest::payload)
                    .containsExactly("first", "second", "third");
            assertThat(full.sourceCoverageEndOffset()).isEqualTo(13);
            assertThat(full.footerBytesRead()).isEqualTo(written.entryIndexRef().length());
            assertThat(full.physicalBytesRead()).isGreaterThanOrEqualTo(full.footerBytesRead());

            CompactedObjectReadResult limited = reader.read(new CompactedObjectReadRequest(
                            writeRequest.streamId(),
                            writeRequest.view(),
                            writeRequest.sourceCoverage(),
                            11,
                            target,
                            writeRequest.payloadFormat(),
                            1,
                            second.length,
                            Duration.ofSeconds(10)))
                    .join();
            assertThat(limited.rows()).extracting(CompactedObjectRow::streamOffset)
                    .containsExactly(11L);
            assertThat(payload(limited.rows().get(0))).isEqualTo("second");
            assertThat(limited.sourceCoverageEndOffset()).isEqualTo(12);
        }
    }

    private static String payload(CompactedObjectRow row) {
        ByteBuffer payload = row.exactPayload();
        byte[] bytes = new byte[payload.remaining()];
        payload.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
