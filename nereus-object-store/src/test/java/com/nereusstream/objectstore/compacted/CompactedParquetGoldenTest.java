/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactedParquetGoldenTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void writesTheFrozenNcp1SchemaMetadataFooterAndExactRecordBoundedGroups() throws Exception {
        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second".getBytes(StandardCharsets.UTF_8);
        byte[] third = "third".getBytes(StandardCharsets.UTF_8);
        long logicalBytes = first.length + second.length + third.length;
        CompactedObjectWriteRequest request =
                CompactedParquetTestSupport.committedRequest(3, logicalBytes, 2, "UNCOMPRESSED");

        try (StagingFileManager staging =
                        CompactedParquetTestSupport.staging(temporaryDirectory, 32L << 20);
                CompactedObjectWriteResult result = new ParquetCompactedObjectWriter(staging, Runnable::run)
                        .write(request, CompactedParquetTestSupport.publisher(List.of(
                                CompactedParquetTestSupport.denseRow(10, first),
                                CompactedParquetTestSupport.denseRow(11, second),
                                CompactedParquetTestSupport.denseRow(12, third))))
                        .join()) {
            byte[] bytes = CompactedParquetTestSupport.collect(result.stagingFile());
            assertThat(Arrays.copyOfRange(bytes, 0, 4)).containsExactly('P', 'A', 'R', '1');
            assertThat(Arrays.copyOfRange(bytes, bytes.length - 4, bytes.length))
                    .containsExactly('P', 'A', 'R', '1');
            assertThat(result.objectLength()).isEqualTo(bytes.length);
            assertThat(result.storageCrc32c()).isEqualTo(Crc32cChecksums.checksum(bytes));
            assertThat(result.objectKey().value())
                    .contains("/compacted/v1/committed/")
                    .endsWith(result.contentSha256().value() + "-" + request.outputAttemptId() + ".parquet");

            byte[] footer = Arrays.copyOfRange(
                    bytes,
                    Math.toIntExact(result.entryIndexRef().offset()),
                    bytes.length);
            assertThat(result.entryIndexRef().checksum()).isEqualTo(Crc32cChecksums.checksum(footer));

            try (ParquetFileReader reader =
                    ParquetFileReader.open(CompactedParquetTestSupport.input(bytes))) {
                assertThat(reader.getFileMetaData().getSchema())
                        .isEqualTo(CompactedObjectFormatV1.COMMITTED_SCHEMA);
                assertThat(reader.getFileMetaData().getKeyValueMetaData())
                        .containsAllEntriesOf(CompactedObjectFormatV1.metadata(request));
                assertThat(reader.getRowGroups()).extracting(group -> group.getRowCount())
                        .containsExactly(2L, 1L);
                assertThat(reader.getRowGroups()).hasSize(2);
                reader.getRowGroups().forEach(group -> {
                    var offsetColumn = group.getColumns().stream()
                            .filter(column -> column.getPath().toDotString().equals("stream_offset"))
                            .findFirst()
                            .orElseThrow();
                    assertThat(offsetColumn.getStatistics().hasNonNullValue()).isTrue();
                });
            }
        }
    }
}
