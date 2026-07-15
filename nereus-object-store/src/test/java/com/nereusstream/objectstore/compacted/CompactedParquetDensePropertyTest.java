/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactedParquetDensePropertyTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void randomizedDenseFilesRoundTripAcrossRowGroupsAndCodecs() throws Exception {
        Random random = new Random(0x4e435031L);
        try (StagingFileManager staging =
                        CompactedParquetTestSupport.staging(temporaryDirectory, 64L << 20);
                LocalFileObjectStore store =
                        new LocalFileObjectStore(temporaryDirectory.resolve("objects"))) {
            ParquetCompactedObjectWriter writer =
                    new ParquetCompactedObjectWriter(staging, Runnable::run);
            ParquetCompactedObjectReader reader =
                    new ParquetCompactedObjectReader(store, Runnable::run);
            CompactedObjectVerifier verifier = new CompactedObjectVerifier(store, reader);

            for (int trial = 0; trial < 24; trial++) {
                int records = 1 + random.nextInt(80);
                int rowGroupRecords = 1 + random.nextInt(Math.min(records, 17));
                String compression = (trial & 1) == 0 ? "ZSTD" : "UNCOMPRESSED";
                List<byte[]> payloads = new ArrayList<>();
                List<CompactedObjectRow> rows = new ArrayList<>();
                long logicalBytes = 0;
                for (int index = 0; index < records; index++) {
                    byte[] payload = new byte[random.nextInt(513)];
                    random.nextBytes(payload);
                    payloads.add(payload);
                    rows.add(CompactedParquetTestSupport.denseRow(10L + index, payload));
                    logicalBytes += payload.length;
                }
                CompactedObjectWriteRequest request = CompactedParquetTestSupport.committedRequest(
                        records,
                        logicalBytes,
                        rowGroupRecords,
                        compression);

                try (CompactedObjectWriteResult result = writer
                        .write(request, CompactedParquetTestSupport.publisher(rows))
                        .join()) {
                    CompactedParquetTestSupport.upload(store, result);
                    CompactedObjectVerificationRequest verification =
                            CompactedObjectVerificationRequest.from(
                                    request,
                                    result,
                                    Duration.ofSeconds(20));
                    verifier.verifyExact(verification, request).join();

                    CompactedObjectReadResult read = reader.read(new CompactedObjectReadRequest(
                                    request.streamId(),
                                    request.view(),
                                    request.sourceCoverage(),
                                    request.sourceCoverage().startOffset(),
                                    verification.target(),
                                    request.payloadFormat(),
                                    records,
                                    Integer.MAX_VALUE,
                                    Duration.ofSeconds(20)))
                            .join();
                    assertThat(read.rows()).hasSize(records);
                    for (int index = 0; index < records; index++) {
                        assertThat(read.rows().get(index).streamOffset()).isEqualTo(10L + index);
                        assertThat(bytes(read.rows().get(index).exactPayload()))
                                .containsExactly(payloads.get(index));
                    }
                }
            }
        }
    }

    private static byte[] bytes(ByteBuffer supplied) {
        ByteBuffer value = supplied.asReadOnlyBuffer();
        byte[] result = new byte[value.remaining()];
        value.get(result);
        return result;
    }
}
