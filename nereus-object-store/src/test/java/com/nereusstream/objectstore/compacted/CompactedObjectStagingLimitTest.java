/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactedObjectStagingLimitTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void stagingBackpressureFailsTheFutureAndDeletesThePartialFile() throws Exception {
        byte[] payload = new byte[16 << 10];
        java.util.Arrays.fill(payload, (byte) 0x5a);
        CompactedObjectWriteRequest request = CompactedParquetTestSupport.committedRequest(
                1,
                payload.length,
                1,
                "UNCOMPRESSED");
        Path stagingDirectory = temporaryDirectory.resolve("staging");
        try (StagingFileManager staging =
                CompactedParquetTestSupport.staging(temporaryDirectory, 1 << 10)) {
            assertThatThrownBy(() -> new ParquetCompactedObjectWriter(staging, Runnable::run)
                            .write(request, CompactedParquetTestSupport.publisher(List.of(
                                    CompactedParquetTestSupport.denseRow(10, payload))))
                            .join())
                    .satisfies(failure -> {
                        NereusException nereus = findNereus(failure);
                        assertThat(nereus.code()).isEqualTo(ErrorCode.BACKPRESSURE_REJECTED);
                        assertThat(nereus.retriable()).isTrue();
                    });
            assertThat(staging.reservedBytes()).isZero();
            try (var files = Files.list(stagingDirectory)) {
                assertThat(files.toList()).isEmpty();
            }
        }
    }

    private static NereusException findNereus(Throwable supplied) {
        Throwable current = supplied;
        while (current != null && !(current instanceof NereusException)) {
            current = current.getCause();
        }
        assertThat(current).isInstanceOf(NereusException.class);
        return (NereusException) current;
    }
}
