/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.Crc32cChecksums;
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

class CompactedParquetCorruptionTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void fullVerifierRejectsChangedPageBytesEvenWhenRangeChecksumsDescribeTheChangedObject()
            throws Exception {
        byte[] payload = "unique-payload-for-page-corruption".getBytes(StandardCharsets.UTF_8);
        CompactedObjectWriteRequest request =
                CompactedParquetTestSupport.committedRequest(1, payload.length, 1, "UNCOMPRESSED");
        Path stagingParent = temporaryDirectory.resolve("data-staging-parent");
        java.nio.file.Files.createDirectories(stagingParent);

        try (StagingFileManager staging = CompactedParquetTestSupport.staging(
                        stagingParent, 32L << 20);
                LocalFileObjectStore store =
                        new LocalFileObjectStore(temporaryDirectory.resolve("objects-data"));
                CompactedObjectWriteResult result = new ParquetCompactedObjectWriter(staging, Runnable::run)
                        .write(request, CompactedParquetTestSupport.publisher(List.of(
                                CompactedParquetTestSupport.denseRow(10, payload))))
                        .join()) {
            byte[] object = CompactedParquetTestSupport.collect(result.stagingFile());
            CompactedParquetTestSupport.upload(store, result);
            CompactedObjectVerificationRequest frozen = CompactedObjectVerificationRequest.from(
                    request,
                    result,
                    Duration.ofSeconds(10));
            ParquetCompactedObjectReader reader =
                    new ParquetCompactedObjectReader(store, Runnable::run);
            CompactedObjectVerifier verifier = new CompactedObjectVerifier(store, reader);
            verifier.verifyExact(frozen, request).join();

            int payloadOffset = indexOf(object, payload);
            assertThat(payloadOffset).isGreaterThanOrEqualTo(0);
            assertThat(payloadOffset + payload.length)
                    .isLessThanOrEqualTo(Math.toIntExact(result.entryIndexRef().offset()));
            byte[] corrupt = object.clone();
            corrupt[payloadOffset + payload.length / 2] ^= 0x40;
            overwrite(store, result, corrupt);

            assertThatThrownBy(() -> verifier.verifyExact(frozen, request).join())
                    .satisfies(failure -> assertChecksumFailure(failure));

            ObjectSliceReadTarget changedChecksumTarget = new ObjectSliceReadTarget(
                    1,
                    result.objectId(),
                    result.objectKey(),
                    frozen.target().objectType(),
                    result.physicalFormat(),
                    request.logicalFormat(),
                    frozen.target().sliceId(),
                    0,
                    result.objectLength(),
                    Crc32cChecksums.checksum(corrupt),
                    result.entryIndexRef());
            assertThatThrownBy(() -> reader.read(new CompactedObjectReadRequest(
                                    request.streamId(),
                                    request.view(),
                                    request.sourceCoverage(),
                                    10,
                                    changedChecksumTarget,
                                    request.payloadFormat(),
                                    1,
                                    1 << 20,
                                    Duration.ofSeconds(10)))
                            .join())
                    .satisfies(failure -> assertChecksumFailure(failure));
        }
    }

    @Test
    void strictReaderRejectsFooterMutationBeforeTrustingParquetMetadata() throws Exception {
        byte[] payload = "footer-check".getBytes(StandardCharsets.UTF_8);
        CompactedObjectWriteRequest request =
                CompactedParquetTestSupport.committedRequest(1, payload.length, 1, "UNCOMPRESSED");
        Path stagingParent = temporaryDirectory.resolve("footer-staging-parent");
        java.nio.file.Files.createDirectories(stagingParent);
        try (StagingFileManager staging =
                        CompactedParquetTestSupport.staging(stagingParent, 32L << 20);
                LocalFileObjectStore store =
                        new LocalFileObjectStore(temporaryDirectory.resolve("objects-footer"));
                CompactedObjectWriteResult result = new ParquetCompactedObjectWriter(staging, Runnable::run)
                        .write(request, CompactedParquetTestSupport.publisher(List.of(
                                CompactedParquetTestSupport.denseRow(10, payload))))
                        .join()) {
            byte[] corrupt = CompactedParquetTestSupport.collect(result.stagingFile());
            int footerByte = Math.toIntExact(result.entryIndexRef().offset()) + 1;
            corrupt[footerByte] ^= 0x01;
            overwrite(store, result, corrupt);

            ParquetCompactedObjectReader reader =
                    new ParquetCompactedObjectReader(store, Runnable::run);
            ObjectSliceReadTarget target = CompactedParquetTestSupport.target(request, result);
            assertThatThrownBy(() -> reader.read(new CompactedObjectReadRequest(
                                    request.streamId(),
                                    request.view(),
                                    request.sourceCoverage(),
                                    10,
                                    target,
                                    request.payloadFormat(),
                                    1,
                                    1 << 20,
                                    Duration.ofSeconds(10)))
                            .join())
                    .satisfies(failure -> assertChecksumFailure(failure));
        }
    }

    private static void overwrite(
            LocalFileObjectStore store,
            CompactedObjectWriteResult result,
            byte[] bytes) {
        store.putObject(
                        result.objectKey(),
                        ByteBuffer.wrap(bytes),
                        new PutObjectOptions(
                                "application/vnd.apache.parquet",
                                Crc32cChecksums.checksum(bytes),
                                false,
                                Map.of(),
                                Duration.ofSeconds(10)))
                .join();
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int index = 0; index <= haystack.length - needle.length; index++) {
            for (int inner = 0; inner < needle.length; inner++) {
                if (haystack[index + inner] != needle[inner]) {
                    continue outer;
                }
            }
            return index;
        }
        return -1;
    }

    private static void assertChecksumFailure(Throwable supplied) {
        Throwable current = supplied;
        while (current != null && !(current instanceof NereusException)) {
            current = current.getCause();
        }
        assertThat(current).isInstanceOfSatisfying(NereusException.class, failure -> {
            assertThat(failure.code()).isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH);
            assertThat(failure.retriable()).isFalse();
        });
    }
}
