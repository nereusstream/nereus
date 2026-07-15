/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadView;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.zip.CRC32C;

/** Full-file checksum plus strict Parquet verification used before an output may be published. */
public final class CompactedObjectVerifier {
    private static final int HASH_CHUNK_BYTES = 8 << 20;
    private static final int VERIFY_RECORDS_PER_READ = 4_096;
    private static final int VERIFY_PAYLOAD_BYTES_PER_READ =
            CompactedObjectFormatV1.MAX_ROW_GROUP_BUFFER_BYTES;

    private final ObjectStore objectStore;
    private final CompactedObjectReader reader;

    public CompactedObjectVerifier(
            ObjectStore objectStore,
            CompactedObjectReader reader) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    public CompletableFuture<CompactedObjectMetadata> verify(
            CompactedObjectVerificationRequest request) {
        if (request == null) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "compacted verification request is required"));
        }
        try {
            VerificationDeadline deadline = new VerificationDeadline(request.timeout());
            HashState hashes = new HashState();
            CompletableFuture<CompactedObjectMetadata> operation = hashNext(request, deadline, hashes, 0)
                    .thenApply(ignored -> hashes.finish(request))
                    .thenCompose(ignored -> scanNext(
                            request,
                            deadline,
                            request.sourceCoverage().startOffset(),
                            null,
                            0,
                            0));
            return operation.handle((value, failure) -> failure == null
                            ? CompletableFuture.completedFuture(value)
                            : CompletableFuture.<CompactedObjectMetadata>failedFuture(mapFailure(failure)))
                    .thenCompose(value -> value);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(mapFailure(failure));
        }
    }

    public CompletableFuture<Void> verifyExact(
            CompactedObjectVerificationRequest request,
            CompactedObjectWriteRequest expected) {
        Objects.requireNonNull(expected, "expected");
        return verify(request).thenApply(metadata -> {
            requireExpectedMetadata(metadata, expected);
            return null;
        });
    }

    private CompletableFuture<Void> hashNext(
            CompactedObjectVerificationRequest request,
            VerificationDeadline deadline,
            HashState hashes,
            long offset) {
        if (offset == request.target().objectLength()) {
            return CompletableFuture.completedFuture(null);
        }
        int count = Math.toIntExact(Math.min(
                HASH_CHUNK_BYTES,
                request.target().objectLength() - offset));
        CompletableFuture<RangeReadResult> read;
        try {
            read = Objects.requireNonNull(objectStore.readRange(
                    request.target().objectKey(),
                    offset,
                    count,
                    new RangeReadOptions(Optional.empty(), deadline.remaining())), "range-read future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(mapFailure(failure));
        }
        long nextOffset = Math.addExact(offset, count);
        return read.thenCompose(result -> {
            requireExactRange(request, offset, count, result);
            hashes.update(result.payload());
            return hashNext(request, deadline, hashes, nextOffset);
        });
    }

    private CompletableFuture<CompactedObjectMetadata> scanNext(
            CompactedObjectVerificationRequest request,
            VerificationDeadline deadline,
            long startOffset,
            CompactedObjectMetadata expectedMetadata,
            long verifiedRecords,
            long verifiedPayloadBytes) {
        CompactedObjectReadRequest readRequest;
        try {
            readRequest = new CompactedObjectReadRequest(
                    request.streamId(),
                    request.view(),
                    request.sourceCoverage(),
                    startOffset,
                    request.target(),
                    request.payloadFormat(),
                    VERIFY_RECORDS_PER_READ,
                    VERIFY_PAYLOAD_BYTES_PER_READ,
                    deadline.remaining());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(mapFailure(failure));
        }
        CompletableFuture<CompactedObjectReadResult> read;
        try {
            read = Objects.requireNonNull(reader.read(readRequest), "compacted-reader future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(mapFailure(failure));
        }
        return read.thenCompose(result -> {
            CompactedObjectMetadata metadata = result.metadata();
            if (expectedMetadata != null && !metadata.equals(expectedMetadata)) {
                throw new CompactedObjectFormatException(
                        "compacted metadata changed between verification ranges");
            }
            long records = Math.addExact(verifiedRecords, result.rows().size());
            long payloadBytes = verifiedPayloadBytes;
            for (CompactedObjectRow row : result.rows()) {
                payloadBytes = Math.addExact(payloadBytes, row.exactPayload().remaining());
            }
            long nextOffset = result.sourceCoverageEndOffset();
            if (nextOffset == request.sourceCoverage().endOffset()) {
                requireCompleteScan(metadata, records, payloadBytes);
                return CompletableFuture.completedFuture(metadata);
            }
            if (nextOffset <= startOffset
                    || !request.sourceCoverage().contains(nextOffset)) {
                throw new CompactedObjectFormatException(
                        "strict compacted verification made no forward coverage progress");
            }
            return scanNext(
                    request,
                    deadline,
                    nextOffset,
                    metadata,
                    records,
                    payloadBytes);
        });
    }

    private static void requireExactRange(
            CompactedObjectVerificationRequest request,
            long offset,
            int count,
            RangeReadResult result) {
        if (!result.key().equals(request.target().objectKey())
                || result.offset() != offset
                || result.length() != count
                || result.payload().remaining() != count) {
            throw new CompactedObjectFormatException(
                    "object store returned mismatched bytes during full compacted verification");
        }
        result.checksum().ifPresent(checksum -> {
            if (!checksum.equals(Crc32cChecksums.checksum(result.payload()))) {
                throw new CompactedObjectFormatException(
                        "object store returned a mismatched range checksum");
            }
        });
    }

    private static void requireCompleteScan(
            CompactedObjectMetadata metadata,
            long verifiedRecords,
            long verifiedPayloadBytes) {
        if (verifiedRecords != metadata.outputRecordCount()
                || (metadata.view() == ReadView.COMMITTED
                        && verifiedPayloadBytes != metadata.logicalBytes())) {
            throw new CompactedObjectFormatException(
                    "strict compacted verification record/byte accounting is incomplete");
        }
    }

    private static void requireExpectedMetadata(
            CompactedObjectMetadata actual,
            CompactedObjectWriteRequest expected) {
        if (actual.view() != expected.view()
                || !actual.streamId().equals(expected.streamId())
                || !actual.sourceCoverage().equals(expected.sourceCoverage())
                || !actual.sourceSetSha256().equals(expected.sourceSetSha256())
                || !actual.policySha256().equals(expected.policySha256())
                || !actual.outputAttemptId().equals(expected.outputAttemptId())
                || actual.payloadFormat() != expected.payloadFormat()
                || !actual.logicalFormat().equals(expected.logicalFormat())
                || !actual.projectionIdentitySha256().equals(expected.projectionIdentitySha256())
                || actual.sourceRecordCount() != expected.sourceRecordCount()
                || actual.outputRecordCount() != expected.expectedOutputRecordCount()
                || actual.entryCount() != expected.entryCount()
                || actual.logicalBytes() != expected.logicalBytes()
                || actual.cumulativeSizeAtEnd() != expected.cumulativeSizeAtEnd()
                || !actual.writerBuild().equals(expected.writerBuild())
                || !actual.compression().equals(expected.compression())
                || actual.targetRowGroupRecords() != expected.targetRowGroupRecords()
                || !actual.topicCompaction().equals(expected.topicCompaction())) {
            throw new CompactedObjectFormatException(
                    "compacted file metadata does not match its frozen write request");
        }
    }

    private static Throwable mapFailure(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        if (current instanceof NereusException) {
            return current;
        }
        if (current instanceof IllegalArgumentException || current instanceof ArithmeticException) {
            return new CompactedObjectFormatException(
                    "invalid compacted verification state", current);
        }
        return new NereusException(
                ErrorCode.OBJECT_READ_FAILED,
                true,
                "full compacted object verification failed",
                current);
    }

    private static final class HashState {
        private final CRC32C crc32c = new CRC32C();
        private final MessageDigest sha256;
        private long bytes;

        private HashState() {
            try {
                sha256 = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
        }

        private void update(ByteBuffer supplied) {
            ByteBuffer payload = supplied.asReadOnlyBuffer();
            int count = payload.remaining();
            ByteBuffer crcInput = payload.asReadOnlyBuffer();
            crc32c.update(crcInput);
            sha256.update(payload);
            bytes = Math.addExact(bytes, count);
        }

        private Void finish(CompactedObjectVerificationRequest request) {
            Checksum crc = Crc32cChecksums.checksum((int) crc32c.getValue());
            Checksum sha = new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(sha256.digest()));
            String keyHash = contentHashFromKey(request.target().objectKey().value());
            if (bytes != request.target().objectLength()
                    || !crc.equals(request.storageCrc32c())
                    || !sha.equals(request.contentSha256())
                    || !sha.value().equals(keyHash)) {
                throw new CompactedObjectFormatException(
                        "complete compacted object checksum/key identity mismatch");
            }
            return null;
        }

        private static String contentHashFromKey(String key) {
            int slash = key.lastIndexOf('/');
            int dash = slash < 0 ? -1 : key.indexOf('-', slash + 1);
            if (slash < 0 || dash != slash + 65) {
                throw new CompactedObjectFormatException(
                        "compacted object key content hash is not canonical");
            }
            String value = key.substring(slash + 1, dash);
            if (value.length() != 64) {
                throw new CompactedObjectFormatException(
                        "compacted object key content hash is not canonical");
            }
            for (int index = 0; index < value.length(); index++) {
                char character = value.charAt(index);
                if (!((character >= '0' && character <= '9')
                        || (character >= 'a' && character <= 'f'))) {
                    throw new CompactedObjectFormatException(
                            "compacted object key content hash is not canonical");
                }
            }
            return value;
        }
    }

    private static final class VerificationDeadline {
        private final long deadlineNanos;

        private VerificationDeadline(Duration timeout) {
            long now = System.nanoTime();
            long duration;
            try {
                duration = timeout.toNanos();
            } catch (ArithmeticException failure) {
                duration = Long.MAX_VALUE;
            }
            long candidate;
            try {
                candidate = Math.addExact(now, duration);
            } catch (ArithmeticException failure) {
                candidate = Long.MAX_VALUE;
            }
            deadlineNanos = candidate;
        }

        private Duration remaining() {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "compacted object verification deadline expired");
            }
            return Duration.ofNanos(remaining);
        }
    }
}
