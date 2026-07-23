/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBoundaryMode;
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

/** Full-file checksum plus closed-schema verification required before NCP2/NTC2 publication. */
public final class RangedCompactedObjectVerifier {
    private static final int HASH_CHUNK_BYTES = 8 << 20;

    private final ObjectStore objectStore;
    private final RangedCompactedObjectReader ncp2Reader;
    private final KafkaTopicCompactedObjectReader ntc2Reader;

    public RangedCompactedObjectVerifier(
            ObjectStore objectStore,
            RangedCompactedObjectReader ncp2Reader,
            KafkaTopicCompactedObjectReader ntc2Reader) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.ncp2Reader = Objects.requireNonNull(ncp2Reader, "ncp2Reader");
        this.ntc2Reader = Objects.requireNonNull(ntc2Reader, "ntc2Reader");
    }

    public CompletableFuture<RangedCompactedObjectMetadata> verify(
            RangedCompactedObjectVerificationRequest request) {
        if (request == null) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "V2 verification request is required"));
        }
        try {
            Deadline deadline = new Deadline(request.timeout());
            HashState hashes = new HashState();
            return hashNext(request, deadline, hashes, 0)
                    .thenApply(ignored -> hashes.finish(request))
                    .thenCompose(ignored -> verifyRows(request, deadline))
                    .exceptionallyCompose(failure -> CompletableFuture.failedFuture(mapFailure(failure)));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(mapFailure(failure));
        }
    }

    public CompletableFuture<Void> verifyExact(
            RangedCompactedObjectVerificationRequest request,
            RangedCompactedObjectWriteRequest expected) {
        return verify(request).thenAccept(metadata -> requireExpected(metadata, expected));
    }

    public CompletableFuture<Void> verifyExact(
            RangedCompactedObjectVerificationRequest request,
            KafkaTopicCompactedObjectWriteRequest expected) {
        return verify(request).thenAccept(metadata -> requireExpected(metadata, expected));
    }

    private CompletableFuture<Void> hashNext(
            RangedCompactedObjectVerificationRequest request,
            Deadline deadline,
            HashState hashes,
            long offset) {
        if (offset == request.target().objectLength()) {
            return CompletableFuture.completedFuture(null);
        }
        int count = Math.toIntExact(Math.min(HASH_CHUNK_BYTES, request.target().objectLength() - offset));
        return objectStore.readRange(
                        request.target().objectKey(), offset, count,
                        new RangeReadOptions(Optional.empty(), deadline.remaining()))
                .thenCompose(result -> {
                    requireRange(request, offset, count, result);
                    hashes.update(result.payload());
                    return hashNext(request, deadline, hashes, Math.addExact(offset, count));
                });
    }

    private CompletableFuture<RangedCompactedObjectMetadata> verifyRows(
            RangedCompactedObjectVerificationRequest request,
            Deadline deadline) {
        if (request.view() == ReadView.COMMITTED) {
            return ncp2Reader.read(new RangedCompactedObjectReadRequest(
                            request.streamId(), request.sourceCoverage(), request.sourceCoverage().startOffset(),
                            request.target(), request.payloadFormat(), ReadBoundaryMode.EXACT_START,
                            FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW, Integer.MAX_VALUE, Integer.MAX_VALUE,
                            deadline.remaining()))
                    .thenApply(RangedCompactedObjectReadResult::metadata);
        }
        return ntc2Reader.read(new KafkaTopicCompactedObjectReadRequest(
                        request.streamId(), request.sourceCoverage(), request.sourceCoverage().startOffset(),
                        request.target(), ReadBoundaryMode.EXACT_START, FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                        Integer.MAX_VALUE, Integer.MAX_VALUE, deadline.remaining()))
                .thenApply(KafkaTopicCompactedObjectReadResult::metadata);
    }

    private static void requireExpected(
            RangedCompactedObjectMetadata actual,
            KafkaTopicCompactedObjectWriteRequest expected) {
        if (actual.view() != ReadView.TOPIC_COMPACTED
                || !actual.streamId().equals(expected.streamId())
                || !actual.sourceCoverage().equals(expected.sourceCoverage())
                || !actual.sourceSetSha256().equals(expected.sourceSetSha256())
                || !actual.policySha256().equals(expected.policySha256())
                || !actual.outputAttemptId().equals(expected.outputAttemptId())
                || actual.outputRecordCount() != expected.outputRecordCount()
                || actual.entryCount() != expected.entryCount()
                || actual.logicalBytes() != expected.logicalBytes()
                || actual.cumulativeSizeAtEnd() != expected.cumulativeSizeAtEnd()
                || actual.targetRowGroupRecords() != expected.targetRowGroupRecords()
                || !actual.compression().equals(expected.compression())
                || !actual.writerBuild().equals(expected.writerBuild())
                || !actual.topicCompaction().equals(Optional.of(expected.topicCompaction()))) {
            throw new CompactedObjectFormatException("NTC2 metadata does not match frozen write request");
        }
    }

    private static void requireExpected(
            RangedCompactedObjectMetadata actual,
            RangedCompactedObjectWriteRequest expected) {
        if (actual.view() != ReadView.COMMITTED
                || !actual.streamId().equals(expected.streamId())
                || !actual.sourceCoverage().equals(expected.sourceCoverage())
                || !actual.sourceSetSha256().equals(expected.sourceSetSha256())
                || !actual.policySha256().equals(expected.policySha256())
                || !actual.outputAttemptId().equals(expected.outputAttemptId())
                || actual.payloadFormat() != expected.payloadFormat()
                || !actual.logicalFormat().equals(expected.logicalFormat())
                || actual.sourceRecordCount() != expected.sourceRecordCount()
                || actual.outputRecordCount() != expected.sourceRecordCount()
                || actual.entryCount() != expected.entryCount()
                || actual.logicalBytes() != expected.logicalBytes()
                || actual.cumulativeSizeAtEnd() != expected.cumulativeSizeAtEnd()
                || actual.targetRowGroupRecords() != expected.targetRowGroupRecords()
                || !actual.compression().equals(expected.compression())
                || !actual.writerBuild().equals(expected.writerBuild())
                || actual.topicCompaction().isPresent()) {
            throw new CompactedObjectFormatException("NCP2 metadata does not match frozen write request");
        }
    }

    private static void requireRange(
            RangedCompactedObjectVerificationRequest request,
            long offset,
            int count,
            RangeReadResult result) {
        if (!result.key().equals(request.target().objectKey())
                || result.offset() != offset
                || result.length() != count
                || result.payload().remaining() != count) {
            throw new CompactedObjectFormatException("object store returned mismatched V2 verification bytes");
        }
        result.checksum().ifPresent(checksum -> {
            if (!checksum.equals(Crc32cChecksums.checksum(result.payload()))) {
                throw new CompactedObjectFormatException("object store returned bad V2 range checksum");
            }
        });
    }

    private static Throwable mapFailure(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        if (current instanceof NereusException) {
            return current;
        }
        return new NereusException(
                ErrorCode.OBJECT_READ_FAILED, true, "full V2 compacted verification failed", current);
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
            crc32c.update(payload.asReadOnlyBuffer());
            sha256.update(payload);
            bytes = Math.addExact(bytes, count);
        }

        private Void finish(RangedCompactedObjectVerificationRequest request) {
            Checksum crc = Crc32cChecksums.checksum((int) crc32c.getValue());
            Checksum sha = new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(sha256.digest()));
            if (bytes != request.target().objectLength()
                    || !crc.equals(request.storageCrc32c())
                    || !sha.equals(request.contentSha256())
                    || !request.target().objectKey().value().contains("/" + sha.value() + "-")) {
                throw new CompactedObjectFormatException("complete V2 object checksum/key identity mismatch");
            }
            return null;
        }
    }

    private static final class Deadline {
        private final long deadlineNanos;

        private Deadline(Duration timeout) {
            long now = System.nanoTime();
            long nanos;
            try {
                nanos = timeout.toNanos();
            } catch (ArithmeticException failure) {
                nanos = Long.MAX_VALUE;
            }
            deadlineNanos = nanos >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + nanos;
        }

        private Duration remaining() {
            long nanos = deadlineNanos == Long.MAX_VALUE
                    ? Long.MAX_VALUE
                    : deadlineNanos - System.nanoTime();
            if (nanos <= 0) {
                throw new NereusException(ErrorCode.TIMEOUT, true, "V2 verification deadline expired");
            }
            return Duration.ofNanos(nanos);
        }
    }
}
