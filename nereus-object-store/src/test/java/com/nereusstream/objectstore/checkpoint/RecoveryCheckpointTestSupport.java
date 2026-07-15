/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.staging.StagedObjectFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class RecoveryCheckpointTestSupport {
    static final Duration TIMEOUT = Duration.ofSeconds(10);

    private RecoveryCheckpointTestSupport() {
    }

    static StagingFileManager staging(Path parent, long bytes) throws IOException {
        Files.createDirectories(parent);
        Path directory = Files.createDirectory(parent.resolve("staging"));
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        return new StagingFileManager(
                directory,
                bytes,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run);
    }

    static RecoveryCheckpointWriteRequest request(String attemptId) {
        return new RecoveryCheckpointWriteRequest(
                "test-cluster",
                new StreamId("s-recovery-test"),
                7,
                attemptId,
                new OffsetRange(10, 14),
                5,
                6,
                100,
                111,
                "commit-5",
                "commit-6",
                "commit-7",
                7,
                sha256("projection"),
                2,
                2);
    }

    static List<RecoveryCheckpointPublication> publications() {
        return List.of(
                publication(1, "a".repeat(26), 10, 14),
                publication(2, "b".repeat(26), 12, 14));
    }

    static List<RecoveryCheckpointEntry> entries() {
        return List.of(
                entry(5, 10, 12, 105, "commit-5", "commit-4", List.of(0)),
                entry(6, 12, 14, 111, "commit-6", "commit-5", List.of(0, 1)));
    }

    static RecoveryCheckpointPublication publication(
            long generation,
            String publicationId,
            long start,
            long end) {
        byte[] canonical = ("generation:" + generation + ":" + publicationId)
                .getBytes(StandardCharsets.UTF_8);
        return new RecoveryCheckpointPublication(
                generation,
                new PublicationId(publicationId),
                new OffsetRange(start, end),
                ByteBuffer.wrap(canonical).asReadOnlyBuffer(),
                sha256(canonical));
    }

    static RecoveryCheckpointEntry entry(
            long version,
            long start,
            long end,
            long cumulativeEnd,
            String commitId,
            String previousCommitId,
            List<Integer> publications) {
        byte[] canonical = ("commit:" + version + ":" + commitId)
                .getBytes(StandardCharsets.UTF_8);
        return new RecoveryCheckpointEntry(
                version,
                new OffsetRange(start, end),
                cumulativeEnd,
                commitId,
                previousCommitId,
                ByteBuffer.wrap(canonical).asReadOnlyBuffer(),
                sha256(canonical),
                publications);
    }

    static RecoveryCheckpointVerifier verifier() {
        return new RecoveryCheckpointVerifier() {
            @Override
            public void verifyPublication(
                    RecoveryCheckpointWriteRequest header,
                    RecoveryCheckpointPublication publication) {
                String expected = "generation:" + publication.generation()
                        + ":" + publication.publicationId().value();
                if (!expected.equals(text(publication.canonicalGenerationIndexRecord()))) {
                    throw new RecoveryCheckpointFormatException("test generation record identity mismatch");
                }
                if (!header.streamId().value().equals("s-recovery-test")) {
                    throw new RecoveryCheckpointFormatException("test header stream mismatch");
                }
            }

            @Override
            public void verifyEntry(
                    RecoveryCheckpointWriteRequest header,
                    RecoveryCheckpointEntry entry) {
                String expected = "commit:" + entry.commitVersion() + ":" + entry.commitId();
                if (!expected.equals(text(entry.canonicalCommitRecord()))) {
                    throw new RecoveryCheckpointFormatException("test commit record identity mismatch");
                }
            }
        };
    }

    static <T> Flow.Publisher<T> publisher(List<T> values) {
        return new TrackingPublisher<>(values);
    }

    static void upload(ObjectStore store, RecoveryCheckpointWriteResult result) {
        store.putObject(
                        result.objectKey(),
                        result.stagingFile(),
                        new PutObjectOptions(
                                RecoveryCheckpointFormatV1.CONTENT_TYPE,
                                result.storageCrc32c(),
                                true,
                                Map.of("nereus-format", "NRC1"),
                                TIMEOUT))
                .join();
    }

    static byte[] collect(StagedObjectFile staged) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompletableFuture<Void> completed = new CompletableFuture<>();
        staged.openPublisher().subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription value) {
                subscription = value;
                value.request(1);
            }

            @Override
            public void onNext(ByteBuffer value) {
                ByteBuffer copy = value.asReadOnlyBuffer();
                byte[] chunk = new byte[copy.remaining()];
                copy.get(chunk);
                bytes.writeBytes(chunk);
                subscription.request(1);
            }

            @Override
            public void onError(Throwable failure) {
                completed.completeExceptionally(failure);
            }

            @Override
            public void onComplete() {
                completed.complete(null);
            }
        });
        completed.join();
        return bytes.toByteArray();
    }

    static Checksum sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    static Checksum sha256(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }

    static Checksum crc32c(byte[] value) {
        return Crc32cChecksums.checksum(value);
    }

    static String text(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static final class TrackingPublisher<T> implements Flow.Publisher<T> {
        private final List<T> values;
        private final AtomicInteger maximumRequest = new AtomicInteger();
        private final AtomicInteger requestCalls = new AtomicInteger();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        TrackingPublisher(List<T> values) {
            this.values = List.copyOf(values);
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {
            subscriber.onSubscribe(new Flow.Subscription() {
                private int index;
                private boolean complete;

                @Override
                public void request(long count) {
                    if (complete) {
                        return;
                    }
                    requestCalls.incrementAndGet();
                    maximumRequest.accumulateAndGet(Math.toIntExact(count), Math::max);
                    if (count != 1) {
                        complete = true;
                        subscriber.onError(new AssertionError("codec demand must be exactly one item"));
                        return;
                    }
                    subscriber.onNext(values.get(index++));
                    if (index == values.size()) {
                        complete = true;
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    complete = true;
                    cancelled.set(true);
                }
            });
        }

        int maximumRequest() {
            return maximumRequest.get();
        }

        int requestCalls() {
            return requestCalls.get();
        }

        boolean cancelled() {
            return cancelled.get();
        }
    }
}
