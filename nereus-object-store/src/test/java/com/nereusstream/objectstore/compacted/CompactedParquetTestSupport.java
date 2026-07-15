/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.staging.StagedObjectFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

final class CompactedParquetTestSupport {
    private CompactedParquetTestSupport() {
    }

    static StagingFileManager staging(Path parent, long bytes) throws IOException {
        Path directory = Files.createDirectory(parent.resolve("staging"));
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        return new StagingFileManager(
                directory,
                bytes,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run);
    }

    static CompactedObjectWriteRequest committedRequest(
            int records,
            long logicalBytes,
            int rowGroupRecords,
            String compression) {
        return new CompactedObjectWriteRequest(
                "test-cluster",
                ReadView.COMMITTED,
                new StreamId("s-compacted-test"),
                new OffsetRange(10, 10 + records),
                "a".repeat(26),
                sha256('1'),
                sha256('2'),
                PayloadFormat.PULSAR_ENTRY_BATCH,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                Optional.empty(),
                records,
                records,
                records,
                logicalBytes,
                List.of(),
                100,
                100 + logicalBytes,
                rowGroupRecords,
                compression,
                "nereus-test-build",
                Optional.empty());
    }

    static CompactedObjectWriteRequest topicRequest(
            int sourceRecords,
            int outputRecords,
            long logicalBytes,
            int rowGroupRecords,
            String compression) {
        return new CompactedObjectWriteRequest(
                "test-cluster",
                ReadView.TOPIC_COMPACTED,
                new StreamId("s-topic-compacted-test"),
                new OffsetRange(20, 20 + sourceRecords),
                "b".repeat(26),
                sha256('3'),
                sha256('4'),
                PayloadFormat.PULSAR_ENTRY_BATCH,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                Optional.empty(),
                sourceRecords,
                outputRecords,
                sourceRecords,
                logicalBytes,
                List.of(),
                500,
                500 + logicalBytes,
                rowGroupRecords,
                compression,
                "nereus-test-build",
                Optional.of(new TopicCompactionFormatSpec(
                        "latest-key",
                        1,
                        "pulsar-message-key-v1")));
    }

    static CompactedObjectRow denseRow(long offset, byte[] payload) {
        return new CompactedObjectRow(
                offset,
                ByteBuffer.wrap(payload),
                Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)),
                OptionalLong.of(1_000 + offset),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty());
    }

    static CompactedObjectRow sparseValue(long offset, byte[] key, byte[] payload) {
        return new CompactedObjectRow(
                offset,
                ByteBuffer.wrap(payload),
                Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)),
                OptionalLong.of(2_000 + offset),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                OptionalInt.of(1),
                Optional.of(ByteBuffer.wrap(key)));
    }

    static CompactedObjectRow sparseTombstone(long offset, byte[] key) {
        byte[] empty = new byte[0];
        return new CompactedObjectRow(
                offset,
                ByteBuffer.wrap(empty),
                Crc32cChecksums.intValue(Crc32cChecksums.checksum(empty)),
                OptionalLong.of(2_000 + offset),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                OptionalInt.of(2),
                Optional.of(ByteBuffer.wrap(key)));
    }

    static ObjectSliceReadTarget target(
            CompactedObjectWriteRequest request,
            CompactedObjectWriteResult result) {
        return new ObjectSliceReadTarget(
                1,
                result.objectId(),
                result.objectKey(),
                ObjectType.STREAM_COMPACTED_OBJECT,
                result.physicalFormat(),
                request.logicalFormat(),
                request.sourceCoverage().startOffset() + "-" + request.sourceCoverage().endOffset(),
                0,
                result.objectLength(),
                result.storageCrc32c(),
                result.entryIndexRef());
    }

    static void upload(ObjectStore store, CompactedObjectWriteResult result) {
        store.putObject(
                        result.objectKey(),
                        result.stagingFile(),
                        new PutObjectOptions(
                                "application/vnd.apache.parquet",
                                result.storageCrc32c(),
                                true,
                                Map.of(),
                                Duration.ofSeconds(10)))
                .join();
    }

    static <T> Flow.Publisher<T> publisher(List<T> values) {
        List<T> immutable = List.copyOf(values);
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean done;

            @Override
            public void request(long count) {
                if (done) {
                    return;
                }
                if (count <= 0) {
                    done = true;
                    subscriber.onError(new IllegalArgumentException("non-positive demand"));
                    return;
                }
                long emitted = 0;
                while (!done && emitted < count && index < immutable.size()) {
                    subscriber.onNext(immutable.get(index++));
                    emitted++;
                }
                if (!done && index == immutable.size()) {
                    done = true;
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                done = true;
            }
        });
    }

    static byte[] collect(StagedObjectFile file) {
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        file.openPublisher().subscribe(new Flow.Subscriber<>() {
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
                result.completeExceptionally(failure);
            }

            @Override
            public void onComplete() {
                result.complete(bytes.toByteArray());
            }
        });
        return result.join();
    }

    static InputFile input(byte[] bytes) {
        byte[] immutable = bytes.clone();
        return new InputFile() {
            @Override
            public long getLength() {
                return immutable.length;
            }

            @Override
            public SeekableInputStream newStream() {
                return new ByteArraySeekableInputStream(immutable);
            }
        };
    }

    static Checksum sha256(char digit) {
        return new Checksum(ChecksumType.SHA256, String.valueOf(digit).repeat(64));
    }

    private static final class ByteArraySeekableInputStream extends SeekableInputStream {
        private final byte[] bytes;
        private int position;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ByteArraySeekableInputStream(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public long getPos() throws IOException {
            ensureOpen();
            return position;
        }

        @Override
        public void seek(long newPosition) throws IOException {
            ensureOpen();
            if (newPosition < 0 || newPosition > bytes.length) {
                throw new IOException("seek outside input");
            }
            position = Math.toIntExact(newPosition);
        }

        @Override
        public int read() throws IOException {
            ensureOpen();
            return position == bytes.length ? -1 : bytes[position++] & 0xff;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            ensureOpen();
            if (position == bytes.length) {
                return -1;
            }
            int count = Math.min(length, bytes.length - position);
            System.arraycopy(bytes, position, target, offset, count);
            position += count;
            return count;
        }

        @Override
        public void readFully(byte[] target) throws IOException {
            readFully(target, 0, target.length);
        }

        @Override
        public void readFully(byte[] target, int offset, int length) throws IOException {
            ensureOpen();
            if (length > bytes.length - position) {
                throw new java.io.EOFException();
            }
            System.arraycopy(bytes, position, target, offset, length);
            position += length;
        }

        @Override
        public int read(ByteBuffer target) throws IOException {
            ensureOpen();
            if (position == bytes.length) {
                return -1;
            }
            int count = Math.min(target.remaining(), bytes.length - position);
            target.put(bytes, position, count);
            position += count;
            return count;
        }

        @Override
        public void readFully(ByteBuffer target) throws IOException {
            ensureOpen();
            if (target.remaining() > bytes.length - position) {
                throw new java.io.EOFException();
            }
            int count = target.remaining();
            target.put(bytes, position, count);
            position += count;
        }

        @Override
        public void close() {
            closed.set(true);
        }

        private void ensureOpen() throws IOException {
            if (closed.get()) {
                throw new IOException("input is closed");
            }
        }
    }
}
