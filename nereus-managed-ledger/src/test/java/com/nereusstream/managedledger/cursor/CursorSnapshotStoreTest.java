/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class CursorSnapshotStoreTest {
    private static final String SECOND_SNAPSHOT = "00110011001100110011001100110011";
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(105), ZoneOffset.UTC);

    @Test
    void writesImmutableObjectVerifiesHeadAndStrictlyReadsItBack() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        DefaultCursorSnapshotStore store = store(
                objectStore, new ArrayDeque<>(java.util.List.of(CursorTestSamples.SNAPSHOT)));

        CursorSnapshotReference reference = store.write(CursorTestSamples.request()).join();
        CursorAckState recovered = store.read(reference, CursorTestSamples.identity()).join();

        assertThat(recovered).isEqualTo(CursorTestSamples.complexState());
        assertThat(reference.objectKey().value()).isEqualTo(
                KeyComponentCodec.encodeComponent(CursorTestSamples.CLUSTER)
                        + "/cursor-snapshots/v1/"
                        + KeyComponentCodec.encodeComponent(
                                CursorTestSamples.identity().ledger().projection().streamId())
                        + "/"
                        + CursorTestSamples.identity().cursorNameHash()
                        + "/0000000000000000001/"
                        + CursorTestSamples.SNAPSHOT
                        + ".ncs");
        assertThat(objectStore.lastPutOptions.ifAbsent()).isTrue();
        assertThat(objectStore.lastPutOptions.contentType())
                .isEqualTo("application/vnd.nereus.cursor-snapshot-v1");
        assertThat(objectStore.lastPutOptions.metadata()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "nereus-format", "NCS1",
                "nereus-object-type", "CURSOR_SNAPSHOT_OBJECT",
                "nereus-snapshot-id", CursorTestSamples.SNAPSHOT));
        assertThat(objectStore.putCalls).isEqualTo(1);
        assertThat(objectStore.headCalls).isEqualTo(2);
        assertThat(objectStore.readCalls).isEqualTo(1);

        store.close();
        assertThat(objectStore.closed).isFalse();
    }

    @Test
    void retriesIfAbsentCollisionWithFreshRandomIdAndNeverOverwritesBytes() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        DefaultCursorSnapshotStore first = store(
                objectStore, new ArrayDeque<>(java.util.List.of(CursorTestSamples.SNAPSHOT)));
        CursorSnapshotReference original = first.write(CursorTestSamples.request()).join();

        DefaultCursorSnapshotStore second = store(
                objectStore,
                new ArrayDeque<>(java.util.List.of(CursorTestSamples.SNAPSHOT, SECOND_SNAPSHOT)));
        CursorSnapshotReference retried = second.write(CursorTestSamples.request()).join();

        assertThat(retried.snapshotId()).isEqualTo(SECOND_SNAPSHOT);
        assertThat(retried.objectKey()).isNotEqualTo(original.objectKey());
        assertThat(objectStore.objects).hasSize(2);
        assertThat(objectStore.putCalls).isEqualTo(3);
    }

    @Test
    void stableWrongHeadMetadataFailsWithoutReadingOrPublishingState() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        DefaultCursorSnapshotStore store = store(
                objectStore, new ArrayDeque<>(java.util.List.of(CursorTestSamples.SNAPSHOT)));
        CursorSnapshotReference reference = store.write(CursorTestSamples.request()).join();
        objectStore.returnWrongMetadata = true;

        assertThatThrownBy(() -> store.read(reference, CursorTestSamples.identity()).join())
                .satisfies(error -> assertThat(rootCause(error))
                        .isInstanceOfSatisfying(NereusException.class, failure ->
                                assertThat(failure.code()).isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH)));
        assertThat(objectStore.readCalls).isZero();
    }

    @Test
    void closeIsIdempotentAndRejectsNewOperationsWithoutClosingSharedObjectStore() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        DefaultCursorSnapshotStore store = store(
                objectStore, new ArrayDeque<>(java.util.List.of(CursorTestSamples.SNAPSHOT)));
        store.close();
        store.close();
        assertThatThrownBy(() -> store.write(CursorTestSamples.request()).join())
                .satisfies(error -> assertThat(rootCause(error))
                        .isInstanceOfSatisfying(NereusException.class, failure ->
                                assertThat(failure.code()).isEqualTo(ErrorCode.STORAGE_CLOSED)));
        assertThat(objectStore.closed).isFalse();
    }

    private static DefaultCursorSnapshotStore store(
            RecordingObjectStore objectStore, ArrayDeque<String> ids) {
        return new DefaultCursorSnapshotStore(
                CursorTestSamples.CLUSTER,
                objectStore,
                CursorStorageConfig.defaults(),
                Duration.ofSeconds(10),
                CLOCK,
                ids::removeFirst,
                System::nanoTime);
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class RecordingObjectStore implements ObjectStore {
        private final Map<ObjectKey, StoredObject> objects = new HashMap<>();
        private PutObjectOptions lastPutOptions;
        private int putCalls;
        private int headCalls;
        private int readCalls;
        private boolean returnWrongMetadata;
        private boolean closed;

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key, ByteBuffer payload, PutObjectOptions options) {
            putCalls++;
            lastPutOptions = options;
            byte[] bytes = bytes(payload);
            Checksum checksum = Crc32cChecksums.checksum(bytes);
            if (!checksum.equals(options.expectedChecksum())) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "checksum mismatch"));
            }
            if (options.ifAbsent() && objects.containsKey(key)) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, false, "object already exists"));
            }
            objects.put(key, new StoredObject(bytes, checksum, options.metadata()));
            return CompletableFuture.completedFuture(
                    new PutObjectResult(key, bytes.length, checksum, checksum.value()));
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key, long offset, long length, RangeReadOptions options) {
            readCalls++;
            StoredObject stored = require(key);
            byte[] range = Arrays.copyOfRange(
                    stored.bytes(), Math.toIntExact(offset), Math.toIntExact(offset + length));
            Checksum checksum = Crc32cChecksums.checksum(range);
            if (options.expectedChecksum().isPresent()
                    && !options.expectedChecksum().orElseThrow().equals(checksum)) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "range checksum mismatch"));
            }
            return CompletableFuture.completedFuture(new RangeReadResult(
                    key, offset, length, ByteBuffer.wrap(range), Optional.of(checksum)));
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key, HeadObjectOptions options) {
            headCalls++;
            StoredObject stored = require(key);
            Map<String, String> metadata = returnWrongMetadata
                    ? Map.of("nereus-format", "wrong")
                    : stored.metadata();
            return CompletableFuture.completedFuture(new HeadObjectResult(
                    key, stored.bytes().length, stored.checksum(), metadata));
        }

        @Override
        public void close() {
            closed = true;
        }

        private StoredObject require(ObjectKey key) {
            StoredObject stored = objects.get(key);
            if (stored == null) {
                throw new NereusException(ErrorCode.OBJECT_NOT_FOUND, true, "missing object");
            }
            return stored;
        }

        private static byte[] bytes(ByteBuffer input) {
            ByteBuffer copy = input.asReadOnlyBuffer();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
    }

    private record StoredObject(byte[] bytes, Checksum checksum, Map<String, String> metadata) {
        private StoredObject {
            bytes = bytes.clone();
            metadata = Map.copyOf(metadata);
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }
}
