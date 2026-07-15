/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.objectstore.staging.PrivateStagedObjectFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplayableObjectUploadContractTest {
    @TempDir
    Path temporary;

    @Test
    void byteBufferSourceIsCopiedReplayableAndCloseOwned() {
        ByteBuffer caller = ByteBuffer.wrap(new byte[] {9, 1, 2, 3, 8});
        caller.position(1).limit(4);
        ByteBufferObjectUpload upload = new ByteBufferObjectUpload(caller);
        caller.put(1, (byte) 99);

        assertThat(collect(upload, Integer.MAX_VALUE).join()).containsExactly(1, 2, 3);
        assertThat(collect(upload, Integer.MAX_VALUE).join()).containsExactly(1, 2, 3);
        assertThat(caller.position()).isEqualTo(1);

        upload.close();
        assertThatThrownBy(upload::openPublisher).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void stagedSourceIsChunkBoundedBackpressuredAndReplayable() throws Exception {
        Path directory = ownerOnlyDirectory("replay");
        byte[] expected = new byte[StagingFileManager.MIN_UPLOAD_CHUNK_BYTES * 2 + 17];
        for (int index = 0; index < expected.length; index++) {
            expected[index] = (byte) (index * 31);
        }
        try (StagingFileManager manager = new StagingFileManager(
                directory,
                expected.length * 2L,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run)) {
            PrivateStagedObjectFile staged = manager.create("contract");
            staged.outputStream().write(expected);
            staged.seal();

            assertThat(staged.sealedLength()).isEqualTo(expected.length);
            assertThat(staged.storageCrc32c()).isEqualTo(Crc32cChecksums.checksum(expected));
            assertThat(staged.contentSha256().value()).hasSize(64);

            DemandControlledSubscriber subscriber = new DemandControlledSubscriber();
            staged.openPublisher().subscribe(subscriber);
            subscriber.request(1);
            assertThat(subscriber.chunkCount()).isEqualTo(1);
            assertThat(subscriber.maximumChunk()).isEqualTo(StagingFileManager.MIN_UPLOAD_CHUNK_BYTES);
            assertThat(subscriber.completed()).isFalse();
            assertThatThrownBy(staged::openPublisher).isInstanceOf(IllegalStateException.class);
            subscriber.request(2);
            assertThat(subscriber.result().join()).containsExactly(expected);

            assertThat(collect(staged, Long.MAX_VALUE).join()).containsExactly(expected);
            staged.close();
            assertThat(manager.reservedBytes()).isZero();
            try (var paths = Files.list(directory)) {
                assertThat(paths.toList()).isEmpty();
            }
        }
    }

    private Path ownerOnlyDirectory(String name) throws Exception {
        return Files.createDirectory(
                temporary.resolve(name),
                java.nio.file.attribute.PosixFilePermissions.asFileAttribute(EnumSet.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE)));
    }

    private static CompletableFuture<byte[]> collect(ReplayableObjectUpload upload, long requestBatch) {
        DemandControlledSubscriber subscriber = new DemandControlledSubscriber();
        upload.openPublisher().subscribe(subscriber);
        subscriber.request(requestBatch);
        return subscriber.result();
    }

    private static final class DemandControlledSubscriber implements Flow.Subscriber<ByteBuffer> {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private Flow.Subscription subscription;
        private int chunkCount;
        private int maximumChunk;
        private boolean completed;

        @Override
        public void onSubscribe(Flow.Subscription value) {
            subscription = value;
        }

        @Override
        public void onNext(ByteBuffer item) {
            assertThat(item.isReadOnly()).isTrue();
            ByteBuffer duplicate = item.asReadOnlyBuffer();
            maximumChunk = Math.max(maximumChunk, duplicate.remaining());
            chunkCount++;
            byte[] chunk = new byte[duplicate.remaining()];
            duplicate.get(chunk);
            bytes.writeBytes(chunk);
        }

        @Override
        public void onError(Throwable failure) {
            result.completeExceptionally(failure);
        }

        @Override
        public void onComplete() {
            completed = true;
            result.complete(bytes.toByteArray());
        }

        private void request(long count) {
            subscription.request(count);
        }

        private int chunkCount() {
            return chunkCount;
        }

        private int maximumChunk() {
            return maximumChunk;
        }

        private boolean completed() {
            return completed;
        }

        private CompletableFuture<byte[]> result() {
            return result;
        }
    }
}
