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

package com.nereusstream.objectstore.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFileObjectStoreGcSafetyTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    @TempDir
    Path root;

    @Test
    void inProgressPrivateUploadIsNeverVisibleToInventoryListing() {
        try (LocalFileObjectStore store = new LocalFileObjectStore(root)) {
            DeferredUpload upload = new DeferredUpload(new byte[] {1, 2, 3});
            CompletableFuture<?> put = store.putObject(
                    new ObjectKey("objects/final"), upload, options(upload.bytes));

            assertThat(store.listObjects(
                            new ObjectKeyPrefix("objects/"),
                            Optional.empty(),
                            new ListObjectsOptions(10, TIMEOUT)).join().objects())
                    .isEmpty();
            upload.emit();
            put.join();
            assertThat(store.listObjects(
                            new ObjectKeyPrefix("objects/"),
                            Optional.empty(),
                            new ListObjectsOptions(10, TIMEOUT)).join().objects())
                    .extracting(value -> value.key().value())
                    .containsExactly("objects/final");
        }
    }

    @Test
    void listAndDeleteNeverFollowSymlinkObjects(@TempDir Path outside) throws Exception {
        try (LocalFileObjectStore store = new LocalFileObjectStore(root)) {
            Path external = Files.write(outside.resolve("external"), new byte[] {9});
            Path link = root.resolve("objects/link");
            Files.createDirectories(link.getParent());
            createSymbolicLinkOrSkip(link, external);

            assertThat(store.listObjects(
                            new ObjectKeyPrefix("objects/"),
                            Optional.empty(),
                            new ListObjectsOptions(10, TIMEOUT)).join().objects())
                    .isEmpty();
            assertThatThrownBy(() -> store.deleteObject(
                            new ObjectKey("objects/link"),
                            new DeleteObjectOptions(
                                    1,
                                    Crc32cChecksums.checksum(new byte[] {9}),
                                    Optional.empty(),
                                    TIMEOUT)).join())
                    .satisfies(error -> assertThat(unwrap(error).code()).isEqualTo(ErrorCode.INVALID_ARGUMENT));
            assertThat(external).exists();
        }
    }

    @Test
    void reservedInternalNamespaceCannotBecomeAProductObject() {
        try (LocalFileObjectStore store = new LocalFileObjectStore(root)) {
            byte[] bytes = {1};
            assertThatThrownBy(() -> store.putObject(
                            new ObjectKey(".nereus-internal-v1/fake"),
                            ByteBuffer.wrap(bytes),
                            options(bytes)).join())
                    .satisfies(error -> assertThat(unwrap(error).code()).isEqualTo(ErrorCode.INVALID_ARGUMENT));
        }
    }

    private static PutObjectOptions options(byte[] bytes) {
        return new PutObjectOptions(
                "application/octet-stream",
                Crc32cChecksums.checksum(bytes),
                true,
                Map.of(),
                TIMEOUT);
    }

    private static NereusException unwrap(Throwable supplied) {
        Throwable failure = supplied;
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        assertThat(failure).isInstanceOf(NereusException.class);
        return (NereusException) failure;
    }

    private static void createSymbolicLinkOrSkip(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException failure) {
            assumeTrue(false, "symbolic links are unavailable: " + failure.getMessage());
        }
    }

    private static final class DeferredUpload implements ReplayableObjectUpload {
        private final byte[] bytes;
        private Flow.Subscriber<? super ByteBuffer> subscriber;
        private boolean requested;

        private DeferredUpload(byte[] bytes) {
            this.bytes = bytes.clone();
        }

        @Override
        public long contentLength() {
            return bytes.length;
        }

        @Override
        public Flow.Publisher<ByteBuffer> openPublisher() {
            return value -> {
                subscriber = value;
                value.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long count) {
                        requested = count > 0;
                    }

                    @Override
                    public void cancel() {
                    }
                });
            };
        }

        private void emit() {
            assertThat(requested).isTrue();
            subscriber.onNext(ByteBuffer.wrap(bytes).asReadOnlyBuffer());
            subscriber.onComplete();
        }

        @Override
        public void close() {
        }
    }
}
