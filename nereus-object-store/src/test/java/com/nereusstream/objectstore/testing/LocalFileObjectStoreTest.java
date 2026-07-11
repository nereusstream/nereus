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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.RangeReadOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFileObjectStoreTest {
    @TempDir
    Path root;

    @Test
    void putHeadAndRangeReadImmutableBytes() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        ObjectKey key = new ObjectKey("cluster/wal/object.nrs");
        byte[] payload = "abcdef".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        store.putObject(key, ByteBuffer.wrap(payload), putOptions(payload)).join();

        assertThat(store.headObject(key, new HeadObjectOptions(Duration.ofSeconds(1))).join().objectLength())
                .isEqualTo(payload.length);
        ByteBuffer range = store.readRange(
                        key,
                        2,
                        3,
                        new RangeReadOptions(Optional.of(Crc32cChecksums.checksum("cde".getBytes(java.nio.charset.StandardCharsets.UTF_8))), Duration.ofSeconds(1)))
                .join()
                .payload();
        byte[] read = new byte[range.remaining()];
        range.get(read);
        assertThat(read).isEqualTo("cde".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertCode(() -> store.readRange(
                        key,
                        0,
                        payload.length + 1L,
                        new RangeReadOptions(Optional.empty(), Duration.ofSeconds(1)))
                .join(), ErrorCode.OBJECT_READ_FAILED);
    }

    @Test
    void rejectsUnsafeLocalKeysBeforeWrite() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        byte[] payload = "x".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        assertCode(() -> store.putObject(new ObjectKey("/absolute"), ByteBuffer.wrap(payload), putOptions(payload)).join(),
                ErrorCode.INVALID_ARGUMENT);
        assertCode(() -> store.putObject(new ObjectKey("a/../b"), ByteBuffer.wrap(payload), putOptions(payload)).join(),
                ErrorCode.INVALID_ARGUMENT);
        assertCode(() -> store.putObject(new ObjectKey("a//b"), ByteBuffer.wrap(payload), putOptions(payload)).join(),
                ErrorCode.INVALID_ARGUMENT);
        assertCode(() -> store.putObject(new ObjectKey("C:/b"), ByteBuffer.wrap(payload), putOptions(payload)).join(),
                ErrorCode.INVALID_ARGUMENT);
        assertThat(Files.exists(root.resolve("absolute"))).isFalse();
    }

    @Test
    void checksumFailureDoesNotExposeFinalObject() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        ObjectKey key = new ObjectKey("cluster/wal/object.nrs");
        byte[] payload = "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        PutObjectOptions badOptions = new PutObjectOptions(
                "application/octet-stream",
                new Checksum(ChecksumType.CRC32C, "00000000"),
                true,
                Map.of(),
                Duration.ofSeconds(1));

        assertCode(() -> store.putObject(key, ByteBuffer.wrap(payload), badOptions).join(),
                ErrorCode.OBJECT_CHECKSUM_MISMATCH);

        assertThat(Files.exists(root.resolve("cluster/wal/object.nrs"))).isFalse();
    }

    @Test
    void rejectsFinalSymlinkEscapeForReadHeadAndPut(@TempDir Path outsideRoot) throws Exception {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        Path outside = outsideRoot.resolve("outside-object");
        Files.writeString(outside, "secret");
        Path link = root.resolve("cluster/wal/object.nrs");
        Files.createDirectories(link.getParent());
        createSymbolicLinkOrSkip(link, outside);
        ObjectKey key = new ObjectKey("cluster/wal/object.nrs");
        byte[] payload = "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        assertCode(() -> store.readRange(
                        key,
                        0,
                        1,
                        new RangeReadOptions(Optional.empty(), Duration.ofSeconds(1)))
                .join(), ErrorCode.INVALID_ARGUMENT);
        assertCode(() -> store.headObject(key, new HeadObjectOptions(Duration.ofSeconds(1))).join(),
                ErrorCode.INVALID_ARGUMENT);
        assertCode(() -> store.putObject(key, ByteBuffer.wrap(payload), putOptions(payload)).join(),
                ErrorCode.INVALID_ARGUMENT);
    }

    @Test
    void rejectsSymlinkParentEscape(@TempDir Path outsideRoot) throws Exception {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        Path outsideDirectory = outsideRoot.resolve("outside-dir");
        Files.createDirectories(outsideDirectory);
        Path link = root.resolve("cluster");
        createSymbolicLinkOrSkip(link, outsideDirectory);
        ObjectKey key = new ObjectKey("cluster/wal/object.nrs");
        byte[] payload = "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        assertCode(() -> store.putObject(key, ByteBuffer.wrap(payload), putOptions(payload)).join(),
                ErrorCode.INVALID_ARGUMENT);
        assertCode(() -> store.readRange(
                        key,
                        0,
                        1,
                        new RangeReadOptions(Optional.empty(), Duration.ofSeconds(1)))
                .join(), ErrorCode.INVALID_ARGUMENT);
        assertCode(() -> store.headObject(key, new HeadObjectOptions(Duration.ofSeconds(1))).join(),
                ErrorCode.INVALID_ARGUMENT);
    }

    @Test
    void duplicateIfAbsentAndCleanupAreTestOnlyLocalBehaviors() throws Exception {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        ObjectKey key = new ObjectKey("cluster/wal/object.nrs");
        byte[] payload = "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        store.putObject(key, ByteBuffer.wrap(payload), putOptions(payload)).join();
        assertCode(() -> store.putObject(key, ByteBuffer.wrap(payload), putOptions(payload)).join(),
                ErrorCode.OBJECT_UPLOAD_FAILED);
        assertThat(Files.exists(root.resolve("cluster/wal/object.nrs"))).isTrue();

        store.deleteAllForTesting();

        try (var remaining = Files.list(root)) {
            assertThat(remaining).isEmpty();
        }
    }

    private PutObjectOptions putOptions(byte[] payload) {
        return new PutObjectOptions(
                "application/octet-stream",
                Crc32cChecksums.checksum(payload),
                true,
                Map.of(),
                Duration.ofSeconds(1));
    }

    private void assertCode(Runnable runnable, ErrorCode code) {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOfSatisfying(NereusException.class, exception ->
                        assertThat(exception.code()).isEqualTo(code));
    }

    private void createSymbolicLinkOrSkip(Path link, Path target) throws IOException {
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException e) {
            assumeTrue(false, "symbolic links are not available: " + e.getMessage());
        }
    }
}
