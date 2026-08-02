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

package com.nereusstream.objectstore.staging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StagingFileManagerTest {
    private static final Instant NOW = Instant.parse("2026-07-15T00:00:00Z");

    @TempDir
    Path temporary;

    @Test
    void globalBudgetCoversOpenAndSealedFilesAndReleasesExactlyOnce() throws Exception {
        Path directory = ownerOnlyDirectory("budget");
        try (StagingFileManager manager = manager(directory, 4)) {
            PrivateStagedObjectFile first = manager.create("first");
            first.outputStream().write(new byte[] {1, 2, 3, 4});
            first.seal();
            assertThat(manager.reservedBytes()).isEqualTo(4);

            PrivateStagedObjectFile second = manager.create("second");
            OutputStream secondOutput = second.outputStream();
            assertThatThrownBy(() -> secondOutput.write(5))
                    .isInstanceOfSatisfying(NereusException.class, error -> assertThat(error.code())
                            .isEqualTo(ErrorCode.BACKPRESSURE_REJECTED));
            first.close();
            assertThat(manager.reservedBytes()).isZero();

            secondOutput.write(new byte[] {5, 6});
            secondOutput.close();
            second.seal();
            assertThat(manager.reservedBytes()).isEqualTo(2);
            second.close();
            second.close();
            assertThat(manager.reservedBytes()).isZero();
        }
    }

    @Test
    void startupCleanupDeletesOnlyValidOldProductFiles() throws Exception {
        Path directory = ownerOnlyDirectory("cleanup");
        Path oldProduct = Files.write(
                directory.resolve("nereus-staging-v1-compacted-0123456789abcdef0123456789abcdef.tmp"), new byte[] {1});
        Path youngProduct = Files.write(
                directory.resolve("nereus-staging-v1-checkpoint-fedcba9876543210fedcba9876543210.tmp"), new byte[] {2});
        Path unrelated = Files.write(directory.resolve("do-not-delete.tmp"), new byte[] {3});
        Files.setLastModifiedTime(oldProduct, FileTime.from(NOW.minus(Duration.ofHours(2))));
        Files.setLastModifiedTime(youngProduct, FileTime.from(NOW.minus(Duration.ofMinutes(1))));
        Files.setLastModifiedTime(unrelated, FileTime.from(NOW.minus(Duration.ofDays(2))));

        try (StagingFileManager manager = manager(directory, 1024)) {
            assertThat(oldProduct).doesNotExist();
            assertThat(youngProduct).exists();
            assertThat(unrelated).exists();
            assertThat(manager.cleanupOrphans()).isZero();
        }
    }

    @Test
    void sealRejectsFileIdentityReplacementBeforeReplay() throws Exception {
        Path directory = ownerOnlyDirectory("identity");
        try (StagingFileManager manager = manager(directory, 1024)) {
            PrivateStagedObjectFile staged = manager.create("identity");
            staged.outputStream().write(new byte[] {1, 2, 3});
            staged.seal();
            Path path = staged.path();
            replacePath(directory, path, new byte[] {1, 2, 3});

            assertThatThrownBy(staged::openPublisher)
                    .isInstanceOfSatisfying(NereusException.class, error -> assertThat(error.code())
                            .isEqualTo(ErrorCode.OBJECT_UPLOAD_FAILED));
            staged.close();
            assertThat(manager.reservedBytes()).isZero();
            assertThat(Files.readAllBytes(path)).containsExactly(1, 2, 3);
        }
    }

    @Test
    void replayUsesSealedFileWhenPathIsReplacedBeforeSubscription() throws Exception {
        Path directory = ownerOnlyDirectory("subscribe-race");
        byte[] sealedBytes = {1, 2, 3};
        byte[] replacementBytes = {9, 8, 7};
        try (StagingFileManager manager = manager(directory, 1024)) {
            PrivateStagedObjectFile staged = manager.create("subscribe-race");
            staged.outputStream().write(sealedBytes);
            staged.seal();
            Path path = staged.path();
            Flow.Publisher<ByteBuffer> publisher = staged.openPublisher();

            replacePath(directory, path, replacementBytes);

            assertThat(collect(publisher).join()).containsExactly(sealedBytes);
            assertThat(Files.readAllBytes(path)).containsExactly(replacementBytes);
            staged.close();
            assertThat(manager.reservedBytes()).isZero();
            assertThat(Files.readAllBytes(path)).containsExactly(replacementBytes);
        }
    }

    @Test
    void sealRejectsSpillFileIdentityReplacementBeforeRead() throws Exception {
        Path directory = ownerOnlyDirectory("spill-identity");
        try (StagingFileManager manager = manager(directory, 1024)) {
            PrivateStagingSpillFile spill = manager.createSpill("spill-identity");
            spill.outputStream().write(new byte[] {1, 2, 3});
            spill.seal();
            Path path = spill.path();
            replacePath(directory, path, new byte[] {1, 2, 3});

            assertThatThrownBy(spill::openVerifiedInputStream)
                    .isInstanceOfSatisfying(NereusException.class, error -> assertThat(error.code())
                            .isEqualTo(ErrorCode.OBJECT_READ_FAILED));
            assertThatThrownBy(spill::openRandomAccessReader)
                    .isInstanceOfSatisfying(NereusException.class, error -> assertThat(error.code())
                            .isEqualTo(ErrorCode.OBJECT_READ_FAILED));
            spill.close();
            assertThat(manager.reservedBytes()).isZero();
            assertThat(Files.readAllBytes(path)).containsExactly(1, 2, 3);
        }
    }

    @Test
    void spillReadersUseSealedFilesAfterPathReplacement() throws Exception {
        Path directory = ownerOnlyDirectory("spill-read");
        try (StagingFileManager manager = manager(directory, 1024)) {
            PrivateStagingSpillFile sequential = manager.createSpill("sequential");
            sequential.outputStream().write(new byte[] {1, 2, 3});
            sequential.seal();
            InputStream input = sequential.openVerifiedInputStream();
            Path sequentialPath = sequential.path();
            replacePath(directory, sequentialPath, new byte[] {9, 8, 7});
            assertThat(input.readAllBytes()).containsExactly(1, 2, 3);
            input.close();
            sequential.close();
            assertThat(Files.readAllBytes(sequentialPath)).containsExactly(9, 8, 7);

            PrivateStagingSpillFile random = manager.createSpill("random");
            random.outputStream().write(new byte[] {4, 5, 6, 7});
            random.seal();
            PrivateStagingSpillFile.RandomAccessReader reader = random.openRandomAccessReader();
            Path randomPath = random.path();
            replacePath(directory, randomPath, new byte[] {0, 0, 0, 0});
            assertThat(reader.readRange(1, 2)).isEqualTo(ByteBuffer.wrap(new byte[] {5, 6}));
            reader.close();
            random.close();
            assertThat(Files.readAllBytes(randomPath)).containsExactly(0, 0, 0, 0);
            assertThat(manager.reservedBytes()).isZero();
        }
    }

    @Test
    void rejectsRelativeOrNonOwnerOnlyDirectoryAndInvalidChunkBounds() throws Exception {
        assertThatThrownBy(() -> new StagingFileManager(
                        Path.of("relative"),
                        1,
                        StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                        Duration.ofSeconds(1),
                        Runnable::run))
                .isInstanceOf(IllegalArgumentException.class);

        Path permissive = Files.createDirectory(temporary.resolve("permissive"));
        Files.setPosixFilePermissions(permissive, PosixFilePermissions.fromString("rwxr-x---"));
        assertThatThrownBy(() -> manager(permissive, 1)).isInstanceOf(IllegalArgumentException.class);

        Path ownerOnly = ownerOnlyDirectory("chunk");
        assertThatThrownBy(() -> new StagingFileManager(
                        ownerOnly,
                        1,
                        StagingFileManager.MIN_UPLOAD_CHUNK_BYTES - 1,
                        Duration.ofSeconds(1),
                        Runnable::run))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private StagingFileManager manager(Path directory, long maximumBytes) {
        return new StagingFileManager(
                directory,
                maximumBytes,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private Path ownerOnlyDirectory(String name) throws Exception {
        return Files.createDirectory(
                temporary.resolve(name),
                PosixFilePermissions.asFileAttribute(EnumSet.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE)));
    }

    private static void replacePath(Path directory, Path path, byte[] replacementBytes) throws Exception {
        Path replacement = Files.write(directory.resolve("replacement.tmp"), replacementBytes);
        Files.move(replacement, path, StandardCopyOption.REPLACE_EXISTING);
    }

    private static CompletableFuture<byte[]> collect(Flow.Publisher<ByteBuffer> publisher) {
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                ByteBuffer value = item.asReadOnlyBuffer();
                byte[] chunk = new byte[value.remaining()];
                value.get(chunk);
                bytes.writeBytes(chunk);
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
        return result;
    }
}
