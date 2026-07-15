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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ListObjectsResult;
import com.nereusstream.objectstore.ListedObject;
import com.nereusstream.objectstore.ObjectAlreadyExistsException;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectAttemptGuard;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/** Test-fixture object store backed by a caller-supplied local directory. */
public final class LocalFileObjectStore implements ObjectStore {
    private final Path root;
    private volatile boolean closed;

    public LocalFileObjectStore(Path root) {
        Objects.requireNonNull(root, "root");
        try {
            Files.createDirectories(root);
            this.root = root.toRealPath();
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot initialize local object store root", e);
        }
    }

    @Override
    public CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ReplayableObjectUpload source,
            PutObjectOptions options) {
        return putObject(key, source, options, (ignored, attempt) -> CompletableFuture.completedFuture(null));
    }

    @Override
    public CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ReplayableObjectUpload source,
            PutObjectOptions options,
            PutObjectAttemptGuard attemptGuard) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(attemptGuard, "attemptGuard");
        if (source.contentLength() < 0 || source.contentLength() > Integer.MAX_VALUE) {
            return CompletableFuture.failedFuture(failure(
                    ErrorCode.INVALID_ARGUMENT, false, "local upload length is outside the supported bound"));
        }
        return attemptGuard.authorize(key, 1)
                .thenCompose(ignored -> collect(source))
                .thenCompose(bytes -> complete(() -> {
            ensureOpen();
            Path target = resolveKey(key);
            rejectExistingSymlink(target);
            if (bytes.length != source.contentLength()) {
                throw failure(ErrorCode.OBJECT_UPLOAD_FAILED, false, "upload source length mismatch");
            }
            Checksum actual = Crc32cChecksums.checksum(bytes);
            if (!actual.equals(options.expectedChecksum())) {
                throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "putObject checksum mismatch");
            }
            Files.createDirectories(target.getParent());
            rejectExistingSymlinkParent(target.getParent());
            Path temporary = target.resolveSibling(target.getFileName() + ".tmp-" + UUID.randomUUID());
            try {
                writeTemporary(temporary, bytes);
                if (options.ifAbsent() && Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                    throw new ObjectAlreadyExistsException("object already exists");
                }
                try {
                    Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(temporary, target);
                }
            } catch (NereusException e) {
                deleteQuietly(temporary);
                throw e;
            } catch (IOException e) {
                deleteQuietly(temporary);
                throw failure(ErrorCode.OBJECT_UPLOAD_FAILED, true, "local object upload failed", e);
            }
            return new PutObjectResult(
                    key,
                    bytes.length,
                    actual,
                    actual.value());
        }));
    }

    @Override
    public CompletableFuture<RangeReadResult> readRange(
            ObjectKey key,
            long offset,
            long length,
            RangeReadOptions options) {
        Objects.requireNonNull(options, "options");
        return complete(() -> {
            ensureOpen();
            if (offset < 0 || length < 0) {
                throw failure(ErrorCode.INVALID_ARGUMENT, false, "range offset and length must be non-negative");
            }
            Path target = resolveKey(key);
            rejectExistingSymlink(target);
            if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(ErrorCode.OBJECT_NOT_FOUND, true, "object not found");
            }
            long size = Files.size(target);
            if (offset > size || length > size - offset) {
                throw failure(ErrorCode.OBJECT_READ_FAILED, false, "range read exceeds object length");
            }
            byte[] bytes = new byte[Math.toIntExact(length)];
            try (FileChannel channel = FileChannel.open(target, StandardOpenOption.READ)) {
                ByteBuffer destination = ByteBuffer.wrap(bytes);
                channel.position(offset);
                while (destination.hasRemaining()) {
                    if (channel.read(destination) < 0) {
                        throw failure(ErrorCode.OBJECT_READ_FAILED, false, "unexpected EOF");
                    }
                }
            } catch (IOException e) {
                throw failure(ErrorCode.OBJECT_READ_FAILED, true, "local object read failed", e);
            }
            Checksum actual = Crc32cChecksums.checksum(bytes);
            if (options.expectedChecksum().isPresent() && !actual.equals(options.expectedChecksum().get())) {
                throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "range checksum mismatch");
            }
            return new RangeReadResult(
                    key,
                    offset,
                    length,
                    ByteBuffer.wrap(bytes).asReadOnlyBuffer(),
                    Optional.of(actual));
        });
    }

    @Override
    public CompletableFuture<HeadObjectResult> headObject(
            ObjectKey key,
            HeadObjectOptions options) {
        Objects.requireNonNull(options, "options");
        return complete(() -> {
            ensureOpen();
            Path target = resolveKey(key);
            rejectExistingSymlink(target);
            if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                throw failure(ErrorCode.OBJECT_NOT_FOUND, true, "object not found");
            }
            byte[] bytes = Files.readAllBytes(target);
            Checksum checksum = Crc32cChecksums.checksum(bytes);
            return new HeadObjectResult(
                    key,
                    bytes.length,
                    checksum,
                    Optional.of(checksum.value()),
                    Map.of());
        });
    }

    @Override
    public CompletableFuture<ListObjectsResult> listObjects(
            ObjectKeyPrefix prefix,
            Optional<String> continuationToken,
            ListObjectsOptions options) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(continuationToken, "continuationToken");
        Objects.requireNonNull(options, "options");
        return complete(() -> {
            ensureOpen();
            String after = continuationToken.orElse("");
            List<ListedObject> all = new ArrayList<>();
            try (var paths = Files.walk(root)) {
                paths.filter(Files::isRegularFile)
                        .filter(path -> !Files.isSymbolicLink(path))
                        .map(path -> new PathAndKey(path, root.relativize(path).toString().replace('\\', '/')))
                        .filter(value -> value.key().startsWith(prefix.value()))
                        .filter(value -> after.isEmpty() || value.key().compareTo(after) > 0)
                        .sorted(Comparator.comparing(PathAndKey::key))
                        .limit(options.maxKeys())
                        .forEach(value -> {
                            try {
                                byte[] bytes = Files.readAllBytes(value.path());
                                Checksum checksum = Crc32cChecksums.checksum(bytes);
                                all.add(new ListedObject(
                                        new ObjectKey(value.key()),
                                        bytes.length,
                                        Optional.of(checksum.value()),
                                        Optional.of(Files.getLastModifiedTime(value.path()).toInstant())));
                            } catch (IOException failure) {
                                throw new java.io.UncheckedIOException(failure);
                            }
                        });
            } catch (java.io.UncheckedIOException | IOException error) {
                throw failure(ErrorCode.OBJECT_READ_FAILED, true, "local object list failed", error);
            }
            boolean more;
            if (all.size() < options.maxKeys()) {
                more = false;
            } else {
                String last = all.get(all.size() - 1).key().value();
                try (var paths = Files.walk(root)) {
                    more = paths.filter(Files::isRegularFile)
                            .map(path -> root.relativize(path).toString().replace('\\', '/'))
                            .anyMatch(key -> key.startsWith(prefix.value()) && key.compareTo(last) > 0);
                }
            }
            Optional<String> next = more
                    ? Optional.of(all.get(all.size() - 1).key().value()) : Optional.empty();
            if (next.equals(continuationToken)) {
                throw failure(ErrorCode.OBJECT_READ_FAILED, false, "local list repeated a continuation token");
            }
            return new ListObjectsResult(prefix, all, next);
        });
    }

    @Override
    public CompletableFuture<DeleteObjectResult> deleteObject(
            ObjectKey key,
            DeleteObjectOptions options) {
        Objects.requireNonNull(options, "options");
        return complete(() -> {
            ensureOpen();
            Path target = resolveKey(key);
            rejectExistingSymlink(target);
            if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                return new DeleteObjectResult(key, DeleteObjectResult.Status.ALREADY_ABSENT);
            }
            byte[] bytes = Files.readAllBytes(target);
            Checksum checksum = Crc32cChecksums.checksum(bytes);
            if (bytes.length != options.expectedLength()
                    || !checksum.equals(options.expectedStorageChecksum())
                    || options.expectedEtag().filter(value -> !value.equals(checksum.value())).isPresent()) {
                throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "delete identity mismatch");
            }
            Files.delete(target);
            return new DeleteObjectResult(key, DeleteObjectResult.Status.DELETED);
        });
    }

    public void deleteAllForTesting() {
        try {
            if (!Files.exists(root)) {
                return;
            }
            try (var stream = Files.walk(root)) {
                stream.sorted(Comparator.reverseOrder())
                        .filter(path -> !path.equals(root))
                        .forEach(LocalFileObjectStore::deleteQuietly);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to clean local object store root", e);
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    private Path resolveKey(ObjectKey key) {
        Objects.requireNonNull(key, "key");
        String value = key.value();
        if (value.startsWith("/") || value.startsWith("\\") || value.contains("\\")) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "object key must be relative and use '/' separators");
        }
        String[] segments = value.split("/", -1);
        Path current = root;
        for (String segment : segments) {
            validateSegment(segment);
            Path next = current.resolve(segment).normalize();
            if (!next.startsWith(root)) {
                throw failure(ErrorCode.INVALID_ARGUMENT, false, "object key escapes local root");
            }
            rejectExistingSymlinkParent(current);
            current = next;
        }
        return current;
    }

    private void validateSegment(String segment) {
        if (segment.isEmpty() || segment.equals(".") || segment.equals("..") || segment.contains(":")) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "object key contains unsafe segment");
        }
    }

    private void rejectExistingSymlinkParent(Path directory) {
        if (Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
            rejectExistingSymlink(directory);
        }
    }

    private void rejectExistingSymlink(Path path) {
        if (Files.isSymbolicLink(path)) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "object key traverses symlink path");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw failure(ErrorCode.STORAGE_CLOSED, false, "object store is closed");
        }
    }

    private CompletableFuture<byte[]> collect(ReplayableObjectUpload source) {
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(Math.toIntExact(source.contentLength()));
        try {
            source.openPublisher().subscribe(new Flow.Subscriber<>() {
                private Flow.Subscription subscription;
                private long received;

                @Override
                public void onSubscribe(Flow.Subscription value) {
                    if (subscription != null) {
                        value.cancel();
                        return;
                    }
                    subscription = value;
                    value.request(1);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    if (result.isDone()) {
                        subscription.cancel();
                        return;
                    }
                    ByteBuffer duplicate = Objects.requireNonNull(item, "item").asReadOnlyBuffer();
                    int length = duplicate.remaining();
                    received = Math.addExact(received, length);
                    if (received > source.contentLength()) {
                        subscription.cancel();
                        result.completeExceptionally(failure(
                                ErrorCode.OBJECT_UPLOAD_FAILED, false, "upload publisher exceeded declared length"));
                        return;
                    }
                    byte[] chunk = new byte[length];
                    duplicate.get(chunk);
                    bytes.writeBytes(chunk);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable throwable) {
                    result.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    if (received != source.contentLength()) {
                        result.completeExceptionally(failure(
                                ErrorCode.OBJECT_UPLOAD_FAILED, false, "upload publisher ended before declared length"));
                    } else {
                        result.complete(bytes.toByteArray());
                    }
                }
            });
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
        }
        return result;
    }

    private void writeTemporary(Path temporary, byte[] bytes) throws IOException {
        try (FileChannel channel = FileChannel.open(
                temporary,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(true);
        }
    }

    private <T> CompletableFuture<T> complete(ThrowingSupplier<T> supplier) {
        try {
            return CompletableFuture.completedFuture(supplier.get());
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message) {
        return new NereusException(code, retriable, message);
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message, Throwable cause) {
        return new NereusException(code, retriable, message, cause);
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // best-effort test cleanup
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private record PathAndKey(Path path, String key) {
    }
}
