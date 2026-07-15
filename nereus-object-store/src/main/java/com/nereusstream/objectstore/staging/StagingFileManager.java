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

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/** Process-shared owner of private staging files and their global byte budget. */
public final class StagingFileManager implements AutoCloseable {
    public static final int MIN_UPLOAD_CHUNK_BYTES = 64 * 1024;
    public static final int MAX_UPLOAD_CHUNK_BYTES = 8 * 1024 * 1024;
    static final String FILE_PREFIX = "nereus-staging-v1-";
    private static final Pattern VALID_FILE_NAME = Pattern.compile(
            "nereus-staging-v1-[a-z0-9](?:[a-z0-9-]{0,31})-[0-9a-f]{32}\\.tmp");
    private static final Set<PosixFilePermission> DIRECTORY_PERMISSIONS = EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE);
    private static final Set<PosixFilePermission> FILE_PERMISSIONS = EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE);

    private final Path directory;
    private final long maxStagingBytes;
    private final int uploadChunkBytes;
    private final Duration orphanGrace;
    private final Executor objectIoExecutor;
    private final Clock clock;
    private final Set<ManagedStagingFile> openFiles = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private long reservedBytes;

    public StagingFileManager(
            Path directory,
            long maxStagingBytes,
            int uploadChunkBytes,
            Duration orphanGrace,
            Executor objectIoExecutor) {
        this(directory, maxStagingBytes, uploadChunkBytes, orphanGrace, objectIoExecutor, Clock.systemUTC());
    }

    StagingFileManager(
            Path directory,
            long maxStagingBytes,
            int uploadChunkBytes,
            Duration orphanGrace,
            Executor objectIoExecutor,
            Clock clock) {
        Objects.requireNonNull(directory, "directory");
        if (!directory.isAbsolute()) {
            throw new IllegalArgumentException("staging directory must be absolute");
        }
        if (maxStagingBytes <= 0) {
            throw new IllegalArgumentException("maxStagingBytes must be positive");
        }
        if (uploadChunkBytes < MIN_UPLOAD_CHUNK_BYTES || uploadChunkBytes > MAX_UPLOAD_CHUNK_BYTES) {
            throw new IllegalArgumentException("uploadChunkBytes must be in [64 KiB, 8 MiB]");
        }
        this.orphanGrace = requirePositiveMillis(orphanGrace, "orphanGrace");
        this.objectIoExecutor = Objects.requireNonNull(objectIoExecutor, "objectIoExecutor");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.maxStagingBytes = maxStagingBytes;
        this.uploadChunkBytes = uploadChunkBytes;
        this.directory = initializeDirectory(directory);
        cleanupOrphans();
    }

    public PrivateStagedObjectFile create(String purpose) {
        String canonicalPurpose = requirePurpose(purpose);
        ensureOpen();
        for (int attempt = 0; attempt < 16; attempt++) {
            String name = FILE_PREFIX + canonicalPurpose + "-"
                    + UUID.randomUUID().toString().replace("-", "") + ".tmp";
            Path path = directory.resolve(name);
            try {
                Files.createFile(path, PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS));
                PrivateStagedObjectFile staged;
                try {
                    staged = new PrivateStagedObjectFile(this, path, uploadChunkBytes, objectIoExecutor);
                } catch (IOException | RuntimeException | Error failure) {
                    Files.deleteIfExists(path);
                    throw failure;
                }
                openFiles.add(staged);
                if (closed.get()) {
                    staged.close();
                    throw closedFailure();
                }
                return staged;
            } catch (java.nio.file.FileAlreadyExistsException collision) {
                // Retry with a fresh 128-bit name.
            } catch (IOException failure) {
                throw new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, true, "failed to create private staging file", failure);
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Error failure) {
                throw failure;
            }
        }
        throw new NereusException(
                ErrorCode.OBJECT_UPLOAD_FAILED, false, "failed to allocate a unique private staging file");
    }

    /** Creates an owner-only temporary file that shares the global staging-byte budget. */
    public PrivateStagingSpillFile createSpill(String purpose) {
        String canonicalPurpose = requirePurpose(purpose);
        ensureOpen();
        for (int attempt = 0; attempt < 16; attempt++) {
            String name = FILE_PREFIX + canonicalPurpose + "-"
                    + UUID.randomUUID().toString().replace("-", "") + ".tmp";
            Path path = directory.resolve(name);
            try {
                Files.createFile(path, PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS));
                PrivateStagingSpillFile staged;
                try {
                    staged = new PrivateStagingSpillFile(this, path);
                } catch (IOException | RuntimeException | Error failure) {
                    Files.deleteIfExists(path);
                    throw failure;
                }
                openFiles.add(staged);
                if (closed.get()) {
                    staged.close();
                    throw closedFailure();
                }
                return staged;
            } catch (java.nio.file.FileAlreadyExistsException collision) {
                // Retry with a fresh 128-bit name.
            } catch (IOException failure) {
                throw new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, true, "failed to create private spill file", failure);
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Error failure) {
                throw failure;
            }
        }
        throw new NereusException(
                ErrorCode.OBJECT_UPLOAD_FAILED, false, "failed to allocate a unique private spill file");
    }

    /** Deletes only closed-process product files older than the configured grace. */
    public int cleanupOrphans() {
        ensureOpen();
        Instant cutoff;
        try {
            cutoff = clock.instant().minus(orphanGrace);
        } catch (DateTimeException failure) {
            throw new IllegalArgumentException("orphanGrace overflows the cleanup clock", failure);
        }
        Set<Path> active = new HashSet<>();
        for (ManagedStagingFile file : openFiles) {
            active.add(file.path());
        }
        int deleted = 0;
        try (var entries = Files.list(directory)) {
            for (Path path : entries.toList()) {
                String name = path.getFileName().toString();
                if (!VALID_FILE_NAME.matcher(name).matches()
                        || active.contains(path)
                        || Files.isSymbolicLink(path)
                        || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    continue;
                }
                Instant modified = Files.getLastModifiedTime(path, LinkOption.NOFOLLOW_LINKS).toInstant();
                if (!modified.isAfter(cutoff) && Files.deleteIfExists(path)) {
                    deleted++;
                }
            }
        } catch (IOException failure) {
            throw new NereusException(
                    ErrorCode.OBJECT_UPLOAD_FAILED, true, "failed to clean orphan staging files", failure);
        }
        return deleted;
    }

    public synchronized long reservedBytes() {
        return reservedBytes;
    }

    public long maxStagingBytes() {
        return maxStagingBytes;
    }

    public int uploadChunkBytes() {
        return uploadChunkBytes;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (ManagedStagingFile file : Set.copyOf(openFiles)) {
            file.close();
        }
    }

    synchronized void reserve(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("reserved staging bytes must be non-negative");
        }
        ensureOpen();
        if (bytes > maxStagingBytes - reservedBytes) {
            throw new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED, true, "global staging byte budget is exhausted");
        }
        reservedBytes = Math.addExact(reservedBytes, bytes);
    }

    synchronized void release(long bytes) {
        if (bytes < 0 || bytes > reservedBytes) {
            throw new IllegalStateException("invalid staging byte release");
        }
        reservedBytes -= bytes;
    }

    void unregister(ManagedStagingFile file) {
        openFiles.remove(file);
    }

    private Path initializeDirectory(Path requested) {
        Path normalized = requested.toAbsolutePath().normalize();
        try {
            if (Files.exists(normalized, LinkOption.NOFOLLOW_LINKS) && Files.isSymbolicLink(normalized)) {
                throw new IllegalArgumentException("staging directory cannot be a symbolic link");
            }
            if (!Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
                Files.createDirectories(normalized);
                Files.setPosixFilePermissions(normalized, DIRECTORY_PERMISSIONS);
            }
            Path real = normalized.toRealPath(LinkOption.NOFOLLOW_LINKS);
            if (!Files.isDirectory(real, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalArgumentException("staging path is not a directory");
            }
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(real, LinkOption.NOFOLLOW_LINKS);
            if (!permissions.equals(DIRECTORY_PERMISSIONS)) {
                throw new IllegalArgumentException("staging directory permissions must be owner-only 0700");
            }
            return real;
        } catch (UnsupportedOperationException failure) {
            throw new IllegalArgumentException("staging directory requires POSIX owner-only permissions", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("cannot initialize staging directory", failure);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw closedFailure();
        }
    }

    private static NereusException closedFailure() {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, "staging file manager is closed");
    }

    private static String requirePurpose(String value) {
        Objects.requireNonNull(value, "purpose");
        if (!value.matches("[a-z0-9](?:[a-z0-9-]{0,31})")) {
            throw new IllegalArgumentException("staging purpose is not canonical");
        }
        return value;
    }

    private static Duration requirePositiveMillis(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }
}
