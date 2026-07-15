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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.objectstore.Crc32cChecksums;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32C;

/** Owner-only staging file whose sealed bytes are replayed without whole-object aggregation. */
public final class PrivateStagedObjectFile implements StagedObjectFile, ManagedStagingFile {
    private enum State {
        OPEN,
        SEALED,
        CLOSED
    }

    private final StagingFileManager manager;
    private final Path path;
    private final int uploadChunkBytes;
    private final Executor objectIoExecutor;
    private final FileChannel writer;
    private final CRC32C crc32c = new CRC32C();
    private final MessageDigest sha256;
    private final StagingOutputStream output = new StagingOutputStream();
    private final AtomicBoolean outputClaimed = new AtomicBoolean();
    private State state = State.OPEN;
    private long writtenBytes;
    private boolean outputClosed;
    private Object sealedFileKey;
    private long sealedLength;
    private Checksum sealedCrc32c;
    private Checksum sealedSha256;
    private AttemptPublisher activeAttempt;

    PrivateStagedObjectFile(
            StagingFileManager manager,
            Path path,
            int uploadChunkBytes,
            Executor objectIoExecutor) throws IOException {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.path = Objects.requireNonNull(path, "path");
        this.uploadChunkBytes = uploadChunkBytes;
        this.objectIoExecutor = Objects.requireNonNull(objectIoExecutor, "objectIoExecutor");
        this.writer = FileChannel.open(path, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        try {
            this.sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    /** Returns the single writer stream. Closing it finishes writes but does not release the staged file. */
    public OutputStream outputStream() {
        synchronized (this) {
            requireState(State.OPEN, "staging file is not writable");
            if (!outputClaimed.compareAndSet(false, true)) {
                throw new IllegalStateException("staging output stream was already claimed");
            }
            return output;
        }
    }

    /** Seals exact length/checksums/file identity and makes the file eligible for upload replay. */
    public synchronized PrivateStagedObjectFile seal() {
        requireState(State.OPEN, "staging file cannot be sealed");
        try {
            closeWriter();
            BasicFileAttributes attributes = Files.readAttributes(
                    path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile()
                    || attributes.isSymbolicLink()
                    || attributes.size() != writtenBytes
                    || attributes.fileKey() == null) {
                throw new IOException("staging file identity or length changed before seal");
            }
            sealedFileKey = attributes.fileKey();
            sealedLength = writtenBytes;
            sealedCrc32c = Crc32cChecksums.checksum((int) crc32c.getValue());
            sealedSha256 = new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(sha256.digest()));
            state = State.SEALED;
            return this;
        } catch (IOException | RuntimeException failure) {
            abort();
            if (failure instanceof NereusException nereus) {
                throw nereus;
            }
            throw new NereusException(
                    ErrorCode.OBJECT_UPLOAD_FAILED, false, "failed to seal private staging file", failure);
        }
    }

    @Override
    public synchronized long sealedLength() {
        requireState(State.SEALED, "staging file is not sealed");
        return sealedLength;
    }

    @Override
    public synchronized Checksum storageCrc32c() {
        requireState(State.SEALED, "staging file is not sealed");
        return sealedCrc32c;
    }

    @Override
    public synchronized Checksum contentSha256() {
        requireState(State.SEALED, "staging file is not sealed");
        return sealedSha256;
    }

    @Override
    public synchronized Flow.Publisher<ByteBuffer> openPublisher() {
        requireState(State.SEALED, "staging file is not sealed");
        if (activeAttempt != null) {
            throw new IllegalStateException("a staging upload attempt is already active");
        }
        validateSealedFile();
        AttemptPublisher publisher = new AttemptPublisher();
        activeAttempt = publisher;
        return publisher;
    }

    @Override
    public void close() {
        AttemptPublisher attempt;
        long release;
        synchronized (this) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSED;
            attempt = activeAttempt;
            activeAttempt = null;
            release = writtenBytes;
            try {
                writer.close();
            } catch (IOException ignored) {
                // Best-effort cleanup still releases the process-local budget.
            }
        }
        if (attempt != null) {
            attempt.cancelFromOwner();
        }
        deleteQuietly();
        manager.release(release);
        manager.unregister(this);
    }

    @Override
    public Path path() {
        return path;
    }

    private synchronized void write(byte[] bytes, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, bytes.length);
        requireState(State.OPEN, "staging file is not writable");
        if (outputClosed) {
            throw new IOException("staging output stream is closed");
        }
        if (length == 0) {
            return;
        }
        manager.reserve(length);
        boolean committed = false;
        try {
            ByteBuffer source = ByteBuffer.wrap(bytes, offset, length);
            while (source.hasRemaining()) {
                writer.write(source);
            }
            crc32c.update(bytes, offset, length);
            sha256.update(bytes, offset, length);
            writtenBytes = Math.addExact(writtenBytes, length);
            committed = true;
        } catch (RuntimeException | IOException failure) {
            abortWithReserved(length);
            if (failure instanceof IOException io) {
                throw io;
            }
            throw failure;
        } finally {
            if (!committed && state != State.CLOSED) {
                manager.release(length);
            }
        }
    }

    private synchronized void closeWriter() throws IOException {
        if (writer.isOpen()) {
            writer.force(true);
            writer.close();
        }
        outputClosed = true;
    }

    private synchronized void validateSealedFile() {
        try {
            BasicFileAttributes attributes = Files.readAttributes(
                    path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile()
                    || attributes.isSymbolicLink()
                    || attributes.size() != sealedLength
                    || !sealedFileKey.equals(attributes.fileKey())) {
                throw new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, false, "sealed staging file identity changed");
            }
        } catch (IOException failure) {
            throw new NereusException(
                    ErrorCode.OBJECT_UPLOAD_FAILED, false, "cannot revalidate sealed staging file", failure);
        }
    }

    private void attemptFinished(AttemptPublisher attempt) {
        synchronized (this) {
            if (activeAttempt == attempt) {
                activeAttempt = null;
            }
        }
    }

    private synchronized void abortWithReserved(long currentWriteReservation) {
        if (state == State.CLOSED) {
            return;
        }
        long release = Math.addExact(writtenBytes, currentWriteReservation);
        state = State.CLOSED;
        try {
            writer.close();
        } catch (IOException ignored) {
            // Best-effort cleanup.
        }
        deleteQuietly();
        manager.release(release);
        manager.unregister(this);
    }

    private synchronized void abort() {
        if (state == State.CLOSED) {
            return;
        }
        long release = writtenBytes;
        state = State.CLOSED;
        try {
            writer.close();
        } catch (IOException ignored) {
            // Best-effort cleanup.
        }
        deleteQuietly();
        manager.release(release);
        manager.unregister(this);
    }

    private void deleteQuietly() {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Startup orphan cleanup owns any residue after a process-local cleanup failure.
        }
    }

    private void requireState(State required, String message) {
        if (state != required) {
            throw new IllegalStateException(message);
        }
    }

    private final class StagingOutputStream extends OutputStream {
        @Override
        public void write(int value) throws IOException {
            byte[] one = {(byte) value};
            PrivateStagedObjectFile.this.write(one, 0, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            PrivateStagedObjectFile.this.write(bytes, offset, length);
        }

        @Override
        public void close() throws IOException {
            synchronized (PrivateStagedObjectFile.this) {
                if (state == State.OPEN && !outputClosed) {
                    writer.force(true);
                    outputClosed = true;
                }
            }
        }
    }

    private final class AttemptPublisher implements Flow.Publisher<ByteBuffer> {
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private volatile FileSubscription subscription;
        private volatile boolean cancelledBeforeSubscribe;

        @Override
        public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
            Objects.requireNonNull(subscriber, "subscriber");
            if (!subscribed.compareAndSet(false, true)) {
                subscriber.onSubscribe(EmptySubscription.INSTANCE);
                subscriber.onError(new IllegalStateException("one attempt publisher supports one subscriber"));
                return;
            }
            if (cancelledBeforeSubscribe) {
                subscriber.onSubscribe(EmptySubscription.INSTANCE);
                subscriber.onComplete();
                attemptFinished(this);
                return;
            }
            try {
                FileSubscription value = new FileSubscription(subscriber, FileChannel.open(
                        path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
                subscription = value;
                subscriber.onSubscribe(value);
            } catch (Throwable failure) {
                subscriber.onSubscribe(EmptySubscription.INSTANCE);
                subscriber.onError(failure);
                attemptFinished(this);
            }
        }

        private void cancelFromOwner() {
            cancelledBeforeSubscribe = true;
            FileSubscription value = subscription;
            if (value != null) {
                value.cancel();
            } else if (subscribed.compareAndSet(false, true)) {
                attemptFinished(this);
            }
        }

        private final class FileSubscription implements Flow.Subscription {
            private final Flow.Subscriber<? super ByteBuffer> downstream;
            private final FileChannel channel;
            private long demand;
            private long position;
            private boolean running;
            private boolean terminated;

            private FileSubscription(
                    Flow.Subscriber<? super ByteBuffer> downstream,
                    FileChannel channel) {
                this.downstream = downstream;
                this.channel = channel;
            }

            @Override
            public void request(long count) {
                if (count <= 0) {
                    fail(new IllegalArgumentException("publisher demand must be positive"));
                    return;
                }
                boolean schedule = false;
                synchronized (this) {
                    if (terminated) {
                        return;
                    }
                    demand = addCap(demand, count);
                    if (!running) {
                        running = true;
                        schedule = true;
                    }
                }
                if (schedule) {
                    scheduleDrain();
                }
            }

            @Override
            public void cancel() {
                terminate(false, null);
            }

            private void scheduleDrain() {
                try {
                    objectIoExecutor.execute(this::drain);
                } catch (RejectedExecutionException failure) {
                    fail(failure);
                }
            }

            private void drain() {
                while (true) {
                    int length;
                    synchronized (this) {
                        if (terminated) {
                            running = false;
                            return;
                        }
                        if (position == sealedLength) {
                            running = false;
                            terminate(true, null);
                            return;
                        }
                        if (demand == 0) {
                            running = false;
                            return;
                        }
                        demand--;
                        length = Math.toIntExact(Math.min(uploadChunkBytes, sealedLength - position));
                    }
                    ByteBuffer chunk = ByteBuffer.allocate(length);
                    try {
                        while (chunk.hasRemaining()) {
                            int read = channel.read(chunk, position + chunk.position());
                            if (read < 0) {
                                throw new IOException("unexpected EOF in sealed staging file");
                            }
                            if (read == 0) {
                                Thread.onSpinWait();
                            }
                        }
                        chunk.flip();
                        synchronized (this) {
                            position = Math.addExact(position, length);
                        }
                        downstream.onNext(chunk.asReadOnlyBuffer());
                    } catch (Throwable failure) {
                        fail(failure);
                        return;
                    }
                }
            }

            private void fail(Throwable failure) {
                terminate(true, Objects.requireNonNull(failure, "failure"));
            }

            private void terminate(boolean signal, Throwable failure) {
                synchronized (this) {
                    if (terminated) {
                        return;
                    }
                    terminated = true;
                }
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // Terminal cleanup.
                }
                attemptFinished(AttemptPublisher.this);
                if (signal) {
                    if (failure == null) {
                        downstream.onComplete();
                    } else {
                        downstream.onError(failure);
                    }
                }
            }
        }
    }

    private enum EmptySubscription implements Flow.Subscription {
        INSTANCE;

        @Override
        public void request(long count) {
        }

        @Override
        public void cancel() {
        }
    }

    private static long addCap(long current, long increment) {
        long result = current + increment;
        return result < 0 ? Long.MAX_VALUE : result;
    }
}
