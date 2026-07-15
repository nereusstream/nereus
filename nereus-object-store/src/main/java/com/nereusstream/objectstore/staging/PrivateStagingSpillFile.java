/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.staging;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/** Owner-only, checksum-verified temporary run file sharing the process staging-byte permit. */
public final class PrivateStagingSpillFile implements ManagedStagingFile {
    private enum State {
        OPEN,
        SEALED,
        CLOSED
    }

    private final StagingFileManager manager;
    private final Path path;
    private final FileChannel writer;
    private final MessageDigest sha256;
    private final SpillOutputStream output = new SpillOutputStream();
    private State state = State.OPEN;
    private boolean outputClaimed;
    private boolean outputClosed;
    private long writtenBytes;
    private Object sealedFileKey;
    private Checksum sealedSha256;
    private VerifiedInputStream activeReader;
    private RandomAccessReader activeRandomReader;

    PrivateStagingSpillFile(StagingFileManager manager, Path path) throws IOException {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.path = Objects.requireNonNull(path, "path");
        this.writer = FileChannel.open(path, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        this.sha256 = newDigest();
    }

    public synchronized OutputStream outputStream() {
        requireState(State.OPEN, "spill file is not writable");
        if (outputClaimed) {
            throw new IllegalStateException("spill output stream was already claimed");
        }
        outputClaimed = true;
        return output;
    }

    public synchronized PrivateStagingSpillFile seal() {
        requireState(State.OPEN, "spill file cannot be sealed");
        try {
            closeWriter();
            BasicFileAttributes attributes = attributes();
            if (!attributes.isRegularFile()
                    || attributes.isSymbolicLink()
                    || attributes.size() != writtenBytes
                    || attributes.fileKey() == null) {
                throw new IOException("spill file identity or length changed before seal");
            }
            sealedFileKey = attributes.fileKey();
            sealedSha256 = new Checksum(
                    ChecksumType.SHA256, HexFormat.of().formatHex(sha256.digest()));
            state = State.SEALED;
            return this;
        } catch (IOException | RuntimeException failure) {
            abort();
            if (failure instanceof NereusException nereus) {
                throw nereus;
            }
            throw new NereusException(
                    ErrorCode.OBJECT_UPLOAD_FAILED, false, "failed to seal private spill file", failure);
        }
    }

    public synchronized long sealedLength() {
        requireState(State.SEALED, "spill file is not sealed");
        return writtenBytes;
    }

    public synchronized Checksum contentSha256() {
        requireState(State.SEALED, "spill file is not sealed");
        return sealedSha256;
    }

    public synchronized InputStream openVerifiedInputStream() {
        requireState(State.SEALED, "spill file is not sealed");
        if (activeReader != null || activeRandomReader != null) {
            throw new IllegalStateException("spill file already has an active reader");
        }
        validateSealedFile();
        try {
            activeReader = new VerifiedInputStream(Files.newInputStream(
                    path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
            return activeReader;
        } catch (IOException failure) {
            throw new NereusException(
                    ErrorCode.OBJECT_READ_FAILED, true, "failed to open private spill file", failure);
        }
    }

    /** Opens one identity-checked random reader for fixed-width checksum-protected run records. */
    public synchronized RandomAccessReader openRandomAccessReader() {
        requireState(State.SEALED, "spill file is not sealed");
        if (activeReader != null || activeRandomReader != null) {
            throw new IllegalStateException("spill file already has an active reader");
        }
        validateSealedFile();
        try {
            activeRandomReader = new RandomAccessReader(
                    FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS),
                    writtenBytes);
            return activeRandomReader;
        } catch (IOException failure) {
            throw new NereusException(
                    ErrorCode.OBJECT_READ_FAILED, true, "failed to open private spill random reader", failure);
        }
    }

    @Override
    public void close() {
        VerifiedInputStream reader;
        RandomAccessReader randomReader;
        long release;
        synchronized (this) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSED;
            reader = activeReader;
            activeReader = null;
            randomReader = activeRandomReader;
            activeRandomReader = null;
            release = writtenBytes;
            try {
                writer.close();
            } catch (IOException ignored) {
            }
        }
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException ignored) {
            }
        }
        if (randomReader != null) {
            randomReader.closeFromOwner();
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
        requireState(State.OPEN, "spill file is not writable");
        if (outputClosed) {
            throw new IOException("spill output stream is closed");
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
            sha256.update(bytes, offset, length);
            writtenBytes = Math.addExact(writtenBytes, length);
            committed = true;
        } catch (IOException | RuntimeException failure) {
            abortWithReserved(length);
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
            BasicFileAttributes attributes = attributes();
            if (!attributes.isRegularFile()
                    || attributes.isSymbolicLink()
                    || attributes.size() != writtenBytes
                    || !sealedFileKey.equals(attributes.fileKey())) {
                throw new NereusException(
                        ErrorCode.OBJECT_READ_FAILED, false, "sealed spill file identity changed");
            }
        } catch (IOException failure) {
            throw new NereusException(
                    ErrorCode.OBJECT_READ_FAILED, false, "cannot revalidate sealed spill file", failure);
        }
    }

    private BasicFileAttributes attributes() throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    }

    private synchronized void readerClosed(VerifiedInputStream reader) {
        if (activeReader == reader) {
            activeReader = null;
        }
    }

    private synchronized void randomReaderClosed(RandomAccessReader reader) {
        if (activeRandomReader == reader) {
            activeRandomReader = null;
        }
    }

    private synchronized void abortWithReserved(long currentReservation) {
        if (state == State.CLOSED) {
            return;
        }
        long release = Math.addExact(writtenBytes, currentReservation);
        state = State.CLOSED;
        closeWriterQuietly();
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
        closeWriterQuietly();
        deleteQuietly();
        manager.release(release);
        manager.unregister(this);
    }

    private void closeWriterQuietly() {
        try {
            writer.close();
        } catch (IOException ignored) {
        }
    }

    private void deleteQuietly() {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
        }
    }

    private void requireState(State expected, String message) {
        if (state != expected) {
            throw new IllegalStateException(message);
        }
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private final class SpillOutputStream extends OutputStream {
        private final byte[] one = new byte[1];

        @Override
        public void write(int value) throws IOException {
            one[0] = (byte) value;
            write(one, 0, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            PrivateStagingSpillFile.this.write(bytes, offset, length);
        }

        @Override
        public void close() {
            synchronized (PrivateStagingSpillFile.this) {
                outputClosed = true;
            }
        }
    }

    private final class VerifiedInputStream extends FilterInputStream {
        private final MessageDigest digest = newDigest();
        private boolean verified;
        private boolean closed;

        private VerifiedInputStream(InputStream input) {
            super(input);
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) {
                digest.update((byte) value);
            } else {
                verifyDigest();
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = super.read(bytes, offset, length);
            if (read > 0) {
                digest.update(bytes, offset, read);
            } else if (read < 0) {
                verifyDigest();
            }
            return read;
        }

        private void verifyDigest() throws IOException {
            if (!verified) {
                verified = true;
                byte[] expected = HexFormat.of().parseHex(sealedSha256.value());
                if (!Arrays.equals(expected, digest.digest())) {
                    throw new IOException("private spill file SHA-256 mismatch");
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                super.close();
            } finally {
                readerClosed(this);
            }
        }
    }

    /** Single-owner bounded random reader. Each caller-owned record must carry its own checksum. */
    public final class RandomAccessReader implements AutoCloseable {
        private final FileChannel channel;
        private final long length;
        private boolean closed;

        private RandomAccessReader(FileChannel channel, long length) {
            this.channel = channel;
            this.length = length;
        }

        public synchronized ByteBuffer readRange(long offset, int length) {
            if (closed) {
                throw new IllegalStateException("spill random reader is closed");
            }
            if (offset < 0 || length < 0 || offset > this.length || length > this.length - offset) {
                throw new IllegalArgumentException("spill read range is outside the sealed file");
            }
            ByteBuffer result = ByteBuffer.allocate(length);
            try {
                while (result.hasRemaining()) {
                    int read = channel.read(result, offset + result.position());
                    if (read < 0) {
                        throw new IOException("unexpected EOF in sealed spill file");
                    }
                    if (read == 0) {
                        Thread.onSpinWait();
                    }
                }
                result.flip();
                return result.asReadOnlyBuffer();
            } catch (IOException failure) {
                throw new NereusException(
                        ErrorCode.OBJECT_READ_FAILED, true, "failed to read private spill range", failure);
            }
        }

        @Override
        public void close() {
            closeInternal(true);
        }

        private void closeFromOwner() {
            closeInternal(false);
        }

        private synchronized void closeInternal(boolean notifyOwner) {
            if (closed) {
                return;
            }
            closed = true;
            try {
                channel.close();
            } catch (IOException ignored) {
            }
            if (notifyOwner) {
                randomReaderClosed(this);
            }
        }
    }
}
