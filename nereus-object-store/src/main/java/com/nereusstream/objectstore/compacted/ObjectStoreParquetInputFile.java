/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/** Seekable Parquet input backed by bounded exact ObjectStore range reads. */
final class ObjectStoreParquetInputFile implements InputFile {
    private static final int MAX_SINGLE_RANGE_BYTES = 8 << 20;

    private final ObjectStore objectStore;
    private final ObjectKey objectKey;
    private final long length;
    private final ReadDeadline deadline;
    private final ReadBudget budget;

    ObjectStoreParquetInputFile(
            ObjectStore objectStore,
            ObjectKey objectKey,
            long length,
            ReadDeadline deadline,
            ReadBudget budget) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.objectKey = Objects.requireNonNull(objectKey, "objectKey");
        if (length <= 0) {
            throw new IllegalArgumentException("Parquet object length must be positive");
        }
        this.length = length;
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        this.budget = Objects.requireNonNull(budget, "budget");
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new RangeStream();
    }

    private final class RangeStream extends SeekableInputStream {
        private final AtomicBoolean closed = new AtomicBoolean();
        private long position;

        @Override
        public long getPos() throws IOException {
            ensureOpen();
            return position;
        }

        @Override
        public void seek(long value) throws IOException {
            ensureOpen();
            if (value < 0 || value > length) {
                throw new IOException("Parquet seek is outside immutable object");
            }
            position = value;
        }

        @Override
        public int read() throws IOException {
            ensureOpen();
            if (position == length) {
                return -1;
            }
            byte[] one = new byte[1];
            readFully(one);
            return one[0] & 0xff;
        }

        @Override
        public int read(byte[] target, int offset, int requested) throws IOException {
            ensureOpen();
            Objects.checkFromIndexSize(offset, requested, target.length);
            if (requested == 0) {
                return 0;
            }
            if (position == length) {
                return -1;
            }
            int count = Math.toIntExact(Math.min(
                    Math.min((long) requested, length - position),
                    MAX_SINGLE_RANGE_BYTES));
            ByteBuffer payload = readRange(position, count);
            payload.get(target, offset, count);
            position += count;
            return count;
        }

        @Override
        public void readFully(byte[] target) throws IOException {
            readFully(target, 0, target.length);
        }

        @Override
        public void readFully(byte[] target, int offset, int requested) throws IOException {
            ensureOpen();
            Objects.checkFromIndexSize(offset, requested, target.length);
            int written = 0;
            while (written < requested) {
                int count = read(target, offset + written, requested - written);
                if (count < 0) {
                    throw new EOFException("truncated Parquet object range");
                }
                written += count;
            }
        }

        @Override
        public int read(ByteBuffer target) throws IOException {
            ensureOpen();
            if (!target.hasRemaining()) {
                return 0;
            }
            if (position == length) {
                return -1;
            }
            int count = Math.toIntExact(Math.min(
                    Math.min((long) target.remaining(), length - position),
                    MAX_SINGLE_RANGE_BYTES));
            target.put(readRange(position, count));
            position += count;
            return count;
        }

        @Override
        public void readFully(ByteBuffer target) throws IOException {
            ensureOpen();
            while (target.hasRemaining()) {
                int count = read(target);
                if (count < 0) {
                    throw new EOFException("truncated Parquet object range");
                }
            }
        }

        @Override
        public void close() {
            closed.set(true);
        }

        private void ensureOpen() throws IOException {
            if (closed.get()) {
                throw new IOException("Parquet range stream is closed");
            }
        }
    }

    private ByteBuffer readRange(long offset, int count) throws IOException {
        budget.reserve(count);
        try {
            RangeReadResult read = objectStore.readRange(
                            objectKey,
                            offset,
                            count,
                            new RangeReadOptions(Optional.empty(), deadline.remaining()))
                    .join();
            if (!read.key().equals(objectKey)
                    || read.offset() != offset
                    || read.length() != count
                    || read.payload().remaining() != count) {
                throw new CompactedObjectFormatException(
                        "object store returned a mismatched Parquet range");
            }
            return read.payload().asReadOnlyBuffer();
        } catch (CompletionException failure) {
            Throwable cause = unwrap(failure);
            if (cause instanceof NereusException nereus) {
                throw new IOException(nereus.getMessage(), nereus);
            }
            throw new IOException("Parquet object range read failed", cause);
        } catch (RuntimeException failure) {
            throw new IOException("Parquet object range read failed", failure);
        }
    }

    static final class ReadBudget {
        private final long maximum;
        private long used;

        ReadBudget(long maximum) {
            if (maximum <= 0) {
                throw new IllegalArgumentException("read budget must be positive");
            }
            this.maximum = maximum;
        }

        synchronized void reserve(long bytes) {
            if (bytes < 0 || bytes > maximum - used) {
                throw new NereusException(
                        ErrorCode.OBJECT_READ_FAILED,
                        false,
                        "Parquet reader exceeded its immutable-object read budget");
            }
            used += bytes;
        }

        synchronized long used() {
            return used;
        }
    }

    static final class ReadDeadline {
        private final long deadlineNanos;

        ReadDeadline(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            long now = System.nanoTime();
            long duration;
            try {
                duration = timeout.toNanos();
            } catch (ArithmeticException failure) {
                duration = Long.MAX_VALUE;
            }
            long candidate;
            try {
                candidate = Math.addExact(now, duration);
            } catch (ArithmeticException failure) {
                candidate = Long.MAX_VALUE;
            }
            this.deadlineNanos = candidate;
        }

        Duration remaining() {
            long value = deadlineNanos - System.nanoTime();
            if (value <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT, true, "compacted object read deadline expired");
            }
            return Duration.ofNanos(value);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
