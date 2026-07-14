/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.codec.MetadataCodecException;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared strict single-key CAS implementation used by fake and Java Oxia F3 adapters. */
final class CursorMetadataStoreCore implements CursorMetadataStore {
    private final PartitionedOxiaClient client;
    private final CursorMetadataStoreConfig config;
    private final ExecutorService operationExecutor;
    private final Semaphore admission;
    private final CopyOnWriteArrayList<WatchRegistration> watches = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    CursorMetadataStoreCore(PartitionedOxiaClient client, CursorMetadataStoreConfig config) {
        this.client = Objects.requireNonNull(client, "client");
        this.config = Objects.requireNonNull(config, "config");
        this.admission = new Semaphore(config.maxPendingOperations());
        this.operationExecutor = Executors.newFixedThreadPool(
                Math.min(4, config.maxPendingOperations()), namedThreadFactory());
    }

    @Override
    public CompletableFuture<Optional<VersionedCursorState>> getCursor(
            String cluster, StreamId streamId, String cursorName) {
        CursorKeyspace keyspace = new CursorKeyspace(cluster);
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        String exactName = CursorNames.requireCursorName(cursorName);
        return submit(deadline -> readCursor(keyspace, exactStream, exactName, deadline));
    }

    @Override
    public CompletableFuture<VersionedCursorState> createCursor(
            String cluster, CursorStateRecord value) {
        CursorKeyspace keyspace = new CursorKeyspace(cluster);
        CursorStateRecord candidate = Objects.requireNonNull(value, "value");
        StreamId streamId = streamId(candidate.projection().streamId());
        return submit(deadline -> {
            String key = keyspace.cursorStateKey(streamId, candidate.cursorName());
            long version = putIfAbsent(
                    key, keyspace.streamPartitionKey(streamId), candidate, CursorStateRecord.class, deadline);
            return new VersionedCursorState(candidate, version);
        });
    }

    @Override
    public CompletableFuture<VersionedCursorState> compareAndSetCursor(
            String cluster, CursorStateRecord value, long expectedMetadataVersion) {
        CursorKeyspace keyspace = new CursorKeyspace(cluster);
        CursorStateRecord candidate = Objects.requireNonNull(value, "value");
        requireVersion(expectedMetadataVersion);
        StreamId streamId = streamId(candidate.projection().streamId());
        return submit(deadline -> {
            String key = keyspace.cursorStateKey(streamId, candidate.cursorName());
            long version = putIfVersion(
                    key,
                    keyspace.streamPartitionKey(streamId),
                    expectedMetadataVersion,
                    candidate,
                    CursorStateRecord.class,
                    deadline);
            return new VersionedCursorState(candidate, version);
        });
    }

    @Override
    public CompletableFuture<CursorScanPage> scanCursors(
            String cluster,
            StreamId streamId,
            Optional<CursorScanToken> continuation,
            int pageSize) {
        CursorKeyspace keyspace = new CursorKeyspace(cluster);
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        Optional<CursorScanToken> token = Objects.requireNonNull(continuation, "continuation");
        if (pageSize <= 0 || pageSize > config.maxScanPageSize()) {
            throw new IllegalArgumentException("cursor pageSize is outside the configured bound");
        }
        String prefix = keyspace.cursorStateScanFrom(exactStream);
        String from = token.map(value -> {
            if (!value.matches(new OxiaKeyspace(cluster).cluster(), exactStream, prefix)) {
                throw new IllegalArgumentException("cursor scan continuation belongs to another scope");
            }
            return value.exclusiveLastKey() + '\0';
        }).orElse(prefix);
        String to = keyspace.cursorStateScanToExclusive(exactStream);
        if (from.compareTo(prefix) < 0 || from.compareTo(to) >= 0) {
            throw new IllegalArgumentException("cursor scan continuation is outside the cursor-state prefix");
        }
        return submit(deadline -> {
            List<PartitionedOxiaClient.VersionedValue> stored = deadline.await(client.rangeScan(
                    from, to, pageSize, keyspace.streamPartitionKey(exactStream)));
            List<VersionedCursorState> records = new ArrayList<>(stored.size());
            String previous = null;
            for (PartitionedOxiaClient.VersionedValue item : stored) {
                if (previous != null && item.key().compareTo(previous) <= 0) {
                    throw invariant("cursor scan keys are not strictly increasing");
                }
                CursorStateRecord decoded = decode(item, CursorStateRecord.class);
                requireCursorIdentity(keyspace, exactStream, item.key(), decoded);
                records.add(new VersionedCursorState(decoded, item.version()));
                previous = item.key();
            }
            Optional<CursorScanToken> next = stored.size() == pageSize
                    ? Optional.of(new CursorScanToken(
                            new OxiaKeyspace(cluster).cluster(), exactStream, prefix, stored.get(stored.size() - 1).key()))
                    : Optional.empty();
            return new CursorScanPage(records, next);
        });
    }

    @Override
    public CompletableFuture<Optional<VersionedCursorRetention>> getRetention(
            String cluster, StreamId streamId) {
        CursorKeyspace keyspace = new CursorKeyspace(cluster);
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        return submit(deadline -> readRetention(keyspace, exactStream, deadline));
    }

    @Override
    public CompletableFuture<VersionedCursorRetention> createRetention(
            String cluster, CursorRetentionRecord value) {
        CursorKeyspace keyspace = new CursorKeyspace(cluster);
        CursorRetentionRecord candidate = Objects.requireNonNull(value, "value");
        StreamId streamId = streamId(candidate.projection().streamId());
        return submit(deadline -> {
            long version = putIfAbsent(
                    keyspace.retentionKey(streamId),
                    keyspace.streamPartitionKey(streamId),
                    candidate,
                    CursorRetentionRecord.class,
                    deadline);
            return new VersionedCursorRetention(candidate, version);
        });
    }

    @Override
    public CompletableFuture<VersionedCursorRetention> compareAndSetRetention(
            String cluster, CursorRetentionRecord value, long expectedMetadataVersion) {
        CursorKeyspace keyspace = new CursorKeyspace(cluster);
        CursorRetentionRecord candidate = Objects.requireNonNull(value, "value");
        requireVersion(expectedMetadataVersion);
        StreamId streamId = streamId(candidate.projection().streamId());
        return submit(deadline -> {
            long version = putIfVersion(
                    keyspace.retentionKey(streamId),
                    keyspace.streamPartitionKey(streamId),
                    expectedMetadataVersion,
                    candidate,
                    CursorRetentionRecord.class,
                    deadline);
            return new VersionedCursorRetention(candidate, version);
        });
    }

    @Override
    public WatchRegistration watchStreamCursors(
            String cluster, StreamId streamId, Runnable invalidation) {
        ensureOpen();
        CursorKeyspace keyspace = new CursorKeyspace(cluster);
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(invalidation, "invalidation");
        WatchRegistration delegate = client.watchPrefix(
                keyspace.cursorWatchPrefix(exactStream),
                keyspace.streamPartitionKey(exactStream),
                invalidation);
        AtomicBoolean active = new AtomicBoolean(true);
        WatchRegistration registration = () -> {
            if (active.compareAndSet(true, false)) {
                delegate.close();
            }
        };
        watches.add(registration);
        if (closed.get()) {
            registration.close();
            watches.remove(registration);
            throw new NereusException(ErrorCode.STORAGE_CLOSED, false, "cursor metadata store is closed");
        }
        return () -> {
            registration.close();
            watches.remove(registration);
        };
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            watches.forEach(WatchRegistration::close);
            watches.clear();
            operationExecutor.shutdownNow();
        }
    }

    private Optional<VersionedCursorState> readCursor(
            CursorKeyspace keyspace, StreamId streamId, String cursorName, Deadline deadline) {
        String key = keyspace.cursorStateKey(streamId, cursorName);
        Optional<PartitionedOxiaClient.VersionedValue> stored = deadline.await(
                client.get(key, keyspace.streamPartitionKey(streamId)));
        if (stored.isEmpty()) {
            return Optional.empty();
        }
        PartitionedOxiaClient.VersionedValue item = stored.orElseThrow();
        CursorStateRecord decoded = decode(item, CursorStateRecord.class);
        requireCursorIdentity(keyspace, streamId, key, decoded);
        if (!decoded.cursorName().equals(cursorName)) {
            throw invariant("cursor name hash collision or exact-name mismatch");
        }
        return Optional.of(new VersionedCursorState(decoded, item.version()));
    }

    private Optional<VersionedCursorRetention> readRetention(
            CursorKeyspace keyspace, StreamId streamId, Deadline deadline) {
        String key = keyspace.retentionKey(streamId);
        Optional<PartitionedOxiaClient.VersionedValue> stored = deadline.await(
                client.get(key, keyspace.streamPartitionKey(streamId)));
        if (stored.isEmpty()) {
            return Optional.empty();
        }
        PartitionedOxiaClient.VersionedValue item = stored.orElseThrow();
        CursorRetentionRecord decoded = decode(item, CursorRetentionRecord.class);
        if (!decoded.projection().streamId().equals(streamId.value()) || !item.key().equals(key)) {
            throw invariant("retention key/record stream identity mismatch");
        }
        return Optional.of(new VersionedCursorRetention(decoded, item.version()));
    }

    private void requireCursorIdentity(
            CursorKeyspace keyspace, StreamId streamId, String key, CursorStateRecord decoded) {
        if (!decoded.projection().streamId().equals(streamId.value())
                || !key.equals(keyspace.cursorStateKey(streamId, decoded.cursorName()))) {
            throw invariant("cursor key/record identity mismatch");
        }
    }

    private <T> long putIfAbsent(
            String key,
            PartitionKey partitionKey,
            T candidate,
            Class<T> recordClass,
            Deadline deadline) {
        byte[] encoded = encode(candidate, recordClass);
        try {
            return deadline.await(client.putIfAbsent(key, encoded, partitionKey)).version();
        } catch (RuntimeException e) {
            if (isConditionCause(unwrap(e))) {
                throw new CursorMetadataConditionFailedException("cursor metadata key already exists", unwrap(e));
            }
            throw e;
        }
    }

    private <T> long putIfVersion(
            String key,
            PartitionKey partitionKey,
            long expectedVersion,
            T candidate,
            Class<T> recordClass,
            Deadline deadline) {
        byte[] encoded = encode(candidate, recordClass);
        try {
            return deadline.await(client.putIfVersion(key, encoded, expectedVersion, partitionKey)).version();
        } catch (RuntimeException e) {
            if (isConditionCause(unwrap(e))) {
                throw new CursorMetadataConditionFailedException("cursor metadata version condition failed", unwrap(e));
            }
            throw e;
        }
    }

    private <T> byte[] encode(T candidate, Class<T> recordClass) {
        byte[] encoded = MetadataRecordCodecFactory.encodeEnvelope(candidate, recordClass);
        if (encoded.length > config.maxValueBytes()) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "encoded F3 metadata exceeds maxValueBytes");
        }
        return encoded;
    }

    private static <T> T decode(PartitionedOxiaClient.VersionedValue stored, Class<T> recordClass) {
        try {
            return MetadataRecordCodecFactory.decodeEnvelope(stored.value(), recordClass);
        } catch (MetadataCodecException | IllegalArgumentException e) {
            throw invariant("invalid durable F3 metadata record: " + recordClass.getSimpleName(), e);
        }
    }

    private <T> CompletableFuture<T> submit(DeadlineOperation<T> operation) {
        if (closed.get()) {
            return NereusException.failedFuture(ErrorCode.STORAGE_CLOSED, false, "cursor metadata store is closed");
        }
        if (!admission.tryAcquire()) {
            return NereusException.failedFuture(
                    ErrorCode.BACKPRESSURE_REJECTED, true, "cursor metadata operation bound is exhausted");
        }
        Deadline deadline = Deadline.start(config.operationTimeout());
        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    ensureOpen();
                    return operation.call(deadline);
                } catch (Throwable error) {
                    throw normalize(error);
                } finally {
                    admission.release();
                }
            }, operationExecutor);
        } catch (RejectedExecutionException e) {
            admission.release();
            return NereusException.failedFuture(
                    closed.get() ? ErrorCode.STORAGE_CLOSED : ErrorCode.BACKPRESSURE_REJECTED,
                    !closed.get(),
                    closed.get() ? "cursor metadata store is closing" : "cursor metadata queue rejected work",
                    e);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new NereusException(ErrorCode.STORAGE_CLOSED, false, "cursor metadata store is closed");
        }
    }

    private static RuntimeException normalize(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof CursorMetadataConditionFailedException condition) {
            return condition;
        }
        if (cause instanceof NereusException nereus) {
            return nereus;
        }
        if (cause instanceof MetadataCodecException || cause instanceof IllegalArgumentException) {
            return invariant("invalid durable F3 metadata", cause);
        }
        if (isConditionCause(cause)) {
            return new CursorMetadataConditionFailedException("cursor metadata condition failed", cause);
        }
        return new NereusException(
                ErrorCode.METADATA_UNAVAILABLE, true, "cursor metadata operation failed", cause);
    }

    private static boolean isConditionCause(Throwable cause) {
        return cause instanceof CursorMetadataConditionFailedException
                || cause instanceof KeyAlreadyExistsException
                || cause instanceof UnexpectedVersionIdException;
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static StreamId streamId(String value) {
        return new StreamId(value);
    }

    private static void requireVersion(long version) {
        if (version < 0) {
            throw new IllegalArgumentException("expected metadata version must be non-negative");
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static ThreadFactory namedThreadFactory() {
        AtomicInteger ids = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, "nereus-f3-metadata-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    @FunctionalInterface
    private interface DeadlineOperation<T> {
        T call(Deadline deadline) throws Exception;
    }

    private static final class Deadline {
        private final long deadlineNanos;

        private Deadline(long deadlineNanos) {
            this.deadlineNanos = deadlineNanos;
        }

        static Deadline start(java.time.Duration timeout) {
            long timeoutNanos;
            try {
                timeoutNanos = timeout.toNanos();
            } catch (ArithmeticException ignored) {
                timeoutNanos = Long.MAX_VALUE;
            }
            long now = System.nanoTime();
            long deadline;
            try {
                deadline = Math.addExact(now, timeoutNanos);
            } catch (ArithmeticException ignored) {
                deadline = Long.MAX_VALUE;
            }
            return new Deadline(deadline);
        }

        <T> T await(CompletableFuture<T> future) {
            try {
                return future.get(remainingNanos(), TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                throw timeout(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NereusException(
                        ErrorCode.CANCELLED, true, "cursor metadata operation was interrupted", e);
            } catch (ExecutionException e) {
                throw new CompletionException(e.getCause());
            }
        }

        private long remainingNanos() {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw timeout(null);
            }
            return remaining;
        }

        private static NereusException timeout(Throwable cause) {
            return new NereusException(
                    ErrorCode.TIMEOUT, true, "cursor metadata operation deadline expired", cause);
        }
    }
}
