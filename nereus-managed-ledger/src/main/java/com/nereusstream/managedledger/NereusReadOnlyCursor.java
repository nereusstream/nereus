/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.managedledger.errors.ManagedLedgerErrorMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;

/** Serialized broker-local read-only cursor over dense F2 offsets. */
public final class NereusReadOnlyCursor implements ReadOnlyCursor {
    private final NereusReadOnlyManagedLedger ledger;
    private final ManagedLedgerErrorMapper errorMapper = new ManagedLedgerErrorMapper();
    private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
    private Position readPosition;
    private boolean closed;

    NereusReadOnlyCursor(NereusReadOnlyManagedLedger ledger, Position readPosition) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.readPosition = Objects.requireNonNull(readPosition, "readPosition");
    }

    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead)
            throws InterruptedException, ManagedLedgerException {
        CompletableFuture<List<Entry>> result = new CompletableFuture<>();
        asyncReadEntries(numberOfEntriesToRead, Long.MAX_VALUE, new ReadEntriesCallback() {
            @Override public void readEntriesComplete(List<Entry> entries, Object ctx) { result.complete(entries); }
            @Override public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null, null);
        try {
            return result.get();
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof ManagedLedgerException managedLedger) {
                throw managedLedger;
            }
            throw new ManagedLedgerException(cause);
        }
    }

    @Override
    public void asyncReadEntries(
            int numberOfEntriesToRead,
            ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition) {
        asyncReadEntries(numberOfEntriesToRead, Long.MAX_VALUE, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntries(
            int numberOfEntriesToRead,
            long maxSizeBytes,
            ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition) {
        Objects.requireNonNull(callback, "callback");
        if (numberOfEntriesToRead <= 0 || maxSizeBytes <= 0) {
            callback.readEntriesFailed(new ManagedLedgerException(
                    "read limits must be positive"), ctx);
            return;
        }
        int maxEntries = Math.min(numberOfEntriesToRead, ledger.runtime().config().maxReadEntries());
        enqueue(() -> readBatch(maxEntries, maxSizeBytes, maxPosition))
                .whenCompleteAsync((entries, error) -> {
                    if (error == null) {
                        callback.readEntriesComplete(entries, ctx);
                    } else {
                        callback.readEntriesFailed(map(error), ctx);
                    }
                }, ledger.runtime().callbackExecutor());
    }

    @Override
    public synchronized Position getReadPosition() {
        return readPosition;
    }

    @Override
    public synchronized boolean hasMoreEntries() {
        return !closed && readPosition.getEntryId() < ledger.currentMetadata().committedEndOffset();
    }

    @Override
    public synchronized long getNumberOfEntries() {
        return Math.max(0, ledger.currentMetadata().committedEndOffset() - readPosition.getEntryId());
    }

    @Override
    public synchronized void skipEntries(int numEntriesToSkip) {
        if (numEntriesToSkip < 0) {
            throw new IllegalArgumentException("numEntriesToSkip cannot be negative");
        }
        StreamMetadata metadata = ledger.currentMetadata();
        long target;
        try {
            target = Math.addExact(readPosition.getEntryId(), numEntriesToSkip);
        } catch (ArithmeticException ignored) {
            target = Long.MAX_VALUE;
        }
        readPosition = ledger.readPosition(Math.min(target, metadata.committedEndOffset()), metadata);
    }

    @Override
    public Position findNewestMatching(
            ManagedCursor.FindPositionConstraint constraint,
            Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
        Objects.requireNonNull(constraint, "constraint");
        Objects.requireNonNull(condition, "condition");
        StreamMetadata metadata = ledger.refreshMetadata().join();
        long offset = metadata.committedEndOffset() - 1;
        int budget = ledger.runtime().config().maxScanEntries();
        while (offset >= metadata.trimOffset() && budget-- > 0) {
            Entry entry = ledger.readAt(offset, metadata).join();
            try {
                if (condition.test(entry)) {
                    return entry.getPosition();
                }
            } finally {
                entry.release();
            }
            offset--;
        }
        if (offset >= metadata.trimOffset()) {
            throw new ManagedLedgerException("Nereus cursor scan budget is exhausted");
        }
        return null;
    }

    @Override
    public long getNumberOfEntries(Range<Position> range) {
        Objects.requireNonNull(range, "range");
        StreamMetadata metadata = ledger.currentMetadata();
        long lower = metadata.trimOffset();
        long upper = metadata.committedEndOffset();
        if (range.hasLowerBound()) {
            lower = range.lowerEndpoint().getEntryId()
                    + (range.lowerBoundType() == BoundType.OPEN ? 1 : 0);
        }
        if (range.hasUpperBound()) {
            upper = range.upperEndpoint().getEntryId()
                    + (range.upperBoundType() == BoundType.CLOSED ? 1 : 0);
        }
        return Math.max(0, Math.min(upper, metadata.committedEndOffset())
                - Math.max(lower, metadata.trimOffset()));
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    @Override
    public void asyncClose(CloseCallback callback, Object ctx) {
        Objects.requireNonNull(callback, "callback");
        enqueue(() -> {
            close();
            return CompletableFuture.completedFuture(null);
        }).whenCompleteAsync((ignored, error) -> {
            if (error == null) {
                callback.closeComplete(ctx);
            } else {
                callback.closeFailed(map(error), ctx);
            }
        }, ledger.runtime().callbackExecutor());
    }

    private synchronized <T> CompletableFuture<T> enqueue(
            java.util.function.Supplier<CompletableFuture<T>> operation) {
        if (closed) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "read-only cursor is closed"));
        }
        CompletableFuture<T> result = tail.thenCompose(ignored -> operation.get());
        tail = result.handle((ignored, error) -> null);
        return result;
    }

    private CompletableFuture<List<Entry>> readBatch(
            int maxEntries, long maxSizeBytes, Position maxPosition) {
        return ledger.refreshMetadata().thenCompose(metadata -> {
            Position inclusiveMax = ledger.normalizeInclusiveMax(maxPosition, metadata);
            long start;
            synchronized (this) {
                start = readPosition.getEntryId();
            }
            long endExclusive = Math.min(
                    metadata.committedEndOffset(), inclusiveMax.getEntryId() + 1);
            List<Entry> entries = new ArrayList<>();
            return readNext(start, endExclusive, maxEntries, maxSizeBytes, 0, metadata, entries)
                    .thenApply(next -> {
                        synchronized (this) {
                            readPosition = ledger.readPosition(next, metadata);
                        }
                        return List.copyOf(entries);
                    });
        });
    }

    private CompletableFuture<Long> readNext(
            long offset,
            long endExclusive,
            int remaining,
            long maxBytes,
            long usedBytes,
            StreamMetadata metadata,
            List<Entry> entries) {
        if (remaining == 0 || offset >= endExclusive) {
            return CompletableFuture.completedFuture(offset);
        }
        return ledger.readAt(offset, metadata).thenCompose(entry -> {
            long nextBytes = usedBytes + entry.getLength();
            if (!entries.isEmpty() && nextBytes > maxBytes) {
                entry.release();
                return CompletableFuture.completedFuture(offset);
            }
            entries.add(entry);
            return readNext(offset + 1, endExclusive, remaining - 1, maxBytes,
                    nextBytes, metadata, entries);
        }).exceptionallyCompose(error -> {
            entries.forEach(Entry::release);
            entries.clear();
            return CompletableFuture.failedFuture(unwrap(error));
        });
    }

    private ManagedLedgerException map(Throwable error) {
        Throwable cause = unwrap(error);
        return cause instanceof ManagedLedgerException managedLedger
                ? managedLedger
                : new ManagedLedgerException("Nereus read-only cursor operation failed", cause);
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
