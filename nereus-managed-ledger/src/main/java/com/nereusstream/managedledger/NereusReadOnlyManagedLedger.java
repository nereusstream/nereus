/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.managedledger.entry.PulsarEntryCodec;
import com.nereusstream.managedledger.errors.ManagedLedgerErrorMapper;
import com.nereusstream.managedledger.errors.OperationContext;
import com.nereusstream.managedledger.projection.F2L0RequestFactory;
import com.nereusstream.managedledger.projection.PositionProjection;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import com.nereusstream.managedledger.snapshot.StreamSnapshotTracker;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;

/** Get-only F2 ledger handle; cursor construction is completed by F2-M4. */
public final class NereusReadOnlyManagedLedger implements ReadOnlyManagedLedger, NereusCursorLedgerView {
    private final NereusManagedLedgerRuntime runtime;
    private final VirtualLedgerProjection projection;
    private final Map<String, String> properties;
    private final StreamSnapshotTracker snapshots;
    private final PulsarEntryCodec codec;
    private final PositionProjection positions = new PositionProjection();
    private final F2L0RequestFactory requests = new F2L0RequestFactory();
    private final ManagedLedgerErrorMapper errorMapper = new ManagedLedgerErrorMapper();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Runnable onClose;

    public NereusReadOnlyManagedLedger(
            NereusManagedLedgerRuntime runtime,
            NereusLedgerOpenResult opened,
            Runnable onClose) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        NereusLedgerOpenResult result = Objects.requireNonNull(opened, "opened");
        this.projection = result.projection();
        this.properties = result.topicProjection().properties();
        this.snapshots = new StreamSnapshotTracker(result.streamMetadata(), 0);
        this.codec = new PulsarEntryCodec(runtime.config().maxEntryBytes());
        this.onClose = Objects.requireNonNull(onClose, "onClose");
    }

    public String getName() {
        return projection.managedLedgerName();
    }

    public VirtualLedgerProjection projection() {
        return projection;
    }

    @Override
    public void asyncReadEntry(Position position, ReadEntryCallback callback, Object ctx) {
        Objects.requireNonNull(callback, "callback");
        if (!runtime.tryAcquireCallbackPermit()) {
            runtime.callbackExecutor().execute(() -> callback.readEntryFailed(
                    new ManagedLedgerException.TooManyRequestsException(
                            "Nereus callback capacity is exhausted"), ctx));
            return;
        }
        if (closed.get()) {
            runtime.releaseCallbackPermit();
            runtime.callbackExecutor().execute(() -> callback.readEntryFailed(
                    new ManagedLedgerException.ManagedLedgerAlreadyClosedException(
                            "read-only managed ledger is closed"), ctx));
            return;
        }
        runtime.streamStorage().getStreamMetadata(projection.streamId())
                .thenApply(snapshots::updateFromMetadata)
                .thenCompose(view -> {
                    long offset = positions.requireReadableEntryOffset(
                            projection, position, view.metadata());
                    return runtime.streamStorage().read(
                            projection.streamId(),
                            offset,
                            requests.singleEntryReadOptions(
                                    runtime.config().maxEntryBytes(), runtime.config().readTimeout()));
                })
                .thenApply(result -> codec.decode(position, result))
                .whenCompleteAsync((entry, error) -> {
                    try {
                        if (error == null) {
                            callback.readEntryComplete(entry, ctx);
                        } else {
                            callback.readEntryFailed(map(error), ctx);
                        }
                    } finally {
                        runtime.releaseCallbackPermit();
                    }
                }, runtime.callbackExecutor());
    }

    @Override
    public long getNumberOfEntries() {
        com.nereusstream.api.StreamMetadata metadata = snapshots.current().metadata();
        return metadata.committedEndOffset() - metadata.trimOffset();
    }

    @Override
    public ReadOnlyCursor createReadOnlyCursor(Position position) {
        if (closed.get()) {
            throw new IllegalStateException("read-only managed ledger is closed");
        }
        StreamMetadata metadata = refreshMetadata().join();
        long offset;
        if (samePosition(position, org.apache.bookkeeper.mledger.PositionFactory.EARLIEST)) {
            offset = metadata.trimOffset();
        } else if (samePosition(position, org.apache.bookkeeper.mledger.PositionFactory.LATEST)) {
            offset = metadata.committedEndOffset();
        } else {
            offset = positions.requireReadPositionOffset(projection, position, metadata);
        }
        return new NereusReadOnlyCursor(this, positions.readPosition(projection, offset, metadata));
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    void factoryClose() {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
    }

    @Override
    public NereusManagedLedgerRuntime runtime() {
        return runtime;
    }

    @Override
    public StreamMetadata currentMetadata() {
        return snapshots.current().metadata();
    }

    @Override
    public CompletableFuture<StreamMetadata> refreshMetadata() {
        return runtime.streamStorage().getStreamMetadata(projection.streamId())
                .thenApply(metadata -> snapshots.updateFromMetadata(metadata).metadata());
    }

    @Override
    public CompletableFuture<Entry> readAt(long offset, StreamMetadata metadata) {
        Position position = positions.entryPosition(projection, offset);
        positions.requireReadableEntryOffset(projection, position, metadata);
        return runtime.streamStorage().read(
                        projection.streamId(),
                        offset,
                        requests.singleEntryReadOptions(
                                runtime.config().maxEntryBytes(), runtime.config().readTimeout()))
                .thenApply(result -> codec.decode(position, result));
    }

    @Override
    public Position readPosition(long offset, StreamMetadata metadata) {
        return positions.readPosition(projection, offset, metadata);
    }

    @Override
    public Position normalizeInclusiveMax(Position position, StreamMetadata metadata) {
        return positions.normalizeInclusiveMaxPosition(projection, position, metadata);
    }

    private static boolean samePosition(Position left, Position right) {
        return left.getLedgerId() == right.getLedgerId() && left.getEntryId() == right.getEntryId();
    }

    private ManagedLedgerException map(Throwable error) {
        return errorMapper.map(unwrap(error), new OperationContext(
                "readOnlyReadEntry",
                false,
                true,
                Optional.of(snapshots.current().metadata().state())));
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
