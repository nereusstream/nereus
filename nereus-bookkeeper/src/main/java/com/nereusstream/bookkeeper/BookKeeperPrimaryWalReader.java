/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.core.read.ReadTargetReaderKey;
import com.nereusstream.core.wal.PrimaryWalReader;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;

/** Exact non-recovery BookKeeper range reader with fixed-slot deletion fencing. */
public final class BookKeeperPrimaryWalReader implements PrimaryWalReader {
    public static final ReadTargetReaderKey KEY = new ReadTargetReaderKey(
            com.nereusstream.api.target.ReadTargetType.BOOKKEEPER_ENTRY_RANGE,
            1,
            Optional.empty(),
            Optional.empty());

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerMetadataStore metadata;
    private final BookKeeperClientOperations client;
    private final BookKeeperPasswordProvider passwordProvider;
    private final BookKeeperLedgerHandleCache handles;
    private final BookKeeperReaderLeaseManager leases;
    private final AtomicBoolean closed = new AtomicBoolean();
    private int inFlightReads;
    private long inFlightBytes;

    public BookKeeperPrimaryWalReader(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerMetadataStore metadata,
            BookKeeperClientOperations client,
            BookKeeperPasswordProvider passwordProvider,
            BookKeeperLedgerHandleCache handles,
            BookKeeperReaderLeaseManager leases) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.client = Objects.requireNonNull(client, "client");
        this.passwordProvider = Objects.requireNonNull(passwordProvider, "passwordProvider");
        this.handles = Objects.requireNonNull(handles, "handles");
        this.leases = Objects.requireNonNull(leases, "leases");
    }

    @Override
    public ReadTargetReaderKey key() {
        return KEY;
    }

    @Override
    public long reservationBytes(ResolvedRange range) {
        ResolvedRange exact = Objects.requireNonNull(range, "range");
        BookKeeperEntryRangeReadTarget target = requireTarget(exact);
        return target.entryMapping() == BookKeeperEntryMapping.RANGED_NEREUS_ENTRY_V1
                ? BookKeeperRangedEntryCodecV1.encodedRangeBytes(
                        exact.logicalBytes(), exact.entryCount())
                : exact.logicalBytes();
    }

    @Override
    public CompletableFuture<PhysicalReadResult> readPhysicalWithStats(
            StreamId streamId,
            long startOffset,
            List<ResolvedRange> ranges,
            ReadOptions options) {
        return readPhysicalWithStats(
                streamId,
                new ReadRequest(
                        startOffset,
                        ReadView.COMMITTED,
                        ReadBoundaryMode.EXACT_START,
                        FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                        options),
                ranges);
    }

    @Override
    public CompletableFuture<PhysicalReadResult> readPhysicalWithStats(
            StreamId streamId,
            ReadRequest request,
            List<ResolvedRange> ranges) {
        ensureOpen();
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        List<ResolvedRange> exactRanges = List.copyOf(Objects.requireNonNull(ranges, "ranges"));
        ReadRequest exactRequest = Objects.requireNonNull(request, "request");
        ReadOptions exactOptions = exactRequest.options();
        long reservedBytes = exactRanges.stream().mapToLong(this::reservationBytes).reduce(0, Math::addExact);
        ReadPermit permit = acquirePermit(reservedBytes);
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(min(
                exactOptions.timeout(), configuration.operationTimeout()));
        ReadAccumulator accumulator = new ReadAccumulator(stream, exactRequest);
        CompletableFuture<PhysicalReadResult> result = readNext(exactRanges, 0, accumulator, deadline)
                .thenApply(ignored -> new PhysicalReadResult(accumulator.batches, accumulator.stats));
        return result.whenComplete((ignored, failure) -> permit.close());
    }

    private CompletableFuture<Void> readNext(
            List<ResolvedRange> ranges,
            int index,
            ReadAccumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        if (index >= ranges.size() || accumulator.full()) {
            return CompletableFuture.completedFuture(null);
        }
        ResolvedRange range = ranges.get(index);
        if (range.offsetRange().endOffset() <= accumulator.request.startOffset()) {
            return readNext(ranges, index + 1, accumulator, deadline);
        }
        BookKeeperEntryRangeReadTarget target = requireTarget(range);
        return readAndVerify(accumulator.stream, range, target, deadline)
                .thenCompose(verified -> {
                    accumulator.add(range, target, verified);
                    return readNext(ranges, index + 1, accumulator, deadline);
                });
    }

    private CompletableFuture<VerifiedRange> readAndVerify(
            StreamId stream,
            ResolvedRange range,
            BookKeeperEntryRangeReadTarget target,
            BookKeeperOperationDeadline deadline) {
        return deadline.bound(metadata.getRoot(
                        cluster,
                        configuration.providerScopeSha256(),
                        target.ledgerId()))
                .thenApply(optional -> optional.orElseThrow(
                        () -> new NereusException(
                                ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND,
                                false,
                                "BookKeeper ledger root is absent")))
                .thenCompose(root -> {
                    requireRoot(root.value(), stream, target);
                    return leases.claim(root, deadline.remaining())
                            .thenCompose(lease -> withReaderLease(
                                    lease,
                                    () -> readPinned(root, range, target, deadline)));
                });
    }

    private CompletableFuture<VerifiedRange> readPinned(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            ResolvedRange range,
            BookKeeperEntryRangeReadTarget target,
            BookKeeperOperationDeadline deadline) {
        BookKeeperLedgerHandleCache.Key key = new BookKeeperLedgerHandleCache.Key(
                target.clusterAlias(), target.ledgerId(), root.value().lifecycleEpoch());
        return handles.borrow(key, () -> open(root.value(), target, deadline))
                .thenCompose(handleLease -> {
                    CompletableFuture<VerifiedRange> read = readEntries(
                            handleLease.handle(), range, target, deadline);
                    return read.whenComplete((ignored, failure) -> handleLease.close());
                });
    }

    private CompletableFuture<ReadHandle> open(
            BookKeeperLedgerRootRecord root,
            BookKeeperEntryRangeReadTarget target,
            BookKeeperOperationDeadline deadline) {
        byte[] password = Objects.requireNonNull(
                passwordProvider.resolve(configuration.passwordRef()),
                "resolved BookKeeper password").clone();
        CompletableFuture<ReadHandle> opened;
        try {
            opened = client.open(
                    target.ledgerId(), configuration.digestType(), password, false, deadline);
        } catch (Throwable failure) {
            Arrays.fill(password, (byte) 0);
            return CompletableFuture.failedFuture(failure);
        }
        return opened.whenComplete((ignored, failure) -> Arrays.fill(password, (byte) 0))
                .thenApply(handle -> {
                    try {
                        if (handle.getId() != target.ledgerId()) {
                            throw invariant("BookKeeper non-recovery open returned another ledger");
                        }
                        BookKeeperLedgerCustomMetadata.fromRoot(cluster, configuration, root)
                                .requireExactImmutableLedgerMetadata(
                                        target.ledgerId(), configuration, handle.getLedgerMetadata());
                        return handle;
                    } catch (Throwable failure) {
                        handle.closeAsync();
                        throw failure;
                    }
                });
    }

    private CompletableFuture<VerifiedRange> readEntries(
            ReadHandle handle,
            ResolvedRange range,
            BookKeeperEntryRangeReadTarget target,
            BookKeeperOperationDeadline deadline) {
        long lastEntryId = Math.addExact(target.firstEntryId(), target.entryCount() - 1L);
        return deadline.bound(client.readUnconfirmed(
                        handle,
                        target.firstEntryId(),
                        lastEntryId,
                        deadline))
                .thenApply(entries -> copyAndVerify(entries, range, target));
    }

    private VerifiedRange copyAndVerify(
            LedgerEntries providerEntries,
            ResolvedRange range,
            BookKeeperEntryRangeReadTarget target) {
        List<byte[]> physicalEntries = new ArrayList<>(target.entryCount());
        try (LedgerEntries exact = providerEntries) {
            int index = 0;
            for (LedgerEntry entry : exact) {
                long expectedId = Math.addExact(target.firstEntryId(), index);
                if (entry.getEntryId() != expectedId || index >= target.entryCount()) {
                    throw invariant("BookKeeper range contains a missing, duplicate, or unexpected entry id");
                }
                ByteBuf buffer = entry.getEntryBuffer();
                byte[] bytes = new byte[buffer.readableBytes()];
                buffer.getBytes(buffer.readerIndex(), bytes);
                physicalEntries.add(bytes);
                index++;
            }
        }
        if (physicalEntries.size() != target.entryCount()) {
            throw invariant("BookKeeper range returned fewer entries than the durable target");
        }
        if (!BookKeeperRangeChecksums.computeBytes(target.firstEntryId(), physicalEntries)
                .equals(target.rangeChecksum())) {
            throw new NereusException(
                    ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH,
                    false,
                    "BookKeeper NBKR1 range checksum mismatch");
        }
        long physicalBytes = physicalEntries.stream()
                .mapToLong(value -> value.length)
                .reduce(0, Math::addExact);
        List<VerifiedEntry> entries = physicalEntries.stream()
                .map(value -> decodeEntry(target.entryMapping(), value))
                .toList();
        long logicalBytes = entries.stream()
                .mapToLong(value -> value.payload().length)
                .reduce(0, Math::addExact);
        long records = entries.stream()
                .mapToLong(VerifiedEntry::recordCount)
                .reduce(0, Math::addExact);
        if (logicalBytes != range.logicalBytes()
                || records != range.recordCount()
                || entries.size() != range.entryCount()) {
            throw invariant("BookKeeper decoded entry facts do not match the resolved logical range");
        }
        return new VerifiedRange(entries, physicalBytes);
    }

    private static VerifiedEntry decodeEntry(
            BookKeeperEntryMapping mapping,
            byte[] physicalEntry) {
        return switch (mapping) {
            case ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY ->
                    new VerifiedEntry(1, physicalEntry);
            case RANGED_NEREUS_ENTRY_V1 -> {
                BookKeeperRangedEntryCodecV1.DecodedEntry decoded =
                        BookKeeperRangedEntryCodecV1.decode(physicalEntry);
                yield new VerifiedEntry(decoded.recordCount(), decoded.payload());
            }
        };
    }

    private CompletableFuture<VerifiedRange> withReaderLease(
            BookKeeperReaderLeaseManager.Lease lease,
            java.util.function.Supplier<CompletableFuture<VerifiedRange>> operation) {
        CompletableFuture<VerifiedRange> read;
        try {
            read = operation.get().thenCompose(value -> lease.revalidate().thenApply(ignored -> value));
        } catch (Throwable failure) {
            read = CompletableFuture.failedFuture(failure);
        }
        return read.handle((value, failure) -> lease.release()
                        .handle((ignored, releaseFailure) -> {
                            if (failure != null) throw new java.util.concurrent.CompletionException(unwrap(failure));
                            return value;
                        }))
                .thenCompose(java.util.function.Function.identity());
    }

    private BookKeeperEntryRangeReadTarget requireTarget(ResolvedRange range) {
        if (!(range.readTarget() instanceof BookKeeperEntryRangeReadTarget target)
                || !target.clusterAlias().equals(configuration.clusterAlias())
                || target.entryCount() != range.entryCount()) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_READ_TARGET,
                    false,
                    "BookKeeper reader received an incompatible resolved range");
        }
        if (target.entryMapping() == BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY
                && (target.entryCount() != range.recordCount()
                        || target.entryCount() != range.offsetRange().recordCount())) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_READ_TARGET,
                    false,
                    "legacy BookKeeper mapping requires one logical offset per physical entry");
        }
        return target;
    }

    private static void requireRoot(
            BookKeeperLedgerRootRecord root,
            StreamId stream,
            BookKeeperEntryRangeReadTarget target) {
        boolean readable = root.lifecycle() == BookKeeperLedgerLifecycle.ACTIVE
                || root.lifecycle() == BookKeeperLedgerLifecycle.SEALING
                || root.lifecycle() == BookKeeperLedgerLifecycle.SEALED;
        if (!readable
                || root.ledgerId() != target.ledgerId()
                || !root.clusterAlias().equals(target.clusterAlias())
                || !root.streamId().equals(stream.value())) {
            throw new NereusException(
                    ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND,
                    false,
                    "BookKeeper ledger root cannot serve the resolved range");
        }
    }

    private synchronized ReadPermit acquirePermit(long bytes) {
        ensureOpen();
        if (bytes > configuration.maxReadBytesInFlight()
                || inFlightReads >= configuration.maxReadsInFlight()
                || Math.addExact(inFlightBytes, bytes) > configuration.maxReadBytesInFlight()) {
            throw new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED,
                    true,
                    "BookKeeper primary reader capacity is exhausted");
        }
        inFlightReads++;
        inFlightBytes += bytes;
        return new ReadPermit(bytes);
    }

    private final class ReadPermit implements AutoCloseable {
        private final long bytes;
        private final AtomicBoolean released = new AtomicBoolean();

        private ReadPermit(long bytes) {
            this.bytes = bytes;
        }

        @Override
        public void close() {
            if (!released.compareAndSet(false, true)) return;
            synchronized (BookKeeperPrimaryWalReader.this) {
                inFlightReads--;
                inFlightBytes -= bytes;
            }
        }
    }

    private static final class ReadAccumulator {
        private final StreamId stream;
        private final ReadRequest request;
        private final List<ReadBatch> batches = new ArrayList<>();
        private final List<PhysicalReadStats> stats = new ArrayList<>();
        private int returnedRecords;
        private long returnedBytes;
        private boolean limitReached;

        private ReadAccumulator(StreamId stream, ReadRequest request) {
            this.stream = stream;
            this.request = request;
        }

        private boolean full() {
            return limitReached
                    || returnedRecords >= request.options().maxRecords()
                    || returnedBytes >= request.options().maxBytes();
        }

        private void add(
                ResolvedRange range,
                BookKeeperEntryRangeReadTarget target,
                VerifiedRange verified) {
            long rangeReturnedBytes = 0;
            ReadSourceRef source = new ReadSourceRef(
                    range.offsetRange(),
                    range.generation(),
                    range.commitVersion(),
                    target,
                    ReadTargetIdentities.sha256(target));
            long offset = range.offsetRange().startOffset();
            for (VerifiedEntry entry : verified.entries) {
                long endOffset = Math.addExact(offset, entry.recordCount());
                if (endOffset <= request.startOffset()) {
                    offset = endOffset;
                    continue;
                }
                if (request.boundaryMode() == ReadBoundaryMode.EXACT_START
                        && offset < request.startOffset()) {
                    throw new NereusException(
                            ErrorCode.OFFSET_NOT_AVAILABLE,
                            false,
                            "requested offset is inside a ranged BookKeeper entry");
                }
                boolean recordLimitExceeded = (long) returnedRecords + entry.recordCount()
                        > request.options().maxRecords();
                byte[] payload = entry.payload();
                boolean byteLimitExceeded = Math.addExact(returnedBytes, payload.length)
                        > request.options().maxBytes();
                boolean firstEntryOverflow = request.firstEntryPolicy()
                        == FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW
                        && batches.isEmpty();
                if ((recordLimitExceeded || byteLimitExceeded) && !firstEntryOverflow) {
                    if (byteLimitExceeded
                            && batches.isEmpty()
                            && target.entryMapping() == BookKeeperEntryMapping.RANGED_NEREUS_ENTRY_V1) {
                        throw new NereusException(
                                ErrorCode.READ_LIMIT_TOO_SMALL,
                                true,
                                "first readable ranged BookKeeper entry exceeds maxBytes");
                    }
                    limitReached = true;
                    break;
                }
                batches.add(new ReadBatch(
                        new OffsetRange(offset, endOffset),
                        range.payloadFormat(),
                        payload,
                        range.schemaRefs(),
                        range.projectionRef(),
                        source));
                returnedRecords = Math.addExact(returnedRecords, entry.recordCount());
                returnedBytes = Math.addExact(returnedBytes, payload.length);
                rangeReturnedBytes = Math.addExact(rangeReturnedBytes, payload.length);
                offset = endOffset;
                if (recordLimitExceeded || byteLimitExceeded) {
                    limitReached = true;
                    break;
                }
            }
            stats.add(new PhysicalReadStats(
                    ReadTargetIdentities.sha256(target),
                    verified.physicalBytes,
                    0,
                    verified.physicalBytes,
                    0,
                    rangeReturnedBytes));
        }
    }

    private record VerifiedEntry(int recordCount, byte[] payload) {
        private VerifiedEntry {
            if (recordCount <= 0) {
                throw new IllegalArgumentException("recordCount must be positive");
            }
            payload = Objects.requireNonNull(payload, "payload").clone();
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }

    private record VerifiedRange(List<VerifiedEntry> entries, long physicalBytes) {
        private VerifiedRange {
            entries = List.copyOf(entries);
            if (entries.isEmpty() || physicalBytes < 0) {
                throw new IllegalArgumentException("invalid verified BookKeeper range");
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        handles.close();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "BookKeeper primary reader is closed");
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
