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

package com.nereusstream.core.read;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolveResult;
import com.nereusstream.api.ResolvedObjectRange;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.DerivedIndexRepairCursor;
import com.nereusstream.metadata.oxia.DerivedIndexRepairResult;
import com.nereusstream.metadata.oxia.MetadataWatcher;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.WatchRegistration;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ReadResolver implements AutoCloseable {
    private static final Comparator<OffsetIndexRecord> GENERATION_ORDER = Comparator
            .comparingLong(OffsetIndexRecord::generation)
            .thenComparingLong(OffsetIndexRecord::commitVersion);

    private final StreamStorageConfig config;
    private final OxiaMetadataStore metadataStore;
    private final OffsetIndexCache cache;
    private final ReadMetricsObserver observer;
    private final Executor callbackExecutor;
    private final ConcurrentHashMap<StreamId, WatchRegistration> watches = new ConcurrentHashMap<>();
    private final Semaphore watchSlots;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ReadResolver(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            Clock clock,
            ReadMetricsObserver observer,
            Executor callbackExecutor) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.cache = new OffsetIndexCache(
                config.enableOffsetIndexCache(),
                config.offsetIndexCacheTtl(),
                clock,
                config.maxCachedStreams(),
                config.maxCommitChainScan());
        this.observer = Objects.requireNonNull(observer, "observer");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.watchSlots = new Semaphore(config.maxCachedStreams());
    }

    CompletableFuture<Resolution> resolve(
            StreamId streamId,
            long startOffset,
            ResolveOptions options,
            ReadOperationDeadline deadline) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(options, "options");
        if (startOffset < 0) {
            return NereusException.failedFuture(
                    ErrorCode.INVALID_ARGUMENT, false, "startOffset must be non-negative");
        }
        if (closed.get()) {
            return NereusException.failedFuture(ErrorCode.STORAGE_CLOSED, false, "read resolver is closed");
        }
        ensureWatch(streamId);
        return loadSnapshot(streamId, deadline)
                .thenComposeAsync(snapshot -> {
                    validateReadable(snapshot.metadata());
                    if (startOffset < snapshot.trim().trimOffset()) {
                        throw new NereusException(
                                ErrorCode.OFFSET_TRIMMED,
                                false,
                                "requested offset is below the stream trim offset");
                    }
                    int maxRanges = Math.min(options.maxRanges(), config.maxResolveRanges());
                    if (startOffset >= snapshot.committed().committedEndOffset()) {
                        return CompletableFuture.completedFuture(emptyResolution(
                                streamId, startOffset, snapshot.metadataVersion()));
                    }
                    BuildState initial = new BuildState(
                            startOffset,
                            List.of(),
                            snapshot.metadataVersion(),
                            false);
                    return buildRanges(
                                    streamId,
                                    snapshot,
                                    options,
                                    maxRanges,
                                    initial,
                                    false,
                                    deadline)
                            .thenApply(state -> new Resolution(
                                    new ResolveResult(
                                            streamId,
                                            startOffset,
                                            state.ranges(),
                                            state.cursor(),
                                            state.metadataVersion()),
                                    state.cacheUsed()));
                }, callbackExecutor);
    }

    void invalidate(StreamId streamId) {
        cache.invalidate(streamId);
    }

    private CompletableFuture<Snapshot> loadSnapshot(
            StreamId streamId,
            ReadOperationDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.getStreamSnapshot(config.cluster(), streamId),
                        "load stream metadata snapshot for resolve")
                .thenApplyAsync(snapshot -> validateSnapshot(streamId, snapshot), callbackExecutor);
    }

    private static Snapshot validateSnapshot(StreamId streamId, StreamMetadataSnapshot snapshot) {
        if (!snapshot.metadata().streamId().equals(streamId.value())) {
            throw invariant("resolve metadata snapshot belongs to another stream");
        }
        return new Snapshot(
                snapshot.metadata(),
                snapshot.trim(),
                snapshot.committedEnd(),
                snapshot.metadataVersion());
    }

    private CompletableFuture<BuildState> buildRanges(
            StreamId streamId,
            Snapshot snapshot,
            ResolveOptions options,
            int maxRanges,
            BuildState state,
            boolean repairedAtCursor,
            ReadOperationDeadline deadline) {
        deadline.check("resolve offset ranges");
        if (state.ranges().size() >= maxRanges
                || state.cursor() >= snapshot.committed().committedEndOffset()) {
            return CompletableFuture.completedFuture(state);
        }
        return scan(streamId, state.cursor(), snapshot.trim().trimOffset(), options.allowCache(), deadline)
                .thenComposeAsync(source -> {
                    OffsetIndexRecord selected = select(streamId, state.cursor(), source.records());
                    if (selected == null) {
                        if (state.cursor() >= snapshot.committed().committedEndOffset()) {
                            return CompletableFuture.completedFuture(state.withCacheUsed(source.fromCache()));
                        }
                        if (repairedAtCursor) {
                            throw invariant("committed offset cannot be materialized into the offset index");
                        }
                        return repair(streamId, state.cursor(), deadline)
                                .thenCompose(ignored -> {
                                    cache.invalidate(streamId);
                                    return buildRanges(
                                            streamId,
                                            snapshot,
                                            options,
                                            maxRanges,
                                            state.withCacheUsed(source.fromCache()),
                                            true,
                                            deadline);
                                });
                    }
                    ResolvedObjectRange range = toResolvedRange(selected);
                    List<ResolvedObjectRange> ranges = new ArrayList<>(state.ranges());
                    ranges.add(range);
                    BuildState advanced = new BuildState(
                            selected.offsetEnd(),
                            List.copyOf(ranges),
                            Math.max(state.metadataVersion(), selected.metadataVersion()),
                            state.cacheUsed() || source.fromCache());
                    return buildRanges(
                            streamId,
                            snapshot,
                            options,
                            maxRanges,
                            advanced,
                            false,
                            deadline);
                }, callbackExecutor);
    }

    private CompletableFuture<ScanSource> scan(
            StreamId streamId,
            long cursor,
            long trimOffset,
            boolean allowCache,
            ReadOperationDeadline deadline) {
        if (allowCache) {
            Optional<List<OffsetIndexRecord>> cached = cache.lookup(streamId, cursor, trimOffset);
            if (cached.isPresent()) {
                observe(observer::onOffsetIndexCacheHit);
                return CompletableFuture.completedFuture(new ScanSource(cached.orElseThrow(), true));
            }
            if (config.enableOffsetIndexCache()) {
                observe(observer::onOffsetIndexCacheMiss);
            }
        }
        return deadline.bound(
                        () -> metadataStore.scanOffsetIndex(
                                config.cluster(),
                                streamId,
                                cursor,
                                config.maxCommitChainScan()),
                        "scan offset index")
                .thenApplyAsync(records -> {
                    for (OffsetIndexRecord record : records) {
                        if (!record.streamId().equals(streamId.value())) {
                            throw invariant("offset-index scan returned another stream");
                        }
                    }
                    cache.putPositive(streamId, records, trimOffset);
                    return new ScanSource(List.copyOf(records), false);
                }, callbackExecutor);
    }

    private CompletableFuture<Void> repair(
            StreamId streamId,
            long targetOffset,
            ReadOperationDeadline deadline) {
        return repairPage(streamId, targetOffset, Optional.empty(), 0, deadline);
    }

    private CompletableFuture<Void> repairPage(
            StreamId streamId,
            long targetOffset,
            Optional<DerivedIndexRepairCursor> continuation,
            int scannedRecords,
            ReadOperationDeadline deadline) {
        int remainingBudget = config.maxCommitChainScan() - scannedRecords;
        if (remainingBudget <= 0) {
            return NereusException.failedFuture(
                    ErrorCode.READ_RESOLUTION_FAILED,
                    true,
                    "derived-index repair exhausted the read repair budget");
        }
        int pageSize = Math.min(config.maxDerivedIndexRepairCommitsPerCall(), remainingBudget);
        return deadline.bound(
                        () -> metadataStore.repairDerivedStreamIndexes(
                                config.cluster(),
                                streamId,
                                targetOffset,
                                continuation,
                                pageSize),
                        "repair derived offset index")
                .thenComposeAsync(result -> continueRepair(
                        streamId, targetOffset, result, scannedRecords, deadline), callbackExecutor);
    }

    private CompletableFuture<Void> continueRepair(
            StreamId streamId,
            long targetOffset,
            DerivedIndexRepairResult result,
            int scannedBefore,
            ReadOperationDeadline deadline) {
        if (!result.streamId().equals(streamId)) {
            throw invariant("derived-index repair returned another stream");
        }
        if (result.targetCovered()) {
            return CompletableFuture.completedFuture(null);
        }
        if (!result.repairBudgetExhausted() || result.continuation().isEmpty()) {
            throw invariant("derived-index repair neither covered the target nor returned a continuation");
        }
        if (result.scannedRecords() <= 0) {
            throw invariant("exhausted derived-index repair made no scan progress");
        }
        int scanned;
        try {
            scanned = Math.addExact(scannedBefore, result.scannedRecords());
        } catch (ArithmeticException e) {
            throw invariant("derived-index repair scan count overflows", e);
        }
        if (scanned >= config.maxCommitChainScan()) {
            return NereusException.failedFuture(
                    ErrorCode.READ_RESOLUTION_FAILED,
                    true,
                    "derived-index repair exhausted the read repair budget");
        }
        return repairPage(streamId, targetOffset, result.continuation(), scanned, deadline);
    }

    private static OffsetIndexRecord select(
            StreamId streamId,
            long cursor,
            List<OffsetIndexRecord> records) {
        return records.stream()
                .filter(record -> record.streamId().equals(streamId.value()))
                .filter(record -> !record.tombstoned())
                .filter(record -> record.offsetStart() <= cursor && cursor < record.offsetEnd())
                .max(GENERATION_ORDER)
                .orElse(null);
    }

    private static ResolvedObjectRange toResolvedRange(OffsetIndexRecord record) {
        try {
            if (!"WAL_OBJECT_V1".equals(record.physicalFormat())
                    || !"OPAQUE_SLICE".equals(record.logicalFormat())) {
                throw invariant("offset index contains an unsupported physical or logical format");
            }
            ObjectType objectType = ObjectType.valueOf(record.objectType());
            PayloadFormat payloadFormat = PayloadFormat.valueOf(record.payloadFormat());
            Checksum sliceChecksum = new Checksum(
                    ChecksumType.valueOf(record.sliceChecksumType()),
                    record.sliceChecksumValue());
            return new ResolvedObjectRange(
                    new OffsetRange(record.offsetStart(), record.offsetEnd()),
                    record.generation(),
                    new ObjectId(record.objectId()),
                    new ObjectKey(record.objectKey()),
                    objectType,
                    record.objectOffset(),
                    record.objectLength(),
                    sliceChecksum,
                    payloadFormat,
                    record.schemaRefs(),
                    toEntryIndexRef(record.entryIndexRef()),
                    toProjectionRef(record.projectionRef()),
                    record.commitVersion());
        } catch (NereusException e) {
            throw e;
        } catch (RuntimeException e) {
            throw invariant("offset index cannot be mapped to a resolved object range", e);
        }
    }

    private static EntryIndexRef toEntryIndexRef(EntryIndexReferenceRecord record) {
        EntryIndexLocation location = EntryIndexLocation.valueOf(record.location());
        Optional<ObjectId> objectId = record.objectId().isEmpty()
                ? Optional.empty()
                : Optional.of(new ObjectId(record.objectId()));
        Optional<ObjectKey> objectKey = record.objectKey().isEmpty()
                ? Optional.empty()
                : Optional.of(new ObjectKey(record.objectKey()));
        byte[] inlineData = record.inlineData();
        Optional<byte[]> inline = inlineData.length == 0 ? Optional.empty() : Optional.of(inlineData);
        return new EntryIndexRef(
                location,
                objectId,
                objectKey,
                inline,
                record.offset(),
                record.length(),
                new Checksum(ChecksumType.valueOf(record.checksumType()), record.checksumValue()));
    }

    private static Optional<ProjectionRef> toProjectionRef(String identity) {
        if (CommitSliceRequest.emptyProjectionIdentity().equals(identity)) {
            return Optional.empty();
        }
        throw new NereusException(
                ErrorCode.UNSUPPORTED_FORMAT,
                false,
                "Phase 1 resolver does not support materialized projection references");
    }

    private void validateReadable(StreamMetadataRecord metadata) {
        StreamState state;
        StorageProfile profile;
        try {
            state = StreamState.valueOf(metadata.state());
            profile = StorageProfile.valueOf(metadata.profile()).canonical();
        } catch (IllegalArgumentException e) {
            throw invariant("stream metadata contains an unknown state or profile", e);
        }
        switch (state) {
            case ACTIVE, SEALED -> {
            }
            case CREATING -> throw new NereusException(
                    ErrorCode.STREAM_NOT_ACTIVE, true, "stream is still being created");
            case DELETING -> throw new NereusException(
                    ErrorCode.STREAM_NOT_ACTIVE, false, "stream is being deleted");
            case DELETED -> throw new NereusException(
                    ErrorCode.STREAM_NOT_FOUND, false, "stream was deleted");
        }
        if (profile != StorageProfile.OBJECT_WAL_SYNC_OBJECT) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "M5 reads support only OBJECT_WAL_SYNC_OBJECT");
        }
    }

    private void ensureWatch(StreamId streamId) {
        if (!config.enableMetadataWatch() || !config.enableOffsetIndexCache() || closed.get()
                || watches.containsKey(streamId)) {
            return;
        }
        try {
            watches.computeIfAbsent(streamId, ignored -> registerBoundedWatch(streamId));
        } catch (RuntimeException ignored) {
            // Watches are hints. TTL and read-through scans remain the correctness path.
        }
    }

    private WatchRegistration registerBoundedWatch(StreamId streamId) {
        if (!watchSlots.tryAcquire()) {
            return null;
        }
        try {
            WatchRegistration delegate = metadataStore.watchStream(
                    config.cluster(), streamId, new CacheInvalidatingWatcher());
            AtomicBoolean active = new AtomicBoolean(true);
            return () -> {
                if (active.compareAndSet(true, false)) {
                    try {
                        delegate.close();
                    } finally {
                        watchSlots.release();
                    }
                }
            };
        } catch (RuntimeException e) {
            watchSlots.release();
            throw e;
        }
    }

    private static Resolution emptyResolution(
            StreamId streamId,
            long startOffset,
            long metadataVersion) {
        return new Resolution(
                new ResolveResult(streamId, startOffset, List.of(), startOffset, metadataVersion),
                false);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        watches.values().forEach(registration -> {
            try {
                registration.close();
            } catch (RuntimeException ignored) {
                // Best-effort hint cleanup.
            }
        });
        watches.clear();
        cache.clear();
    }

    private final class CacheInvalidatingWatcher implements MetadataWatcher {
        @Override
        public void onOffsetIndexUpdated(StreamId streamId, long committedEndOffset, long metadataVersion) {
            cache.invalidateFromWatch(streamId, metadataVersion);
        }

        @Override
        public void onTrimUpdated(StreamId streamId, long trimOffset, long metadataVersion) {
            cache.invalidateFromWatch(streamId, metadataVersion);
        }

        @Override
        public void onAppendSessionChanged(StreamId streamId, long epoch, long leaseVersion) {
        }

        @Override
        public void onWatchReconnected(StreamId streamId, long metadataVersion) {
            cache.invalidate(streamId);
        }
    }

    record Resolution(ResolveResult result, boolean cacheUsed) {
    }

    private record Snapshot(
            StreamMetadataRecord metadata,
            TrimRecord trim,
            CommittedEndOffsetRecord committed,
            long metadataVersion) {
    }

    private record ScanSource(List<OffsetIndexRecord> records, boolean fromCache) {
    }

    private record BuildState(
            long cursor,
            List<ResolvedObjectRange> ranges,
            long metadataVersion,
            boolean cacheUsed) {
        private BuildState {
            ranges = List.copyOf(ranges);
        }

        private BuildState withCacheUsed(boolean used) {
            return new BuildState(cursor, ranges, metadataVersion, cacheUsed || used);
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static void observe(Runnable callback) {
        try {
            callback.run();
        } catch (RuntimeException ignored) {
            // Metrics callbacks cannot alter resolve correctness.
        }
    }
}
