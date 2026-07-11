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

package com.nereusstream.metadata.oxia;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.metadata.oxia.codec.MetadataCodecException;
import com.nereusstream.metadata.oxia.codec.Phase1MetadataCodecs;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.CommittedSliceRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitRecord;
import com.nereusstream.metadata.oxia.records.StreamHeadRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.StreamNameRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.metadata.oxia.records.VisibleSliceReferenceRecord;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/** Production Phase 1 metadata adapter backed by the public Oxia Java client API. */
public final class OxiaJavaClientMetadataStore implements OxiaMetadataStore {
    private static final int MAX_CAS_RETRIES = 64;

    private final OxiaClientConfiguration configuration;
    private final SyncOxiaClient oxiaClient;
    private final PartitionedOxiaClient client;
    private final Clock clock;
    private final ExecutorService operationExecutor;
    private final ExecutorService clientExecutor;
    private final ExecutorService watchExecutor;
    private final CopyOnWriteArrayList<WatchRegistration> watches = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public static OxiaJavaClientMetadataStore connect(
            OxiaClientConfiguration configuration,
            Clock clock) {
        Objects.requireNonNull(configuration, "configuration");
        try {
            SyncOxiaClient client = OxiaClientBuilder.create(configuration.serviceAddress())
                    .namespace(configuration.namespace())
                    .requestTimeout(configuration.requestTimeout())
                    .sessionTimeout(configuration.sessionTimeout())
                    .syncClient();
            return new OxiaJavaClientMetadataStore(configuration, client, clock);
        } catch (OxiaException e) {
            throw new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE, true, "failed to create Oxia metadata client", e);
        }
    }

    OxiaJavaClientMetadataStore(
            OxiaClientConfiguration configuration,
            SyncOxiaClient oxiaClient,
            Clock clock) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.oxiaClient = Objects.requireNonNull(oxiaClient, "oxiaClient");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.operationExecutor = boundedExecutor(
                4, configuration.maxPendingOperations(), "nereus-oxia-operation");
        this.clientExecutor = Executors.newFixedThreadPool(8, daemonFactory("nereus-oxia-client"));
        this.watchExecutor = boundedExecutor(
                2, configuration.maxPendingOperations(), "nereus-oxia-watch");
        this.client = new PartitionedOxiaClient(
                new OxiaJavaClientBackend(oxiaClient, clientExecutor, watchExecutor));
    }

    @Override
    public CompletableFuture<StreamMetadataRecord> createOrGetStream(
            String cluster,
            StreamName streamName,
            StreamCreateOptions options) {
        return complete(() -> {
            Objects.requireNonNull(streamName, "streamName");
            Objects.requireNonNull(options, "options");
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            StreamId streamId = DeterministicIds.streamIdFor(streamName);
            Optional<StreamHeadRecord> existing = getHead(keyspace, streamId);
            if (existing.isPresent()) {
                validateStreamIdentity(existing.orElseThrow(), streamName, streamId);
                return existing.orElseThrow().toMetadataRecord();
            }
            long now = clock.millis();
            StreamHeadRecord candidate = new StreamHeadRecord(
                    streamId.value(),
                    streamName.value(),
                    DeterministicIds.streamNameHash(streamName),
                    StreamState.ACTIVE.name(),
                    options.profile().canonical().name(),
                    options.attributes(),
                    now,
                    0,
                    0,
                    0,
                    0,
                    0,
                    "",
                    AppendSessionSnapshotRecord.EMPTY,
                    0);
            StreamHeadRecord created;
            try {
                created = putIfAbsent(
                        keyspace.streamHeadKey(streamId),
                        keyspace.streamPartitionKey(streamId),
                        candidate,
                        StreamHeadRecord.class);
            } catch (BackendConditionException e) {
                requireCause(e, KeyAlreadyExistsException.class);
                created = getHead(keyspace, streamId).orElseThrow(() -> invariant("stream head disappeared"));
                validateStreamIdentity(created, streamName, streamId);
            }
            putStreamNameAuditBestEffort(keyspace, streamName, streamId, now);
            return created.toMetadataRecord();
        });
    }

    @Override
    public CompletableFuture<StreamMetadataRecord> getStream(String cluster, StreamId streamId) {
        return complete(() -> headOrThrow(new OxiaKeyspace(cluster), streamId).toMetadataRecord());
    }

    @Override
    public CompletableFuture<StreamMetadataSnapshot> getStreamSnapshot(String cluster, StreamId streamId) {
        return complete(() -> snapshot(headOrThrow(new OxiaKeyspace(cluster), streamId)));
    }

    @Override
    public CompletableFuture<AppendSessionRecord> acquireAppendSession(
            String cluster,
            StreamId streamId,
            AppendSessionOptions options) {
        return complete(() -> {
            Objects.requireNonNull(options, "options");
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                StreamHeadRecord head = headOrThrow(keyspace, streamId);
                requireActive(head);
                long now = clock.millis();
                AppendSessionSnapshotRecord current = head.appendSession();
                boolean empty = current.isEmpty();
                boolean expired = !empty && current.expiresAtMillis() <= now;
                boolean sameWriter = !empty && current.writerId().equals(options.writerId());
                if (!empty && !expired && !sameWriter) {
                    throw failure(ErrorCode.FENCED_APPEND, true, "append session is owned by another writer");
                }
                if (!empty && expired && !sameWriter && !options.allowStealExpiredSession()) {
                    throw failure(
                            ErrorCode.APPEND_SESSION_EXPIRED, true, "expired session steal is not allowed");
                }
                long epoch = (!empty && sameWriter && !expired) ? current.epoch() : current.epoch() + 1;
                String token = (!empty && sameWriter && !expired)
                        ? current.fencingToken()
                        : newFencingToken(streamId, options.writerId(), epoch);
                AppendSessionSnapshotRecord session = new AppendSessionSnapshotRecord(
                        options.writerId(),
                        epoch,
                        token,
                        current.leaseVersion() + 1,
                        leaseExpiration(now, options.ttl()));
                Optional<StreamHeadRecord> updated = casHead(keyspace, streamId, head, withSession(head, session));
                if (updated.isPresent()) {
                    return AppendSessionRecord.fromHead(streamId.value(), session);
                }
            }
            throw condition("append session CAS retry budget exhausted");
        });
    }

    @Override
    public CompletableFuture<AppendSessionRecord> renewAppendSession(
            String cluster,
            StreamId streamId,
            String writerId,
            long epoch,
            String fencingToken,
            Duration ttl) {
        return complete(() -> {
            Objects.requireNonNull(writerId, "writerId");
            Objects.requireNonNull(fencingToken, "fencingToken");
            Objects.requireNonNull(ttl, "ttl");
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                StreamHeadRecord head = headOrThrow(keyspace, streamId);
                requireActive(head);
                AppendSessionSnapshotRecord current = head.appendSession();
                long now = clock.millis();
                if (current.isEmpty() || current.expiresAtMillis() <= now) {
                    throw failure(ErrorCode.APPEND_SESSION_EXPIRED, true, "append session expired");
                }
                if (!current.writerId().equals(writerId)
                        || current.epoch() != epoch
                        || !current.fencingToken().equals(fencingToken)) {
                    throw failure(ErrorCode.FENCED_APPEND, true, "append session token does not match");
                }
                AppendSessionSnapshotRecord session = new AppendSessionSnapshotRecord(
                        writerId,
                        epoch,
                        fencingToken,
                        current.leaseVersion() + 1,
                        leaseExpiration(now, ttl));
                if (casHead(keyspace, streamId, head, withSession(head, session)).isPresent()) {
                    return AppendSessionRecord.fromHead(streamId.value(), session);
                }
            }
            throw condition("append session renewal CAS retry budget exhausted");
        });
    }

    @Override
    public CompletableFuture<Void> putObjectManifest(String cluster, ObjectManifestRecord manifest) {
        return complete(() -> {
            Phase1ObjectManifestValidator.validateNewUpload(manifest);
            ObjectId objectId = new ObjectId(manifest.objectId());
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            try {
                putIfAbsent(
                        keyspace.objectManifestKey(objectId),
                        keyspace.objectPartitionKey(objectId),
                        manifest,
                        ObjectManifestRecord.class);
            } catch (BackendConditionException e) {
                requireCause(e, KeyAlreadyExistsException.class);
                ObjectManifestRecord existing = getObjectManifestSync(keyspace, objectId)
                        .orElseThrow(() -> invariant("object manifest disappeared"));
                if ("DELETED".equals(existing.state())
                        || !Phase1ObjectManifestValidator.sameImmutableIdentity(existing, manifest)) {
                    throw invariant("object manifest conflict");
                }
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Optional<ObjectManifestRecord>> getObjectManifest(
            String cluster,
            ObjectId objectId) {
        return complete(() -> getObjectManifestSync(new OxiaKeyspace(cluster), objectId));
    }

    @Override
    public CompletableFuture<Optional<ObjectReferenceRecord>> getObjectReferences(
            String cluster,
            ObjectId objectId) {
        return complete(() -> getObjectReferencesSync(new OxiaKeyspace(cluster), objectId));
    }

    @Override
    public CompletableFuture<ObjectReferenceRecord> repairObjectReferences(
            String cluster,
            ObjectId objectId) {
        return complete(() -> repairObjectReferencesSync(new OxiaKeyspace(cluster), objectId));
    }

    @Override
    public CompletableFuture<CommitSliceResult> commitStreamSlice(
            String cluster,
            CommitSliceRequest request) {
        return completeAppend(() -> commitStreamSliceSync(new OxiaKeyspace(cluster), request));
    }

    @Override
    public CompletableFuture<DerivedIndexRepairResult> repairDerivedStreamIndexes(
            String cluster,
            StreamId streamId,
            long targetOffset,
            Optional<DerivedIndexRepairCursor> continuation,
            int maxCommitsToScan) {
        return complete(() -> repairDerivedStreamIndexesSync(
                new OxiaKeyspace(cluster),
                streamId,
                targetOffset,
                continuation,
                maxCommitsToScan));
    }

    @Override
    public CompletableFuture<List<OffsetIndexRecord>> scanOffsetIndex(
            String cluster,
            StreamId streamId,
            long startOffset,
            int limit) {
        return complete(() -> {
            if (startOffset < 0 || limit <= 0) {
                throw new IllegalArgumentException("startOffset must be non-negative and limit positive");
            }
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            PartitionKey partitionKey = keyspace.streamPartitionKey(streamId);
            return join(client.rangeScan(
                            keyspace.offsetIndexScanFromExclusive(streamId, startOffset),
                            keyspace.offsetIndexScanToExclusive(streamId),
                            limit,
                            partitionKey))
                    .stream()
                    .map(value -> decodeOffsetIndex(keyspace, streamId, value))
                    .filter(record -> record.offsetEnd() > startOffset)
                    .toList();
        });
    }

    @Override
    public CompletableFuture<CommittedEndOffsetRecord> getCommittedEndOffset(
            String cluster,
            StreamId streamId) {
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(new OxiaKeyspace(cluster), streamId);
            return new CommittedEndOffsetRecord(
                    streamId.value(),
                    head.committedEndOffset(),
                    head.cumulativeSize(),
                    head.commitVersion(),
                    head.metadataVersion());
        });
    }

    @Override
    public CompletableFuture<TrimRecord> updateTrim(
            String cluster,
            StreamId streamId,
            long beforeOffset,
            String reason) {
        return complete(() -> {
            if (reason == null || beforeOffset < 0) {
                throw new NereusException(ErrorCode.INVALID_ARGUMENT, false, "trim reason/range is invalid");
            }
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                StreamHeadRecord head = headOrThrow(keyspace, streamId);
                if (!StreamState.ACTIVE.name().equals(head.state())
                        && !StreamState.SEALED.name().equals(head.state())) {
                    throw failure(ErrorCode.STREAM_NOT_ACTIVE, false, "stream state does not allow trim");
                }
                if (beforeOffset < head.trimOffset() || beforeOffset > head.committedEndOffset()) {
                    throw failure(ErrorCode.INVALID_ARGUMENT, false, "trim offset is outside committed range");
                }
                StreamHeadRecord candidate = withTrim(head, beforeOffset);
                Optional<StreamHeadRecord> updated = casHead(keyspace, streamId, head, candidate);
                if (updated.isPresent()) {
                    return new TrimRecord(
                            streamId.value(),
                            beforeOffset,
                            reason,
                            clock.millis(),
                            updated.orElseThrow().metadataVersion());
                }
            }
            throw condition("trim CAS retry budget exhausted");
        });
    }

    @Override
    public CompletableFuture<TrimRecord> getTrim(String cluster, StreamId streamId) {
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(new OxiaKeyspace(cluster), streamId);
            return new TrimRecord(
                    streamId.value(), head.trimOffset(), "", clock.millis(), head.metadataVersion());
        });
    }

    @Override
    public WatchRegistration watchStream(
            String cluster,
            StreamId streamId,
            MetadataWatcher watcher) {
        Objects.requireNonNull(watcher, "watcher");
        ensureOpen();
        OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
        AtomicBoolean active = new AtomicBoolean(true);
        WatchRegistration backend = client.watchPrefix(
                keyspace.streamHeadKey(streamId),
                keyspace.streamPartitionKey(streamId),
                () -> deliverWatch(keyspace, streamId, watcher, active));
        WatchRegistration registration = () -> {
            if (active.compareAndSet(true, false)) {
                backend.close();
            }
        };
        watches.add(registration);
        return () -> {
            registration.close();
            watches.remove(registration);
        };
    }

    private void deliverWatch(
            OxiaKeyspace keyspace,
            StreamId streamId,
            MetadataWatcher watcher,
            AtomicBoolean active) {
        if (!active.get() || closed.get()) {
            return;
        }
        try {
            StreamHeadRecord head = headOrThrow(keyspace, streamId);
            watcher.onOffsetIndexUpdated(streamId, head.committedEndOffset(), head.metadataVersion());
            watcher.onTrimUpdated(streamId, head.trimOffset(), head.metadataVersion());
            if (!head.appendSession().isEmpty()) {
                watcher.onAppendSessionChanged(
                        streamId,
                        head.appendSession().epoch(),
                        head.appendSession().leaseVersion());
            }
        } catch (RuntimeException ignored) {
            // Watch delivery is an invalidation hint and never changes mutation results.
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        watches.forEach(WatchRegistration::close);
        watches.clear();
        operationExecutor.shutdown();
        watchExecutor.shutdown();
        clientExecutor.shutdown();
        try {
            oxiaClient.close();
        } catch (Exception ignored) {
            // Close is best effort after admission has stopped.
        }
    }

    private CommitSliceResult commitStreamSliceSync(OxiaKeyspace keyspace, CommitSliceRequest request) {
        Objects.requireNonNull(request, "request");
        String commitId = request.commitId();
        StreamCommitRecord replay = findMarkerReplay(keyspace, request);
        if (replay != null) {
            validateReplay(request, replay, AppendOutcome.KNOWN_COMMITTED);
            materializeForAppend(keyspace, replay);
            return commitResult(replay, request.projectionRef());
        }

        StreamHeadRecord initialHead = headOrThrow(keyspace, request.streamId());
        if (initialHead.committedEndOffset() > request.expectedStartOffset()) {
            replay = appendSearchResult(searchReplay(keyspace, request, commitId), "commit replay search");
            if (replay != null) {
                validateReplay(request, replay, AppendOutcome.KNOWN_COMMITTED);
                materializeForAppend(keyspace, replay);
                return commitResult(replay, request.projectionRef());
            }
        }
        validateCommitPreconditions(initialHead, request);
        ObjectManifestRecord manifest = getObjectManifestSync(keyspace, request.objectId())
                .orElseThrow(() -> appendFailure(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "object manifest is missing"));
        try {
            Phase1ObjectManifestValidator.validateCommitCandidate(manifest, request, false);
        } catch (NereusException e) {
            throw new NereusException(
                    e.code(), e.retriable(), e.getMessage(), e, AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        StreamCommitRecord commit = buildCommit(request, initialHead, manifest, commitId);
        String commitKey = keyspace.streamCommitKey(request.streamId(), commitId);
        try {
            commit = putIfAbsent(
                    commitKey,
                    keyspace.streamPartitionKey(request.streamId()),
                    commit,
                    StreamCommitRecord.class);
        } catch (BackendConditionException e) {
            requireCause(e, KeyAlreadyExistsException.class);
            StreamCommitRecord existing = getCommit(keyspace, request.streamId(), commitId)
                    .orElseThrow(() -> invariant("commit intent disappeared after put conflict"));
            validateReplay(request, existing, AppendOutcome.KNOWN_NOT_COMMITTED);
            commit = existing;
        }

        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            StreamHeadRecord head = headOrThrow(keyspace, request.streamId());
            if (!sameCommitAnchor(initialHead, head)) {
                replay = appendSearchResult(searchReplay(keyspace, request, commitId), "head CAS replay search");
                if (replay != null) {
                    validateReplay(request, replay, AppendOutcome.KNOWN_COMMITTED);
                    materializeForAppend(keyspace, replay);
                    return commitResult(replay, request.projectionRef());
                }
            }
            validateCommitPreconditions(head, request);
            validateCommitAgainstHead(request, commit, head, AppendOutcome.KNOWN_NOT_COMMITTED);
            StreamHeadRecord candidate = withCommit(head, commit);
            Optional<StreamHeadRecord> committed = casHeadForCommit(keyspace, request.streamId(), head, candidate);
            if (committed.isEmpty()) {
                continue;
            }
            try {
                materializeForAppend(keyspace, commit);
            } catch (NereusException e) {
                throw new NereusException(
                        e.code(), e.retriable(), e.getMessage(), e, AppendOutcome.KNOWN_COMMITTED);
            }
            updateObjectAuditBestEffort(keyspace, manifest, commit);
            return commitResult(commit, request.projectionRef());
        }
        throw appendFailure(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                AppendOutcome.KNOWN_NOT_COMMITTED,
                "stream-head CAS retry budget exhausted");
    }

    private DerivedIndexRepairResult repairDerivedStreamIndexesSync(
            OxiaKeyspace keyspace,
            StreamId streamId,
            long targetOffset,
            Optional<DerivedIndexRepairCursor> continuation,
            int maxCommitsToScan) {
        Objects.requireNonNull(continuation, "continuation");
        if (targetOffset < 0 || maxCommitsToScan <= 0) {
            throw new IllegalArgumentException("targetOffset must be non-negative and maxCommitsToScan positive");
        }
        StreamHeadRecord head = headOrThrow(keyspace, streamId);
        ChainExpectation headExpectation = expectationFromHead(head);
        continuation.ifPresent(cursor -> validateContinuation(keyspace, streamId, targetOffset, head, cursor));
        if (targetOffset >= head.committedEndOffset()) {
            return new DerivedIndexRepairResult(
                    streamId, targetOffset, targetOffset, 0, 0, true, false, Optional.empty(), head.commitVersion());
        }
        String observedHeadId = head.lastCommitId();
        long observedVersion = head.commitVersion();
        String commitId = head.lastCommitId();
        ChainExpectation expectation = headExpectation;
        if (continuation.isPresent()) {
            DerivedIndexRepairCursor cursor = continuation.orElseThrow();
            observedHeadId = cursor.observedHeadCommitId();
            observedVersion = cursor.observedCommitVersion();
            commitId = cursor.nextCommitId();
            expectation = new ChainExpectation(
                    cursor.nextOffsetEnd(), cursor.nextCumulativeSize(), cursor.nextCommitVersion());
        }
        int scanned = 0;
        int repaired = 0;
        long repairedFrom = Long.MAX_VALUE;
        long repairedTo = 0;
        boolean covered = false;
        while (!commitId.isEmpty() && scanned < maxCommitsToScan) {
            StreamCommitRecord commit = getCommit(keyspace, streamId, commitId)
                    .orElseThrow(() -> invariant("broken commit chain"));
            ChainExpectation previous = validateReachableCommit(streamId, commitId, commit, expectation);
            scanned++;
            if (materializeDerived(keyspace, commit)) {
                repaired++;
                repairedFrom = Math.min(repairedFrom, commit.offsetStart());
                repairedTo = Math.max(repairedTo, commit.offsetEnd());
            }
            if (commit.offsetStart() <= targetOffset && targetOffset < commit.offsetEnd()) {
                covered = true;
                break;
            }
            commitId = commit.previousCommitId();
            expectation = previous;
        }
        boolean exhausted = !covered && !commitId.isEmpty();
        if (!covered && !exhausted) {
            throw invariant("commit chain ended before covering the repair target");
        }
        Optional<DerivedIndexRepairCursor> next = exhausted
                ? Optional.of(new DerivedIndexRepairCursor(
                        streamId,
                        targetOffset,
                        observedHeadId,
                        observedVersion,
                        commitId,
                        expectation.offsetEnd(),
                        expectation.cumulativeSize(),
                        expectation.commitVersion()))
                : Optional.empty();
        return new DerivedIndexRepairResult(
                streamId,
                repairedFrom == Long.MAX_VALUE ? targetOffset : repairedFrom,
                repairedFrom == Long.MAX_VALUE ? targetOffset : repairedTo,
                scanned,
                repaired,
                covered,
                exhausted,
                next,
                observedVersion);
    }

    private ObjectReferenceRecord repairObjectReferencesSync(OxiaKeyspace keyspace, ObjectId objectId) {
        ObjectManifestRecord manifest = getObjectManifestSync(keyspace, objectId)
                .orElseThrow(() -> invariant("object manifest is missing"));
        Phase1ObjectManifestValidator.validateStoredManifest(manifest);
        List<VisibleSliceReferenceRecord> visible = new ArrayList<>();
        for (StreamSliceManifestRecord slice : manifest.slices()) {
            StreamCommitRecord commit = findReachableManifestCommit(keyspace, manifest, slice);
            if (commit != null) {
                materializeDerived(keyspace, commit);
                visible.add(new VisibleSliceReferenceRecord(
                        commit.streamId(), commit.sliceId(), commit.offsetStart(), commit.offsetEnd(),
                        commit.generation(), commit.commitVersion()));
            } else if ("VISIBLE".equals(slice.state())) {
                throw invariant("visible manifest slice has no matching reachable commit");
            }
        }
        visible = visible.stream()
                .sorted(Comparator.comparing(VisibleSliceReferenceRecord::streamId)
                        .thenComparingLong(VisibleSliceReferenceRecord::offsetStart)
                        .thenComparing(VisibleSliceReferenceRecord::sliceId))
                .toList();
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            Optional<ObjectReferenceRecord> existing = getObjectReferencesSync(keyspace, objectId);
            if (existing.isPresent() && !visible.containsAll(existing.orElseThrow().visibleSlices())) {
                throw invariant("object-reference repair would remove an existing visible reference");
            }
            ObjectReferenceRecord candidate = new ObjectReferenceRecord(
                    objectId.value(), visible, clock.millis(), 0);
            try {
                if (existing.isEmpty()) {
                    return putIfAbsent(
                            keyspace.objectReferencesKey(objectId),
                            keyspace.objectPartitionKey(objectId),
                            candidate,
                            ObjectReferenceRecord.class);
                }
                return putIfVersion(
                        keyspace.objectReferencesKey(objectId),
                        keyspace.objectPartitionKey(objectId),
                        candidate,
                        existing.orElseThrow().metadataVersion(),
                        ObjectReferenceRecord.class);
            } catch (BackendConditionException e) {
                if (!(e.getCause() instanceof KeyAlreadyExistsException)
                        && !(e.getCause() instanceof UnexpectedVersionIdException)) {
                    throw e;
                }
            }
        }
        throw condition("object-reference repair CAS retry budget exhausted");
    }

    private StreamCommitRecord buildCommit(
            CommitSliceRequest request,
            StreamHeadRecord head,
            ObjectManifestRecord manifest,
            String commitId) {
        try {
            return new StreamCommitRecord(
                    request.streamId().value(),
                    commitId,
                    head.lastCommitId(),
                    request.expectedStartOffset(),
                    Math.addExact(request.expectedStartOffset(), request.recordCount()),
                    0,
                    Math.addExact(head.cumulativeSize(), request.logicalBytes()),
                    Math.addExact(head.commitVersion(), 1),
                    request.writerId(),
                    request.writerRunIdHash(),
                    request.epoch(),
                    request.fencingTokenHash(),
                    request.objectId().value(),
                    request.objectKey().value(),
                    request.sliceId(),
                    manifest.objectType(),
                    "WAL_OBJECT_V1",
                    "OPAQUE_SLICE",
                    request.payloadFormat().name(),
                    request.objectChecksum().type().name(),
                    request.objectChecksum().value(),
                    request.objectOffset(),
                    request.objectLength(),
                    request.recordCount(),
                    request.entryCount(),
                    request.logicalBytes(),
                    request.schemaRefs(),
                    EntryIndexReferenceRecord.fromApi(request.entryIndexRef()),
                    request.projectionIdentity(),
                    request.sliceChecksum().type().name(),
                    request.sliceChecksum().value(),
                    request.minEventTimeMillis(),
                    request.maxEventTimeMillis(),
                    clock.millis(),
                    0);
        } catch (ArithmeticException e) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "commit head-derived fields overflow",
                    e,
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
    }

    private Optional<StreamHeadRecord> casHeadForCommit(
            OxiaKeyspace keyspace,
            StreamId streamId,
            StreamHeadRecord expected,
            StreamHeadRecord candidate) {
        try {
            return casHead(keyspace, streamId, expected, candidate);
        } catch (NereusException e) {
            throw new NereusException(
                    e.code(), e.retriable(), e.getMessage(), e, AppendOutcome.MAY_HAVE_COMMITTED);
        } catch (RuntimeException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof UnexpectedVersionIdException) {
                return Optional.empty();
            }
            throw new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "stream-head CAS result is unknown",
                    cause,
                    AppendOutcome.MAY_HAVE_COMMITTED);
        }
    }

    private void validateCommitPreconditions(StreamHeadRecord head, CommitSliceRequest request) {
        if (!StreamState.ACTIVE.name().equals(head.state())) {
            throw appendFailure(
                    ErrorCode.STREAM_NOT_ACTIVE,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "stream is not active");
        }
        AppendSessionSnapshotRecord session = head.appendSession();
        if (session.isEmpty() || session.expiresAtMillis() <= clock.millis()) {
            throw appendFailure(
                    ErrorCode.APPEND_SESSION_EXPIRED,
                    true,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "append session expired");
        }
        if (!session.writerId().equals(request.writerId())
                || session.epoch() != request.epoch()
                || !session.fencingToken().equals(request.fencingToken())) {
            throw appendFailure(
                    ErrorCode.FENCED_APPEND,
                    true,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "append session token does not match");
        }
        if (head.committedEndOffset() != request.expectedStartOffset()) {
            throw appendFailure(
                    ErrorCode.OFFSET_CONFLICT,
                    true,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "expected start offset does not match committed end");
        }
    }

    private void validateReplay(
            CommitSliceRequest request,
            StreamCommitRecord replay,
            AppendOutcome outcome) {
        if (!replay.commitId().equals(request.commitId())
                || !replay.streamId().equals(request.streamId().value())
                || !replay.writerId().equals(request.writerId())
                || !replay.writerRunIdHash().equals(request.writerRunIdHash())
                || replay.writerEpoch() != request.epoch()
                || !replay.fencingTokenHash().equals(request.fencingTokenHash())
                || replay.offsetStart() != request.expectedStartOffset()
                || replay.offsetEnd() != expectedEnd(request)
                || replay.generation() != 0
                || replay.recordCount() != request.recordCount()
                || replay.entryCount() != request.entryCount()
                || replay.logicalBytes() != request.logicalBytes()
                || !replay.objectId().equals(request.objectId().value())
                || !replay.objectKey().equals(request.objectKey().value())
                || !replay.objectType().equals(ObjectType.MULTI_STREAM_WAL_OBJECT.name())
                || !replay.physicalFormat().equals("WAL_OBJECT_V1")
                || !replay.logicalFormat().equals("OPAQUE_SLICE")
                || !replay.objectChecksumType().equals(request.objectChecksum().type().name())
                || !replay.objectChecksumValue().equals(request.objectChecksum().value())
                || replay.objectOffset() != request.objectOffset()
                || replay.objectLength() != request.objectLength()
                || !replay.sliceId().equals(request.sliceId())
                || !replay.payloadFormat().equals(request.payloadFormat().name())
                || !replay.schemaRefs().equals(request.schemaRefs())
                || !replay.entryIndexRef().equals(EntryIndexReferenceRecord.fromApi(request.entryIndexRef()))
                || !replay.projectionRef().equals(request.projectionIdentity())
                || !replay.sliceChecksumType().equals(request.sliceChecksum().type().name())
                || !replay.sliceChecksumValue().equals(request.sliceChecksum().value())
                || replay.minEventTimeMillis() != request.minEventTimeMillis()
                || replay.maxEventTimeMillis() != request.maxEventTimeMillis()) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION, false, outcome, "replayed commit does not match request");
        }
    }

    private void validateCommitAgainstHead(
            CommitSliceRequest request,
            StreamCommitRecord commit,
            StreamHeadRecord head,
            AppendOutcome outcome) {
        long end;
        long cumulative;
        long version;
        try {
            end = Math.addExact(request.expectedStartOffset(), request.recordCount());
            cumulative = Math.addExact(head.cumulativeSize(), request.logicalBytes());
            version = Math.addExact(head.commitVersion(), 1);
        } catch (ArithmeticException e) {
            throw new NereusException(ErrorCode.INVALID_ARGUMENT, false, "commit fields overflow", e, outcome);
        }
        if (!commit.previousCommitId().equals(head.lastCommitId())
                || commit.offsetStart() != request.expectedStartOffset()
                || commit.offsetEnd() != end
                || commit.cumulativeSize() != cumulative
                || commit.commitVersion() != version) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    outcome,
                    "stored commit does not match current head snapshot");
        }
    }

    private StreamCommitRecord findMarkerReplay(OxiaKeyspace keyspace, CommitSliceRequest request) {
        Optional<CommittedSliceRecord> marker = getRecord(
                keyspace.committedSliceKey(request.streamId(), request.objectId(), request.sliceId()),
                keyspace.streamPartitionKey(request.streamId()),
                CommittedSliceRecord.class);
        if (marker.isEmpty()) {
            return null;
        }
        CommittedSliceRecord value = marker.orElseThrow();
        if (!value.streamId().equals(request.streamId().value())
                || !value.objectId().equals(request.objectId().value())
                || !value.sliceId().equals(request.sliceId())
                || value.offsetStart() != request.expectedStartOffset()
                || value.offsetEnd() != expectedEnd(request)) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    "committed-slice marker does not match request");
        }
        StreamCommitRecord commit = getCommit(keyspace, request.streamId(), request.commitId())
                .orElseThrow(() -> appendFailure(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        AppendOutcome.MAY_HAVE_COMMITTED,
                        "committed marker has no commit record"));
        StreamHeadRecord head = headOrThrow(keyspace, request.streamId());
        if (!commitMatchesMarker(commit, value)
                || head.commitVersion() < value.commitVersion()
                || head.committedEndOffset() < value.offsetEnd()) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    "committed marker has no reachable head proof");
        }
        return commit;
    }

    private SearchResult searchReplay(
            OxiaKeyspace keyspace,
            CommitSliceRequest request,
            String commitId) {
        return searchReachable(
                keyspace,
                request.streamId(),
                commit -> commit.commitId().equals(commitId),
                commit -> commit.offsetStart() <= request.expectedStartOffset());
    }

    private SearchResult searchReachable(
            OxiaKeyspace keyspace,
            StreamId streamId,
            Predicate<StreamCommitRecord> predicate,
            Predicate<StreamCommitRecord> provesNotFound) {
        Optional<StreamHeadRecord> optionalHead = getHead(keyspace, streamId);
        if (optionalHead.isEmpty()) {
            return new SearchResult(SearchStatus.NOT_FOUND, null, 0);
        }
        StreamHeadRecord head = optionalHead.orElseThrow();
        ChainExpectation expectation;
        try {
            expectation = expectationFromHead(head);
        } catch (NereusException e) {
            return new SearchResult(SearchStatus.BROKEN, null, 0);
        }
        String current = head.lastCommitId();
        int scanned = 0;
        while (!current.isEmpty() && scanned < configuration.maxCommitChainScan()) {
            Optional<StreamCommitRecord> optionalCommit = getCommit(keyspace, streamId, current);
            if (optionalCommit.isEmpty()) {
                return new SearchResult(SearchStatus.BROKEN, null, scanned);
            }
            StreamCommitRecord commit = optionalCommit.orElseThrow();
            try {
                expectation = validateReachableCommit(streamId, current, commit, expectation);
            } catch (NereusException e) {
                return new SearchResult(SearchStatus.BROKEN, null, scanned);
            }
            scanned++;
            if (predicate.test(commit)) {
                return new SearchResult(SearchStatus.FOUND, commit, scanned);
            }
            if (provesNotFound.test(commit)) {
                return new SearchResult(SearchStatus.NOT_FOUND, null, scanned);
            }
            current = commit.previousCommitId();
        }
        if (current.isEmpty() && !isGenesis(expectation)) {
            return new SearchResult(SearchStatus.BROKEN, null, scanned);
        }
        return new SearchResult(
                current.isEmpty() ? SearchStatus.NOT_FOUND : SearchStatus.EXHAUSTED,
                null,
                scanned);
    }

    private StreamCommitRecord appendSearchResult(SearchResult result, String operation) {
        return switch (result.status()) {
            case FOUND -> result.commit();
            case NOT_FOUND -> null;
            case EXHAUSTED -> throw appendFailure(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    operation + " exhausted the commit-chain scan budget");
            case BROKEN -> throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    operation + " found a broken commit chain");
        };
    }

    private ChainExpectation expectationFromHead(StreamHeadRecord head) {
        boolean empty = head.lastCommitId().isEmpty();
        if (empty != (head.commitVersion() == 0)
                || empty != (head.committedEndOffset() == 0)
                || (empty && head.cumulativeSize() != 0)) {
            throw invariant("stream head commit anchor is invalid");
        }
        return new ChainExpectation(head.committedEndOffset(), head.cumulativeSize(), head.commitVersion());
    }

    private ChainExpectation validateReachableCommit(
            StreamId streamId,
            String expectedId,
            StreamCommitRecord commit,
            ChainExpectation expectation) {
        long calculatedEnd;
        try {
            calculatedEnd = Math.addExact(commit.offsetStart(), commit.recordCount());
        } catch (ArithmeticException e) {
            throw invariant("commit logical range overflows");
        }
        if (!commit.streamId().equals(streamId.value())
                || !commit.commitId().equals(expectedId)
                || commit.generation() != 0
                || commit.offsetEnd() != calculatedEnd
                || commit.offsetEnd() != expectation.offsetEnd()
                || commit.cumulativeSize() != expectation.cumulativeSize()
                || commit.commitVersion() != expectation.commitVersion()
                || !ObjectType.MULTI_STREAM_WAL_OBJECT.name().equals(commit.objectType())
                || !"WAL_OBJECT_V1".equals(commit.physicalFormat())
                || !"OPAQUE_SLICE".equals(commit.logicalFormat())) {
            throw invariant("commit chain record is inconsistent");
        }
        long previousSize = commit.cumulativeSize() - commit.logicalBytes();
        long previousVersion = commit.commitVersion() - 1;
        if (previousSize < 0
                || (commit.previousCommitId().isEmpty()
                        && (commit.offsetStart() != 0 || previousSize != 0 || previousVersion != 0))
                || (!commit.previousCommitId().isEmpty()
                        && (commit.offsetStart() == 0 || previousVersion <= 0))) {
            throw invariant("commit chain predecessor is inconsistent");
        }
        return new ChainExpectation(commit.offsetStart(), previousSize, previousVersion);
    }

    private void validateContinuation(
            OxiaKeyspace keyspace,
            StreamId streamId,
            long targetOffset,
            StreamHeadRecord currentHead,
            DerivedIndexRepairCursor cursor) {
        if (!cursor.streamId().equals(streamId)
                || cursor.targetOffset() != targetOffset
                || cursor.observedCommitVersion() > currentHead.commitVersion()) {
            throw invariant("repair continuation does not match current stream head");
        }
        StreamCommitRecord observed = getCommit(keyspace, streamId, cursor.observedHeadCommitId())
                .orElseThrow(() -> invariant("repair continuation observed head is missing"));
        if (observed.commitVersion() != cursor.observedCommitVersion()
                || cursor.nextOffsetEnd() > observed.offsetStart()
                || cursor.nextCumulativeSize() > observed.cumulativeSize()
                || cursor.nextCommitVersion() >= observed.commitVersion()) {
            throw invariant("repair continuation position is invalid");
        }
        validateReachableCommit(
                streamId,
                cursor.observedHeadCommitId(),
                observed,
                new ChainExpectation(observed.offsetEnd(), observed.cumulativeSize(), observed.commitVersion()));
    }

    private boolean materializeDerived(OxiaKeyspace keyspace, StreamCommitRecord commit) {
        boolean repaired = false;
        StreamId streamId = new StreamId(commit.streamId());
        ObjectId objectId = new ObjectId(commit.objectId());
        OffsetIndexRecord index = toOffsetIndex(commit);
        String indexKey = keyspace.offsetIndexKey(streamId, commit.offsetEnd(), commit.generation());
        try {
            putIfAbsent(indexKey, keyspace.streamPartitionKey(streamId), index, OffsetIndexRecord.class);
            repaired = true;
        } catch (BackendConditionException e) {
            requireCause(e, KeyAlreadyExistsException.class);
            OffsetIndexRecord existing = getRecord(
                            indexKey, keyspace.streamPartitionKey(streamId), OffsetIndexRecord.class)
                    .orElseThrow(() -> invariant("offset index disappeared after put conflict"));
            if (!hydrate(existing, 0, OffsetIndexRecord.class).equals(hydrate(index, 0, OffsetIndexRecord.class))) {
                throw invariant("offset index conflict");
            }
        }
        CommittedSliceRecord marker = new CommittedSliceRecord(
                commit.streamId(), commit.objectId(), commit.sliceId(), commit.offsetStart(), commit.offsetEnd(),
                commit.generation(), commit.commitVersion(), 0);
        String markerKey = keyspace.committedSliceKey(streamId, objectId, commit.sliceId());
        try {
            putIfAbsent(markerKey, keyspace.streamPartitionKey(streamId), marker, CommittedSliceRecord.class);
            repaired = true;
        } catch (BackendConditionException e) {
            requireCause(e, KeyAlreadyExistsException.class);
            CommittedSliceRecord existing = getRecord(
                            markerKey, keyspace.streamPartitionKey(streamId), CommittedSliceRecord.class)
                    .orElseThrow(() -> invariant("committed marker disappeared after put conflict"));
            if (!hydrate(existing, 0, CommittedSliceRecord.class)
                    .equals(hydrate(marker, 0, CommittedSliceRecord.class))) {
                throw invariant("committed slice conflict");
            }
        }
        return repaired;
    }

    private void materializeForAppend(OxiaKeyspace keyspace, StreamCommitRecord commit) {
        try {
            materializeDerived(keyspace, commit);
        } catch (Throwable error) {
            throw normalize(error, Optional.of(AppendOutcome.KNOWN_COMMITTED));
        }
    }

    private OffsetIndexRecord toOffsetIndex(StreamCommitRecord commit) {
        return new OffsetIndexRecord(
                commit.streamId(), commit.offsetStart(), commit.offsetEnd(), commit.generation(),
                commit.cumulativeSize(), commit.objectId(), commit.objectKey(), commit.sliceId(), commit.objectType(),
                commit.physicalFormat(), commit.logicalFormat(), commit.payloadFormat(), commit.objectOffset(),
                commit.objectLength(), commit.recordCount(), commit.entryCount(), commit.logicalBytes(),
                commit.schemaRefs(), commit.entryIndexRef(), commit.projectionRef(), commit.sliceChecksumType(),
                commit.sliceChecksumValue(), commit.minEventTimeMillis(), commit.maxEventTimeMillis(),
                commit.commitVersion(), false, 0);
    }

    private StreamCommitRecord findReachableManifestCommit(
            OxiaKeyspace keyspace,
            ObjectManifestRecord manifest,
            StreamSliceManifestRecord slice) {
        StreamId streamId = new StreamId(slice.streamId());
        SearchResult result = searchReachable(
                keyspace,
                streamId,
                commit -> commit.objectId().equals(manifest.objectId())
                        && commit.sliceId().equals(slice.sliceId()),
                commit -> false);
        return switch (result.status()) {
            case FOUND -> {
                if (!commitMatchesManifest(manifest, slice, result.commit())) {
                    throw invariant("reachable commit conflicts with object manifest slice");
                }
                yield result.commit();
            }
            case NOT_FOUND -> null;
            case EXHAUSTED -> throw failure(
                    ErrorCode.METADATA_UNAVAILABLE, true, "object repair exhausted commit-chain scan budget");
            case BROKEN -> throw invariant("object repair found a broken commit chain");
        };
    }

    private void updateObjectAuditBestEffort(
            OxiaKeyspace keyspace,
            ObjectManifestRecord original,
            StreamCommitRecord commit) {
        try {
            ObjectId objectId = new ObjectId(original.objectId());
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                ObjectManifestRecord current = getObjectManifestSync(keyspace, objectId).orElseThrow();
                List<StreamSliceManifestRecord> slices = current.slices().stream()
                        .map(slice -> slice.streamId().equals(commit.streamId())
                                        && slice.sliceId().equals(commit.sliceId())
                                ? slice.withState("VISIBLE")
                                : slice)
                        .toList();
                String state = slices.stream().allMatch(slice -> "VISIBLE".equals(slice.state()))
                        ? "VISIBLE"
                        : "PARTIALLY_VISIBLE";
                try {
                    putIfVersion(
                            keyspace.objectManifestKey(objectId),
                            keyspace.objectPartitionKey(objectId),
                            current.withStateAndSlices(state, slices, 0),
                            current.metadataVersion(),
                            ObjectManifestRecord.class);
                    break;
                } catch (BackendConditionException e) {
                    requireCause(e, UnexpectedVersionIdException.class);
                }
            }
            repairObjectReferencesSync(keyspace, objectId);
        } catch (RuntimeException ignored) {
            // Object audit is repairable and cannot weaken a committed append result.
        }
    }

    private Optional<StreamCommitRecord> getCommit(
            OxiaKeyspace keyspace,
            StreamId streamId,
            String commitId) {
        Optional<StreamCommitRecord> result = getRecord(
                keyspace.streamCommitKey(streamId, commitId),
                keyspace.streamPartitionKey(streamId),
                StreamCommitRecord.class);
        result.ifPresent(commit -> {
            if (!commit.streamId().equals(streamId.value()) || !commit.commitId().equals(commitId)) {
                throw invariant("stream commit key/value identity mismatch");
            }
        });
        return result;
    }

    private static CommitSliceResult commitResult(
            StreamCommitRecord commit,
            Optional<ProjectionRef> projection) {
        return new CommitSliceResult(
                new StreamId(commit.streamId()),
                new OffsetRange(commit.offsetStart(), commit.offsetEnd()),
                commit.offsetEnd(),
                commit.generation(),
                commit.commitVersion(),
                projection);
    }

    private static boolean sameCommitAnchor(StreamHeadRecord left, StreamHeadRecord right) {
        return left.committedEndOffset() == right.committedEndOffset()
                && left.cumulativeSize() == right.cumulativeSize()
                && left.commitVersion() == right.commitVersion()
                && left.lastCommitId().equals(right.lastCommitId());
    }

    private static boolean commitMatchesMarker(StreamCommitRecord commit, CommittedSliceRecord marker) {
        return commit.streamId().equals(marker.streamId())
                && commit.objectId().equals(marker.objectId())
                && commit.sliceId().equals(marker.sliceId())
                && commit.offsetStart() == marker.offsetStart()
                && commit.offsetEnd() == marker.offsetEnd()
                && commit.generation() == marker.generation()
                && commit.commitVersion() == marker.commitVersion();
    }

    private static boolean commitMatchesManifest(
            ObjectManifestRecord manifest,
            StreamSliceManifestRecord slice,
            StreamCommitRecord commit) {
        return commit.streamId().equals(slice.streamId())
                && commit.objectId().equals(manifest.objectId())
                && commit.objectKey().equals(manifest.objectKey())
                && commit.objectType().equals(manifest.objectType())
                && commit.objectChecksumType().equals(manifest.objectChecksumType())
                && commit.objectChecksumValue().equals(manifest.objectChecksumValue())
                && commit.writerId().equals(manifest.writerId())
                && commit.writerRunIdHash().equals(manifest.writerRunIdHash())
                && commit.writerEpoch() == manifest.writerEpoch()
                && commit.sliceId().equals(slice.sliceId())
                && commit.writerEpoch() == slice.writerEpoch()
                && commit.objectOffset() == slice.objectOffset()
                && commit.objectLength() == slice.objectLength()
                && commit.recordCount() == slice.recordCount()
                && commit.entryCount() == slice.entryCount()
                && commit.logicalBytes() == slice.logicalBytes()
                && commit.schemaRefs().equals(slice.schemaRefs())
                && commit.entryIndexRef().equals(slice.entryIndexRef())
                && commit.sliceChecksumType().equals(slice.sliceChecksumType())
                && commit.sliceChecksumValue().equals(slice.sliceChecksumValue())
                && commit.payloadFormat().equals(slice.payloadFormat());
    }

    private static boolean isGenesis(ChainExpectation expectation) {
        return expectation.offsetEnd() == 0
                && expectation.cumulativeSize() == 0
                && expectation.commitVersion() == 0;
    }

    private static long expectedEnd(CommitSliceRequest request) {
        try {
            return Math.addExact(request.expectedStartOffset(), request.recordCount());
        } catch (ArithmeticException e) {
            throw appendFailure(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "commit offset range overflows");
        }
    }

    private static NereusException appendFailure(
            ErrorCode code,
            boolean retriable,
            AppendOutcome outcome,
            String message) {
        return new NereusException(code, retriable, message, outcome);
    }

    private Optional<ObjectManifestRecord> getObjectManifestSync(OxiaKeyspace keyspace, ObjectId objectId) {
        Optional<ObjectManifestRecord> result = getRecord(
                keyspace.objectManifestKey(objectId),
                keyspace.objectPartitionKey(objectId),
                ObjectManifestRecord.class);
        result.ifPresent(record -> {
            if (!record.objectId().equals(objectId.value())) {
                throw invariant("object manifest key/value identity mismatch");
            }
        });
        return result;
    }

    private Optional<StreamHeadRecord> getHead(OxiaKeyspace keyspace, StreamId streamId) {
        Optional<StreamHeadRecord> result = getRecord(
                keyspace.streamHeadKey(streamId),
                keyspace.streamPartitionKey(streamId),
                StreamHeadRecord.class);
        result.ifPresent(head -> {
            StreamName streamName;
            try {
                streamName = new StreamName(head.streamName());
            } catch (RuntimeException e) {
                throw invariant("stream head contains an invalid stream name");
            }
            if (!head.streamId().equals(streamId.value())
                    || !DeterministicIds.streamIdFor(streamName).equals(streamId)
                    || !head.streamNameHash().equals(DeterministicIds.streamNameHash(streamName))) {
                throw invariant("stream head key/value identity mismatch");
            }
        });
        return result;
    }

    private Optional<ObjectReferenceRecord> getObjectReferencesSync(
            OxiaKeyspace keyspace,
            ObjectId objectId) {
        Optional<ObjectReferenceRecord> result = getRecord(
                keyspace.objectReferencesKey(objectId),
                keyspace.objectPartitionKey(objectId),
                ObjectReferenceRecord.class);
        result.ifPresent(record -> {
            if (!record.objectId().equals(objectId.value())) {
                throw invariant("object reference key/value identity mismatch");
            }
        });
        return result;
    }

    private OffsetIndexRecord decodeOffsetIndex(
            OxiaKeyspace keyspace,
            StreamId streamId,
            PartitionedOxiaClient.VersionedValue value) {
        OffsetIndexRecord record = decode(value, OffsetIndexRecord.class);
        String expectedKey = keyspace.offsetIndexKey(
                streamId, record.offsetEnd(), record.generation());
        if (!record.streamId().equals(streamId.value()) || !value.key().equals(expectedKey)) {
            throw invariant("offset index key/value identity mismatch");
        }
        return record;
    }

    private StreamMetadataSnapshot snapshot(StreamHeadRecord head) {
        long observedAtMillis = clock.millis();
        return new StreamMetadataSnapshot(
                head.toMetadataRecord(),
                new CommittedEndOffsetRecord(
                        head.streamId(),
                        head.committedEndOffset(),
                        head.cumulativeSize(),
                        head.commitVersion(),
                        head.metadataVersion()),
                new TrimRecord(
                        head.streamId(),
                        head.trimOffset(),
                        "",
                        observedAtMillis,
                        head.metadataVersion()));
    }

    private StreamHeadRecord headOrThrow(OxiaKeyspace keyspace, StreamId streamId) {
        return getHead(keyspace, streamId)
                .orElseThrow(() -> failure(ErrorCode.STREAM_NOT_FOUND, false, "stream does not exist"));
    }

    private <T> Optional<T> getRecord(
            String key,
            PartitionKey partitionKey,
            Class<T> recordClass) {
        return join(client.get(key, partitionKey)).map(value -> decode(value, recordClass));
    }

    private <T> T putIfAbsent(
            String key,
            PartitionKey partitionKey,
            T record,
            Class<T> recordClass) {
        PartitionedOxiaClient.WriteResult result = join(client.putIfAbsent(
                key, encode(record, recordClass), partitionKey));
        return hydrate(record, result.version(), recordClass);
    }

    private <T> T putIfVersion(
            String key,
            PartitionKey partitionKey,
            T record,
            long expectedVersion,
            Class<T> recordClass) {
        PartitionedOxiaClient.WriteResult result = join(client.putIfVersion(
                key, encode(record, recordClass), expectedVersion, partitionKey));
        return hydrate(record, result.version(), recordClass);
    }

    private Optional<StreamHeadRecord> casHead(
            OxiaKeyspace keyspace,
            StreamId streamId,
            StreamHeadRecord expected,
            StreamHeadRecord candidate) {
        try {
            return Optional.of(putIfVersion(
                    keyspace.streamHeadKey(streamId),
                    keyspace.streamPartitionKey(streamId),
                    candidate,
                    expected.metadataVersion(),
                    StreamHeadRecord.class));
        } catch (BackendConditionException e) {
            requireCause(e, UnexpectedVersionIdException.class);
            return Optional.empty();
        }
    }

    private <T> T decode(PartitionedOxiaClient.VersionedValue value, Class<T> recordClass) {
        T decoded = Phase1MetadataCodecs.decodeEnvelope(value.value(), recordClass);
        return hydrate(decoded, value.version(), recordClass);
    }

    private <T> byte[] encode(T record, Class<T> recordClass) {
        return Phase1MetadataCodecs.encodeEnvelope(hydrate(record, 0, recordClass), recordClass);
    }

    @SuppressWarnings("unchecked")
    private static <T> T hydrate(T record, long version, Class<T> recordClass) {
        Object hydrated;
        if (record instanceof StreamHeadRecord value) {
            hydrated = new StreamHeadRecord(
                    value.streamId(), value.streamName(), value.streamNameHash(), value.state(), value.profile(),
                    value.attributes(), value.createdAtMillis(), value.policyVersion(), value.committedEndOffset(),
                    value.cumulativeSize(), value.commitVersion(), value.trimOffset(), value.lastCommitId(),
                    value.appendSession(), version);
        } else if (record instanceof StreamCommitRecord value) {
            hydrated = copyCommit(value, version);
        } else if (record instanceof ObjectManifestRecord value) {
            hydrated = new ObjectManifestRecord(
                    value.objectId(), value.objectKey(), value.objectType(), value.state(), value.formatMajorVersion(),
                    value.formatMinorVersion(), value.writerVersion(), value.writerId(), value.writerRunIdHash(),
                    value.writerEpoch(), value.createdAtMillis(), value.uploadedAtMillis(), value.objectLength(),
                    value.objectChecksumType(), value.objectChecksumValue(), value.storageChecksumType(),
                    value.storageChecksumValue(), value.slices(), value.orphanExpiresAtMillis(), version);
        } else if (record instanceof ObjectReferenceRecord value) {
            hydrated = new ObjectReferenceRecord(value.objectId(), value.visibleSlices(), value.updatedAtMillis(), version);
        } else if (record instanceof OffsetIndexRecord value) {
            hydrated = copyOffsetIndex(value, version);
        } else if (record instanceof CommittedSliceRecord value) {
            hydrated = new CommittedSliceRecord(
                    value.streamId(), value.objectId(), value.sliceId(), value.offsetStart(), value.offsetEnd(),
                    value.generation(), value.commitVersion(), version);
        } else if (record instanceof StreamNameRecord value) {
            hydrated = new StreamNameRecord(
                    value.streamName(), value.streamId(), value.streamNameHash(), value.createdAtMillis(), version);
        } else {
            throw new IllegalArgumentException("record type does not carry metadataVersion: " + recordClass.getName());
        }
        return (T) hydrated;
    }

    private void putStreamNameAuditBestEffort(
            OxiaKeyspace keyspace,
            StreamName streamName,
            StreamId streamId,
            long createdAtMillis) {
        try {
            putIfAbsent(
                    keyspace.streamNameKey(streamName),
                    keyspace.streamPartitionKey(streamId),
                    new StreamNameRecord(
                            streamName.value(), streamId.value(), DeterministicIds.streamNameHash(streamName),
                            createdAtMillis, 0),
                    StreamNameRecord.class);
        } catch (RuntimeException ignored) {
            // The deterministic stream head remains authoritative.
        }
    }

    private <T> CompletableFuture<T> complete(Callable<T> operation) {
        if (closed.get()) {
            return NereusException.failedFuture(ErrorCode.STORAGE_CLOSED, false, "metadata store is closed");
        }
        try {
            return CompletableFuture.supplyAsync(() -> {
                ensureOpen();
                try {
                    return operation.call();
                } catch (Throwable error) {
                    throw normalize(error, Optional.empty());
                }
            }, operationExecutor);
        } catch (RejectedExecutionException e) {
            if (!closed.get()) {
                return NereusException.failedFuture(
                        ErrorCode.BACKPRESSURE_REJECTED, true, "metadata operation queue is full");
            }
            return NereusException.failedFuture(
                    ErrorCode.STORAGE_CLOSED, false, "metadata store is closing");
        }
    }

    private <T> CompletableFuture<T> completeAppend(Callable<T> operation) {
        if (closed.get()) {
            return NereusException.failedAppendFuture(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "metadata store is closed");
        }
        try {
            return CompletableFuture.supplyAsync(() -> {
                ensureOpen();
                try {
                    return operation.call();
                } catch (Throwable error) {
                    throw normalize(error, Optional.of(AppendOutcome.KNOWN_NOT_COMMITTED));
                }
            }, operationExecutor);
        } catch (RejectedExecutionException e) {
            if (!closed.get()) {
                return NereusException.failedAppendFuture(
                        ErrorCode.BACKPRESSURE_REJECTED,
                        true,
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "metadata operation queue is full");
            }
            return NereusException.failedAppendFuture(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "metadata store is closing");
        }
    }

    private static NereusException normalize(Throwable error, Optional<AppendOutcome> fallback) {
        Throwable cause = unwrap(error);
        if (cause instanceof NereusException nereus) {
            if (fallback.isPresent() && nereus.appendOutcome().isEmpty()) {
                return new NereusException(
                        nereus.code(), nereus.retriable(), nereus.getMessage(), nereus, fallback.orElseThrow());
            }
            return nereus;
        }
        ErrorCode code;
        boolean retriable;
        if (cause instanceof UnexpectedVersionIdException || cause instanceof KeyAlreadyExistsException) {
            code = ErrorCode.METADATA_CONDITION_FAILED;
            retriable = true;
        } else if (cause instanceof MetadataCodecException) {
            code = ErrorCode.METADATA_INVARIANT_VIOLATION;
            retriable = false;
        } else if (cause instanceof IllegalArgumentException) {
            code = ErrorCode.INVALID_ARGUMENT;
            retriable = false;
        } else {
            code = ErrorCode.METADATA_UNAVAILABLE;
            retriable = true;
        }
        String message = "Oxia metadata operation failed: " + cause.getClass().getSimpleName();
        return fallback.<NereusException>map(outcome -> new NereusException(
                        code, retriable, message, cause, outcome))
                .orElseGet(() -> new NereusException(code, retriable, message, cause));
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw failure(ErrorCode.STORAGE_CLOSED, false, "metadata store is closed");
        }
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof BackendConditionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof UnexpectedVersionIdException || cause instanceof KeyAlreadyExistsException) {
                throw new BackendConditionException(cause);
            }
            throw e;
        }
    }

    private static void requireCause(BackendConditionException error, Class<? extends Throwable> expected) {
        if (!expected.isInstance(error.getCause())) {
            throw error;
        }
    }

    private static void validateStreamIdentity(StreamHeadRecord head, StreamName name, StreamId streamId) {
        if (!head.streamId().equals(streamId.value()) || !head.streamName().equals(name.value())) {
            throw invariant("deterministic stream id collision");
        }
    }

    private static void requireActive(StreamHeadRecord head) {
        if (!StreamState.ACTIVE.name().equals(head.state())) {
            throw failure(ErrorCode.STREAM_NOT_ACTIVE, false, "stream is not active");
        }
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message) {
        return new NereusException(code, retriable, message);
    }

    private static NereusException invariant(String message) {
        return failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException condition(String message) {
        return failure(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static long leaseExpiration(long now, Duration ttl) {
        try {
            long millis = ttl.toMillis();
            if (millis <= 0) {
                throw new ArithmeticException("ttl below millisecond resolution");
            }
            return Math.addExact(now, millis);
        } catch (ArithmeticException e) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "append session ttl is outside millisecond range");
        }
    }

    private String newFencingToken(StreamId streamId, String writerId, long epoch) {
        return DeterministicIds.stableHashComponent(
                streamId.value() + "\0" + writerId + "\0" + epoch + "\0" + clock.millis()
                        + "\0" + java.util.UUID.randomUUID());
    }

    private static StreamHeadRecord withSession(
            StreamHeadRecord head,
            AppendSessionSnapshotRecord session) {
        return copyHead(head, head.committedEndOffset(), head.cumulativeSize(), head.commitVersion(),
                head.trimOffset(), head.lastCommitId(), session);
    }

    private static StreamHeadRecord withTrim(StreamHeadRecord head, long trimOffset) {
        return copyHead(head, head.committedEndOffset(), head.cumulativeSize(), head.commitVersion(),
                trimOffset, head.lastCommitId(), head.appendSession());
    }

    private static StreamHeadRecord withCommit(StreamHeadRecord head, StreamCommitRecord commit) {
        return copyHead(head, commit.offsetEnd(), commit.cumulativeSize(), commit.commitVersion(),
                head.trimOffset(), commit.commitId(), head.appendSession());
    }

    private static StreamHeadRecord copyHead(
            StreamHeadRecord head,
            long committedEndOffset,
            long cumulativeSize,
            long commitVersion,
            long trimOffset,
            String lastCommitId,
            AppendSessionSnapshotRecord session) {
        return new StreamHeadRecord(
                head.streamId(), head.streamName(), head.streamNameHash(), head.state(), head.profile(),
                head.attributes(), head.createdAtMillis(), head.policyVersion(), committedEndOffset,
                cumulativeSize, commitVersion, trimOffset, lastCommitId, session, 0);
    }

    private static StreamCommitRecord copyCommit(StreamCommitRecord value, long version) {
        return new StreamCommitRecord(
                value.streamId(), value.commitId(), value.previousCommitId(), value.offsetStart(), value.offsetEnd(),
                value.generation(), value.cumulativeSize(), value.commitVersion(), value.writerId(),
                value.writerRunIdHash(), value.writerEpoch(), value.fencingTokenHash(), value.objectId(),
                value.objectKey(), value.sliceId(), value.objectType(), value.physicalFormat(), value.logicalFormat(),
                value.payloadFormat(), value.objectChecksumType(), value.objectChecksumValue(), value.objectOffset(),
                value.objectLength(), value.recordCount(), value.entryCount(), value.logicalBytes(), value.schemaRefs(),
                value.entryIndexRef(), value.projectionRef(), value.sliceChecksumType(), value.sliceChecksumValue(),
                value.minEventTimeMillis(), value.maxEventTimeMillis(), value.preparedAtMillis(), version);
    }

    private static OffsetIndexRecord copyOffsetIndex(OffsetIndexRecord value, long version) {
        return new OffsetIndexRecord(
                value.streamId(), value.offsetStart(), value.offsetEnd(), value.generation(), value.cumulativeSize(),
                value.objectId(), value.objectKey(), value.sliceId(), value.objectType(), value.physicalFormat(),
                value.logicalFormat(), value.payloadFormat(), value.objectOffset(), value.objectLength(),
                value.recordCount(), value.entryCount(), value.logicalBytes(), value.schemaRefs(), value.entryIndexRef(),
                value.projectionRef(), value.sliceChecksumType(), value.sliceChecksumValue(), value.minEventTimeMillis(),
                value.maxEventTimeMillis(), value.commitVersion(), value.tombstoned(), version);
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong ids = new AtomicLong();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static ExecutorService boundedExecutor(int threads, int queueCapacity, String prefix) {
        return new ThreadPoolExecutor(
                threads,
                threads,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                daemonFactory(prefix),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private record ChainExpectation(long offsetEnd, long cumulativeSize, long commitVersion) {
    }

    private enum SearchStatus {
        FOUND,
        NOT_FOUND,
        EXHAUSTED,
        BROKEN
    }

    private record SearchResult(SearchStatus status, StreamCommitRecord commit, int scanned) {
    }

    private static final class BackendConditionException extends RuntimeException {
        private BackendConditionException(Throwable cause) {
            super(cause);
        }
    }
}
