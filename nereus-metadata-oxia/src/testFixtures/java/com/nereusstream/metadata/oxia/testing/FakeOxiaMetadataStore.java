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

package com.nereusstream.metadata.oxia.testing;

import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.AppendOutcome;
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
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.CommitSliceResult;
import com.nereusstream.metadata.oxia.DerivedIndexRepairCursor;
import com.nereusstream.metadata.oxia.DerivedIndexRepairResult;
import com.nereusstream.metadata.oxia.MetadataWatcher;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.Phase1ObjectManifestValidator;
import com.nereusstream.metadata.oxia.WatchRegistration;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/** In-memory metadata store with the same single-key stream-head CAS semantics required for Phase 1. */
public final class FakeOxiaMetadataStore implements OxiaMetadataStore {
    public enum FailurePoint {
        BEFORE_COMMIT_LOG_PUT,
        AFTER_COMMIT_LOG_PUT,
        BEFORE_HEAD_CAS,
        AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX,
        AFTER_DERIVED_INDEX_BEFORE_OBJECT_AUDIT,
        AFTER_DERIVED_INDEX_BEFORE_RESPONSE
    }

    public enum WatchDelivery {
        NORMAL,
        DROP_NEXT,
        DUPLICATE_NEXT,
        STALE_THEN_CURRENT_NEXT,
        COLLAPSE_NEXT,
        RECONNECT_THEN_CURRENT_NEXT
    }

    public enum HeadCasInterleaving {
        SAME_WRITER_RENEW,
        TRIM
    }

    public record PartitionedAccess(String key, String partitionKey, String operation) {
    }

    public record StoredMetadataValue(String key, String recordType, byte[] envelope) {
        public StoredMetadataValue {
            key = Objects.requireNonNull(key, "key");
            recordType = Objects.requireNonNull(recordType, "recordType");
            envelope = Objects.requireNonNull(envelope, "envelope").clone();
        }

        @Override
        public byte[] envelope() {
            return envelope.clone();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof StoredMetadataValue that)) {
                return false;
            }
            return key.equals(that.key)
                    && recordType.equals(that.recordType)
                    && Arrays.equals(envelope, that.envelope);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(key, recordType);
            result = 31 * result + Arrays.hashCode(envelope);
            return result;
        }
    }

    private record RegisteredWatcher(
            String cluster,
            StreamId streamId,
            MetadataWatcher watcher) {
    }

    private record RepairMaterialization(boolean repaired) {
    }

    private enum ChainSearchStatus {
        FOUND,
        NOT_FOUND,
        BUDGET_EXHAUSTED,
        BROKEN
    }

    private record ChainSearchResult(
            ChainSearchStatus status,
            StreamCommitRecord commit,
            int scannedRecords) {
        private ChainSearchResult {
            Objects.requireNonNull(status, "status");
            if ((status == ChainSearchStatus.FOUND) != (commit != null)) {
                throw new IllegalArgumentException("found chain search results must carry exactly one commit");
            }
            if (scannedRecords < 0) {
                throw new IllegalArgumentException("scannedRecords must be non-negative");
            }
        }
    }

    private record CommitChainExpectation(
            long offsetEnd,
            long cumulativeSize,
            long commitVersion) {
    }

    private static final class AppendAttemptState {
        private AppendOutcome failureOutcome = AppendOutcome.KNOWN_NOT_COMMITTED;

        void markCommitted() {
            failureOutcome = AppendOutcome.KNOWN_COMMITTED;
        }

        AppendOutcome failureOutcome() {
            return failureOutcome;
        }
    }

    private static final int DEFAULT_COMMIT_CHAIN_SCAN_LIMIT = 10_000;

    private final LongSupplier clock;
    private final int maxCommitChainScan;
    private final CodecBackedRecordMap<StreamHeadRecord> streamHeads =
            new CodecBackedRecordMap<>(StreamHeadRecord.class);
    private final CodecBackedRecordMap<StreamNameRecord> streamNames =
            new CodecBackedRecordMap<>(StreamNameRecord.class);
    private final CodecBackedRecordMap<StreamCommitRecord> commitByKey =
            new CodecBackedRecordMap<>(StreamCommitRecord.class);
    private final CodecBackedRecordMap<StreamCommitRecord> commitById =
            new CodecBackedRecordMap<>(StreamCommitRecord.class);
    private final CodecBackedRecordMap<OffsetIndexRecord> offsetIndexes =
            new CodecBackedRecordMap<>(OffsetIndexRecord.class);
    private final CodecBackedRecordMap<CommittedSliceRecord> committedSlices =
            new CodecBackedRecordMap<>(CommittedSliceRecord.class);
    private final CodecBackedRecordMap<ObjectManifestRecord> objectManifests =
            new CodecBackedRecordMap<>(ObjectManifestRecord.class);
    private final CodecBackedRecordMap<ObjectReferenceRecord> objectReferences =
            new CodecBackedRecordMap<>(ObjectReferenceRecord.class);
    private final List<PartitionedAccess> accessLog = new ArrayList<>();
    private final List<RegisteredWatcher> watchers = new ArrayList<>();

    private long nextMetadataVersion = 1;
    private long nextTokenId = 1;
    private FailurePoint failNext;
    private HeadCasInterleaving interleaveBeforeHeadCas;
    private Duration interleaveRenewTtl;
    private long interleaveTrimOffset;
    private WatchDelivery nextWatchDelivery = WatchDelivery.NORMAL;
    private boolean collapsedWatchEventPending;
    private long objectAuditFailureCount;
    private long watchDeliveryFailureCount;
    private boolean closed;

    public FakeOxiaMetadataStore(LongSupplier clock) {
        this(clock, DEFAULT_COMMIT_CHAIN_SCAN_LIMIT);
    }

    public FakeOxiaMetadataStore(LongSupplier clock, int maxCommitChainScan) {
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxCommitChainScan <= 0) {
            throw new IllegalArgumentException("maxCommitChainScan must be positive");
        }
        this.maxCommitChainScan = maxCommitChainScan;
    }

    public FakeOxiaMetadataStore() {
        this(System::currentTimeMillis);
    }

    public synchronized void failNext(FailurePoint failurePoint) {
        failNext = Objects.requireNonNull(failurePoint, "failurePoint");
    }

    public synchronized void interleaveBeforeNextHeadCasWithSameWriterRenew(Duration ttl) {
        interleaveBeforeHeadCas = HeadCasInterleaving.SAME_WRITER_RENEW;
        interleaveRenewTtl = Objects.requireNonNull(ttl, "ttl");
    }

    public synchronized void interleaveBeforeNextHeadCasWithTrim(long beforeOffset, String reason) {
        interleaveBeforeHeadCas = HeadCasInterleaving.TRIM;
        interleaveTrimOffset = beforeOffset;
        Objects.requireNonNull(reason, "reason");
    }

    public synchronized List<PartitionedAccess> accessLog() {
        return List.copyOf(accessLog);
    }

    public synchronized List<StoredMetadataValue> storedMetadataValuesForTesting() {
        List<StoredMetadataValue> values = new ArrayList<>();
        values.addAll(streamHeads.storedValues());
        values.addAll(streamNames.storedValues());
        values.addAll(commitByKey.storedValues());
        values.addAll(commitById.storedValues());
        values.addAll(offsetIndexes.storedValues());
        values.addAll(committedSlices.storedValues());
        values.addAll(objectManifests.storedValues());
        values.addAll(objectReferences.storedValues());
        return List.copyOf(values);
    }

    public synchronized void setNextWatchDelivery(WatchDelivery delivery) {
        nextWatchDelivery = Objects.requireNonNull(delivery, "delivery");
    }

    public synchronized long objectAuditFailureCount() {
        return objectAuditFailureCount;
    }

    public synchronized long watchDeliveryFailureCount() {
        return watchDeliveryFailureCount;
    }

    @Override
    public CompletableFuture<StreamMetadataRecord> createOrGetStream(
            String cluster,
            StreamName streamName,
            StreamCreateOptions options) {
        return complete(() -> {
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            StreamId streamId = DeterministicIds.streamIdFor(streamName);
            recordStreamAccess(keyspace.streamHeadKey(streamId), keyspace, streamId, "get");
            StreamHeadRecord existing = streamHeads.get(headMapKey(cluster, streamId));
            if (existing != null) {
                validateStreamIdentity(existing, streamName, streamId);
                return existing.toMetadataRecord();
            }

            long now = clock.getAsLong();
            StreamHeadRecord created = new StreamHeadRecord(
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
                    nextVersion());
            recordStreamAccess(keyspace.streamHeadKey(streamId), keyspace, streamId, "putIfAbsent");
            streamHeads.put(headMapKey(cluster, streamId), created);
            recordStreamAccess(keyspace.streamNameKey(streamName), keyspace, streamId, "putDerived");
            streamNames.put(streamNameMapKey(cluster, streamName), new StreamNameRecord(
                    streamName.value(),
                    streamId.value(),
                    DeterministicIds.streamNameHash(streamName),
                    now,
                    nextVersion()));
            return created.toMetadataRecord();
        });
    }

    @Override
    public CompletableFuture<StreamMetadataRecord> getStream(String cluster, StreamId streamId) {
        return complete(() -> headOrThrow(cluster, streamId).toMetadataRecord());
    }

    @Override
    public CompletableFuture<StreamMetadataSnapshot> getStreamSnapshot(String cluster, StreamId streamId) {
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(cluster, streamId);
            long observedAtMillis = clock.getAsLong();
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
        });
    }

    @Override
    public CompletableFuture<AppendSessionRecord> acquireAppendSession(
            String cluster,
            StreamId streamId,
            AppendSessionOptions options) {
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(cluster, streamId);
            requireActive(head);
            long now = clock.getAsLong();
            AppendSessionSnapshotRecord current = head.appendSession();
            boolean empty = current.isEmpty();
            boolean expired = !empty && current.expiresAtMillis() <= now;
            boolean sameWriter = !empty && current.writerId().equals(options.writerId());
            if (!empty && !expired && !sameWriter) {
                throw failure(ErrorCode.FENCED_APPEND, true, "append session is owned by another writer");
            }
            if (!empty && expired && !sameWriter && !options.allowStealExpiredSession()) {
                throw failure(ErrorCode.APPEND_SESSION_EXPIRED, true, "expired session steal is not allowed");
            }

            long epoch = (!empty && sameWriter && !expired) ? current.epoch() : current.epoch() + 1;
            String token = (!empty && sameWriter && !expired) ? current.fencingToken() : newFencingToken(streamId, options.writerId(), epoch);
            long leaseVersion = current.leaseVersion() + 1;
            AppendSessionSnapshotRecord updatedSession = new AppendSessionSnapshotRecord(
                    options.writerId(),
                    epoch,
                    token,
                    leaseVersion,
                    leaseExpiration(now, options.ttl()));
            StreamHeadRecord updated = withSession(head, updatedSession, nextVersion());
            recordHeadCas(cluster, streamId);
            streamHeads.put(headMapKey(cluster, streamId), updated);
            notifyAppendSessionChanged(cluster, streamId, updatedSession.epoch(), updatedSession.leaseVersion());
            return AppendSessionRecord.fromHead(streamId.value(), updatedSession);
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
            StreamHeadRecord head = headOrThrow(cluster, streamId);
            requireActive(head);
            AppendSessionSnapshotRecord current = head.appendSession();
            long now = clock.getAsLong();
            if (current.isEmpty() || current.expiresAtMillis() <= now) {
                throw failure(ErrorCode.APPEND_SESSION_EXPIRED, true, "append session expired");
            }
            if (!current.writerId().equals(writerId) || current.epoch() != epoch
                    || !current.fencingToken().equals(fencingToken)) {
                throw failure(ErrorCode.FENCED_APPEND, true, "append session token does not match");
            }
            AppendSessionSnapshotRecord updatedSession = new AppendSessionSnapshotRecord(
                    writerId,
                    epoch,
                    fencingToken,
                    current.leaseVersion() + 1,
                    leaseExpiration(now, ttl));
            StreamHeadRecord updated = withSession(head, updatedSession, nextVersion());
            recordHeadCas(cluster, streamId);
            streamHeads.put(headMapKey(cluster, streamId), updated);
            notifyAppendSessionChanged(cluster, streamId, updatedSession.epoch(), updatedSession.leaseVersion());
            return AppendSessionRecord.fromHead(streamId.value(), updatedSession);
        });
    }

    @Override
    public CompletableFuture<Void> putObjectManifest(String cluster, ObjectManifestRecord manifest) {
        return complete(() -> {
            Phase1ObjectManifestValidator.validateNewUpload(manifest);
            ObjectId objectId = new ObjectId(manifest.objectId());
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            recordObjectAccess(keyspace.objectManifestKey(objectId), keyspace, objectId, "putIfAbsent");
            String key = objectMapKey(cluster, objectId);
            ObjectManifestRecord existing = objectManifests.get(key);
            if (existing != null && "DELETED".equals(existing.state())) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "object manifest was deleted");
            }
            if (existing != null && !Phase1ObjectManifestValidator.sameImmutableIdentity(existing, manifest)) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "object manifest conflict");
            }
            objectManifests.putIfAbsent(key, manifestWithVersion(manifest, nextVersion()));
            return null;
        });
    }

    @Override
    public CompletableFuture<Optional<ObjectManifestRecord>> getObjectManifest(String cluster, ObjectId objectId) {
        return complete(() -> {
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            recordObjectAccess(keyspace.objectManifestKey(objectId), keyspace, objectId, "get");
            return Optional.ofNullable(objectManifests.get(objectMapKey(cluster, objectId)));
        });
    }

    @Override
    public CompletableFuture<Optional<ObjectReferenceRecord>> getObjectReferences(String cluster, ObjectId objectId) {
        return complete(() -> {
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            recordObjectAccess(keyspace.objectReferencesKey(objectId), keyspace, objectId, "get");
            return Optional.ofNullable(objectReferences.get(objectMapKey(cluster, objectId)));
        });
    }

    @Override
    public CompletableFuture<ObjectReferenceRecord> repairObjectReferences(String cluster, ObjectId objectId) {
        return complete(() -> {
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            recordObjectAccess(keyspace.objectManifestKey(objectId), keyspace, objectId, "get");
            ObjectManifestRecord manifest = objectManifests.get(objectMapKey(cluster, objectId));
            if (manifest == null) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "object manifest is missing");
            }
            Phase1ObjectManifestValidator.validateStoredManifest(manifest);
            if (!manifest.objectId().equals(objectId.value())) {
                throw failure(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "object manifest id does not match its lookup key");
            }
            List<VisibleSliceReferenceRecord> visibleSlices = new ArrayList<>();
            for (StreamSliceManifestRecord slice : manifest.slices()) {
                StreamCommitRecord commit = findReachableCommitForManifestSlice(cluster, manifest, slice);
                if (commit != null) {
                    materializeDerivedRecords(cluster, commit);
                    visibleSlices.add(new VisibleSliceReferenceRecord(
                            commit.streamId(),
                            commit.sliceId(),
                            commit.offsetStart(),
                            commit.offsetEnd(),
                            commit.generation(),
                            commit.commitVersion()));
                } else if ("VISIBLE".equals(slice.state())) {
                    throw failure(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "visible manifest slice has no matching reachable commit");
                }
            }
            ObjectReferenceRecord existing = objectReferences.get(objectMapKey(cluster, objectId));
            if (existing != null && (!existing.objectId().equals(objectId.value())
                    || !visibleSlices.containsAll(existing.visibleSlices()))) {
                throw failure(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "object-reference repair conflicts with existing visible state");
            }
            ObjectReferenceRecord repaired = new ObjectReferenceRecord(
                    objectId.value(),
                    visibleSlices.stream()
                            .sorted(Comparator.comparing(VisibleSliceReferenceRecord::streamId)
                                    .thenComparingLong(VisibleSliceReferenceRecord::offsetStart)
                                    .thenComparing(VisibleSliceReferenceRecord::sliceId))
                            .toList(),
                    clock.getAsLong(),
                    nextVersion());
            recordObjectAccess(keyspace.objectReferencesKey(objectId), keyspace, objectId, "putIfVersion");
            objectReferences.put(objectMapKey(cluster, objectId), repaired);
            return repaired;
        });
    }

    @Override
    public CompletableFuture<CommitSliceResult> commitStreamSlice(String cluster, CommitSliceRequest request) {
        AppendAttemptState attemptState = new AppendAttemptState();
        return completeAppend(attemptState, () -> {
            String commitId = request.commitId();
            StreamCommitRecord replay = findCommittedSliceMarkerReplay(cluster, request);
            StreamHeadRecord head = null;
            if (replay == null) {
                head = headOrThrow(cluster, request.streamId());
                if (head.committedEndOffset() > request.expectedStartOffset()) {
                    ChainSearchResult replaySearch = searchReachableCommitForReplay(cluster, request, commitId);
                    replay = appendReplayResultOrThrow(replaySearch, "commit replay search");
                }
            }
            if (replay != null) {
                attemptState.markCommitted();
                validateReplay(request, replay, AppendOutcome.KNOWN_COMMITTED);
                materializeDerivedRecordsForAppend(cluster, replay);
                return resultFromCommit(replay, request.projectionRef());
            }

            maybeFail(FailurePoint.BEFORE_COMMIT_LOG_PUT);
            if (head == null) {
                head = headOrThrow(cluster, request.streamId());
            }
            if (!StreamState.ACTIVE.name().equals(head.state())) {
                throw appendFailure(
                        ErrorCode.STREAM_NOT_ACTIVE,
                        false,
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "stream is not active");
            }
            validateSession(head, request);
            if (head.committedEndOffset() != request.expectedStartOffset()) {
                throw appendFailure(
                        ErrorCode.OFFSET_CONFLICT,
                        true,
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "expected start offset does not match committed end");
            }
            ObjectManifestRecord manifest = validateManifest(cluster, request, false);
            StreamCommitRecord commit = buildCommit(request, head, manifest, commitId, nextVersion());
            String commitKey = commitMapKey(cluster, request.streamId(), commitId);
            StreamCommitRecord existing = commitByKey.get(commitKey);
            if (existing == null) {
                recordStreamAccess(new OxiaKeyspace(cluster).streamCommitKey(request.streamId(), commitId),
                        new OxiaKeyspace(cluster), request.streamId(), "putIfAbsent");
                commitByKey.put(commitKey, commit);
                commitById.put(commitIdentityMapKey(cluster, request.streamId(), commitId), commit);
            } else {
                validateReplay(request, existing, AppendOutcome.KNOWN_NOT_COMMITTED);
                validateCommitAgainstHeadSnapshot(
                        request,
                        existing,
                        head,
                        AppendOutcome.KNOWN_NOT_COMMITTED);
                commit = existing;
            }
            maybeFail(FailurePoint.AFTER_COMMIT_LOG_PUT);
            maybeFail(FailurePoint.BEFORE_HEAD_CAS);

            applyBeforeHeadCasInterleaving(cluster, request.streamId());
            StreamHeadRecord currentHead = headOrThrow(cluster, request.streamId());
            if (currentHead.metadataVersion() != head.metadataVersion()) {
                if (!sameCommitAnchor(head, currentHead)) {
                    ChainSearchResult headSearch = searchReachableCommitForReplay(cluster, request, commitId);
                    StreamCommitRecord committed =
                            appendReplayResultOrThrow(headSearch, "head CAS replay search");
                    if (committed != null) {
                        attemptState.markCommitted();
                        validateReplay(request, committed, AppendOutcome.KNOWN_COMMITTED);
                        materializeDerivedRecordsForAppend(cluster, committed);
                        return resultFromCommit(committed, request.projectionRef());
                    }
                }
                if (!StreamState.ACTIVE.name().equals(currentHead.state())) {
                    throw appendFailure(
                            ErrorCode.STREAM_NOT_ACTIVE,
                            false,
                            AppendOutcome.KNOWN_NOT_COMMITTED,
                            "stream is not active");
                }
                validateSession(currentHead, request);
                if (currentHead.committedEndOffset() != request.expectedStartOffset()) {
                    throw appendFailure(
                            ErrorCode.OFFSET_CONFLICT,
                            true,
                            AppendOutcome.KNOWN_NOT_COMMITTED,
                            "expected start offset does not match committed end");
                }
            }
            validateCommitAgainstHeadSnapshot(
                    request,
                    commit,
                    currentHead,
                    AppendOutcome.KNOWN_NOT_COMMITTED);
            StreamHeadRecord newHead = withCommit(currentHead, commit, nextVersion());
            recordHeadCas(cluster, request.streamId());
            streamHeads.put(headMapKey(cluster, request.streamId()), newHead);
            attemptState.markCommitted();
            maybeFail(FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
            materializeDerivedRecordsForAppend(cluster, commit);
            if (consumeFailure(FailurePoint.AFTER_DERIVED_INDEX_BEFORE_OBJECT_AUDIT)) {
                objectAuditFailureCount++;
            } else {
                updateObjectAuditMetadataBestEffort(cluster, manifest, commit);
            }
            maybeFail(FailurePoint.AFTER_DERIVED_INDEX_BEFORE_RESPONSE);
            return resultFromCommit(commit, request.projectionRef());
        });
    }

    private boolean sameCommitAnchor(StreamHeadRecord left, StreamHeadRecord right) {
        return left.committedEndOffset() == right.committedEndOffset()
                && left.cumulativeSize() == right.cumulativeSize()
                && left.commitVersion() == right.commitVersion()
                && left.lastCommitId().equals(right.lastCommitId());
    }

    @Override
    public CompletableFuture<DerivedIndexRepairResult> repairDerivedStreamIndexes(
            String cluster,
            StreamId streamId,
            long targetOffset,
            Optional<DerivedIndexRepairCursor> continuation,
            int maxCommitsToScan) {
        return complete(() -> {
            Objects.requireNonNull(continuation, "continuation");
            if (targetOffset < 0 || maxCommitsToScan <= 0) {
                throw new IllegalArgumentException("targetOffset must be non-negative and maxCommitsToScan positive");
            }
            StreamHeadRecord head = headOrThrow(cluster, streamId);
            CommitChainExpectation headExpectation = expectationFromHead(head);
            continuation.ifPresent(cursor ->
                    validateRepairContinuation(cluster, streamId, targetOffset, head, cursor));
            if (targetOffset >= head.committedEndOffset()) {
                return new DerivedIndexRepairResult(
                        streamId,
                        targetOffset,
                        targetOffset,
                        0,
                        0,
                        true,
                        false,
                        Optional.empty(),
                        head.commitVersion());
            }

            String observedHeadCommitId = head.lastCommitId();
            long observedCommitVersion = head.commitVersion();
            String commitId = head.lastCommitId();
            CommitChainExpectation expectation = headExpectation;
            if (continuation.isPresent()) {
                DerivedIndexRepairCursor cursor = continuation.orElseThrow();
                observedHeadCommitId = cursor.observedHeadCommitId();
                observedCommitVersion = cursor.observedCommitVersion();
                commitId = cursor.nextCommitId();
                expectation = new CommitChainExpectation(
                        cursor.nextOffsetEnd(),
                        cursor.nextCumulativeSize(),
                        cursor.nextCommitVersion());
            }

            int scanned = 0;
            int repaired = 0;
            boolean targetCovered = false;
            long repairedFrom = Long.MAX_VALUE;
            long repairedTo = 0;
            while (!commitId.isEmpty() && scanned < maxCommitsToScan) {
                StreamCommitRecord commit = commitById.get(commitIdentityMapKey(cluster, streamId, commitId));
                if (commit == null) {
                    throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "broken commit chain");
                }
                CommitChainExpectation previousExpectation =
                        validateReachableCommit(streamId, commitId, commit, expectation);
                scanned++;
                RepairMaterialization materialization = materializeDerivedRecords(cluster, commit);
                if (materialization.repaired()) {
                    repaired++;
                    repairedFrom = Math.min(repairedFrom, commit.offsetStart());
                    repairedTo = Math.max(repairedTo, commit.offsetEnd());
                }
                if (commit.offsetStart() <= targetOffset && targetOffset < commit.offsetEnd()) {
                    targetCovered = true;
                    break;
                }
                commitId = commit.previousCommitId();
                expectation = previousExpectation;
            }
            boolean exhausted = !targetCovered && !commitId.isEmpty();
            if (!targetCovered && !exhausted) {
                throw failure(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "commit chain ended before covering the repair target");
            }
            long from = repairedFrom == Long.MAX_VALUE ? targetOffset : repairedFrom;
            long to = repairedFrom == Long.MAX_VALUE ? targetOffset : repairedTo;
            Optional<DerivedIndexRepairCursor> nextCursor = exhausted
                    ? Optional.of(new DerivedIndexRepairCursor(
                            streamId,
                            targetOffset,
                            observedHeadCommitId,
                            observedCommitVersion,
                            commitId,
                            expectation.offsetEnd(),
                            expectation.cumulativeSize(),
                            expectation.commitVersion()))
                    : Optional.empty();
            return new DerivedIndexRepairResult(
                    streamId,
                    from,
                    to,
                    scanned,
                    repaired,
                    targetCovered,
                    exhausted,
                    nextCursor,
                    observedCommitVersion);
        });
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
            recordStreamAccess(keyspace.offsetIndexScanFromExclusive(streamId, startOffset), keyspace, streamId, "scan");
            return offsetIndexes.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(offsetMapPrefix(cluster, streamId)))
                    .sorted(Map.Entry.comparingByKey())
                    .map(Map.Entry::getValue)
                    .filter(record -> record.offsetEnd() > startOffset)
                    .limit(limit)
                    .toList();
        });
    }

    @Override
    public CompletableFuture<CommittedEndOffsetRecord> getCommittedEndOffset(String cluster, StreamId streamId) {
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(cluster, streamId);
            return new CommittedEndOffsetRecord(
                    streamId.value(),
                    head.committedEndOffset(),
                    head.cumulativeSize(),
                    head.commitVersion(),
                    head.metadataVersion());
        });
    }

    @Override
    public CompletableFuture<TrimRecord> updateTrim(String cluster, StreamId streamId, long beforeOffset, String reason) {
        return complete(() -> {
            if (reason == null) {
                throw failure(ErrorCode.INVALID_ARGUMENT, false, "trim reason cannot be null");
            }
            StreamHeadRecord head = headOrThrow(cluster, streamId);
            if (!StreamState.ACTIVE.name().equals(head.state())
                    && !StreamState.SEALED.name().equals(head.state())) {
                throw failure(ErrorCode.STREAM_NOT_ACTIVE, false, "stream state does not allow trim");
            }
            if (beforeOffset < head.trimOffset() || beforeOffset > head.committedEndOffset()) {
                throw failure(ErrorCode.INVALID_ARGUMENT, false, "trim offset is outside committed range");
            }
            StreamHeadRecord updated = new StreamHeadRecord(
                    head.streamId(),
                    head.streamName(),
                    head.streamNameHash(),
                    head.state(),
                    head.profile(),
                    head.attributes(),
                    head.createdAtMillis(),
                    head.policyVersion(),
                    head.committedEndOffset(),
                    head.cumulativeSize(),
                    head.commitVersion(),
                    beforeOffset,
                    head.lastCommitId(),
                    head.appendSession(),
                    nextVersion());
            recordHeadCas(cluster, streamId);
            streamHeads.put(headMapKey(cluster, streamId), updated);
            notifyTrimUpdated(cluster, streamId, beforeOffset, updated.metadataVersion());
            return new TrimRecord(streamId.value(), beforeOffset, reason, clock.getAsLong(), updated.metadataVersion());
        });
    }

    @Override
    public CompletableFuture<TrimRecord> getTrim(String cluster, StreamId streamId) {
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(cluster, streamId);
            return new TrimRecord(streamId.value(), head.trimOffset(), "", clock.getAsLong(), head.metadataVersion());
        });
    }

    @Override
    public WatchRegistration watchStream(String cluster, StreamId streamId, MetadataWatcher watcher) {
        Objects.requireNonNull(watcher, "watcher");
        synchronized (this) {
            if (closed) {
                throw failure(ErrorCode.STORAGE_CLOSED, false, "metadata store is closed");
            }
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            recordStreamAccess(keyspace.streamHeadKey(streamId), keyspace, streamId, "watch");
            RegisteredWatcher registration = new RegisteredWatcher(cluster, streamId, watcher);
            watchers.add(registration);
            return () -> {
                synchronized (FakeOxiaMetadataStore.this) {
                    watchers.remove(registration);
                }
            };
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    private StreamHeadRecord headOrThrow(String cluster, StreamId streamId) {
        OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
        recordStreamAccess(keyspace.streamHeadKey(streamId), keyspace, streamId, "get");
        StreamHeadRecord head = streamHeads.get(headMapKey(cluster, streamId));
        if (head == null) {
            throw failure(ErrorCode.STREAM_NOT_FOUND, false, "stream does not exist");
        }
        return head;
    }

    private void validateStreamIdentity(StreamHeadRecord head, StreamName streamName, StreamId streamId) {
        if (!head.streamId().equals(streamId.value()) || !head.streamName().equals(streamName.value())) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "deterministic stream id collision");
        }
    }

    private void requireActive(StreamHeadRecord head) {
        if (!StreamState.ACTIVE.name().equals(head.state())) {
            throw failure(ErrorCode.STREAM_NOT_ACTIVE, false, "stream is not active");
        }
    }

    private void validateSession(StreamHeadRecord head, CommitSliceRequest request) {
        AppendSessionSnapshotRecord session = head.appendSession();
        if (session.isEmpty() || session.expiresAtMillis() <= clock.getAsLong()) {
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
    }

    private ObjectManifestRecord validateManifest(String cluster, CommitSliceRequest request, boolean replay) {
        ObjectManifestRecord manifest = objectManifests.get(objectMapKey(cluster, request.objectId()));
        if (manifest == null) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "object manifest is missing");
        }
        try {
            Phase1ObjectManifestValidator.validateCommitCandidate(manifest, request, replay);
        } catch (NereusException e) {
            throw new NereusException(
                    e.code(),
                    e.retriable(),
                    e.getMessage(),
                    e,
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        return manifest;
    }

    private StreamCommitRecord buildCommit(
            CommitSliceRequest request,
            StreamHeadRecord head,
            ObjectManifestRecord manifest,
            String commitId,
            long metadataVersion) {
        try {
            long offsetEnd = Math.addExact(request.expectedStartOffset(), request.recordCount());
            long cumulativeSize = Math.addExact(head.cumulativeSize(), request.logicalBytes());
            long commitVersion = Math.addExact(head.commitVersion(), 1);
            return new StreamCommitRecord(
                    request.streamId().value(),
                    commitId,
                    head.lastCommitId(),
                    request.expectedStartOffset(),
                    offsetEnd,
                    0,
                    cumulativeSize,
                    commitVersion,
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
                    clock.getAsLong(),
                    metadataVersion);
        } catch (ArithmeticException e) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "commit head-derived fields overflow",
                    e,
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
    }

    private RepairMaterialization materializeDerivedRecords(String cluster, StreamCommitRecord commit) {
        boolean repaired = false;
        StreamId streamId = new StreamId(commit.streamId());
        ObjectId objectId = new ObjectId(commit.objectId());
        OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
        OffsetIndexRecord offsetIndexRecord = new OffsetIndexRecord(
                commit.streamId(),
                commit.offsetStart(),
                commit.offsetEnd(),
                commit.generation(),
                commit.cumulativeSize(),
                commit.objectId(),
                commit.objectKey(),
                commit.sliceId(),
                commit.objectType(),
                commit.physicalFormat(),
                commit.logicalFormat(),
                commit.payloadFormat(),
                commit.objectOffset(),
                commit.objectLength(),
                commit.recordCount(),
                commit.entryCount(),
                commit.logicalBytes(),
                commit.schemaRefs(),
                commit.entryIndexRef(),
                commit.projectionRef(),
                commit.sliceChecksumType(),
                commit.sliceChecksumValue(),
                commit.minEventTimeMillis(),
                commit.maxEventTimeMillis(),
                commit.commitVersion(),
                false,
                nextVersion());
        String offsetKey = offsetMapKey(cluster, streamId, commit.offsetEnd(), commit.generation());
        OffsetIndexRecord existingOffset = offsetIndexes.get(offsetKey);
        if (existingOffset != null && !offsetIndexMatches(existingOffset, offsetIndexRecord)) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "offset index conflict");
        }
        if (existingOffset == null) {
            recordStreamAccess(keyspace.offsetIndexKey(streamId, commit.offsetEnd(), commit.generation()), keyspace, streamId, "putDerived");
            offsetIndexes.put(offsetKey, offsetIndexRecord);
            notifyOffsetIndexUpdated(cluster, streamId, commit.offsetEnd(), offsetIndexRecord.metadataVersion());
            repaired = true;
        }

        CommittedSliceRecord committedSliceRecord = new CommittedSliceRecord(
                commit.streamId(),
                commit.objectId(),
                commit.sliceId(),
                commit.offsetStart(),
                commit.offsetEnd(),
                commit.generation(),
                commit.commitVersion(),
                nextVersion());
        String markerKey = committedSliceMapKey(cluster, streamId, objectId, commit.sliceId());
        CommittedSliceRecord existingMarker = committedSlices.get(markerKey);
        if (existingMarker != null && !committedSliceMatches(existingMarker, committedSliceRecord)) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "committed slice conflict");
        }
        if (existingMarker == null) {
            recordStreamAccess(keyspace.committedSliceKey(streamId, objectId, commit.sliceId()), keyspace, streamId, "putDerived");
            committedSlices.put(markerKey, committedSliceRecord);
            repaired = true;
        }
        return new RepairMaterialization(repaired);
    }

    private RepairMaterialization materializeDerivedRecordsForAppend(
            String cluster,
            StreamCommitRecord commit) {
        try {
            return materializeDerivedRecords(cluster, commit);
        } catch (NereusException e) {
            throw new NereusException(
                    e.code(),
                    e.retriable(),
                    e.getMessage(),
                    e,
                    AppendOutcome.KNOWN_COMMITTED);
        }
    }

    private void applyBeforeHeadCasInterleaving(String cluster, StreamId streamId) {
        HeadCasInterleaving interleaving = interleaveBeforeHeadCas;
        if (interleaving == null) {
            return;
        }
        interleaveBeforeHeadCas = null;
        StreamHeadRecord head = headOrThrow(cluster, streamId);
        if (interleaving == HeadCasInterleaving.SAME_WRITER_RENEW) {
            AppendSessionSnapshotRecord current = head.appendSession();
            long now = clock.getAsLong();
            if (current.isEmpty() || current.expiresAtMillis() <= now) {
                throw failure(ErrorCode.APPEND_SESSION_EXPIRED, true, "append session expired during interleaved renew");
            }
            AppendSessionSnapshotRecord updatedSession = new AppendSessionSnapshotRecord(
                    current.writerId(),
                    current.epoch(),
                    current.fencingToken(),
                    current.leaseVersion() + 1,
                    leaseExpiration(now, interleaveRenewTtl));
            StreamHeadRecord updated = withSession(head, updatedSession, nextVersion());
            recordHeadCas(cluster, streamId);
            streamHeads.put(headMapKey(cluster, streamId), updated);
            notifyAppendSessionChanged(cluster, streamId, updatedSession.epoch(), updatedSession.leaseVersion());
            interleaveRenewTtl = null;
            return;
        }
        if (interleaving == HeadCasInterleaving.TRIM) {
            if (interleaveTrimOffset < head.trimOffset() || interleaveTrimOffset > head.committedEndOffset()) {
                throw failure(ErrorCode.INVALID_ARGUMENT, false, "interleaved trim offset is outside committed range");
            }
            StreamHeadRecord updated = new StreamHeadRecord(
                    head.streamId(),
                    head.streamName(),
                    head.streamNameHash(),
                    head.state(),
                    head.profile(),
                    head.attributes(),
                    head.createdAtMillis(),
                    head.policyVersion(),
                    head.committedEndOffset(),
                    head.cumulativeSize(),
                    head.commitVersion(),
                    interleaveTrimOffset,
                    head.lastCommitId(),
                    head.appendSession(),
                    nextVersion());
            recordHeadCas(cluster, streamId);
            streamHeads.put(headMapKey(cluster, streamId), updated);
            notifyTrimUpdated(cluster, streamId, interleaveTrimOffset, updated.metadataVersion());
        }
    }

    private void updateObjectAuditMetadata(String cluster, ObjectManifestRecord manifest, StreamCommitRecord commit) {
        List<StreamSliceManifestRecord> slices = new ArrayList<>();
        boolean allVisible = true;
        for (StreamSliceManifestRecord slice : manifest.slices()) {
            StreamSliceManifestRecord next = slice;
            if (slice.streamId().equals(commit.streamId()) && slice.sliceId().equals(commit.sliceId())) {
                next = slice.withState("VISIBLE");
            }
            allVisible &= "VISIBLE".equals(next.state());
            slices.add(next);
        }
        ObjectManifestRecord updatedManifest = manifest.withStateAndSlices(allVisible ? "VISIBLE" : "PARTIALLY_VISIBLE", slices, nextVersion());
        objectManifests.put(objectMapKey(cluster, new ObjectId(manifest.objectId())), updatedManifest);

        String referenceKey = objectMapKey(cluster, new ObjectId(commit.objectId()));
        ObjectReferenceRecord existing = objectReferences.get(referenceKey);
        List<VisibleSliceReferenceRecord> visible = new ArrayList<>(
                existing == null ? List.of() : existing.visibleSlices());
        VisibleSliceReferenceRecord reference = new VisibleSliceReferenceRecord(
                commit.streamId(),
                commit.sliceId(),
                commit.offsetStart(),
                commit.offsetEnd(),
                commit.generation(),
                commit.commitVersion());
        if (!visible.contains(reference)) {
            visible.add(reference);
        }
        objectReferences.put(referenceKey, new ObjectReferenceRecord(commit.objectId(), visible, clock.getAsLong(), nextVersion()));
    }

    private void updateObjectAuditMetadataBestEffort(
            String cluster,
            ObjectManifestRecord manifest,
            StreamCommitRecord commit) {
        try {
            updateObjectAuditMetadata(cluster, manifest, commit);
        } catch (RuntimeException e) {
            objectAuditFailureCount++;
        }
    }

    private ChainSearchResult searchReachableCommit(
            String cluster,
            StreamId streamId,
            Predicate<StreamCommitRecord> predicate) {
        return searchReachableCommit(cluster, streamId, predicate, commit -> false);
    }

    private ChainSearchResult searchReachableCommitForReplay(
            String cluster,
            CommitSliceRequest request,
            String commitId) {
        return searchReachableCommit(
                cluster,
                request.streamId(),
                commit -> commit.commitId().equals(commitId),
                commit -> commit.offsetStart() <= request.expectedStartOffset());
    }

    private ChainSearchResult searchReachableCommit(
            String cluster,
            StreamId streamId,
            Predicate<StreamCommitRecord> predicate,
            Predicate<StreamCommitRecord> provesNotFound) {
        StreamHeadRecord head = streamHeads.get(headMapKey(cluster, streamId));
        if (head == null) {
            return new ChainSearchResult(ChainSearchStatus.NOT_FOUND, null, 0);
        }
        CommitChainExpectation expectation;
        try {
            expectation = expectationFromHead(head);
        } catch (NereusException e) {
            return new ChainSearchResult(ChainSearchStatus.BROKEN, null, 0);
        }
        String current = head.lastCommitId();
        int searched = 0;
        while (!current.isEmpty() && searched < maxCommitChainScan) {
            StreamCommitRecord commit = commitById.get(commitIdentityMapKey(cluster, streamId, current));
            if (commit == null) {
                return new ChainSearchResult(ChainSearchStatus.BROKEN, null, searched);
            }
            CommitChainExpectation previousExpectation;
            try {
                previousExpectation = validateReachableCommit(streamId, current, commit, expectation);
            } catch (NereusException e) {
                return new ChainSearchResult(ChainSearchStatus.BROKEN, null, searched);
            }
            searched++;
            if (predicate.test(commit)) {
                return new ChainSearchResult(ChainSearchStatus.FOUND, commit, searched);
            }
            if (provesNotFound.test(commit)) {
                return new ChainSearchResult(ChainSearchStatus.NOT_FOUND, null, searched);
            }
            current = commit.previousCommitId();
            expectation = previousExpectation;
        }
        if (current.isEmpty() && !isGenesisExpectation(expectation)) {
            return new ChainSearchResult(ChainSearchStatus.BROKEN, null, searched);
        }
        return new ChainSearchResult(
                current.isEmpty() ? ChainSearchStatus.NOT_FOUND : ChainSearchStatus.BUDGET_EXHAUSTED,
                null,
                searched);
    }

    private CommitChainExpectation expectationFromHead(StreamHeadRecord head) {
        boolean emptyChain = head.lastCommitId().isEmpty();
        if (emptyChain != (head.commitVersion() == 0)
                || emptyChain != (head.committedEndOffset() == 0)
                || (emptyChain && head.cumulativeSize() != 0)) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "stream head commit anchor is invalid");
        }
        return new CommitChainExpectation(
                head.committedEndOffset(),
                head.cumulativeSize(),
                head.commitVersion());
    }

    private CommitChainExpectation validateReachableCommit(
            StreamId streamId,
            String expectedCommitId,
            StreamCommitRecord commit,
            CommitChainExpectation expectation) {
        long calculatedOffsetEnd;
        try {
            calculatedOffsetEnd = Math.addExact(commit.offsetStart(), commit.recordCount());
        } catch (ArithmeticException e) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "commit logical range overflows");
        }
        if (!commit.streamId().equals(streamId.value())
                || !commit.commitId().equals(expectedCommitId)
                || commit.generation() != 0
                || commit.offsetEnd() != calculatedOffsetEnd
                || commit.offsetEnd() != expectation.offsetEnd()
                || commit.cumulativeSize() != expectation.cumulativeSize()
                || commit.commitVersion() != expectation.commitVersion()
                || commit.commitVersion() <= 0
                || !ObjectType.MULTI_STREAM_WAL_OBJECT.name().equals(commit.objectType())
                || !"WAL_OBJECT_V1".equals(commit.physicalFormat())
                || !"OPAQUE_SLICE".equals(commit.logicalFormat())) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "commit chain record is inconsistent");
        }

        long previousCumulativeSize = commit.cumulativeSize() - commit.logicalBytes();
        long previousCommitVersion = commit.commitVersion() - 1;
        if (previousCumulativeSize < 0
                || (commit.previousCommitId().isEmpty()
                        && (commit.offsetStart() != 0
                                || previousCumulativeSize != 0
                                || previousCommitVersion != 0))
                || (!commit.previousCommitId().isEmpty()
                        && (commit.offsetStart() == 0 || previousCommitVersion <= 0))) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "commit chain predecessor is inconsistent");
        }
        return new CommitChainExpectation(
                commit.offsetStart(),
                previousCumulativeSize,
                previousCommitVersion);
    }

    private boolean isGenesisExpectation(CommitChainExpectation expectation) {
        return expectation.offsetEnd() == 0
                && expectation.cumulativeSize() == 0
                && expectation.commitVersion() == 0;
    }

    private void validateRepairContinuation(
            String cluster,
            StreamId streamId,
            long targetOffset,
            StreamHeadRecord currentHead,
            DerivedIndexRepairCursor cursor) {
        if (!cursor.streamId().equals(streamId)
                || cursor.targetOffset() != targetOffset
                || cursor.observedCommitVersion() > currentHead.commitVersion()) {
            throw failure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "derived-index repair continuation does not match the current stream head");
        }
        StreamCommitRecord observedHead = commitById.get(commitIdentityMapKey(
                cluster,
                streamId,
                cursor.observedHeadCommitId()));
        if (observedHead == null
                || observedHead.commitVersion() != cursor.observedCommitVersion()
                || (currentHead.commitVersion() == cursor.observedCommitVersion()
                        && (!currentHead.lastCommitId().equals(cursor.observedHeadCommitId())
                                || currentHead.committedEndOffset() != observedHead.offsetEnd()
                                || currentHead.cumulativeSize() != observedHead.cumulativeSize()))
                || cursor.nextOffsetEnd() > observedHead.offsetStart()
                || cursor.nextCumulativeSize() > observedHead.cumulativeSize()
                || cursor.nextCommitVersion() >= observedHead.commitVersion()) {
            throw failure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "derived-index repair continuation references an invalid committed-chain position");
        }
        validateReachableCommit(
                streamId,
                cursor.observedHeadCommitId(),
                observedHead,
                new CommitChainExpectation(
                        observedHead.offsetEnd(),
                        observedHead.cumulativeSize(),
                        observedHead.commitVersion()));
    }

    private StreamCommitRecord appendReplayResultOrThrow(ChainSearchResult result, String operation) {
        return switch (result.status()) {
            case FOUND -> result.commit();
            case NOT_FOUND -> null;
            case BUDGET_EXHAUSTED -> throw appendFailure(
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

    private StreamCommitRecord findCommittedSliceMarkerReplay(String cluster, CommitSliceRequest request) {
        StreamId streamId = request.streamId();
        ObjectId objectId = request.objectId();
        OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
        recordStreamAccess(keyspace.committedSliceKey(streamId, objectId, request.sliceId()), keyspace, streamId, "get");
        CommittedSliceRecord marker = committedSlices.get(
                committedSliceMapKey(cluster, streamId, objectId, request.sliceId()));
        if (marker == null) {
            return null;
        }
        long expectedEnd = expectedEndOffset(request);
        if (!marker.streamId().equals(streamId.value())
                || !marker.objectId().equals(objectId.value())
                || !marker.sliceId().equals(request.sliceId())
                || marker.offsetStart() != request.expectedStartOffset()
                || marker.offsetEnd() != expectedEnd) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    "committed-slice marker does not match request");
        }
        StreamCommitRecord commit = commitById.get(commitIdentityMapKey(
                cluster,
                streamId,
                request.commitId()));
        StreamHeadRecord head = streamHeads.get(headMapKey(cluster, streamId));
        if (commit == null
                || head == null
                || !commitMatchesMarker(commit, marker)
                || head.commitVersion() < marker.commitVersion()
                || head.committedEndOffset() < marker.offsetEnd()) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    "committed-slice marker does not have a valid committed record/head proof");
        }
        return commit;
    }

    private StreamCommitRecord findReachableCommitForManifestSlice(
            String cluster,
            ObjectManifestRecord manifest,
            StreamSliceManifestRecord slice) {
        StreamId streamId = new StreamId(slice.streamId());
        ChainSearchResult search = searchReachableCommit(
                cluster,
                streamId,
                commit -> commit.objectId().equals(manifest.objectId())
                        && commit.sliceId().equals(slice.sliceId()));
        return switch (search.status()) {
            case FOUND -> {
                if (!commitMatchesManifestSlice(manifest, slice, search.commit())) {
                    throw failure(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "reachable commit conflicts with object manifest slice");
                }
                yield search.commit();
            }
            case NOT_FOUND -> null;
            case BUDGET_EXHAUSTED -> throw failure(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "object-reference repair exhausted the commit-chain scan budget");
            case BROKEN -> throw failure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "object-reference repair found a broken commit chain");
        };
    }

    private boolean commitMatchesManifestSlice(
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

    private boolean commitMatchesMarker(StreamCommitRecord commit, CommittedSliceRecord marker) {
        return commit.streamId().equals(marker.streamId())
                && commit.objectId().equals(marker.objectId())
                && commit.sliceId().equals(marker.sliceId())
                && commit.offsetStart() == marker.offsetStart()
                && commit.offsetEnd() == marker.offsetEnd()
                && commit.generation() == marker.generation()
                && commit.commitVersion() == marker.commitVersion();
    }

    private void validateReplay(
            CommitSliceRequest request,
            StreamCommitRecord replay,
            AppendOutcome mismatchOutcome) {
        long expectedOffsetEnd = expectedEndOffset(request);
        if (!replay.commitId().equals(request.commitId())
                || !replay.streamId().equals(request.streamId().value())
                || !replay.writerId().equals(request.writerId())
                || !replay.writerRunIdHash().equals(request.writerRunIdHash())
                || replay.writerEpoch() != request.epoch()
                || !replay.fencingTokenHash().equals(request.fencingTokenHash())
                || replay.offsetStart() != request.expectedStartOffset()
                || replay.offsetEnd() != expectedOffsetEnd
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
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    mismatchOutcome,
                    "replayed commit does not match request");
        }
    }

    private void validateCommitAgainstHeadSnapshot(
            CommitSliceRequest request,
            StreamCommitRecord commit,
            StreamHeadRecord head,
            AppendOutcome mismatchOutcome) {
        long expectedOffsetEnd;
        long expectedCumulativeSize;
        long expectedCommitVersion;
        try {
            expectedOffsetEnd = Math.addExact(request.expectedStartOffset(), request.recordCount());
            expectedCumulativeSize = Math.addExact(head.cumulativeSize(), request.logicalBytes());
            expectedCommitVersion = Math.addExact(head.commitVersion(), 1);
        } catch (ArithmeticException e) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "commit head-derived fields overflow",
                    e,
                    mismatchOutcome);
        }
        if (!commit.previousCommitId().equals(head.lastCommitId())
                || commit.offsetStart() != request.expectedStartOffset()
                || commit.offsetEnd() != expectedOffsetEnd
                || commit.cumulativeSize() != expectedCumulativeSize
                || commit.commitVersion() != expectedCommitVersion) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    mismatchOutcome,
                    "stored commit does not match the current head snapshot");
        }
    }

    private CommitSliceResult resultFromCommit(StreamCommitRecord commit, Optional<ProjectionRef> projectionRef) {
        return new CommitSliceResult(
                new StreamId(commit.streamId()),
                new OffsetRange(commit.offsetStart(), commit.offsetEnd()),
                commit.offsetEnd(),
                commit.generation(),
                commit.commitVersion(),
                projectionRef);
    }

    private void maybeFail(FailurePoint point) {
        if (failNext == point) {
            failNext = null;
            AppendOutcome outcome = switch (point) {
                case BEFORE_COMMIT_LOG_PUT, AFTER_COMMIT_LOG_PUT, BEFORE_HEAD_CAS ->
                        AppendOutcome.KNOWN_NOT_COMMITTED;
                case AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX, AFTER_DERIVED_INDEX_BEFORE_RESPONSE ->
                        AppendOutcome.KNOWN_COMMITTED;
                case AFTER_DERIVED_INDEX_BEFORE_OBJECT_AUDIT -> throw new IllegalStateException(
                        "object audit failures are consumed without failing append");
            };
            throw appendFailure(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    outcome,
                    "injected metadata failure at " + point);
        }
    }

    private boolean consumeFailure(FailurePoint point) {
        if (failNext == point) {
            failNext = null;
            return true;
        }
        return false;
    }

    private long expectedEndOffset(CommitSliceRequest request) {
        try {
            return Math.addExact(request.expectedStartOffset(), request.recordCount());
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("expected start offset + recordCount must not overflow", e);
        }
    }

    private long leaseExpiration(long nowMillis, Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        long ttlMillis;
        try {
            ttlMillis = ttl.toMillis();
            if (ttlMillis <= 0) {
                throw new ArithmeticException("ttl is below millisecond resolution");
            }
            return Math.addExact(nowMillis, ttlMillis);
        } catch (ArithmeticException e) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "append session ttl is outside millisecond range");
        }
    }

    private void recordHeadCas(String cluster, StreamId streamId) {
        OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
        recordStreamAccess(keyspace.streamHeadKey(streamId), keyspace, streamId, "putIfVersion");
    }

    private void recordStreamAccess(String key, OxiaKeyspace keyspace, StreamId streamId, String operation) {
        String expected = keyspace.streamPartitionKey(streamId).value();
        accessLog.add(new PartitionedAccess(key, expected, operation));
    }

    private void recordObjectAccess(String key, OxiaKeyspace keyspace, ObjectId objectId, String operation) {
        String expected = keyspace.objectPartitionKey(objectId).value();
        accessLog.add(new PartitionedAccess(key, expected, operation));
    }

    private void notifyOffsetIndexUpdated(String cluster, StreamId streamId, long committedEndOffset, long metadataVersion) {
        deliverWatchEvent(cluster, streamId, metadataVersion, version ->
                watcher -> watcher.onOffsetIndexUpdated(streamId, committedEndOffset, version));
    }

    private void notifyTrimUpdated(String cluster, StreamId streamId, long trimOffset, long metadataVersion) {
        deliverWatchEvent(cluster, streamId, metadataVersion, version ->
                watcher -> watcher.onTrimUpdated(streamId, trimOffset, version));
    }

    private void notifyAppendSessionChanged(String cluster, StreamId streamId, long epoch, long leaseVersion) {
        deliverWatchEvent(cluster, streamId, leaseVersion, ignored ->
                watcher -> watcher.onAppendSessionChanged(streamId, epoch, leaseVersion));
    }

    private void deliverWatchEvent(String cluster, StreamId streamId, long version, WatchEventFactory factory) {
        List<RegisteredWatcher> matching = watchers.stream()
                .filter(watcher -> watcher.cluster().equals(cluster) && watcher.streamId().equals(streamId))
                .toList();
        if (matching.isEmpty()) {
            nextWatchDelivery = WatchDelivery.NORMAL;
            return;
        }
        WatchDelivery delivery = nextWatchDelivery;
        nextWatchDelivery = WatchDelivery.NORMAL;
        if (delivery == WatchDelivery.COLLAPSE_NEXT) {
            collapsedWatchEventPending = true;
            return;
        }
        if (delivery == WatchDelivery.DROP_NEXT) {
            return;
        }
        if (collapsedWatchEventPending) {
            collapsedWatchEventPending = false;
        }
        if (delivery == WatchDelivery.RECONNECT_THEN_CURRENT_NEXT) {
            for (RegisteredWatcher watcher : matching) {
                deliverWatchSafely(
                        ignored -> ignored.onWatchReconnected(streamId, version),
                        watcher.watcher());
            }
        }
        if (delivery == WatchDelivery.STALE_THEN_CURRENT_NEXT) {
            long staleVersion = Math.max(0, version - 1);
            for (RegisteredWatcher watcher : matching) {
                deliverWatchSafely(factory.event(staleVersion), watcher.watcher());
            }
        }
        int deliveries = delivery == WatchDelivery.DUPLICATE_NEXT ? 2 : 1;
        for (int i = 0; i < deliveries; i++) {
            for (RegisteredWatcher watcher : matching) {
                deliverWatchSafely(factory.event(version), watcher.watcher());
            }
        }
    }

    private void deliverWatchSafely(WatchEvent event, MetadataWatcher watcher) {
        try {
            event.deliver(watcher);
        } catch (RuntimeException e) {
            watchDeliveryFailureCount++;
        }
    }

    private long nextVersion() {
        return nextMetadataVersion++;
    }

    private String newFencingToken(StreamId streamId, String writerId, long epoch) {
        return DeterministicIds.stableHashComponent(streamId.value() + "\0" + writerId + "\0" + epoch + "\0" + nextTokenId++);
    }

    private StreamHeadRecord withSession(StreamHeadRecord head, AppendSessionSnapshotRecord session, long metadataVersion) {
        return new StreamHeadRecord(
                head.streamId(),
                head.streamName(),
                head.streamNameHash(),
                head.state(),
                head.profile(),
                head.attributes(),
                head.createdAtMillis(),
                head.policyVersion(),
                head.committedEndOffset(),
                head.cumulativeSize(),
                head.commitVersion(),
                head.trimOffset(),
                head.lastCommitId(),
                session,
                metadataVersion);
    }

    private StreamHeadRecord withCommit(StreamHeadRecord head, StreamCommitRecord commit, long metadataVersion) {
        return new StreamHeadRecord(
                head.streamId(),
                head.streamName(),
                head.streamNameHash(),
                head.state(),
                head.profile(),
                head.attributes(),
                head.createdAtMillis(),
                head.policyVersion(),
                commit.offsetEnd(),
                commit.cumulativeSize(),
                commit.commitVersion(),
                head.trimOffset(),
                commit.commitId(),
                head.appendSession(),
                metadataVersion);
    }

    private ObjectManifestRecord manifestWithVersion(ObjectManifestRecord manifest, long metadataVersion) {
        return new ObjectManifestRecord(
                manifest.objectId(),
                manifest.objectKey(),
                manifest.objectType(),
                manifest.state(),
                manifest.formatMajorVersion(),
                manifest.formatMinorVersion(),
                manifest.writerVersion(),
                manifest.writerId(),
                manifest.writerRunIdHash(),
                manifest.writerEpoch(),
                manifest.createdAtMillis(),
                manifest.uploadedAtMillis(),
                manifest.objectLength(),
                manifest.objectChecksumType(),
                manifest.objectChecksumValue(),
                manifest.storageChecksumType(),
                manifest.storageChecksumValue(),
                manifest.slices(),
                manifest.orphanExpiresAtMillis(),
                metadataVersion);
    }

    private boolean offsetIndexMatches(OffsetIndexRecord left, OffsetIndexRecord right) {
        return left.streamId().equals(right.streamId())
                && left.offsetStart() == right.offsetStart()
                && left.offsetEnd() == right.offsetEnd()
                && left.generation() == right.generation()
                && left.cumulativeSize() == right.cumulativeSize()
                && left.objectId().equals(right.objectId())
                && left.objectKey().equals(right.objectKey())
                && left.sliceId().equals(right.sliceId())
                && left.objectType().equals(right.objectType())
                && left.physicalFormat().equals(right.physicalFormat())
                && left.logicalFormat().equals(right.logicalFormat())
                && left.payloadFormat().equals(right.payloadFormat())
                && left.objectOffset() == right.objectOffset()
                && left.objectLength() == right.objectLength()
                && left.recordCount() == right.recordCount()
                && left.entryCount() == right.entryCount()
                && left.logicalBytes() == right.logicalBytes()
                && left.schemaRefs().equals(right.schemaRefs())
                && left.entryIndexRef().equals(right.entryIndexRef())
                && left.projectionRef().equals(right.projectionRef())
                && left.sliceChecksumType().equals(right.sliceChecksumType())
                && left.sliceChecksumValue().equals(right.sliceChecksumValue())
                && left.minEventTimeMillis() == right.minEventTimeMillis()
                && left.maxEventTimeMillis() == right.maxEventTimeMillis()
                && left.commitVersion() == right.commitVersion()
                && left.tombstoned() == right.tombstoned();
    }

    private boolean committedSliceMatches(CommittedSliceRecord left, CommittedSliceRecord right) {
        return left.streamId().equals(right.streamId())
                && left.objectId().equals(right.objectId())
                && left.sliceId().equals(right.sliceId())
                && left.offsetStart() == right.offsetStart()
                && left.offsetEnd() == right.offsetEnd()
                && left.generation() == right.generation()
                && left.commitVersion() == right.commitVersion();
    }

    private String headMapKey(String cluster, StreamId streamId) {
        return cluster + "|head|" + streamId.value();
    }

    private String streamNameMapKey(String cluster, StreamName streamName) {
        return cluster + "|name|" + DeterministicIds.streamNameHash(streamName);
    }

    private String objectMapKey(String cluster, ObjectId objectId) {
        return cluster + "|object|" + objectId.value();
    }

    private String commitMapKey(String cluster, StreamId streamId, String commitId) {
        return cluster + "|commit|" + streamId.value() + "|" + commitId;
    }

    private String commitIdentityMapKey(String cluster, StreamId streamId, String commitId) {
        return commitMapKey(cluster, streamId, commitId);
    }

    private String offsetMapKey(String cluster, StreamId streamId, long offsetEnd, long generation) {
        return new OxiaKeyspace(cluster).offsetIndexKey(streamId, offsetEnd, generation);
    }

    private String offsetMapPrefix(String cluster, StreamId streamId) {
        String key = new OxiaKeyspace(cluster).offsetIndexKey(streamId, 0, 0);
        return key.substring(0, key.length() - "/0000000000000000000/0000000000000000000".length());
    }

    private String committedSliceMapKey(String cluster, StreamId streamId, ObjectId objectId, String sliceId) {
        return new OxiaKeyspace(cluster).committedSliceKey(streamId, objectId, sliceId);
    }

    private <T> CompletableFuture<T> complete(ThrowingSupplier<T> supplier) {
        synchronized (this) {
            if (closed) {
                return failed(failure(ErrorCode.STORAGE_CLOSED, false, "metadata store is closed"));
            }
            try {
                return CompletableFuture.completedFuture(supplier.get());
            } catch (Throwable t) {
                return failed(t);
            }
        }
    }

    private <T> CompletableFuture<T> completeAppend(
            AppendAttemptState attemptState,
            ThrowingSupplier<T> supplier) {
        synchronized (this) {
            if (closed) {
                return failed(appendFailure(
                        ErrorCode.STORAGE_CLOSED,
                        false,
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "metadata store is closed"));
            }
            try {
                return CompletableFuture.completedFuture(supplier.get());
            } catch (NereusException e) {
                if (e.appendOutcome().isPresent()
                        && (attemptState.failureOutcome() != AppendOutcome.KNOWN_COMMITTED
                                || e.appendOutcome().orElseThrow() == AppendOutcome.KNOWN_COMMITTED)) {
                    return failed(e);
                }
                return failed(new NereusException(
                        e.code(),
                        e.retriable(),
                        e.getMessage(),
                        e,
                        attemptState.failureOutcome()));
            } catch (Throwable t) {
                return failed(new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "unexpected append state-machine failure",
                        t,
                        attemptState.failureOutcome()));
            }
        }
    }

    private static <T> CompletableFuture<T> failed(Throwable throwable) {
        return CompletableFuture.failedFuture(throwable);
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message) {
        return new NereusException(code, retriable, message);
    }

    private static NereusException appendFailure(
            ErrorCode code,
            boolean retriable,
            AppendOutcome outcome,
            String message) {
        return new NereusException(code, retriable, message, outcome);
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    private interface WatchEventFactory {
        WatchEvent event(long metadataVersion);
    }

    @FunctionalInterface
    private interface WatchEvent {
        void deliver(MetadataWatcher watcher);
    }

    private static final class CodecBackedRecordMap<T> {
        private final Class<T> recordClass;
        private final Map<String, byte[]> values = new HashMap<>();

        private CodecBackedRecordMap(Class<T> recordClass) {
            this.recordClass = Objects.requireNonNull(recordClass, "recordClass");
        }

        T get(String key) {
            byte[] encoded = values.get(Objects.requireNonNull(key, "key"));
            return encoded == null ? null : decode(encoded);
        }

        void put(String key, T record) {
            values.put(Objects.requireNonNull(key, "key"), encode(record));
        }

        void putIfAbsent(String key, T record) {
            values.putIfAbsent(Objects.requireNonNull(key, "key"), encode(record));
        }

        List<Map.Entry<String, T>> entrySet() {
            return values.entrySet().stream()
                    .map(entry -> Map.entry(entry.getKey(), decode(entry.getValue())))
                    .toList();
        }

        List<StoredMetadataValue> storedValues() {
            return values.entrySet().stream()
                    .map(entry -> new StoredMetadataValue(
                            entry.getKey(),
                            recordClass.getSimpleName(),
                            entry.getValue()))
                    .toList();
        }

        private byte[] encode(T record) {
            return Phase1MetadataCodecs.encodeEnvelope(record, recordClass);
        }

        private T decode(byte[] encoded) {
            return Phase1MetadataCodecs.decodeEnvelope(encoded, recordClass);
        }
    }
}
