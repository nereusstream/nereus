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

package io.nereus.metadata.oxia.testing;

import io.nereus.api.AppendSessionOptions;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.ObjectId;
import io.nereus.api.ObjectType;
import io.nereus.api.OffsetRange;
import io.nereus.api.ProjectionRef;
import io.nereus.api.StreamCreateOptions;
import io.nereus.api.StreamId;
import io.nereus.api.StreamName;
import io.nereus.api.StreamState;
import io.nereus.api.keys.DeterministicIds;
import io.nereus.metadata.oxia.CommitSliceRequest;
import io.nereus.metadata.oxia.CommitSliceResult;
import io.nereus.metadata.oxia.DerivedIndexRepairResult;
import io.nereus.metadata.oxia.MetadataWatcher;
import io.nereus.metadata.oxia.OxiaKeyspace;
import io.nereus.metadata.oxia.OxiaMetadataStore;
import io.nereus.metadata.oxia.WatchRegistration;
import io.nereus.metadata.oxia.codec.Phase1MetadataCodecs;
import io.nereus.metadata.oxia.records.AppendSessionRecord;
import io.nereus.metadata.oxia.records.AppendSessionSnapshotRecord;
import io.nereus.metadata.oxia.records.CommittedEndOffsetRecord;
import io.nereus.metadata.oxia.records.CommittedSliceRecord;
import io.nereus.metadata.oxia.records.EntryIndexReferenceRecord;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.metadata.oxia.records.ObjectReferenceRecord;
import io.nereus.metadata.oxia.records.OffsetIndexRecord;
import io.nereus.metadata.oxia.records.StreamCommitRecord;
import io.nereus.metadata.oxia.records.StreamHeadRecord;
import io.nereus.metadata.oxia.records.StreamMetadataRecord;
import io.nereus.metadata.oxia.records.StreamNameRecord;
import io.nereus.metadata.oxia.records.StreamSliceManifestRecord;
import io.nereus.metadata.oxia.records.TrimRecord;
import io.nereus.metadata.oxia.records.VisibleSliceReferenceRecord;
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

    private static final int REPLAY_SEARCH_LIMIT = 10_000;

    private final LongSupplier clock;
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
    private boolean closed;

    public FakeOxiaMetadataStore(LongSupplier clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
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
                throw failure(ErrorCode.FENCED_APPEND, false, "append session is owned by another writer");
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
                    now + options.ttl().toMillis());
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
                throw failure(ErrorCode.FENCED_APPEND, false, "append session token does not match");
            }
            AppendSessionSnapshotRecord updatedSession = new AppendSessionSnapshotRecord(
                    writerId,
                    epoch,
                    fencingToken,
                    current.leaseVersion() + 1,
                    now + ttl.toMillis());
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
            ObjectId objectId = new ObjectId(manifest.objectId());
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            recordObjectAccess(keyspace.objectManifestKey(objectId), keyspace, objectId, "putIfAbsent");
            String key = objectMapKey(cluster, objectId);
            ObjectManifestRecord existing = objectManifests.get(key);
            if (existing != null && !sameManifestIdentity(existing, manifest)) {
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
                }
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
        return complete(() -> {
            String commitId = request.commitId();
            StreamCommitRecord replay = findCommittedSliceMarkerReplay(cluster, request);
            if (replay == null) {
                replay = findReachableCommit(cluster, request.streamId(), commitId);
            }
            if (replay != null) {
                validateReplay(request, replay);
                materializeDerivedRecords(cluster, replay);
                return resultFromCommit(replay, request.projectionRef());
            }

            maybeFail(FailurePoint.BEFORE_COMMIT_LOG_PUT);
            StreamHeadRecord head = headOrThrow(cluster, request.streamId());
            requireActive(head);
            validateSession(head, request);
            if (head.committedEndOffset() != request.expectedStartOffset()) {
                throw failure(ErrorCode.OFFSET_CONFLICT, true, "expected start offset does not match committed end");
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
                validateReplay(request, existing);
                commit = existing;
            }
            maybeFail(FailurePoint.AFTER_COMMIT_LOG_PUT);
            maybeFail(FailurePoint.BEFORE_HEAD_CAS);

            applyBeforeHeadCasInterleaving(cluster, request.streamId());
            StreamHeadRecord currentHead = headOrThrow(cluster, request.streamId());
            if (currentHead.metadataVersion() != head.metadataVersion()) {
                if (headReaches(cluster, currentHead, commitId)) {
                    validateReplay(request, commit);
                    materializeDerivedRecords(cluster, commit);
                    return resultFromCommit(commit, request.projectionRef());
                }
                validateSession(currentHead, request);
                if (currentHead.committedEndOffset() != request.expectedStartOffset()) {
                    throw failure(ErrorCode.OFFSET_CONFLICT, true, "expected start offset does not match committed end");
                }
            }
            StreamHeadRecord newHead = withCommit(currentHead, commit, nextVersion());
            recordHeadCas(cluster, request.streamId());
            streamHeads.put(headMapKey(cluster, request.streamId()), newHead);
            maybeFail(FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
            materializeDerivedRecords(cluster, commit);
            if (!consumeFailure(FailurePoint.AFTER_DERIVED_INDEX_BEFORE_OBJECT_AUDIT)) {
                updateObjectAuditMetadata(cluster, manifest, commit);
            }
            maybeFail(FailurePoint.AFTER_DERIVED_INDEX_BEFORE_RESPONSE);
            return resultFromCommit(commit, request.projectionRef());
        });
    }

    @Override
    public CompletableFuture<DerivedIndexRepairResult> repairDerivedStreamIndexes(
            String cluster,
            StreamId streamId,
            long targetOffset,
            int maxRecordsToRepair) {
        return complete(() -> {
            if (targetOffset < 0 || maxRecordsToRepair <= 0) {
                throw new IllegalArgumentException("targetOffset must be non-negative and maxRecordsToRepair positive");
            }
            StreamHeadRecord head = headOrThrow(cluster, streamId);
            if (targetOffset >= head.committedEndOffset()) {
                return new DerivedIndexRepairResult(streamId, targetOffset, targetOffset, 0, true, false, head.commitVersion());
            }
            int repaired = 0;
            boolean targetCovered = false;
            long repairedFrom = Long.MAX_VALUE;
            long repairedTo = 0;
            String commitId = head.lastCommitId();
            while (!commitId.isEmpty()) {
                StreamCommitRecord commit = commitById.get(commitIdentityMapKey(cluster, streamId, commitId));
                if (commit == null) {
                    throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "broken commit chain");
                }
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
                if (materialization.repaired() && repaired >= maxRecordsToRepair) {
                    break;
                }
            }
            boolean exhausted = !targetCovered && !commitId.isEmpty();
            long from = repairedFrom == Long.MAX_VALUE ? targetOffset : repairedFrom;
            long to = repairedFrom == Long.MAX_VALUE ? targetOffset : repairedTo;
            return new DerivedIndexRepairResult(streamId, from, to, repaired, targetCovered, exhausted, head.commitVersion());
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
            StreamHeadRecord head = headOrThrow(cluster, streamId);
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
            throw failure(ErrorCode.APPEND_SESSION_EXPIRED, true, "append session expired");
        }
        if (!session.writerId().equals(request.writerId())
                || session.epoch() != request.epoch()
                || !session.fencingToken().equals(request.fencingToken())) {
            throw failure(ErrorCode.FENCED_APPEND, false, "append session token does not match");
        }
    }

    private ObjectManifestRecord validateManifest(String cluster, CommitSliceRequest request, boolean replay) {
        ObjectManifestRecord manifest = objectManifests.get(objectMapKey(cluster, request.objectId()));
        if (manifest == null) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "object manifest is missing");
        }
        if (!manifest.objectKey().equals(request.objectKey().value())
                || !manifest.objectType().equals(ObjectType.MULTI_STREAM_WAL_OBJECT.name())
                || !manifest.writerId().equals(request.writerId())
                || !manifest.writerRunIdHash().equals(request.writerRunIdHash())
                || manifest.writerEpoch() != request.epoch()
                || !manifest.objectChecksumType().equals(request.objectChecksum().type().name())
                || !manifest.objectChecksumValue().equals(request.objectChecksum().value())) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "object manifest does not match commit request");
        }
        StreamSliceManifestRecord slice = findManifestSlice(manifest, request.sliceId());
        if (slice == null) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "object manifest slice is missing");
        }
        if (!replay && !"UPLOADED".equals(slice.state())) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "new commit requires uploaded manifest slice");
        }
        if (slice.writerEpoch() != request.epoch()
                || slice.objectOffset() != request.objectOffset()
                || slice.objectLength() != request.objectLength()
                || slice.recordCount() != request.recordCount()
                || slice.entryCount() != request.entryCount()
                || slice.logicalBytes() != request.logicalBytes()
                || !slice.payloadFormat().equals(request.payloadFormat().name())
                || !slice.schemaRefs().equals(request.schemaRefs())
                || !slice.entryIndexRef().equals(EntryIndexReferenceRecord.fromApi(request.entryIndexRef()))
                || !slice.sliceChecksumType().equals(request.sliceChecksum().type().name())
                || !slice.sliceChecksumValue().equals(request.sliceChecksum().value())) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "object manifest slice does not match commit request");
        }
        return manifest;
    }

    private StreamCommitRecord buildCommit(
            CommitSliceRequest request,
            StreamHeadRecord head,
            ObjectManifestRecord manifest,
            String commitId,
            long metadataVersion) {
        long offsetEnd = Math.addExact(request.expectedStartOffset(), request.recordCount());
        long cumulativeSize = Math.addExact(head.cumulativeSize(), request.logicalBytes());
        return new StreamCommitRecord(
                request.streamId().value(),
                commitId,
                head.lastCommitId(),
                request.expectedStartOffset(),
                offsetEnd,
                0,
                cumulativeSize,
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
                clock.getAsLong(),
                metadataVersion);
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
                    now + interleaveRenewTtl.toMillis());
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

    private StreamCommitRecord findReachableCommit(String cluster, StreamId streamId, String commitId) {
        StreamHeadRecord head = streamHeads.get(headMapKey(cluster, streamId));
        if (head == null) {
            return null;
        }
        String current = head.lastCommitId();
        int searched = 0;
        while (!current.isEmpty() && searched++ < REPLAY_SEARCH_LIMIT) {
            StreamCommitRecord commit = commitById.get(commitIdentityMapKey(cluster, streamId, current));
            if (commit == null) {
                return null;
            }
            if (commit.commitId().equals(commitId)) {
                return commit;
            }
            current = commit.previousCommitId();
        }
        return null;
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
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "committed-slice marker does not match request");
        }
        StreamCommitRecord commit = findReachableCommitForMarker(cluster, streamId, marker);
        if (commit == null) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "committed-slice marker is not reachable from head");
        }
        return commit;
    }

    private StreamCommitRecord findReachableCommitForMarker(
            String cluster,
            StreamId streamId,
            CommittedSliceRecord marker) {
        StreamHeadRecord head = streamHeads.get(headMapKey(cluster, streamId));
        if (head == null) {
            return null;
        }
        String current = head.lastCommitId();
        int searched = 0;
        while (!current.isEmpty() && searched++ < REPLAY_SEARCH_LIMIT) {
            StreamCommitRecord commit = commitById.get(commitIdentityMapKey(cluster, streamId, current));
            if (commit == null) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "broken commit chain");
            }
            if (commitMatchesMarker(commit, marker)) {
                return commit;
            }
            current = commit.previousCommitId();
        }
        return null;
    }

    private StreamCommitRecord findReachableCommitForManifestSlice(
            String cluster,
            ObjectManifestRecord manifest,
            StreamSliceManifestRecord slice) {
        StreamId streamId = new StreamId(slice.streamId());
        StreamHeadRecord head = streamHeads.get(headMapKey(cluster, streamId));
        if (head == null) {
            return null;
        }
        String current = head.lastCommitId();
        int searched = 0;
        while (!current.isEmpty() && searched++ < REPLAY_SEARCH_LIMIT) {
            StreamCommitRecord commit = commitById.get(commitIdentityMapKey(cluster, streamId, current));
            if (commit == null) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "broken commit chain");
            }
            if (commitMatchesManifestSlice(manifest, slice, commit)) {
                return commit;
            }
            current = commit.previousCommitId();
        }
        return null;
    }

    private boolean commitMatchesManifestSlice(
            ObjectManifestRecord manifest,
            StreamSliceManifestRecord slice,
            StreamCommitRecord commit) {
        return commit.streamId().equals(slice.streamId())
                && commit.objectId().equals(manifest.objectId())
                && commit.objectKey().equals(manifest.objectKey())
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

    private boolean headReaches(String cluster, StreamHeadRecord head, String commitId) {
        return findReachableCommit(cluster, new StreamId(head.streamId()), commitId) != null;
    }

    private void validateReplay(CommitSliceRequest request, StreamCommitRecord replay) {
        if (!replay.commitId().equals(request.commitId())
                || !replay.streamId().equals(request.streamId().value())
                || !replay.writerId().equals(request.writerId())
                || !replay.writerRunIdHash().equals(request.writerRunIdHash())
                || replay.writerEpoch() != request.epoch()
                || !replay.fencingTokenHash().equals(request.fencingTokenHash())
                || replay.offsetStart() != request.expectedStartOffset()
                || replay.recordCount() != request.recordCount()
                || replay.entryCount() != request.entryCount()
                || replay.logicalBytes() != request.logicalBytes()
                || !replay.objectId().equals(request.objectId().value())
                || !replay.objectKey().equals(request.objectKey().value())
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
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "replayed commit does not match request");
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
            throw failure(ErrorCode.METADATA_UNAVAILABLE, true, "injected metadata failure at " + point);
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
                watcher.watcher().onWatchReconnected(streamId, version);
            }
        }
        if (delivery == WatchDelivery.STALE_THEN_CURRENT_NEXT) {
            long staleVersion = Math.max(0, version - 1);
            for (RegisteredWatcher watcher : matching) {
                factory.event(staleVersion).deliver(watcher.watcher());
            }
        }
        int deliveries = delivery == WatchDelivery.DUPLICATE_NEXT ? 2 : 1;
        for (int i = 0; i < deliveries; i++) {
            for (RegisteredWatcher watcher : matching) {
                factory.event(version).deliver(watcher.watcher());
            }
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

    private StreamSliceManifestRecord findManifestSlice(ObjectManifestRecord manifest, String sliceId) {
        return manifest.slices().stream()
                .filter(slice -> slice.sliceId().equals(sliceId))
                .findFirst()
                .orElse(null);
    }

    private boolean sameManifestIdentity(ObjectManifestRecord left, ObjectManifestRecord right) {
        return left.objectId().equals(right.objectId())
                && left.objectKey().equals(right.objectKey())
                && left.objectType().equals(right.objectType())
                && left.objectLength() == right.objectLength()
                && left.objectChecksumType().equals(right.objectChecksumType())
                && left.objectChecksumValue().equals(right.objectChecksumValue())
                && left.storageChecksumType().equals(right.storageChecksumType())
                && left.storageChecksumValue().equals(right.storageChecksumValue())
                && left.slices().equals(right.slices());
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

    private static <T> CompletableFuture<T> failed(Throwable throwable) {
        return CompletableFuture.failedFuture(throwable);
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message) {
        return new NereusException(code, retriable, message);
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
