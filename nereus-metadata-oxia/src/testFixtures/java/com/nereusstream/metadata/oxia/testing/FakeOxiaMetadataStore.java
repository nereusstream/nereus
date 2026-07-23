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

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.AppendSessionRequest;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.CommitSliceResult;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailCursor;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.GcRetirementProtectionScanPage;
import com.nereusstream.metadata.oxia.GcRetirementRemovalScanPage;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.ObjectPhysicalReferenceProof;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperPhysicalReferenceProof;
import com.nereusstream.metadata.oxia.BookKeeperStableAppendProtectionValidator;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.PhysicalReferenceProof;
import com.nereusstream.metadata.oxia.PhysicalReferencePurpose;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectRootScanPage;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import com.nereusstream.metadata.oxia.ReaderLeaseScanPage;
import com.nereusstream.metadata.oxia.StableAppendResult;
import com.nereusstream.metadata.oxia.AppendReplayCursor;
import com.nereusstream.metadata.oxia.AppendReplayRecords;
import com.nereusstream.metadata.oxia.AppendReplaySearchResult;
import com.nereusstream.metadata.oxia.AppendReplayStatus;
import com.nereusstream.metadata.oxia.AppendAuthoritySessionTransitions;
import com.nereusstream.metadata.oxia.StreamStateTransitionRequest;
import com.nereusstream.metadata.oxia.DerivedIndexRepairCursor;
import com.nereusstream.metadata.oxia.DerivedIndexRepairResult;
import com.nereusstream.metadata.oxia.MetadataWatcher;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedGcRetirementManifest;
import com.nereusstream.metadata.oxia.VersionedGcRetirementProtection;
import com.nereusstream.metadata.oxia.VersionedGcRetirementRemoval;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedReaderLease;
import com.nereusstream.metadata.oxia.Phase1ObjectManifestValidator;
import com.nereusstream.metadata.oxia.WatchRegistration;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.CommittedAppendRecord;
import com.nereusstream.metadata.oxia.records.CommittedSliceRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexTargetRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamHeadRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.StreamNameRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.metadata.oxia.records.VisibleSliceReferenceRecord;
import java.time.Duration;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/** In-memory metadata store with the same single-key stream-head CAS semantics required for Phase 1. */
public final class FakeOxiaMetadataStore implements OxiaMetadataStore, PhysicalObjectMetadataStore {
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

    private record TargetChainView(
            String streamId, String commitId, String previousCommitId,
            long offsetStart, long offsetEnd, long cumulativeSize, long commitVersion, long logicalBytes) {
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
    private final CodecBackedRecordMap<StreamCommitTargetRecord> targetCommitByKey =
            new CodecBackedRecordMap<>(StreamCommitTargetRecord.class);
    private final CodecBackedRecordMap<StreamCommitTargetRecord> targetCommitById =
            new CodecBackedRecordMap<>(StreamCommitTargetRecord.class);
    private final CodecBackedRecordMap<OffsetIndexRecord> offsetIndexes =
            new CodecBackedRecordMap<>(OffsetIndexRecord.class);
    private final CodecBackedRecordMap<CommittedSliceRecord> committedSlices =
            new CodecBackedRecordMap<>(CommittedSliceRecord.class);
    private final CodecBackedRecordMap<OffsetIndexTargetRecord> targetOffsetIndexes =
            new CodecBackedRecordMap<>(OffsetIndexTargetRecord.class);
    private final CodecBackedRecordMap<CommittedAppendRecord> committedAppends =
            new CodecBackedRecordMap<>(CommittedAppendRecord.class);
    private final CodecBackedRecordMap<ObjectManifestRecord> objectManifests =
            new CodecBackedRecordMap<>(ObjectManifestRecord.class);
    private final CodecBackedRecordMap<ObjectReferenceRecord> objectReferences =
            new CodecBackedRecordMap<>(ObjectReferenceRecord.class);
    private final FakePhysicalObjectMetadataStore physicalObjects = new FakePhysicalObjectMetadataStore();
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
    private final BookKeeperStableAppendProtectionValidator bookKeeperProtectionValidator;
    private boolean closed;

    public FakeOxiaMetadataStore(LongSupplier clock) {
        this(clock, DEFAULT_COMMIT_CHAIN_SCAN_LIMIT);
    }

    public FakeOxiaMetadataStore(LongSupplier clock, int maxCommitChainScan) {
        this(clock, maxCommitChainScan, null);
    }

    public FakeOxiaMetadataStore(
            LongSupplier clock,
            BookKeeperLedgerMetadataStore bookKeeperMetadata,
            BookKeeperMetadataStoreConfig bookKeeperConfiguration) {
        this(clock, DEFAULT_COMMIT_CHAIN_SCAN_LIMIT,
                new BookKeeperStableAppendProtectionValidator(
                        bookKeeperMetadata, bookKeeperConfiguration));
    }

    private FakeOxiaMetadataStore(
            LongSupplier clock,
            int maxCommitChainScan,
            BookKeeperStableAppendProtectionValidator bookKeeperProtectionValidator) {
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxCommitChainScan <= 0) {
            throw new IllegalArgumentException("maxCommitChainScan must be positive");
        }
        this.maxCommitChainScan = maxCommitChainScan;
        this.bookKeeperProtectionValidator = bookKeeperProtectionValidator;
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
        values.addAll(targetCommitByKey.storedValues());
        values.addAll(targetCommitById.storedValues());
        values.addAll(offsetIndexes.storedValues());
        values.addAll(committedSlices.storedValues());
        values.addAll(targetOffsetIndexes.storedValues());
        values.addAll(committedAppends.storedValues());
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
    public CompletableFuture<Void> revalidateAppendSession(
            String cluster,
            AppendSession session) {
        return complete(() -> {
            AppendSession expected = Objects.requireNonNull(session, "session");
            StreamHeadRecord head = headOrThrow(cluster, expected.streamId());
            requireActive(head);
            AppendSessionSnapshotRecord current = head.appendSession();
            long now = clock.getAsLong();
            if (current.isEmpty() || current.expiresAtMillis() <= now) {
                throw failure(
                        ErrorCode.APPEND_SESSION_EXPIRED,
                        true,
                        "append session expired before guarded object upload");
            }
            if (!current.writerId().equals(expected.writerId())
                    || current.epoch() != expected.epoch()
                    || !current.fencingToken().equals(expected.fencingToken())
                    || current.leaseVersion() < expected.leaseVersion()) {
                throw failure(
                        ErrorCode.FENCED_APPEND,
                        false,
                        "append session changed before guarded object upload");
            }
            return null;
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
            AppendAuthoritySessionTransitions.requireLegacyMode(head);
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
    public CompletableFuture<AppendSessionRecord> acquireAppendSession(
            String cluster,
            StreamId streamId,
            AppendSessionRequest request) {
        Objects.requireNonNull(request, "request");
        if (request.authority().isEmpty()) {
            return acquireAppendSession(cluster, streamId, request.options());
        }
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(cluster, streamId);
            requireActive(head);
            long now = clock.getAsLong();
            AppendSessionSnapshotRecord updatedSession = AppendAuthoritySessionTransitions.acquire(
                    head,
                    request.options(),
                    request.authority().orElseThrow(),
                    now,
                    leaseExpiration(now, request.options().ttl()),
                    epoch -> newFencingToken(streamId, request.options().writerId(), epoch));
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
            AppendSessionSnapshotRecord updatedSession =
                    AppendAuthoritySessionTransitions.preserveAuthorityOnRenewal(
                            current, leaseExpiration(now, ttl));
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
                VisibleSliceReferenceRecord reference = findReachableCommitForManifestSlice(cluster, manifest, slice);
                if (reference != null) {
                    visibleSlices.add(reference);
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

    @Override
    public CompletableFuture<PreparedStableAppend> prepareStableAppend(
            String cluster, CommitAppendRequest request) {
        AppendAttemptState attemptState = new AppendAttemptState();
        return completeAppend(attemptState, () -> {
            String commitId = request.commitId();
            StreamHeadRecord head = headOrThrow(cluster, request.streamId());
            CommittedAppendRecord marker = committedAppends.get(
                    new OxiaKeyspace(cluster).committedAppendKey(request.streamId(), commitId));
            if (marker != null) {
                StreamCommitTargetRecord commit = targetCommitById.get(
                        commitIdentityMapKey(cluster, request.streamId(), commitId));
                if (commit == null) throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "generic marker has no commit");
                validateTargetReplay(request, commit, AppendOutcome.KNOWN_COMMITTED);
                return preparedStableAppend(cluster, request, commit, true);
            }
            if (head.committedEndOffset() > request.expectedStartOffset()) {
                AppendReplaySearchResult replay = searchTargetReplaySync(cluster, request, Optional.empty(), maxCommitChainScan);
                if (replay.status() == AppendReplayStatus.FOUND) {
                    StreamCommitTargetRecord commit = targetCommitById.get(
                            commitIdentityMapKey(cluster, request.streamId(), commitId));
                    if (commit == null) {
                        throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                "reachable generic commit intent is absent");
                    }
                    return preparedStableAppend(cluster, request, commit, true);
                }
                if (replay.status() == AppendReplayStatus.CONTINUE) {
                    throw appendFailure(ErrorCode.METADATA_CONDITION_FAILED, true,
                            AppendOutcome.MAY_HAVE_COMMITTED, "append replay requires continuation");
                }
            }
            validateTargetPreconditions(head, request);
            maybeFail(FailurePoint.BEFORE_COMMIT_LOG_PUT);
            StreamCommitTargetRecord commit = buildTargetCommit(request, head, nextVersion());
            String key = commitMapKey(cluster, request.streamId(), commitId);
            StreamCommitTargetRecord existing = targetCommitByKey.get(key);
            if (existing == null) {
                targetCommitByKey.put(key, commit);
                targetCommitById.put(commitIdentityMapKey(cluster, request.streamId(), commitId), commit);
            } else {
                validateTargetReplay(request, existing, AppendOutcome.KNOWN_NOT_COMMITTED);
                commit = existing;
            }
            maybeFail(FailurePoint.AFTER_COMMIT_LOG_PUT);
            StreamHeadRecord current = headOrThrow(cluster, request.streamId());
            if (!sameCommitAnchor(head, current)) {
                AppendReplaySearchResult replay = searchTargetReplaySync(
                        cluster,
                        request,
                        Optional.empty(),
                        maxCommitChainScan);
                if (replay.status() == AppendReplayStatus.FOUND) {
                    return preparedStableAppend(cluster, request, commit, true);
                }
                if (replay.status() == AppendReplayStatus.CONTINUE) {
                    throw appendFailure(ErrorCode.METADATA_CONDITION_FAILED, true,
                            AppendOutcome.MAY_HAVE_COMMITTED, "append replay requires continuation");
                }
            }
            validateTargetPreconditions(current, request);
            validateTargetAgainstHead(request, commit, current);
            return preparedStableAppend(cluster, request, commit, false);
        });
    }

    @Override
    public CompletableFuture<StableAppendResult> commitPreparedStableAppend(
            String cluster,
            PreparedStableAppend prepared,
            PhysicalReferenceProof protectionProof) {
        AppendAttemptState attemptState = new AppendAttemptState();
        return completeAppend(attemptState, () -> {
            CommitAppendRequest request = prepared.request();
            StreamCommitTargetRecord commit = requireExactPreparedIntent(cluster, prepared);
            validateStableProtection(
                    cluster,
                    prepared,
                    protectionProof);
            String markerKey = new OxiaKeyspace(cluster).committedAppendKey(
                    request.streamId(),
                    request.commitId());
            if (committedAppends.get(markerKey) != null) {
                StreamHeadRecord head = headOrThrow(cluster, request.streamId());
                validateTargetReplay(request, commit, AppendOutcome.KNOWN_COMMITTED);
                attemptState.markCommitted();
                return new StableAppendResult(targetReachable(commit, request.projectionRef(), head), false);
            }
            StreamHeadRecord head = headOrThrow(cluster, request.streamId());
            if (head.committedEndOffset() > request.expectedStartOffset()) {
                AppendReplaySearchResult replay = searchTargetReplaySync(
                        cluster,
                        request,
                        Optional.empty(),
                        maxCommitChainScan);
                if (replay.status() == AppendReplayStatus.FOUND) {
                    attemptState.markCommitted();
                    return new StableAppendResult(replay.committedAppend().orElseThrow(), false);
                }
                if (replay.status() == AppendReplayStatus.CONTINUE) {
                    throw appendFailure(ErrorCode.METADATA_CONDITION_FAILED, true,
                            AppendOutcome.MAY_HAVE_COMMITTED, "append replay requires continuation");
                }
            }
            validateTargetPreconditions(head, request);
            maybeFail(FailurePoint.BEFORE_HEAD_CAS);
            applyBeforeHeadCasInterleaving(cluster, request.streamId());
            StreamHeadRecord current = headOrThrow(cluster, request.streamId());
            commit = requireExactPreparedIntent(cluster, prepared);
            validateStableProtection(
                    cluster,
                    prepared,
                    protectionProof);
            validateTargetPreconditions(current, request);
            validateTargetAgainstHead(request, commit, current);
            StreamHeadRecord updated = withTargetCommit(current, commit, nextVersion());
            recordHeadCas(cluster, request.streamId());
            streamHeads.put(headMapKey(cluster, request.streamId()), updated);
            attemptState.markCommitted();
            maybeFail(FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
            return new StableAppendResult(targetReachable(commit, request.projectionRef(), updated), true);
        });
    }

    @Override
    public CompletableFuture<MaterializedGenerationZero> materializeGenerationZero(
            String cluster, ReachableCommittedAppend reachableAppend) {
        AppendAttemptState state = new AppendAttemptState();
        state.markCommitted();
        return completeAppend(state, () -> {
            CommittedAppend append = reachableAppend.committedAppend();
            StreamCommitTargetRecord commit = targetCommitById.get(
                    commitIdentityMapKey(cluster, append.streamId(), append.commitId()));
            if (commit == null || !targetCommitted(commit, append.projectionRef()).equals(append)) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "reachable generic commit proof conflicts with durable state");
            }
            requireTargetReachable(cluster, append);
            String indexKey = offsetMapKey(cluster, append.streamId(), append.range().endOffset(), 0);
            OffsetIndexTargetRecord index = new OffsetIndexTargetRecord(
                    commit.streamId(), commit.offsetStart(), commit.offsetEnd(), 0, commit.cumulativeSize(),
                    commit.readTarget(), commit.payloadFormat(), commit.recordCount(), commit.entryCount(),
                    commit.logicalBytes(), commit.schemaRefs(), commit.projectionRef(), commit.minEventTimeMillis(),
                    commit.maxEventTimeMillis(), commit.commitVersion(), false, nextVersion());
            OffsetIndexTargetRecord oldIndex = targetOffsetIndexes.get(indexKey);
            if (oldIndex != null && !withoutVersion(oldIndex).equals(withoutVersion(index))) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "generic offset index conflict");
            }
            targetOffsetIndexes.putIfAbsent(indexKey, index);
            String markerKey = new OxiaKeyspace(cluster).committedAppendKey(append.streamId(), append.commitId());
            CommittedAppendRecord marker = new CommittedAppendRecord(
                    commit.streamId(), commit.commitId(), commit.offsetStart(), commit.offsetEnd(), 0,
                    commit.commitVersion(), commit.readTarget().identityChecksumValue(), nextVersion());
            CommittedAppendRecord oldMarker = committedAppends.get(markerKey);
            if (oldMarker != null && !withoutVersion(oldMarker).equals(withoutVersion(marker))) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "generic append marker conflict");
            }
            committedAppends.putIfAbsent(markerKey, marker);
            maybeFail(FailurePoint.AFTER_DERIVED_INDEX_BEFORE_RESPONSE);
            OffsetIndexTargetRecord durableIndex = targetOffsetIndexes.get(indexKey);
            return new MaterializedGenerationZero(
                    append,
                    indexKey,
                    durableIndex.metadataVersion(),
                    sha256(targetOffsetIndexes.envelope(indexKey)));
        });
    }

    @Override
    public CompletableFuture<Void> revalidateMaterializedGenerationZero(
            String cluster, MaterializedGenerationZero materialized) {
        AppendAttemptState state = new AppendAttemptState();
        state.markCommitted();
        return completeAppend(state, () -> {
            CommittedAppend append = materialized.committedAppend();
            StreamCommitTargetRecord commit = targetCommitById.get(
                    commitIdentityMapKey(cluster, append.streamId(), append.commitId()));
            if (commit == null || !targetCommitted(commit, append.projectionRef()).equals(append)) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "materialized generation-zero proof conflicts with its commit");
            }
            requireTargetReachable(cluster, append);
            String indexKey = offsetMapKey(cluster, append.streamId(), append.range().endOffset(), 0);
            OffsetIndexTargetRecord index = targetOffsetIndexes.get(indexKey);
            if (!materialized.indexKey().equals(indexKey)
                    || index == null
                    || index.metadataVersion() != materialized.indexMetadataVersion()
                    || !sha256(targetOffsetIndexes.envelope(indexKey)).equals(materialized.indexRecordSha256())
                    || !withoutVersion(index).equals(withoutVersion(generationZeroIndex(commit, 0)))) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "generation-zero index changed during protection");
            }
            return null;
        });
    }

    private PreparedStableAppend preparedStableAppend(
            String cluster,
            CommitAppendRequest request,
            StreamCommitTargetRecord commit,
            boolean replayWasReachable) {
        String key = commitMapKey(cluster, request.streamId(), request.commitId());
        validateTargetReplay(request, commit, AppendOutcome.KNOWN_NOT_COMMITTED);
        return new PreparedStableAppend(
                request,
                request.commitId(),
                key,
                commit.metadataVersion(),
                sha256(targetCommitByKey.envelope(key)),
                com.nereusstream.api.ReadTargetIdentities.sha256(request.readTarget()),
                replayWasReachable);
    }

    private StreamCommitTargetRecord requireExactPreparedIntent(
            String cluster,
            PreparedStableAppend prepared) {
        String key = commitMapKey(
                cluster,
                prepared.request().streamId(),
                prepared.request().commitId());
        StreamCommitTargetRecord commit = targetCommitByKey.get(key);
        if (!prepared.commitKey().equals(key)
                || !prepared.commitId().equals(prepared.request().commitId())
                || commit == null) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "prepared stable append intent is absent or non-canonical");
        }
        validateTargetReplay(
                prepared.request(),
                commit,
                AppendOutcome.KNOWN_NOT_COMMITTED);
        if (commit.metadataVersion() != prepared.commitMetadataVersion()
                || !sha256(targetCommitByKey.envelope(key)).equals(prepared.commitRecordSha256())) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "prepared stable append intent changed before head commit");
        }
        return commit;
    }

    private void validateStableProtection(
            String cluster,
            PreparedStableAppend prepared,
            PhysicalReferenceProof protectionProof) {
        if (protectionProof instanceof BookKeeperPhysicalReferenceProof proof) {
            if (bookKeeperProtectionValidator == null) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "BookKeeper physical-reference proof validation is not installed");
            }
            bookKeeperProtectionValidator.validate(cluster, prepared, proof).join();
            return;
        }
        if (!(protectionProof instanceof ObjectPhysicalReferenceProof proof)) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "primary physical-reference proof type is not installed");
        }
        if (proof.purpose() != PhysicalReferencePurpose.REACHABLE_APPEND
                || proof.targetType() != prepared.request().readTarget().type()
                || !proof.targetIdentitySha256().equals(prepared.primaryTargetIdentitySha256())) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "stable append physical-reference proof is non-canonical");
        }
        ObjectProtectionIdentity identity = proof.protectionIdentity();
        long rootMetadataVersion = proof.rootMetadataVersion();
        long rootLifecycleEpoch = proof.rootLifecycleEpoch();
        long protectionMetadataVersion = proof.protectionMetadataVersion();
        Checksum protectionRecordSha256 = proof.protectionRecordSha256();
        String expectedReferenceId = reachableAppendReferenceId(prepared);
        if (identity == null
                || !identity.object().equals(prepared.objectKeyHash())
                || identity.type() != ObjectProtectionType.REACHABLE_APPEND
                || !identity.referenceId().equals(expectedReferenceId)) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "stable append protection identity is non-canonical");
        }
        VersionedPhysicalObjectRoot root = physicalObjects.getRoot(
                        cluster,
                        prepared.objectKeyHash())
                .join()
                .orElseThrow(() -> failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "physical Object WAL root is absent before head commit"));
        if (root.metadataVersion() != rootMetadataVersion
                || root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                || root.value().lifecycleEpoch() != rootLifecycleEpoch) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "physical Object WAL root changed before head commit");
        }
        requireRootMatchesPreparedTarget(cluster, prepared, root.value());
        VersionedObjectProtection protection = physicalObjects.protection(cluster, identity)
                .orElseThrow(() -> failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "REACHABLE_APPEND protection is absent before head commit"));
        ObjectProtectionRecord value = protection.value();
        if (protection.metadataVersion() != protectionMetadataVersion
                || !protection.durableValueSha256().equals(protectionRecordSha256)
                || value.protectionTypeId() != ObjectProtectionType.REACHABLE_APPEND.wireId()
                || !value.objectKeyHash().equals(prepared.objectKeyHash().value())
                || !value.referenceId().equals(expectedReferenceId)
                || !value.ownerKey().equals(prepared.commitKey())
                || value.ownerMetadataVersion() != prepared.commitMetadataVersion()
                || !value.ownerIdentitySha256().equals(prepared.commitRecordSha256().value())
                || value.rootLifecycleEpoch() != rootLifecycleEpoch
                || value.expiresAtMillis() != 0) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "REACHABLE_APPEND protection changed before head commit");
        }
    }

    private void requireRootMatchesPreparedTarget(
            String cluster,
            PreparedStableAppend prepared,
            PhysicalObjectRootRecord root) {
        ObjectSliceReadTarget target = (ObjectSliceReadTarget) prepared.request().readTarget();
        ObjectManifestRecord manifest = objectManifests.get(
                objectMapKey(cluster, target.objectId()));
        if (manifest == null) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "Object WAL manifest is absent before head commit");
        }
        long targetEnd = Math.addExact(target.objectOffset(), target.objectLength());
        String expectedContentSha = "SHA256".equals(manifest.objectChecksumType())
                ? manifest.objectChecksumValue()
                : "";
        if (!root.objectKeyHash().equals(prepared.objectKeyHash().value())
                || !root.objectKey().equals(target.objectKey().value())
                || !root.objectId().equals(target.objectId().value())
                || root.objectKindId() != 1
                || root.objectLength() != manifest.objectLength()
                || targetEnd > root.objectLength()
                || !root.storageChecksumType().equals(manifest.storageChecksumType())
                || !root.storageChecksumValue().equals(manifest.storageChecksumValue())
                || !root.contentSha256().equals(expectedContentSha)) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "physical Object WAL root conflicts with manifest/target identity");
        }
    }

    private static String reachableAppendReferenceId(PreparedStableAppend prepared) {
        return "ra1-" + DeterministicIds.stableHashComponent(
                prepared.request().streamId().value()
                        + prepared.commitId()
                        + prepared.objectKeyHash().value());
    }

    private static OffsetIndexTargetRecord generationZeroIndex(
            StreamCommitTargetRecord commit,
            long metadataVersion) {
        return new OffsetIndexTargetRecord(
                commit.streamId(),
                commit.offsetStart(),
                commit.offsetEnd(),
                0,
                commit.cumulativeSize(),
                commit.readTarget(),
                commit.payloadFormat(),
                commit.recordCount(),
                commit.entryCount(),
                commit.logicalBytes(),
                commit.schemaRefs(),
                commit.projectionRef(),
                commit.minEventTimeMillis(),
                commit.maxEventTimeMillis(),
                commit.commitVersion(),
                false,
                metadataVersion);
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private void requireTargetReachable(String cluster, CommittedAppend expected) {
        StreamHeadRecord head = headOrThrow(cluster, expected.streamId());
        String commitId = head.lastCommitId();
        CommitChainExpectation expectation = expectationFromHead(head);
        int scanned = 0;
        while (!commitId.isEmpty() && scanned++ < maxCommitChainScan) {
            Object durable = anyCommit(cluster, expected.streamId(), commitId);
            if (durable == null) throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "reachability proof found a broken chain");
            TargetChainView view = targetChainView(durable);
            if (view.offsetEnd() != expectation.offsetEnd()
                    || view.cumulativeSize() != expectation.cumulativeSize()
                    || view.commitVersion() != expectation.commitVersion()) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "reachability proof found an inconsistent chain");
            }
            if (commitId.equals(expected.commitId())) return;
            if (view.offsetStart() <= expected.range().startOffset()) break;
            commitId = view.previousCommitId();
            expectation = new CommitChainExpectation(
                    view.offsetStart(), view.cumulativeSize() - view.logicalBytes(), view.commitVersion() - 1);
        }
        throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                "commit is not reachable from the current head");
    }

    @Override
    public CompletableFuture<AppendReplaySearchResult> searchAppendReplay(
            String cluster, CommitAppendRequest request, Optional<AppendReplayCursor> continuation,
            int maxCommitsToScan) {
        return complete(() -> searchTargetReplaySync(cluster, request, continuation, maxCommitsToScan));
    }

    @Override
    public CompletableFuture<AppendRecoveryTailPage> readAppendRecoveryTail(
            String cluster,
            StreamId streamId,
            AppendRecoveryAnchor anchor,
            Optional<AppendRecoveryTailCursor> continuation,
            int maxCommitsToScan) {
        return complete(() -> readAppendRecoveryTailSync(
                cluster, streamId, anchor, continuation, maxCommitsToScan));
    }

    private AppendRecoveryTailPage readAppendRecoveryTailSync(
            String cluster,
            StreamId streamId,
            AppendRecoveryAnchor anchor,
            Optional<AppendRecoveryTailCursor> continuation,
            int maxCommitsToScan) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(anchor, "anchor");
        Objects.requireNonNull(continuation, "continuation");
        if (!anchor.streamId().equals(streamId)
                || maxCommitsToScan <= 0
                || maxCommitsToScan > 1_000) {
            throw new IllegalArgumentException("invalid fake append recovery page request");
        }
        StreamHeadRecord currentHead = headOrThrow(cluster, streamId);
        if (anchor.commitVersion() > currentHead.commitVersion()
                || anchor.offsetEnd() > currentHead.committedEndOffset()
                || anchor.cumulativeSize() > currentHead.cumulativeSize()) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "append recovery anchor is ahead of the current stream head");
        }
        AppendRecoveryHead observedHead = recoveryHead(streamId, currentHead);
        String nextCommitId = currentHead.lastCommitId();
        CommitChainExpectation expectation = expectationFromHead(currentHead);
        if (continuation.isPresent()) {
            AppendRecoveryTailCursor cursor = continuation.orElseThrow();
            validateRecoveryTailCursor(cluster, streamId, anchor, currentHead, cursor);
            observedHead = cursor.observedHead();
            nextCommitId = cursor.nextCommitId();
            expectation = new CommitChainExpectation(
                    cursor.nextOffsetEnd(),
                    cursor.nextCumulativeSize(),
                    cursor.nextCommitVersion());
        }

        List<AppendRecoveryCommit> commits = new ArrayList<>(maxCommitsToScan);
        while (commits.size() < maxCommitsToScan) {
            if (nextCommitId.equals(anchor.lastCommitId())) {
                requireRecoveryBridge(anchor, expectation);
                return new AppendRecoveryTailPage(
                        anchor, observedHead, commits, true, Optional.empty());
            }
            if (nextCommitId.isEmpty()) {
                if (!anchor.isGenesis()) {
                    throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                            "fake live commit tail ended before the recovery anchor");
                }
                requireRecoveryBridge(anchor, expectation);
                return new AppendRecoveryTailPage(
                        anchor, observedHead, commits, true, Optional.empty());
            }
            Object durable = anyCommit(cluster, streamId, nextCommitId);
            if (durable == null) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "fake anchor-aware walk found a missing live commit");
            }
            TargetChainView view = targetChainView(durable);
            if (!view.streamId().equals(streamId.value())
                    || !view.commitId().equals(nextCommitId)
                    || view.offsetEnd() != expectation.offsetEnd()
                    || view.cumulativeSize() != expectation.cumulativeSize()
                    || view.commitVersion() != expectation.commitVersion()) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "fake anchor-aware walk found an inconsistent live commit");
            }
            commits.add(recoveryCommit(cluster, streamId, durable));
            expectation = predecessorExpectation(view);
            nextCommitId = view.previousCommitId();
        }
        if (nextCommitId.equals(anchor.lastCommitId())) {
            requireRecoveryBridge(anchor, expectation);
            return new AppendRecoveryTailPage(
                    anchor, observedHead, commits, true, Optional.empty());
        }
        AppendRecoveryTailCursor cursor = new AppendRecoveryTailCursor(
                streamId,
                anchor,
                observedHead,
                nextCommitId,
                expectation.offsetEnd(),
                expectation.cumulativeSize(),
                expectation.commitVersion());
        return new AppendRecoveryTailPage(
                anchor, observedHead, commits, false, Optional.of(cursor));
    }

    private AppendRecoveryCommit recoveryCommit(
            String cluster,
            StreamId streamId,
            Object durable) {
        StreamCommitTargetRecord canonical;
        AppendRecoveryCommitEncoding encoding;
        long metadataVersion;
        byte[] sourceBytes;
        String key = new OxiaKeyspace(cluster).streamCommitKey(
                streamId, targetChainView(durable).commitId());
        if (durable instanceof StreamCommitTargetRecord target) {
            canonical = withoutVersion(target);
            encoding = AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1;
            metadataVersion = target.metadataVersion();
            sourceBytes = targetCommitByKey.envelope(key);
        } else {
            StreamCommitRecord legacy = (StreamCommitRecord) durable;
            canonical = canonicalRecoveryCommit(legacy);
            encoding = AppendRecoveryCommitEncoding.LEGACY_STREAM_COMMIT_V1;
            metadataVersion = legacy.metadataVersion();
            sourceBytes = commitByKey.envelope(key);
        }
        byte[] canonicalBytes = MetadataRecordCodecFactory.encodeEnvelope(
                canonical, StreamCommitTargetRecord.class);
        return new AppendRecoveryCommit(
                key,
                encoding,
                canonical,
                metadataVersion,
                sha256(sourceBytes),
                ByteBuffer.wrap(canonicalBytes),
                sha256(canonicalBytes));
    }

    private static StreamCommitTargetRecord withoutVersion(StreamCommitTargetRecord value) {
        return new StreamCommitTargetRecord(
                value.streamId(), value.commitId(), value.previousCommitId(),
                value.offsetStart(), value.offsetEnd(), value.generation(),
                value.cumulativeSize(), value.commitVersion(), value.writerId(),
                value.writerRunIdHash(), value.writerEpoch(), value.fencingTokenHash(),
                value.readTarget(), value.payloadFormat(), value.recordCount(),
                value.entryCount(), value.logicalBytes(), value.schemaRefs(),
                value.projectionRef(), value.minEventTimeMillis(), value.maxEventTimeMillis(),
                value.preparedAtMillis(), 0);
    }

    private static StreamCommitTargetRecord canonicalRecoveryCommit(StreamCommitRecord value) {
        EntryIndexReferenceRecord rawIndex = value.entryIndexRef();
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.valueOf(rawIndex.location()),
                rawIndex.objectId().isEmpty()
                        ? Optional.empty()
                        : Optional.of(new ObjectId(rawIndex.objectId())),
                rawIndex.objectKey().isEmpty()
                        ? Optional.empty()
                        : Optional.of(new ObjectKey(rawIndex.objectKey())),
                rawIndex.inlineData().length == 0
                        ? Optional.empty()
                        : Optional.of(rawIndex.inlineData()),
                rawIndex.offset(), rawIndex.length(),
                new Checksum(
                        ChecksumType.valueOf(rawIndex.checksumType()),
                        rawIndex.checksumValue()));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                new ObjectId(value.objectId()),
                new ObjectKey(value.objectKey()),
                ObjectType.valueOf(value.objectType()),
                value.physicalFormat(),
                value.logicalFormat(),
                value.sliceId(),
                value.objectOffset(),
                value.objectLength(),
                new Checksum(
                        ChecksumType.valueOf(value.sliceChecksumType()),
                        value.sliceChecksumValue()),
                index);
        ReadTargetRecord readTarget = ReadTargetCodecRegistry.phase15().encode(target);
        return new StreamCommitTargetRecord(
                value.streamId(), value.commitId(), value.previousCommitId(),
                value.offsetStart(), value.offsetEnd(), 0, value.cumulativeSize(),
                value.commitVersion(), value.writerId(), value.writerRunIdHash(),
                value.writerEpoch(), value.fencingTokenHash(), readTarget,
                value.payloadFormat(), value.recordCount(), value.entryCount(),
                value.logicalBytes(), value.schemaRefs(), value.projectionRef(),
                value.minEventTimeMillis(), value.maxEventTimeMillis(),
                value.preparedAtMillis(), 0);
    }

    private void validateRecoveryTailCursor(
            String cluster,
            StreamId streamId,
            AppendRecoveryAnchor anchor,
            StreamHeadRecord currentHead,
            AppendRecoveryTailCursor cursor) {
        if (!cursor.streamId().equals(streamId)
                || !cursor.anchor().equals(anchor)
                || cursor.observedHead().commitVersion() > currentHead.commitVersion()) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "fake append recovery continuation changed its anchors");
        }
        AppendRecoveryHead observed = cursor.observedHead();
        Object durable = anyCommit(cluster, streamId, observed.lastCommitId());
        if (durable == null) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "fake append recovery continuation head is missing");
        }
        TargetChainView view = targetChainView(durable);
        if (view.offsetEnd() != observed.offsetEnd()
                || view.cumulativeSize() != observed.cumulativeSize()
                || view.commitVersion() != observed.commitVersion()) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "fake append recovery continuation head changed");
        }
    }

    private static AppendRecoveryHead recoveryHead(StreamId streamId, StreamHeadRecord head) {
        return new AppendRecoveryHead(
                streamId,
                head.lastCommitId(),
                head.committedEndOffset(),
                head.cumulativeSize(),
                head.commitVersion(),
                head.metadataVersion());
    }

    private static CommitChainExpectation predecessorExpectation(TargetChainView view) {
        long size = Math.subtractExact(view.cumulativeSize(), view.logicalBytes());
        long version = Math.subtractExact(view.commitVersion(), 1);
        if (size < 0
                || (view.previousCommitId().isEmpty()
                        && (view.offsetStart() != 0 || size != 0 || version != 0))
                || (!view.previousCommitId().isEmpty()
                        && (view.offsetStart() == 0 || version <= 0))) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "fake append recovery predecessor scalars are inconsistent");
        }
        return new CommitChainExpectation(view.offsetStart(), size, version);
    }

    private static void requireRecoveryBridge(
            AppendRecoveryAnchor anchor,
            CommitChainExpectation expectation) {
        if (expectation.offsetEnd() != anchor.offsetEnd()
                || expectation.cumulativeSize() != anchor.cumulativeSize()
                || expectation.commitVersion() != anchor.commitVersion()) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "fake live tail does not bridge its recovery anchor");
        }
    }

    @Override
    public CompletableFuture<StreamMetadataSnapshot> transitionStreamState(
            String cluster, StreamStateTransitionRequest request) {
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(cluster, request.streamId());
            if (head.metadataVersion() != request.expectedMetadataVersion()
                    || !head.state().equals(request.expectedState().name())) {
                throw failure(ErrorCode.METADATA_CONDITION_FAILED, true, "stream state transition conflict");
            }
            StreamHeadRecord updated = new StreamHeadRecord(
                    head.streamId(), head.streamName(), head.streamNameHash(), request.targetState().name(), head.profile(),
                    head.attributes(), head.createdAtMillis(), head.policyVersion(), head.committedEndOffset(),
                    head.cumulativeSize(), head.commitVersion(), head.trimOffset(), head.lastCommitId(),
                    head.appendSession(), nextVersion());
            recordHeadCas(cluster, request.streamId());
            streamHeads.put(headMapKey(cluster, request.streamId()), updated);
            return snapshot(updated);
        });
    }

    private boolean sameCommitAnchor(StreamHeadRecord left, StreamHeadRecord right) {
        return left.committedEndOffset() == right.committedEndOffset()
                && left.cumulativeSize() == right.cumulativeSize()
                && left.commitVersion() == right.commitVersion()
                && left.lastCommitId().equals(right.lastCommitId());
    }

    private void validateTargetPreconditions(StreamHeadRecord head, CommitAppendRequest request) {
        if (!StreamState.ACTIVE.name().equals(head.state())) {
            throw appendFailure(ErrorCode.STREAM_NOT_ACTIVE, false, AppendOutcome.KNOWN_NOT_COMMITTED,
                    "stream is not active");
        }
        AppendSessionSnapshotRecord session = head.appendSession();
        if (session.isEmpty() || session.expiresAtMillis() <= clock.getAsLong()) {
            throw appendFailure(ErrorCode.APPEND_SESSION_EXPIRED, true, AppendOutcome.KNOWN_NOT_COMMITTED,
                    "append session expired");
        }
        if (!session.writerId().equals(request.writerId()) || session.epoch() != request.epoch()
                || !session.fencingToken().equals(request.fencingToken())) {
            throw appendFailure(ErrorCode.FENCED_APPEND, true, AppendOutcome.KNOWN_NOT_COMMITTED,
                    "append session token does not match");
        }
        if (head.committedEndOffset() != request.expectedStartOffset()) {
            throw appendFailure(ErrorCode.OFFSET_CONFLICT, true, AppendOutcome.KNOWN_NOT_COMMITTED,
                    "expected start offset does not match committed end");
        }
    }

    private StreamCommitTargetRecord buildTargetCommit(
            CommitAppendRequest request, StreamHeadRecord head, long version) {
        return new StreamCommitTargetRecord(
                request.streamId().value(), request.commitId(), head.lastCommitId(), request.expectedStartOffset(),
                Math.addExact(request.expectedStartOffset(), request.recordCount()), 0,
                Math.addExact(head.cumulativeSize(), request.logicalBytes()), Math.addExact(head.commitVersion(), 1),
                request.writerId(), request.writerRunIdHash(), request.epoch(), request.fencingTokenHash(),
                request.readTargetRecord(), request.payloadFormat().name(), request.recordCount(), request.entryCount(),
                request.logicalBytes(), request.schemaRefs(), request.projectionIdentity(), request.minEventTimeMillis(),
                request.maxEventTimeMillis(), clock.getAsLong(), version);
    }

    private void validateTargetReplay(
            CommitAppendRequest request, StreamCommitTargetRecord commit, AppendOutcome outcome) {
        AppendReplayRecords.requireMatches(request, commit, outcome);
    }

    private void validateTargetAgainstHead(
            CommitAppendRequest request, StreamCommitTargetRecord commit, StreamHeadRecord head) {
        if (!commit.previousCommitId().equals(head.lastCommitId())
                || commit.offsetStart() != head.committedEndOffset()
                || commit.offsetEnd() != Math.addExact(request.expectedStartOffset(), request.recordCount())
                || commit.cumulativeSize() != Math.addExact(head.cumulativeSize(), request.logicalBytes())
                || commit.commitVersion() != Math.addExact(head.commitVersion(), 1)) {
            throw appendFailure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, AppendOutcome.KNOWN_NOT_COMMITTED,
                    "generic commit does not match current head");
        }
    }

    private ReachableCommittedAppend targetReachable(
            StreamCommitTargetRecord commit, Optional<ProjectionRef> projection, StreamHeadRecord head) {
        return ReachableCommittedAppend.verified(targetCommitted(commit, projection), head.lastCommitId(),
                head.committedEndOffset(), head.cumulativeSize(), head.commitVersion());
    }

    private CommittedAppend targetCommitted(
            StreamCommitTargetRecord commit, Optional<ProjectionRef> projection) {
        return AppendReplayRecords.hydrate(commit, projection);
    }

    private AppendReplaySearchResult searchTargetReplaySync(
            String cluster, CommitAppendRequest request, Optional<AppendReplayCursor> continuation,
            int maxCommitsToScan) {
        if (maxCommitsToScan <= 0) throw new IllegalArgumentException("maxCommitsToScan must be positive");
        StreamHeadRecord head = headOrThrow(cluster, request.streamId());
        String observedId = head.lastCommitId();
        long observedEnd = head.committedEndOffset();
        long observedSize = head.cumulativeSize();
        long observedVersion = head.commitVersion();
        String next = observedId;
        CommitChainExpectation expectation = new CommitChainExpectation(observedEnd, observedSize, observedVersion);
        if (continuation.isPresent()) {
            AppendReplayCursor cursor = continuation.orElseThrow();
            if (!cursor.streamId().equals(request.streamId()) || !cursor.commitId().equals(request.commitId())
                    || cursor.expectedStartOffset() != request.expectedStartOffset()
                    || cursor.observedHeadCommitVersion() > head.commitVersion()) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "invalid append replay cursor");
            }
            Object anchorValue = anyCommit(cluster, request.streamId(), cursor.observedHeadCommitId());
            if (anchorValue == null) throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "append replay cursor anchor is missing");
            TargetChainView anchor = targetChainView(anchorValue);
            if (anchor.offsetEnd() != cursor.observedHeadOffsetEnd()
                    || anchor.cumulativeSize() != cursor.observedHeadCumulativeSize()
                    || anchor.commitVersion() != cursor.observedHeadCommitVersion()) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "append replay cursor anchor changed");
            }
            observedId = cursor.observedHeadCommitId(); observedEnd = cursor.observedHeadOffsetEnd();
            observedSize = cursor.observedHeadCumulativeSize(); observedVersion = cursor.observedHeadCommitVersion();
            next = cursor.nextCommitId();
            expectation = new CommitChainExpectation(
                    cursor.nextOffsetEnd(), cursor.nextCumulativeSize(), cursor.nextCommitVersion());
        }
        int scanned = 0;
        while (!next.isEmpty() && scanned < maxCommitsToScan) {
            Object durable = anyCommit(cluster, request.streamId(), next);
            if (durable == null) throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "broken commit chain");
            TargetChainView view = targetChainView(durable);
            if (!view.streamId().equals(request.streamId().value()) || !view.commitId().equals(next)
                    || view.offsetEnd() != expectation.offsetEnd()
                    || view.cumulativeSize() != expectation.cumulativeSize()
                    || view.commitVersion() != expectation.commitVersion()) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "inconsistent mixed commit chain");
            }
            scanned++;
            if (next.equals(request.commitId())) {
                if (!(durable instanceof StreamCommitTargetRecord target)) {
                    throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "generic ID aliases legacy commit");
                }
                validateTargetReplay(request, target, AppendOutcome.KNOWN_COMMITTED);
                return new AppendReplaySearchResult(AppendReplayStatus.FOUND,
                        Optional.of(ReachableCommittedAppend.verified(targetCommitted(target, request.projectionRef()),
                                observedId, observedEnd, observedSize, observedVersion)), Optional.empty(), scanned);
            }
            if (view.offsetStart() <= request.expectedStartOffset()) {
                return new AppendReplaySearchResult(
                        AppendReplayStatus.PROVEN_NOT_COMMITTED, Optional.empty(), Optional.empty(), scanned);
            }
            next = view.previousCommitId();
            expectation = new CommitChainExpectation(view.offsetStart(),
                    view.cumulativeSize() - view.logicalBytes(), view.commitVersion() - 1);
        }
        if (next.isEmpty()) return new AppendReplaySearchResult(
                AppendReplayStatus.PROVEN_NOT_COMMITTED, Optional.empty(), Optional.empty(), scanned);
        AppendReplayCursor cursor = new AppendReplayCursor(request.streamId(), request.commitId(),
                request.expectedStartOffset(), observedId, observedEnd, observedSize, observedVersion, next,
                expectation.offsetEnd(), expectation.cumulativeSize(), expectation.commitVersion());
        return new AppendReplaySearchResult(AppendReplayStatus.CONTINUE, Optional.empty(), Optional.of(cursor), scanned);
    }

    private Object anyCommit(String cluster, StreamId streamId, String commitId) {
        StreamCommitTargetRecord target = targetCommitById.get(commitIdentityMapKey(cluster, streamId, commitId));
        return target != null ? target : commitById.get(commitIdentityMapKey(cluster, streamId, commitId));
    }

    private static TargetChainView targetChainView(Object durable) {
        if (durable instanceof StreamCommitTargetRecord value) return new TargetChainView(
                value.streamId(), value.commitId(), value.previousCommitId(), value.offsetStart(), value.offsetEnd(),
                value.cumulativeSize(), value.commitVersion(), value.logicalBytes());
        StreamCommitRecord value = (StreamCommitRecord) durable;
        return new TargetChainView(value.streamId(), value.commitId(), value.previousCommitId(), value.offsetStart(),
                value.offsetEnd(), value.cumulativeSize(), value.commitVersion(), value.logicalBytes());
    }

    private static OffsetIndexTargetRecord withoutVersion(OffsetIndexTargetRecord value) {
        return new OffsetIndexTargetRecord(value.streamId(), value.offsetStart(), value.offsetEnd(), value.generation(),
                value.cumulativeSize(), value.readTarget(), value.payloadFormat(), value.recordCount(), value.entryCount(),
                value.logicalBytes(), value.schemaRefs(), value.projectionRef(), value.minEventTimeMillis(),
                value.maxEventTimeMillis(), value.commitVersion(), value.tombstoned(), 0);
    }

    private static OffsetIndexEntry legacyIndexEntry(OffsetIndexRecord record) {
        EntryIndexReferenceRecord raw = record.entryIndexRef();
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.valueOf(raw.location()),
                raw.objectId().isEmpty() ? Optional.empty() : Optional.of(new ObjectId(raw.objectId())),
                raw.objectKey().isEmpty() ? Optional.empty() : Optional.of(new ObjectKey(raw.objectKey())),
                raw.inlineData().length == 0 ? Optional.empty() : Optional.of(raw.inlineData()),
                raw.offset(), raw.length(),
                new Checksum(ChecksumType.valueOf(raw.checksumType()), raw.checksumValue()));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1, new ObjectId(record.objectId()), new ObjectKey(record.objectKey()),
                ObjectType.valueOf(record.objectType()), record.physicalFormat(), record.logicalFormat(),
                record.sliceId(), record.objectOffset(), record.objectLength(),
                new Checksum(ChecksumType.valueOf(record.sliceChecksumType()), record.sliceChecksumValue()), index);
        return new OffsetIndexEntry(new StreamId(record.streamId()),
                new OffsetRange(record.offsetStart(), record.offsetEnd()), record.generation(), record.cumulativeSize(),
                target, PayloadFormat.valueOf(record.payloadFormat()), record.recordCount(), record.entryCount(),
                record.logicalBytes(), record.schemaRefs(), ProjectionIdentity.decode(record.projectionRef()),
                record.commitVersion(), record.tombstoned(), record.metadataVersion());
    }

    private static OffsetIndexEntry targetIndexEntry(OffsetIndexTargetRecord record) {
        return new OffsetIndexEntry(new StreamId(record.streamId()),
                new OffsetRange(record.offsetStart(), record.offsetEnd()), record.generation(), record.cumulativeSize(),
                ReadTargetCodecRegistry.phase15().decode(record.readTarget()),
                PayloadFormat.valueOf(record.payloadFormat()), record.recordCount(), record.entryCount(),
                record.logicalBytes(), record.schemaRefs(), ProjectionIdentity.decode(record.projectionRef()),
                record.commitVersion(), record.tombstoned(), record.metadataVersion());
    }

    private static CommittedAppendRecord withoutVersion(CommittedAppendRecord value) {
        return new CommittedAppendRecord(value.streamId(), value.commitId(), value.offsetStart(), value.offsetEnd(),
                value.generation(), value.commitVersion(), value.readTargetIdentitySha256(), 0);
    }

    private StreamMetadataSnapshot snapshot(StreamHeadRecord head) {
        return new StreamMetadataSnapshot(head.toMetadataRecord(),
                new CommittedEndOffsetRecord(head.streamId(), head.committedEndOffset(), head.cumulativeSize(),
                        head.commitVersion(), head.metadataVersion()),
                new TrimRecord(head.streamId(), head.trimOffset(), "", clock.getAsLong(), head.metadataVersion()));
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
                Object commit = anyCommit(cluster, streamId, commitId);
                if (commit == null) {
                    throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "broken commit chain");
                }
                TargetChainView view = targetChainView(commit);
                if (!view.streamId().equals(streamId.value()) || !view.commitId().equals(commitId)
                        || view.offsetEnd() != expectation.offsetEnd()
                        || view.cumulativeSize() != expectation.cumulativeSize()
                        || view.commitVersion() != expectation.commitVersion()) {
                    throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                            "mixed-version commit chain is inconsistent");
                }
                CommitChainExpectation previousExpectation = new CommitChainExpectation(
                        view.offsetStart(), view.cumulativeSize() - view.logicalBytes(), view.commitVersion() - 1);
                scanned++;
                RepairMaterialization materialization = commit instanceof StreamCommitTargetRecord target
                        ? materializeTargetDerivedRecords(cluster, target)
                        : materializeDerivedRecords(cluster, (StreamCommitRecord) commit);
                if (materialization.repaired()) {
                    repaired++;
                    repairedFrom = Math.min(repairedFrom, view.offsetStart());
                    repairedTo = Math.max(repairedTo, view.offsetEnd());
                }
                if (view.offsetStart() <= targetOffset && targetOffset < view.offsetEnd()) {
                    targetCovered = true;
                    break;
                }
                commitId = view.previousCommitId();
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
    public CompletableFuture<List<OffsetIndexEntry>> scanOffsetIndex(
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
            Map<String, OffsetIndexEntry> entries = new HashMap<>();
            offsetIndexes.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(offsetMapPrefix(cluster, streamId)))
                    .forEach(entry -> entries.put(entry.getKey(), legacyIndexEntry(entry.getValue())));
            targetOffsetIndexes.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(offsetMapPrefix(cluster, streamId)))
                    .forEach(entry -> {
                        OffsetIndexEntry previous = entries.put(entry.getKey(), targetIndexEntry(entry.getValue()));
                        if (previous != null) throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                "legacy and generic indexes occupy the same durable key");
                    });
            return entries.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue)
                    .filter(record -> record.range().endOffset() > startOffset).limit(limit).toList();
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
    public CompletableFuture<Optional<VersionedPhysicalObjectRoot>> getRoot(
            String cluster,
            ObjectKeyHash object) {
        return physicalObjects.getRoot(cluster, object);
    }

    @Override
    public CompletableFuture<VersionedPhysicalObjectRoot> createRoot(
            String cluster,
            PhysicalObjectRootRecord root) {
        return physicalObjects.createRoot(cluster, root);
    }

    @Override
    public CompletableFuture<VersionedPhysicalObjectRoot> compareAndSetRoot(
            String cluster,
            PhysicalObjectRootRecord root,
            long expectedVersion) {
        return physicalObjects.compareAndSetRoot(cluster, root, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteRoot(
            String cluster,
            ObjectKeyHash object,
            long expectedVersion,
            Checksum expectedRootSha256) {
        return physicalObjects.deleteRoot(cluster, object, expectedVersion, expectedRootSha256);
    }

    @Override
    public CompletableFuture<PhysicalObjectRootScanPage> scanRoots(
            String cluster,
            int shard,
            Optional<F4ScanToken> continuation,
            int limit) {
        return physicalObjects.scanRoots(cluster, shard, continuation, limit);
    }

    @Override
    public CompletableFuture<VersionedReaderLease> createOrCompareReaderLease(
            String cluster,
            ObjectReaderLeaseRecord lease) {
        return physicalObjects.createOrCompareReaderLease(cluster, lease);
    }

    @Override
    public CompletableFuture<VersionedReaderLease> compareAndSetReaderLease(
            String cluster,
            ObjectReaderLeaseRecord lease,
            long expectedVersion) {
        return physicalObjects.compareAndSetReaderLease(cluster, lease, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteReaderLease(
            String cluster,
            ObjectKeyHash object,
            String processRunId,
            long expectedVersion) {
        return physicalObjects.deleteReaderLease(cluster, object, processRunId, expectedVersion);
    }

    @Override
    public CompletableFuture<ReaderLeaseScanPage> scanReaderLeases(
            String cluster,
            ObjectKeyHash object,
            Optional<F4ScanToken> continuation,
            int limit) {
        return physicalObjects.scanReaderLeases(cluster, object, continuation, limit);
    }

    @Override
    public CompletableFuture<VersionedObjectProtection> createProtection(
            String cluster,
            ObjectProtectionRecord protection) {
        return physicalObjects.createProtection(cluster, protection);
    }

    @Override
    public CompletableFuture<VersionedObjectProtection> compareAndSetProtection(
            String cluster,
            ObjectProtectionRecord protection,
            long expectedVersion) {
        return physicalObjects.compareAndSetProtection(cluster, protection, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteProtection(
            String cluster,
            ObjectProtectionIdentity protection,
            long expectedVersion) {
        return physicalObjects.deleteProtection(cluster, protection, expectedVersion);
    }

    @Override
    public CompletableFuture<ObjectProtectionScanPage> scanProtections(
            String cluster,
            ObjectKeyHash object,
            Optional<F4ScanToken> continuation,
            int limit) {
        return physicalObjects.scanProtections(cluster, object, continuation, limit);
    }

    @Override
    public CompletableFuture<Optional<VersionedGcRetirementManifest>> getRetirementManifest(
            String cluster, ObjectKeyHash object, String gcAttemptId) {
        return physicalObjects.getRetirementManifest(cluster, object, gcAttemptId);
    }

    @Override
    public CompletableFuture<VersionedGcRetirementManifest> createRetirementManifest(
            String cluster, GcRetirementManifestRecord manifest) {
        return physicalObjects.createRetirementManifest(cluster, manifest);
    }

    @Override
    public CompletableFuture<VersionedGcRetirementProtection> createRetirementProtection(
            String cluster, GcRetirementProtectionRecord protection) {
        return physicalObjects.createRetirementProtection(cluster, protection);
    }

    @Override
    public CompletableFuture<VersionedGcRetirementRemoval> createRetirementRemoval(
            String cluster, GcRetirementRemovalRecord removal) {
        return physicalObjects.createRetirementRemoval(cluster, removal);
    }

    @Override
    public CompletableFuture<GcRetirementProtectionScanPage> scanRetirementProtections(
            String cluster,
            ObjectKeyHash object,
            String gcAttemptId,
            Optional<F4ScanToken> continuation,
            int limit) {
        return physicalObjects.scanRetirementProtections(
                cluster, object, gcAttemptId, continuation, limit);
    }

    @Override
    public CompletableFuture<GcRetirementRemovalScanPage> scanRetirementRemovals(
            String cluster,
            ObjectKeyHash object,
            String gcAttemptId,
            Optional<F4ScanToken> continuation,
            int limit) {
        return physicalObjects.scanRetirementRemovals(
                cluster, object, gcAttemptId, continuation, limit);
    }

    @Override
    public synchronized void close() {
        closed = true;
        physicalObjects.close();
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

    private RepairMaterialization materializeTargetDerivedRecords(
            String cluster, StreamCommitTargetRecord commit) {
        StreamId streamId = new StreamId(commit.streamId());
        String indexKey = offsetMapKey(cluster, streamId, commit.offsetEnd(), commit.generation());
        OffsetIndexTargetRecord index = new OffsetIndexTargetRecord(
                commit.streamId(), commit.offsetStart(), commit.offsetEnd(), commit.generation(),
                commit.cumulativeSize(), commit.readTarget(), commit.payloadFormat(), commit.recordCount(),
                commit.entryCount(), commit.logicalBytes(), commit.schemaRefs(), commit.projectionRef(),
                commit.minEventTimeMillis(), commit.maxEventTimeMillis(), commit.commitVersion(), false, nextVersion());
        boolean repaired = false;
        OffsetIndexTargetRecord existingIndex = targetOffsetIndexes.get(indexKey);
        if (existingIndex != null && !withoutVersion(existingIndex).equals(withoutVersion(index))) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "generic offset index conflict");
        }
        if (existingIndex == null) {
            targetOffsetIndexes.put(indexKey, index);
            repaired = true;
        }
        String markerKey = new OxiaKeyspace(cluster).committedAppendKey(streamId, commit.commitId());
        CommittedAppendRecord marker = new CommittedAppendRecord(
                commit.streamId(), commit.commitId(), commit.offsetStart(), commit.offsetEnd(), commit.generation(),
                commit.commitVersion(), commit.readTarget().identityChecksumValue(), nextVersion());
        CommittedAppendRecord existingMarker = committedAppends.get(markerKey);
        if (existingMarker != null && !withoutVersion(existingMarker).equals(withoutVersion(marker))) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, "generic append marker conflict");
        }
        if (existingMarker == null) {
            committedAppends.put(markerKey, marker);
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
            AppendSessionSnapshotRecord updatedSession =
                    AppendAuthoritySessionTransitions.preserveAuthorityOnRenewal(
                            current, leaseExpiration(now, interleaveRenewTtl));
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
        Object observedHead = anyCommit(cluster, streamId, cursor.observedHeadCommitId());
        TargetChainView observed = observedHead == null ? null : targetChainView(observedHead);
        if (observedHead == null
                || observed.commitVersion() != cursor.observedCommitVersion()
                || (currentHead.commitVersion() == cursor.observedCommitVersion()
                        && (!currentHead.lastCommitId().equals(cursor.observedHeadCommitId())
                                || currentHead.committedEndOffset() != observed.offsetEnd()
                                || currentHead.cumulativeSize() != observed.cumulativeSize()))
                || cursor.nextOffsetEnd() > observed.offsetStart()
                || cursor.nextCumulativeSize() > observed.cumulativeSize()
                || cursor.nextCommitVersion() >= observed.commitVersion()) {
            throw failure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "derived-index repair continuation references an invalid committed-chain position");
        }
        if (!observed.streamId().equals(streamId.value())
                || !observed.commitId().equals(cursor.observedHeadCommitId())) {
            throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "repair continuation observed head identity is invalid");
        }
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

    private VisibleSliceReferenceRecord findReachableCommitForManifestSlice(
            String cluster,
            ObjectManifestRecord manifest,
            StreamSliceManifestRecord slice) {
        StreamId streamId = new StreamId(slice.streamId());
        StreamHeadRecord head = streamHeads.get(headMapKey(cluster, streamId));
        if (head == null) return null;
        String commitId = head.lastCommitId();
        CommitChainExpectation expectation = expectationFromHead(head);
        int scanned = 0;
        while (!commitId.isEmpty() && scanned++ < maxCommitChainScan) {
            Object durable = anyCommit(cluster, streamId, commitId);
            if (durable == null) throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "object-reference repair found a broken commit chain");
            TargetChainView view = targetChainView(durable);
            if (view.offsetEnd() != expectation.offsetEnd()
                    || view.cumulativeSize() != expectation.cumulativeSize()
                    || view.commitVersion() != expectation.commitVersion()) {
                throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                        "object-reference repair found an inconsistent commit chain");
            }
            boolean matches = false;
            if (durable instanceof StreamCommitRecord legacy) {
                if (legacy.objectId().equals(manifest.objectId()) && legacy.sliceId().equals(slice.sliceId())) {
                    if (!commitMatchesManifestSlice(manifest, slice, legacy)) {
                        throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                "reachable commit conflicts with object manifest slice");
                    }
                    materializeDerivedRecords(cluster, legacy);
                    matches = true;
                }
            } else {
                StreamCommitTargetRecord targetCommit = (StreamCommitTargetRecord) durable;
                com.nereusstream.api.target.ReadTarget decoded =
                        ReadTargetCodecRegistry.phase15().decode(targetCommit.readTarget());
                if (decoded instanceof ObjectSliceReadTarget target
                        && target.objectId().value().equals(manifest.objectId())
                        && target.sliceId().equals(slice.sliceId())) {
                    if (!target.objectKey().value().equals(manifest.objectKey())
                            || !target.objectType().name().equals(manifest.objectType())
                            || targetCommit.writerId().equals(manifest.writerId()) == false
                            || !targetCommit.writerRunIdHash().equals(manifest.writerRunIdHash())
                            || targetCommit.writerEpoch() != manifest.writerEpoch()
                            || target.objectOffset() != slice.objectOffset()
                            || target.objectLength() != slice.objectLength()
                            || !target.sliceChecksum().type().name().equals(slice.sliceChecksumType())
                            || !target.sliceChecksum().value().equals(slice.sliceChecksumValue())) {
                        throw failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                "generic object target conflicts with manifest slice");
                    }
                    materializeTargetDerivedRecords(cluster, targetCommit);
                    matches = true;
                }
            }
            if (matches) return new VisibleSliceReferenceRecord(
                    view.streamId(), slice.sliceId(), view.offsetStart(), view.offsetEnd(), 0, view.commitVersion());
            commitId = view.previousCommitId();
            expectation = new CommitChainExpectation(
                    view.offsetStart(), view.cumulativeSize() - view.logicalBytes(), view.commitVersion() - 1);
        }
        if (!commitId.isEmpty()) throw failure(ErrorCode.METADATA_UNAVAILABLE, true,
                "object-reference repair exhausted the commit-chain scan budget");
        return null;
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

    private StreamHeadRecord withTargetCommit(
            StreamHeadRecord head, StreamCommitTargetRecord commit, long metadataVersion) {
        return new StreamHeadRecord(
                head.streamId(), head.streamName(), head.streamNameHash(), head.state(), head.profile(),
                head.attributes(), head.createdAtMillis(), head.policyVersion(), commit.offsetEnd(),
                commit.cumulativeSize(), commit.commitVersion(), head.trimOffset(), commit.commitId(),
                head.appendSession(), metadataVersion);
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
        return new OxiaKeyspace(cluster).streamCommitKey(streamId, commitId);
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

        byte[] envelope(String key) {
            byte[] encoded = values.get(Objects.requireNonNull(key, "key"));
            if (encoded == null) {
                throw new IllegalStateException("fake metadata value is absent");
            }
            return encoded.clone();
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
            return MetadataRecordCodecFactory.encodeEnvelope(record, recordClass);
        }

        private T decode(byte[] encoded) {
            return MetadataRecordCodecFactory.decodeEnvelope(encoded, recordClass);
        }
    }
}
