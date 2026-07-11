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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.CommitSliceResult;
import com.nereusstream.metadata.oxia.DerivedIndexRepairCursor;
import com.nereusstream.metadata.oxia.DerivedIndexRepairResult;
import com.nereusstream.metadata.oxia.MetadataWatcher;
import com.nereusstream.metadata.oxia.Phase1ObjectManifestValidator;
import com.nereusstream.metadata.oxia.WatchRegistration;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodec;
import com.nereusstream.metadata.oxia.codec.MetadataRecordEnvelope;
import com.nereusstream.metadata.oxia.codec.Phase1MetadataCodecs;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.CommittedSliceRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitRecord;
import com.nereusstream.metadata.oxia.records.StreamHeadRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
class FakeOxiaMetadataStoreTest {
    private static final String CLUSTER = "cluster";
    private static final String WRITER_ID = "writer";
    private static final String WRITER_RUN_ID_HASH = "runhash";

    private final AtomicLong clock = new AtomicLong(1_000);
    private final FakeOxiaMetadataStore store = new FakeOxiaMetadataStore(clock::get);

    @Test
    void createOrGetStreamUsesDeterministicStreamIdAndExactName() {
        StreamName streamName = new StreamName("tenant/ns/topic");

        StreamMetadataRecord created = createStream(streamName);
        StreamMetadataRecord loaded = store.createOrGetStream(
                CLUSTER,
                streamName,
                new StreamCreateOptions(StorageProfile.OBJECT_WAL, Map.of("alias", "topic")))
                .join();

        assertThat(created.streamId()).isEqualTo(DeterministicIds.streamIdFor(streamName).value());
        assertThat(loaded.streamId()).isEqualTo(created.streamId());
        assertThat(loaded.streamName()).isEqualTo(streamName.value());
        assertThat(loaded.streamNameHash()).isEqualTo(DeterministicIds.streamNameHash(streamName));
        assertThat(created.profile()).isEqualTo(StorageProfile.OBJECT_WAL_SYNC_OBJECT.name());
        assertThat(loaded.profile()).isEqualTo(StorageProfile.OBJECT_WAL_SYNC_OBJECT.name());
    }

    @Test
    void createOrGetStreamPersistsProfileSelectionForNewProfiles() {
        StreamName streamName = new StreamName("tenant/ns/async-topic");

        StreamMetadataRecord created = store.createOrGetStream(
                CLUSTER,
                streamName,
                new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT, Map.of("alias", "topic")))
                .join();

        assertThat(created.profile()).isEqualTo(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT.name());
    }

    @Test
    void appendSessionAcquireAndRenewUseHeadCasVersions() {
        StreamId streamId = new StreamId(createStream(new StreamName("s")).streamId());

        AppendSessionRecord acquired = store.acquireAppendSession(
                CLUSTER,
                streamId,
                new AppendSessionOptions(WRITER_ID, Duration.ofMillis(100), false))
                .join();
        AppendSessionRecord renewed = store.renewAppendSession(
                CLUSTER,
                streamId,
                WRITER_ID,
                acquired.epoch(),
                acquired.fencingToken(),
                Duration.ofMillis(200))
                .join();

        assertThat(renewed.epoch()).isEqualTo(acquired.epoch());
        assertThat(renewed.fencingToken()).isEqualTo(acquired.fencingToken());
        assertThat(renewed.leaseVersion()).isEqualTo(acquired.leaseVersion() + 1);
        assertThat(renewed.expiresAtMillis()).isEqualTo(clock.get() + 200);
    }

    @Test
    void appendSessionFencingExpiryAndStealFollowPublicErrorContract() {
        StreamId streamId = new StreamId(createStream(new StreamName("session-fencing")).streamId());
        AppendSessionRecord sessionA = store.acquireAppendSession(
                        CLUSTER,
                        streamId,
                        new AppendSessionOptions("writer-a", Duration.ofMillis(10), false))
                .join();

        assertNereusFailureWithoutAppendOutcome(
                () -> store.acquireAppendSession(
                                CLUSTER,
                                streamId,
                                new AppendSessionOptions("writer-b", Duration.ofMillis(10), true))
                        .join(),
                ErrorCode.FENCED_APPEND,
                true);
        assertNereusFailureWithoutAppendOutcome(
                () -> store.renewAppendSession(
                                CLUSTER,
                                streamId,
                                sessionA.writerId(),
                                sessionA.epoch(),
                                "wrong-token",
                                Duration.ofMillis(10))
                        .join(),
                ErrorCode.FENCED_APPEND,
                true);
        assertNereusFailureWithoutAppendOutcome(
                () -> store.renewAppendSession(
                                CLUSTER,
                                streamId,
                                sessionA.writerId(),
                                sessionA.epoch(),
                                sessionA.fencingToken(),
                                Duration.ofNanos(1))
                        .join(),
                ErrorCode.INVALID_ARGUMENT,
                false);
        assertNereusFailureWithoutAppendOutcome(
                () -> store.renewAppendSession(
                                CLUSTER,
                                streamId,
                                sessionA.writerId(),
                                sessionA.epoch(),
                                sessionA.fencingToken(),
                                Duration.ofSeconds(Long.MAX_VALUE))
                        .join(),
                ErrorCode.INVALID_ARGUMENT,
                false);

        clock.addAndGet(11);
        assertNereusFailureWithoutAppendOutcome(
                () -> store.renewAppendSession(
                                CLUSTER,
                                streamId,
                                sessionA.writerId(),
                                sessionA.epoch(),
                                sessionA.fencingToken(),
                                Duration.ofMillis(10))
                        .join(),
                ErrorCode.APPEND_SESSION_EXPIRED,
                true);
        assertNereusFailureWithoutAppendOutcome(
                () -> store.acquireAppendSession(
                                CLUSTER,
                                streamId,
                                new AppendSessionOptions("writer-b", Duration.ofMillis(10), false))
                        .join(),
                ErrorCode.APPEND_SESSION_EXPIRED,
                true);

        AppendSessionRecord sessionB = store.acquireAppendSession(
                        CLUSTER,
                        streamId,
                        new AppendSessionOptions("writer-b", Duration.ofMillis(10), true))
                .join();
        assertThat(sessionB.epoch()).isEqualTo(sessionA.epoch() + 1);
        assertThat(sessionB.fencingToken()).isNotEqualTo(sessionA.fencingToken());

        clock.addAndGet(11);
        AppendSessionRecord reacquired = store.acquireAppendSession(
                        CLUSTER,
                        streamId,
                        new AppendSessionOptions("writer-b", Duration.ofMillis(10), false))
                .join();
        assertThat(reacquired.epoch()).isEqualTo(sessionB.epoch() + 1);
        assertThat(reacquired.fencingToken()).isNotEqualTo(sessionB.fencingToken());
    }

    @Test
    void invalidTrimInputFailsBeforeHeadMutation() {
        StreamId streamId = new StreamId(createStream(new StreamName("trim-validation")).streamId());

        assertNereusFailureWithoutAppendOutcome(
                () -> store.updateTrim(CLUSTER, streamId, 0, null).join(),
                ErrorCode.INVALID_ARGUMENT,
                false);
        assertNereusFailureWithoutAppendOutcome(
                () -> store.updateTrim(CLUSTER, streamId, 1, "beyond-end").join(),
                ErrorCode.INVALID_ARGUMENT,
                false);

        assertThat(store.getTrim(CLUSTER, streamId).join().trimOffset()).isZero();
    }

    @Test
    void watcherFailureNeverChangesMetadataOperationOutcome() {
        StreamId streamId = new StreamId(createStream(new StreamName("watcher-failure")).streamId());
        store.watchStream(CLUSTER, streamId, new ThrowingWatcher());

        AppendSessionRecord session = acquireSession(streamId);
        assertThat(store.updateTrim(CLUSTER, streamId, 0, "watcher-failure").join().trimOffset()).isZero();
        CommitSliceRequest request = request(streamId, session, "watcher-object", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();
        assertThat(store.commitStreamSlice(CLUSTER, request).join().commitVersion()).isEqualTo(1);

        assertThat(store.watchDeliveryFailureCount()).isEqualTo(3);
    }

    @Test
    void closedStoreRejectsNewWatchRegistration() {
        FakeOxiaMetadataStore localStore = new FakeOxiaMetadataStore(clock::get);
        StreamId streamId = new StreamId(createStream(
                localStore,
                new StreamName("closed-watch")).streamId());
        localStore.close();

        assertThatThrownBy(() -> localStore.watchStream(CLUSTER, streamId, new RecordingWatcher()))
                .isInstanceOfSatisfying(NereusException.class, exception -> {
                    assertThat(exception.code()).isEqualTo(ErrorCode.STORAGE_CLOSED);
                    assertThat(exception.retriable()).isFalse();
                });
    }

    @Test
    void commitStreamSliceUsesHeadCasAndMaterializesOffsetIndex() {
        StreamId streamId = new StreamId(createStream(new StreamName("commit-stream")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-1", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();

        CommitSliceResult result = store.commitStreamSlice(CLUSTER, request).join();
        List<OffsetIndexRecord> indexes = store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join();

        assertThat(result.range().startOffset()).isEqualTo(0);
        assertThat(result.range().endOffset()).isEqualTo(1);
        assertThat(result.commitVersion()).isEqualTo(1);
        assertThat(indexes).hasSize(1);
        assertThat(indexes.get(0).commitVersion()).isEqualTo(result.commitVersion());
        assertThat(indexes.get(0).sliceChecksumValue()).isEqualTo(request.sliceChecksum().value());
        assertThat(store.getObjectReferences(CLUSTER, request.objectId()).join()).isPresent();
        assertThat(store.accessLog()).allSatisfy(access ->
                assertThat(access.partitionKey()).isNotBlank());
    }

    @Test
    void sameSliceRetryRepairsDerivedIndexesAfterHeadCasFailure() {
        StreamId streamId = new StreamId(createStream(new StreamName("repair-stream")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-2", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();
        store.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);

        assertNereusFailure(
                () -> store.commitStreamSlice(CLUSTER, request).join(),
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                AppendOutcome.KNOWN_COMMITTED);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).isEmpty();

        DerivedIndexRepairResult repair = store.repairDerivedStreamIndexes(CLUSTER, streamId, 0, 10).join();
        assertThat(repair.targetCovered()).isTrue();
        assertThat(repair.scannedRecords()).isEqualTo(1);
        assertThat(repair.repairedRecords()).isEqualTo(1);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).hasSize(1);
        assertThat(store.getObjectReferences(CLUSTER, request.objectId()).join()).isEmpty();

        ObjectReferenceRecord references = store.repairObjectReferences(CLUSTER, request.objectId()).join();
        assertThat(references.visibleSlices()).hasSize(1);
        assertThat(references.visibleSlices().get(0).streamId()).isEqualTo(streamId.value());
        assertThat(references.visibleSlices().get(0).offsetStart()).isEqualTo(0);
        assertThat(references.visibleSlices().get(0).offsetEnd()).isEqualTo(1);

        CommitSliceResult replayed = store.commitStreamSlice(CLUSTER, request).join();

        assertThat(replayed.commitVersion()).isEqualTo(1);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).hasSize(1);
    }

    @Test
    void manifestValidationRejectsCrossStreamSliceBeforeHeadCommit() {
        StreamId streamId = new StreamId(createStream(new StreamName("manifest-stream-mismatch")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-stream-mismatch", "slice-1", 0, 1);
        ObjectManifestRecord valid = manifest(request);
        StreamSliceManifestRecord slice = valid.slices().get(0);
        ObjectManifestRecord mismatched = copyManifest(
                valid,
                valid.state(),
                valid.formatMajorVersion(),
                List.of(copySlice(slice, 0, "different-stream", slice.sliceId(), slice.objectOffset(), slice.objectLength())));
        store.putObjectManifest(CLUSTER, mismatched).join();

        assertNereusFailure(
                () -> store.commitStreamSlice(CLUSTER, request).join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                AppendOutcome.KNOWN_NOT_COMMITTED);
        assertThat(store.getCommittedEndOffset(CLUSTER, streamId).join().committedEndOffset()).isZero();
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).isEmpty();
    }

    @Test
    void sharedManifestValidationRejectsMismatchedObjectIdAndAggregateState() {
        StreamId streamId = new StreamId(createStream(new StreamName("manifest-object-id-mismatch")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-id-mismatch", "slice-1", 0, 1);
        ObjectManifestRecord valid = manifest(request);
        ObjectManifestRecord mismatched = copyManifestWithObjectId(valid, "different-object-id");

        assertThatThrownBy(() -> Phase1ObjectManifestValidator.validateCommitCandidate(mismatched, request, false))
                .isInstanceOfSatisfying(NereusException.class, exception ->
                        assertThat(exception.code()).isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
        assertThatThrownBy(() -> Phase1ObjectManifestValidator.validateStoredManifest(
                        copyManifest(valid, "PARTIALLY_VISIBLE", 1, valid.slices())))
                .isInstanceOfSatisfying(NereusException.class, exception ->
                        assertThat(exception.code()).isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
    }

    @Test
    void newManifestRejectsUnsupportedFormatAndInvalidSliceLayout() {
        StreamId streamId = new StreamId(createStream(new StreamName("manifest-shape-validation")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-shape", "slice-1", 0, 1);
        ObjectManifestRecord valid = manifest(request);
        StreamSliceManifestRecord slice = valid.slices().get(0);

        assertNereusCode(
                () -> store.putObjectManifest(
                                CLUSTER,
                                copyManifest(valid, "VISIBLE", 1, valid.slices()))
                        .join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
        assertNereusCode(
                () -> store.putObjectManifest(
                                CLUSTER,
                                copyManifest(
                                        valid,
                                        valid.state(),
                                        1,
                                        List.of(
                                                slice,
                                                copySlice(slice, 2, slice.streamId(), "slice-2", 30, 20))))
                        .join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
        assertNereusCode(
                () -> store.putObjectManifest(
                                CLUSTER,
                                copyManifest(
                                        valid,
                                        valid.state(),
                                        1,
                                        List.of(
                                                slice,
                                                copySlice(slice, 1, slice.streamId(), "slice-2", 20, 20))))
                        .join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
        assertNereusCode(
                () -> store.putObjectManifest(
                                CLUSTER,
                                copyManifest(valid, valid.state(), 2, valid.slices()))
                        .join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
        assertNereusCode(
                () -> store.putObjectManifest(
                                CLUSTER,
                                copyManifest(
                                        valid,
                                        valid.state(),
                                        1,
                                        List.of(
                                                slice,
                                                copySlice(slice, 1, slice.streamId(), slice.sliceId(), 30, 20))))
                        .join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
        assertNereusCode(
                () -> store.putObjectManifest(
                                CLUSTER,
                                copyManifest(
                                        valid,
                                        valid.state(),
                                        1,
                                        List.of(
                                                slice,
                                                copySlice(slice, 0, slice.streamId(), "slice-2", 30, 20))))
                        .join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
        assertNereusCode(
                () -> store.putObjectManifest(
                                CLUSTER,
                                copyManifest(
                                        valid,
                                        valid.state(),
                                        1,
                                        List.of(copySlice(
                                                slice,
                                                0,
                                                slice.streamId(),
                                                slice.sliceId(),
                                                120,
                                                20))))
                        .join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
    }

    @Test
    void manifestRetryIgnoresPostCommitAuditStateButKeepsImmutableIdentityStrict() {
        StreamId streamId = new StreamId(createStream(new StreamName("manifest-audit-retry")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-audit-retry", "slice-1", 0, 1);
        ObjectManifestRecord uploaded = manifest(request);
        store.putObjectManifest(CLUSTER, uploaded).join();
        store.commitStreamSlice(CLUSTER, request).join();

        store.putObjectManifest(CLUSTER, uploaded).join();

        assertThat(store.getObjectManifest(CLUSTER, request.objectId()).join())
                .hasValueSatisfying(stored -> assertThat(stored.state()).isEqualTo("VISIBLE"));
    }

    @Test
    void sameSliceRetryChecksCommittedSliceMarkerBeforeHeadChain() {
        StreamId streamId = new StreamId(createStream(new StreamName("marker-replay-stream")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-marker", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();
        store.commitStreamSlice(CLUSTER, request).join();
        int accessCountBeforeRetry = store.accessLog().size();

        CommitSliceResult replayed = store.commitStreamSlice(CLUSTER, request).join();
        List<FakeOxiaMetadataStore.PartitionedAccess> retryAccesses =
                store.accessLog().subList(accessCountBeforeRetry, store.accessLog().size());

        assertThat(replayed.commitVersion()).isEqualTo(1);
        assertThat(retryAccesses).isNotEmpty();
        assertThat(retryAccesses.get(0).key()).contains("/committed-slices/");
        assertThat(retryAccesses.get(0).operation()).isEqualTo("get");
    }

    @Test
    void postCommitObjectAuditFailureLeavesVisibleDataAndRepairableReferences() {
        StreamId streamId = new StreamId(createStream(new StreamName("object-audit-failure-stream")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-audit-failure", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();
        store.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_DERIVED_INDEX_BEFORE_OBJECT_AUDIT);

        CommitSliceResult result = store.commitStreamSlice(CLUSTER, request).join();

        assertThat(result.commitVersion()).isEqualTo(1);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).hasSize(1);
        assertThat(store.getObjectReferences(CLUSTER, request.objectId()).join()).isEmpty();
        assertThat(store.objectAuditFailureCount()).isEqualTo(1);

        ObjectReferenceRecord repaired = store.repairObjectReferences(CLUSTER, request.objectId()).join();
        assertThat(repaired.visibleSlices()).hasSize(1);
        assertThat(repaired.visibleSlices().get(0).offsetStart()).isEqualTo(0);
        assertThat(repaired.visibleSlices().get(0).offsetEnd()).isEqualTo(1);
    }

    @Test
    void fakeStorePersistsMetadataValuesThroughSharedCodec() {
        StreamId streamId = new StreamId(createStream(new StreamName("codec-backed-stream")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-codec", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();
        store.commitStreamSlice(CLUSTER, request).join();

        List<FakeOxiaMetadataStore.StoredMetadataValue> stored = store.storedMetadataValuesForTesting();

        assertThat(stored)
                .extracting(FakeOxiaMetadataStore.StoredMetadataValue::recordType)
                .contains(
                        StreamHeadRecord.class.getSimpleName(),
                        ObjectManifestRecord.class.getSimpleName(),
                        StreamCommitRecord.class.getSimpleName(),
                        OffsetIndexRecord.class.getSimpleName(),
                        CommittedSliceRecord.class.getSimpleName(),
                        ObjectReferenceRecord.class.getSimpleName());
        assertThat(stored).allSatisfy(value -> {
            MetadataRecordEnvelope.DecodedEnvelope envelope = MetadataRecordEnvelope.decode(value.envelope());
            assertThat(envelope.recordType()).isEqualTo(value.recordType());
            MetadataRecordCodec<?> codec = Phase1MetadataCodecs.registry().codecForType(envelope.recordType());
            assertThat(codec.decode(envelope.payload())).isNotNull();
        });
    }

    @Test
    void offsetConflictIsDistinctFromStaleSession() {
        StreamId streamId = new StreamId(createStream(new StreamName("conflict-stream")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest first = request(streamId, session, "object-3", "slice-1", 0, 1);
        CommitSliceRequest conflict = request(streamId, session, "object-4", "slice-2", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(first)).join();
        store.putObjectManifest(CLUSTER, manifest(conflict)).join();
        store.commitStreamSlice(CLUSTER, first).join();

        assertNereusFailure(
                () -> store.commitStreamSlice(CLUSTER, conflict).join(),
                ErrorCode.OFFSET_CONFLICT,
                true,
                AppendOutcome.KNOWN_NOT_COMMITTED);
    }

    @Test
    void objectReferenceRepairDoesNotCreateVisibilityFromManifestOnlySlices() {
        StreamId streamId = new StreamId(createStream(new StreamName("manifest-only-stream")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-5", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();

        ObjectReferenceRecord repaired = store.repairObjectReferences(CLUSTER, request.objectId()).join();

        assertThat(repaired.visibleSlices()).isEmpty();
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).isEmpty();
    }

    @Test
    void watchEventsCanBeMissedDuplicatedStaleCollapsedAndReconnectBeforeCurrent() {
        StreamId streamId = new StreamId(createStream(new StreamName("watch-stream")).streamId());
        RecordingWatcher watcher = new RecordingWatcher();
        try (WatchRegistration ignored = store.watchStream(CLUSTER, streamId, watcher)) {
            store.setNextWatchDelivery(FakeOxiaMetadataStore.WatchDelivery.DROP_NEXT);
            AppendSessionRecord session = acquireSession(streamId);
            assertThat(watcher.events()).isEmpty();

            store.setNextWatchDelivery(FakeOxiaMetadataStore.WatchDelivery.DUPLICATE_NEXT);
            store.updateTrim(CLUSTER, streamId, 0, "test").join();
            assertThat(watcher.events()).containsExactly("trim:0", "trim:0");
            watcher.clear();

            CommitSliceRequest request = request(streamId, session, "object-6", "slice-1", 0, 1);
            store.putObjectManifest(CLUSTER, manifest(request)).join();
            store.setNextWatchDelivery(FakeOxiaMetadataStore.WatchDelivery.STALE_THEN_CURRENT_NEXT);
            store.commitStreamSlice(CLUSTER, request).join();

            assertThat(watcher.events()).hasSize(2);
            assertThat(watcher.events().get(0)).startsWith("offset:1:");
            assertThat(watcher.events().get(1)).startsWith("offset:1:");
            long staleVersion = Long.parseLong(watcher.events().get(0).substring("offset:1:".length()));
            long currentVersion = Long.parseLong(watcher.events().get(1).substring("offset:1:".length()));
            assertThat(staleVersion).isLessThan(currentVersion);
            watcher.clear();

            store.setNextWatchDelivery(FakeOxiaMetadataStore.WatchDelivery.COLLAPSE_NEXT);
            store.updateTrim(CLUSTER, streamId, 0, "collapsed").join();
            assertThat(watcher.events()).isEmpty();
            store.updateTrim(CLUSTER, streamId, 0, "current-after-collapse").join();
            assertThat(watcher.events()).containsExactly("trim:0");
            watcher.clear();

            store.setNextWatchDelivery(FakeOxiaMetadataStore.WatchDelivery.RECONNECT_THEN_CURRENT_NEXT);
            store.updateTrim(CLUSTER, streamId, 0, "reconnect").join();
            assertThat(watcher.events()).hasSize(2);
            assertThat(watcher.events().get(0)).startsWith("reconnect:");
            assertThat(watcher.events().get(1)).isEqualTo("trim:0");
        }
    }

    @Test
    void sameWriterRenewIsCompatibleHeadVersionChange() {
        StreamId streamId = new StreamId(createStream(new StreamName("renew-head-conflict")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-renew", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();

        store.interleaveBeforeNextHeadCasWithSameWriterRenew(Duration.ofMillis(2_000));

        CommitSliceResult result = store.commitStreamSlice(CLUSTER, request).join();
        StreamHeadRecord head = storedHead(streamId);

        assertThat(result.commitVersion()).isEqualTo(1);
        assertThat(result.range().startOffset()).isEqualTo(0);
        assertThat(result.range().endOffset()).isEqualTo(1);
        assertThat(head.committedEndOffset()).isEqualTo(1);
        assertThat(head.commitVersion()).isEqualTo(1);
        assertThat(head.appendSession().leaseVersion()).isEqualTo(session.leaseVersion() + 1);
        assertThat(head.appendSession().expiresAtMillis()).isEqualTo(clock.get() + 2_000);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).hasSize(1);
    }

    @Test
    void sameWriterTrimIsCompatibleHeadVersionChange() {
        StreamId streamId = new StreamId(createStream(new StreamName("trim-head-conflict")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest first = request(streamId, session, "object-trim-1", "slice-1", 0, 1);
        CommitSliceRequest second = request(streamId, session, "object-trim-2", "slice-2", 1, 1);
        store.putObjectManifest(CLUSTER, manifest(first)).join();
        store.putObjectManifest(CLUSTER, manifest(second)).join();
        store.commitStreamSlice(CLUSTER, first).join();

        store.interleaveBeforeNextHeadCasWithTrim(1, "test trim");

        CommitSliceResult result = store.commitStreamSlice(CLUSTER, second).join();
        StreamHeadRecord head = storedHead(streamId);

        assertThat(result.commitVersion()).isEqualTo(2);
        assertThat(head.committedEndOffset()).isEqualTo(2);
        assertThat(head.commitVersion()).isEqualTo(2);
        assertThat(head.trimOffset()).isEqualTo(1);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).hasSize(2);
    }

    @Test
    void repairBudgetExhaustionStopsAtMaxScannedCommitsAndContinues() {
        StreamId streamId = new StreamId(createStream(new StreamName("repair-budget-exhaustion")).streamId());
        AppendSessionRecord session = acquireSession(streamId);

        CommitSliceRequest req1 = request(streamId, session, "object-repair-1", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(req1)).join();
        store.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
        assertNereusCode(
                () -> store.commitStreamSlice(CLUSTER, req1).join(),
                ErrorCode.METADATA_UNAVAILABLE);

        CommitSliceRequest req2 = request(streamId, session, "object-repair-2", "slice-2", 1, 1);
        store.putObjectManifest(CLUSTER, manifest(req2)).join();
        store.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
        assertNereusCode(
                () -> store.commitStreamSlice(CLUSTER, req2).join(),
                ErrorCode.METADATA_UNAVAILABLE);

        CommitSliceRequest req3 = request(streamId, session, "object-repair-3", "slice-3", 2, 1);
        store.putObjectManifest(CLUSTER, manifest(req3)).join();
        store.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
        assertNereusCode(
                () -> store.commitStreamSlice(CLUSTER, req3).join(),
                ErrorCode.METADATA_UNAVAILABLE);

        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).isEmpty();

        DerivedIndexRepairResult repair = store.repairDerivedStreamIndexes(CLUSTER, streamId, 0, 1).join();

        assertThat(repair.scannedRecords()).isEqualTo(1);
        assertThat(repair.repairedRecords()).isEqualTo(1);
        assertThat(repair.targetCovered()).isFalse();
        assertThat(repair.repairBudgetExhausted()).isTrue();
        assertThat(repair.repairedFromOffset()).isEqualTo(2);
        assertThat(repair.repairedToOffset()).isEqualTo(3);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join())
                .extracting(OffsetIndexRecord::offsetStart)
                .containsExactly(2L);

        DerivedIndexRepairResult secondRepair = store.repairDerivedStreamIndexes(
                        CLUSTER,
                        streamId,
                        0,
                        repair.continuation(),
                        1)
                .join();
        assertThat(secondRepair.scannedRecords()).isEqualTo(1);
        assertThat(secondRepair.repairedRecords()).isEqualTo(1);
        assertThat(secondRepair.targetCovered()).isFalse();
        assertThat(secondRepair.repairBudgetExhausted()).isTrue();
        assertThat(secondRepair.repairedFromOffset()).isEqualTo(1);
        assertThat(secondRepair.repairedToOffset()).isEqualTo(2);

        DerivedIndexRepairResult thirdRepair = store.repairDerivedStreamIndexes(
                        CLUSTER,
                        streamId,
                        0,
                        secondRepair.continuation(),
                        1)
                .join();
        assertThat(thirdRepair.scannedRecords()).isEqualTo(1);
        assertThat(thirdRepair.repairedRecords()).isEqualTo(1);
        assertThat(thirdRepair.targetCovered()).isTrue();
        assertThat(thirdRepair.repairBudgetExhausted()).isFalse();
        assertThat(thirdRepair.repairedFromOffset()).isEqualTo(0);
        assertThat(thirdRepair.repairedToOffset()).isEqualTo(1);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join())
                .extracting(OffsetIndexRecord::offsetStart)
                .containsExactly(0L, 1L, 2L);
    }

    @Test
    void replayBudgetExhaustionIsUnknownAndRepairContinuationMakesProgressAcrossExistingIndexes() {
        FakeOxiaMetadataStore boundedStore = new FakeOxiaMetadataStore(clock::get, 2);
        StreamId streamId = new StreamId(createStream(
                boundedStore,
                new StreamName("bounded-replay-search")).streamId());
        AppendSessionRecord session = acquireSession(boundedStore, streamId);

        CommitSliceRequest first = request(streamId, session, "bounded-object-1", "slice-1", 0, 1);
        boundedStore.putObjectManifest(CLUSTER, manifest(first)).join();
        boundedStore.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
        assertNereusFailure(
                () -> boundedStore.commitStreamSlice(CLUSTER, first).join(),
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                AppendOutcome.KNOWN_COMMITTED);

        CommitSliceRequest second = request(streamId, session, "bounded-object-2", "slice-2", 1, 1);
        CommitSliceRequest third = request(streamId, session, "bounded-object-3", "slice-3", 2, 1);
        boundedStore.putObjectManifest(CLUSTER, manifest(second)).join();
        boundedStore.putObjectManifest(CLUSTER, manifest(third)).join();
        boundedStore.commitStreamSlice(CLUSTER, second).join();
        boundedStore.commitStreamSlice(CLUSTER, third).join();

        assertNereusFailure(
                () -> boundedStore.commitStreamSlice(CLUSTER, first).join(),
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                AppendOutcome.MAY_HAVE_COMMITTED);

        DerivedIndexRepairResult page1 = boundedStore.repairDerivedStreamIndexes(
                        CLUSTER,
                        streamId,
                        0,
                        Optional.empty(),
                        1)
                .join();
        assertNereusCode(
                () -> boundedStore.repairDerivedStreamIndexes(
                                CLUSTER,
                                streamId,
                                1,
                                page1.continuation(),
                                1)
                        .join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
        DerivedIndexRepairResult page2 = boundedStore.repairDerivedStreamIndexes(
                        CLUSTER,
                        streamId,
                        0,
                        page1.continuation(),
                        1)
                .join();
        DerivedIndexRepairResult page3 = boundedStore.repairDerivedStreamIndexes(
                        CLUSTER,
                        streamId,
                        0,
                        page2.continuation(),
                        1)
                .join();

        assertThat(page1.scannedRecords()).isEqualTo(1);
        assertThat(page1.repairedRecords()).isZero();
        assertThat(page1.continuation()).isPresent();
        DerivedIndexRepairCursor firstCursor = page1.continuation().orElseThrow();
        assertThat(firstCursor.nextOffsetEnd()).isEqualTo(2);
        assertThat(firstCursor.nextCumulativeSize()).isEqualTo(14);
        assertThat(firstCursor.nextCommitVersion()).isEqualTo(2);
        DerivedIndexRepairCursor tamperedCursor = new DerivedIndexRepairCursor(
                firstCursor.streamId(),
                firstCursor.targetOffset(),
                firstCursor.observedHeadCommitId(),
                firstCursor.observedCommitVersion(),
                firstCursor.nextCommitId(),
                firstCursor.nextOffsetEnd(),
                firstCursor.nextCumulativeSize() - 1,
                firstCursor.nextCommitVersion());
        assertNereusCode(
                () -> boundedStore.repairDerivedStreamIndexes(
                                CLUSTER,
                                streamId,
                                0,
                                Optional.of(tamperedCursor),
                                1)
                        .join(),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
        assertThat(page2.scannedRecords()).isEqualTo(1);
        assertThat(page2.repairedRecords()).isZero();
        assertThat(page2.continuation()).isPresent();
        assertThat(page2.continuation().orElseThrow().observedHeadCommitId())
                .isEqualTo(firstCursor.observedHeadCommitId());
        assertThat(page2.observedCommitVersion()).isEqualTo(page1.observedCommitVersion());
        assertThat(page3.scannedRecords()).isEqualTo(1);
        assertThat(page3.repairedRecords()).isEqualTo(1);
        assertThat(page3.targetCovered()).isTrue();
        assertThat(page3.continuation()).isEmpty();

        CommitSliceResult replayed = boundedStore.commitStreamSlice(CLUSTER, first).join();
        assertThat(replayed.commitVersion()).isEqualTo(1);
    }

    @Test
    void newAppendSkipsHistoryAndReplayStopsWhenExpectedOffsetIsProvenOccupied() {
        FakeOxiaMetadataStore boundedStore = new FakeOxiaMetadataStore(clock::get, 2);
        StreamId streamId = new StreamId(createStream(
                boundedStore,
                new StreamName("new-append-bounded-history")).streamId());
        AppendSessionRecord session = acquireSession(boundedStore, streamId);

        for (int index = 0; index < 3; index++) {
            CommitSliceRequest request = request(
                    streamId,
                    session,
                    "new-object-" + index,
                    "slice-" + index,
                    index,
                    1);
            boundedStore.putObjectManifest(CLUSTER, manifest(request)).join();
            assertThat(boundedStore.commitStreamSlice(CLUSTER, request).join().commitVersion())
                    .isEqualTo(index + 1);
        }

        CommitSliceRequest fourth = request(streamId, session, "new-object-3", "slice-3", 3, 1);
        boundedStore.putObjectManifest(CLUSTER, manifest(fourth)).join();
        boundedStore.interleaveBeforeNextHeadCasWithSameWriterRenew(Duration.ofMillis(200));

        CommitSliceResult result = boundedStore.commitStreamSlice(CLUSTER, fourth).join();

        assertThat(result.commitVersion()).isEqualTo(4);
        assertThat(result.committedEndOffset()).isEqualTo(4);

        CommitSliceRequest displaced = request(
                streamId,
                session,
                "displaced-object",
                "displaced-slice",
                2,
                1);
        boundedStore.putObjectManifest(CLUSTER, manifest(displaced)).join();
        assertNereusFailure(
                () -> boundedStore.commitStreamSlice(CLUSTER, displaced).join(),
                ErrorCode.OFFSET_CONFLICT,
                true,
                AppendOutcome.KNOWN_NOT_COMMITTED);
    }

    @Test
    void commitVersionIsMonotonicAcrossSequentialCommits() {
        StreamId streamId = new StreamId(createStream(new StreamName("monotonic-version")).streamId());
        AppendSessionRecord session = acquireSession(streamId);

        CommitSliceRequest req1 = request(streamId, session, "object-mono-1", "slice-1", 0, 1);
        CommitSliceRequest req2 = request(streamId, session, "object-mono-2", "slice-2", 1, 1);
        CommitSliceRequest req3 = request(streamId, session, "object-mono-3", "slice-3", 2, 1);
        store.putObjectManifest(CLUSTER, manifest(req1)).join();
        store.putObjectManifest(CLUSTER, manifest(req2)).join();
        store.putObjectManifest(CLUSTER, manifest(req3)).join();

        CommitSliceResult first = store.commitStreamSlice(CLUSTER, req1).join();
        CommitSliceResult second = store.commitStreamSlice(CLUSTER, req2).join();
        CommitSliceResult third = store.commitStreamSlice(CLUSTER, req3).join();

        assertThat(first.commitVersion()).isEqualTo(1);
        assertThat(second.commitVersion()).isEqualTo(2);
        assertThat(third.commitVersion()).isEqualTo(3);

        CommittedEndOffsetRecord committedEnd = store.getCommittedEndOffset(CLUSTER, streamId).join();
        assertThat(committedEnd.committedEndOffset()).isEqualTo(3);
        assertThat(committedEnd.commitVersion()).isEqualTo(3);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join())
                .extracting(OffsetIndexRecord::commitVersion)
                .containsExactly(1L, 2L, 3L);
        assertThat(storedRecords(StreamCommitRecord.class).stream()
                .map(StreamCommitRecord::commitVersion)
                .distinct()
                .sorted()
                .toList())
                .containsExactly(1L, 2L, 3L);
        assertThat(storedRecords(CommittedSliceRecord.class).stream()
                .map(CommittedSliceRecord::commitVersion)
                .sorted()
                .toList())
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void staleEpochIsFencedBeforeOffsetConflict() {
        String writerA = "writerA";
        String writerB = "writerB";
        StreamId streamId = new StreamId(createStream(new StreamName("fenced-before-conflict")).streamId());
        AppendSessionRecord sessionA = store.acquireAppendSession(
                CLUSTER, streamId,
                new AppendSessionOptions(writerA, Duration.ofMillis(10), false))
                .join();
        clock.set(clock.get() + 11);

        AppendSessionRecord sessionB = store.acquireAppendSession(
                CLUSTER, streamId,
                new AppendSessionOptions(writerB, Duration.ofMillis(1), true))
                .join();
        CommitSliceRequest winner = request(
                streamId, sessionB, writerB, "object-winner", "slice-winner", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(winner)).join();
        store.commitStreamSlice(CLUSTER, winner).join();

        CommitSliceRequest staleRequest = new CommitSliceRequest(
                streamId,
                writerA,
                WRITER_RUN_ID_HASH,
                sessionA.epoch(),
                sessionA.fencingToken(),
                0,
                "stale-slice",
                1,
                1,
                7,
                List.of(),
                new ObjectId("object-stale"),
                new ObjectKey("object-stale-key"),
                checksum("aaaaaaaa"),
                10,
                20,
                entryIndexRef(),
                checksum("bbbbbbbb"),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                Optional.empty());
        store.putObjectManifest(CLUSTER, manifest(staleRequest)).join();

        assertNereusFailure(
                () -> store.commitStreamSlice(CLUSTER, staleRequest).join(),
                ErrorCode.FENCED_APPEND,
                true,
                AppendOutcome.KNOWN_NOT_COMMITTED);
    }

    @Test
    void appendInfrastructureRejectionsStillCarryKnownNotCommittedOutcome() {
        FakeOxiaMetadataStore localStore = new FakeOxiaMetadataStore(clock::get);
        StreamId missingStream = new StreamId("missing-stream");
        AppendSessionRecord syntheticSession = new AppendSessionRecord(
                missingStream.value(),
                WRITER_ID,
                1,
                "synthetic-token",
                1,
                clock.get() + 100);
        CommitSliceRequest request = request(
                missingStream,
                syntheticSession,
                "object-missing-stream",
                "slice-1",
                0,
                1);

        assertNereusFailure(
                () -> localStore.commitStreamSlice(CLUSTER, request).join(),
                ErrorCode.STREAM_NOT_FOUND,
                false,
                AppendOutcome.KNOWN_NOT_COMMITTED);

        localStore.close();
        assertNereusFailure(
                () -> localStore.commitStreamSlice(CLUSTER, request).join(),
                ErrorCode.STORAGE_CLOSED,
                false,
                AppendOutcome.KNOWN_NOT_COMMITTED);
    }

    @Test
    void retryReusesStoredCommitLogAfterFirstAttemptFailedHeadCasAndSessionRenew() {
        StreamId streamId = new StreamId(createStream(new StreamName("retry-commit-log")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-retry-cl", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();
        store.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_COMMIT_LOG_PUT);

        assertNereusFailure(
                () -> store.commitStreamSlice(CLUSTER, request).join(),
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                AppendOutcome.KNOWN_NOT_COMMITTED);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).isEmpty();

        AppendSessionRecord renewed = store.renewAppendSession(
                        CLUSTER,
                        streamId,
                        session.writerId(),
                        session.epoch(),
                        session.fencingToken(),
                        Duration.ofMillis(200))
                .join();
        assertThat(renewed.epoch()).isEqualTo(session.epoch());
        assertThat(renewed.fencingToken()).isEqualTo(session.fencingToken());

        CommitSliceResult result = store.commitStreamSlice(CLUSTER, request).join();

        assertThat(result.commitVersion()).isEqualTo(1);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).hasSize(1);
        assertThat(storedRecords(StreamCommitRecord.class).stream()
                .map(StreamCommitRecord::commitId)
                .distinct()
                .toList())
                .containsExactly(request.commitId());
    }

    private StreamMetadataRecord createStream(StreamName streamName) {
        return createStream(store, streamName);
    }

    private StreamMetadataRecord createStream(FakeOxiaMetadataStore metadataStore, StreamName streamName) {
        return metadataStore.createOrGetStream(
                CLUSTER,
                streamName,
                new StreamCreateOptions(StorageProfile.OBJECT_WAL, Map.of("alias", streamName.value())))
                .join();
    }

    private AppendSessionRecord acquireSession(StreamId streamId) {
        return acquireSession(store, streamId);
    }

    private AppendSessionRecord acquireSession(FakeOxiaMetadataStore metadataStore, StreamId streamId) {
        return metadataStore.acquireAppendSession(
                CLUSTER,
                streamId,
                new AppendSessionOptions(WRITER_ID, Duration.ofMillis(1_000), false))
                .join();
    }

    private CommitSliceRequest request(
            StreamId streamId,
            AppendSessionRecord session,
            String objectId,
            String sliceId,
            long expectedStartOffset,
            int recordCount) {
        return request(streamId, session, WRITER_ID, objectId, sliceId, expectedStartOffset, recordCount);
    }

    private CommitSliceRequest request(
            StreamId streamId,
            AppendSessionRecord session,
            String writerId,
            String objectId,
            String sliceId,
            long expectedStartOffset,
            int recordCount) {
        return new CommitSliceRequest(
                streamId,
                writerId,
                WRITER_RUN_ID_HASH,
                session.epoch(),
                session.fencingToken(),
                expectedStartOffset,
                sliceId,
                recordCount,
                recordCount,
                7,
                List.of(),
                new ObjectId(objectId),
                new ObjectKey(objectId + "-key"),
                checksum("11111111"),
                10,
                20,
                entryIndexRef(),
                checksum("22222222"),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                Optional.empty());
    }

    private StreamHeadRecord storedHead(StreamId streamId) {
        return store.storedMetadataValuesForTesting().stream()
                .filter(value -> value.recordType().equals(StreamHeadRecord.class.getSimpleName()))
                .filter(value -> value.key().contains("|head|" + streamId.value()))
                .findFirst()
                .map(value -> Phase1MetadataCodecs.decodeEnvelope(value.envelope(), StreamHeadRecord.class))
                .orElseThrow();
    }

    private <T> List<T> storedRecords(Class<T> recordClass) {
        return store.storedMetadataValuesForTesting().stream()
                .filter(value -> value.recordType().equals(recordClass.getSimpleName()))
                .map(value -> Phase1MetadataCodecs.decodeEnvelope(value.envelope(), recordClass))
                .toList();
    }

    private ObjectManifestRecord manifest(CommitSliceRequest request) {
        return new ObjectManifestRecord(
                request.objectId().value(),
                request.objectKey().value(),
                ObjectType.MULTI_STREAM_WAL_OBJECT.name(),
                "UPLOADED",
                1,
                0,
                "test-writer",
                request.writerId(),
                request.writerRunIdHash(),
                request.epoch(),
                clock.get(),
                clock.get(),
                128,
                request.objectChecksum().type().name(),
                request.objectChecksum().value(),
                ChecksumType.CRC32C.name(),
                "33333333",
                List.of(new StreamSliceManifestRecord(
                        0,
                        request.streamId().value(),
                        request.sliceId(),
                        request.epoch(),
                        request.objectOffset(),
                        request.objectLength(),
                        request.recordCount(),
                        request.entryCount(),
                        request.logicalBytes(),
                        request.schemaRefs(),
                        EntryIndexReferenceRecord.fromApi(request.entryIndexRef()),
                        request.sliceChecksum().type().name(),
                        request.sliceChecksum().value(),
                        request.payloadFormat().name(),
                        "UPLOADED")),
                clock.get() + 10_000,
                0);
    }

    private ObjectManifestRecord copyManifest(
            ObjectManifestRecord manifest,
            String state,
            int formatMajorVersion,
            List<StreamSliceManifestRecord> slices) {
        return new ObjectManifestRecord(
                manifest.objectId(),
                manifest.objectKey(),
                manifest.objectType(),
                state,
                formatMajorVersion,
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
                slices,
                manifest.orphanExpiresAtMillis(),
                manifest.metadataVersion());
    }

    private ObjectManifestRecord copyManifestWithObjectId(ObjectManifestRecord manifest, String objectId) {
        return new ObjectManifestRecord(
                objectId,
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
                manifest.metadataVersion());
    }

    private StreamSliceManifestRecord copySlice(
            StreamSliceManifestRecord slice,
            int ordinal,
            String streamId,
            String sliceId,
            long objectOffset,
            long objectLength) {
        return new StreamSliceManifestRecord(
                ordinal,
                streamId,
                sliceId,
                slice.writerEpoch(),
                objectOffset,
                objectLength,
                slice.recordCount(),
                slice.entryCount(),
                slice.logicalBytes(),
                slice.schemaRefs(),
                slice.entryIndexRef(),
                slice.sliceChecksumType(),
                slice.sliceChecksumValue(),
                slice.payloadFormat(),
                slice.state());
    }

    private EntryIndexRef entryIndexRef() {
        return new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                100,
                10,
                checksum("44444444"));
    }

    private Checksum checksum(String value) {
        return new Checksum(ChecksumType.CRC32C, value);
    }

    private void assertNereusCode(Runnable runnable, ErrorCode code) {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOfSatisfying(NereusException.class, exception ->
                        assertThat(exception.code()).isEqualTo(code));
    }

    private void assertNereusFailure(
            Runnable runnable,
            ErrorCode code,
            boolean retriable,
            AppendOutcome appendOutcome) {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOfSatisfying(NereusException.class, exception -> {
                    assertThat(exception.code()).isEqualTo(code);
                    assertThat(exception.retriable()).isEqualTo(retriable);
                    assertThat(exception.appendOutcome()).contains(appendOutcome);
                });
    }

    private void assertNereusFailureWithoutAppendOutcome(
            Runnable runnable,
            ErrorCode code,
            boolean retriable) {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOfSatisfying(NereusException.class, exception -> {
                    assertThat(exception.code()).isEqualTo(code);
                    assertThat(exception.retriable()).isEqualTo(retriable);
                    assertThat(exception.appendOutcome()).isEmpty();
                });
    }

    private static final class RecordingWatcher implements MetadataWatcher {
        private final List<String> events = new ArrayList<>();

        @Override
        public void onOffsetIndexUpdated(StreamId streamId, long committedEndOffset, long metadataVersion) {
            events.add("offset:" + committedEndOffset + ":" + metadataVersion);
        }

        @Override
        public void onTrimUpdated(StreamId streamId, long trimOffset, long metadataVersion) {
            events.add("trim:" + trimOffset);
        }

        @Override
        public void onAppendSessionChanged(StreamId streamId, long epoch, long leaseVersion) {
            events.add("session:" + epoch + ":" + leaseVersion);
        }

        @Override
        public void onWatchReconnected(StreamId streamId, long metadataVersion) {
            events.add("reconnect:" + metadataVersion);
        }

        List<String> events() {
            return List.copyOf(events);
        }

        void clear() {
            events.clear();
        }
    }

    private static final class ThrowingWatcher implements MetadataWatcher {
        @Override
        public void onOffsetIndexUpdated(StreamId streamId, long committedEndOffset, long metadataVersion) {
            throw new IllegalStateException("offset callback failure");
        }

        @Override
        public void onTrimUpdated(StreamId streamId, long trimOffset, long metadataVersion) {
            throw new IllegalStateException("trim callback failure");
        }

        @Override
        public void onAppendSessionChanged(StreamId streamId, long epoch, long leaseVersion) {
            throw new IllegalStateException("session callback failure");
        }

        @Override
        public void onWatchReconnected(StreamId streamId, long metadataVersion) {
            throw new IllegalStateException("reconnect callback failure");
        }
    }
}
