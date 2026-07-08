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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.nereus.api.AppendSessionOptions;
import io.nereus.api.Checksum;
import io.nereus.api.ChecksumType;
import io.nereus.api.EntryIndexLocation;
import io.nereus.api.EntryIndexRef;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.ObjectId;
import io.nereus.api.ObjectKey;
import io.nereus.api.ObjectType;
import io.nereus.api.PayloadFormat;
import io.nereus.api.StorageProfile;
import io.nereus.api.StreamCreateOptions;
import io.nereus.api.StreamId;
import io.nereus.api.StreamName;
import io.nereus.api.keys.DeterministicIds;
import io.nereus.metadata.oxia.CommitSliceRequest;
import io.nereus.metadata.oxia.CommitSliceResult;
import io.nereus.metadata.oxia.DerivedIndexRepairResult;
import io.nereus.metadata.oxia.MetadataWatcher;
import io.nereus.metadata.oxia.WatchRegistration;
import io.nereus.metadata.oxia.codec.MetadataRecordCodec;
import io.nereus.metadata.oxia.codec.MetadataRecordEnvelope;
import io.nereus.metadata.oxia.codec.Phase1MetadataCodecs;
import io.nereus.metadata.oxia.records.AppendSessionRecord;
import io.nereus.metadata.oxia.records.CommittedEndOffsetRecord;
import io.nereus.metadata.oxia.records.CommittedSliceRecord;
import io.nereus.metadata.oxia.records.EntryIndexReferenceRecord;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.metadata.oxia.records.ObjectReferenceRecord;
import io.nereus.metadata.oxia.records.OffsetIndexRecord;
import io.nereus.metadata.oxia.records.StreamCommitRecord;
import io.nereus.metadata.oxia.records.StreamHeadRecord;
import io.nereus.metadata.oxia.records.StreamMetadataRecord;
import io.nereus.metadata.oxia.records.StreamSliceManifestRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

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

        assertNereusCode(
                () -> store.commitStreamSlice(CLUSTER, request).join(),
                ErrorCode.METADATA_UNAVAILABLE);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).isEmpty();

        DerivedIndexRepairResult repair = store.repairDerivedStreamIndexes(CLUSTER, streamId, 0, 10).join();
        assertThat(repair.targetCovered()).isTrue();
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

        assertNereusCode(
                () -> store.commitStreamSlice(CLUSTER, conflict).join(),
                ErrorCode.OFFSET_CONFLICT);
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
    void repairBudgetExhaustionStopsAtMaxRecords() {
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

        assertThat(repair.repairedRecords()).isEqualTo(1);
        assertThat(repair.targetCovered()).isFalse();
        assertThat(repair.repairBudgetExhausted()).isTrue();
        assertThat(repair.repairedFromOffset()).isEqualTo(2);
        assertThat(repair.repairedToOffset()).isEqualTo(3);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join())
                .extracting(OffsetIndexRecord::offsetStart)
                .containsExactly(2L);

        DerivedIndexRepairResult secondRepair = store.repairDerivedStreamIndexes(CLUSTER, streamId, 0, 1).join();
        assertThat(secondRepair.repairedRecords()).isEqualTo(1);
        assertThat(secondRepair.targetCovered()).isFalse();
        assertThat(secondRepair.repairBudgetExhausted()).isTrue();
        assertThat(secondRepair.repairedFromOffset()).isEqualTo(1);
        assertThat(secondRepair.repairedToOffset()).isEqualTo(2);

        DerivedIndexRepairResult thirdRepair = store.repairDerivedStreamIndexes(CLUSTER, streamId, 0, 1).join();
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

        assertNereusCode(
                () -> store.commitStreamSlice(CLUSTER, staleRequest).join(),
                ErrorCode.FENCED_APPEND);
    }

    @Test
    void retryReusesStoredCommitLogAfterFirstAttemptFailedHeadCas() {
        StreamId streamId = new StreamId(createStream(new StreamName("retry-commit-log")).streamId());
        AppendSessionRecord session = acquireSession(streamId);
        CommitSliceRequest request = request(streamId, session, "object-retry-cl", "slice-1", 0, 1);
        store.putObjectManifest(CLUSTER, manifest(request)).join();
        store.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_COMMIT_LOG_PUT);

        assertNereusCode(
                () -> store.commitStreamSlice(CLUSTER, request).join(),
                ErrorCode.METADATA_UNAVAILABLE);
        assertThat(store.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).isEmpty();

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
        return store.createOrGetStream(
                CLUSTER,
                streamName,
                new StreamCreateOptions(StorageProfile.OBJECT_WAL, Map.of("alias", streamName.value())))
                .join();
    }

    private AppendSessionRecord acquireSession(StreamId streamId) {
        return store.acquireAppendSession(
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
}
