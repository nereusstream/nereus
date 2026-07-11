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

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.AppendOutcome;
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
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import com.nereusstream.metadata.oxia.testing.OxiaMetadataStoreContractScenario;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class OxiaJavaClientMetadataStoreIntegrationTest {
    private static final String IMAGE = "oxia/oxia:0.16.3";
    private static final String WRITER = "writer-a";
    private static final String RUN = "run-a";

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(IMAGE)).withShards(4);

    @Test
    void productionAdapterPersistsCommitReplayTrimAndReferencesAcrossRestart() throws Exception {
        String cluster = "m7/restart/" + UUID.randomUUID();
        OxiaClientConfiguration config = configuration();
        StreamId streamId;
        CommitSliceRequest request;
        try (OxiaJavaClientMetadataStore first = OxiaJavaClientMetadataStore.connect(config, Clock.systemUTC())) {
            streamId = new StreamId(first.createOrGetStream(
                    cluster,
                    new StreamName("orders"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                    .join().streamId());
            AppendSessionRecord session = first.acquireAppendSession(
                    cluster,
                    streamId,
                    new AppendSessionOptions(WRITER, Duration.ofSeconds(30), false)).join();
            request = request(streamId, session, "object-1", "slice-1", 0);
            first.putObjectManifest(cluster, manifest(request)).join();

            CommitSliceResult committed = first.commitStreamSlice(cluster, request).join();
            assertThat(committed.commitVersion()).isEqualTo(1);
            assertThat(first.commitStreamSlice(cluster, request).join()).isEqualTo(committed);
            assertThat(first.scanOffsetIndex(cluster, streamId, 0, 10).join()).hasSize(1);
            assertThat(first.getObjectReferences(cluster, request.objectId()).join())
                    .get().extracting(reference -> reference.visibleSlices().size()).isEqualTo(1);
        }

        try (OxiaJavaClientMetadataStore restarted = OxiaJavaClientMetadataStore.connect(config, Clock.systemUTC())) {
            assertThat(restarted.getCommittedEndOffset(cluster, streamId).join().committedEndOffset())
                    .isEqualTo(1);
            assertThat(restarted.scanOffsetIndex(cluster, streamId, 0, 10).join()).hasSize(1);
            assertThat(restarted.commitStreamSlice(cluster, request).join().commitVersion()).isEqualTo(1);
            restarted.updateTrim(cluster, streamId, 1, "restart-retention").join();
            assertThat(restarted.getTrim(cluster, streamId).join().trimOffset()).isEqualTo(1);
        }
    }

    @Test
    void boundedRepairRecreatesDeletedDerivedIndexFromReachableHead() throws Exception {
        String cluster = "m7/repair/" + UUID.randomUUID();
        OxiaClientConfiguration config = configuration();
        try (OxiaJavaClientMetadataStore store = OxiaJavaClientMetadataStore.connect(config, Clock.systemUTC())) {
            StreamId streamId = new StreamId(store.createOrGetStream(
                    cluster,
                    new StreamName("repair"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                    .join().streamId());
            AppendSessionRecord session = store.acquireAppendSession(
                    cluster,
                    streamId,
                    new AppendSessionOptions(WRITER, Duration.ofSeconds(30), false)).join();
            CommitSliceRequest first = request(streamId, session, "object-a", "slice-a", 0);
            CommitSliceRequest second = request(streamId, session, "object-b", "slice-b", 1);
            store.putObjectManifest(cluster, manifest(first)).join();
            store.commitStreamSlice(cluster, first).join();
            store.putObjectManifest(cluster, manifest(second)).join();
            store.commitStreamSlice(cluster, second).join();

            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            try (var raw = OxiaClientBuilder.create(OXIA.getServiceAddress()).syncClient()) {
                raw.delete(
                        keyspace.offsetIndexKey(streamId, 1, 0),
                        Set.of(DeleteOption.PartitionKey(keyspace.streamPartitionKey(streamId).value())));
            }
            assertThat(store.scanOffsetIndex(cluster, streamId, 0, 10).join()).hasSize(1);

            DerivedIndexRepairResult firstPage = store.repairDerivedStreamIndexes(
                    cluster, streamId, 0, Optional.empty(), 1).join();
            assertThat(firstPage.repairBudgetExhausted()).isTrue();
            DerivedIndexRepairResult secondPage = store.repairDerivedStreamIndexes(
                    cluster, streamId, 0, firstPage.continuation(), 1).join();
            assertThat(secondPage.targetCovered()).isTrue();
            assertThat(secondPage.repairedRecords()).isEqualTo(1);
            assertThat(store.scanOffsetIndex(cluster, streamId, 0, 10).join()).hasSize(2);
        }
    }

    @Test
    void separateAdaptersContendThroughRealStreamHeadCasWithoutCreatingAGap() {
        String cluster = "m7/cas/" + UUID.randomUUID();
        OxiaClientConfiguration config = configuration();
        try (OxiaJavaClientMetadataStore first = OxiaJavaClientMetadataStore.connect(config, Clock.systemUTC());
                OxiaJavaClientMetadataStore second = OxiaJavaClientMetadataStore.connect(config, Clock.systemUTC())) {
            StreamId streamId = new StreamId(first.createOrGetStream(
                    cluster,
                    new StreamName("cas"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                    .join().streamId());
            AppendSessionRecord owner = first.acquireAppendSession(
                    cluster,
                    streamId,
                    new AppendSessionOptions(WRITER, Duration.ofSeconds(30), false)).join();
            AppendSessionRecord sameOwner = second.acquireAppendSession(
                    cluster,
                    streamId,
                    new AppendSessionOptions(WRITER, Duration.ofSeconds(30), false)).join();
            assertThat(sameOwner.epoch()).isEqualTo(owner.epoch());
            assertThat(sameOwner.leaseVersion()).isGreaterThan(owner.leaseVersion());
            CommitSliceRequest left = request(streamId, sameOwner, "cas-left", "slice-left", 0);
            CommitSliceRequest right = request(streamId, sameOwner, "cas-right", "slice-right", 0);
            first.putObjectManifest(cluster, manifest(left)).join();
            second.putObjectManifest(cluster, manifest(right)).join();

            List<CompletableFuture<CommitSliceResult>> attempts = List.of(
                    first.commitStreamSlice(cluster, left),
                    second.commitStreamSlice(cluster, right));
            CompletableFuture.allOf(attempts.stream()
                    .map(future -> future.handle((value, error) -> null))
                    .toArray(CompletableFuture[]::new)).join();

            assertThat(attempts.stream().filter(future -> !future.isCompletedExceptionally())).hasSize(1);
            CompletableFuture<CommitSliceResult> loser = attempts.stream()
                    .filter(CompletableFuture::isCompletedExceptionally)
                    .findFirst().orElseThrow();
            NereusException failure = failure(loser);
            assertThat(failure.code()).isEqualTo(ErrorCode.OFFSET_CONFLICT);
            assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);
            assertThat(first.getCommittedEndOffset(cluster, streamId).join().committedEndOffset()).isEqualTo(1);
            assertThat(first.scanOffsetIndex(cluster, streamId, 0, 10).join()).hasSize(1);
        }
    }

    @Test
    void realNotificationStreamInvalidatesRegisteredStreamWatcher() throws Exception {
        String cluster = "m7/watch/" + UUID.randomUUID();
        try (OxiaJavaClientMetadataStore store =
                OxiaJavaClientMetadataStore.connect(configuration(), Clock.systemUTC())) {
            StreamId streamId = new StreamId(store.createOrGetStream(
                    cluster,
                    new StreamName("watch"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                    .join().streamId());
            CountDownLatch trimObserved = new CountDownLatch(1);
            try (WatchRegistration ignored = store.watchStream(cluster, streamId, new MetadataWatcher() {
                @Override
                public void onOffsetIndexUpdated(StreamId changed, long endOffset, long metadataVersion) {
                }

                @Override
                public void onTrimUpdated(StreamId changed, long trimOffset, long metadataVersion) {
                    if (changed.equals(streamId)) {
                        trimObserved.countDown();
                    }
                }

                @Override
                public void onAppendSessionChanged(StreamId changed, long epoch, long leaseVersion) {
                }
            })) {
                store.updateTrim(cluster, streamId, 0, "watch").join();
                assertThat(trimObserved.await(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void realAdapterPassesTheSharedFakeRealOperationContract() {
        try (OxiaJavaClientMetadataStore store =
                OxiaJavaClientMetadataStore.connect(configuration(), Clock.systemUTC())) {
            assertThat(OxiaMetadataStoreContractScenario.run(
                            store, "shared/real/" + UUID.randomUUID()).commitVersion())
                    .isEqualTo(1);
        }
    }

    private static OxiaClientConfiguration configuration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                100,
                1_024);
    }

    private static CommitSliceRequest request(
            StreamId streamId,
            AppendSessionRecord session,
            String objectId,
            String sliceId,
            long offset) {
        return new CommitSliceRequest(
                streamId,
                WRITER,
                RUN,
                session.epoch(),
                session.fencingToken(),
                offset,
                sliceId,
                1,
                1,
                7,
                List.of(),
                new ObjectId(objectId),
                new ObjectKey(objectId + "-key"),
                checksum("11111111"),
                10,
                20,
                new EntryIndexRef(
                        EntryIndexLocation.OBJECT_FOOTER,
                        Optional.of(new ObjectId(objectId)),
                        Optional.of(new ObjectKey(objectId + "-key")),
                        Optional.empty(),
                        64,
                        16,
                        checksum("22222222")),
                checksum("33333333"),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                Optional.empty());
    }

    private static ObjectManifestRecord manifest(CommitSliceRequest request) {
        long now = System.currentTimeMillis();
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
                now,
                now,
                128,
                request.objectChecksum().type().name(),
                request.objectChecksum().value(),
                ChecksumType.CRC32C.name(),
                "44444444",
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
                now + 60_000,
                0);
    }

    private static Checksum checksum(String value) {
        return new Checksum(ChecksumType.CRC32C, value);
    }

    private static NereusException failure(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("future completed successfully");
        } catch (CompletionException e) {
            assertThat(e.getCause()).isInstanceOf(NereusException.class);
            return (NereusException) e.getCause();
        }
    }
}
