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

package io.nereus.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.nereus.api.AppendBatch;
import io.nereus.api.AppendEntry;
import io.nereus.api.AppendOptions;
import io.nereus.api.AppendOutcome;
import io.nereus.api.AppendResult;
import io.nereus.api.DurabilityLevel;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.PayloadFormat;
import io.nereus.api.ReadIsolation;
import io.nereus.api.ReadOptions;
import io.nereus.api.ResolveOptions;
import io.nereus.api.StorageProfile;
import io.nereus.api.StreamCreateOptions;
import io.nereus.api.StreamId;
import io.nereus.api.StreamName;
import io.nereus.api.TrimOptions;
import io.nereus.core.recovery.MetadataOrphanObjectScanner;
import io.nereus.core.recovery.OrphanObjectAssessment;
import io.nereus.core.recovery.OrphanObjectStatus;
import io.nereus.core.recovery.RecoveryMetricsObserver;
import io.nereus.metadata.oxia.OxiaClientConfiguration;
import io.nereus.metadata.oxia.OxiaJavaClientMetadataStore;
import io.nereus.metadata.oxia.OxiaKeyspace;
import io.nereus.metadata.oxia.OxiaMetadataStore;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.objectstore.HeadObjectOptions;
import io.nereus.objectstore.testing.LocalFileObjectStore;
import io.nereus.objectstore.wal.DefaultWalObjectReader;
import io.nereus.objectstore.wal.DefaultWalObjectWriter;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.testcontainers.OxiaContainer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class Phase1FinalIntegrationTest {
    private static final String IMAGE = "oxia/oxia:0.16.3";
    private static final Clock CLOCK = Clock.systemUTC();

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(IMAGE)).withShards(4);

    @TempDir
    Path root;

    @Test
    void fullStreamStorageFlowSurvivesFacadeMetadataAndObjectStoreRestart() {
        String cluster = "m8/full/" + UUID.randomUUID();
        Path objects = root.resolve("restart-objects");
        StreamId streamId;
        AppendResult firstAppend;
        AppendResult secondAppend;

        try (OxiaJavaClientMetadataStore metadata = metadata();
                LocalFileObjectStore objectStore = new LocalFileObjectStore(objects);
                DefaultStreamStorage storage = storage(cluster, metadata, objectStore)) {
            streamId = storage.createOrGetStream(
                    new StreamName("orders-0"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                    .join().streamId();
            firstAppend = storage.append(streamId, batch("a", "b", "c"), appendOptions()).join();
            secondAppend = storage.append(streamId, batch("d", "e"), appendOptions()).join();

            assertThat(storage.read(streamId, 0, readOptions()).join().batches())
                    .extracting(value -> text(value.payload()))
                    .containsExactly("a", "b", "c", "d", "e");
            assertThat(storage.resolve(
                    streamId, 3, new ResolveOptions(10, true, true)).join().ranges())
                    .singleElement().satisfies(range -> {
                        assertThat(range.offsetRange().startOffset()).isEqualTo(3);
                        assertThat(range.offsetRange().endOffset()).isEqualTo(5);
                    });
            long objectLength = objectStore.headObject(
                    firstAppend.objectKey(), new HeadObjectOptions(Duration.ofSeconds(5)))
                    .join().objectLength();
            storage.trim(streamId, 2, new TrimOptions(Duration.ofSeconds(5), "m8-retention")).join();
            assertThat(failure(storage.read(streamId, 1, readOptions())).code())
                    .isEqualTo(ErrorCode.OFFSET_TRIMMED);
            assertThat(storage.read(streamId, 2, readOptions()).join().batches())
                    .extracting(value -> text(value.payload()))
                    .containsExactly("c", "d", "e");
            assertThat(objectStore.headObject(
                    firstAppend.objectKey(), new HeadObjectOptions(Duration.ofSeconds(5)))
                    .join().objectLength()).isEqualTo(objectLength);
        }

        try (OxiaJavaClientMetadataStore metadata = metadata();
                LocalFileObjectStore objectStore = new LocalFileObjectStore(objects);
                DefaultStreamStorage restarted = storage(cluster, metadata, objectStore)) {
            assertThat(restarted.read(streamId, 2, readOptions()).join().batches())
                    .extracting(value -> text(value.payload()))
                    .containsExactly("c", "d", "e");
            AppendResult afterRestart = restarted.append(
                    streamId, batch("f"), appendOptions()).join();
            assertThat(afterRestart.range().startOffset()).isEqualTo(5);
            assertThat(afterRestart.range().endOffset()).isEqualTo(6);
            assertThat(afterRestart.objectId()).isNotEqualTo(firstAppend.objectId());
            assertThat(afterRestart.objectId()).isNotEqualTo(secondAppend.objectId());
            assertThat(restarted.getStreamMetadata(streamId).join().committedEndOffset()).isEqualTo(6);
        }
    }

    @Test
    void orphanRemainsInvisibleAndReachableHeadRepairsDeletedDerivedIndexAfterRestart() throws Exception {
        String cluster = "m8/failures/" + UUID.randomUUID();
        Path objects = root.resolve("failure-objects");
        AtomicReference<ObjectManifestRecord> orphanManifest = new AtomicReference<>();

        try (OxiaJavaClientMetadataStore metadata = metadata();
                LocalFileObjectStore objectStore = new LocalFileObjectStore(objects)) {
            OxiaMetadataStore failBeforeHead = failNextCommitBeforeHead(metadata, orphanManifest);
            try (DefaultStreamStorage storage = storage(cluster, failBeforeHead, objectStore)) {
                StreamId orphanStream = storage.createOrGetStream(
                        new StreamName("orphan"),
                        new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                        .join().streamId();
                NereusException appendFailure = failure(storage.append(
                        orphanStream, batch("orphan"), appendOptions()));
                assertThat(appendFailure.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);
                assertThat(metadata.getCommittedEndOffset(cluster, orphanStream).join().committedEndOffset())
                        .isZero();
                assertThat(storage.read(orphanStream, 0, readOptions()).join().batches()).isEmpty();
            }

            ObjectManifestRecord manifest = orphanManifest.get();
            assertThat(manifest).isNotNull();
            try (MetadataOrphanObjectScanner scanner = new MetadataOrphanObjectScanner(
                    cluster, metadata, RecoveryMetricsObserver.noop(), Runnable::run)) {
                OrphanObjectAssessment assessment = scanner.scan(
                        new io.nereus.api.ObjectId(manifest.objectId())).join();
                assertThat(assessment.status()).isEqualTo(OrphanObjectStatus.UNREFERENCED_MANIFEST);
                assertThat(assessment.deletionAllowed()).isFalse();
            }
            assertThat(objectStore.headObject(
                    new io.nereus.api.ObjectKey(manifest.objectKey()),
                    new HeadObjectOptions(Duration.ofSeconds(5))).join().objectLength())
                    .isEqualTo(manifest.objectLength());
        }

        StreamId repairStream;
        AppendResult committed;
        try (OxiaJavaClientMetadataStore metadata = metadata();
                LocalFileObjectStore objectStore = new LocalFileObjectStore(objects);
                DefaultStreamStorage storage = storage(cluster, metadata, objectStore)) {
            repairStream = storage.createOrGetStream(
                    new StreamName("repair"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                    .join().streamId();
            committed = storage.append(repairStream, batch("committed"), appendOptions()).join();
        }

        OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
        String partition = keyspace.streamPartitionKey(repairStream).value();
        try (var raw = OxiaClientBuilder.create(OXIA.getServiceAddress()).syncClient()) {
            raw.delete(
                    keyspace.offsetIndexKey(repairStream, committed.range().endOffset(), committed.generation()),
                    Set.of(DeleteOption.PartitionKey(partition)));
            raw.delete(
                    keyspace.committedSliceKey(repairStream, committed.objectId(), committed.sliceId()),
                    Set.of(DeleteOption.PartitionKey(partition)));
        }

        try (OxiaJavaClientMetadataStore metadata = metadata();
                LocalFileObjectStore objectStore = new LocalFileObjectStore(objects);
                DefaultStreamStorage restarted = storage(cluster, metadata, objectStore)) {
            assertThat(metadata.scanOffsetIndex(cluster, repairStream, 0, 10).join()).isEmpty();
            assertThat(restarted.read(repairStream, 0, readOptions()).join().batches())
                    .extracting(value -> text(value.payload()))
                    .containsExactly("committed");
            assertThat(metadata.scanOffsetIndex(cluster, repairStream, 0, 10).join()).hasSize(1);
            try (MetadataOrphanObjectScanner scanner = new MetadataOrphanObjectScanner(
                    cluster, metadata, RecoveryMetricsObserver.noop(), Runnable::run)) {
                assertThat(scanner.scan(committed.objectId()).join().status())
                        .isEqualTo(OrphanObjectStatus.FULLY_REFERENCED);
            }
        }
    }

    private OxiaJavaClientMetadataStore metadata() {
        return OxiaJavaClientMetadataStore.connect(
                new OxiaClientConfiguration(
                        OXIA.getServiceAddress(),
                        "default",
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        1_000),
                CLOCK);
    }

    private static DefaultStreamStorage storage(
            String cluster,
            OxiaMetadataStore metadata,
            LocalFileObjectStore objectStore) {
        return new DefaultStreamStorage(
                StreamStorageConfig.defaults(cluster, "writer-a"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "phase1-writer", CLOCK),
                new DefaultWalObjectReader(objectStore),
                CLOCK,
                Runnable::run);
    }

    private static OxiaMetadataStore failNextCommitBeforeHead(
            OxiaMetadataStore delegate,
            AtomicReference<ObjectManifestRecord> manifestCapture) {
        AtomicBoolean fail = new AtomicBoolean(true);
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("putObjectManifest")) {
                        manifestCapture.set((ObjectManifestRecord) args[1]);
                    }
                    if (method.getName().equals("commitStreamSlice") && fail.compareAndSet(true, false)) {
                        return NereusException.failedAppendFuture(
                                ErrorCode.METADATA_UNAVAILABLE,
                                true,
                                AppendOutcome.KNOWN_NOT_COMMITTED,
                                "injected M8 failure before stream-head CAS");
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static AppendBatch batch(String... values) {
        long now = System.currentTimeMillis();
        List<AppendEntry> entries = Arrays.stream(values)
                .map(value -> new AppendEntry(value.getBytes(StandardCharsets.UTF_8), 1, now, Map.of()))
                .toList();
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                entries,
                entries.size(),
                entries.size(),
                now,
                now,
                List.of(),
                Map.of(),
                Optional.empty());
    }

    private static AppendOptions appendOptions() {
        return new AppendOptions(
                Optional.empty(),
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                Duration.ofSeconds(10),
                true,
                Map.of());
    }

    private static ReadOptions readOptions() {
        return new ReadOptions(100, 1 << 20, ReadIsolation.COMMITTED, Duration.ofSeconds(10));
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

    private static String text(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
