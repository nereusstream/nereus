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

package io.nereus.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.nereus.api.AppendBatch;
import io.nereus.api.AppendEntry;
import io.nereus.api.AppendOptions;
import io.nereus.api.AppendOutcome;
import io.nereus.api.DurabilityLevel;
import io.nereus.api.NereusException;
import io.nereus.api.ObjectId;
import io.nereus.api.ObjectKey;
import io.nereus.api.PayloadFormat;
import io.nereus.api.ReadIsolation;
import io.nereus.api.ReadOptions;
import io.nereus.api.StorageProfile;
import io.nereus.api.StreamCreateOptions;
import io.nereus.api.StreamId;
import io.nereus.api.StreamName;
import io.nereus.core.DefaultStreamStorage;
import io.nereus.core.StreamStorageConfig;
import io.nereus.metadata.oxia.OxiaMetadataStore;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.metadata.oxia.testing.FakeOxiaMetadataStore;
import io.nereus.objectstore.HeadObjectOptions;
import io.nereus.objectstore.testing.LocalFileObjectStore;
import io.nereus.objectstore.wal.DefaultWalObjectReader;
import io.nereus.objectstore.wal.DefaultWalObjectWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetadataOrphanObjectScannerTest {
    private static final String CLUSTER = "cluster/a";
    private static final Instant NOW = Instant.parse("2026-07-11T09:10:11Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @TempDir
    Path root;

    @Test
    void missingManifestIsDiagnosticOnlyAndNeverDeletionAuthorization() {
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        RecordingRecoveryMetrics metrics = new RecordingRecoveryMetrics();
        try (MetadataOrphanObjectScanner scanner = new MetadataOrphanObjectScanner(
                CLUSTER, metadata, metrics, Runnable::run)) {
            OrphanObjectAssessment assessment = scanner.scan(new ObjectId("operational-object-id")).join();

            assertThat(assessment.status()).isEqualTo(OrphanObjectStatus.MISSING_MANIFEST);
            assertThat(assessment.manifestSliceCount()).isZero();
            assertThat(assessment.reachableSliceCount()).isZero();
            assertThat(assessment.deletionAllowed()).isFalse();
            assertThat(metrics.assessments).hasValue(1);
            assertThat(metrics.repairs).hasValue(0);
        } finally {
            metadata.close();
        }
    }

    @Test
    void uploadedManifestWithoutReachableHeadIsAnUnreferencedCandidateAndRemainsStored() {
        try (TestContext context = context()) {
            StreamId streamId = context.createStream("manifest-only");
            context.metadata.failNext(FakeOxiaMetadataStore.FailurePoint.BEFORE_HEAD_CAS);

            NereusException failure = failure(context.storage.append(
                    streamId, batch("not-committed"), appendOptions()));
            ObjectManifestRecord manifest = context.lastManifest.get();

            assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);
            assertThat(manifest).isNotNull();
            OrphanObjectAssessment assessment = context.scanner.scan(new ObjectId(manifest.objectId())).join();
            assertThat(assessment.status()).isEqualTo(OrphanObjectStatus.UNREFERENCED_MANIFEST);
            assertThat(assessment.reachableSliceCount()).isZero();
            assertThat(assessment.deletionAllowed()).isFalse();
            assertThat(context.metadata.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).isEmpty();
            assertThat(context.metadata.getCommittedEndOffset(CLUSTER, streamId).join().committedEndOffset())
                    .isZero();
            assertThat(context.objectStore.headObject(
                    new ObjectKey(manifest.objectKey()),
                    new HeadObjectOptions(Duration.ofSeconds(1))).join().objectLength())
                    .isEqualTo(manifest.objectLength());
        }
    }

    @Test
    void reachableHeadWithMissingIndexIsRepairedAndNeverClassifiedAsOrphan() {
        try (TestContext context = context()) {
            StreamId streamId = context.createStream("committed-repair");
            context.metadata.failNext(
                    FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);

            NereusException failure = failure(context.storage.append(
                    streamId, batch("committed"), appendOptions()));
            ObjectManifestRecord manifest = context.lastManifest.get();
            ObjectId objectId = new ObjectId(manifest.objectId());

            assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_COMMITTED);
            assertThat(context.metadata.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).isEmpty();

            OrphanObjectAssessment assessment = context.scanner.scan(objectId).join();
            assertThat(assessment.status()).isEqualTo(OrphanObjectStatus.FULLY_REFERENCED);
            assertThat(assessment.reachableSliceCount()).isEqualTo(1);
            assertThat(assessment.deletionAllowed()).isFalse();
            assertThat(context.metadata.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).hasSize(1);
            assertThat(context.storage.read(
                    streamId,
                    0,
                    new ReadOptions(10, 1024, ReadIsolation.COMMITTED, Duration.ofSeconds(1)))
                    .join().batches())
                    .extracting(batch -> new String(batch.payload(), StandardCharsets.UTF_8))
                    .containsExactly("committed");
            assertThat(context.metrics.repairs).hasValue(1);
            assertThat(context.metrics.assessments).hasValue(1);

            try (MetadataOrphanObjectScanner throwingMetricsScanner = new MetadataOrphanObjectScanner(
                    CLUSTER,
                    context.metadata,
                    new RecoveryMetricsObserver() {
                        @Override
                        public void onObjectReferenceRepair(int reachableSliceCount) {
                            throw new IllegalStateException("metrics failed");
                        }

                        @Override
                        public void onObjectAssessed(OrphanObjectStatus status) {
                            throw new IllegalStateException("metrics failed");
                        }
                    },
                    Runnable::run)) {
                assertThat(throwingMetricsScanner.scan(objectId).join().status())
                        .isEqualTo(OrphanObjectStatus.FULLY_REFERENCED);
            }
        }
    }

    @Test
    void closedScannerRejectsNewDiagnosticsWithoutClosingMetadata() {
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        MetadataOrphanObjectScanner scanner = new MetadataOrphanObjectScanner(
                CLUSTER, metadata, RecoveryMetricsObserver.noop(), Runnable::run);
        scanner.close();

        assertThat(failure(scanner.scan(new ObjectId("closed-object"))).code())
                .isEqualTo(io.nereus.api.ErrorCode.STORAGE_CLOSED);
        metadata.createOrGetStream(
                CLUSTER,
                new StreamName("metadata-still-open"),
                new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join();
        metadata.close();
    }

    @Test
    void assessmentRejectsStatusAndCountContradictions() {
        ObjectId objectId = new ObjectId("object-id");

        assertThatThrownBy(() -> new OrphanObjectAssessment(
                objectId, OrphanObjectStatus.FULLY_REFERENCED, 2, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrphanObjectAssessment(
                objectId, OrphanObjectStatus.PARTIALLY_REFERENCED, 2, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrphanObjectAssessment(
                objectId, OrphanObjectStatus.UNREFERENCED_MANIFEST, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private TestContext context() {
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        AtomicReference<ObjectManifestRecord> lastManifest = new AtomicReference<>();
        OxiaMetadataStore capturing = captureManifest(metadata, lastManifest);
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root.resolve("objects-" + System.nanoTime()));
        DefaultStreamStorage storage = new DefaultStreamStorage(
                StreamStorageConfig.defaults(CLUSTER, "writer-a"),
                capturing,
                new DefaultWalObjectWriter(objectStore, "test-writer-1", CLOCK),
                new DefaultWalObjectReader(objectStore),
                CLOCK,
                Runnable::run);
        RecordingRecoveryMetrics metrics = new RecordingRecoveryMetrics();
        MetadataOrphanObjectScanner scanner = new MetadataOrphanObjectScanner(
                CLUSTER, metadata, metrics, Runnable::run);
        return new TestContext(storage, metadata, objectStore, scanner, metrics, lastManifest);
    }

    private static OxiaMetadataStore captureManifest(
            OxiaMetadataStore delegate,
            AtomicReference<ObjectManifestRecord> capture) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("putObjectManifest")) {
                        capture.set((ObjectManifestRecord) args[1]);
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static AppendBatch batch(String value) {
        AppendEntry entry = new AppendEntry(
                value.getBytes(StandardCharsets.UTF_8), 1, NOW.toEpochMilli(), Map.of());
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(entry),
                1,
                1,
                NOW.toEpochMilli(),
                NOW.toEpochMilli(),
                List.of(),
                Map.of(),
                Optional.empty());
    }

    private static AppendOptions appendOptions() {
        return new AppendOptions(
                Optional.empty(),
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                Duration.ofSeconds(1),
                true,
                Map.of());
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

    private static final class RecordingRecoveryMetrics implements RecoveryMetricsObserver {
        private final AtomicInteger repairs = new AtomicInteger();
        private final AtomicInteger assessments = new AtomicInteger();

        @Override
        public void onObjectReferenceRepair(int reachableSliceCount) {
            repairs.incrementAndGet();
        }

        @Override
        public void onObjectAssessed(OrphanObjectStatus status) {
            assessments.incrementAndGet();
        }
    }

    private record TestContext(
            DefaultStreamStorage storage,
            FakeOxiaMetadataStore metadata,
            LocalFileObjectStore objectStore,
            MetadataOrphanObjectScanner scanner,
            RecordingRecoveryMetrics metrics,
            AtomicReference<ObjectManifestRecord> lastManifest) implements AutoCloseable {
        StreamId createStream(String name) {
            return storage.createOrGetStream(
                    new StreamName(name),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                    .join().streamId();
        }

        @Override
        public void close() {
            scanner.close();
            storage.close();
            metadata.close();
            objectStore.close();
        }
    }
}
