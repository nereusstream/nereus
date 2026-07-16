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

package com.nereusstream.core.append;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.profile.Phase4StorageProfileResolver;
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.core.recovery.MetadataAppendRecoverySearcher;
import com.nereusstream.core.trim.TrimMetricsObserver;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import com.nereusstream.objectstore.wal.PreparedWalObject;
import com.nereusstream.objectstore.wal.WalObjectWriter;
import com.nereusstream.objectstore.wal.WalWriteRequest;
import com.nereusstream.objectstore.wal.WalWriteResult;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AsyncAppendPhysicalProtectionTest {
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @TempDir
    Path temporaryDirectory;

    @Test
    void failedSecondaryProtectionNeverRevokesStableWalAcknowledgement() {
        var stable = AsyncObjectWalAppendCoordinatorTest.stable();
        MaterializedGenerationZero materialized =
                AsyncObjectWalAppendCoordinatorTest.materialized(
                        stable.reachableAppend().committedAppend());
        AsyncObjectWalAppendCoordinatorTest.ControlledPublisher publisher =
                new AsyncObjectWalAppendCoordinatorTest.ControlledPublisher();
        AsyncObjectWalAppendCoordinator coordinator =
                new AsyncObjectWalAppendCoordinator(
                        append -> CompletableFuture.completedFuture(
                                materialized),
                        publisher,
                        Duration.ofSeconds(5),
                        Runnable::run);

        var acknowledged = coordinator.completeAfterStableCommit(
                stable,
                DurabilityLevel.WAL_DURABLE,
                Duration.ofMillis(1));
        publisher.visible.completeExceptionally(
                new IllegalStateException("protection unavailable"));

        assertThat(acknowledged).isCompletedWithValue(
                stable.reachableAppend().committedAppend());
        assertThat(coordinator.inFlightBackgroundRepairCount()).isZero();
        assertThat(coordinator.backgroundRepairFailureCount()).isOne();
    }

    @Test
    void strictBoundaryPropagatesTheSameProtectionFailure() {
        var stable = AsyncObjectWalAppendCoordinatorTest.stable();
        MaterializedGenerationZero materialized =
                AsyncObjectWalAppendCoordinatorTest.materialized(
                        stable.reachableAppend().committedAppend());
        AsyncObjectWalAppendCoordinatorTest.ControlledPublisher publisher =
                new AsyncObjectWalAppendCoordinatorTest.ControlledPublisher();
        AsyncObjectWalAppendCoordinator coordinator =
                new AsyncObjectWalAppendCoordinator(
                        append -> CompletableFuture.completedFuture(
                                materialized),
                        publisher,
                        Duration.ofSeconds(5),
                        Runnable::run);

        var strict = coordinator.completeAfterStableCommit(
                stable,
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                Duration.ofSeconds(5));
        publisher.visible.completeExceptionally(
                new IllegalStateException("protection unavailable"));

        assertThat(strict).isCompletedExceptionally();
    }

    @Test
    void fullAppendReturnsAtProtectedHeadAndDetachedRepairPublishesProtection() {
        StreamStorageConfig config =
                StreamStorageConfig.defaults("async-cluster", "writer");
        FakeOxiaMetadataStore metadata =
                new FakeOxiaMetadataStore(CLOCK::millis);
        CompletableFuture<MaterializedGenerationZero> delayedMaterialization =
                new CompletableFuture<>();
        AtomicReference<ReachableCommittedAppend> reachable =
                new AtomicReference<>();
        OxiaMetadataStore delayed = delayMaterialization(
                metadata, delayedMaterialization, reachable);
        LocalFileObjectStore objects =
                new LocalFileObjectStore(temporaryDirectory.resolve("objects"));
        DefaultObjectProtectionManager protections =
                new DefaultObjectProtectionManager(
                        config.cluster(),
                        metadata,
                        Duration.ofMinutes(10),
                        Duration.ZERO,
                        Duration.ofHours(24),
                        CLOCK);
        GenerationZeroPhysicalReferencePublisher physicalReferences =
                new DefaultGenerationZeroPhysicalReferencePublisher(
                        config.cluster(), delayed, metadata, protections);
        DefaultStreamStorage storage = new DefaultStreamStorage(
                config,
                delayed,
                new DefaultWalObjectWriter(objects, "test-writer", CLOCK),
                new DefaultWalObjectReader(objects),
                physicalReferences,
                new MetadataAppendRecoverySearcher(
                        config.cluster(), delayed),
                new Phase4StorageProfileResolver(),
                CLOCK,
                Runnable::run,
                ReadMetricsObserver.noop(),
                TrimMetricsObserver.noop());
        try {
            var stream = storage.createOrGetStream(
                            new StreamName("async-object-wal"),
                            new StreamCreateOptions(
                                    StorageProfile
                                            .OBJECT_WAL_ASYNC_OBJECT,
                                    Map.of()))
                    .join();

            var append = storage.append(
                            stream.streamId(),
                            batch("payload"),
                            new AppendOptions(
                                    Optional.empty(),
                                    DurabilityLevel.WAL_DURABLE,
                                    Duration.ofSeconds(5),
                                    true,
                                    Map.of()))
                    .join();

            assertThat(append.committedEndOffset()).isOne();
            assertThat(reachable).doesNotHaveValue(null);
            assertThat(delayedMaterialization).isNotDone();
            assertThat(metadata.getCommittedEndOffset(
                            config.cluster(), stream.streamId())
                    .join()
                    .committedEndOffset())
                    .isOne();

            MaterializedGenerationZero materialized =
                    metadata.materializeGenerationZero(
                                    config.cluster(), reachable.get())
                            .join();
            delayedMaterialization.complete(materialized);

            ObjectKeyHash object = ObjectKeyHash.from(
                    ((ObjectSliceReadTarget) append.readTarget())
                            .objectKey());
            assertThat(metadata.scanProtections(
                                    config.cluster(),
                                    object,
                                    Optional.empty(),
                                    16)
                            .join()
                            .values())
                    .anyMatch(value -> ObjectProtectionType.fromWireId(
                                    value.value().protectionTypeId())
                            == ObjectProtectionType.VISIBLE_GENERATION);
        } finally {
            storage.close();
            metadata.close();
            objects.close();
        }
    }

    @Test
    void serializedAdmissionCompletesBeforePrimaryWalPreparation() {
        StreamStorageConfig config =
                StreamStorageConfig.defaults(
                        "async-admission-cluster", "writer");
        FakeOxiaMetadataStore metadata =
                new FakeOxiaMetadataStore(CLOCK::millis);
        LocalFileObjectStore objects =
                new LocalFileObjectStore(
                        temporaryDirectory.resolve(
                                "admission-objects"));
        DefaultObjectProtectionManager protections =
                new DefaultObjectProtectionManager(
                        config.cluster(),
                        metadata,
                        Duration.ofMinutes(10),
                        Duration.ZERO,
                        Duration.ofHours(24),
                        CLOCK);
        GenerationZeroPhysicalReferencePublisher physicalReferences =
                new DefaultGenerationZeroPhysicalReferencePublisher(
                        config.cluster(),
                        metadata,
                        metadata,
                        protections);
        DefaultWalObjectWriter delegate =
                new DefaultWalObjectWriter(
                        objects, "test-writer", CLOCK);
        AtomicInteger prepares = new AtomicInteger();
        WalObjectWriter writer = new WalObjectWriter() {
            @Override
            public PreparedWalObject prepare(
                    WalWriteRequest request) {
                prepares.incrementAndGet();
                return delegate.prepare(request);
            }

            @Override
            public CompletableFuture<WalWriteResult> upload(
                    PreparedWalObject preparedObject) {
                return delegate.upload(preparedObject);
            }
        };
        CompletableFuture<Void> admission =
                new CompletableFuture<>();
        AtomicReference<AppendAdmissionRequest> admitted =
                new AtomicReference<>();
        DefaultStreamStorage storage = new DefaultStreamStorage(
                config,
                metadata,
                writer,
                new DefaultWalObjectReader(objects),
                physicalReferences,
                new MetadataAppendRecoverySearcher(
                        config.cluster(), metadata),
                new Phase4StorageProfileResolver(),
                request -> {
                    admitted.set(request);
                    return admission;
                },
                CLOCK,
                Runnable::run,
                ReadMetricsObserver.noop(),
                TrimMetricsObserver.noop());
        try {
            var stream = storage.createOrGetStream(
                            new StreamName(
                                    "async-admission-object-wal"),
                            new StreamCreateOptions(
                                    StorageProfile
                                            .OBJECT_WAL_ASYNC_OBJECT,
                                    Map.of()))
                    .join();

            CompletableFuture<?> append = storage.append(
                    stream.streamId(),
                    batch("admitted-payload"),
                    new AppendOptions(
                            Optional.empty(),
                            DurabilityLevel
                                    .WAL_DURABLE_AND_INDEX_COMMITTED,
                            Duration.ofSeconds(5),
                            true,
                            Map.of()));

            assertThat(admitted).doesNotHaveValue(null);
            assertThat(admitted.get().streamId())
                    .isEqualTo(stream.streamId());
            assertThat(admitted.get().storageProfile())
                    .isEqualTo(StorageProfile
                            .OBJECT_WAL_ASYNC_OBJECT);
            assertThat(prepares).hasValue(0);
            assertThat(append).isNotDone();
            assertThat(metadata.getCommittedEndOffset(
                            config.cluster(), stream.streamId())
                    .join()
                    .committedEndOffset())
                    .isZero();

            admission.complete(null);

            assertThat(append.join()).isNotNull();
            assertThat(prepares).hasValue(1);
        } finally {
            storage.close();
            metadata.close();
            objects.close();
        }
    }

    private static OxiaMetadataStore delayMaterialization(
            OxiaMetadataStore delegate,
            CompletableFuture<MaterializedGenerationZero> delayed,
            AtomicReference<ReachableCommittedAppend> reachable) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals(
                            "materializeGenerationZero")) {
                        reachable.set(
                                (ReachableCommittedAppend) arguments[1]);
                        return delayed;
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static AppendBatch batch(String payload) {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(new AppendEntry(bytes, 1, 10_000, Map.of())),
                1,
                1,
                10_000,
                10_000,
                List.of(),
                Map.of(),
                Optional.empty());
    }
}
