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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AppendCoordinatorLaneLifecycleTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-11T00:00:00Z"), ZoneOffset.UTC);

    @TempDir
    Path temporaryDirectory;

    @Test
    void releasesLaneAfterKnownSuccessfulAppend() {
        TestContext context = context("success");

        context.coordinator().append(context.streamId(), batch(), options()).join();

        assertThat(context.coordinator().retainedLaneCount()).isZero();
    }

    @Test
    void retainsSuspendedLaneAfterKnownCommittedFailure() {
        TestContext context = context("uncertain");
        context.metadata().failNext(
                FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);

        AtomicReference<NereusException> captured = new AtomicReference<>();
        assertThatThrownBy(() -> context.coordinator().append(context.streamId(), batch(), options()).join())
                .isInstanceOfSatisfying(CompletionException.class, error -> {
                    NereusException failure = (NereusException) error.getCause();
                    captured.set(failure);
                    assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_COMMITTED);
                    assertThat(failure.appendAttemptId()).isPresent();
                });
        context.coordinator().recoverAppend(context.streamId(),
                captured.get().appendAttemptId().orElseThrow(),
                new AppendRecoveryOptions(Duration.ofSeconds(1))).join();
        assertThat(context.coordinator().retainedLaneCount()).isZero();
    }

    private TestContext context(String name) {
        StreamStorageConfig config = StreamStorageConfig.defaults("cluster/a", "writer-a");
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        StreamId streamId = new StreamId(metadata.createOrGetStream(
                        config.cluster(),
                        new StreamName("lane-" + name),
                        new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                .join().streamId());
        AppendSessionManager sessions = new AppendSessionManager(config, metadata, CLOCK);
        LocalFileObjectStore objectStore = new LocalFileObjectStore(temporaryDirectory.resolve(name));
        DefaultObjectProtectionManager protections = new DefaultObjectProtectionManager(
                config.cluster(),
                metadata,
                Duration.ofMinutes(10),
                Duration.ZERO,
                Duration.ofHours(24),
                CLOCK);
        AppendCoordinator coordinator = new AppendCoordinator(
                config,
                metadata,
                new DefaultWalObjectWriter(objectStore, "test-writer", CLOCK),
                sessions,
                new DefaultGenerationZeroPhysicalReferencePublisher(
                        config.cluster(), metadata, metadata, protections),
                CLOCK,
                Runnable::run);
        return new TestContext(metadata, coordinator, streamId);
    }

    private static AppendBatch batch() {
        byte[] payload = "value".getBytes(StandardCharsets.UTF_8);
        long eventTime = CLOCK.millis();
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(new AppendEntry(payload, 1, eventTime, Map.of())),
                1,
                1,
                eventTime,
                eventTime,
                List.of(),
                Map.of(),
                Optional.empty());
    }

    private static AppendOptions options() {
        return new AppendOptions(
                Optional.empty(),
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                Duration.ofSeconds(5),
                true,
                Map.of());
    }

    private record TestContext(
            FakeOxiaMetadataStore metadata,
            AppendCoordinator coordinator,
            StreamId streamId) {
    }
}
