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

import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.profile.AppendAckBoundary;
import com.nereusstream.core.profile.ObjectPublicationMode;
import com.nereusstream.core.profile.StorageExecutionPlan;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import com.nereusstream.metadata.oxia.StableAppendResult;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AsyncObjectWalAppendCoordinatorTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test
    void walDurableReturnsBeforeDetachedGenerationZeroWorkStarts() {
        StableAppendResult stable = stable();
        MaterializedGenerationZero materialized =
                materialized(stable.reachableAppend().committedAppend());
        AtomicInteger materializeCalls = new AtomicInteger();
        ControlledPublisher publisher = new ControlledPublisher();
        QueuedExecutor executor = new QueuedExecutor();
        AsyncObjectWalAppendCoordinator coordinator =
                new AsyncObjectWalAppendCoordinator(
                        append -> {
                            materializeCalls.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    materialized);
                        },
                        publisher,
                        TIMEOUT,
                        executor);

        CompletableFuture<CommittedAppend> acknowledged =
                coordinator.completeAfterStableCommit(
                        stable,
                        DurabilityLevel.WAL_DURABLE,
                        Duration.ofMillis(1));

        assertThat(acknowledged).isCompletedWithValue(
                stable.reachableAppend().committedAppend());
        assertThat(materializeCalls).hasValue(0);
        assertThat(publisher.visibleCalls).hasValue(0);
        assertThat(executor.queued()).isOne();

        executor.runNext();

        assertThat(materializeCalls).hasValue(1);
        assertThat(publisher.visibleCalls).hasValue(1);
        assertThat(coordinator.inFlightBackgroundRepairCount()).isOne();
        publisher.visible.complete(protectedGeneration(materialized));
        assertThat(coordinator.inFlightBackgroundRepairCount()).isZero();
        assertThat(coordinator.backgroundRepairFailureCount()).isZero();
    }

    @Test
    void strictDurabilityWaitsForExactVisibleGenerationProtection() {
        StableAppendResult stable = stable();
        MaterializedGenerationZero materialized =
                materialized(stable.reachableAppend().committedAppend());
        ControlledPublisher publisher = new ControlledPublisher();
        AsyncObjectWalAppendCoordinator coordinator =
                new AsyncObjectWalAppendCoordinator(
                        append -> CompletableFuture.completedFuture(
                                materialized),
                        publisher,
                        TIMEOUT,
                        Runnable::run);

        CompletableFuture<CommittedAppend> strict =
                coordinator.completeAfterStableCommit(
                        stable,
                        DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                        TIMEOUT);

        assertThat(strict).isNotDone();
        assertThat(publisher.visibleCalls).hasValue(1);
        publisher.visible.complete(protectedGeneration(materialized));
        assertThat(strict).isCompletedWithValue(
                stable.reachableAppend().committedAppend());
    }

    @Test
    void requiredObjectPolicyWaitsForGenerationZeroThenExactHigherGenerationProof() {
        StableAppendResult stable = stable();
        MaterializedGenerationZero materialized =
                materialized(stable.reachableAppend().committedAppend());
        ControlledPublisher publisher = new ControlledPublisher();
        AtomicReference<RequiredObjectGenerationRequest> observed = new AtomicReference<>();
        CompletableFuture<RequiredObjectGenerationProof> required = new CompletableFuture<>();
        AsyncObjectWalAppendCoordinator coordinator = new AsyncObjectWalAppendCoordinator(
                append -> CompletableFuture.completedFuture(materialized),
                publisher,
                TIMEOUT,
                Runnable::run,
                (request, timeout) -> {
                    observed.set(request);
                    return required;
                });
        StorageExecutionPlan plan = new StorageExecutionPlan(
                StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT,
                ReadTargetType.BOOKKEEPER_ENTRY_RANGE,
                ObjectPublicationMode.SYNCHRONOUS,
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                AppendAckBoundary.REQUIRED_OBJECT_GENERATION);

        CompletableFuture<CommittedAppend> acknowledged =
                coordinator.completeAfterStableCommit(stable, plan, TIMEOUT);

        assertThat(acknowledged).isNotDone();
        assertThat(observed).hasValue(null);
        publisher.visible.complete(protectedGeneration(materialized));
        RequiredObjectGenerationRequest request = new RequiredObjectGenerationRequest(
                stable.reachableAppend().committedAppend().streamId(),
                stable.reachableAppend().committedAppend().range(),
                stable.reachableAppend().committedAppend().commitVersion());
        assertThat(observed).hasValue(request);
        assertThat(acknowledged).isNotDone();

        required.complete(new RequiredObjectGenerationProof(
                request,
                "task-1",
                1,
                "/generation/1",
                3,
                sha(),
                sha()));
        assertThat(acknowledged).isCompletedWithValue(
                stable.reachableAppend().committedAppend());
    }

    static StableAppendResult stable() {
        CommittedAppend committed = committed();
        return new StableAppendResult(
                ReachableCommittedAppend.verified(
                        committed,
                        committed.commitId(),
                        committed.range().endOffset(),
                        committed.cumulativeSize(),
                        committed.commitVersion()),
                true);
    }

    static CommittedAppend committed() {
        ObjectKey key = new ObjectKey("wal/async-object");
        ObjectId id = new ObjectId("async-object");
        Checksum sha = sha();
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                id,
                key,
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "slice-0",
                0,
                128,
                sha,
                new EntryIndexRef(
                        EntryIndexLocation.INLINE,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new byte[] {1}),
                        0,
                        0,
                        sha));
        return new CommittedAppend(
                new StreamId("async-object-stream"),
                "commit-1",
                "",
                target,
                new OffsetRange(0, 1),
                0,
                10,
                1,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                10,
                List.of(),
                Optional.empty(),
                1,
                1);
    }

    static MaterializedGenerationZero materialized(
            CommittedAppend committed) {
        return new MaterializedGenerationZero(
                committed,
                "/index/async-object-stream/1/0",
                7,
                sha());
    }

    static ProtectedGenerationZero protectedGeneration(
            MaterializedGenerationZero materialized) {
        ObjectKey key = ((ObjectSliceReadTarget)
                        materialized.committedAppend().readTarget())
                .objectKey();
        ObjectProtectionIdentity identity = new ObjectProtectionIdentity(
                ObjectKeyHash.from(key),
                ObjectProtectionType.VISIBLE_GENERATION,
                GenerationZeroProtectionIdentities
                        .visibleGenerationReferenceId(materialized));
        return new ProtectedGenerationZero(
                materialized,
                identity,
                3,
                1,
                4,
                sha());
    }

    static Checksum sha() {
        return new Checksum(ChecksumType.SHA256, "a".repeat(64));
    }

    static final class ControlledPublisher
            implements GenerationZeroPhysicalReferencePublisher {
        final AtomicInteger visibleCalls = new AtomicInteger();
        final CompletableFuture<ProtectedGenerationZero> visible =
                new CompletableFuture<>();

        @Override
        public CompletableFuture<Void> authorizeUpload(
                AppendSession session,
                PhysicalObjectIdentity object,
                Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ProtectedStableAppend> protectBeforeHead(
                PreparedStableAppend append, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
                MaterializedGenerationZero append, Duration timeout) {
            visibleCalls.incrementAndGet();
            return visible;
        }
    }

    static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        int queued() {
            return tasks.size();
        }

        void runNext() {
            tasks.remove().run();
        }
    }
}
