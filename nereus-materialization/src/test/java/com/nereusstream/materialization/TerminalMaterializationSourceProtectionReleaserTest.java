/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TerminalMaterializationSourceProtectionReleaserTest {
    @Test
    void releasesEveryExactSourceBeforeTheTerminalTaskLosesItsRemovalAuthority() {
        MaterializationWorkerTestHarness.Scenario scenario =
                MaterializationWorkerTestHarness.scenario(value -> value);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            VersionedMaterializationTask planned = scenario.tasks()
                    .get(scenario.task().streamId(), scenario.task().taskId())
                    .join()
                    .orElseThrow();
            VersionedMaterializationTask claimed = scenario.tasks()
                    .claim(planned, "c".repeat(26), "p".repeat(26), 2_000)
                    .join();
            FakeSourceProtectionAdapter adapter =
                    new FakeSourceProtectionAdapter(scenario.task(), claimed);
            VersionedMaterializationTask cancelled = scenario.tasks()
                    .failClaim(
                            claimed,
                            TaskLifecycle.CANCELLED,
                            TaskFailureClass.SOURCE_RETIRED,
                            "exact source retired",
                            0)
                    .join();
            AtomicInteger guardCalls = new AtomicInteger();
            DefaultTerminalMaterializationSourceProtectionReleaser releaser =
                    new DefaultTerminalMaterializationSourceProtectionReleaser(
                            CLUSTER,
                            scenario.tasks(),
                            new MaterializationSourceProtectionRegistry(java.util.List.of(adapter)),
                            Duration.ofSeconds(10),
                            scheduler);

            assertThat(releaser.release(
                            cancelled,
                            () -> {
                                guardCalls.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            })
                    .join())
                    .isEqualTo(2);
            assertThat(adapter.protections).isEmpty();
            assertThat(adapter.released).hasValue(2);
            assertThat(guardCalls).hasValue(5);

            assertThat(releaser.release(
                            cancelled, MaterializationTaskMutationGuard.noOp())
                    .join())
                    .isZero();
        } finally {
            scheduler.shutdownNow();
            scenario.generations().close();
        }
    }

    private static final class FakeSourceProtectionAdapter
            implements MaterializationSourceProtectionAdapter<ObjectSliceReadTarget> {
        private final Map<String, MaterializationSourceProtection> protections =
                new LinkedHashMap<>();
        private final AtomicInteger released = new AtomicInteger();

        private FakeSourceProtectionAdapter(
                MaterializationTask task,
                VersionedMaterializationTask ownerTask) {
            ObjectProtectionOwner owner =
                    MaterializationProtectionIdentities.taskOwner(ownerTask);
            for (SourceGeneration source : task.sources()) {
                String referenceId = MaterializationProtectionIdentities.sourceReferenceId(
                        CLUSTER, task, source);
                protections.put(
                        referenceId,
                        new MaterializationSourceProtection(
                                ReadTargetType.OBJECT_SLICE,
                                referenceId,
                                owner,
                                ownerTask.metadataVersion(),
                                source));
            }
        }

        @Override
        public ReadTargetType targetType() {
            return ReadTargetType.OBJECT_SLICE;
        }

        @Override
        public Class<ObjectSliceReadTarget> targetClass() {
            return ObjectSliceReadTarget.class;
        }

        @Override
        public CompletableFuture<MaterializationSourceProtection> acquireOrTransfer(
                StreamId streamId,
                SourceGeneration source,
                String referenceId,
                ObjectProtectionOwner owner,
                OwnerRevalidator ownerRevalidator) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Optional<MaterializationSourceProtection>> findExisting(
                StreamId streamId,
                SourceGeneration source,
                String referenceId) {
            return CompletableFuture.completedFuture(
                    Optional.ofNullable(protections.get(referenceId)));
        }

        @Override
        public CompletableFuture<MaterializationSourceProtection> revalidate(
                MaterializationSourceProtection protection,
                OwnerRevalidator ownerRevalidator) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<MaterializationSourceProtection> transfer(
                MaterializationSourceProtection protection,
                ObjectProtectionOwner newOwner,
                OwnerRevalidator newOwnerRevalidator) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> release(
                MaterializationSourceProtection protection,
                RemovalAuthorizer removalAuthorizer) {
            MaterializationSourceProtection current =
                    protections.get(protection.referenceId());
            if (!protection.equals(current)) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("stale source protection"));
            }
            return removalAuthorizer.authorize(current).thenRun(() -> {
                protections.remove(current.referenceId(), current);
                released.incrementAndGet();
            });
        }
    }
}
