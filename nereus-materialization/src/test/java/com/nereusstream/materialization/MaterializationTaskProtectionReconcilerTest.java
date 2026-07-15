/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.GenerationPublicationTestSupport.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.PublicationId;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class MaterializationTaskProtectionReconcilerTest {
    @Test
    void reconstructsEveryProtectionAndConvergesAcrossTaskStateCrashCuts() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            MaterializationTaskStore tasks = new MaterializationTaskStore(
                    CLUSTER,
                    context.generations(),
                    GenerationPublicationTestSupport.CLOCK);
            DefaultMaterializationTaskProtectionReconciler reconciler =
                    new DefaultMaterializationTaskProtectionReconciler(
                            CLUSTER,
                            tasks,
                            context.generations(),
                            (target, view) -> context.physical()
                                    .getRoot(CLUSTER, ObjectKeyHash.from(target.objectKey()))
                                    .thenApply(root -> PhysicalObjectIdentity.from(
                                            root.orElseThrow().value())),
                            context.protections(),
                            Duration.ofSeconds(30),
                            context.scheduler());
            VersionedMaterializationTask ready = tasks.get(
                    context.task().streamId(), context.task().taskId()).join().orElseThrow();

            MaterializationTaskProtections readyProtections = reconciler.reconcile(ready).join();

            assertThat(readyProtections.sources()).hasSize(context.task().sources().size());
            assertThat(readyProtections.output()).isPresent();
            assertThat(readyProtections.sources())
                    .allSatisfy(protection -> assertThat(protection.owner())
                            .isEqualTo(MaterializationProtectionIdentities.taskOwner(ready)));
            assertThat(readyProtections.output().orElseThrow().owner())
                    .isEqualTo(MaterializationProtectionIdentities.taskOwner(ready));

            VersionedMaterializationTask publishing = tasks.compareAndSet(
                    MaterializationRecordMapper.publishing(
                            ready.value(), new PublicationId("p".repeat(26)), 1_100),
                    ready.metadataVersion()).join();
            VersionedMaterializationTask attached = tasks.compareAndSet(
                    MaterializationRecordMapper.attachGeneration(
                            publishing.value(), 7, 1_200),
                    publishing.metadataVersion()).join();

            MaterializationTaskProtections recovered = reconciler.reconcile(attached).join();

            assertThat(recovered.sources()).allSatisfy(protection -> {
                assertThat(protection.owner())
                        .isEqualTo(MaterializationProtectionIdentities.taskOwner(attached));
                assertThat(protection.metadataVersion())
                        .isGreaterThan(readyProtections.sources().get(0).metadataVersion());
            });
            assertThat(recovered.output().orElseThrow().owner())
                    .isEqualTo(MaterializationProtectionIdentities.taskOwner(attached));
            assertThat(recovered.output().orElseThrow().metadataVersion())
                    .isGreaterThan(readyProtections.output().orElseThrow().metadataVersion());

            assertThatThrownBy(() -> reconciler.reconcile(ready).join())
                    .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);

            MaterializationTaskProtections duplicate = reconciler.reconcile(attached).join();
            assertThat(duplicate.sources()).isEqualTo(recovered.sources());
            assertThat(duplicate.output()).isEqualTo(recovered.output());
        }
    }
}
