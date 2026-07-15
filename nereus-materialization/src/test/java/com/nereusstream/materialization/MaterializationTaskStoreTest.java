/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class MaterializationTaskStoreTest {
    @Test
    void revalidatesExactSourcesAndConvergesCreatesFromDifferentPlannerClocks() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/z-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/z-4", 2, 4, 100, 100, 4));
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        MaterializationTask task = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 1)
                .join()
                .get(0);
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        GenerationMetadataStore exactSources = MaterializationPlannerTestSupport.generationStore(
                candidates, List.of(), durable);
        MaterializationTaskStore first = new MaterializationTaskStore(
                CLUSTER,
                exactSources,
                Clock.fixed(Instant.ofEpochMilli(2_000), ZoneOffset.UTC));
        MaterializationTaskStore second = new MaterializationTaskStore(
                CLUSTER,
                exactSources,
                Clock.fixed(Instant.ofEpochMilli(9_000), ZoneOffset.UTC));

        VersionedMaterializationTask created = first.create(task).join();
        VersionedMaterializationTask recovered = second.create(task).join();

        assertThat(recovered).isEqualTo(created);
        assertThat(created.value().createdAtMillis()).isEqualTo(2_000);
        assertThat(first.requireTask(created, policy)).isEqualTo(task);
        assertThat(first.get(STREAM, task.taskId()).join()).contains(created);
    }

    @Test
    void refusesToReuseATaskKeyWhenAnyExactSourceDisappeared() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/z-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/z-4", 2, 4, 100, 100, 4));
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        MaterializationTask task = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 1)
                .join()
                .get(0);
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        MaterializationTaskStore changed = new MaterializationTaskStore(
                CLUSTER,
                MaterializationPlannerTestSupport.generationStore(
                        candidates.subList(0, 1), List.of(), durable),
                Clock.fixed(Instant.ofEpochMilli(2_000), ZoneOffset.UTC));

        assertThatThrownBy(() -> changed.create(task).join())
                .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class)
                .rootCause()
                .extracting("code")
                .isEqualTo(com.nereusstream.api.ErrorCode.METADATA_CONDITION_FAILED);
    }
}
