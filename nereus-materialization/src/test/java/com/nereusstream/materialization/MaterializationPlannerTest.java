/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadView;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MaterializationPlannerTest {
    @Test
    void plansWholeGapFreeGenerationZeroEdgesAndDoesNotClipAStraddlingTrim() {
        List<VersionedGenerationCandidate> sources = List.of(
                MaterializationPlannerTestSupport.zero("/index/z-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/z-4", 2, 4, 100, 100, 4));
        DefaultMaterializationPlanner planner = MaterializationPlannerTestSupport.planner(
                sources, List.of(), 1, 4);

        List<MaterializationTask> tasks = planner.plan(
                STREAM,
                new OffsetRange(1, 4),
                MaterializationPlannerTestSupport.policy(),
                10).join();

        assertThat(tasks).hasSize(1);
        MaterializationTask task = tasks.get(0);
        assertThat(task.coverage()).isEqualTo(new OffsetRange(0, 4));
        assertThat(task.sources()).extracting(source -> source.range())
                .containsExactly(new OffsetRange(0, 2), new OffsetRange(2, 4));
        assertThat(task.sources()).allMatch(source -> source.generation() == 0);
        assertThat(task.sources()).allMatch(source -> source.projectionRef().isPresent());
        assertThat(task.taskSequence()).isEqualTo(4);
    }

    @Test
    void stopsConservativelyAtTheFirstGapInsteadOfClippingOrSkippingIt() {
        List<VersionedGenerationCandidate> sources = List.of(
                MaterializationPlannerTestSupport.zero("/index/z-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/z-6", 4, 6, 200, 100, 6));
        DefaultMaterializationPlanner planner = MaterializationPlannerTestSupport.planner(
                sources, List.of(), 0, 6);

        List<MaterializationTask> tasks = planner.plan(
                STREAM,
                new OffsetRange(0, 6),
                MaterializationPlannerTestSupport.policy(),
                10).join();

        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).coverage()).isEqualTo(new OffsetRange(0, 2));
    }

    @Test
    void bootstrapsTopicCompactionFromCommittedSourcesWithoutCrossingTargetViewIdentity() {
        List<VersionedGenerationCandidate> sources = List.of(
                MaterializationPlannerTestSupport.zero("/index/topic-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/topic-4", 2, 4, 100, 100, 4));
        MaterializationPolicy policy = MaterializationPolicyFactory.topicCompacted(
                new TopicCompactionSpec("latest", 1, "test-key-v1"),
                2,
                16,
                1_000,
                1_000_000,
                128,
                "ZSTD");
        GenerationMetadataStore durable =
                com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory.inMemory(
                        Clock.systemUTC());
        GenerationMetadataStore store = MaterializationPlannerTestSupport.generationStore(
                sources, List.of(), durable);
        DefaultMaterializationPlanner planner = new DefaultMaterializationPlanner(
                MaterializationPlannerTestSupport.CLUSTER,
                MaterializationPlannerTestSupport.l0Store(
                        MaterializationPlannerTestSupport.snapshot(0, 4)),
                store,
                2);

        MaterializationTask task = planner.plan(STREAM, new OffsetRange(0, 4), policy, 1)
                .join()
                .get(0);

        assertThat(task.view()).isEqualTo(ReadView.TOPIC_COMPACTED);
        assertThat(task.sourceView()).isEqualTo(ReadView.COMMITTED);
        assertThat(task.taskKind()).isEqualTo(TaskKind.TOPIC_KEY_COMPACTION);
        assertThat(task.sources()).allMatch(source -> source.view() == ReadView.COMMITTED);
        MaterializationTaskStore taskStore = new MaterializationTaskStore(
                MaterializationPlannerTestSupport.CLUSTER, store, Clock.systemUTC());
        var created = taskStore.create(task).join();
        assertThat(taskStore.requireTask(created)).isEqualTo(task);
        assertThat(task.policy().topicCompaction()).contains(
                new TopicCompactionSpec("latest", 1, "test-key-v1"));
    }

    @Test
    void reachesTopicFixedPointFromTheExactPublishedTargetTaskIdentity() {
        List<VersionedGenerationCandidate> sources = List.of(
                MaterializationPlannerTestSupport.zero("/index/topic-fixed-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/topic-fixed-4", 2, 4, 100, 100, 4));
        MaterializationPolicy policy = MaterializationPolicyFactory.topicCompacted(
                new TopicCompactionSpec("latest", 1, "test-key-v1"),
                2,
                16,
                1_000,
                1_000_000,
                128,
                "ZSTD");
        MaterializationTask planned = MaterializationPlannerTestSupport.planner(
                        sources, List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 1)
                .join()
                .get(0);
        List<VersionedGenerationCandidate> withPublishedTarget = new java.util.ArrayList<>(sources);
        withPublishedTarget.add(MaterializationPlannerTestSupport.publishedTopic(planned, 2));

        List<MaterializationTask> replay = MaterializationPlannerTestSupport.planner(
                        withPublishedTarget, List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 1)
                .join();

        assertThat(replay).isEmpty();
    }
}
