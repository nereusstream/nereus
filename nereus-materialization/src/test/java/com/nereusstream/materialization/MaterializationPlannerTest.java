/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import java.util.List;
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
}
