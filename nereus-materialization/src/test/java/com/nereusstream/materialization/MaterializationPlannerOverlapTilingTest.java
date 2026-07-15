/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import java.util.List;
import org.junit.jupiter.api.Test;

class MaterializationPlannerOverlapTilingTest {
    @Test
    void selectsOneCanonicalWholeEdgePathAcrossNestedAndCrossingGenerations() {
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        Checksum oldPolicy = MaterializationPlannerTestSupport.sha('8');
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/a-zero-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/b-zero-4", 2, 4, 100, 100, 4),
                MaterializationPlannerTestSupport.zero("/index/c-zero-6", 4, 6, 200, 100, 6),
                MaterializationPlannerTestSupport.higher(
                        "/index/d-wide-4", 0, 4, 3, 0, 200, 4, oldPolicy, "OLD_FORMAT"),
                MaterializationPlannerTestSupport.higher(
                        "/index/e-cross-6", 2, 6, 4, 100, 200, 6, oldPolicy, "OLD_FORMAT"));

        List<MaterializationTask> tasks = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 6)
                .plan(STREAM, new OffsetRange(0, 6), policy, 10)
                .join();

        assertThat(tasks).hasSize(1);
        MaterializationTask selected = tasks.get(0);
        assertThat(selected.coverage()).isEqualTo(new OffsetRange(0, 6));
        assertThat(selected.sources()).extracting(SourceGeneration::indexKey)
                .containsExactly("/index/d-wide-4", "/index/c-zero-6");
        assertThat(selected.sources().get(0).range().endOffset())
                .isEqualTo(selected.sources().get(1).range().startOffset());
    }
}
