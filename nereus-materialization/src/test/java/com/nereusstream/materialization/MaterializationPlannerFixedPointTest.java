/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MaterializationPlannerFixedPointTest {
    @Test
    void oneHealthyCurrentPolicyGenerationSuppressesTheGenerationZeroRewritePath() {
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        List<VersionedGenerationCandidate> sources = List.of(
                MaterializationPlannerTestSupport.zero("/index/a-zero-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/b-zero-4", 2, 4, 100, 100, 4),
                MaterializationPlannerTestSupport.higher(
                        "/index/c-current-4",
                        0,
                        4,
                        3,
                        0,
                        200,
                        4,
                        policy.digestSha256(),
                        policy.targetPhysicalFormat()));

        List<MaterializationTask> tasks = MaterializationPlannerTestSupport.planner(
                        sources, List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 10)
                .join();

        assertThat(tasks).isEmpty();
    }

    @Test
    void oldPolicyRewritesOnceAndCurrentPolicyMergeConvergesAfterOnePublishedWholeRange() {
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        Checksum oldPolicy = MaterializationPlannerTestSupport.sha('9');
        VersionedGenerationCandidate oldWhole = MaterializationPlannerTestSupport.higher(
                "/index/a-old-4",
                0,
                4,
                1,
                0,
                200,
                4,
                oldPolicy,
                "OLD_COMPACTED_FORMAT");
        List<MaterializationTask> oldPolicyPlan = MaterializationPlannerTestSupport.planner(
                        List.of(oldWhole), List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 10)
                .join();
        assertThat(oldPolicyPlan).hasSize(1);

        List<VersionedGenerationCandidate> mergeSources = new ArrayList<>();
        mergeSources.add(MaterializationPlannerTestSupport.higher(
                "/index/b-current-2",
                0,
                2,
                2,
                0,
                100,
                2,
                policy.digestSha256(),
                policy.targetPhysicalFormat()));
        mergeSources.add(MaterializationPlannerTestSupport.higher(
                "/index/c-current-4",
                2,
                4,
                3,
                100,
                100,
                4,
                policy.digestSha256(),
                policy.targetPhysicalFormat()));
        List<MaterializationTask> mergePlan = MaterializationPlannerTestSupport.planner(
                        mergeSources, List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 10)
                .join();
        assertThat(mergePlan).hasSize(1);
        assertThat(mergePlan.get(0).sources()).hasSize(2);

        mergeSources.add(MaterializationPlannerTestSupport.higher(
                "/index/d-published-4",
                0,
                4,
                4,
                0,
                200,
                4,
                policy.digestSha256(),
                policy.targetPhysicalFormat()));
        List<MaterializationTask> postPublication = MaterializationPlannerTestSupport.planner(
                        mergeSources, List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 10)
                .join();
        assertThat(postPublication).isEmpty();
    }
}
