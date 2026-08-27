/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignFeasibilityV3;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class M3V3FormalCampaignPlanTest {
    private static final String PLAN_DIGEST =
            "019fcac748460c9cb72ac953d4afbb5e71ecb15d7199310ecf616b9f12eb35e9";

    @Test
    void zeroDecisionInventoryAndDigestMatchTheIndependentPlanProjection() {
        var actions = M3V3FormalCampaignPlan.zeroDecisionActions();
        Map<M3V3FormalCampaignPlan.ActionKind, Long> counts = actions.stream()
                .collect(Collectors.groupingBy(
                        M3V3FormalCampaignPlan.PlannedActionV3::kind,
                        () -> new java.util.EnumMap<>(M3V3FormalCampaignPlan.ActionKind.class),
                        Collectors.counting()));

        assertThat(actions).hasSize(720);
        assertThat(actions).extracting(M3V3FormalCampaignPlan.PlannedActionV3::identity)
                .doesNotHaveDuplicates();
        assertThat(counts)
                .containsEntry(M3V3FormalCampaignPlan.ActionKind.NATIVE_INTERVAL, 48L)
                .containsEntry(M3V3FormalCampaignPlan.ActionKind.CANDIDATE_INTERVAL, 280L)
                .containsEntry(M3V3FormalCampaignPlan.ActionKind.FAULT_ACTION, 360L)
                .containsEntry(M3V3FormalCampaignPlan.ActionKind.SCALE_ACTION, 32L);
        assertThat(M3V3FormalCampaignPlan.zeroDecisionPlanDigest().toHex()).isEqualTo(PLAN_DIGEST);
    }

    @Test
    void feasibilityRunsBeforeAnyFormalRuntimeAndRetainsTheHardEnvelope() {
        var feasibility = AllocatorCampaignFeasibilityV3.requireFormalFeasible();

        assertThat(feasibility.maximumExecutedCells()).isEqualTo(328);
        assertThat(feasibility.maximumTotalActions()).isEqualTo(720);
        assertThat(feasibility.phaseBudgets().totalSeconds()).isEqualTo(34_260);
        assertThat(feasibility.hardCapSeconds()).isEqualTo(48_000);
    }
}
