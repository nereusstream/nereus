/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignFeasibilityV5.Status;
import org.junit.jupiter.api.Test;

class AllocatorCampaignFeasibilityV5Test {
    @Test
    void v4AdmissionIsStructurallyInfeasibleForTheFrozenTwoXStorm() {
        var result = AllocatorCampaignFeasibilityV5.evaluate(AllocatorCampaignFeasibilityV5.V4_ADMISSION);

        assertThat(result.status()).isEqualTo(Status.STORM_ADMISSION_INFEASIBLE);
        assertThat(result.optimisticRequestsPerSecond()).isEqualTo(1_024);
        assertThat(result.instantaneousStormFeasible()).isFalse();
        assertThat(result.terminalDrainFeasible()).isFalse();
        assertThat(result.offeredDuringStorm()).isEqualTo(20_000);
        assertThat(result.serviceThroughDrain()).isEqualTo(12_288);
    }

    @Test
    void v5AdmissionClosesTheStormBoundWithoutChangingTheScheduleOrBudgets() {
        var result = AllocatorCampaignFeasibilityV5.requireFormalFeasible();

        assertThat(result.status()).isEqualTo(Status.PLAN_FEASIBLE);
        assertThat(result.admission())
                .isEqualTo(new AllocatorCampaignFeasibilityV3.AdmissionTuple(4, 128, 512, 1));
        assertThat(result.optimisticRequestsPerSecond()).isEqualTo(2_048);
        assertThat(result.instantaneousStormFeasible()).isTrue();
        assertThat(result.terminalDrainFeasible()).isTrue();
        assertThat(result.serviceThroughDrain()).isEqualTo(24_576);
        assertThat(AllocatorEvidenceAdmissionPolicyV5.EXACT_STORM_OUTSTANDING_PER_ACTOR).isEqualTo(125);
        assertThat(AllocatorNativeExecutionProfileV5.scheduleDigest())
                .isEqualTo(AllocatorNativeExecutionProfileV4.scheduleDigest());
        assertThat(AllocatorNativeExecutionProfileV5.executionProfileDigest())
                .isNotEqualTo(AllocatorNativeExecutionProfileV4.executionProfileDigest());
        assertThat(AllocatorCampaignPlanProfileV5.zeroDecisionActionIdentities())
                .containsExactlyElementsOf(AllocatorCampaignPlanProfileV4.zeroDecisionActionIdentities());
        assertThat(AllocatorCampaignPlanProfileV5.zeroDecisionPlanDigest())
                .isNotEqualTo(AllocatorCampaignPlanProfileV4.zeroDecisionPlanDigest());
    }
}
