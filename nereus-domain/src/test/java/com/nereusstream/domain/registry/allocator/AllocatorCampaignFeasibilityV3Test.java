/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignFeasibilityV3.AdmissionTuple;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignFeasibilityV3.Result;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignFeasibilityV3.Status;
import org.junit.jupiter.api.Test;

class AllocatorCampaignFeasibilityV3Test {
    @Test
    void legacyFourByOneAdmissionIsStructurallyInfeasible() {
        Result result = AllocatorCampaignFeasibilityV3.evaluate(new AdmissionTuple(4, 1, 4, 1));

        assertThat(result.status()).isEqualTo(Status.PLAN_INFEASIBLE);
        assertThat(result.structuralBounds())
                .filteredOn(bound -> bound.latencyMillis() == 25 && bound.offeredRate() == 200)
                .singleElement()
                .satisfies(bound -> {
                    assertThat(bound.optimisticRequestsPerSecond()).isEqualTo(160);
                    assertThat(bound.structurallyFeasible()).isFalse();
                });
    }

    @Test
    void formalFourBySixtyFourAdmissionCoversFrozenRowsAndCompletionCase() {
        Result result = AllocatorCampaignFeasibilityV3.requireFormalFeasible();

        assertThat(result.status()).isEqualTo(Status.PLAN_FEASIBLE);
        assertThat(result.admission()).isEqualTo(new AdmissionTuple(4, 64, 256, 1));
        assertThat(result.structuralBounds())
                .filteredOn(bound -> bound.latencyMillis() <= 25)
                .allMatch(bound -> bound.structurallyFeasible());
        assertThat(result.structuralBounds())
                .filteredOn(bound -> bound.latencyMillis() == 250 && bound.offeredRate() == 1000)
                .singleElement()
                .satisfies(bound -> assertThat(bound.optimisticRequestsPerSecond()).isEqualTo(1024));
        assertThat(result.logicalCells()).isEqualTo(328);
        assertThat(result.minimumExecutedCells()).isEqualTo(13);
        assertThat(result.minimumPromotableExecutedCells()).isEqualTo(17);
        assertThat(result.maximumTotalActions()).isEqualTo(720);
        assertThat(result.phaseBudgets().totalSeconds()).isEqualTo(34_260);
        assertThat(result.hardCapSeconds() - result.phaseBudgets().totalSeconds()).isEqualTo(13_740);
    }

    @Test
    void exactDerivedFloorInventoryIsReported() {
        assertThat(AllocatorCampaignFeasibilityV3.requireFormalFeasible().derivedFloors())
                .containsEntry(1000, 800)
                .containsEntry(750, 600)
                .containsEntry(500, 400)
                .containsEntry(333, 267)
                .containsEntry(250, 200)
                .containsEntry(200, 200);
    }
}
