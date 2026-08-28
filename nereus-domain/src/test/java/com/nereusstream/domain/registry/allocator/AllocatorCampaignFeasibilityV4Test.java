/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class AllocatorCampaignFeasibilityV4Test {
    @Test
    void rejectsTheV3SingleTerminalBoundary() {
        var result =
                AllocatorCampaignFeasibilityV4.evaluate(new AllocatorCampaignFeasibilityV4.TerminalBoundary(40, 0, 5));

        assertThat(result.status()).isEqualTo(AllocatorCampaignFeasibilityV4.Status.TERMINAL_CENSORING_INFEASIBLE);
    }

    @Test
    void formalBoundaryBindsTailProofAndBudgets() {
        var result = AllocatorCampaignFeasibilityV4.requireFormalFeasible();

        assertThat(result.status()).isEqualTo(AllocatorCampaignFeasibilityV4.Status.PLAN_FEASIBLE);
        assertThat(result.terminalBoundary()).isEqualTo(new AllocatorCampaignFeasibilityV4.TerminalBoundary(40, 2, 5));
        assertThat(result.requiredTailCollisions())
                .containsExactly(
                        new AllocatorCampaignFeasibilityV4.TailCollision(800, 31_960, 9_730, 23_875, 25_000),
                        new AllocatorCampaignFeasibilityV4.TailCollision(1_000, 39_943, 1_269, 9_750, 28_500));
        assertThat(result.phaseBudgets().intervalSeconds()).isEqualTo(13_776);
        assertThat(result.phaseBudgets().totalSeconds()).isEqualTo(34_916);
        assertThat(result.hardCapSeconds() - result.phaseBudgets().totalSeconds())
                .isEqualTo(13_084);
        assertThat(result.scheduleProfileSha256())
                .isEqualTo(AllocatorNativeExecutionProfileV3.scheduleDigest().toHex());
        assertThat(result.executionProfileSha256())
                .isNotEqualTo(AllocatorNativeExecutionProfileV3.executionProfileDigest()
                        .toHex());
    }
}
