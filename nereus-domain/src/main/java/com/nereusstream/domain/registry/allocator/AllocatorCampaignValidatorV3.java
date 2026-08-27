/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Campaign;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Plan;
import java.util.Objects;

/** Recomputes the complete ADR-0108 plan and rejects caller-claimed disposition differences. */
public final class AllocatorCampaignValidatorV3 {
    private AllocatorCampaignValidatorV3() {}

    public static Plan validate(Campaign campaign) {
        Objects.requireNonNull(campaign, "campaign");
        Plan recomputed = AllocatorCampaignPlannerV3.plan(campaign.observations());
        if (!campaign.dispositions().equals(recomputed.dispositions())) {
            throw new IllegalArgumentException(
                    "allocator V3 caller dispositions differ from deterministic validator recomputation");
        }
        return recomputed;
    }

    public static void validateIntervalConservation(IntervalEvidence interval) {
        AllocatorCampaignPlannerV3.validateConservation(Objects.requireNonNull(interval, "interval"));
    }
}
