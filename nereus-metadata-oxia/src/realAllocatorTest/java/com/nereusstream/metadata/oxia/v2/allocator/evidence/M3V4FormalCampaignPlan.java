/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPlanProfileV4;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.RequiredAction;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalCampaignPlan.PlannedActionV3;
import java.util.List;

/** ADR-0125 projection of the unchanged logical action inventory onto the source-bound V4 execution profile. */
final class M3V4FormalCampaignPlan {
    static final int MAXIMUM_TOTAL_EXECUTED_ACTIONS = 720;
    static final long CAMPAIGN_WALL_CLOCK_CAP_SECONDS = 48_000;

    private M3V4FormalCampaignPlan() {}

    static List<PlannedActionV3> zeroDecisionActions() {
        List<PlannedActionV3> actions = M3V3FormalCampaignPlan.zeroDecisionActions();
        if (!actions.stream().map(PlannedActionV3::identity).toList()
                .equals(AllocatorCampaignPlanProfileV4.zeroDecisionActionIdentities())) {
            throw new IllegalStateException("allocator V4 physical plan differs from its domain profile");
        }
        return actions;
    }

    static List<PlannedActionV3> actionsFor(RequiredAction required, List<Observation> prefix) {
        return M3V3FormalCampaignPlan.actionsFor(required, prefix);
    }

    static Sha256Digest zeroDecisionPlanDigest() {
        zeroDecisionActions();
        return AllocatorCampaignPlanProfileV4.zeroDecisionPlanDigest();
    }

    static int completedPhysicalActions(List<Observation> observations) {
        return M3V3FormalCampaignPlan.completedPhysicalActions(observations);
    }
}
