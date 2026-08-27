/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.BaselineStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Campaign;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Plan;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** Closed selector over a validator-complete V3 campaign. */
public final class AllocatorCampaignSelectorV3 {
    private AllocatorCampaignSelectorV3() {}

    public static AllocatorCampaignEvaluationV3 evaluate(Campaign campaign) {
        Plan plan = AllocatorCampaignValidatorV3.validate(campaign);
        if (!plan.completed()) {
            throw new IllegalArgumentException("allocator V3 campaign is incomplete and cannot be evaluated");
        }
        if (plan.baselineStatus() == BaselineStatus.UNAVAILABLE) {
            return new AllocatorCampaignEvaluationV3(
                    AllocatorCampaignEvaluationV3.Status.NATIVE_BASELINE_UNAVAILABLE,
                    List.of(),
                    Optional.empty(),
                    plan.executedPerformanceCells(),
                    plan.dispositions().size());
        }
        List<Candidate> qualified = plan.qualifiedCandidates();
        boolean strict = qualified.contains(Candidate.STRICT);
        Optional<Candidate> range =
                qualified.stream().filter(Candidate::range).min(Comparator.comparingLong(Candidate::rangeSize));
        AllocatorCampaignEvaluationV3.Status status;
        Optional<Candidate> selected;
        if (strict && range.isEmpty()) {
            status = AllocatorCampaignEvaluationV3.Status.STRICT_SELECTED;
            selected = Optional.of(Candidate.STRICT);
        } else if (!strict && range.isPresent()) {
            status = AllocatorCampaignEvaluationV3.Status.RANGE_SELECTED;
            selected = range;
        } else if (!strict) {
            status = AllocatorCampaignEvaluationV3.Status.NONE_QUALIFIED;
            selected = Optional.empty();
        } else {
            status = AllocatorCampaignEvaluationV3.Status.BOTH_QUALIFIED;
            selected = Optional.empty();
        }
        return new AllocatorCampaignEvaluationV3(
                status,
                qualified,
                selected,
                plan.executedPerformanceCells(),
                plan.dispositions().size());
    }
}
