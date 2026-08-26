/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Campaign;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Plan;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** Closed selector over a validator-complete V2 campaign. */
public final class AllocatorCampaignSelectorV2 {
    private AllocatorCampaignSelectorV2() {}

    public static AllocatorCampaignEvaluationV2 evaluate(Campaign campaign) {
        Plan plan = AllocatorCampaignValidatorV2.validate(campaign);
        if (!plan.completed()) {
            throw new IllegalArgumentException("allocator V2 campaign is incomplete and cannot be evaluated");
        }
        List<Candidate> qualified = plan.qualifiedCandidates();
        boolean strict = qualified.contains(Candidate.STRICT);
        Optional<Candidate> range =
                qualified.stream().filter(Candidate::range).min(Comparator.comparingLong(Candidate::rangeSize));
        AllocatorCampaignEvaluationV2.Status status;
        Optional<Candidate> selected;
        if (strict && range.isEmpty()) {
            status = AllocatorCampaignEvaluationV2.Status.STRICT_SELECTED;
            selected = Optional.of(Candidate.STRICT);
        } else if (!strict && range.isPresent()) {
            status = AllocatorCampaignEvaluationV2.Status.RANGE_SELECTED;
            selected = range;
        } else if (!strict) {
            status = AllocatorCampaignEvaluationV2.Status.NONE_QUALIFIED;
            selected = Optional.empty();
        } else {
            status = AllocatorCampaignEvaluationV2.Status.BOTH_QUALIFIED;
            selected = Optional.empty();
        }
        return new AllocatorCampaignEvaluationV2(
                status,
                qualified,
                selected,
                plan.executedPerformanceCells(),
                plan.dispositions().size());
    }
}
