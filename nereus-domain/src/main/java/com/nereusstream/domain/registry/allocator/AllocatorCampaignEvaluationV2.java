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

import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Candidate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Validator-created ADR-0104 evaluation; non-selection outcomes remain valid and explicitly non-promotable. */
public final class AllocatorCampaignEvaluationV2 {
    public enum Status {
        STRICT_SELECTED,
        RANGE_SELECTED,
        NONE_QUALIFIED,
        BOTH_QUALIFIED
    }

    private final Status status;
    private final List<Candidate> qualifiedCandidates;
    private final Optional<Candidate> selectedCandidate;
    private final int executedPerformanceCells;
    private final int dispositionCells;

    AllocatorCampaignEvaluationV2(
            Status status,
            List<Candidate> qualifiedCandidates,
            Optional<Candidate> selectedCandidate,
            int executedPerformanceCells,
            int dispositionCells) {
        this.status = Objects.requireNonNull(status, "status");
        this.qualifiedCandidates = List.copyOf(qualifiedCandidates);
        this.selectedCandidate = Objects.requireNonNull(selectedCandidate, "selectedCandidate");
        this.executedPerformanceCells = executedPerformanceCells;
        this.dispositionCells = dispositionCells;
        if (executedPerformanceCells + dispositionCells != AllocatorCampaignV2.LOGICAL_PERFORMANCE_CELLS
                || selectionEligible() != selectedCandidate.isPresent()) {
            throw new IllegalArgumentException("allocator V2 evaluation accounting or selection differs");
        }
    }

    public boolean selectionEligible() {
        return status == Status.STRICT_SELECTED || status == Status.RANGE_SELECTED;
    }

    public Status status() {
        return status;
    }

    public List<Candidate> qualifiedCandidates() {
        return qualifiedCandidates;
    }

    public Optional<Candidate> selectedCandidate() {
        return selectedCandidate;
    }

    public int executedPerformanceCells() {
        return executedPerformanceCells;
    }

    public int dispositionCells() {
        return dispositionCells;
    }
}
