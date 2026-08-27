/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Validator-created ADR-0108 evaluation with an explicit incomparable-baseline terminal state. */
public final class AllocatorCampaignEvaluationV3 {
    public enum Status {
        STRICT_SELECTED,
        RANGE_SELECTED,
        NONE_QUALIFIED,
        BOTH_QUALIFIED,
        NATIVE_BASELINE_UNAVAILABLE
    }

    private final Status status;
    private final List<Candidate> qualifiedCandidates;
    private final Optional<Candidate> selectedCandidate;
    private final int executedPerformanceCells;
    private final int dispositionCells;

    AllocatorCampaignEvaluationV3(
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
        if (executedPerformanceCells + dispositionCells != AllocatorCampaignV3.LOGICAL_PERFORMANCE_CELLS
                || selectionEligible() != selectedCandidate.isPresent()
                || (status == Status.NATIVE_BASELINE_UNAVAILABLE && !this.qualifiedCandidates.isEmpty())) {
            throw new IllegalArgumentException("allocator V3 evaluation accounting or selection differs");
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
