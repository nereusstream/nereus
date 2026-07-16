/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import java.util.List;
import java.util.Objects;

/** Exact DRAINING wrappers produced or revalidated by one pre-drain attempt. */
public record HigherGenerationPreDrainResult(
        HigherGenerationPreDrainStatus status,
        List<VersionedGenerationIndex> drainingIndexes,
        int transitionedCount,
        int alreadyDrainingCount) {
    public HigherGenerationPreDrainResult {
        Objects.requireNonNull(status, "status");
        drainingIndexes = List.copyOf(Objects.requireNonNull(
                drainingIndexes, "drainingIndexes"));
        if (transitionedCount < 0
                || alreadyDrainingCount < 0
                || Math.addExact(transitionedCount, alreadyDrainingCount)
                        != drainingIndexes.size()) {
            throw new IllegalArgumentException(
                    "higher-generation pre-drain counts do not match wrappers");
        }
        boolean ready = status == HigherGenerationPreDrainStatus.DRAINING_READY;
        if (ready != !drainingIndexes.isEmpty()) {
            throw new IllegalArgumentException(
                    "higher-generation pre-drain status does not match wrappers");
        }
    }

    static HigherGenerationPreDrainResult disabled() {
        return empty(HigherGenerationPreDrainStatus.MUTATION_DISABLED);
    }

    static HigherGenerationPreDrainResult noMatchingIndex() {
        return empty(HigherGenerationPreDrainStatus.NO_MATCHING_INDEX);
    }

    static HigherGenerationPreDrainResult notEligibleYet() {
        return empty(HigherGenerationPreDrainStatus.NOT_ELIGIBLE_YET);
    }

    private static HigherGenerationPreDrainResult empty(
            HigherGenerationPreDrainStatus status) {
        return new HigherGenerationPreDrainResult(status, List.of(), 0, 0);
    }
}
