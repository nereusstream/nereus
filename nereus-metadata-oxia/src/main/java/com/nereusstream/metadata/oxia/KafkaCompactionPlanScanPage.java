/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One bounded, key-ordered page of immutable Kafka compaction plans.
 */
public record KafkaCompactionPlanScanPage(
        List<VersionedKafkaCompactionPlan> plans, Optional<KafkaCompactionPlanScanToken> continuation) {
    public KafkaCompactionPlanScanPage {
        plans = List.copyOf(Objects.requireNonNull(plans, "plans"));
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (plans.isEmpty() && continuation.isPresent()) {
            throw new IllegalArgumentException("empty Kafka compaction plan page cannot carry a continuation");
        }
    }
}
