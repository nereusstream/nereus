/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Immutable create/get/exact-delete contract for restart-safe Kafka compaction plans. */
public interface KafkaCompactionPlanMetadataStore {
    CompletableFuture<Optional<VersionedKafkaCompactionPlan>> getCompactionPlan(
            KafkaPartitionId id, String materializationTaskId);

    CompletableFuture<VersionedKafkaCompactionPlan> putCompactionPlanIfAbsent(
            KafkaCompactionPlanRecord value);

    CompletableFuture<Void> deleteCompactionPlan(VersionedKafkaCompactionPlan expected);
}
