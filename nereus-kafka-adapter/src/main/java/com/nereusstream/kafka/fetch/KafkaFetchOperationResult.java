/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.fetch;

import java.util.List;
import java.util.Objects;

/** Final owned response facts delivered once on the callback executor. */
public record KafkaFetchOperationResult(
        List<KafkaFetchPartitionResult> partitions,
        int responseBytes,
        boolean responseBudgetExhausted,
        boolean timedOut,
        int readAttempts) {
    public KafkaFetchOperationResult {
        partitions = List.copyOf(Objects.requireNonNull(partitions, "partitions"));
        if (partitions.isEmpty() || responseBytes < 0 || readAttempts < partitions.size()) {
            throw new IllegalArgumentException("invalid Fetch operation result");
        }
        int exactBytes = 0;
        boolean omitted = false;
        for (KafkaFetchPartitionResult partition : partitions) {
            exactBytes = Math.addExact(exactBytes, partition.includedBytes());
            omitted |= partition.omittedForResponseBudget();
        }
        if (exactBytes != responseBytes || omitted != responseBudgetExhausted) {
            throw new IllegalArgumentException("Fetch result byte-budget facts do not match partition results");
        }
    }
}
