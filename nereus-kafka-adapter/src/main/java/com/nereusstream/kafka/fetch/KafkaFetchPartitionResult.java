/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.fetch;

import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaStorageReadResult;
import java.util.Objects;
import java.util.Optional;

/** Included partition bytes or an explicit omission caused only by the request-wide byte budget. */
public record KafkaFetchPartitionResult(
        KafkaPartitionIdentity identity,
        Optional<KafkaStorageReadResult> readResult,
        boolean omittedForResponseBudget) {
    public KafkaFetchPartitionResult {
        Objects.requireNonNull(identity, "identity");
        readResult = Objects.requireNonNull(readResult, "readResult");
        if (omittedForResponseBudget == readResult.isPresent()) {
            throw new IllegalArgumentException(
                    "a Fetch partition must be included or explicitly omitted for response budget");
        }
    }

    public int includedBytes() {
        return readResult.map(result -> result.fetchAssembly().sizeInBytes()).orElse(0);
    }
}
