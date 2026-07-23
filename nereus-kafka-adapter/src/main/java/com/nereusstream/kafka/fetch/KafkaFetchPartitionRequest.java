/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.fetch;

import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionStorage;
import com.nereusstream.kafka.partition.KafkaStorageReadRequest;
import java.util.Objects;

/** One immutable partition read in a Fetch operation. */
public record KafkaFetchPartitionRequest(
        KafkaPartitionStorage storage,
        KafkaStorageReadRequest readRequest) {
    public KafkaFetchPartitionRequest {
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(readRequest, "readRequest");
    }

    public KafkaPartitionIdentity identity() {
        return storage.identity();
    }
}
