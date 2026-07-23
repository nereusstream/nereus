/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.metadata;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import java.util.Objects;

public record KafkaPartitionBinding(
        KafkaPartitionIdentity identity,
        StreamName streamName,
        StreamId streamId,
        VersionedKafkaPartitionBinding durableRoot) {
    public KafkaPartitionBinding {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(streamName, "streamName");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(durableRoot, "durableRoot");
    }
}
