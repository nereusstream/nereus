/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.metadata.oxia.VersionedKafkaBrokerCapability;
import com.nereusstream.metadata.oxia.VersionedKafkaStorageProtocolActivation;
import com.nereusstream.metadata.oxia.VersionedKafkaStorageReadiness;
import java.util.List;
import java.util.Objects;

/** Exact authority bundle admitted against one KRaft snapshot. */
public record VerifiedKafkaStorageActivation(
        KafkaStorageClusterSnapshot clusterSnapshot,
        VersionedKafkaStorageProtocolActivation activation,
        VersionedKafkaStorageReadiness readiness,
        List<VersionedKafkaBrokerCapability> capabilities) {
    public VerifiedKafkaStorageActivation {
        Objects.requireNonNull(clusterSnapshot, "clusterSnapshot");
        Objects.requireNonNull(activation, "activation");
        Objects.requireNonNull(readiness, "readiness");
        capabilities = List.copyOf(Objects.requireNonNull(capabilities, "capabilities"));
        if (capabilities.size() != clusterSnapshot.brokers().size()) {
            throw new IllegalArgumentException("verified capabilities must cover the exact KRaft broker set");
        }
    }
}
