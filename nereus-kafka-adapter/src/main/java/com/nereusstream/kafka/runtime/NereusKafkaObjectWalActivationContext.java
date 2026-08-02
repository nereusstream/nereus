/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.runtime;

import com.nereusstream.kafka.activation.KafkaBrokerCapabilitySpecification;
import com.nereusstream.kafka.activation.KafkaStorageClusterSnapshotProvider;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Kafka-fork activation facts required by the production Object-WAL runtime path.
 */
public record NereusKafkaObjectWalActivationContext(
        KafkaBrokerCapabilitySpecification capability,
        KafkaStorageClusterSnapshotProvider clusterSnapshots,
        Duration activationWaitTimeout,
        Duration activationPollInterval,
        Optional<NereusKafkaCompactionContext> compaction,
        Optional<NereusKafkaMaintenanceContext> maintenance) {
    public NereusKafkaObjectWalActivationContext(
            KafkaBrokerCapabilitySpecification capability,
            KafkaStorageClusterSnapshotProvider clusterSnapshots,
            Duration activationWaitTimeout,
            Duration activationPollInterval) {
        this(
                capability,
                clusterSnapshots,
                activationWaitTimeout,
                activationPollInterval,
                Optional.empty(),
                Optional.empty());
    }

    public NereusKafkaObjectWalActivationContext(
            KafkaBrokerCapabilitySpecification capability,
            KafkaStorageClusterSnapshotProvider clusterSnapshots,
            Duration activationWaitTimeout,
            Duration activationPollInterval,
            Optional<NereusKafkaCompactionContext> compaction) {
        this(capability, clusterSnapshots, activationWaitTimeout, activationPollInterval, compaction, Optional.empty());
    }

    public NereusKafkaObjectWalActivationContext {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(clusterSnapshots, "clusterSnapshots");
        activationWaitTimeout = positive(activationWaitTimeout, "activationWaitTimeout");
        activationPollInterval = positive(activationPollInterval, "activationPollInterval");
        compaction = Objects.requireNonNull(compaction, "compaction");
        maintenance = Objects.requireNonNull(maintenance, "maintenance");
        if (activationPollInterval.compareTo(activationWaitTimeout) > 0) {
            throw new IllegalArgumentException("activationPollInterval cannot exceed activationWaitTimeout");
        }
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }
}
