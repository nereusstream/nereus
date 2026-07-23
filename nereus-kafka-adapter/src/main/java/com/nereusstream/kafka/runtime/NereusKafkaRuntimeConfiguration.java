/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.partition.KafkaStorageProfilePolicy;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/** Immutable product-side inputs required to assemble one native Kafka storage runtime. */
public record NereusKafkaRuntimeConfiguration(
        String nereusCluster,
        String kafkaClusterId,
        String writerId,
        Duration appendSessionTtl,
        Duration appendSessionRenewalInterval,
        String operationOwnerId,
        long operationOwnerEpoch,
        Duration operationTtl,
        int recoveryChunkRecords,
        int recoveryChunkBytes,
        Set<StorageProfile> executableProfiles) {
    public NereusKafkaRuntimeConfiguration {
        nereusCluster = nonblank(nereusCluster, "nereusCluster");
        kafkaClusterId = nonblank(kafkaClusterId, "kafkaClusterId");
        writerId = nonblank(writerId, "writerId");
        appendSessionTtl = positive(appendSessionTtl, "appendSessionTtl");
        appendSessionRenewalInterval = positive(
                appendSessionRenewalInterval, "appendSessionRenewalInterval");
        operationOwnerId = nonblank(operationOwnerId, "operationOwnerId");
        operationTtl = positive(operationTtl, "operationTtl");
        if (recoveryChunkRecords <= 0 || recoveryChunkBytes <= 0) {
            throw new IllegalArgumentException("recovery chunk limits must be positive");
        }
        executableProfiles = Set.copyOf(Objects.requireNonNull(executableProfiles, "executableProfiles"));
        if (appendSessionRenewalInterval.compareTo(appendSessionTtl) >= 0) {
            throw new IllegalArgumentException(
                    "appendSessionRenewalInterval must be shorter than appendSessionTtl");
        }
        if (operationOwnerEpoch <= 0) {
            throw new IllegalArgumentException("operationOwnerEpoch must be positive");
        }
        if (executableProfiles.isEmpty()
                || !KafkaStorageProfilePolicy.activatedProfiles().containsAll(executableProfiles)) {
            throw new IllegalArgumentException(
                    "executableProfiles must be a non-empty subset of activated Kafka storage profiles");
        }
    }

    private static String nonblank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be nonblank");
        }
        return value;
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }
}
