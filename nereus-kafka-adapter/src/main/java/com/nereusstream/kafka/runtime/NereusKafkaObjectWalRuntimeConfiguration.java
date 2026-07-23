/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/** Complete provider configuration for the first strict synchronous Object-WAL Kafka runtime. */
public record NereusKafkaObjectWalRuntimeConfiguration(
        NereusKafkaRuntimeConfiguration runtime,
        StreamStorageConfig streamStorage,
        OxiaClientConfiguration oxia,
        ObjectStoreConfiguration objectStore,
        Duration pendingProtectionDuration,
        Duration maximumClockSkew,
        Duration orphanGrace,
        int callbackThreads) {
    private static final Set<StorageProfile> EXECUTABLE_PROFILES =
            Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT);

    public NereusKafkaObjectWalRuntimeConfiguration {
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(streamStorage, "streamStorage");
        Objects.requireNonNull(oxia, "oxia");
        Objects.requireNonNull(objectStore, "objectStore");
        pendingProtectionDuration = positive(
                pendingProtectionDuration, "pendingProtectionDuration");
        maximumClockSkew = nonnegative(maximumClockSkew, "maximumClockSkew");
        orphanGrace = positive(orphanGrace, "orphanGrace");
        if (callbackThreads <= 0 || callbackThreads > 256) {
            throw new IllegalArgumentException("callbackThreads must be in [1,256]");
        }
        if (!runtime.executableProfiles().equals(EXECUTABLE_PROFILES)) {
            throw new IllegalArgumentException(
                    "synchronous Object-WAL runtime must expose exactly OBJECT_WAL_SYNC_OBJECT");
        }
        if (!streamStorage.cluster().equals(runtime.nereusCluster())
                || !streamStorage.writerId().equals(runtime.writerId())
                || !streamStorage.appendSessionTtl().equals(runtime.appendSessionTtl())) {
            throw new IllegalArgumentException(
                    "StreamStorage cluster, writer and session TTL must match the Kafka runtime");
        }
        if (streamStorage.autoAcquireAppendSession()) {
            throw new IllegalArgumentException(
                    "Kafka StreamStorage must disable legacy automatic append-session acquisition");
        }
        if (streamStorage.maxCommitChainScan() != oxia.maxCommitChainScan()) {
            throw new IllegalArgumentException(
                    "StreamStorage and Oxia commit-chain scan limits must match");
        }
        if (pendingProtectionDuration.compareTo(maximumClockSkew) <= 0) {
            throw new IllegalArgumentException(
                    "pendingProtectionDuration must exceed maximumClockSkew");
        }
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private static Duration nonnegative(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }
}
