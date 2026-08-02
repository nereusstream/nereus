/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Complete provider configuration for the Object-WAL and optional BookKeeper-WAL Kafka runtime.
 */
public record NereusKafkaObjectWalRuntimeConfiguration(
        NereusKafkaRuntimeConfiguration runtime,
        StreamStorageConfig streamStorage,
        OxiaClientConfiguration oxia,
        ObjectStoreConfiguration objectStore,
        Duration pendingProtectionDuration,
        Duration maximumClockSkew,
        Duration orphanGrace,
        int callbackThreads,
        MaterializationConfig materialization,
        Optional<NereusKafkaBookKeeperWalRuntimeConfiguration> bookKeeper) {
    private static final Set<StorageProfile> OBJECT_WAL_PROFILES =
            Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT, StorageProfile.OBJECT_WAL_ASYNC_OBJECT);
    private static final Set<StorageProfile> OBJECT_AND_BOOKKEEPER_PROFILES = Set.of(
            StorageProfile.OBJECT_WAL_SYNC_OBJECT,
            StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
            StorageProfile.BOOKKEEPER_WAL_ONLY,
            StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
            StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);

    public NereusKafkaObjectWalRuntimeConfiguration(
            NereusKafkaRuntimeConfiguration runtime,
            StreamStorageConfig streamStorage,
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            Duration pendingProtectionDuration,
            Duration maximumClockSkew,
            Duration orphanGrace,
            int callbackThreads) {
        this(
                runtime,
                streamStorage,
                oxia,
                objectStore,
                pendingProtectionDuration,
                maximumClockSkew,
                orphanGrace,
                callbackThreads,
                legacyMaterialization(runtime),
                Optional.empty());
    }

    /**
     * Source-compatible pre-materialization constructor. New production callers must pass an explicit config.
     */
    public NereusKafkaObjectWalRuntimeConfiguration(
            NereusKafkaRuntimeConfiguration runtime,
            StreamStorageConfig streamStorage,
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            Duration pendingProtectionDuration,
            Duration maximumClockSkew,
            Duration orphanGrace,
            int callbackThreads,
            Optional<NereusKafkaBookKeeperWalRuntimeConfiguration> bookKeeper) {
        this(
                runtime,
                streamStorage,
                oxia,
                objectStore,
                pendingProtectionDuration,
                maximumClockSkew,
                orphanGrace,
                callbackThreads,
                legacyMaterialization(runtime),
                bookKeeper);
    }

    public NereusKafkaObjectWalRuntimeConfiguration(
            NereusKafkaRuntimeConfiguration runtime,
            StreamStorageConfig streamStorage,
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            Duration pendingProtectionDuration,
            Duration maximumClockSkew,
            Duration orphanGrace,
            int callbackThreads,
            MaterializationConfig materialization) {
        this(
                runtime,
                streamStorage,
                oxia,
                objectStore,
                pendingProtectionDuration,
                maximumClockSkew,
                orphanGrace,
                callbackThreads,
                materialization,
                Optional.empty());
    }

    public NereusKafkaObjectWalRuntimeConfiguration {
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(streamStorage, "streamStorage");
        Objects.requireNonNull(oxia, "oxia");
        Objects.requireNonNull(objectStore, "objectStore");
        pendingProtectionDuration = positive(pendingProtectionDuration, "pendingProtectionDuration");
        maximumClockSkew = nonnegative(maximumClockSkew, "maximumClockSkew");
        orphanGrace = positive(orphanGrace, "orphanGrace");
        materialization = Objects.requireNonNull(materialization, "materialization");
        bookKeeper = Objects.requireNonNull(bookKeeper, "bookKeeper");
        if (callbackThreads <= 0 || callbackThreads > 256) {
            throw new IllegalArgumentException("callbackThreads must be in [1,256]");
        }
        Set<StorageProfile> expectedProfiles =
                bookKeeper.isPresent() ? OBJECT_AND_BOOKKEEPER_PROFILES : OBJECT_WAL_PROFILES;
        if (!runtime.executableProfiles().equals(expectedProfiles)) {
            throw new IllegalArgumentException(
                    "runtime executable profiles do not match its installed primary-WAL providers");
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
            throw new IllegalArgumentException("StreamStorage and Oxia commit-chain scan limits must match");
        }
        if (pendingProtectionDuration.compareTo(maximumClockSkew) <= 0) {
            throw new IllegalArgumentException("pendingProtectionDuration must exceed maximumClockSkew");
        }
        Duration safeReadWindow = pendingProtectionDuration.minus(maximumClockSkew);
        if (safeReadWindow.compareTo(runtime.operationTtl()) < 0) {
            throw new IllegalArgumentException(
                    "pendingProtectionDuration minus maximumClockSkew must cover runtime operationTtl");
        }
        if (!materialization
                .committedPolicy()
                .targetPhysicalFormat()
                .equals(com.nereusstream.materialization.MaterializationPolicy.KAFKA_COMMITTED_FORMAT)) {
            throw new IllegalArgumentException(
                    "Kafka runtime materialization must use the closed NCP2 committed policy");
        }
    }

    private static MaterializationConfig legacyMaterialization(NereusKafkaRuntimeConfiguration runtime) {
        Objects.requireNonNull(runtime, "runtime");
        String identity = DeterministicIds.stableHashComponent(runtime.kafkaClusterId() + '\n' + runtime.writerId());
        Path staging = Path.of(System.getProperty("java.io.tmpdir"))
                .toAbsolutePath()
                .normalize()
                .resolve("nereus-kafka-materialization")
                .resolve(identity);
        return MaterializationConfig.kafkaDefaults(staging);
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
