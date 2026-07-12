/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.NereusManagedLedgerFactoryConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import java.util.Objects;

/** Fully typed configuration mapped by the broker before any runtime resource is created. */
public record NereusRuntimeConfiguration(
        OxiaClientConfiguration oxia,
        ObjectStoreConfiguration objectStore,
        StreamStorageConfig streamStorage,
        NereusManagedLedgerFactoryConfig managedLedger,
        ProjectionMetadataStoreConfig projectionMetadata) {
    public NereusRuntimeConfiguration {
        Objects.requireNonNull(oxia, "oxia");
        Objects.requireNonNull(objectStore, "objectStore");
        Objects.requireNonNull(streamStorage, "streamStorage");
        Objects.requireNonNull(managedLedger, "managedLedger");
        Objects.requireNonNull(projectionMetadata, "projectionMetadata");
        if (oxia.maxCommitChainScan() != streamStorage.maxCommitChainScan()) {
            throw new IllegalArgumentException("Oxia and StreamStorage maxCommitChainScan must match");
        }
        if (projectionMetadata.maxPendingOperations() > oxia.maxPendingOperations()) {
            throw new IllegalArgumentException("projection pending operations exceed the shared Oxia limit");
        }
        if (streamStorage.maxCachedStreams() < managedLedger.maxOpenLedgers()) {
            throw new IllegalArgumentException("maxCachedStreams must cover maxOpenLedgers");
        }
        if (streamStorage.maxRetainedAppendAttempts() != managedLedger.maxRetainedAppendAttempts()) {
            throw new IllegalArgumentException("core and facade retained append-attempt limits must match");
        }
        if (streamStorage.maxInFlightAppends() > managedLedger.maxPendingCallbacks()) {
            throw new IllegalArgumentException("maxInFlightAppends exceeds maxPendingCallbacks");
        }
        if (objectStore.requestTimeout().compareTo(streamStorage.appendTimeout()) > 0
                || objectStore.requestTimeout().compareTo(streamStorage.readTimeout()) > 0) {
            throw new IllegalArgumentException("object-store timeout must fit append and read deadlines");
        }
        if (managedLedger.metadataTimeout().compareTo(oxia.requestTimeout()) != 0
                || projectionMetadata.operationTimeout().compareTo(managedLedger.metadataTimeout()) != 0) {
            throw new IllegalArgumentException("metadata operation deadlines must match");
        }
    }
}
