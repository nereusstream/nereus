/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.NereusManagedLedgerFactoryConfig;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.metadata.oxia.CursorMetadataStoreConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import java.nio.file.Path;
import java.util.Objects;

/** Fully typed configuration mapped by the broker before any runtime resource is created. */
public record NereusRuntimeConfiguration(
        OxiaClientConfiguration oxia,
        ObjectStoreConfiguration objectStore,
        StreamStorageConfig streamStorage,
        NereusManagedLedgerFactoryConfig managedLedger,
        ProjectionMetadataStoreConfig projectionMetadata,
        CursorMetadataStoreConfig cursorMetadata,
        CursorStorageConfig cursorStorage,
        MaterializationConfig materialization) {
    public NereusRuntimeConfiguration(
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            StreamStorageConfig streamStorage,
            NereusManagedLedgerFactoryConfig managedLedger,
            ProjectionMetadataStoreConfig projectionMetadata) {
        this(
                oxia,
                objectStore,
                streamStorage,
                managedLedger,
                projectionMetadata,
                CursorMetadataStoreConfig.defaults(),
                CursorStorageConfig.defaults(),
                defaultMaterialization(streamStorage));
    }

    public NereusRuntimeConfiguration(
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            StreamStorageConfig streamStorage,
            NereusManagedLedgerFactoryConfig managedLedger,
            ProjectionMetadataStoreConfig projectionMetadata,
            CursorMetadataStoreConfig cursorMetadata,
            CursorStorageConfig cursorStorage) {
        this(
                oxia,
                objectStore,
                streamStorage,
                managedLedger,
                projectionMetadata,
                cursorMetadata,
                cursorStorage,
                defaultMaterialization(streamStorage));
    }

    public NereusRuntimeConfiguration {
        Objects.requireNonNull(oxia, "oxia");
        Objects.requireNonNull(objectStore, "objectStore");
        Objects.requireNonNull(streamStorage, "streamStorage");
        Objects.requireNonNull(managedLedger, "managedLedger");
        Objects.requireNonNull(projectionMetadata, "projectionMetadata");
        Objects.requireNonNull(cursorMetadata, "cursorMetadata");
        Objects.requireNonNull(cursorStorage, "cursorStorage");
        Objects.requireNonNull(materialization, "materialization");
        if (oxia.maxCommitChainScan() != streamStorage.maxCommitChainScan()) {
            throw new IllegalArgumentException("Oxia and StreamStorage maxCommitChainScan must match");
        }
        if (projectionMetadata.maxPendingOperations() > oxia.maxPendingOperations()) {
            throw new IllegalArgumentException("projection pending operations exceed the shared Oxia limit");
        }
        if (cursorMetadata.maxPendingOperations() > oxia.maxPendingOperations()) {
            throw new IllegalArgumentException("cursor pending operations exceed the shared Oxia limit");
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
                || projectionMetadata.operationTimeout().compareTo(managedLedger.metadataTimeout()) != 0
                || cursorMetadata.operationTimeout().compareTo(managedLedger.metadataTimeout()) != 0
                || cursorStorage.cursorMetadataOperationTimeout()
                        .compareTo(managedLedger.metadataTimeout()) != 0) {
            throw new IllegalArgumentException("metadata operation deadlines must match");
        }
        if (cursorMetadata.maxValueBytes() != cursorStorage.cursorMetadataValueMaxBytes()) {
            throw new IllegalArgumentException("cursor metadata value limits must match");
        }
        if (cursorStorage.cursorScanPageSize() > cursorMetadata.maxScanPageSize()) {
            throw new IllegalArgumentException("cursor scan page exceeds the metadata-store limit");
        }
        if (cursorStorage.cursorSnapshotOperationTimeout().compareTo(managedLedger.closeTimeout()) > 0) {
            throw new IllegalArgumentException("cursor snapshot timeout must fit managed-ledger close timeout");
        }
        if (materialization.maxConcurrentWorkers()
                > streamStorage.maxConcurrentObjectReads()) {
            throw new IllegalArgumentException(
                    "materialization workers exceed core object-read concurrency");
        }
        long materializationReadBytes;
        try {
            materializationReadBytes = Math.multiplyExact(
                    materialization.sourceReadPageBytes(),
                    materialization.maxConcurrentWorkers());
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(
                    "materialization read-buffer requirement overflows",
                    failure);
        }
        if (materializationReadBytes
                > streamStorage.maxReadBufferBytes()) {
            throw new IllegalArgumentException(
                    "materialization worker reads exceed the core read-buffer budget");
        }
        if (objectStore.requestTimeout()
                        .compareTo(materialization.operationTimeout())
                > 0) {
            throw new IllegalArgumentException(
                    "object-store timeout must fit the materialization operation deadline");
        }
        if (materialization.closeTimeout()
                        .compareTo(managedLedger.closeTimeout())
                > 0) {
            throw new IllegalArgumentException(
                    "materialization close timeout must fit managed-ledger close timeout");
        }
    }

    private static MaterializationConfig defaultMaterialization(
            StreamStorageConfig streamStorage) {
        Objects.requireNonNull(streamStorage, "streamStorage");
        String temporary = System.getProperty("java.io.tmpdir");
        if (temporary == null || temporary.isBlank()) {
            throw new IllegalArgumentException(
                    "java.io.tmpdir is required for the compatibility materialization configuration");
        }
        Path staging = Path.of(
                        temporary,
                        "nereus-materialization",
                        streamStorage.processRunId())
                .toAbsolutePath()
                .normalize();
        return MaterializationConfig.defaults(staging);
    }
}
