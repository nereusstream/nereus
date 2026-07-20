/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.NereusManagedLedgerFactoryConfig;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.retention.NereusRetentionConfig;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.metadata.oxia.CursorMetadataStoreConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/** Fully typed configuration mapped by the broker before any runtime resource is created. */
public record NereusRuntimeConfiguration(
        OxiaClientConfiguration oxia,
        ObjectStoreConfiguration objectStore,
        StreamStorageConfig streamStorage,
        NereusManagedLedgerFactoryConfig managedLedger,
        ProjectionMetadataStoreConfig projectionMetadata,
        CursorMetadataStoreConfig cursorMetadata,
        CursorStorageConfig cursorStorage,
        MaterializationConfig materialization,
        NereusRetentionConfig retention,
        PhysicalGcConfig physicalGc,
        Optional<NereusBookKeeperRuntimeConfiguration> bookKeeper) {
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
                defaultMaterialization(streamStorage),
                NereusRetentionConfig.defaults(),
                PhysicalGcConfig.defaults(),
                Optional.empty());
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
                defaultMaterialization(streamStorage),
                NereusRetentionConfig.defaults(),
                PhysicalGcConfig.defaults(),
                Optional.empty());
    }

    public NereusRuntimeConfiguration(
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            StreamStorageConfig streamStorage,
            NereusManagedLedgerFactoryConfig managedLedger,
            ProjectionMetadataStoreConfig projectionMetadata,
            CursorMetadataStoreConfig cursorMetadata,
            CursorStorageConfig cursorStorage,
            MaterializationConfig materialization) {
        this(
                oxia,
                objectStore,
                streamStorage,
                managedLedger,
                projectionMetadata,
                cursorMetadata,
                cursorStorage,
                materialization,
                NereusRetentionConfig.defaults(),
                PhysicalGcConfig.defaults(),
                Optional.empty());
    }

    /** Compatibility constructor retained while broker-side physical-GC config mapping remains default-off. */
    public NereusRuntimeConfiguration(
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            StreamStorageConfig streamStorage,
            NereusManagedLedgerFactoryConfig managedLedger,
            ProjectionMetadataStoreConfig projectionMetadata,
            CursorMetadataStoreConfig cursorMetadata,
            CursorStorageConfig cursorStorage,
            MaterializationConfig materialization,
            NereusRetentionConfig retention) {
        this(
                oxia,
                objectStore,
                streamStorage,
                managedLedger,
                projectionMetadata,
                cursorMetadata,
                cursorStorage,
                materialization,
                retention,
                PhysicalGcConfig.defaults(),
                Optional.empty());
    }

    /** Compatibility constructor for the Object-WAL-only production mapping. */
    public NereusRuntimeConfiguration(
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            StreamStorageConfig streamStorage,
            NereusManagedLedgerFactoryConfig managedLedger,
            ProjectionMetadataStoreConfig projectionMetadata,
            CursorMetadataStoreConfig cursorMetadata,
            CursorStorageConfig cursorStorage,
            MaterializationConfig materialization,
            NereusRetentionConfig retention,
            PhysicalGcConfig physicalGc) {
        this(
                oxia,
                objectStore,
                streamStorage,
                managedLedger,
                projectionMetadata,
                cursorMetadata,
                cursorStorage,
                materialization,
                retention,
                physicalGc,
                Optional.empty());
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
        Objects.requireNonNull(retention, "retention");
        Objects.requireNonNull(physicalGc, "physicalGc");
        bookKeeper = Objects.requireNonNull(bookKeeper, "bookKeeper");
        if (managedLedger.defaultStorageProfile().usesBookKeeperWal() && bookKeeper.isEmpty()) {
            throw new IllegalArgumentException(
                    "BookKeeper default storage profile requires BookKeeper runtime configuration");
        }
        bookKeeper.ifPresent(configuration -> validateBookKeeper(
                configuration,
                streamStorage,
                managedLedger));
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
        if (retention.operationTimeout().compareTo(retention.closeTimeout()) > 0) {
            throw new IllegalArgumentException(
                    "retention operation timeout must fit retention close timeout");
        }
        if (retention.closeTimeout().compareTo(managedLedger.closeTimeout()) > 0) {
            throw new IllegalArgumentException(
                    "retention close timeout must fit managed-ledger close timeout");
        }
        if (retention.maxQueuedPlans() > managedLedger.maxPendingCallbacks()) {
            throw new IllegalArgumentException(
                    "retention queue exceeds managed-ledger callback capacity");
        }
        physicalGc.validateAgainst(streamStorage, materialization);
        if (!physicalGc.maximumClockSkew().equals(materialization.maximumClockSkew())) {
            throw new IllegalArgumentException(
                    "physical GC and materialization maximumClockSkew must match");
        }
        if (objectStore.requestTimeout().compareTo(physicalGc.operationTimeout()) > 0) {
            throw new IllegalArgumentException(
                    "object-store timeout must fit the physical-GC operation deadline");
        }
        if (physicalGc.operationTimeout().compareTo(physicalGc.closeTimeout()) > 0) {
            throw new IllegalArgumentException(
                    "physical-GC operation timeout must fit its close timeout");
        }
        if (physicalGc.closeTimeout().compareTo(managedLedger.closeTimeout()) > 0) {
            throw new IllegalArgumentException(
                    "physical-GC close timeout must fit managed-ledger close timeout");
        }
        if (cursorStorage.cursorRecordsPerStreamMax()
                > com.nereusstream.managedledger.retention.CursorSnapshotGcScanner.MAX_INVENTORY_VALUES) {
            throw new IllegalArgumentException(
                    "cursor inventory exceeds the physical-GC complete-scan bound");
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

    private static void validateBookKeeper(
            NereusBookKeeperRuntimeConfiguration bookKeeper,
            StreamStorageConfig streamStorage,
            NereusManagedLedgerFactoryConfig managedLedger) {
        var wal = bookKeeper.wal();
        if (!wal.clusterAlias().equals(streamStorage.cluster())) {
            throw new IllegalArgumentException(
                    "BookKeeper clusterAlias must equal the Nereus stream cluster");
        }
        if (wal.operationTimeout().compareTo(streamStorage.appendTimeout()) > 0
                || wal.operationTimeout().compareTo(streamStorage.readTimeout()) > 0
                || wal.operationTimeout().compareTo(managedLedger.closeTimeout()) > 0) {
            throw new IllegalArgumentException(
                    "BookKeeper operation timeout must fit append, read, and close deadlines");
        }
        if (wal.allocationTimeout().compareTo(streamStorage.appendTimeout()) > 0
                || wal.sealTimeout().compareTo(managedLedger.closeTimeout()) > 0
                || wal.deleteTimeout().compareTo(managedLedger.closeTimeout()) > 0) {
            throw new IllegalArgumentException(
                    "BookKeeper allocation/seal/delete timeout exceeds its outer deadline");
        }
        if (wal.maxWritesInFlight() > streamStorage.maxInFlightAppends()) {
            throw new IllegalArgumentException(
                    "BookKeeper write concurrency exceeds core append concurrency");
        }
        if (wal.maxReadsInFlight() > streamStorage.maxConcurrentObjectReads()
                || wal.maxReadBytesInFlight() > streamStorage.maxReadBufferBytes()) {
            throw new IllegalArgumentException(
                    "BookKeeper read limits exceed core read resources");
        }
        bookKeeper.metadataStore();
    }
}
