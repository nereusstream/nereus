/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.AppendAdmissionGuard;
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.core.read.Phase4ReadComponents;
import com.nereusstream.core.recovery.AppendRecoverySearcher;
import com.nereusstream.core.trim.TrimMetricsObserver;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import com.nereusstream.materialization.MaterializationSourceProvider;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns the BK-M2 provider adapters while leaving the caller-owned BookKeeper client untouched. */
public final class BookKeeperWalRuntime implements AutoCloseable {
    private final BookKeeperPrimaryWalAppender appender;
    private final BookKeeperPrimaryWalReader reader;
    private final PrimaryWalRegistry registry;
    private final BookKeeperGenerationZeroPhysicalReferencePublisher physicalReferences;
    private final BookKeeperStorageProfileResolver profileResolver = new BookKeeperStorageProfileResolver();
    private final AtomicBoolean closed = new AtomicBoolean();

    public BookKeeperWalRuntime(
            BookKeeperPrimaryWalAppender appender,
            BookKeeperPrimaryWalReader reader,
            BookKeeperPrimaryPhysicalReferenceAdapter physicalReferences) {
        this.appender = Objects.requireNonNull(appender, "appender");
        this.reader = Objects.requireNonNull(reader, "reader");
        if (appender.targetType() != ReadTargetType.BOOKKEEPER_ENTRY_RANGE
                || reader.key().targetType() != ReadTargetType.BOOKKEEPER_ENTRY_RANGE) {
            throw new IllegalArgumentException("BookKeeper runtime adapters must bind the exact BK read target");
        }
        this.registry = new PrimaryWalRegistry(List.of(appender), List.of(reader));
        this.physicalReferences = new BookKeeperGenerationZeroPhysicalReferencePublisher(
                Objects.requireNonNull(physicalReferences, "physicalReferences"));
    }

    public PrimaryWalRegistry primaryWalRegistry() {
        ensureOpen();
        return registry;
    }

    public BookKeeperStorageProfileResolver profileResolver() {
        ensureOpen();
        return profileResolver;
    }

    /** Pairs the owned BK reader with its durable F4 source-protection authority. */
    public MaterializationSourceProvider materializationSourceProvider(
            BookKeeperMaterializationSourceProtectionAdapter sourceProtections) {
        ensureOpen();
        return new MaterializationSourceProvider(
                reader,
                Objects.requireNonNull(sourceProtections, "sourceProtections"));
    }

    public BookKeeperGenerationZeroPhysicalReferencePublisher generationZeroPhysicalReferences() {
        ensureOpen();
        return physicalReferences;
    }

    public DefaultStreamStorage newGenerationZeroStorage(
            StreamStorageConfig configuration,
            OxiaMetadataStore metadata,
            AppendRecoverySearcher recoverySearcher,
            AppendAdmissionGuard appendAdmissionGuard,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        ensureOpen();
        return new DefaultStreamStorage(
                configuration,
                metadata,
                registry,
                physicalReferences,
                recoverySearcher,
                profileResolver,
                appendAdmissionGuard,
                clock,
                callbackExecutor,
                readMetricsObserver,
                trimMetricsObserver);
    }

    /** Composes BK generation zero with the shared higher-Object generation resolver and readers. */
    public DefaultStreamStorage newGenerationAwareStorage(
            StreamStorageConfig configuration,
            OxiaMetadataStore metadata,
            AppendRecoverySearcher recoverySearcher,
            AppendAdmissionGuard appendAdmissionGuard,
            Phase4ReadComponents readComponents,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        ensureOpen();
        return new DefaultStreamStorage(
                configuration,
                metadata,
                registry,
                physicalReferences,
                recoverySearcher,
                profileResolver,
                appendAdmissionGuard,
                readComponents,
                clock,
                callbackExecutor,
                readMetricsObserver,
                trimMetricsObserver);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        reader.close();
        appender.close();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("BookKeeper WAL runtime is closed");
        }
    }
}
