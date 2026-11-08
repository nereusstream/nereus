/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1.RetentionInstallation;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** One actual M5 SDK factory owns its guarded BK scope and metadata writer before its first native mutation. */
public final class PulsarNativeManagedLedgerFactoryV2 implements AutoCloseable {
    private record ObjectScope(
            NereusPulsarLedgerOffloaderV1 offloader,
            PhysicalResourceIdV2.Namespace namespace,
            long maximumBytes,
            Optional<Supplier<CompletionStage<Void>>> enrolledOwnership) {}

    private final PulsarNativeBindingV1 binding;
    private final PulsarNativeBookKeeperCreateV1 creation;
    private final ManagedLedgerFactoryImpl factory;
    private final Executor executor;
    private final Optional<ObjectScope> objectScope;
    private PulsarNativeMetadataWriterV2 metadataWriter;
    private RetentionInstallation retentionInstallation;
    private final java.util.Set<ManagedLedgerImpl> ownedLedgers = new java.util.HashSet<>();
    private CompletableFuture<ManagedLedgerImpl> opening;
    private CompletableFuture<Void> closing;
    private CompletableFuture<Void> readOpening;
    private CompletableFuture<Void> readClosing;
    private PulsarNativeM4ControlV1 readControl;
    private PulsarNativeSourcePreparationV1 readPreparation;
    private PulsarNativeM4ReadOwnerV1 selectedReadOwner;
    private boolean factoryClosed;
    private boolean closed;

    private PulsarNativeManagedLedgerFactoryV2(
            PulsarNativeBindingV1 binding,
            PulsarNativeBookKeeperCreateV1 creation,
            ManagedLedgerFactoryImpl factory,
            Executor executor,
            Optional<ObjectScope> objectScope) {
        this.binding = binding;
        this.creation = creation;
        this.factory = factory;
        this.executor = executor;
        this.objectScope = objectScope;
    }

    public static CompletableFuture<PulsarNativeManagedLedgerFactoryV2> openBookKeeperOnly(
            PulsarNativeBindingV1 binding,
            MetadataStoreExtended metadata,
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long existingQuotaCapacity,
            ManagedLedgerFactoryConfig factoryConfig,
            M5ClosedWriterRegistryV1 registry,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            String aggregateKey,
            Executor executor) {
        return openNativeFactory(
                binding,
                metadata,
                metadataServiceUri,
                bootstrap,
                budget,
                existingQuotaCapacity,
                factoryConfig,
                registry,
                aggregate,
                aggregateKey,
                Optional.empty(),
                executor,
                Optional.empty());
    }

    public static CompletableFuture<PulsarNativeManagedLedgerFactoryV2> open(
            PulsarNativeBindingV1 binding,
            MetadataStoreExtended metadata,
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long existingQuotaCapacity,
            ManagedLedgerFactoryConfig factoryConfig,
            M5ClosedWriterRegistryV1 registry,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            String aggregateKey,
            NereusPulsarLedgerOffloaderV1 offloader,
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace objectNamespace,
            long maximumObjectBytes,
            Executor executor) {
        return open(
                binding,
                metadata,
                metadataServiceUri,
                bootstrap,
                budget,
                existingQuotaCapacity,
                factoryConfig,
                registry,
                aggregate,
                aggregateKey,
                offloader,
                objectNamespace,
                maximumObjectBytes,
                executor,
                java.util.Optional.empty());
    }

    /** Explicit policy for newly created Native scopes; historical scopes and ledger tags are never upgraded. */
    public static CompletableFuture<PulsarNativeManagedLedgerFactoryV2> open(
            PulsarNativeBindingV1 binding,
            MetadataStoreExtended metadata,
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long existingQuotaCapacity,
            ManagedLedgerFactoryConfig factoryConfig,
            M5ClosedWriterRegistryV1 registry,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            String aggregateKey,
            NereusPulsarLedgerOffloaderV1 offloader,
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace objectNamespace,
            long maximumObjectBytes,
            Executor executor,
            java.util.Optional<com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1>
                    bookKeeperGcGrace) {
        try {
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        var objectScope = new ObjectScope(
                Objects.requireNonNull(offloader),
                Objects.requireNonNull(objectNamespace),
                maximumObjectBytes,
                Optional.empty());
        return openNativeFactory(
                binding,
                metadata,
                metadataServiceUri,
                bootstrap,
                budget,
                existingQuotaCapacity,
                factoryConfig,
                registry,
                aggregate,
                aggregateKey,
                Optional.of(objectScope),
                executor,
                bookKeeperGcGrace);
    }

    public static CompletableFuture<PulsarNativeManagedLedgerFactoryV2> openEnrolledOffload(
            PulsarNativeBindingV1 binding,
            MetadataStoreExtended metadata,
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long existingQuotaCapacity,
            ManagedLedgerFactoryConfig factoryConfig,
            M5ClosedWriterRegistryV1 registry,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            String aggregateKey,
            NereusPulsarLedgerOffloaderV1 offloader,
            PhysicalResourceIdV2.Namespace objectNamespace,
            long maximumObjectBytes,
            Executor executor,
            Supplier<CompletionStage<Void>> requireNativeOwnership,
            Optional<com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1> gcGrace) {
        try {
            // The historical Native compile API remains supported by the ordinary M5 library path.
            // Only the enrolled Broker path requires the Native SPI that retains the original UUID.
            if (SourceSafeLedgerOffloader.class
                            .getMethod("preservesIncompleteAttempt")
                            .getReturnType()
                    != boolean.class) {
                throw new NoSuchMethodException("original attempt SPI does not return boolean");
            }
        } catch (NoSuchMethodException failure) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Enrolled Native offload requires original attempt retention in the Pulsar runtime", failure));
        }
        return openNativeFactory(
                binding,
                metadata,
                metadataServiceUri,
                bootstrap,
                budget,
                existingQuotaCapacity,
                factoryConfig,
                registry,
                aggregate,
                aggregateKey,
                Optional.of(new ObjectScope(
                        Objects.requireNonNull(offloader),
                        Objects.requireNonNull(objectNamespace),
                        maximumObjectBytes,
                        Optional.of(requireNativeOwnership))),
                executor,
                gcGrace);
    }

    private static CompletableFuture<PulsarNativeManagedLedgerFactoryV2> openNativeFactory(
            PulsarNativeBindingV1 binding,
            MetadataStoreExtended metadata,
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long existingQuotaCapacity,
            ManagedLedgerFactoryConfig factoryConfig,
            M5ClosedWriterRegistryV1 registry,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            String aggregateKey,
            Optional<ObjectScope> objectScope,
            Executor executor,
            java.util.Optional<com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1>
                    bookKeeperGcGrace) {
        try {
            Objects.requireNonNull(bookKeeperGcGrace, "explicit optional Native BK GC policy");
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
            var expectedProfile = objectScope.isPresent()
                    ? StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT
                    : StorageProfileV1.BOOKKEEPER_WAL_ONLY;
            if (binding.authority().aggregate().initialEpoch().storageProfile() != expectedProfile) {
                throw new IllegalArgumentException("Native factory physical profile differs from the Binding");
            }
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        var observer = new CompletableFuture<PulsarNativeManagedLedgerFactoryV2>();
        binding.openFactoryBookKeeper(
                        metadataServiceUri, bootstrap, budget, existingQuotaCapacity, executor, bookKeeperGcGrace)
                .thenCompose(creation -> CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return new PulsarNativeManagedLedgerFactoryV2(
                                        binding,
                                        creation,
                                        new ManagedLedgerFactoryImpl(
                                                metadata, creation.namespace().borrowClient(), factoryConfig),
                                        executor,
                                        objectScope);
                            } catch (Throwable failure) {
                                try {
                                    creation.close();
                                } catch (Exception cleanup) {
                                    failure.addSuppressed(cleanup);
                                }
                                throw new java.util.concurrent.CompletionException(failure);
                            }
                        },
                        executor))
                .thenCompose(original -> binding.claimFirstNativeFactory(
                                original.creation, (MetaStoreImpl) original.factory.getMetaStore())
                        .thenCompose(ignored -> PulsarNativeMetadataWriterV2.install(
                                binding,
                                budget,
                                (MetaStoreImpl) original.factory.getMetaStore(),
                                registry,
                                aggregate,
                                aggregateKey,
                                original.creation))
                        .thenCompose(writer -> {
                            original.metadataWriter = writer;
                            return writer.openSdkRecoveryReference(registry)
                                    .thenCompose(unused -> objectScope
                                            .map(scope -> writer.admitObjectPhysicalScope(
                                                            scope.offloader(), scope.namespace(), scope.maximumBytes())
                                                    .thenCompose(ignored -> scope.enrolledOwnership()
                                                            .map(gate -> writer.enableNativeOutput(registry, gate))
                                                            .orElseGet(() -> CompletableFuture.completedFuture(null))))
                                            .orElseGet(() -> CompletableFuture.completedFuture(null)))
                                    .thenCompose(unused -> writer.admitSealedPlainSources())
                                    .thenApply(unused -> original);
                        })
                        .exceptionallyCompose(failure -> original.closeAsync().handle((unused, cleanup) -> {
                            if (cleanup != null) {
                                failure.addSuppressed(cleanup);
                            }
                            throw new java.util.concurrent.CompletionException(failure);
                        })))
                .whenComplete((original, failure) -> {
                    if (failure != null) {
                        observer.completeExceptionally(failure);
                    } else if (!observer.complete(original)) {
                        original.closeAsync();
                    }
                });
        return observer;
    }

    public M5BookKeeperNamespaceAuthorityV2 namespace() {
        return creation.namespace();
    }

    public ManagedLedgerImpl openLedger(ManagedLedgerConfig config) throws Exception {
        return openLedgerAsync(config, () -> CompletableFuture.completedFuture(true))
                .get();
    }

    /** Opens on the actual Broker ownership check without blocking its topic-load or metadata executor. */
    public CompletableFuture<ManagedLedgerImpl> openLedgerAsync(
            ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownership) {
        var original = new CompletableFuture<ManagedLedgerImpl>();
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(new ManagedLedgerException.ManagedLedgerFactoryClosedException());
            }
            if (opening != null && !opening.isDone()) {
                return CompletableFuture.failedFuture(new IllegalStateException("Native factory already owns an open"));
            }
            opening = original;
        }
        requireOwnership(ownership)
                .thenCompose(unused -> prepareRetention(config))
                .thenCompose(installation -> openNative(installation.config(), ownership)
                        .thenCompose(ledger -> {
                            synchronized (this) {
                                ownedLedgers.add(ledger);
                            }
                            return installation
                                    .installed(ledger)
                                    .thenCompose(unused -> requireOwnership(ownership))
                                    .thenApply(unused -> {
                                        requireOpen();
                                        return ledger;
                                    })
                                    .exceptionallyCompose(
                                            failure -> closeLedger(ledger).handle((unused, cleanup) -> {
                                                if (cleanup != null) {
                                                    failure.addSuppressed(cleanup);
                                                } else {
                                                    synchronized (this) {
                                                        ownedLedgers.remove(ledger);
                                                    }
                                                }
                                                throw new CompletionException(failure);
                                            }));
                        }))
                .whenComplete((ledger, failure) -> {
                    if (failure == null) {
                        original.complete(ledger);
                    } else {
                        original.completeExceptionally(failure);
                    }
                });
        return original.copy();
    }

    private CompletableFuture<RetentionInstallation> prepareRetention(ManagedLedgerConfig config) {
        requireOpen();
        if (objectScope.isEmpty() && config.getLedgerOffloader() != NullLedgerOffloader.INSTANCE) {
            throw new IllegalArgumentException("BookKeeper-only route requires the native disabled offloader");
        }
        var spec = creation.spec();
        if (config.getEnsembleSize() != spec.ensemble()
                || config.getWriteQuorumSize() != spec.writeQuorum()
                || config.getAckQuorumSize() != spec.ackQuorum()) {
            throw new IllegalArgumentException("Native factory data quorums differ from its exact creation scope");
        }
        config.setMetadataEnsembleSize(spec.ensemble())
                .setMetadataWriteQuorumSize(spec.writeQuorum())
                .setMetadataAckQuorumSize(spec.ackQuorum());
        binding.configureCreate(config);
        var installation = retentionInstallation == null
                ? binding.prepareNativeRetention(creation, (MetaStoreImpl) factory.getMetaStore(), config)
                : CompletableFuture.completedFuture(retentionInstallation);
        return installation
                .thenApply(value -> {
                    value.requireRequested(config);
                    retentionInstallation = value;
                    return value;
                })
                .toCompletableFuture();
    }

    private static CompletableFuture<Void> requireOwnership(Supplier<CompletableFuture<Boolean>> ownership) {
        return CompletableFuture.completedFuture(null)
                .thenCompose(unused -> ownership.get())
                .thenAccept(owned -> {
                    if (!Boolean.TRUE.equals(owned)) {
                        throw new CompletionException(new ManagedLedgerException.ManagedLedgerFencedException());
                    }
                });
    }

    private synchronized void requireOpen() {
        if (closed) {
            throw new CompletionException(new ManagedLedgerException.ManagedLedgerFactoryClosedException());
        }
    }

    private CompletableFuture<ManagedLedgerImpl> openNative(
            ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownership) {
        requireOpen();
        var opened = new CompletableFuture<ManagedLedgerImpl>();
        factory.asyncOpen(
                binding.managedLedgerName(),
                config,
                new OpenLedgerCallback() {
                    @Override
                    public void openLedgerComplete(ManagedLedger ledger, Object context) {
                        opened.complete((ManagedLedgerImpl) ledger);
                    }

                    @Override
                    public void openLedgerFailed(ManagedLedgerException failure, Object context) {
                        opened.completeExceptionally(failure);
                    }
                },
                ownership,
                null);
        return opened;
    }

    synchronized void requireOwnedLedger(ManagedLedgerImpl ledger) {
        requireOpen();
        if (!ownedLedgers.contains(ledger)) {
            throw new IllegalArgumentException("Native factory does not own this ledger");
        }
    }

    com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity bindingIdentity() {
        return binding.bindingIdentity();
    }

    /** Opens the exact previously published Native read plan before Broker data admission. */
    public synchronized CompletableFuture<Void> recoverSelectedReadOwner(
            ManagedLedgerImpl ledger,
            PulsarNativeM4DrainAdmissionV1 admission,
            PulsarNativeReadCellBudgetV1 budget,
            M5ClosedWriterRegistryV1 registry,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            String aggregateKey,
            int readSlots) {
        requireOpen();
        if (!ownedLedgers.contains(ledger) || readOpening != null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Native read plan must own its exact ledger"));
        }
        var scope = objectScope.orElseThrow();
        readControl = binding.m4Control();
        var sdk = creation.namespace();
        readOpening = ledger.captureNativeReadView()
                .thenAccept(view ->
                        admission.requireConfiguration(view, sdk, scope.offloader(), binding.metadataNamespace()))
                .thenCompose(unused -> readControl.captureSelectedSourceMap())
                .thenCompose(selected -> {
                    if (selected.source().isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    boolean materialized = selected.manifest()
                            || selected.map()
                                    .map(map -> PulsarNativeMaterializedReadV2.isRecipe(map.actualViewBytes()))
                                    .orElse(false);
                    return readControl
                            .installControlWriter(registry, aggregate, aggregateKey, budget)
                            .thenCompose(unused -> materialized
                                    ? readControl.prepareMaterializedReadSources(
                                            ledger, sdk, scope.offloader(), scope.namespace(), budget, executor)
                                    : readControl.preparePublishedReadSources(
                                            ledger, sdk, scope.offloader(), scope.namespace(), budget))
                            .thenCompose(preparation -> {
                                readPreparation = preparation;
                                return materialized
                                        ? readControl.prepareMaterializedReadOwner(
                                                preparation, admission, sdk, scope.offloader(), executor)
                                        : readControl.prepareColdReadOwner(
                                                preparation, admission, sdk, scope.offloader(), executor);
                            })
                            .thenCompose(
                                    recovery -> recovery.recover(executor, ledger.getExecutor(), readSlots, budget))
                            .thenAccept(owner -> selectedReadOwner = owner);
                });
        return readOpening.copy();
    }

    /** Drains the original selected read owner while its actual ManagedLedger is still open. */
    public synchronized CompletableFuture<Void> drainSelectedReadOwner() {
        if (readOpening == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (readClosing == null || readClosing.isCompletedExceptionally()) {
            readClosing = readOpening
                    .handle((unused, failure) -> null)
                    .thenCompose(unused -> selectedReadOwner != null
                            ? selectedReadOwner.drainForRecovery(executor)
                            : CompletableFuture.completedFuture(null))
                    .thenCompose(unused ->
                            readPreparation == null ? CompletableFuture.completedFuture(null) : readPreparation.close())
                    .thenCompose(unused -> readControl.closeControlWriter());
        }
        return readClosing.copy();
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        closed = true;
        if (closing == null || closing.isCompletedExceptionally()) {
            var opened = opening == null
                    ? CompletableFuture.completedFuture(null)
                    : opening.handle((ledger, failure) -> null);
            closing = opened.thenCompose(unused -> drainSelectedReadOwner())
                    .thenCompose(unused -> closeNativeFactory())
                    .thenCompose(unused -> metadataWriter == null
                            ? CompletableFuture.completedFuture(null)
                            : metadataWriter.prepareSdkClose())
                    .thenRunAsync(
                            () -> {
                                try {
                                    creation.closeSdkMetadataScope();
                                } catch (Exception failure) {
                                    throw new java.util.concurrent.CompletionException(failure);
                                }
                            },
                            executor)
                    .thenCompose(unused -> metadataWriter == null
                            ? CompletableFuture.completedFuture(null)
                            : metadataWriter.closeAsync())
                    .thenRunAsync(
                            () -> {
                                try {
                                    creation.close();
                                } catch (Exception failure) {
                                    throw new java.util.concurrent.CompletionException(failure);
                                }
                            },
                            executor);
        }
        return closing.copy();
    }

    private synchronized CompletableFuture<Void> closeNativeFactory() {
        if (factoryClosed) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(ownedLedgers.stream()
                        .map(PulsarNativeManagedLedgerFactoryV2::closeLedger)
                        .toArray(CompletableFuture[]::new))
                .thenCompose(unused -> {
                    try {
                        return factory.shutdownAsync();
                    } catch (Exception failure) {
                        return CompletableFuture.failedFuture(failure);
                    }
                })
                .thenRun(() -> {
                    synchronized (this) {
                        ownedLedgers.clear();
                        factoryClosed = true;
                    }
                });
    }

    private static CompletableFuture<Void> closeLedger(ManagedLedgerImpl ledger) {
        var original = new CompletableFuture<Void>();
        try {
            ledger.asyncClose(
                    new org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback() {
                        public void closeComplete(Object context) {
                            original.complete(null);
                        }

                        public void closeFailed(
                                org.apache.bookkeeper.mledger.ManagedLedgerException failure, Object context) {
                            original.completeExceptionally(failure);
                        }
                    },
                    null);
        } catch (Throwable failure) {
            original.completeExceptionally(failure);
        }
        return original;
    }

    public void close() throws Exception {
        closeAsync().get();
    }
}
