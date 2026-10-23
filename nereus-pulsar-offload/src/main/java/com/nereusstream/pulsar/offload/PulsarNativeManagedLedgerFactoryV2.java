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

import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** One actual M5 SDK factory owns its guarded BK scope and metadata writer before its first native mutation. */
public final class PulsarNativeManagedLedgerFactoryV2 implements AutoCloseable {
    private final PulsarNativeBindingV1 binding;
    private final PulsarNativeBookKeeperCreateV1 creation;
    private final ManagedLedgerFactoryImpl factory;
    private final Executor executor;
    private PulsarNativeMetadataWriterV2 metadataWriter;
    private PulsarNativeBindingV1.RetentionInstallation retentionInstallation;
    private final java.util.Set<ManagedLedgerImpl> ownedLedgers = new java.util.HashSet<>();
    private CompletableFuture<Void> closing;
    private boolean factoryClosed;
    private boolean closed;

    private PulsarNativeManagedLedgerFactoryV2(
            PulsarNativeBindingV1 binding,
            PulsarNativeBookKeeperCreateV1 creation,
            ManagedLedgerFactoryImpl factory,
            Executor executor) {
        this.binding = binding;
        this.creation = creation;
        this.factory = factory;
        this.executor = executor;
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
            java.util.Objects.requireNonNull(bookKeeperGcGrace, "explicit optional Native BK GC policy");
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
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
                                        executor);
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
                                    .thenCompose(unused -> writer.admitObjectPhysicalScope(
                                            offloader, objectNamespace, maximumObjectBytes))
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

    public synchronized ManagedLedgerImpl openLedger(ManagedLedgerConfig config) throws Exception {
        if (closed) {
            throw new IllegalStateException("Original Native M5 factory is closed");
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
        if (retentionInstallation == null) {
            retentionInstallation = binding.prepareNativeRetention(
                            creation, (MetaStoreImpl) factory.getMetaStore(), config)
                    .toCompletableFuture()
                    .get();
        }
        retentionInstallation.requireRequested(config);
        var ledger = (ManagedLedgerImpl) factory.open(binding.managedLedgerName(), retentionInstallation.config());
        ownedLedgers.add(ledger);
        retentionInstallation.installed(ledger).toCompletableFuture().get();
        return ledger;
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        closed = true;
        if (closing == null || closing.isCompletedExceptionally()) {
            CompletableFuture<Void> nativeClose;
            try {
                nativeClose = factoryClosed
                        ? CompletableFuture.completedFuture(null)
                        : CompletableFuture.allOf(ownedLedgers.stream()
                                        .map(PulsarNativeManagedLedgerFactoryV2::closeLedger)
                                        .toArray(CompletableFuture[]::new))
                                .thenCompose(unused -> {
                                    try {
                                        return factory.shutdownAsync();
                                    } catch (Exception failure) {
                                        return CompletableFuture.failedFuture(failure);
                                    }
                                })
                                .thenRun(() -> factoryClosed = true);
            } catch (Exception failure) {
                nativeClose = CompletableFuture.failedFuture(failure);
            }
            closing = nativeClose
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
