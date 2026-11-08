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

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.oxia.v2.OxiaV2CapabilityStore;
import com.nereusstream.metadata.oxia.v2.OxiaV2CapabilityStoreFactory;
import com.nereusstream.metadata.oxia.v2.continuity.RevalidationScheduler;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuityState;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1;
import com.nereusstream.pulsar.offload.PulsarNativeManagedLedgerFactoryV2;
import com.nereusstream.pulsar.offload.PulsarNativeReadCellBudgetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl.BookkeeperFactoryForCustomEnsemblePlacementPolicy;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.broker.storage.nereus.v2.MetadataStoreNereusOwnershipWitnessProvider;
import org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarActiveFence;
import org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarAuthorityInstaller;
import org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarManagedLedgerStorage;
import org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarTopicAuthority;
import org.apache.pulsar.broker.storage.nereus.v2.OxiaNereusPulsarBindingAuthorityProvider;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.NereusOwnershipMetadataStore;

/** Native managed-ledger storage for one explicit Nereus Protocol Cell. Other namespaces use stock storage. */
public final class PulsarM6ManagedLedgerStorage extends ManagedLedgerClientFactory
        implements NereusPulsarManagedLedgerStorage {
    private PulsarM6BookKeeperCellConfig configuration;
    private ManagedLedgerFactoryConfig factoryConfiguration;
    private MetadataStoreExtended nativeMetadata;
    private AsyncOxiaClient cellMetadata;
    private OxiaV2CapabilityStore p1;
    private M5BookKeeperNamespaceAuthorityV2 bookKeeper;
    private PulsarM6ObjectWalAllocator objectAllocator;
    private PulsarM6ObjectWalProvider objectProvider;
    private ThreadPoolExecutor objectIo;
    private Duration objectTimeout;
    private Map<TopicName, PulsarNativeBindingV1> admittedBindings;
    private CompletableFuture<PulsarNativeReadCellBudgetV1> activeBudget;
    private CompletableFuture<Void> priorDrain = CompletableFuture.completedFuture(null);
    private int activeScopes;
    private boolean closing;

    private record FactoryScope(
            PulsarNativeManagedLedgerFactoryV2 ledgerFactory, Optional<PulsarM6AsyncObjectScope> objectScope) {}

    @Override
    public void initialize(
            ServiceConfiguration conf,
            MetadataStoreExtended metadata,
            BookKeeperClientFactory bookKeeperProvider,
            EventLoopGroup eventLoop,
            OpenTelemetry telemetry)
            throws Exception {
        if (!(metadata instanceof NereusOwnershipMetadataStore)
                || !ExtensibleLoadManagerImpl.class.getName().equals(conf.getLoadManagerClassName())
                || !org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl
                        .class
                        .getName()
                        .equals(conf.getLoadManagerServiceUnitStateTableViewClassName())
                || conf.getLoadBalancerServiceUnitTableViewSyncer()
                        != ServiceConfiguration.ServiceUnitTableViewSyncerType.None) {
            throw new IllegalArgumentException(
                    "Nereus BookKeeper requires native Oxia ownership with MetadataStore ELM");
        }
        var cellPath = conf.getProperty("nereusPulsarCellConfig");
        if (!(cellPath instanceof String path) || path.isBlank()) {
            throw new IllegalArgumentException("nereusPulsarCellConfig is required for Nereus storage");
        }
        configuration = PulsarM6BookKeeperCellConfig.read(Path.of(path));
        nativeMetadata = metadata;
        if (!configuration.bookKeeperUri().equals(conf.getBookkeeperMetadataStoreUrl())) {
            throw new IllegalArgumentException("Configured Cell differs from the native BookKeeper namespace");
        }
        var bookKeeperJar = Path.of(BookKeeper.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        if (!Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(bookKeeperJar)))
                .equals(configuration.bookKeeper().clientArtifactSha256())) {
            throw new IllegalArgumentException("Loaded BookKeeper client differs from the admitted Cell artifact");
        }
        super.initialize(conf, metadata, bookKeeperProvider, eventLoop, telemetry);
        try {
            int timeout = conf.getMetadataStoreOperationTimeoutSeconds();
            var oxia = configuration.metadata();
            cellMetadata = OxiaClientBuilder.create(oxia.serviceAddress())
                    .namespace(oxia.namespace())
                    .asyncClient()
                    .get(timeout, TimeUnit.SECONDS);
            var ready = new CompletableFuture<Void>();
            p1 = OxiaV2CapabilityStoreFactory.connect(oxia, new RevalidationScheduler() {
                        @Override
                        public boolean request(long clientGeneration, long invalidationEpoch) {
                            ready.complete(null);
                            return true;
                        }

                        @Override
                        public void close() {
                            ready.cancel(false);
                        }
                    })
                    .get(timeout, TimeUnit.SECONDS);
            ready.get(timeout, TimeUnit.SECONDS);
            var bindings = new HashMap<TopicName, PulsarNativeBindingV1>();
            for (var topic : configuration.generations().keySet()) {
                bindings.put(topic, readBinding(topic).toCompletableFuture().get(timeout, TimeUnit.SECONDS));
            }
            admittedBindings = Map.copyOf(bindings);
            if (configuration.objectWal().isPresent()) {
                initializeObjectWal(configuration.objectWal().orElseThrow(), Duration.ofSeconds(timeout));
                return;
            }
            bookKeeper = M5BookKeeperNamespaceAuthorityV2.connectExisting(
                    configuration.bookKeeperUri(), configuration.bookKeeper());
            var source = bookKeeper
                    .readBinding()
                    .toCompletableFuture()
                    .get(timeout, TimeUnit.SECONDS)
                    .orElseThrow(() -> new IllegalStateException("Native BookKeeper namespace is not provisioned"));
            if (!source.metadataNamespace().equals(configuration.metadataIdentity())
                    || !source.physicalNamespace()
                            .serviceIdentity()
                            .value()
                            .equals(configuration.bookKeeperNamespace())) {
                throw new IllegalArgumentException("Native BookKeeper namespace differs from the exact Cell scope");
            }
            var delete = configuration.objectScope().flatMap(PulsarM6AsyncObjectScope.Configuration::bookKeeperDelete);
            if (delete.isPresent()) {
                delete.orElseThrow()
                        .requireNativeBudget(bookKeeper)
                        .toCompletableFuture()
                        .get(timeout, TimeUnit.SECONDS);
            }
        } catch (Exception failure) {
            try {
                close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private void initializeObjectWal(PulsarM6ObjectWalSettings settings, Duration timeout) throws Exception {
        if (java.net.URI.create(configuration.bookKeeperUri()).getPath().equals(settings.zkLedgerRoot())) {
            throw new IllegalArgumentException(
                    "Object WAL compatibility namespace must exclude stock BookKeeper writers");
        }
        objectTimeout = timeout;
        var locked = PulsarM6ObjectWalNamespace.read(settings.zkServers(), settings.zkLedgerRoot(), timeout);
        if (!locked.proof().equals(settings.namespaceProof())) {
            throw new IllegalArgumentException(
                    "Pulsar Object WAL BookKeeper namespace differs from the admission proof");
        }
        objectAllocator = new PulsarM6ObjectWalAllocator(
                cellMetadata,
                () -> {
                    if (p1.continuitySnapshot().state() != StoreContinuityState.READY) {
                        throw new IllegalStateException("Pulsar Object WAL requires ready Oxia continuity");
                    }
                },
                configuration.metadata().authorityRoot(),
                configuration.cell(),
                locked,
                settings.registry(),
                configuration.bookKeeper().clientArtifactSha256(),
                settings.selectionBytes(),
                settings.artifacts());
        objectAllocator.open().toCompletableFuture().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        var namespace = OxiaPhysicalMetadataNamespaceV2.connect(cellMetadata, configuration.metadataIdentity())
                .toCompletableFuture()
                .get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        objectProvider = PulsarM6ObjectWalProvider.open(settings.provider(), configuration.cell(), namespace, timeout);
        int maximumTasks = Math.multiplyExact(
                admittedBindings.size(),
                Math.addExact(
                        8,
                        Math.multiplyExact(
                                configuration.readLimits().maximumEntries(),
                                Math.toIntExact(configuration.share().readSlots()))));
        var ids = new java.util.concurrent.atomic.AtomicInteger();
        objectIo = new ThreadPoolExecutor(
                settings.ioThreads(),
                settings.ioThreads(),
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(maximumTasks),
                runnable -> new Thread(runnable, "pulsar-object-wal-provider-" + ids.incrementAndGet()));
    }

    @Override
    protected ManagedLedgerFactoryImpl createManagedLedgerFactory(
            MetadataStoreExtended metadata,
            OpenTelemetry telemetry,
            BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperFactory,
            ManagedLedgerFactoryConfig configuration,
            StatsLogger stats)
            throws Exception {
        this.factoryConfiguration = configuration;
        return super.createManagedLedgerFactory(metadata, telemetry, bookKeeperFactory, configuration, stats);
    }

    @Override
    public boolean selects(TopicName topic) {
        return configuration.namespace().equals(topic.getNamespaceObject()) && !SystemTopicNames.isSystemTopic(topic);
    }

    private PulsarTopicIncarnationIdentity incarnation(TopicName topic) {
        Long generation = configuration.generations().get(topic);
        if (generation == null) {
            throw new IllegalArgumentException("Topic has no admitted Nereus Cell binding");
        }
        return new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString(topic.getPersistenceNamingEncoding()),
                PulsarTopicName.fromString(topic.toString()),
                new PulsarBindingGeneration(generation));
    }

    private CompletionStage<PulsarNativeBindingV1> readBinding(TopicName topic) {
        var incarnation = incarnation(topic);
        var read = configuration.objectWal().isPresent()
                ? PulsarNativeBindingV1.readObjectWal(
                        cellMetadata,
                        configuration.metadataIdentity(),
                        configuration.metadata().authorityRoot(),
                        incarnation)
                : PulsarNativeBindingV1.read(
                        cellMetadata,
                        configuration.metadataIdentity(),
                        configuration.metadata().authorityRoot(),
                        incarnation);
        return read.thenCompose(binding -> {
            var aggregate = binding.authority().aggregate();
            if (!aggregate.binding().cellIdentity().equals(configuration.cell())
                    || aggregate.initialEpoch().storageProfile() != configuration.profile()) {
                throw new IllegalArgumentException("Pulsar Binding differs from the admitted BookKeeper Cell");
            }
            return configuration
                            .objectScope()
                            .flatMap(PulsarM6AsyncObjectScope.Configuration::bookKeeperDelete)
                            .isPresent()
                    ? binding.requireBookKeeperDeleteAuditEnrollment(configuration.writers())
                            .thenApply(unused -> binding)
                    : CompletableFuture.completedFuture(binding);
        });
    }

    @Override
    public CompletionStage<OpenedLedger> openTopic(
            PulsarService broker,
            TopicName topic,
            ManagedLedgerFactory stockFactory,
            ManagedLedgerConfig config,
            Supplier<CompletableFuture<Boolean>> stockOwnership) {
        return broker.getNamespaceService().getBundleAsync(topic).thenCompose(bundle -> {
            var channel = ServiceUnitStateChannelImpl.get(broker);
            if (!(channel instanceof ServiceUnitStateChannelImpl nativeChannel)) {
                return CompletableFuture.failedFuture(new IllegalStateException("Qualified ELM channel is absent"));
            }
            var incarnation = incarnation(topic);
            var bundleName = bundle.toString();
            final org.apache.pulsar.broker.storage.nereus.v2.NereusP1BundleFence nativeBundle;
            try {
                nativeBundle = nativeChannel.captureNativeBundleFence(bundleName);
            } catch (Exception failure) {
                return CompletableFuture.failedFuture(failure);
            }
            var installer = new NereusPulsarAuthorityInstaller(
                    new MetadataStoreNereusOwnershipWitnessProvider(nativeMetadata, broker.getBrokerId()),
                    new OxiaNereusPulsarBindingAuthorityProvider(p1),
                    new NereusPulsarActiveFence());
            Supplier<CompletableFuture<Boolean>> nativeOwnership = () -> nativeBundle.isCurrent()
                    ? stockOwnership.get().thenApply(owned -> owned && nativeBundle.isCurrent())
                    : CompletableFuture.completedFuture(false);
            return readBinding(topic).thenCompose(binding -> installer
                    .install(bundleName, incarnation, nativeBundle)
                    .thenCompose(first -> {
                        try (first) {
                            requireSameBinding(first, binding);
                        } catch (Exception failure) {
                            return CompletableFuture.failedFuture(failure);
                        }
                        var original = first.word().ownership();
                        var topicAuthority = new NereusPulsarTopicAuthority(broker.getExecutor(), invalidated -> {
                            try {
                                var refreshed = nativeChannel.captureNativeBundleFence(bundleName);
                                if (!refreshed.matches(original)) {
                                    return CompletableFuture.failedFuture(
                                            new IllegalStateException("Native ledger cannot reuse a new acquisition"));
                                }
                                return installer
                                        .install(bundleName, incarnation, refreshed, invalidated)
                                        .thenApply(next -> {
                                            try {
                                                requireSameBinding(next, binding);
                                                return next;
                                            } catch (RuntimeException error) {
                                                try {
                                                    next.close();
                                                } catch (Exception cleanup) {
                                                    error.addSuppressed(cleanup);
                                                }
                                                throw error;
                                            }
                                        });
                            } catch (Exception failure) {
                                return CompletableFuture.failedFuture(failure);
                            }
                        });
                        return openScoped(binding, broker, config, nativeOwnership, topicAuthority);
                    }));
        });
    }

    private CompletionStage<OpenedLedger> openScoped(
            PulsarNativeBindingV1 binding,
            PulsarService broker,
            ManagedLedgerConfig config,
            Supplier<CompletableFuture<Boolean>> ownership,
            NereusPulsarTopicAuthority topicAuthority) {
        config.setNativeWriteAuthority(new ManagedLedgerConfig.NativeWriteAuthority() {
            @Override
            public CompletionStage<Void> refresh() {
                return topicAuthority.requestRefresh();
            }

            @Override
            public Object capture(Object context) {
                var current = topicAuthority.captureOrNull();
                if (current != null && context instanceof PublishContext publish) {
                    publish.setStorageAuthorityWord(current);
                }
                return current;
            }

            @Override
            public boolean isCurrent(Object permit) {
                return permit instanceof NereusPulsarActiveFence.ValidWord captured
                        && topicAuthority.isCurrent(captured);
            }
        });
        if (configuration.objectWal().isPresent()) {
            return openObjectWal(binding, broker, config, ownership, topicAuthority);
        }
        return acquireBudget().thenCompose(budget -> {
            var root = configuration.metadata().authorityRoot();
            var aggregate = new M5PulsarAggregateRetirementCoordinatorV1(
                    new Oxia09ExactMetadataTransactionStoreV1(cellMetadata));
            var key = new OxiaV2AuthorityKeys(root)
                    .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
            var admitted = ownership
                    .get()
                    .thenCompose(owned -> owned
                            ? openFactory(binding, broker, budget, aggregate, key, config, topicAuthority)
                            : CompletableFuture.failedFuture(
                                    new IllegalStateException("Native bundle was revoked before open")));
            return admitted.exceptionallyCompose(error -> {
                        if (unwrap(error).getSuppressed().length != 0) {
                            return CompletableFuture.failedFuture(new UnsettledOpenException(error));
                        }
                        return releaseBudget().handle((unused, cleanup) -> {
                            throw openFailure(error, cleanup);
                        });
                    })
                    .thenCompose(factory -> factory.ledgerFactory()
                            .openLedgerAsync(config, ownership)
                            .thenCompose(ledger -> configuration
                                    .objectScope()
                                    .flatMap(PulsarM6AsyncObjectScope.Configuration::bookKeeperDelete)
                                    .map(policy -> factory.ledgerFactory()
                                            .recoverSelectedReadOwner(
                                                    ledger,
                                                    policy.drainAdmission(),
                                                    budget,
                                                    configuration.writers(),
                                                    aggregate,
                                                    key,
                                                    Math.toIntExact(configuration
                                                            .share()
                                                            .readSlots())))
                                    .orElseGet(() -> CompletableFuture.completedFuture(null))
                                    .thenApply(unused -> ledger))
                            .thenCompose(ledger -> topicAuthority
                                    .requestRefresh()
                                    .thenApply(unused -> new OpenedLedger(
                                            ledger,
                                            topicAuthority,
                                            factory.ledgerFactory()::drainSelectedReadOwner,
                                            scopeCloser(factory))))
                            .exceptionallyCompose(error -> topicAuthority
                                    .closeAsync()
                                    .thenCompose(unused -> closeScope(factory))
                                    .handle((unused, cleanup) -> {
                                        throw openFailure(error, cleanup);
                                    })));
        });
    }

    private CompletionStage<OpenedLedger> openObjectWal(
            PulsarNativeBindingV1 binding,
            PulsarService broker,
            ManagedLedgerConfig config,
            Supplier<CompletableFuture<Boolean>> ownership,
            NereusPulsarTopicAuthority authority) {
        var settings = configuration.objectWal().orElseThrow();
        var reads = configuration.readLimits();
        var limits = new PulsarM6ObjectWalTopic.Limits(
                Math.toIntExact(reads.maximumOwnedEntryBytes()),
                config.getMaxEntriesPerLedger(),
                settings.maximumPendingAppends(),
                settings.maximumPendingBytes(),
                reads.maximumEntries(),
                Math.toIntExact(configuration.share().readSlots()));
        var name = binding.authority().aggregate().binding().incarnationIdentity();
        var persistence = ((PulsarTopicIncarnationIdentity) name)
                .persistenceName()
                .value()
                .value();
        java.util.concurrent.Executor ordered =
                task -> broker.getOrderedExecutor().executeOrdered(persistence, task);
        synchronized (this) {
            if (closing) {
                return CompletableFuture.failedFuture(new IllegalStateException("Pulsar Object WAL Cell is closing"));
            }
            activeScopes++;
        }
        var released = new java.util.concurrent.atomic.AtomicBoolean();
        Runnable release = () -> {
            if (released.compareAndSet(false, true)) {
                synchronized (this) {
                    activeScopes--;
                }
            }
        };
        var installed = new java.util.concurrent.atomic.AtomicReference<PulsarM6ObjectWalTopic>();
        return CompletableFuture.completedFuture(null)
                .thenCompose(unused -> ownership.get())
                .thenCompose(owned -> {
                    if (!owned) {
                        return CompletableFuture.<PulsarM6ObjectWalTopic>failedFuture(
                                new IllegalStateException("Pulsar Object WAL bundle was revoked before open"));
                    }
                    return PulsarM6ObjectWalTopic.open(
                            objectAllocator,
                            objectProvider,
                            cellMetadata,
                            configuration.metadata().authorityRoot(),
                            binding,
                            authority,
                            limits,
                            objectTimeout,
                            objectIo,
                            ordered);
                })
                .thenCompose(topic -> {
                    installed.set(topic);
                    return PulsarM6ObjectWalCursorStore.open(
                                    new Oxia09ExactMetadataTransactionStoreV1(cellMetadata),
                                    configuration.metadata().authorityRoot(),
                                    topic.chain().incarnation().value(),
                                    topic.owner())
                            .thenApply(cursors -> {
                                var ledger = new PulsarM6ObjectWalManagedLedger(
                                        persistence,
                                        topic,
                                        cursors,
                                        config,
                                        reads.maximumEntries(),
                                        reads.maximumOwnedBatchBytes());
                                return new OpenedLedger(ledger, authority, ledger::drain, () -> {
                                    release.run();
                                    return CompletableFuture.completedFuture(null);
                                });
                            });
                })
                .exceptionallyCompose(error -> {
                    var topic = installed.get();
                    return authority
                            .closeAsync()
                            .thenCompose(
                                    unused -> topic == null ? CompletableFuture.completedFuture(null) : topic.close())
                            .handle((unused, cleanup) -> {
                                if (cleanup != null) {
                                    throw openFailure(error, cleanup);
                                }
                                if (unwrap(error).getSuppressed().length != 0) {
                                    throw new UnsettledOpenException(error);
                                }
                                release.run();
                                throw openFailure(error, null);
                            });
                });
    }

    private CompletableFuture<FactoryScope> openFactory(
            PulsarNativeBindingV1 binding,
            PulsarService broker,
            PulsarNativeReadCellBudgetV1 budget,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            String key,
            ManagedLedgerConfig ledgerConfig,
            NereusPulsarTopicAuthority topicAuthority) {
        if (configuration.objectScope().isEmpty()) {
            return PulsarNativeManagedLedgerFactoryV2.openBookKeeperOnly(
                            binding,
                            nativeMetadata,
                            configuration.bookKeeperUri(),
                            bookKeeper,
                            budget,
                            configuration.bookKeeperQuota(),
                            factoryConfiguration,
                            configuration.writers(),
                            aggregate,
                            key,
                            broker.getExecutor())
                    .thenApply(factory -> new FactoryScope(factory, Optional.empty()));
        }
        var configured = configuration.objectScope().orElseThrow();
        return PulsarM6AsyncObjectScope.open(configured, configuration.metadataIdentity())
                .thenCompose(object -> {
                    ledgerConfig.setLedgerOffloader(object.offloader());
                    return PulsarNativeManagedLedgerFactoryV2.openEnrolledOffload(
                                    binding,
                                    nativeMetadata,
                                    configuration.bookKeeperUri(),
                                    bookKeeper,
                                    budget,
                                    configuration.bookKeeperQuota(),
                                    factoryConfiguration,
                                    configuration.writers(),
                                    aggregate,
                                    key,
                                    object.offloader(),
                                    configured.physicalNamespace(),
                                    S3C1ObjectProviderTransport.EVIDENCED_MAXIMUM_OBJECT_BYTES,
                                    broker.getExecutor(),
                                    topicAuthority::requestRefresh,
                                    configured
                                            .bookKeeperDelete()
                                            .map(PulsarM6AsyncObjectScope.BookKeeperDeletePolicy::grace))
                            .thenApply(factory -> new FactoryScope(factory, Optional.of(object)))
                            .exceptionallyCompose(error -> {
                                if (unwrap(error).getSuppressed().length != 0) {
                                    return CompletableFuture.failedFuture(error);
                                }
                                return object.closeAsync().handle((unused, cleanup) -> {
                                    if (cleanup != null) {
                                        unwrap(error).addSuppressed(cleanup);
                                    }
                                    throw new java.util.concurrent.CompletionException(error);
                                });
                            });
                });
    }

    private static Throwable unwrap(Throwable error) {
        while (error instanceof java.util.concurrent.CompletionException && error.getCause() != null) {
            error = error.getCause();
        }
        return error;
    }

    private static RuntimeException openFailure(Throwable error, Throwable cleanup) {
        if (cleanup != null) {
            error.addSuppressed(cleanup);
            return new UnsettledOpenException(error);
        }
        return new java.util.concurrent.CompletionException(error);
    }

    private static void requireSameBinding(
            NereusPulsarAuthorityInstaller.Installation installation, PulsarNativeBindingV1 binding) {
        var actual = installation.word().binding();
        if (!installation.fence().isCurrent(installation.word())
                || !actual.bindingId().equals(binding.bindingIdentity().bindingId())
                || !actual.storageEpochId()
                        .digest()
                        .equals(binding.bindingIdentity().storageEpochSha256())
                || !actual.aggregateDigest()
                        .equals(binding.authority().aggregate().canonicalStoredDigest())) {
            throw new IllegalStateException("Native open did not acquire the exact current Binding");
        }
    }

    private synchronized CompletableFuture<PulsarNativeReadCellBudgetV1> acquireBudget() {
        if (closing) {
            return CompletableFuture.failedFuture(new IllegalStateException("Nereus Cell storage is closing"));
        }
        activeScopes++;
        var candidate = activeBudget;
        if (candidate == null) {
            var profile = configuration.share();
            Map<BindingIdentity, PulsarNativeReadCellBudgetV1.Usage> shares = new HashMap<>();
            admittedBindings.values().forEach(binding -> shares.put(binding.bindingIdentity(), profile));
            int count = shares.size();
            var limit = new PulsarNativeReadCellBudgetV1.Usage(
                    Math.multiplyExact(profile.owners(), count), Math.multiplyExact(profile.readSlots(), count),
                    Math.multiplyExact(profile.encodedBytes(), count),
                            Math.multiplyExact(profile.decodedBytes(), count));
            var initial = admittedBindings.values().iterator().next();
            var budget = initial.readCellBudget(limit, shares, configuration.readLimits());
            var original = priorDrain.thenCompose(unused -> budget.admit());
            activeBudget = original;
            candidate = original;
            original.whenComplete((unused, failure) -> {
                if (failure != null) {
                    synchronized (this) {
                        if (activeBudget == original) {
                            activeBudget = null;
                            activeScopes = 0;
                        }
                    }
                }
            });
        }
        return candidate.copy();
    }

    private synchronized CompletableFuture<Void> releaseBudget() {
        if (--activeScopes != 0) {
            return CompletableFuture.completedFuture(null);
        }
        var original = activeBudget;
        activeBudget = null;
        priorDrain = original.thenCompose(PulsarNativeReadCellBudgetV1::closeCell);
        return priorDrain.copy();
    }

    private CompletableFuture<Void> closeScope(FactoryScope factory) {
        return factory.ledgerFactory()
                .closeAsync()
                .thenCompose(unused -> factory.objectScope()
                        .map(PulsarM6AsyncObjectScope::closeAsync)
                        .orElseGet(() -> CompletableFuture.completedFuture(null)))
                .thenCompose(unused -> releaseBudget());
    }

    private Supplier<CompletableFuture<Void>> scopeCloser(FactoryScope factory) {
        return new Supplier<>() {
            private CompletableFuture<Void> closingScope;

            @Override
            public synchronized CompletableFuture<Void> get() {
                if (closingScope == null) {
                    closingScope = closeScope(factory);
                }
                return closingScope.copy();
            }
        };
    }

    @Override
    public void close() throws IOException {
        CompletableFuture<Void> settled;
        synchronized (this) {
            closing = true;
            if (activeScopes != 0) {
                throw new IOException("Nereus Cell still owns unsettled native ManagedLedger scopes");
            }
            settled = priorDrain;
        }
        try {
            settled.get();
            if (objectIo != null) {
                objectIo.shutdown();
                if (!objectIo.awaitTermination(objectTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    throw new IllegalStateException("Pulsar Object WAL provider work is still active");
                }
            }
            try (var stock = (AutoCloseable) super::close;
                    var bk = bookKeeper;
                    var cell = cellMetadata;
                    var active = p1;
                    var object = objectProvider) {
                // These providers are closed only after the final Cell actor really drained.
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException(interrupted);
        } catch (Exception failure) {
            throw new IOException("Nereus Cell did not close", failure);
        }
    }
}
