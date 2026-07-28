/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.materialization.CommittedGenerationRetirementAuthority;
import com.nereusstream.materialization.MaterializationSourceProvider;
import com.nereusstream.materialization.MaterializationStreamTrigger;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.BookKeeper;

/**
 * Provider-neutral production composition for one BookKeeper primary-WAL runtime.
 *
 * <p>The runtime owns the BookKeeper metadata view and all Nereus BookKeeper adapters. It borrows
 * the shared Oxia runtime, the BookKeeper client, capability stores, readiness provider, password
 * provider and clock. The caller must close this runtime before closing any borrowed dependency.
 */
public final class BookKeeperPrimaryWalRuntime implements AutoCloseable {
    private static final long ESTIMATED_READ_HANDLE_BYTES = 64L * 1024L;

    private final OxiaJavaBookKeeperMetadataStore metadata;
    private final String deploymentId;
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerIdNamespaceReservation namespace;
    private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
    private final BookKeeperLedgerIdNamespaceReservationAdminStore namespaceAdminStore;
    private final DefaultBookKeeperProtocolActivationVerifier activationVerifier;
    private final BookKeeperProtocolActivationStore activationStore;
    private final BookKeeperBrokerReadinessProvider brokerReadiness;
    private final BookKeeperClientOperations operations;
    private final BookKeeperPasswordProvider passwords;
    private final Clock clock;
    private final BookKeeperWalRuntime walRuntime;
    private final BookKeeperPrimaryPhysicalReferenceAdapter physicalReferences;
    private final BookKeeperMaterializationSourceProtectionAdapter sourceProtections;
    private final Optional<BookKeeperPrimaryWalRuntimeCapabilityBinding> capabilityBinding;
    private final AtomicBoolean closed = new AtomicBoolean();

    public static BookKeeperPrimaryWalRuntime create(
            String deploymentId,
            String cluster,
            String processRunId,
            BookKeeperWalConfiguration configuration,
            OxiaClientConfiguration oxia,
            SharedOxiaClientRuntime sharedOxia,
            BookKeeper borrowedClient,
            BookKeeperLedgerIdNamespaceReservationAdminStore namespaceReservations,
            BookKeeperProtocolActivationStore activationStore,
            BookKeeperBrokerReadinessProvider brokerReadiness,
            BookKeeperPasswordProvider passwords,
            Clock clock) {
        String exactDeploymentId = text(deploymentId, "deploymentId");
        String exactCluster = text(cluster, "cluster");
        String exactProcessRunId = text(processRunId, "processRunId");
        BookKeeperWalConfiguration wal = Objects.requireNonNull(configuration, "configuration");
        Clock exactClock = Objects.requireNonNull(clock, "clock");
        BookKeeperMetadataStoreConfig metadataConfiguration = new BookKeeperMetadataStoreConfig(
                wal.maxAppendRangesPerLedger(),
                wal.protectionSlotsPerRange(),
                wal.maxReaderLeasesPerLedger(),
                wal.maxUncertainAllocations());
        OxiaJavaBookKeeperMetadataStore metadata =
                OxiaJavaBookKeeperMetadataStore.usingSharedRuntime(
                        Objects.requireNonNull(oxia, "oxia"),
                        Objects.requireNonNull(sharedOxia, "sharedOxia"),
                        exactClock,
                        metadataConfiguration);
        try {
            BookKeeperPasswordProvider exactPasswords = Objects.requireNonNull(
                    passwords, "passwords");
            byte[] passwordProbe = exactPasswords.resolve(wal.passwordRef());
            if (passwordProbe == null) {
                throw new IllegalArgumentException("BookKeeper password provider returned null");
            }
            Arrays.fill(passwordProbe, (byte) 0);
            BookKeeperLedgerIdNamespaceReservationAdminStore exactReservations =
                    Objects.requireNonNull(namespaceReservations, "namespaceReservations");
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier =
                    new BookKeeperLedgerIdNamespaceReservationVerifier(
                            exactReservations, exactDeploymentId);
            BookKeeperLedgerIdNamespaceReservation namespace = namespaceVerifier
                    .requireActive(wal, wal.operationTimeout())
                    .join();
            BookKeeperProtocolActivationStore exactActivationStore =
                    Objects.requireNonNull(activationStore, "activationStore");
            Optional<BookKeeperProtocolActivation> activation = exactActivationStore
                    .read(wal, namespace, wal.operationTimeout())
                    .join();
            BookKeeperBrokerReadinessProvider exactReadiness =
                    Objects.requireNonNull(brokerReadiness, "brokerReadiness");
            DefaultBookKeeperProtocolActivationVerifier activationVerifier =
                    new DefaultBookKeeperProtocolActivationVerifier(
                            exactActivationStore, wal, namespace, exactReadiness);
            BookKeeperClientOperations operations = new DefaultBookKeeperClientOperations(
                    Objects.requireNonNull(borrowedClient, "borrowedClient"));
            BookKeeperWriterStateMachine writer = new BookKeeperWriterStateMachine(
                    exactCluster,
                    wal,
                    metadata,
                    exactClock,
                    exactProcessRunId);
            BookKeeperLedgerAllocator allocator = new BookKeeperLedgerAllocator(
                    exactCluster,
                    wal,
                    metadata,
                    metadata,
                    namespaceVerifier,
                    operations,
                    exactPasswords,
                    writer,
                    exactClock);
            BookKeeperLedgerRecovery recovery = new BookKeeperLedgerRecovery(
                    exactCluster,
                    wal,
                    metadata,
                    metadata,
                    namespaceVerifier,
                    operations,
                    exactPasswords,
                    writer,
                    exactClock);
            BookKeeperPrimaryWalAppender appender = new BookKeeperPrimaryWalAppender(
                    exactCluster,
                    wal,
                    metadata,
                    metadata,
                    allocator,
                    recovery,
                    writer,
                    operations,
                    exactClock);
            BookKeeperPrimaryWalReader reader = new BookKeeperPrimaryWalReader(
                    exactCluster,
                    wal,
                    metadata,
                    operations,
                    exactPasswords,
                    handleCache(wal),
                    new BookKeeperReaderLeaseManager(
                            exactCluster,
                            wal,
                            metadata,
                            exactClock,
                            exactProcessRunId));
            BookKeeperPrimaryPhysicalReferenceAdapter references =
                    new BookKeeperPrimaryPhysicalReferenceAdapter(
                            exactCluster,
                            wal,
                            metadata,
                            metadata,
                            exactClock);
            return new BookKeeperPrimaryWalRuntime(
                    metadata,
                    exactDeploymentId,
                    exactCluster,
                    wal,
                    namespace,
                    namespaceVerifier,
                    exactReservations,
                    activationVerifier,
                    exactActivationStore,
                    exactReadiness,
                    operations,
                    exactPasswords,
                    exactClock,
                    new BookKeeperWalRuntime(appender, reader, references),
                    references,
                    new BookKeeperMaterializationSourceProtectionAdapter(
                            exactCluster,
                            wal,
                            metadata,
                            exactClock),
                    activation.filter(BookKeeperProtocolActivation::supportsAllPublications)
                            .map(active -> new BookKeeperPrimaryWalRuntimeCapabilityBinding(
                                    1,
                                    wal.configurationBindingSha256(),
                                    namespace.ledgerIdNamespaceSha256(),
                                    active.publicationActivationSha256(),
                                    1)));
        } catch (Throwable failure) {
            try {
                metadata.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(
                    "failed to create BookKeeper primary-WAL runtime", failure);
        }
    }

    private BookKeeperPrimaryWalRuntime(
            OxiaJavaBookKeeperMetadataStore metadata,
            String deploymentId,
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperLedgerIdNamespaceReservationAdminStore namespaceAdminStore,
            DefaultBookKeeperProtocolActivationVerifier activationVerifier,
            BookKeeperProtocolActivationStore activationStore,
            BookKeeperBrokerReadinessProvider brokerReadiness,
            BookKeeperClientOperations operations,
            BookKeeperPasswordProvider passwords,
            Clock clock,
            BookKeeperWalRuntime walRuntime,
            BookKeeperPrimaryPhysicalReferenceAdapter physicalReferences,
            BookKeeperMaterializationSourceProtectionAdapter sourceProtections,
            Optional<BookKeeperPrimaryWalRuntimeCapabilityBinding> capabilityBinding) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.deploymentId = Objects.requireNonNull(deploymentId, "deploymentId");
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.namespaceVerifier = Objects.requireNonNull(namespaceVerifier, "namespaceVerifier");
        this.namespaceAdminStore = Objects.requireNonNull(namespaceAdminStore, "namespaceAdminStore");
        this.activationVerifier = Objects.requireNonNull(activationVerifier, "activationVerifier");
        this.activationStore = Objects.requireNonNull(activationStore, "activationStore");
        this.brokerReadiness = Objects.requireNonNull(brokerReadiness, "brokerReadiness");
        this.operations = Objects.requireNonNull(operations, "operations");
        this.passwords = Objects.requireNonNull(passwords, "passwords");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.walRuntime = Objects.requireNonNull(walRuntime, "walRuntime");
        this.physicalReferences = Objects.requireNonNull(physicalReferences, "physicalReferences");
        this.sourceProtections = Objects.requireNonNull(sourceProtections, "sourceProtections");
        this.capabilityBinding = Objects.requireNonNull(capabilityBinding, "capabilityBinding");
    }

    public String deploymentId() {
        ensureOpen();
        return deploymentId;
    }

    public String cluster() {
        ensureOpen();
        return cluster;
    }

    public BookKeeperWalConfiguration configuration() {
        ensureOpen();
        return configuration;
    }

    public OxiaJavaBookKeeperMetadataStore metadata() {
        ensureOpen();
        return metadata;
    }

    public BookKeeperLedgerIdNamespaceReservation namespace() {
        ensureOpen();
        return namespace;
    }

    public BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier() {
        ensureOpen();
        return namespaceVerifier;
    }

    public BookKeeperLedgerIdNamespaceReservationAdminStore namespaceAdminStore() {
        ensureOpen();
        return namespaceAdminStore;
    }

    public DefaultBookKeeperProtocolActivationVerifier activationVerifier() {
        ensureOpen();
        return activationVerifier;
    }

    public BookKeeperProtocolActivationStore activationStore() {
        ensureOpen();
        return activationStore;
    }

    public BookKeeperBrokerReadinessProvider brokerReadiness() {
        ensureOpen();
        return brokerReadiness;
    }

    public BookKeeperClientOperations operations() {
        ensureOpen();
        return operations;
    }

    public BookKeeperPasswordProvider passwords() {
        ensureOpen();
        return passwords;
    }

    public Clock clock() {
        ensureOpen();
        return clock;
    }

    public BookKeeperWalRuntime walRuntime() {
        ensureOpen();
        return walRuntime;
    }

    public BookKeeperPrimaryPhysicalReferenceAdapter physicalReferences() {
        ensureOpen();
        return physicalReferences;
    }

    public MaterializationSourceProvider materializationSourceProvider() {
        ensureOpen();
        return walRuntime.materializationSourceProvider(sourceProtections);
    }

    /**
     * Creates the single provider-neutral whole-ledger collector for this runtime.
     *
     * <p>The returned service borrows this runtime, the shared materialization graph and both
     * executors. Callers must start it after materialization starts and close it before any borrowed
     * dependency. Disabled and dry-run configurations deliberately create no scanner and perform no
     * reference mutation.
     */
    public Optional<BookKeeperLedgerRetentionService> createRetentionService(
            BookKeeperLedgerGcConfiguration gcConfiguration,
            OxiaMetadataStore l0,
            CommittedGenerationRetirementAuthority committedGenerations,
            MaterializationStreamTrigger materializationTrigger,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor) {
        ensureOpen();
        BookKeeperLedgerGcConfiguration exactGc =
                Objects.requireNonNull(gcConfiguration, "gcConfiguration");
        exactGc.validateAgainst(configuration);
        if (!exactGc.enabled() || exactGc.dryRun()) {
            return Optional.empty();
        }
        BookKeeperWalOnlyRetirementAuthority commonAuthority =
                new BookKeeperWalOnlyRetirementAuthority(
                        cluster,
                        Objects.requireNonNull(l0, "l0"),
                        metadata);
        BookKeeperAsyncObjectRetirementAuthority retirementAuthority =
                new BookKeeperAsyncObjectRetirementAuthority(
                        cluster,
                        configuration,
                        commonAuthority,
                        Objects.requireNonNull(
                                committedGenerations,
                                "committedGenerations"),
                        metadata);
        BookKeeperWalReferenceManager referenceManager =
                new BookKeeperWalReferenceManager(
                        cluster,
                        configuration,
                        metadata,
                        retirementAuthority);
        BookKeeperWalOnlyReferenceRetirementCoordinator referenceRetirement =
                new BookKeeperWalOnlyReferenceRetirementCoordinator(
                        cluster,
                        configuration,
                        metadata,
                        retirementAuthority,
                        referenceManager);
        BookKeeperSealedLedgerMaterializationTrigger sealedTrigger =
                new BookKeeperSealedLedgerMaterializationTrigger(
                        cluster,
                        configuration,
                        metadata,
                        Objects.requireNonNull(
                                materializationTrigger,
                                "materializationTrigger"));
        BookKeeperWalRetentionGate gate =
                new BookKeeperWalRetentionGate(
                        cluster,
                        configuration,
                        exactGc,
                        metadata,
                        metadata,
                        namespaceVerifier,
                        activationVerifier,
                        operations,
                        clock);
        BookKeeperLedgerRetentionManager manager =
                new BookKeeperLedgerRetentionManager(
                        cluster,
                        configuration,
                        exactGc,
                        metadata,
                        namespaceVerifier,
                        activationVerifier,
                        operations,
                        gate,
                        clock);
        BookKeeperLedgerRetentionScanner scanner =
                new BookKeeperLedgerRetentionScanner(
                        cluster,
                        configuration,
                        exactGc,
                        namespace.ledgerIdNamespaceSha256().value(),
                        metadata,
                        sealedTrigger,
                        referenceRetirement,
                        gate,
                        manager);
        return Optional.of(
                new BookKeeperLedgerRetentionService(
                        scanner,
                        configuration.retentionScanInterval(),
                        configuration.operationTimeout(),
                        Objects.requireNonNull(scheduler, "scheduler"),
                        Objects.requireNonNull(
                                callbackExecutor,
                                "callbackExecutor")));
    }

    public Optional<BookKeeperPrimaryWalRuntimeCapabilityBinding> capabilityBinding() {
        ensureOpen();
        return capabilityBinding;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = null;
        try {
            walRuntime.close();
        } catch (Throwable closeFailure) {
            failure = closeFailure;
        }
        try {
            metadata.close();
        } catch (Throwable closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(
                    "failed to close BookKeeper primary-WAL runtime", failure);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("BookKeeper primary-WAL runtime is closed");
        }
    }

    private static BookKeeperLedgerHandleCache handleCache(
            BookKeeperWalConfiguration configuration) {
        long estimatedBytes = Math.min(
                ESTIMATED_READ_HANDLE_BYTES,
                configuration.maxReadBytesInFlight());
        int byteBoundHandles = Math.toIntExact(Math.min(
                Integer.MAX_VALUE,
                configuration.maxReadBytesInFlight() / estimatedBytes));
        int maxHandles = Math.max(
                1,
                Math.min(configuration.maxReadsInFlight(), byteBoundHandles));
        return new BookKeeperLedgerHandleCache(
                maxHandles,
                configuration.maxReadBytesInFlight(),
                estimatedBytes,
                configuration.readerLeaseTtl());
    }

    private static String text(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return exact;
    }
}
