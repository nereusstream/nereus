/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.bookkeeper.BookKeeperAsyncObjectRetirementAuthority;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadinessProvider;
import com.nereusstream.bookkeeper.BookKeeperDeletionActivationCoordinator;
import com.nereusstream.bookkeeper.BookKeeperLedgerGcConfiguration;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceProvisioningCoordinator;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationAdminStore;
import com.nereusstream.bookkeeper.BookKeeperLedgerRetentionManager;
import com.nereusstream.bookkeeper.BookKeeperLedgerRetentionScanner;
import com.nereusstream.bookkeeper.BookKeeperLedgerRetentionService;
import com.nereusstream.bookkeeper.BookKeeperPasswordProvider;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalRuntime;
import com.nereusstream.bookkeeper.BookKeeperPrimaryPhysicalReferenceAdapter;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationCoordinator;
import com.nereusstream.bookkeeper.BookKeeperRootCoverageProofProducer;
import com.nereusstream.bookkeeper.BookKeeperScopeCapabilityProbe;
import com.nereusstream.bookkeeper.BookKeeperSealedLedgerMaterializationTrigger;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.bookkeeper.BookKeeperWalOnlyReferenceRetirementCoordinator;
import com.nereusstream.bookkeeper.BookKeeperWalOnlyRetirementAuthority;
import com.nereusstream.bookkeeper.BookKeeperWalReferenceManager;
import com.nereusstream.bookkeeper.BookKeeperWalRetentionGate;
import com.nereusstream.bookkeeper.BookKeeperWalRuntime;
import com.nereusstream.bookkeeper.OxiaBookKeeperProtocolActivationStore;
import com.nereusstream.materialization.CommittedGenerationRetirementAuthority;
import com.nereusstream.materialization.MaterializationSourceProvider;
import com.nereusstream.materialization.MaterializationStreamTrigger;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.BookKeeper;

/** Owns Nereus BookKeeper adapters and metadata views, but never the borrowed broker client. */
final class ProductionBookKeeperPrimaryWalRuntime implements AutoCloseable {
    private final NereusBookKeeperRuntimeConfiguration runtimeConfiguration;
    private final BookKeeperLedgerGcConfiguration gcConfiguration;
    private final BookKeeperPrimaryWalRuntime runtime;
    private final Optional<BookKeeperPrimaryWalCapabilityBinding> capabilityBinding;
    private final AtomicBoolean closed = new AtomicBoolean();

    static ProductionBookKeeperPrimaryWalRuntime create(
            NereusBookKeeperRuntimeConfiguration configuration,
            String cluster,
            String processRunId,
            OxiaClientConfiguration oxia,
            SharedOxiaClientRuntime sharedOxia,
            BookKeeper borrowedClient,
            BookKeeperLedgerIdNamespaceReservationAdminStore namespaceReservations,
            BookKeeperBrokerReadinessProvider brokerReadiness,
            ObjectStoreSecretResolver secrets,
            Clock clock) {
        NereusBookKeeperRuntimeConfiguration exact = Objects.requireNonNull(
                configuration, "configuration");
        BookKeeperPrimaryWalRuntime runtime = BookKeeperPrimaryWalRuntime.create(
                exact.deploymentId(),
                cluster,
                processRunId,
                exact.wal(),
                Objects.requireNonNull(oxia, "oxia"),
                Objects.requireNonNull(sharedOxia, "sharedOxia"),
                Objects.requireNonNull(borrowedClient, "borrowedClient"),
                Objects.requireNonNull(namespaceReservations, "namespaceReservations"),
                new OxiaBookKeeperProtocolActivationStore(oxia, sharedOxia),
                Objects.requireNonNull(brokerReadiness, "brokerReadiness"),
                passwordProvider(Objects.requireNonNull(secrets, "secrets")),
                Objects.requireNonNull(clock, "clock"));
        return new ProductionBookKeeperPrimaryWalRuntime(
                exact,
                runtime,
                runtime.capabilityBinding().map(binding ->
                        new BookKeeperPrimaryWalCapabilityBinding(
                                binding.protocolVersion(),
                                binding.configurationBindingSha256(),
                                binding.ledgerIdNamespaceSha256(),
                                binding.publicationActivationSha256(),
                                binding.requiredObjectGenerationCompletionVersion())));
    }

    private ProductionBookKeeperPrimaryWalRuntime(
            NereusBookKeeperRuntimeConfiguration runtimeConfiguration,
            BookKeeperPrimaryWalRuntime runtime,
            Optional<BookKeeperPrimaryWalCapabilityBinding> capabilityBinding) {
        this.runtimeConfiguration = Objects.requireNonNull(
                runtimeConfiguration, "runtimeConfiguration");
        this.gcConfiguration = runtimeConfiguration.ledgerGc();
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.capabilityBinding = Objects.requireNonNull(capabilityBinding, "capabilityBinding");
    }

    BookKeeperWalRuntime walRuntime() {
        ensureOpen();
        return runtime.walRuntime();
    }

    BookKeeperPrimaryPhysicalReferenceAdapter physicalReferences() {
        ensureOpen();
        return runtime.physicalReferences();
    }

    MaterializationSourceProvider materializationSourceProvider() {
        ensureOpen();
        return runtime.materializationSourceProvider();
    }

    Optional<BookKeeperPrimaryWalCapabilityBinding> capabilityBinding() {
        ensureOpen();
        return capabilityBinding;
    }

    BookKeeperPrimaryWalAdministration administration(
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            ManagedLedgerProjectionMetadataStore projections) {
        ensureOpen();
        BookKeeperProtocolActivationCoordinator activationCoordinator =
                new BookKeeperProtocolActivationCoordinator(
                        runtime.activationStore(), runtime.clock());
        BookKeeperDeletionActivationCoordinator deletionCoordinator =
                new BookKeeperDeletionActivationCoordinator(
                        runtime.configuration(),
                        gcConfiguration,
                        runtime.namespace(),
                        runtime.namespaceVerifier(),
                        runtime.brokerReadiness(),
                        runtime.activationStore(),
                        activationCoordinator,
                        new BookKeeperRootCoverageProofProducer(
                                runtime.cluster(),
                                runtime.configuration(),
                                runtime.namespace().ledgerIdNamespaceSha256().value(),
                                runtime.metadata()),
                        new BookKeeperStreamCoverageProofProducer(
                                runtime.cluster(),
                                runtime.configuration(),
                                runtime.namespace().ledgerIdNamespaceSha256().value(),
                                Objects.requireNonNull(generations, "generations"),
                                Objects.requireNonNull(l0, "l0"),
                                Objects.requireNonNull(projections, "projections")),
                        new BookKeeperScopeCapabilityProbe(
                                runtime.cluster(),
                                runtime.configuration(),
                                runtime.namespace(),
                                runtime.namespaceVerifier(),
                                runtime.metadata(),
                                runtime.operations(),
                                runtime.passwords(),
                                runtime.clock()));
        return new BookKeeperPrimaryWalAdministration(
                runtimeConfiguration,
                runtime.namespaceVerifier(),
                new BookKeeperLedgerIdNamespaceProvisioningCoordinator(
                        runtime.namespaceAdminStore(), runtime.clock()),
                runtime.activationStore(),
                activationCoordinator,
                Optional.of(deletionCoordinator));
    }

    Optional<BookKeeperLedgerRetentionService> createRetentionService(
            OxiaMetadataStore l0,
            CommittedGenerationRetirementAuthority committedGenerations,
            MaterializationStreamTrigger materializationTrigger,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor) {
        ensureOpen();
        if (!gcConfiguration.enabled() || gcConfiguration.dryRun()) {
            return Optional.empty();
        }
        BookKeeperWalOnlyRetirementAuthority commonAuthority =
                new BookKeeperWalOnlyRetirementAuthority(
                        runtime.cluster(), l0, runtime.metadata());
        BookKeeperAsyncObjectRetirementAuthority retirementAuthority =
                new BookKeeperAsyncObjectRetirementAuthority(
                        runtime.cluster(),
                        runtime.configuration(),
                        commonAuthority,
                        committedGenerations,
                        runtime.metadata());
        BookKeeperWalReferenceManager referenceManager =
                new BookKeeperWalReferenceManager(
                        runtime.cluster(),
                        runtime.configuration(),
                        runtime.metadata(),
                        retirementAuthority);
        BookKeeperWalOnlyReferenceRetirementCoordinator referenceRetirement =
                new BookKeeperWalOnlyReferenceRetirementCoordinator(
                        runtime.cluster(),
                        runtime.configuration(),
                        runtime.metadata(),
                        retirementAuthority,
                        referenceManager);
        BookKeeperSealedLedgerMaterializationTrigger sealedTrigger =
                new BookKeeperSealedLedgerMaterializationTrigger(
                        runtime.cluster(),
                        runtime.configuration(),
                        runtime.metadata(),
                        materializationTrigger);
        BookKeeperWalRetentionGate gate = new BookKeeperWalRetentionGate(
                runtime.cluster(),
                runtime.configuration(),
                gcConfiguration,
                runtime.metadata(),
                runtime.metadata(),
                runtime.namespaceVerifier(),
                runtime.activationVerifier(),
                runtime.operations(),
                runtime.clock());
        BookKeeperLedgerRetentionManager manager =
                new BookKeeperLedgerRetentionManager(
                        runtime.cluster(),
                        runtime.configuration(),
                        gcConfiguration,
                        runtime.metadata(),
                        runtime.namespaceVerifier(),
                        runtime.activationVerifier(),
                        runtime.operations(),
                        gate,
                        runtime.clock());
        BookKeeperLedgerRetentionScanner scanner =
                new BookKeeperLedgerRetentionScanner(
                        runtime.cluster(),
                        runtime.configuration(),
                        gcConfiguration,
                        runtime.namespace().ledgerIdNamespaceSha256().value(),
                        runtime.metadata(),
                        sealedTrigger,
                        referenceRetirement,
                        gate,
                        manager);
        return Optional.of(new BookKeeperLedgerRetentionService(
                scanner,
                configuration.retentionScanInterval(),
                configuration.operationTimeout(),
                scheduler,
                callbackExecutor));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        runtime.close();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("BookKeeper primary-WAL runtime is closed");
        }
    }

    private static BookKeeperPasswordProvider passwordProvider(ObjectStoreSecretResolver secrets) {
        return reference -> {
            char[] characters = secrets.resolve(reference.reference()).orElseThrow(() -> new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "BookKeeper password secret reference is unavailable"));
            ByteBuffer encoded = null;
            try {
                encoded = StandardCharsets.UTF_8.encode(CharBuffer.wrap(characters));
                byte[] password = new byte[encoded.remaining()];
                encoded.get(password);
                return password;
            } finally {
                Arrays.fill(characters, '\0');
                if (encoded != null && encoded.hasArray()) {
                    Arrays.fill(encoded.array(), (byte) 0);
                }
            }
        };
    }
}
