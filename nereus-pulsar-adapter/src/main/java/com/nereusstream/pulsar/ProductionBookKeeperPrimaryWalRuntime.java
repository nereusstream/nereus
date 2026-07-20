/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.bookkeeper.BookKeeperClientOperations;
import com.nereusstream.bookkeeper.BookKeeperLedgerAllocator;
import com.nereusstream.bookkeeper.BookKeeperLedgerHandleCache;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationStore;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationVerifier;
import com.nereusstream.bookkeeper.BookKeeperLedgerRecovery;
import com.nereusstream.bookkeeper.BookKeeperMaterializationSourceProtectionAdapter;
import com.nereusstream.bookkeeper.BookKeeperPasswordProvider;
import com.nereusstream.bookkeeper.BookKeeperPrimaryPhysicalReferenceAdapter;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalAppender;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalReader;
import com.nereusstream.bookkeeper.BookKeeperReaderLeaseManager;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.bookkeeper.BookKeeperWalRuntime;
import com.nereusstream.bookkeeper.BookKeeperWriterStateMachine;
import com.nereusstream.bookkeeper.DefaultBookKeeperClientOperations;
import com.nereusstream.materialization.MaterializationSourceProvider;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.BookKeeper;

/** Owns Nereus BookKeeper adapters and metadata views, but never the borrowed broker client. */
final class ProductionBookKeeperPrimaryWalRuntime implements AutoCloseable {
    private static final long ESTIMATED_READ_HANDLE_BYTES = 64L * 1024L;

    private final OxiaJavaBookKeeperMetadataStore metadata;
    private final BookKeeperWalRuntime walRuntime;
    private final BookKeeperPrimaryPhysicalReferenceAdapter physicalReferences;
    private final BookKeeperMaterializationSourceProtectionAdapter sourceProtections;
    private final BookKeeperPrimaryWalCapabilityBinding capabilityBinding;
    private final AtomicBoolean closed = new AtomicBoolean();

    static ProductionBookKeeperPrimaryWalRuntime create(
            NereusBookKeeperRuntimeConfiguration configuration,
            String cluster,
            String processRunId,
            OxiaClientConfiguration oxia,
            SharedOxiaClientRuntime sharedOxia,
            BookKeeper borrowedClient,
            BookKeeperLedgerIdNamespaceReservationStore namespaceReservations,
            ObjectStoreSecretResolver secrets,
            Clock clock) {
        NereusBookKeeperRuntimeConfiguration exact = Objects.requireNonNull(
                configuration, "configuration");
        BookKeeperWalConfiguration wal = exact.wal();
        BookKeeperMetadataStoreConfig metadataConfiguration = exact.metadataStore();
        OxiaJavaBookKeeperMetadataStore metadata =
                OxiaJavaBookKeeperMetadataStore.usingSharedRuntime(
                        Objects.requireNonNull(oxia, "oxia"),
                        Objects.requireNonNull(sharedOxia, "sharedOxia"),
                        Objects.requireNonNull(clock, "clock"),
                        metadataConfiguration);
        try {
            BookKeeperPasswordProvider passwords = passwordProvider(
                    Objects.requireNonNull(secrets, "secrets"));
            byte[] passwordProbe = passwords.resolve(wal.passwordRef());
            Arrays.fill(passwordProbe, (byte) 0);
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier =
                    new BookKeeperLedgerIdNamespaceReservationVerifier(
                            Objects.requireNonNull(namespaceReservations, "namespaceReservations"),
                            exact.deploymentId());
            BookKeeperLedgerIdNamespaceReservation namespace = namespaceVerifier
                    .requireActive(wal, wal.operationTimeout())
                    .join();
            BookKeeperClientOperations operations = new DefaultBookKeeperClientOperations(
                    Objects.requireNonNull(borrowedClient, "borrowedClient"));
            BookKeeperWriterStateMachine writer = new BookKeeperWriterStateMachine(
                    cluster,
                    wal,
                    metadata,
                    clock,
                    processRunId);
            BookKeeperLedgerAllocator allocator = new BookKeeperLedgerAllocator(
                    cluster,
                    wal,
                    metadata,
                    metadata,
                    namespaceVerifier,
                    operations,
                    passwords,
                    writer,
                    clock);
            BookKeeperLedgerRecovery recovery = new BookKeeperLedgerRecovery(
                    cluster,
                    wal,
                    metadata,
                    metadata,
                    namespaceVerifier,
                    operations,
                    passwords,
                    writer,
                    clock);
            BookKeeperPrimaryWalAppender appender = new BookKeeperPrimaryWalAppender(
                    cluster,
                    wal,
                    metadata,
                    metadata,
                    allocator,
                    recovery,
                    writer,
                    operations,
                    clock);
            BookKeeperPrimaryWalReader reader = new BookKeeperPrimaryWalReader(
                    cluster,
                    wal,
                    metadata,
                    operations,
                    passwords,
                    handleCache(wal),
                    new BookKeeperReaderLeaseManager(
                            cluster,
                            wal,
                            metadata,
                            clock,
                            processRunId));
            BookKeeperPrimaryPhysicalReferenceAdapter references =
                    new BookKeeperPrimaryPhysicalReferenceAdapter(
                            cluster,
                            wal,
                            metadata,
                            metadata,
                            clock);
            return new ProductionBookKeeperPrimaryWalRuntime(
                    metadata,
                    new BookKeeperWalRuntime(appender, reader, references),
                    references,
                    new BookKeeperMaterializationSourceProtectionAdapter(
                            cluster,
                            wal,
                            metadata,
                            clock),
                    new BookKeeperPrimaryWalCapabilityBinding(
                            1,
                            wal.configurationBindingSha256(),
                            namespace.ledgerIdNamespaceSha256(),
                            1));
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
            throw new IllegalStateException("failed to create BookKeeper primary-WAL runtime", failure);
        }
    }

    private ProductionBookKeeperPrimaryWalRuntime(
            OxiaJavaBookKeeperMetadataStore metadata,
            BookKeeperWalRuntime walRuntime,
            BookKeeperPrimaryPhysicalReferenceAdapter physicalReferences,
            BookKeeperMaterializationSourceProtectionAdapter sourceProtections,
            BookKeeperPrimaryWalCapabilityBinding capabilityBinding) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.walRuntime = Objects.requireNonNull(walRuntime, "walRuntime");
        this.physicalReferences = Objects.requireNonNull(physicalReferences, "physicalReferences");
        this.sourceProtections = Objects.requireNonNull(sourceProtections, "sourceProtections");
        this.capabilityBinding = Objects.requireNonNull(capabilityBinding, "capabilityBinding");
    }

    BookKeeperWalRuntime walRuntime() {
        ensureOpen();
        return walRuntime;
    }

    BookKeeperPrimaryPhysicalReferenceAdapter physicalReferences() {
        ensureOpen();
        return physicalReferences;
    }

    MaterializationSourceProvider materializationSourceProvider() {
        ensureOpen();
        return walRuntime.materializationSourceProvider(sourceProtections);
    }

    BookKeeperPrimaryWalCapabilityBinding capabilityBinding() {
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
            throw new IllegalStateException("failed to close BookKeeper primary-WAL runtime", failure);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("BookKeeper primary-WAL runtime is closed");
        }
    }

    private static BookKeeperLedgerHandleCache handleCache(BookKeeperWalConfiguration configuration) {
        long estimatedBytes = Math.min(
                ESTIMATED_READ_HANDLE_BYTES,
                configuration.maxReadBytesInFlight());
        int byteBoundHandles = Math.toIntExact(Math.min(
                Integer.MAX_VALUE,
                configuration.maxReadBytesInFlight() / estimatedBytes));
        int maxHandles = Math.max(1, Math.min(
                configuration.maxReadsInFlight(),
                byteBoundHandles));
        return new BookKeeperLedgerHandleCache(
                maxHandles,
                configuration.maxReadBytesInFlight(),
                estimatedBytes,
                configuration.readerLeaseTtl());
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
