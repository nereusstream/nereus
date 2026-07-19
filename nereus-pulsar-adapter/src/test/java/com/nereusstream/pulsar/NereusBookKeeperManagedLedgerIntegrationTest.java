/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.bookkeeper.BookKeeperWalRuntime;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.AppendAdmissionGuard;
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.core.recovery.MetadataAppendRecoverySearcher;
import com.nereusstream.core.trim.TrimMetricsObserver;
import com.nereusstream.managedledger.NereusManagedLedger;
import com.nereusstream.managedledger.NereusManagedLedgerFactory;
import com.nereusstream.managedledger.NereusManagedLedgerFactoryConfig;
import com.nereusstream.managedledger.NereusManagedLedgerRuntime;
import com.nereusstream.managedledger.cursor.CursorOwnerSession;
import com.nereusstream.managedledger.cursor.CursorRetentionCoordinator;
import com.nereusstream.managedledger.cursor.CursorRetentionView;
import com.nereusstream.managedledger.cursor.CursorSnapshotStore;
import com.nereusstream.managedledger.cursor.CursorStorage;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.generation.ManagedLedgerMaterializationRegistrationCoordinator;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.managedledger.integration.NereusCreationPermit;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreProvider;
import io.oxia.client.api.SyncOxiaClient;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.junit.jupiter.api.Test;

class NereusBookKeeperManagedLedgerIntegrationTest {
    private static final String NAME = "tenant/ns/persistent/bk-only";
    private static final String PROCESS_RUN_ID = "ABCDEFGHIJKLMNOPQRSTUV";

    @Test
    void facadePreservesEntryBytesAndVirtualPositionOverBookKeeperGenerationZero() throws Exception {
        try (InMemoryBookKeeperPrimaryWalFixture provider =
                new InMemoryBookKeeperPrimaryWalFixture()) {
            FakeOxiaMetadataStore l0 = l0(provider);
            StreamStorageConfig streamConfig = StreamStorageConfig.defaults(
                    InMemoryBookKeeperPrimaryWalFixture.CLUSTER, "writer-1");
            try (BookKeeperWalRuntime bookKeeper = new BookKeeperWalRuntime(
                            provider.appender, provider.reader, provider.references);
                    DefaultStreamStorage storage = bookKeeper.newGenerationZeroStorage(
                            streamConfig,
                            l0,
                            new MetadataAppendRecoverySearcher(streamConfig.cluster(), l0),
                            AppendAdmissionGuard.noOp(),
                            InMemoryBookKeeperPrimaryWalFixture.CLOCK,
                            Runnable::run,
                            ReadMetricsObserver.noop(),
                            TrimMetricsObserver.noop());
                    NereusManagedLedgerRuntime runtime = managedLedgerRuntime(storage, l0)) {
                NereusManagedLedgerFactory factory = new NereusManagedLedgerFactory(
                        runtime,
                        creationGuard(),
                        managedLedgerConfig(),
                        new ManagedLedgerFactoryConfig(),
                        false);
                try {
                    NereusManagedLedger ledger = (NereusManagedLedger) factory.open(
                            NAME, managedLedgerConfig());
                    byte[] payload = "pulsar-entry-over-bk".getBytes(StandardCharsets.UTF_8);
                    Position position = ledger.addEntry(payload, 2);

                    var resolved = storage.resolve(
                            ledger.projection().streamId(),
                            0,
                            new ResolveOptions(1, true, true))
                            .join();
                    assertThat(resolved.ranges()).singleElement().satisfies(range -> {
                        assertThat(range.readTarget()).isInstanceOf(
                                com.nereusstream.api.target.BookKeeperEntryRangeReadTarget.class);
                        long physicalLedger = ((com.nereusstream.api.target.BookKeeperEntryRangeReadTarget)
                                range.readTarget()).ledgerId();
                        assertThat(position.getLedgerId()).isNotEqualTo(physicalLedger);
                    });
                    assertThat(position.getLedgerId()).isEqualTo(ledger.projection().virtualLedgerId());
                    assertThat(position.getEntryId()).isZero();

                    Entry read = read(ledger, position).join();
                    try {
                        assertThat(read.getData()).containsExactly(payload);
                        assertThat(read.getPosition()).isEqualTo(position);
                    } finally {
                        read.release();
                    }
                    assertThat(provider.operations.recoveryOpenCalls).isZero();
                    assertThat(provider.operations.normalOpenCalls).isOne();
                } finally {
                    factory.shutdown();
                }
            }
        }
    }

    private static FakeOxiaMetadataStore l0(InMemoryBookKeeperPrimaryWalFixture provider) {
        return new FakeOxiaMetadataStore(
                () -> 1_000L,
                provider.metadata,
                new BookKeeperMetadataStoreConfig(
                        provider.configuration.maxAppendRangesPerLedger(),
                        provider.configuration.protectionSlotsPerRange(),
                        provider.configuration.maxReaderLeasesPerLedger(),
                        provider.configuration.maxUncertainAllocations()));
    }

    private static NereusManagedLedgerRuntime managedLedgerRuntime(
            DefaultStreamStorage storage,
            FakeOxiaMetadataStore l0) throws ReflectiveOperationException {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ExecutorService callbacks = Executors.newSingleThreadExecutor();
        CursorStorage cursors = cursorStorage();
        ManagedLedgerMaterializationRegistrationCoordinator registration =
                (name, identity) -> CompletableFuture.completedFuture(null);
        return new NereusManagedLedgerRuntime(
                storage,
                new FakeManagedLedgerProjectionMetadataStore(),
                proxy(GenerationMetadataStore.class),
                registration,
                proxy(CursorMetadataStore.class),
                proxy(CursorSnapshotStore.class),
                proxy(CursorRetentionCoordinator.class),
                cursors,
                CursorStorageConfig.defaults(),
                ledger -> CompletableFuture.completedFuture(null),
                l0,
                sharedRuntime(),
                proxy(ObjectStore.class),
                proxy(ObjectStoreProvider.class),
                scheduler,
                callbacks,
                managedLedgerFactoryConfig(),
                InMemoryBookKeeperPrimaryWalFixture.CLUSTER,
                PROCESS_RUN_ID,
                "pulsar-f2/" + PROCESS_RUN_ID);
    }

    private static CursorStorage cursorStorage() {
        return (CursorStorage) Proxy.newProxyInstance(
                CursorStorage.class.getClassLoader(),
                new Class<?>[] {CursorStorage.class},
                (instance, method, arguments) -> switch (method.getName()) {
                    case "claimAndLoadActiveCursors" -> CompletableFuture.completedFuture(List.of());
                    case "retentionView" -> {
                        CursorOwnerSession owner = (CursorOwnerSession) arguments[0];
                        yield CompletableFuture.completedFuture(new CursorRetentionView(
                                owner.ledger(), owner.ownerSessionId(), CursorRetentionView.Lifecycle.ACTIVE,
                                1, 0, 0, 0, Optional.empty(), Optional.empty()));
                    }
                    case "close" -> null;
                    case "toString" -> "bk-only-cursor-storage";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static NereusManagedLedgerFactoryConfig managedLedgerFactoryConfig() {
        NereusManagedLedgerFactoryConfig defaults = NereusManagedLedgerFactoryConfig.defaults(1024);
        return new NereusManagedLedgerFactoryConfig(
                defaults.storageClassName(),
                defaults.metadataTimeout(),
                defaults.appendTimeout(),
                defaults.appendRecoveryTimeout(),
                defaults.readTimeout(),
                defaults.closeTimeout(),
                defaults.tailPollInterval(),
                defaults.maxEntryBytes(),
                defaults.maxReadEntries(),
                defaults.maxOpenLedgers(),
                defaults.maxPendingCallbacks(),
                defaults.maxRetainedAppendAttempts(),
                defaults.maxScanEntries(),
                StorageProfile.BOOKKEEPER_WAL_ONLY);
    }

    private static ManagedLedgerConfig managedLedgerConfig() {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setStorageClassName("nereus");
        config.setCreateIfMissing(true);
        config.setProperties(Map.of("source", "bk-m2"));
        return config;
    }

    private static NereusCreationGuard creationGuard() {
        return name -> CompletableFuture.completedFuture(new NereusCreationPermit() {
            @Override public String persistenceName() { return name; }
            @Override public long bindingGeneration() { return 1; }
            @Override public CompletableFuture<Void> validateBeforeProjectionPublish() {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private static CompletableFuture<Entry> read(NereusManagedLedger ledger, Position position) {
        CompletableFuture<Entry> result = new CompletableFuture<>();
        ledger.asyncReadEntry(position, new ReadEntryCallback() {
            @Override public void readEntryComplete(Entry entry, Object context) {
                result.complete(entry);
            }
            @Override public void readEntryFailed(ManagedLedgerException exception, Object context) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (instance, method, arguments) -> switch (method.getName()) {
                    case "close" -> null;
                    case "toString" -> "bk-only-no-op-" + type.getSimpleName();
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static SharedOxiaClientRuntime sharedRuntime() throws ReflectiveOperationException {
        SyncOxiaClient client = (SyncOxiaClient) Proxy.newProxyInstance(
                SyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {SyncOxiaClient.class},
                (instance, method, arguments) -> switch (method.getName()) {
                    case "notifications", "close" -> null;
                    case "toString" -> "bk-only-shared-oxia-client";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        Method factory = SharedOxiaClientRuntime.class.getDeclaredMethod(
                "usingClient", OxiaClientConfiguration.class, SyncOxiaClient.class, Clock.class);
        factory.setAccessible(true);
        return (SharedOxiaClientRuntime) factory.invoke(
                null, OxiaClientConfiguration.defaults("unused:6648"), client, Clock.systemUTC());
    }
}
