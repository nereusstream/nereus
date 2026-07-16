/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StreamStorage;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.managedledger.cursor.CursorProtocolActivationGuard;
import com.nereusstream.managedledger.cursor.CursorRetentionCoordinator;
import com.nereusstream.managedledger.cursor.CursorSnapshotStore;
import com.nereusstream.managedledger.cursor.CursorStorage;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.generation.ManagedLedgerMaterializationRegistrationCoordinator;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreProvider;
import io.oxia.client.api.SyncOxiaClient;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class NereusManagedLedgerRuntimeTest {
    private static final String PROCESS_RUN_ID = "ABCDEFGHIJKLMNOPQRSTUV";

    @Test
    void closesEveryOwnedRoleOnceInDependencyReverseOrder() throws Exception {
        List<String> closes = new ArrayList<>();
        AtomicInteger sharedClientCloses = new AtomicInteger();
        SharedOxiaClientRuntime shared = sharedRuntime(sharedClientCloses);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ExecutorService callbacks = Executors.newSingleThreadExecutor();
        NereusManagedLedgerRuntime runtime = new NereusManagedLedgerRuntime(
                proxy(StreamStorage.class, "stream", closes, false),
                proxy(ManagedLedgerProjectionMetadataStore.class, "projection", closes, false),
                proxy(GenerationMetadataStore.class, "generation", closes, false),
                allowRegistration(),
                proxy(CursorMetadataStore.class, "cursor-metadata", closes, false),
                proxy(CursorSnapshotStore.class, "cursor-snapshot", closes, false),
                proxy(CursorRetentionCoordinator.class, "cursor-retention", closes, false),
                proxy(CursorStorage.class, "cursor-storage", closes, false),
                CursorStorageConfig.defaults(),
                allowActivation(),
                proxy(OxiaMetadataStore.class, "l0", closes, false),
                shared,
                proxy(ObjectStore.class, "object", closes, false),
                proxy(ObjectStoreProvider.class, "provider", closes, false),
                scheduler,
                callbacks,
                NereusManagedLedgerFactoryConfig.defaults(1024),
                "cluster/a",
                PROCESS_RUN_ID,
                "pulsar-f2/" + PROCESS_RUN_ID);

        assertThat(runtime.tryAcquireCallbackPermit()).isTrue();
        runtime.releaseCallbackPermit();
        runtime.close();
        runtime.close();

        assertThat(closes).containsExactly(
                "cursor-storage",
                "cursor-retention",
                "cursor-snapshot",
                "cursor-metadata",
                "generation",
                "projection",
                "stream",
                "l0",
                "object",
                "provider");
        assertThat(sharedClientCloses).hasValue(1);
        assertThat(callbacks.isShutdown()).isTrue();
        assertThat(scheduler.isShutdown()).isTrue();
        assertThat(runtime.isClosed()).isTrue();
    }

    @Test
    void exposesAndClosesTheF4ReadPinProtectionAndPhysicalStoreTriplet() throws Exception {
        List<String> closes = new ArrayList<>();
        AtomicInteger sharedClientCloses = new AtomicInteger();
        SharedOxiaClientRuntime shared = sharedRuntime(sharedClientCloses);
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        ExecutorService callbacks = Executors.newSingleThreadExecutor();
        ObjectReadPinManager readPins =
                proxy(ObjectReadPinManager.class, "read-pins", closes, false);
        NereusManagedLedgerRuntime runtime = new NereusManagedLedgerRuntime(
                proxy(StreamStorage.class, "stream", closes, false),
                proxy(ManagedLedgerProjectionMetadataStore.class, "projection", closes, false),
                proxy(GenerationMetadataStore.class, "generation", closes, false),
                allowRegistration(),
                proxy(CursorMetadataStore.class, "cursor-metadata", closes, false),
                proxy(CursorSnapshotStore.class, "cursor-snapshot", closes, false),
                proxy(CursorRetentionCoordinator.class, "cursor-retention", closes, false),
                proxy(CursorStorage.class, "cursor-storage", closes, false),
                CursorStorageConfig.defaults(),
                allowActivation(),
                readPins,
                proxy(AutoCloseable.class, "protection", closes, false),
                proxy(AutoCloseable.class, "physical", closes, false),
                proxy(OxiaMetadataStore.class, "l0", closes, false),
                shared,
                proxy(ObjectStore.class, "object", closes, false),
                proxy(ObjectStoreProvider.class, "provider", closes, false),
                scheduler,
                callbacks,
                NereusManagedLedgerFactoryConfig.defaults(1024),
                "cluster/a",
                PROCESS_RUN_ID,
                "pulsar-f2/" + PROCESS_RUN_ID);

        assertThat(runtime.objectReadPinManager()).isSameAs(readPins);
        runtime.close();

        assertThat(closes).containsExactly(
                "cursor-storage",
                "cursor-retention",
                "cursor-snapshot",
                "cursor-metadata",
                "generation",
                "projection",
                "stream",
                "read-pins",
                "protection",
                "physical",
                "l0",
                "object",
                "provider");
        assertThat(sharedClientCloses).hasValue(1);
    }

    @Test
    void aggregatesCloseFailuresAndStillClosesLaterResources() throws Exception {
        List<String> closes = new ArrayList<>();
        AtomicInteger sharedClientCloses = new AtomicInteger();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ExecutorService callbacks = Executors.newSingleThreadExecutor();
        NereusManagedLedgerRuntime runtime = new NereusManagedLedgerRuntime(
                proxy(StreamStorage.class, "stream", closes, true),
                proxy(ManagedLedgerProjectionMetadataStore.class, "projection", closes, true),
                proxy(GenerationMetadataStore.class, "generation", closes, false),
                allowRegistration(),
                proxy(CursorMetadataStore.class, "cursor-metadata", closes, false),
                proxy(CursorSnapshotStore.class, "cursor-snapshot", closes, false),
                proxy(CursorRetentionCoordinator.class, "cursor-retention", closes, false),
                proxy(CursorStorage.class, "cursor-storage", closes, false),
                CursorStorageConfig.defaults(),
                allowActivation(),
                proxy(OxiaMetadataStore.class, "l0", closes, false),
                sharedRuntime(sharedClientCloses),
                proxy(ObjectStore.class, "object", closes, false),
                proxy(ObjectStoreProvider.class, "provider", closes, false),
                scheduler,
                callbacks,
                NereusManagedLedgerFactoryConfig.defaults(1024),
                "cluster/a",
                PROCESS_RUN_ID,
                "pulsar-f2/" + PROCESS_RUN_ID);

        assertThatThrownBy(runtime::close)
                .isInstanceOf(RuntimeException.class)
                .satisfies(error -> assertThat(error.getSuppressed()).hasSize(2));

        assertThat(closes).containsExactly(
                "cursor-storage",
                "cursor-retention",
                "cursor-snapshot",
                "cursor-metadata",
                "generation",
                "projection",
                "stream",
                "l0",
                "object",
                "provider");
        assertThat(sharedClientCloses).hasValue(1);
        assertThat(callbacks.isShutdown()).isTrue();
        assertThat(scheduler.isShutdown()).isTrue();
    }

    @Test
    void rejectsWeakProcessIdentityBeforeOwnershipTransfer() throws Exception {
        List<String> closes = new ArrayList<>();
        SharedOxiaClientRuntime shared = sharedRuntime(new AtomicInteger());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ExecutorService callbacks = Executors.newSingleThreadExecutor();
        try {
            assertThatThrownBy(() -> new NereusManagedLedgerRuntime(
                            proxy(StreamStorage.class, "stream", closes, false),
                            proxy(ManagedLedgerProjectionMetadataStore.class, "projection", closes, false),
                            proxy(GenerationMetadataStore.class, "generation", closes, false),
                            allowRegistration(),
                            proxy(CursorMetadataStore.class, "cursor-metadata", closes, false),
                            proxy(CursorSnapshotStore.class, "cursor-snapshot", closes, false),
                            proxy(CursorRetentionCoordinator.class, "cursor-retention", closes, false),
                            proxy(CursorStorage.class, "cursor-storage", closes, false),
                            CursorStorageConfig.defaults(),
                            allowActivation(),
                            proxy(OxiaMetadataStore.class, "l0", closes, false),
                            shared,
                            proxy(ObjectStore.class, "object", closes, false),
                            proxy(ObjectStoreProvider.class, "provider", closes, false),
                            scheduler,
                            callbacks,
                            NereusManagedLedgerFactoryConfig.defaults(1024),
                            "cluster/a",
                            "too-short",
                            "pulsar-f2/too-short"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("128-bit");
            assertThat(closes).isEmpty();
        } finally {
            shared.close();
            callbacks.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(
            Class<T> type,
            String name,
            List<String> closes,
            boolean failClose) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (instance, method, arguments) -> {
                    if ("close".equals(method.getName())) {
                        closes.add(name);
                        if (failClose) {
                            throw new IllegalStateException(name);
                        }
                        return null;
                    }
                    if ("toString".equals(method.getName())) {
                        return name;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static SharedOxiaClientRuntime sharedRuntime(AtomicInteger closes) throws Exception {
        SyncOxiaClient client = (SyncOxiaClient) Proxy.newProxyInstance(
                SyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {SyncOxiaClient.class},
                (instance, method, arguments) -> switch (method.getName()) {
                    case "notifications" -> null;
                    case "close" -> {
                        closes.incrementAndGet();
                        yield null;
                    }
                    case "toString" -> "runtime-test-client";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        Method factory = SharedOxiaClientRuntime.class.getDeclaredMethod(
                "usingClient", OxiaClientConfiguration.class, SyncOxiaClient.class, Clock.class);
        factory.setAccessible(true);
        return (SharedOxiaClientRuntime) factory.invoke(
                null, OxiaClientConfiguration.defaults("unused:6648"), client, Clock.systemUTC());
    }

    private static CursorProtocolActivationGuard allowActivation() {
        return ledger -> CompletableFuture.completedFuture(null);
    }

    private static ManagedLedgerMaterializationRegistrationCoordinator
            allowRegistration() {
        return (name, identity) ->
                CompletableFuture.completedFuture(null);
    }
}
