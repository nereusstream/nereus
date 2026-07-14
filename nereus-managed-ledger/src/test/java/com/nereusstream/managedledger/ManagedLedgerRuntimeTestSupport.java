/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.api.StreamStorage;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorOwnerSession;
import com.nereusstream.managedledger.cursor.CursorProtocolActivationGuard;
import com.nereusstream.managedledger.cursor.CursorSnapshotStore;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.cursor.TestCursorRetentionCoordinator;
import com.nereusstream.managedledger.cursor.TestCursorStorage;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreProvider;
import io.oxia.client.api.SyncOxiaClient;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

final class ManagedLedgerRuntimeTestSupport {
    static final String PROCESS_RUN_ID = "ABCDEFGHIJKLMNOPQRSTUV";
    private static final AtomicLong OWNER_IDS = new AtomicLong();

    private ManagedLedgerRuntimeTestSupport() {
    }

    static NereusManagedLedgerRuntime runtime(
            StreamStorage streamStorage,
            ManagedLedgerProjectionMetadataStore projectionStore) {
        try {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            ExecutorService callbacks = Executors.newSingleThreadExecutor();
            CursorStorageConfig cursorConfig = CursorStorageConfig.defaults();
            CursorProtocolActivationGuard activationGuard = ledger ->
                    java.util.concurrent.CompletableFuture.completedFuture(null);
            return new NereusManagedLedgerRuntime(
                    streamStorage,
                    projectionStore,
                    proxy(CursorMetadataStore.class),
                    proxy(CursorSnapshotStore.class),
                    new TestCursorRetentionCoordinator(),
                    new TestCursorStorage(),
                    cursorConfig,
                    activationGuard,
                    proxy(OxiaMetadataStore.class),
                    sharedRuntime(),
                    proxy(ObjectStore.class),
                    proxy(ObjectStoreProvider.class),
                    scheduler,
                    callbacks,
                    NereusManagedLedgerFactoryConfig.defaults(1024),
                    "cluster/a",
                    PROCESS_RUN_ID,
                    "pulsar-f2/" + PROCESS_RUN_ID);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    static NereusWritableLedgerOpenResult writable(
            NereusManagedLedgerRuntime runtime,
            NereusLedgerOpenResult ledger) {
        String name = ledger.topicProjection().managedLedgerName();
        CursorOwnerSession owner = new CursorOwnerSession(
                new CursorLedgerIdentity(
                        name,
                        ManagedLedgerProjectionNames.managedLedgerNameHash(name),
                        ledger.topicProjection().projectionIdentity()),
                String.format("%032x", OWNER_IDS.incrementAndGet()));
        return new NereusWritableLedgerOpenResult(
                ledger,
                owner,
                runtime.cursorStorage().claimAndLoadActiveCursors(owner).join(),
                runtime.cursorStorage().retentionView(owner).join());
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (instance, method, arguments) -> {
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    if ("toString".equals(method.getName())) {
                        return "no-op-" + type.getSimpleName();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static SharedOxiaClientRuntime sharedRuntime() throws ReflectiveOperationException {
        SyncOxiaClient client = (SyncOxiaClient) Proxy.newProxyInstance(
                SyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {SyncOxiaClient.class},
                (instance, method, arguments) -> switch (method.getName()) {
                    case "notifications", "close" -> null;
                    case "toString" -> "open-coordinator-test-client";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        Method factory = SharedOxiaClientRuntime.class.getDeclaredMethod(
                "usingClient", OxiaClientConfiguration.class, SyncOxiaClient.class, Clock.class);
        factory.setAccessible(true);
        return (SharedOxiaClientRuntime) factory.invoke(
                null, OxiaClientConfiguration.defaults("unused:6648"), client, Clock.systemUTC());
    }
}
