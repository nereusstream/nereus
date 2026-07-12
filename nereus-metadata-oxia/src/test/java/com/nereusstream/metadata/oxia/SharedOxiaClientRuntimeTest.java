/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.client.api.SyncOxiaClient;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SharedOxiaClientRuntimeTest {
    @Test
    void adaptersCloseLocallyAndTheRuntimeClosesTheClientExactlyOnce() {
        AtomicInteger clientCloses = new AtomicInteger();
        OxiaClientConfiguration config = OxiaClientConfiguration.defaults("unused:6648");
        SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.usingClient(
                config, emptyClient(clientCloses), Clock.systemUTC());
        OxiaJavaClientMetadataStore l0 =
                OxiaJavaClientMetadataStore.usingSharedRuntime(config, runtime, Clock.systemUTC());
        ManagedLedgerProjectionMetadataStore projection =
                ManagedLedgerProjectionMetadataStore.usingSharedRuntime(
                        config, runtime, ProjectionMetadataStoreConfig.defaults(), Clock.systemUTC());

        l0.close();
        assertThat(clientCloses).hasValue(0);
        assertThat(projection.getProjection("cluster", "tenant/ns/topic").join()).isEmpty();
        projection.close();
        assertThat(clientCloses).hasValue(0);
        assertThat(runtime.isClosed()).isFalse();

        runtime.close();
        runtime.close();
        assertThat(clientCloses).hasValue(1);
        assertThat(runtime.isClosed()).isTrue();
    }

    @Test
    void legacyConstructorRetainsOwningCloseBehavior() {
        AtomicInteger clientCloses = new AtomicInteger();
        OxiaJavaClientMetadataStore store = new OxiaJavaClientMetadataStore(
                OxiaClientConfiguration.defaults("unused:6648"),
                emptyClient(clientCloses),
                Clock.systemUTC());

        store.close();
        store.close();

        assertThat(clientCloses).hasValue(1);
    }

    @Test
    void sharedRuntimeRejectsAdapterConfigurationDrift() {
        AtomicInteger clientCloses = new AtomicInteger();
        OxiaClientConfiguration config = OxiaClientConfiguration.defaults("unused:6648");
        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.usingClient(
                config, emptyClient(clientCloses), Clock.systemUTC())) {
            OxiaClientConfiguration drifted = new OxiaClientConfiguration(
                    config.serviceAddress(), config.namespace(), config.requestTimeout(), config.sessionTimeout(),
                    config.maxCommitChainScan() + 1, config.maxPendingOperations());

            assertThatThrownBy(() -> ManagedLedgerProjectionMetadataStore.usingSharedRuntime(
                            drifted, runtime, ProjectionMetadataStoreConfig.defaults(), Clock.systemUTC()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("does not match");
        }
        assertThat(clientCloses).hasValue(1);
    }

    private static SyncOxiaClient emptyClient(AtomicInteger closes) {
        return (SyncOxiaClient) Proxy.newProxyInstance(
                SyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {SyncOxiaClient.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "notifications" -> {
                        yield null;
                    }
                    case "get" -> null;
                    case "close" -> {
                        closes.incrementAndGet();
                        yield null;
                    }
                    case "toString" -> "SharedOxiaClientRuntimeTestClient";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
