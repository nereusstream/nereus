/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorProtocolActivationGuard;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DefaultNereusRuntimeProviderCursorTest {
    @Test
    void providerPreservesCanonicalActivationGuardIdentity() {
        AtomicInteger permits = new AtomicInteger();
        CursorProtocolActivationGuard guard = ledger -> {
            permits.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        };
        NereusRuntimeContext context = new NereusRuntimeContext(
                eventLoopGroup(),
                OpenTelemetry.noop(),
                creationGuard(),
                guard,
                reference -> Optional.empty(),
                getClass().getClassLoader());

        CursorProtocolActivationGuard selected =
                DefaultNereusRuntimeProvider.cursorProtocolActivationGuard(context);
        assertThat(selected).isSameAs(guard);
        assertThat(context.generationProtocolActivationEnabled()).isFalse();
        selected.acquireFirstActivationPermit(ledger()).join();
        assertThat(permits).hasValue(1);
    }

    @Test
    void canonicalContextCarriesTheExplicitGenerationActivationSwitch() {
        NereusRuntimeContext context = new NereusRuntimeContext(
                eventLoopGroup(),
                OpenTelemetry.noop(),
                creationGuard(),
                CursorProtocolActivationGuard.unavailable(),
                GenerationCapabilityReadinessProvider.unavailable(),
                true,
                reference -> Optional.empty(),
                getClass().getClassLoader());

        assertThat(context.generationProtocolActivationEnabled()).isTrue();
    }

    @Test
    void legacyContextBridgeIsNamedFailClosedSentinel() {
        NereusRuntimeContext context = new NereusRuntimeContext(
                eventLoopGroup(),
                OpenTelemetry.noop(),
                creationGuard(),
                reference -> Optional.empty(),
                getClass().getClassLoader());

        CompletableFuture<Void> permit = DefaultNereusRuntimeProvider
                .cursorProtocolActivationGuard(context)
                .acquireFirstActivationPermit(ledger());
        assertThatThrownBy(permit::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NEREUS_CURSOR_CAPABILITY_NOT_READY");
    }

    private static NereusCreationGuard creationGuard() {
        return name -> CompletableFuture.failedFuture(
                new AssertionError("creation guard is not used by this wiring test"));
    }

    private static EventLoopGroup eventLoopGroup() {
        return (EventLoopGroup) Proxy.newProxyInstance(
                EventLoopGroup.class.getClassLoader(),
                new Class<?>[] {EventLoopGroup.class},
                (instance, method, arguments) -> {
                    throw new AssertionError("event loop is not used by this wiring test");
                });
    }

    private static CursorLedgerIdentity ledger() {
        String name = "tenant/ns/persistent/provider-cursor";
        return new CursorLedgerIdentity(
                name,
                ManagedLedgerProjectionNames.managedLedgerNameHash(name),
                new ManagedLedgerProjectionIdentity(
                        1,
                        1,
                        ManagedLedgerProjectionNames.streamId(name, 1).value(),
                        ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID));
    }
}
