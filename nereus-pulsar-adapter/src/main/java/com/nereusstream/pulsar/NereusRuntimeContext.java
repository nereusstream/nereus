/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.bookkeeper.BookKeeperBrokerReadinessProvider;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.managedledger.cursor.CursorProtocolActivationGuard;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Objects;
import java.util.Optional;
import org.apache.bookkeeper.client.api.BookKeeper;

/** Borrowed broker resources. The Nereus runtime never closes any value in this record. */
public record NereusRuntimeContext(
        EventLoopGroup eventLoopGroup,
        OpenTelemetry openTelemetry,
        NereusCreationGuard creationGuard,
        CursorProtocolActivationGuard cursorProtocolActivationGuard,
        GenerationCapabilityReadinessProvider generationCapabilityReadinessProvider,
        boolean generationProtocolActivationEnabled,
        ObjectStoreSecretResolver secretResolver,
        ClassLoader pluginClassLoader,
        Optional<BookKeeper> borrowedBookKeeperClient,
        BookKeeperBrokerReadinessProvider bookKeeperBrokerReadinessProvider,
        BookKeeperPrimaryWalCapabilitySink bookKeeperPrimaryWalCapabilitySink,
        BookKeeperPrimaryWalAdministrationSink bookKeeperPrimaryWalAdministrationSink) {
    public NereusRuntimeContext(
            EventLoopGroup eventLoopGroup,
            OpenTelemetry openTelemetry,
            NereusCreationGuard creationGuard,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            GenerationCapabilityReadinessProvider generationCapabilityReadinessProvider,
            boolean generationProtocolActivationEnabled,
            ObjectStoreSecretResolver secretResolver,
            ClassLoader pluginClassLoader,
            Optional<BookKeeper> borrowedBookKeeperClient) {
        this(
                eventLoopGroup,
                openTelemetry,
                creationGuard,
                cursorProtocolActivationGuard,
                generationCapabilityReadinessProvider,
                generationProtocolActivationEnabled,
                secretResolver,
                pluginClassLoader,
                borrowedBookKeeperClient,
                BookKeeperBrokerReadinessProvider.unavailable(),
                BookKeeperPrimaryWalCapabilitySink.unavailable(),
                BookKeeperPrimaryWalAdministrationSink.unavailable());
    }

    public NereusRuntimeContext(
            EventLoopGroup eventLoopGroup,
            OpenTelemetry openTelemetry,
            NereusCreationGuard creationGuard,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            GenerationCapabilityReadinessProvider generationCapabilityReadinessProvider,
            boolean generationProtocolActivationEnabled,
            ObjectStoreSecretResolver secretResolver,
            ClassLoader pluginClassLoader) {
        this(
                eventLoopGroup,
                openTelemetry,
                creationGuard,
                cursorProtocolActivationGuard,
                generationCapabilityReadinessProvider,
                generationProtocolActivationEnabled,
                secretResolver,
                pluginClassLoader,
                Optional.empty(),
                BookKeeperBrokerReadinessProvider.unavailable(),
                BookKeeperPrimaryWalCapabilitySink.unavailable(),
                BookKeeperPrimaryWalAdministrationSink.unavailable());
    }

    public NereusRuntimeContext(
            EventLoopGroup eventLoopGroup,
            OpenTelemetry openTelemetry,
            NereusCreationGuard creationGuard,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            GenerationCapabilityReadinessProvider generationCapabilityReadinessProvider,
            ObjectStoreSecretResolver secretResolver,
            ClassLoader pluginClassLoader) {
        this(
                eventLoopGroup,
                openTelemetry,
                creationGuard,
                cursorProtocolActivationGuard,
                generationCapabilityReadinessProvider,
                false,
                secretResolver,
                pluginClassLoader,
                Optional.empty(),
                BookKeeperBrokerReadinessProvider.unavailable(),
                BookKeeperPrimaryWalCapabilitySink.unavailable(),
                BookKeeperPrimaryWalAdministrationSink.unavailable());
    }

    public NereusRuntimeContext(
            EventLoopGroup eventLoopGroup,
            OpenTelemetry openTelemetry,
            NereusCreationGuard creationGuard,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            ObjectStoreSecretResolver secretResolver,
            ClassLoader pluginClassLoader) {
        this(
                eventLoopGroup,
                openTelemetry,
                creationGuard,
                cursorProtocolActivationGuard,
                GenerationCapabilityReadinessProvider.unavailable(),
                false,
                secretResolver,
                pluginClassLoader,
                Optional.empty(),
                BookKeeperBrokerReadinessProvider.unavailable(),
                BookKeeperPrimaryWalCapabilitySink.unavailable(),
                BookKeeperPrimaryWalAdministrationSink.unavailable());
    }

    public NereusRuntimeContext(
            EventLoopGroup eventLoopGroup,
            OpenTelemetry openTelemetry,
            NereusCreationGuard creationGuard,
            ObjectStoreSecretResolver secretResolver,
            ClassLoader pluginClassLoader) {
        this(
                eventLoopGroup,
                openTelemetry,
                creationGuard,
                CursorProtocolActivationGuard.unavailable(),
                GenerationCapabilityReadinessProvider.unavailable(),
                false,
                secretResolver,
                pluginClassLoader,
                Optional.empty(),
                BookKeeperBrokerReadinessProvider.unavailable(),
                BookKeeperPrimaryWalCapabilitySink.unavailable(),
                BookKeeperPrimaryWalAdministrationSink.unavailable());
    }

    public NereusRuntimeContext {
        Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
        Objects.requireNonNull(openTelemetry, "openTelemetry");
        Objects.requireNonNull(creationGuard, "creationGuard");
        Objects.requireNonNull(cursorProtocolActivationGuard, "cursorProtocolActivationGuard");
        Objects.requireNonNull(
                generationCapabilityReadinessProvider,
                "generationCapabilityReadinessProvider");
        Objects.requireNonNull(secretResolver, "secretResolver");
        Objects.requireNonNull(pluginClassLoader, "pluginClassLoader");
        borrowedBookKeeperClient = Objects.requireNonNull(
                borrowedBookKeeperClient, "borrowedBookKeeperClient");
        Objects.requireNonNull(
                bookKeeperBrokerReadinessProvider,
                "bookKeeperBrokerReadinessProvider");
        Objects.requireNonNull(
                bookKeeperPrimaryWalCapabilitySink,
                "bookKeeperPrimaryWalCapabilitySink");
        Objects.requireNonNull(
                bookKeeperPrimaryWalAdministrationSink,
                "bookKeeperPrimaryWalAdministrationSink");
    }
}
