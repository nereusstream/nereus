/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Objects;

/** Borrowed broker resources. The Nereus runtime never closes any value in this record. */
public record NereusRuntimeContext(
        EventLoopGroup eventLoopGroup,
        OpenTelemetry openTelemetry,
        NereusCreationGuard creationGuard,
        ObjectStoreSecretResolver secretResolver,
        ClassLoader pluginClassLoader) {
    public NereusRuntimeContext {
        Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
        Objects.requireNonNull(openTelemetry, "openTelemetry");
        Objects.requireNonNull(creationGuard, "creationGuard");
        Objects.requireNonNull(secretResolver, "secretResolver");
        Objects.requireNonNull(pluginClassLoader, "pluginClassLoader");
    }
}
