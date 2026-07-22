/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import java.util.Optional;

/**
 * Typed lookup over runtime components that share one lifecycle owner.
 *
 * <p>The owner remains the only close boundary. Callers may inspect a component for administration,
 * diagnostics, or deterministic integration tests, but must not close the returned component directly.
 */
public interface OwnedRuntimeComponents extends AutoCloseable {
    <T extends AutoCloseable> Optional<T> component(Class<T> componentType);

    default <T extends AutoCloseable> T requireComponent(Class<T> componentType) {
        return component(componentType).orElseThrow(() -> new IllegalArgumentException(
                "owned runtime component is not installed: " + componentType.getName()));
    }
}
