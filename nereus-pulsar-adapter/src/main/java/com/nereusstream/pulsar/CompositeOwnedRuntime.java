/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** Idempotent close owner for runtime components that must remain distinct from borrowed broker resources. */
final class CompositeOwnedRuntime implements OwnedRuntimeComponents {
    private final List<AutoCloseable> resources;
    private final AtomicBoolean closed = new AtomicBoolean();

    CompositeOwnedRuntime(AutoCloseable... resources) {
        Objects.requireNonNull(resources, "resources");
        Map<AutoCloseable, Boolean> identities = new IdentityHashMap<>();
        ArrayList<AutoCloseable> values = new ArrayList<>();
        for (AutoCloseable resource : resources) {
            if (resource != null && identities.put(resource, Boolean.TRUE) == null) {
                values.add(resource);
            }
        }
        this.resources = List.copyOf(values);
    }

    @Override
    public <T extends AutoCloseable> Optional<T> component(Class<T> componentType) {
        Objects.requireNonNull(componentType, "componentType");
        return resources.stream()
                .filter(componentType::isInstance)
                .findFirst()
                .map(componentType::cast);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        List<Throwable> failures = new ArrayList<>();
        resources.forEach(resource -> {
            try {
                resource.close();
            } catch (Throwable failure) {
                failures.add(failure);
            }
        });
        if (!failures.isEmpty()) {
            RuntimeException aggregate = new RuntimeException("failed to close owned runtime components");
            failures.forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }
}
