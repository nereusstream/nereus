/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Explicit resource ownership ledger; owned resources close once in reverse construction order. */
public final class KafkaRuntimeResources implements AutoCloseable {
    private final List<Resource> resources;
    private final AtomicBoolean closed = new AtomicBoolean();

    public KafkaRuntimeResources(List<Resource> resources) {
        Objects.requireNonNull(resources, "resources");
        List<Resource> exact = List.copyOf(resources);
        IdentityHashMap<AutoCloseable, Resource> identities = new IdentityHashMap<>();
        for (Resource resource : exact) {
            Resource previous = identities.put(resource.value(), resource);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Kafka runtime resource " + resource.name()
                                + " duplicates " + previous.name()
                                + " with " + previous.ownership() + "/" + resource.ownership()
                                + " ownership");
            }
        }
        this.resources = exact;
    }

    public List<Resource> resources() {
        return resources;
    }

    public boolean closed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        List<Throwable> failures = new ArrayList<>();
        for (int index = resources.size() - 1; index >= 0; index--) {
            Resource resource = resources.get(index);
            if (resource.ownership() != ResourceOwnership.OWNED) {
                continue;
            }
            try {
                resource.value().close();
            } catch (Throwable failure) {
                failures.add(new IllegalStateException(
                        "Failed to close owned Kafka runtime resource " + resource.name(), failure));
            }
        }
        if (!failures.isEmpty()) {
            IllegalStateException combined = new IllegalStateException(
                    "Failed to close " + failures.size() + " owned Kafka runtime resource(s)");
            failures.forEach(combined::addSuppressed);
            throw combined;
        }
    }

    public record Resource(
            String name,
            AutoCloseable value,
            ResourceOwnership ownership) {
        public Resource {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(value, "value");
            Objects.requireNonNull(ownership, "ownership");
            if (name.isBlank()) {
                throw new IllegalArgumentException("Kafka runtime resource name must be nonblank");
            }
        }

        public static Resource owned(String name, AutoCloseable value) {
            return new Resource(name, value, ResourceOwnership.OWNED);
        }

        public static Resource borrowed(String name, AutoCloseable value) {
            return new Resource(name, value, ResourceOwnership.BORROWED);
        }
    }
}
