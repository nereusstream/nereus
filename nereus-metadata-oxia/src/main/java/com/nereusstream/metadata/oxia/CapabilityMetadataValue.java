/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.Objects;

/** Exact durable bytes/version returned by the shared runtime's read-only capability view. */
public record CapabilityMetadataValue(String key, byte[] value, long version) {
    public CapabilityMetadataValue {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key cannot be blank");
        }
        value = Objects.requireNonNull(value, "value").clone();
        if (value.length == 0 || version < 0) {
            throw new IllegalArgumentException("capability metadata value is invalid");
        }
    }

    @Override
    public byte[] value() {
        return value.clone();
    }
}
