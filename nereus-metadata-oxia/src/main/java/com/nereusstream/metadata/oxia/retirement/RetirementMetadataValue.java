/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import java.util.Objects;

/** Exact durable bytes and version returned to a focused retirement adapter. */
public record RetirementMetadataValue(String key, byte[] value, long version) {
    public RetirementMetadataValue {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key cannot be blank");
        }
        value = Objects.requireNonNull(value, "value").clone();
        if (value.length == 0 || version < 0) {
            throw new IllegalArgumentException("retirement metadata value is invalid");
        }
    }

    @Override
    public byte[] value() {
        return value.clone();
    }
}
