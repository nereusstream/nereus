/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.metadata.oxia.PartitionKey;
import java.util.Objects;

/** Opaque exact-key capability constructible only by the focused retirement package. */
public final class RetirementMetadataKey {
    private final String key;
    private final PartitionKey partitionKey;

    RetirementMetadataKey(String key, PartitionKey partitionKey) {
        this.key = Objects.requireNonNull(key, "key");
        this.partitionKey = Objects.requireNonNull(partitionKey, "partitionKey");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key cannot be blank");
        }
    }

    public String key() {
        return key;
    }

    public PartitionKey partitionKey() {
        return partitionKey;
    }
}
