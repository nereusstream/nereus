/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.time.Duration;
import java.util.Objects;

public record ProjectionMetadataStoreConfig(
        Duration operationTimeout,
        int maxPendingOperations,
        int maxValueBytes) {
    public static final int F2_MAX_VALUE_BYTES = 64 * 1024;

    public ProjectionMetadataStoreConfig {
        Objects.requireNonNull(operationTimeout, "operationTimeout");
        if (operationTimeout.isZero() || operationTimeout.isNegative()) {
            throw new IllegalArgumentException("operationTimeout must be positive");
        }
        if (maxPendingOperations <= 0) {
            throw new IllegalArgumentException("maxPendingOperations must be positive");
        }
        if (maxValueBytes != F2_MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("F2 maxValueBytes must equal 64 KiB");
        }
    }

    public static ProjectionMetadataStoreConfig defaults() {
        return new ProjectionMetadataStoreConfig(Duration.ofSeconds(30), 1024, F2_MAX_VALUE_BYTES);
    }
}
