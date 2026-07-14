/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.time.Duration;
import java.util.Objects;

/** Bounded operation and value limits for the F3 cursor metadata adapter. */
public record CursorMetadataStoreConfig(
        Duration operationTimeout,
        int maxPendingOperations,
        int maxValueBytes,
        int maxScanPageSize) {
    public static final int F3_MAX_VALUE_BYTES = 64 * 1024;

    public CursorMetadataStoreConfig {
        Objects.requireNonNull(operationTimeout, "operationTimeout");
        if (operationTimeout.isZero() || operationTimeout.isNegative()) {
            throw new IllegalArgumentException("operationTimeout must be positive");
        }
        if (maxPendingOperations <= 0) {
            throw new IllegalArgumentException("maxPendingOperations must be positive");
        }
        if (maxValueBytes != F3_MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("maxValueBytes must equal the frozen F3 value");
        }
        if (maxScanPageSize <= 0) {
            throw new IllegalArgumentException("maxScanPageSize must be positive");
        }
    }

    public static CursorMetadataStoreConfig defaults() {
        return new CursorMetadataStoreConfig(
                Duration.ofSeconds(30), 1_024, F3_MAX_VALUE_BYTES, 256);
    }
}
