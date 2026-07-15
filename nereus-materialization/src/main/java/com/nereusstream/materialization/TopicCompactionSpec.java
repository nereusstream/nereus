/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.Objects;

/** Immutable strategy identity for the separate sparse topic-compacted view. */
public record TopicCompactionSpec(
        String strategyId,
        long strategyVersion,
        String keyCodecId) {
    public TopicCompactionSpec {
        strategyId = requireText(strategyId, "strategyId");
        keyCodecId = requireText(keyCodecId, "keyCodecId");
        if (strategyVersion <= 0) {
            throw new IllegalArgumentException("strategyVersion must be positive");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
