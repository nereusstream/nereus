/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.util.Objects;

/** Protocol-neutral NTC1 strategy identity copied from the durable materialization policy. */
public record TopicCompactionFormatSpec(
        String strategyId,
        long strategyVersion,
        String keyCodecId) {
    public TopicCompactionFormatSpec {
        strategyId = requireText(strategyId, "strategyId");
        if (strategyVersion <= 0) {
            throw new IllegalArgumentException("strategyVersion must be positive");
        }
        keyCodecId = requireText(keyCodecId, "keyCodecId");
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
