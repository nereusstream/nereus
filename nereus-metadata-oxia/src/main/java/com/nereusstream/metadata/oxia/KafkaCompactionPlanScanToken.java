/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.metadata.oxia;

import java.util.Objects;

/**
 * Opaque process-local continuation for one partition's compaction-plan children.
 */
public final class KafkaCompactionPlanScanToken {
    private final KafkaPartitionId partition;
    private final String scanPrefix;
    private final String exclusiveLastKey;

    KafkaCompactionPlanScanToken(KafkaPartitionId partition, String scanPrefix, String exclusiveLastKey) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.scanPrefix = requireText(scanPrefix, "scanPrefix");
        this.exclusiveLastKey = requireText(exclusiveLastKey, "exclusiveLastKey");
        if (!exclusiveLastKey.startsWith(scanPrefix)) {
            throw new IllegalArgumentException("exclusiveLastKey must be inside scanPrefix");
        }
    }

    KafkaPartitionId partition() {
        return partition;
    }

    String scanPrefix() {
        return scanPrefix;
    }

    String resumeFromInclusive() {
        return exclusiveLastKey + '\0';
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
