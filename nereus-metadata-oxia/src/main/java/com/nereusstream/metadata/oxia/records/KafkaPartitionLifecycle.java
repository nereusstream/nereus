/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

public enum KafkaPartitionLifecycle {
    CREATING(1), ACTIVE(2), DELETING(3), DELETED(4), CORRUPT(5);

    private final int wireId;

    KafkaPartitionLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static KafkaPartitionLifecycle fromWireId(int id) {
        for (KafkaPartitionLifecycle value : values()) {
            if (value.wireId == id) return value;
        }
        throw new IllegalArgumentException("unknown Kafka partition lifecycle wire ID: " + id);
    }
}
