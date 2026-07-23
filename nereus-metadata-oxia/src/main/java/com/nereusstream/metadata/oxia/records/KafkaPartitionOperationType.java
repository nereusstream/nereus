/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

public enum KafkaPartitionOperationType {
    NONE(0), CREATE(1), DELETE(2), REPAIR(3);

    private final int wireId;

    KafkaPartitionOperationType(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static KafkaPartitionOperationType fromWireId(int id) {
        for (KafkaPartitionOperationType value : values()) {
            if (value.wireId == id) return value;
        }
        throw new IllegalArgumentException("unknown Kafka operation wire ID: " + id);
    }
}
