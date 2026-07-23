/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

public enum KafkaPayloadMapping {
    KAFKA_RECORD_BATCH_V1(1);

    private final int wireId;

    KafkaPayloadMapping(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static KafkaPayloadMapping fromWireId(int id) {
        if (id == 1) return KAFKA_RECORD_BATCH_V1;
        throw new IllegalArgumentException("unknown Kafka payload mapping wire ID: " + id);
    }
}
