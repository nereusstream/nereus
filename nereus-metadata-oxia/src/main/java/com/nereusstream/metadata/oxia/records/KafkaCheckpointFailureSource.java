/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Stable source of the first durable checkpoint quarantine decision. */
public enum KafkaCheckpointFailureSource {
    RECOVERY(1),
    RETENTION(2);

    private final int wireId;

    KafkaCheckpointFailureSource(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static KafkaCheckpointFailureSource fromWireId(int wireId) {
        for (KafkaCheckpointFailureSource value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown Kafka checkpoint failure source: " + wireId);
    }
}
