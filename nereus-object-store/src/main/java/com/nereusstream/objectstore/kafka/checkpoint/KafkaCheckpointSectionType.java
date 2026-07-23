/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

/** Closed required NKC1 section IDs. */
public enum KafkaCheckpointSectionType {
    PRODUCER_STATE(1),
    ABORTED_TRANSACTION_INDEX(2),
    LEADER_EPOCH_RANGES(3),
    VIRTUAL_SEGMENT_DESCRIPTORS(4),
    TIME_INDEX(5),
    LOGICAL_BYTE_POSITION_INDEX(6),
    OPEN_TRANSACTION_SUMMARY(7);

    private final int wireId;

    KafkaCheckpointSectionType(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static KafkaCheckpointSectionType fromWireId(int wireId) {
        for (KafkaCheckpointSectionType value : values()) {
            if (value.wireId == wireId) return value;
        }
        throw new IllegalArgumentException("unknown Kafka checkpoint section id: " + wireId);
    }
}
