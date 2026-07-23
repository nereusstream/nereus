/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Closed wire lifecycle for one Kafka native-storage protocol activation epoch. */
public enum KafkaStorageActivationLifecycle {
    PREPARED(1),
    ACTIVE(2);

    private final int wireId;

    KafkaStorageActivationLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static KafkaStorageActivationLifecycle fromWireId(int wireId) {
        return switch (wireId) {
            case 1 -> PREPARED;
            case 2 -> ACTIVE;
            default -> throw new IllegalArgumentException(
                    "unknown Kafka storage activation lifecycle wire id: " + wireId);
        };
    }
}
