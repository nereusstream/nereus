/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

/** Closed durable NTC2 disposition IDs; Java ordinals are never serialized. */
public enum KafkaCompactionDispositionV2 {
    RETAIN_VALUE(1),
    RETAIN_TOMBSTONE(2),
    RETAIN_UNKEYED(3),
    RETAIN_CONTROL(4);

    private final int wireId;

    KafkaCompactionDispositionV2(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static KafkaCompactionDispositionV2 fromWireId(int wireId) {
        for (KafkaCompactionDispositionV2 value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new CompactedObjectFormatException("unknown NTC2 disposition wire id: " + wireId);
    }
}
