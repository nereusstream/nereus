/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Durable deletion fence state for one immutable physical object key. */
public enum PhysicalObjectLifecycle {
    ACTIVE(1), MARKED(2), DELETING(3), DELETED(4), QUARANTINED(5);

    private final int wireId;

    PhysicalObjectLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static PhysicalObjectLifecycle fromWireId(int wireId) {
        for (PhysicalObjectLifecycle value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown physical object lifecycle wire id: " + wireId);
    }
}
