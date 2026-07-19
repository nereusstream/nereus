/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** A reserved row already vetoes GC; ACTIVE additionally names a durable owner. */
public enum ProtectionLifecycle {
    RESERVED(1), ACTIVE(2);

    private final int wireId;

    ProtectionLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static ProtectionLifecycle fromWireId(int wireId) {
        for (ProtectionLifecycle value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown protection lifecycle wire id: " + wireId);
    }
}
