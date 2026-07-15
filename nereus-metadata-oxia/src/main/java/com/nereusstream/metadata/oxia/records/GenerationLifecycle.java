/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Closed lifecycle for a higher-generation index record. */
public enum GenerationLifecycle {
    PREPARED(1), COMMITTED(2), QUARANTINED(3), DRAINING(4), RETIRED(5), ABORTED(6);

    private final int wireId;

    GenerationLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static GenerationLifecycle fromWireId(int wireId) {
        for (GenerationLifecycle value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown generation lifecycle wire id: " + wireId);
    }
}
