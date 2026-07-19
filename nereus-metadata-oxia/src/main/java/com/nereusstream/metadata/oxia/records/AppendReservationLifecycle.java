/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Recovery lifecycle for one pre-reserved BookKeeper append range. */
public enum AppendReservationLifecycle {
    RESERVED(1), WRITING(2), DURABLE(3), COMMIT_PREPARED(4), HEAD_COMMITTED(5), ABANDONED(6);

    private final int wireId;

    AppendReservationLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static AppendReservationLifecycle fromWireId(int wireId) {
        for (AppendReservationLifecycle value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown append reservation lifecycle wire id: " + wireId);
    }
}
