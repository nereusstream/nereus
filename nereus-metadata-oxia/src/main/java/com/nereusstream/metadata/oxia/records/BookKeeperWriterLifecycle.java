/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Durable lifecycle for one stream-scoped BookKeeper writer. */
public enum BookKeeperWriterLifecycle {
    IDLE(1), ALLOCATING(2), ACTIVE(3), RECOVERING(4), CLOSED(5);

    private final int wireId;

    BookKeeperWriterLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static BookKeeperWriterLifecycle fromWireId(int wireId) {
        for (BookKeeperWriterLifecycle value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown BookKeeper writer lifecycle wire id: " + wireId);
    }
}
