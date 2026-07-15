/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

/** Closed V1 classification of physically collectible object-store bytes. */
public enum PhysicalObjectKind {
    OBJECT_WAL(1),
    COMMITTED_COMPACTED(2),
    TOPIC_COMPACTED(3),
    RECOVERY_CHECKPOINT(4),
    CURSOR_SNAPSHOT(5),
    INDEX_OBJECT(6),
    FUTURE_CATALOG_OBJECT(7);

    private final int wireId;

    PhysicalObjectKind(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static PhysicalObjectKind fromWireId(int value) {
        for (PhysicalObjectKind kind : values()) {
            if (kind.wireId == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown physical object kind id: " + value);
    }
}
