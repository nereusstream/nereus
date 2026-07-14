/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Stable on-wire lifecycle identifiers for cursor roots. */
public enum CursorRecordLifecycle {
    ACTIVE(1),
    DELETED(2);

    private final int id;

    CursorRecordLifecycle(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static CursorRecordLifecycle fromId(int id) {
        return switch (id) {
            case 1 -> ACTIVE;
            case 2 -> DELETED;
            default -> throw new IllegalArgumentException("unknown cursor lifecycle ID: " + id);
        };
    }
}
