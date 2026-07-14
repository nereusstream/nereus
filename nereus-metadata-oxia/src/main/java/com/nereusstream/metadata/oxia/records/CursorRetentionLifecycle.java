/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Stable on-wire lifecycle identifiers for the per-stream cursor retention root. */
public enum CursorRetentionLifecycle {
    ACTIVE(1),
    PROTECTION_PENDING(2),
    TRIM_PENDING(3);

    private final int id;

    CursorRetentionLifecycle(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static CursorRetentionLifecycle fromId(int id) {
        return switch (id) {
            case 1 -> ACTIVE;
            case 2 -> PROTECTION_PENDING;
            case 3 -> TRIM_PENDING;
            default -> throw new IllegalArgumentException("unknown retention lifecycle ID: " + id);
        };
    }
}
