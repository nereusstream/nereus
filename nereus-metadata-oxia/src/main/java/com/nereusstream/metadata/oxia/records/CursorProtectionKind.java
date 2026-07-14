/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Stable on-wire kind identifiers for recoverable protection intents. */
public enum CursorProtectionKind {
    CREATE(1),
    RECREATE(2),
    BACKWARD_RESET(3);

    private final int id;

    CursorProtectionKind(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static CursorProtectionKind fromId(int id) {
        return switch (id) {
            case 1 -> CREATE;
            case 2 -> RECREATE;
            case 3 -> BACKWARD_RESET;
            default -> throw new IllegalArgumentException("unknown cursor protection kind ID: " + id);
        };
    }
}
