/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Exact owner class for a physical BookKeeper range protection. */
public enum BookKeeperProtectionType {
    REACHABLE_APPEND(1), VISIBLE_GENERATION(2), MATERIALIZATION_SOURCE(3), APPEND_RECOVERY(4), REPAIR(5);

    private final int wireId;

    BookKeeperProtectionType(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static BookKeeperProtectionType fromWireId(int wireId) {
        for (BookKeeperProtectionType value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown BookKeeper protection type wire id: " + wireId);
    }
}
