/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

/** Stable wire lifecycle for the durable BookKeeper primary-WAL activation. */
public enum BookKeeperProtocolActivationLifecycle {
    PREPARED(1),
    ACTIVE(2);

    private final int wireId;

    BookKeeperProtocolActivationLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static BookKeeperProtocolActivationLifecycle fromWireId(int wireId) {
        return switch (wireId) {
            case 1 -> PREPARED;
            case 2 -> ACTIVE;
            default -> throw new IllegalArgumentException(
                    "unknown BookKeeper activation lifecycle wire id: " + wireId);
        };
    }
}
