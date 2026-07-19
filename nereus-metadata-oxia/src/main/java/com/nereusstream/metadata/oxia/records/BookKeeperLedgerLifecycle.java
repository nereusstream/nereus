/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Authoritative Nereus lifecycle for one exact physical BookKeeper ledger. */
public enum BookKeeperLedgerLifecycle {
    ALLOCATING(1), ACTIVE(2), SEALING(3), SEALED(4), MARKED(5), DELETING(6), DELETED(7),
    ABORTED(8), QUARANTINED(9);

    private final int wireId;

    BookKeeperLedgerLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static BookKeeperLedgerLifecycle fromWireId(int wireId) {
        for (BookKeeperLedgerLifecycle value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown BookKeeper ledger lifecycle wire id: " + wireId);
    }
}
