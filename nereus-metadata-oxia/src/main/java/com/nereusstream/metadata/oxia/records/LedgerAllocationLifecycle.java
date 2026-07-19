/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Durable workflow lifecycle for an exact-id BookKeeper allocation. */
public enum LedgerAllocationLifecycle {
    PREPARED(1), ROOT_RESERVED(2), CREATE_UNCERTAIN(3), PHYSICAL_CREATED(4), ACTIVATED(5),
    FOREIGN_COLLISION(6), ABORTED(7);

    private final int wireId;

    LedgerAllocationLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static LedgerAllocationLifecycle fromWireId(int wireId) {
        for (LedgerAllocationLifecycle value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown ledger allocation lifecycle wire id: " + wireId);
    }
}
