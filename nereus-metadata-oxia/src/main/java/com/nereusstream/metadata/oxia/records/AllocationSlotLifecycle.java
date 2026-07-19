/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Cluster-wide exact allocation slot state. */
public enum AllocationSlotLifecycle {
    CLAIMED(1), CREATE_STARTED(2), CREATE_UNCERTAIN(3);

    private final int wireId;

    AllocationSlotLifecycle(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static AllocationSlotLifecycle fromWireId(int wireId) {
        for (AllocationSlotLifecycle value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown allocation slot lifecycle wire id: " + wireId);
    }
}
