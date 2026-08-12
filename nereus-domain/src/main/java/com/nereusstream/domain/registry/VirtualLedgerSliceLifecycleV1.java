/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

/** Irreversible Registry slice lifecycle. */
public enum VirtualLedgerSliceLifecycleV1 {
    ACTIVE(1),
    RETIRING(2),
    RETIRED(3);

    private final int code;

    VirtualLedgerSliceLifecycleV1(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public boolean canAdvanceTo(VirtualLedgerSliceLifecycleV1 successor) {
        return this == successor
                || (this == ACTIVE && successor == RETIRING)
                || (this == RETIRING && successor == RETIRED);
    }

    public static VirtualLedgerSliceLifecycleV1 fromCode(int code) {
        return switch (code) {
            case 1 -> ACTIVE;
            case 2 -> RETIRING;
            case 3 -> RETIRED;
            default ->
                throw new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID,
                        "unknown virtual-ledger slice lifecycle: " + code);
        };
    }
}
