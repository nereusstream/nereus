/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.Objects;

/** Exact topic name plus its immutable F2 projection identity. */
public record CursorLedgerIdentity(
        String managedLedgerName,
        String managedLedgerNameHash,
        ManagedLedgerProjectionIdentity projection) {
    public CursorLedgerIdentity {
        managedLedgerName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(managedLedgerNameHash, "managedLedgerNameHash");
        if (!ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName)
                .equals(managedLedgerNameHash)) {
            throw new IllegalArgumentException("managed-ledger name/hash mismatch");
        }
        Objects.requireNonNull(projection, "projection");
    }
}
