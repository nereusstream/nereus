/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.util.Objects;

public record PositionIndexRecord(
        String managedLedgerName,
        String managedLedgerNameHash,
        ManagedLedgerProjectionIdentity identity,
        int positionMappingVersion,
        String formula,
        long metadataVersion) {
    public PositionIndexRecord {
        managedLedgerName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(managedLedgerNameHash, "managedLedgerNameHash");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(formula, "formula");
        if (!managedLedgerNameHash.equals(ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName))
                || !identity.streamId().equals(
                        ManagedLedgerProjectionNames.streamId(managedLedgerName, identity.incarnation()).value())
                || positionMappingVersion != ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION
                || !ManagedLedgerProjectionNames.POSITION_FORMULA_V1.equals(formula)
                || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid position-index projection record");
        }
    }
}
