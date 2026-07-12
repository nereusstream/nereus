/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.util.Objects;

public record VirtualLedgerProjectionRecord(
        String managedLedgerName,
        String managedLedgerNameHash,
        ManagedLedgerProjectionIdentity identity,
        long startOffset,
        int positionMappingVersion,
        long metadataVersion) {
    public VirtualLedgerProjectionRecord {
        managedLedgerName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(managedLedgerNameHash, "managedLedgerNameHash");
        Objects.requireNonNull(identity, "identity");
        if (!managedLedgerNameHash.equals(ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName))
                || !identity.streamId().equals(
                        ManagedLedgerProjectionNames.streamId(managedLedgerName, identity.incarnation()).value())
                || startOffset != 0
                || positionMappingVersion != ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION
                || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid virtual-ledger projection record");
        }
    }
}
