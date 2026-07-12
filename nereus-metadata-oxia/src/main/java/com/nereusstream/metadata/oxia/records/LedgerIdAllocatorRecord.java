/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;

public record LedgerIdAllocatorRecord(
        long nextLedgerId,
        long allocations,
        long metadataVersion) {
    public LedgerIdAllocatorRecord {
        if (nextLedgerId < ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID
                || allocations < 0
                || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid managed-ledger allocator record");
        }
    }
}
