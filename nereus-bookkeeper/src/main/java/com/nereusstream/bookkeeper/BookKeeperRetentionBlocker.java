/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

/** Stable process-level reasons why one exact ledger cannot enter whole-ledger collection. */
public enum BookKeeperRetentionBlocker {
    ROOT_CHANGED_OR_INELIGIBLE,
    LATE_CREATE_HAZARD,
    PROVIDER_METADATA_MISMATCH,
    ACTIVATION_MISMATCH,
    ALLOCATION_SLOT_PRESENT,
    WRITER_SELECTS_LEDGER,
    PROTECTION_PRESENT,
    READER_LEASE_PRESENT,
    INVENTORY_LIMIT_EXCEEDED
}
