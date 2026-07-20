/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

/** Accounting for one complete all-256-shard BookKeeper ledger-retention pass. */
public record BookKeeperLedgerRetentionScanResult(
        boolean mutationEnabled,
        int shardsScanned,
        int rootsScanned,
        int matchingRoots,
        int sealedRoots,
        int inFlightRoots,
        int materializationTriggers,
        int materializationTriggerFailures,
        int protectionsRetired,
        int rootsMarked,
        int rootsAdvanced,
        int rootsBlocked,
        int rootsFailed) {
    public BookKeeperLedgerRetentionScanResult {
        if (shardsScanned < 0
                || shardsScanned > BookKeeperLedgerRetentionScanner.ROOT_SHARDS
                || rootsScanned < 0
                || matchingRoots < 0
                || matchingRoots > rootsScanned
                || sealedRoots < 0
                || inFlightRoots < 0
                || sealedRoots + inFlightRoots > matchingRoots
                || materializationTriggers < 0
                || materializationTriggers > 1
                || materializationTriggerFailures < 0
                || materializationTriggerFailures > materializationTriggers
                || protectionsRetired < 0
                || rootsMarked < 0
                || rootsAdvanced < 0
                || rootsBlocked < 0
                || rootsFailed < 0) {
            throw new IllegalArgumentException("BookKeeper retention scan accounting is invalid");
        }
        if (!mutationEnabled
                && (shardsScanned != 0
                        || rootsScanned != 0
                        || matchingRoots != 0
                        || protectionsRetired != 0
                        || rootsMarked != 0
                        || rootsAdvanced != 0)) {
            throw new IllegalArgumentException("disabled BookKeeper retention cannot report mutations or scans");
        }
    }
}
