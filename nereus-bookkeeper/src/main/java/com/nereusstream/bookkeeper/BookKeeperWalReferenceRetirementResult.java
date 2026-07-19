/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

/** Bounded outcome of one ledger-scoped BK_ONLY reference-retirement pass. */
public record BookKeeperWalReferenceRetirementResult(
        int scannedProtections,
        int newlyRetiredProtections,
        int remainingProtections) {
    public BookKeeperWalReferenceRetirementResult {
        if (scannedProtections < 0
                || newlyRetiredProtections < 0
                || remainingProtections < 0
                || newlyRetiredProtections > scannedProtections
                || remainingProtections > scannedProtections) {
            throw new IllegalArgumentException("invalid BookKeeper reference-retirement counts");
        }
    }

    public boolean fullyRetired() {
        return remainingProtections == 0;
    }
}
