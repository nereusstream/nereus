/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

/** Bounded summary from one complete fixed-slot uncertain-allocation recovery pass. */
public record BookKeeperUncertainAllocationRecoveryResult(
        int scannedSlots,
        int uncertainSlots,
        int absentLedgers,
        int recoveredLedgers,
        int quarantinedLedgers) {
    public BookKeeperUncertainAllocationRecoveryResult {
        if (scannedSlots < 0
                || uncertainSlots < 0
                || absentLedgers < 0
                || recoveredLedgers < 0
                || quarantinedLedgers < 0
                || uncertainSlots > scannedSlots
                || absentLedgers + recoveredLedgers + quarantinedLedgers > uncertainSlots) {
            throw new IllegalArgumentException("BookKeeper uncertain-allocation recovery counts are invalid");
        }
    }
}
