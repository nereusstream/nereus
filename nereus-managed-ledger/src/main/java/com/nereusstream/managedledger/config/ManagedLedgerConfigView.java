/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.config;

public record ManagedLedgerConfigView(
        String storageClassName,
        double throttleMarkDelete) {
    public ManagedLedgerConfigView {
        if (storageClassName != null && !"nereus".equals(storageClassName)) {
            throw new IllegalArgumentException("storageClassName must be null or nereus");
        }
        if (!Double.isFinite(throttleMarkDelete) || throttleMarkDelete < 0) {
            throw new IllegalArgumentException("throttleMarkDelete must be finite and non-negative");
        }
    }
}
