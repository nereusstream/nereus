/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import java.time.Duration;
import java.util.Objects;

/** Product-owned F2 limits and deadlines, independent of BookKeeper compatibility tuning. */
public record NereusManagedLedgerFactoryConfig(
        String storageClassName,
        Duration metadataTimeout,
        Duration appendTimeout,
        Duration appendRecoveryTimeout,
        Duration readTimeout,
        Duration closeTimeout,
        Duration tailPollInterval,
        int maxEntryBytes,
        int maxReadEntries,
        int maxOpenLedgers,
        int maxPendingCallbacks,
        int maxRetainedAppendAttempts,
        int maxScanEntries) {
    public static final String STORAGE_CLASS_NAME = "nereus";

    public NereusManagedLedgerFactoryConfig {
        if (!STORAGE_CLASS_NAME.equals(storageClassName)) {
            throw new IllegalArgumentException("storageClassName must equal nereus");
        }
        requirePositive(metadataTimeout, "metadataTimeout");
        requirePositive(appendTimeout, "appendTimeout");
        requirePositive(appendRecoveryTimeout, "appendRecoveryTimeout");
        requirePositive(readTimeout, "readTimeout");
        requirePositive(closeTimeout, "closeTimeout");
        requirePositive(tailPollInterval, "tailPollInterval");
        Duration appendAndRecovery;
        try {
            appendAndRecovery = appendTimeout.plus(appendRecoveryTimeout);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("append and recovery timeout sum overflows", e);
        }
        if (closeTimeout.compareTo(appendAndRecovery) < 0) {
            throw new IllegalArgumentException("closeTimeout must cover appendTimeout plus appendRecoveryTimeout");
        }
        if (maxEntryBytes <= 0
                || maxReadEntries <= 0
                || maxOpenLedgers <= 0
                || maxPendingCallbacks <= 0
                || maxRetainedAppendAttempts <= 0
                || maxScanEntries <= 0) {
            throw new IllegalArgumentException("all F2 managed-ledger limits must be positive");
        }
    }

    public static NereusManagedLedgerFactoryConfig defaults(int maxEntryBytes) {
        return new NereusManagedLedgerFactoryConfig(
                STORAGE_CLASS_NAME,
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(75),
                Duration.ofSeconds(1),
                maxEntryBytes,
                100,
                10_000,
                1_024,
                1_024,
                10_000);
    }

    private static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
