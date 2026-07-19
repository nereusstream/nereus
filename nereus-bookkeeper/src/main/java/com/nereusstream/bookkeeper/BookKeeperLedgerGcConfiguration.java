/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.time.Duration;
import java.util.Objects;

/** Local rollout controls for whole-ledger deletion; configuration is never deletion authority. */
public record BookKeeperLedgerGcConfiguration(
        int maxConcurrentDeletes,
        Duration maxClockSkew,
        Duration drainGrace,
        Duration lateCreateAuditGrace,
        boolean enabled,
        boolean dryRun) {
    public BookKeeperLedgerGcConfiguration {
        if (maxConcurrentDeletes <= 0) throw new IllegalArgumentException("maxConcurrentDeletes must be positive");
        nonNegative(maxClockSkew, "maxClockSkew");
        positive(drainGrace, "drainGrace");
        positive(lateCreateAuditGrace, "lateCreateAuditGrace");
        if (!dryRun && !enabled) throw new IllegalArgumentException("disabled BookKeeper GC must remain dry-run");
    }

    public static BookKeeperLedgerGcConfiguration safeDefault() {
        return new BookKeeperLedgerGcConfiguration(1, Duration.ofSeconds(30), Duration.ofMinutes(5),
                Duration.ofDays(7), false, true);
    }

    public void validateAgainst(BookKeeperWalConfiguration wal) {
        Objects.requireNonNull(wal, "wal");
        Duration minimum = wal.readerLeaseTtl().plus(maxClockSkew);
        if (drainGrace.compareTo(minimum) < 0) {
            throw new IllegalArgumentException("drainGrace must cover reader lease TTL plus clock skew");
        }
    }

    private static void positive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) throw new IllegalArgumentException(field + " must be positive");
    }
    private static void nonNegative(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isNegative()) throw new IllegalArgumentException(field + " must be non-negative");
    }
}
