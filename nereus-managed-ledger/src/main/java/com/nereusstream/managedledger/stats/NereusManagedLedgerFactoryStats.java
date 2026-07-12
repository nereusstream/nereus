/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.stats;

import java.util.Objects;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;

/** Stock factory metrics view; F2 has no BookKeeper entry cache. */
public final class NereusManagedLedgerFactoryStats implements ManagedLedgerFactoryMXBean {
    private final IntSupplier openLedgers;

    public NereusManagedLedgerFactoryStats(IntSupplier openLedgers) {
        this.openLedgers = Objects.requireNonNull(openLedgers, "openLedgers");
    }

    @Override public int getNumberOfManagedLedgers() { return openLedgers.getAsInt(); }
    @Override public long getCacheUsedSize() { return 0; }
    @Override public long getCacheMaxSize() { return 0; }
    @Override public double getCacheHitsRate() { return 0; }
    @Override public long getCacheHitsTotal() { return 0; }
    @Override public double getCacheMissesRate() { return 0; }
    @Override public long getCacheMissesTotal() { return 0; }
    @Override public double getCacheHitsThroughput() { return 0; }
    @Override public long getCacheHitsBytesTotal() { return 0; }
    @Override public double getCacheMissesThroughput() { return 0; }
    @Override public long getCacheMissesBytesTotal() { return 0; }
    @Override public long getNumberOfCacheEvictions() { return 0; }
    @Override public long getNumberOfCacheEvictionsTotal() { return 0; }
    @Override public long getCacheInsertedEntriesCount() { return 0; }
    @Override public long getCacheEvictedEntriesCount() { return 0; }
    @Override public long getCacheEntriesCount() { return 0; }
}
