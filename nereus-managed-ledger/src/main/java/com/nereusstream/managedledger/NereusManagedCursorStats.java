/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import org.apache.bookkeeper.mledger.ManagedCursorMXBean;

/** Cursor stats without fictitious cursor-ledger persistence. */
final class NereusManagedCursorStats implements ManagedCursorMXBean {
    private final String name;
    private final String ledgerName;
    NereusManagedCursorStats(String name, String ledgerName) { this.name = name; this.ledgerName = ledgerName; }
    @Override public String getName() { return name; }
    @Override public String getLedgerName() { return ledgerName; }
    @Override public void persistToLedger(boolean success) { }
    @Override public void persistToZookeeper(boolean success) { }
    @Override public long getPersistLedgerSucceed() { return 0; }
    @Override public long getPersistLedgerErrors() { return 0; }
    @Override public long getPersistZookeeperSucceed() { return 0; }
    @Override public long getPersistZookeeperErrors() { return 0; }
    @Override public void addWriteCursorLedgerSize(long size) { }
    @Override public void addReadCursorLedgerSize(long size) { }
    @Override public long getWriteCursorLedgerSize() { return 0; }
    @Override public long getWriteCursorLedgerLogicalSize() { return 0; }
    @Override public long getReadCursorLedgerSize() { return 0; }
}
