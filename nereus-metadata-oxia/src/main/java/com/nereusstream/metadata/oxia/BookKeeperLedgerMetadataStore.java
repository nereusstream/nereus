/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Focused exact-ledger root/protection/reader metadata surface. */
public interface BookKeeperLedgerMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>>> getRoot(
            String cluster, String providerScopeSha256, long ledgerId);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> createRoot(
            String cluster, BookKeeperLedgerRootRecord value);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> compareAndSetRoot(
            String cluster, BookKeeperLedgerRootRecord value, long expectedVersion);
    CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>>> scanRoots(
            String cluster, int shard, Optional<BookKeeperScanToken> continuation, int limit);

    CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>> getProtection(
            String cluster, String providerScopeSha256, long ledgerId, int rangeSlot, int protectionSlot);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> createProtection(
            String cluster, String providerScopeSha256, BookKeeperLedgerProtectionRecord value);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> compareAndSetProtection(
            String cluster, String providerScopeSha256, BookKeeperLedgerProtectionRecord value, long expectedVersion);
    CompletableFuture<Void> deleteProtection(String cluster, String providerScopeSha256, long ledgerId,
            int rangeSlot, int protectionSlot, long expectedVersion);
    CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>> scanProtections(
            String cluster, String providerScopeSha256, long ledgerId,
            Optional<BookKeeperScanToken> continuation, int limit);

    CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>>> getReaderLease(
            String cluster, String providerScopeSha256, long ledgerId, int readerSlot);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> createReaderLease(
            String cluster, String providerScopeSha256, BookKeeperLedgerReaderLeaseRecord value);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> compareAndSetReaderLease(
            String cluster, String providerScopeSha256, BookKeeperLedgerReaderLeaseRecord value, long expectedVersion);
    CompletableFuture<Void> deleteReaderLease(String cluster, String providerScopeSha256, long ledgerId,
            int readerSlot, long expectedVersion);
    CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>>> scanReaderLeases(
            String cluster, String providerScopeSha256, long ledgerId,
            Optional<BookKeeperScanToken> continuation, int limit);

    @Override void close();
}
