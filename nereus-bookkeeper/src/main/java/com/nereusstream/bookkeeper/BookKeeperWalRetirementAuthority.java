/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Profile-specific bridge from monotonic L0/F4 facts to exact BookKeeper protection retirement. */
public interface BookKeeperWalRetirementAuthority extends BookKeeperProtectionRetirementVerifier {
    CompletableFuture<Optional<BookKeeperProtectionRetirementProof>> proveLogicalTrim(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout);

    CompletableFuture<Optional<BookKeeperProtectionRetirementProof>> proveAbandonedAppend(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout);

    default CompletableFuture<Optional<BookKeeperProtectionRetirementProof>> proveHealthyHigherGeneration(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout) {
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
