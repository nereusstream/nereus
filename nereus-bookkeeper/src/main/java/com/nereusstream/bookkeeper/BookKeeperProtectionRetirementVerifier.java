/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Revalidates monotonic trim/F4 owner-retirement evidence before and after the physical tombstone CAS. */
@FunctionalInterface
public interface BookKeeperProtectionRetirementVerifier {
    CompletableFuture<Void> revalidate(
            BookKeeperProtectionRetirementProof proof,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout);
}
