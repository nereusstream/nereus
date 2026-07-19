/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import java.util.Objects;
import org.apache.bookkeeper.client.api.WriteAdvHandle;

/** Active durable metadata identities plus the process-local write handle that produced them. */
public record AllocatedBookKeeperLedger(
        WriteAdvHandle handle,
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer,
        BookKeeperVersionedValue<LedgerAllocationIntentRecord> allocation,
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
        BookKeeperLedgerCustomMetadata customMetadata) {
    public AllocatedBookKeeperLedger {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(writer, "writer");
        Objects.requireNonNull(allocation, "allocation");
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(customMetadata, "customMetadata");
        if (handle.getId() != root.value().ledgerId()
                || writer.value().activeLedgerId() != root.value().ledgerId()
                || allocation.value().candidateLedgerId() != root.value().ledgerId()) {
            throw new IllegalArgumentException("allocated ledger handle and durable identities differ");
        }
    }
}
