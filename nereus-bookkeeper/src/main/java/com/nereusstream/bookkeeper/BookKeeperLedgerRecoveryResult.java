/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import java.util.Objects;

/** Durable outcome of fencing/sealing one previous active writer ledger. */
public record BookKeeperLedgerRecoveryResult(
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer,
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> sealedRoot) {
    public BookKeeperLedgerRecoveryResult {
        Objects.requireNonNull(writer, "writer");
        Objects.requireNonNull(sealedRoot, "sealedRoot");
    }
}
