/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import java.util.Objects;

/** Exact durable dynamic-slot handle kept opaque behind the materialization source SPI. */
public record BookKeeperMaterializationSourceProtectionHandle(
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection) {
    public BookKeeperMaterializationSourceProtectionHandle {
        Objects.requireNonNull(protection, "protection");
    }
}
