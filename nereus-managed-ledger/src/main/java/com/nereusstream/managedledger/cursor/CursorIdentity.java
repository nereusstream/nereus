/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.CursorNames;
import java.util.Objects;

/** Exact cursor-name key and generation within one immutable projection identity. */
public record CursorIdentity(
        CursorLedgerIdentity ledger,
        String cursorName,
        String cursorNameHash,
        long cursorGeneration) {
    public CursorIdentity {
        Objects.requireNonNull(ledger, "ledger");
        cursorName = CursorNames.requireCursorName(cursorName);
        Objects.requireNonNull(cursorNameHash, "cursorNameHash");
        if (!CursorNames.cursorNameHash(cursorName).equals(cursorNameHash)) {
            throw new IllegalArgumentException("cursor name/hash mismatch");
        }
        if (cursorGeneration <= 0) {
            throw new IllegalArgumentException("cursorGeneration must be positive");
        }
    }
}
