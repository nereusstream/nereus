/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.CursorIds;
import java.util.Objects;

/** One per-writable-open session used to fence all cursor and retention roots. */
public record CursorOwnerSession(CursorLedgerIdentity ledger, String ownerSessionId) {
    public CursorOwnerSession {
        Objects.requireNonNull(ledger, "ledger");
        ownerSessionId = CursorIds.requireRandomId(ownerSessionId, "ownerSessionId");
    }
}
