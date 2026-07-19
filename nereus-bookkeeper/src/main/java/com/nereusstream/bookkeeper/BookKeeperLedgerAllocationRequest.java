/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendSession;
import com.nereusstream.api.StreamId;
import java.time.Duration;
import java.util.Objects;

/** One pre-entry-write request to acquire an exact advanced BookKeeper ledger. */
public record BookKeeperLedgerAllocationRequest(
        StreamId streamId, AppendSession session, Duration timeout) {
    public BookKeeperLedgerAllocationRequest {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(timeout, "timeout");
        if (!session.streamId().equals(streamId)) {
            throw new IllegalArgumentException("allocation stream and append session differ");
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("allocation timeout must be positive");
        }
    }
}
