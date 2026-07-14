/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Objects;

/** Complete immutable input for writing one full cursor ack-state snapshot. */
public record CursorSnapshotWriteRequest(
        CursorIdentity identity,
        long sourceMutationSequence,
        CursorAckState fullState,
        long createdAtMillis) {
    public CursorSnapshotWriteRequest {
        Objects.requireNonNull(identity, "identity");
        if (sourceMutationSequence < 1) {
            throw new IllegalArgumentException("sourceMutationSequence must be positive");
        }
        Objects.requireNonNull(fullState, "fullState");
        if (createdAtMillis < 0) {
            throw new IllegalArgumentException("createdAtMillis must be non-negative");
        }
    }
}
