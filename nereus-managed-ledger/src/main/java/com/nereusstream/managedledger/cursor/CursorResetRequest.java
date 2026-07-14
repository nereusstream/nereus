/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Objects;
import java.util.Optional;

/** Normalized direct next-read target for one durable cursor reset. */
public record CursorResetRequest(
        long nextReadOffset,
        Optional<BatchAckState> targetBatchAck,
        boolean force,
        long observedTrimOffset,
        long observedCommittedEndOffset) {
    public CursorResetRequest {
        targetBatchAck = Objects.requireNonNull(targetBatchAck, "targetBatchAck");
        if (nextReadOffset < 0
                || observedTrimOffset < 0
                || observedCommittedEndOffset < observedTrimOffset
                || nextReadOffset < observedTrimOffset
                || nextReadOffset > observedCommittedEndOffset) {
            throw new IllegalArgumentException("normalized reset bounds are invalid");
        }
        if (targetBatchAck.isPresent() && nextReadOffset == observedCommittedEndOffset) {
            throw new IllegalArgumentException("a one-past-tail reset cannot carry batch state");
        }
    }
}
