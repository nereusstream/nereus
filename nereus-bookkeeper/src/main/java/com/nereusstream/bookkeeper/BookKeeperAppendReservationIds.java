/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Stable O(1) mapping from one logical append attempt to its immutable BookKeeper range reservation. */
public final class BookKeeperAppendReservationIds {
    private BookKeeperAppendReservationIds() { }

    public static String forAttempt(StreamId streamId, AppendAttemptId attemptId) {
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        AppendAttemptId attempt = Objects.requireNonNull(attemptId, "attemptId");
        return BookKeeperIdentityDigests.sha256(
                "NBKR-RESERVATION-V2\0" + stream.value() + "\0" + attempt.value());
    }
}
