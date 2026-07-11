/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.StreamId;
import java.time.Duration;
import java.util.Objects;
public record PrimaryAppendRequest(StreamId streamId, AppendBatch batch, AppendSession session,
        long expectedStartOffset, AppendAttemptId attemptId, Duration timeout) {
    public PrimaryAppendRequest { Objects.requireNonNull(streamId); Objects.requireNonNull(batch);
        Objects.requireNonNull(session); Objects.requireNonNull(attemptId); Objects.requireNonNull(timeout);
        if (expectedStartOffset < 0 || timeout.isZero() || timeout.isNegative())
            throw new IllegalArgumentException("invalid primary append request"); }
}
