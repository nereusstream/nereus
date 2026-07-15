/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Process-local continuation bound to one observed head and one recovery-root anchor. */
public record AppendRecoveryTailCursor(
        StreamId streamId,
        AppendRecoveryAnchor anchor,
        AppendRecoveryHead observedHead,
        String nextCommitId,
        long nextOffsetEnd,
        long nextCumulativeSize,
        long nextCommitVersion) {
    public AppendRecoveryTailCursor {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(anchor, "anchor");
        Objects.requireNonNull(observedHead, "observedHead");
        nextCommitId = Objects.requireNonNull(nextCommitId, "nextCommitId");
        if (!streamId.equals(anchor.streamId()) || !streamId.equals(observedHead.streamId())) {
            throw new IllegalArgumentException("append recovery cursor identities belong to different streams");
        }
        if (nextCommitId.isEmpty()
                || nextOffsetEnd < 0
                || nextCumulativeSize < 0
                || nextCommitVersion <= anchor.commitVersion()
                || nextCommitVersion > observedHead.commitVersion()) {
            throw new IllegalArgumentException("append recovery cursor continuation is invalid");
        }
    }
}
