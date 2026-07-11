/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;
import com.nereusstream.api.StreamId;
import java.util.Objects;
public record AppendReplayCursor(StreamId streamId, String commitId, long expectedStartOffset,
        String observedHeadCommitId, long observedHeadOffsetEnd, long observedHeadCumulativeSize,
        long observedHeadCommitVersion, String nextCommitId, long nextOffsetEnd,
        long nextCumulativeSize, long nextCommitVersion) {
    public AppendReplayCursor { Objects.requireNonNull(streamId); Objects.requireNonNull(commitId);
        Objects.requireNonNull(observedHeadCommitId); Objects.requireNonNull(nextCommitId);
        if (expectedStartOffset < 0 || observedHeadOffsetEnd < 0 || observedHeadCumulativeSize < 0
                || observedHeadCommitVersion < 0 || nextOffsetEnd < 0 || nextCumulativeSize < 0 || nextCommitVersion < 0)
            throw new IllegalArgumentException("append replay cursor fields are invalid"); }
}
