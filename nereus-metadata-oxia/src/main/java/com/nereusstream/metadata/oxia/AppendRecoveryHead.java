/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Immutable observed stream-head anchor retained across a paged recovery walk. */
public record AppendRecoveryHead(
        StreamId streamId,
        String lastCommitId,
        long offsetEnd,
        long cumulativeSize,
        long commitVersion,
        long metadataVersion) {
    public AppendRecoveryHead {
        Objects.requireNonNull(streamId, "streamId");
        lastCommitId = Objects.requireNonNull(lastCommitId, "lastCommitId");
        if (offsetEnd < 0 || cumulativeSize < 0 || commitVersion < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("append recovery head scalars must be non-negative");
        }
        boolean empty = lastCommitId.isEmpty();
        if (empty != (offsetEnd == 0)
                || empty != (cumulativeSize == 0)
                || empty != (commitVersion == 0)) {
            throw new IllegalArgumentException("append recovery head is not canonical");
        }
    }
}
