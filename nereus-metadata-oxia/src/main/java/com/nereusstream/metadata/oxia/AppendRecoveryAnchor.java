/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Exact predecessor scalars at which an anchor-aware live commit walk may stop. */
public record AppendRecoveryAnchor(
        StreamId streamId,
        String lastCommitId,
        long offsetEnd,
        long cumulativeSize,
        long commitVersion) {
    public AppendRecoveryAnchor {
        Objects.requireNonNull(streamId, "streamId");
        lastCommitId = Objects.requireNonNull(lastCommitId, "lastCommitId");
        if (offsetEnd < 0 || cumulativeSize < 0 || commitVersion < 0) {
            throw new IllegalArgumentException("append recovery anchor scalars must be non-negative");
        }
        boolean genesis = lastCommitId.isEmpty();
        if (genesis != (offsetEnd == 0)
                || genesis != (cumulativeSize == 0)
                || genesis != (commitVersion == 0)) {
            throw new IllegalArgumentException("append recovery anchor is not canonical");
        }
    }

    public static AppendRecoveryAnchor genesis(StreamId streamId) {
        return new AppendRecoveryAnchor(streamId, "", 0, 0, 0);
    }

    public boolean isGenesis() {
        return lastCommitId.isEmpty();
    }
}
