/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Exact append-only commit-chain position used as the descendant of a reachability proof. */
public record StreamCommitAnchor(
        StreamId streamId,
        String lastCommitId,
        long committedEndOffset,
        long cumulativeSize,
        long commitVersion) {
    public StreamCommitAnchor {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(lastCommitId, "lastCommitId");
        if (lastCommitId.getBytes(StandardCharsets.UTF_8).length > 64 * 1024) {
            throw new IllegalArgumentException("lastCommitId is too large");
        }
        if (committedEndOffset < 0 || cumulativeSize < 0 || commitVersion < 0) {
            throw new IllegalArgumentException("stream commit anchor scalars must be non-negative");
        }
        boolean genesis = lastCommitId.isEmpty();
        if (genesis != (committedEndOffset == 0)
                || genesis != (cumulativeSize == 0)
                || genesis != (commitVersion == 0)) {
            throw new IllegalArgumentException("stream commit anchor is not canonical");
        }
    }

    public boolean isGenesis() {
        return lastCommitId.isEmpty();
    }
}
