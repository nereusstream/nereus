/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Exact stream/commit identity decoded from one canonical L0 commit-log key. */
public record StreamCommitKeyIdentity(StreamId streamId, String commitId) {
    public StreamCommitKeyIdentity {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(commitId, "commitId");
        if (commitId.isBlank()) {
            throw new IllegalArgumentException("commitId cannot be blank");
        }
    }
}
