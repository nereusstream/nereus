/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.snapshot;

import com.nereusstream.api.StreamMetadata;
import java.util.Objects;

public record StreamSnapshotView(
        StreamMetadata metadata,
        long observedCommitVersion,
        boolean localAppendOverlay) {
    public StreamSnapshotView {
        Objects.requireNonNull(metadata, "metadata");
        if (observedCommitVersion < 0) {
            throw new IllegalArgumentException("observedCommitVersion must be non-negative");
        }
    }
}
