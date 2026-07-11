/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.util.Objects;

/** Fast replay marker for a reachable generic append. */
public record CommittedAppendRecord(
        String streamId, String commitId, long offsetStart, long offsetEnd,
        long generation, long commitVersion, String readTargetIdentitySha256, long metadataVersion) {
    public CommittedAppendRecord {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(commitId, "commitId");
        Objects.requireNonNull(readTargetIdentitySha256, "readTargetIdentitySha256");
        if (streamId.isBlank() || commitId.isBlank() || readTargetIdentitySha256.isBlank()
                || offsetStart < 0 || offsetEnd <= offsetStart || generation != 0
                || commitVersion <= 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("committed append fields are invalid");
        }
    }
}
