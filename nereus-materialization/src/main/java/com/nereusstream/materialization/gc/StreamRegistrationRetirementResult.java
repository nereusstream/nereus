/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Exact mutation summary for one materialization stream-registration retirement pass. */
public record StreamRegistrationRetirementResult(
        StreamId streamId,
        long registrationMetadataVersion,
        StreamRegistrationRetirementStatus status,
        int protectionsRetired,
        int indexesRetired,
        int tasksRetired,
        int checkpointsRetired,
        int retentionStatsRetired,
        int sequencesRetired,
        boolean recoveryRootRetired,
        boolean registrationRetired) {
    public StreamRegistrationRetirementResult {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(status, "status");
        if (registrationMetadataVersion < 0
                || protectionsRetired < 0
                || indexesRetired < 0
                || tasksRetired < 0
                || checkpointsRetired < 0
                || retentionStatsRetired < 0
                || sequencesRetired < 0) {
            throw new IllegalArgumentException("stream-retirement versions/counts must be non-negative");
        }
        if (registrationRetired != (status == StreamRegistrationRetirementStatus.RETIRED)) {
            throw new IllegalArgumentException("only RETIRED may report registration removal");
        }
    }

    public static StreamRegistrationRetirementResult simple(
            StreamId streamId,
            long registrationMetadataVersion,
            StreamRegistrationRetirementStatus status) {
        return new StreamRegistrationRetirementResult(
                streamId,
                registrationMetadataVersion,
                status,
                0,
                0,
                0,
                0,
                0,
                0,
                false,
                false);
    }
}
