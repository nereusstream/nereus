/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.checkpoint;

import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Durable stream-head and process-local Kafka-state facts captured under the partition snapshot lock. */
public record KafkaCheckpointSourceState(
        AppendAuthority authority,
        String writerId,
        long sessionEpoch,
        String fencingToken,
        long leaseVersion,
        long trimOffset,
        long endOffset,
        long commitVersion,
        String lastCommitId,
        Checksum headSha256,
        boolean appendInFlight,
        long stateMapEndOffset) {
    public KafkaCheckpointSourceState {
        Objects.requireNonNull(authority, "authority");
        writerId = text(writerId, "writerId");
        fencingToken = text(fencingToken, "fencingToken");
        lastCommitId = text(lastCommitId, "lastCommitId");
        Objects.requireNonNull(headSha256, "headSha256");
        if (headSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("headSha256 must use SHA256");
        }
        if (sessionEpoch < 0 || leaseVersion < 0 || trimOffset < 0 || endOffset < trimOffset
                || commitVersion <= 0 || stateMapEndOffset < 0) {
            throw new IllegalArgumentException("invalid Kafka checkpoint source state");
        }
    }

    public boolean sameSession(KafkaCheckpointSourceState other) {
        return authority.equals(other.authority)
                && writerId.equals(other.writerId)
                && sessionEpoch == other.sessionEpoch
                && fencingToken.equals(other.fencingToken)
                && other.leaseVersion >= leaseVersion;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > 64 * 1024) {
            throw new IllegalArgumentException(name + " must be nonblank and bounded");
        }
        return value;
    }
}
