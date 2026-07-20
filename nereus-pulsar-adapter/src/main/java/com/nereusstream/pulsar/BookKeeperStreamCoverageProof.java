/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Complete registered-stream/projection traversal for every durable BookKeeper profile. */
public record BookKeeperStreamCoverageProof(
        long brokerReadinessEpoch,
        Checksum brokerSetSha256,
        int shardsScanned,
        long registrationsScanned,
        long bookKeeperStreamsVerified,
        Checksum coverageSha256) {
    public static final int STREAM_SHARDS = 64;

    public BookKeeperStreamCoverageProof {
        if (brokerReadinessEpoch <= 0 || shardsScanned != STREAM_SHARDS) {
            throw new IllegalArgumentException("stream coverage requires positive readiness and every registry shard");
        }
        brokerSetSha256 = sha256(brokerSetSha256, "brokerSetSha256");
        coverageSha256 = sha256(coverageSha256, "coverageSha256");
        if (registrationsScanned < 0
                || bookKeeperStreamsVerified < 0
                || bookKeeperStreamsVerified > registrationsScanned) {
            throw new IllegalArgumentException("stream coverage counts are invalid");
        }
    }

    private static Checksum sha256(Checksum value, String name) {
        Checksum exact = Objects.requireNonNull(value, name);
        if (exact.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return exact;
    }
}
