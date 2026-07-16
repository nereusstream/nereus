/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.StreamId;
import java.util.Objects;
import java.util.Optional;

/** Exact committed-marker facts captured before a source-retirement plan is frozen. */
public record VersionedGenerationZeroMarker(
        String key,
        StreamId streamId,
        GenerationZeroMarkerIdentity identity,
        long offsetStart,
        long offsetEnd,
        long commitVersion,
        Optional<Checksum> readTargetIdentitySha256,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedGenerationZeroMarker {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(identity, "identity");
        readTargetIdentitySha256 = Objects.requireNonNull(
                readTargetIdentitySha256, "readTargetIdentitySha256");
        if (key.isBlank() || offsetStart < 0 || offsetEnd <= offsetStart
                || commitVersion <= 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("generation-zero marker facts are invalid");
        }
        readTargetIdentitySha256.ifPresent(value ->
                RetirementMetadataSupport.requireSha256(
                        value, "readTargetIdentitySha256"));
        if ((identity instanceof GenericCommittedAppendIdentity)
                != readTargetIdentitySha256.isPresent()) {
            throw new IllegalArgumentException(
                    "only a generic committed marker carries read-target identity");
        }
        durableValueSha256 = RetirementMetadataSupport.requireSha256(
                durableValueSha256, "durableValueSha256");
    }
}
