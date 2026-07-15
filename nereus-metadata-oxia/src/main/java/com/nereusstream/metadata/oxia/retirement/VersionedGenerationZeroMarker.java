/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Exact committed-marker facts captured before a source-retirement plan is frozen. */
public record VersionedGenerationZeroMarker(
        String key,
        StreamId streamId,
        GenerationZeroMarkerIdentity identity,
        long offsetStart,
        long offsetEnd,
        long commitVersion,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedGenerationZeroMarker {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(identity, "identity");
        if (key.isBlank() || offsetStart < 0 || offsetEnd <= offsetStart
                || commitVersion <= 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("generation-zero marker facts are invalid");
        }
        durableValueSha256 = RetirementMetadataSupport.requireSha256(
                durableValueSha256, "durableValueSha256");
    }
}
