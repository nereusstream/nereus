/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Process-local proof of one view-scoped sequence allocation. */
public record AllocatedGeneration(
        StreamId streamId,
        ReadView view,
        GenerationId generation,
        PublicationId publicationId,
        long allocationSequence,
        long sequenceMetadataVersion,
        Checksum sequenceValueSha256) {
    public AllocatedGeneration {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(generation, "generation");
        Objects.requireNonNull(publicationId, "publicationId");
        if (generation.value() < 1 || allocationSequence < 1) {
            throw new IllegalArgumentException("allocated generation and sequence must be positive");
        }
        F4ValueValidation.version(sequenceMetadataVersion);
        sequenceValueSha256 = F4ValueValidation.sha256(sequenceValueSha256, "sequenceValueSha256");
    }
}
