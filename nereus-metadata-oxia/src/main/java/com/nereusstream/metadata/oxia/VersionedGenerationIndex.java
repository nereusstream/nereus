/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import java.util.Objects;

/** Exact durable higher-generation index value and authoritative Oxia version. */
public record VersionedGenerationIndex(
        String key,
        GenerationIndexRecord value,
        long metadataVersion,
        Checksum durableValueSha256) implements VersionedGenerationCandidate {
    public VersionedGenerationIndex {
        key = F4ValueValidation.text(key, "key");
        Objects.requireNonNull(value, "value");
        F4ValueValidation.version(metadataVersion);
        if (value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("generation index value/version does not match wrapper");
        }
        durableValueSha256 = F4ValueValidation.sha256(durableValueSha256, "durableValueSha256");
    }
}
