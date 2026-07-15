/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import java.util.Objects;

/** Exact durable generation-zero index bytes plus hydrated compatibility value. */
public record VersionedGenerationZeroIndex(
        String key,
        GenerationZeroIndexEncoding encoding,
        OffsetIndexEntry value,
        long metadataVersion,
        Checksum durableValueSha256) implements VersionedGenerationCandidate {
    public VersionedGenerationZeroIndex {
        key = F4ValueValidation.text(key, "key");
        Objects.requireNonNull(encoding, "encoding");
        Objects.requireNonNull(value, "value");
        F4ValueValidation.version(metadataVersion);
        if (value.generation() != 0 || value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("generation-zero value/version does not match wrapper");
        }
        durableValueSha256 = F4ValueValidation.sha256(durableValueSha256, "durableValueSha256");
    }
}
