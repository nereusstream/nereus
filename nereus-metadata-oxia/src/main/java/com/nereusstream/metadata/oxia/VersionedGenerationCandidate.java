/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;

/** Closed generation-zero/higher-generation scan union. */
public sealed interface VersionedGenerationCandidate
        permits VersionedGenerationZeroIndex, VersionedGenerationIndex {
    String key();

    long metadataVersion();

    Checksum durableValueSha256();
}
