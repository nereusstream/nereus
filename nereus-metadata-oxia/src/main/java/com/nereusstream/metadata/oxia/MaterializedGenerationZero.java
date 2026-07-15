/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import java.util.Objects;

/** Exact durable generation-zero index identity produced for one reachable append. */
public record MaterializedGenerationZero(
        CommittedAppend committedAppend,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexRecordSha256) {
    public MaterializedGenerationZero {
        Objects.requireNonNull(committedAppend, "committedAppend");
        indexKey = F4ValueValidation.text(indexKey, "indexKey");
        F4ValueValidation.version(indexMetadataVersion);
        indexRecordSha256 = F4ValueValidation.sha256(indexRecordSha256, "indexRecordSha256");
        if (committedAppend.generation() != 0) {
            throw new IllegalArgumentException("materialized append must belong to generation zero");
        }
    }
}
