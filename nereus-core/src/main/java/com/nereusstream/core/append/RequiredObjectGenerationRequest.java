/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Exact stable-commit identity whose producer waits for a readable higher Object generation. */
public record RequiredObjectGenerationRequest(
        StreamId streamId,
        OffsetRange sourceRange,
        long sourceCommitVersion) {
    public RequiredObjectGenerationRequest {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceRange, "sourceRange");
        if (sourceRange.isEmpty() || sourceCommitVersion <= 0) {
            throw new IllegalArgumentException("required Object-generation source identity is invalid");
        }
    }
}
