/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Exact durable identity of one higher-generation index key. */
public record GenerationIndexIdentity(
        StreamId streamId,
        ReadView view,
        long offsetEnd,
        long generation) {
    public GenerationIndexIdentity {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(view, "view");
        if (offsetEnd <= 0 || generation < 1) {
            throw new IllegalArgumentException("higher-generation identity fields are invalid");
        }
    }
}
