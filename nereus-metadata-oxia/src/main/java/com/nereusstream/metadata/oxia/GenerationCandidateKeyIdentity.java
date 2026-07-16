/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Exact routing identity decoded from one canonical generation-candidate key. */
public record GenerationCandidateKeyIdentity(
        StreamId streamId,
        ReadView view,
        long offsetEnd,
        long generation) {
    public GenerationCandidateKeyIdentity {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(view, "view");
        if (offsetEnd <= 0 || generation < 0) {
            throw new IllegalArgumentException("generation-candidate key identity is invalid");
        }
        if (generation == 0 && view != ReadView.COMMITTED) {
            throw new IllegalArgumentException(
                    "generation zero exists only in the COMMITTED view");
        }
    }

    public boolean generationZero() {
        return generation == 0;
    }

    public GenerationIndexIdentity higherGenerationIdentity() {
        if (generationZero()) {
            throw new IllegalStateException("generation zero has no higher-generation identity");
        }
        return new GenerationIndexIdentity(streamId, view, offsetEnd, generation);
    }
}
