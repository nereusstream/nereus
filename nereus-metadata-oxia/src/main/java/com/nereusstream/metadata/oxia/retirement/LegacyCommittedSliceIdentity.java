/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.ObjectId;
import java.util.Objects;

/** Legacy committed-slice marker key identity. */
public record LegacyCommittedSliceIdentity(ObjectId objectId, String sliceId)
        implements GenerationZeroMarkerIdentity {
    public LegacyCommittedSliceIdentity {
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(sliceId, "sliceId");
        if (sliceId.isBlank()) {
            throw new IllegalArgumentException("sliceId cannot be blank");
        }
    }
}
