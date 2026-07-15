/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import java.util.Objects;

/** Generic committed-append marker key identity. */
public record GenericCommittedAppendIdentity(String commitId)
        implements GenerationZeroMarkerIdentity {
    public GenericCommittedAppendIdentity {
        Objects.requireNonNull(commitId, "commitId");
        if (commitId.isBlank()) {
            throw new IllegalArgumentException("commitId cannot be blank");
        }
    }
}
