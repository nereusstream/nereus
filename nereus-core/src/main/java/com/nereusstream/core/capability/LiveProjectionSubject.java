/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcReferenceQuery;
import java.util.Objects;

public record LiveProjectionSubject(
        StreamId streamId,
        ProjectionRef projectionRef,
        Checksum projectionIdentitySha256) implements GenerationActivationSubject {
    public LiveProjectionSubject {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(projectionRef, "projectionRef");
        projectionIdentitySha256 = GcReferenceQuery.requireSha256(
                projectionIdentitySha256, "projectionIdentitySha256");
    }
}
