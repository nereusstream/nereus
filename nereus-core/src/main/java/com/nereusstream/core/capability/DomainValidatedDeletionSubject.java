/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import java.util.Objects;

public record DomainValidatedDeletionSubject(
        GcReferenceQuery referenceQuery,
        Checksum projectionDomainSnapshotSha256) implements GenerationActivationSubject {
    public DomainValidatedDeletionSubject {
        Objects.requireNonNull(referenceQuery, "referenceQuery");
        projectionDomainSnapshotSha256 = GcReferenceQuery.requireSha256(
                projectionDomainSnapshotSha256, "projectionDomainSnapshotSha256");
    }
}
