/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import java.util.Objects;

/** Compact authenticated identity of one complete reference-domain snapshot. */
public record GcDomainSnapshotProof(
        String domainId,
        int protocolVersion,
        Checksum queryIdentitySha256,
        Checksum snapshotSha256) {
    public GcDomainSnapshotProof {
        domainId = requireDomainId(domainId);
        if (protocolVersion <= 0) {
            throw new IllegalArgumentException("protocolVersion must be positive");
        }
        queryIdentitySha256 = GcReferenceQuery.requireSha256(
                queryIdentitySha256, "queryIdentitySha256");
        snapshotSha256 = GcReferenceQuery.requireSha256(snapshotSha256, "snapshotSha256");
    }

    public static GcDomainSnapshotProof from(GcReferenceSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (!snapshot.complete() || snapshot.veto()) {
            throw new IllegalArgumentException(
                    "a retirement proof requires a complete non-vetoing snapshot");
        }
        return new GcDomainSnapshotProof(
                snapshot.domainId(),
                snapshot.protocolVersion(),
                snapshot.queryIdentitySha256(),
                snapshot.snapshotSha256());
    }

    private static String requireDomainId(String value) {
        Objects.requireNonNull(value, "domainId");
        if (value.length() > 128 || !value.matches("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")) {
            throw new IllegalArgumentException("domainId is not canonical");
        }
        return value;
    }
}
