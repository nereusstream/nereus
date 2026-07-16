/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.util.Objects;

/** Compact immutable identity of one complete reference-domain snapshot in a GC journal. */
public record GcDomainSnapshotProofRecord(
        String domainId,
        int protocolVersion,
        String queryIdentitySha256,
        String snapshotSha256) {
    public GcDomainSnapshotProofRecord {
        Objects.requireNonNull(domainId, "domainId");
        if (domainId.length() > 128
                || !domainId.matches("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")) {
            throw new IllegalArgumentException("domainId is not canonical");
        }
        F4RecordValidation.requirePositive(protocolVersion, "protocolVersion");
        queryIdentitySha256 = F4RecordValidation.requireSha256(
                queryIdentitySha256, "queryIdentitySha256");
        snapshotSha256 = F4RecordValidation.requireSha256(
                snapshotSha256, "snapshotSha256");
    }
}
