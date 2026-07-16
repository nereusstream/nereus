/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.util.Objects;

/** Canonical durable identity of one required physical-GC reference domain. */
public record ReferenceDomainVersionRecord(String domainId, int protocolVersion)
        implements Comparable<ReferenceDomainVersionRecord> {
    public ReferenceDomainVersionRecord {
        Objects.requireNonNull(domainId, "domainId");
        if (domainId.length() > 128
                || !domainId.matches("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")) {
            throw new IllegalArgumentException("domainId is not canonical");
        }
        if (protocolVersion <= 0) {
            throw new IllegalArgumentException("protocolVersion must be positive");
        }
    }

    @Override
    public int compareTo(ReferenceDomainVersionRecord other) {
        int byId = domainId.compareTo(other.domainId);
        return byId != 0
                ? byId
                : Integer.compare(protocolVersion, other.protocolVersion);
    }
}
