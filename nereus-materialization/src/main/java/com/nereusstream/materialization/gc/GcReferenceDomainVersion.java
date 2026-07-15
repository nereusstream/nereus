/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import java.util.Objects;

/** Canonical local reference-domain capability identity compared with cluster activation metadata. */
public record GcReferenceDomainVersion(String domainId, int protocolVersion)
        implements Comparable<GcReferenceDomainVersion> {
    public GcReferenceDomainVersion {
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
    public int compareTo(GcReferenceDomainVersion other) {
        int byId = domainId.compareTo(other.domainId);
        return byId != 0 ? byId : Integer.compare(protocolVersion, other.protocolVersion);
    }
}
