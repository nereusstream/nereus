/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReference;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Bounded canonical accumulator shared by metadata-backed reference domains. */
final class GcReferenceSnapshotAccumulator {
    private static final Comparator<GcAuthorityToken> AUTHORITY_ORDER = Comparator
            .comparing(GcAuthorityToken::authorityKey)
            .thenComparingLong(GcAuthorityToken::metadataVersion)
            .thenComparing(value -> value.identitySha256().value());
    private static final Comparator<GcReference> REFERENCE_ORDER = Comparator
            .comparing(GcReference::referenceType)
            .thenComparing(GcReference::referenceId)
            .thenComparing(GcReference::ownerKey)
            .thenComparingLong(GcReference::ownerMetadataVersion)
            .thenComparing(value -> value.ownerIdentitySha256().value());

    private final String domainId;
    private final int protocolVersion;
    private final GcReferenceQuery query;
    private final int maxAuthorities;
    private final int maxReferences;
    private final ArrayList<GcAuthorityToken> authorities = new ArrayList<>();
    private final ArrayList<GcReference> references = new ArrayList<>();
    private long authorityCount;
    private long referenceCount;
    private boolean limitExceeded;
    private boolean veto;

    GcReferenceSnapshotAccumulator(
            String domainId,
            int protocolVersion,
            GcReferenceQuery query,
            PhysicalGcConfig config) {
        this.domainId = Objects.requireNonNull(domainId, "domainId");
        this.protocolVersion = protocolVersion;
        this.query = Objects.requireNonNull(query, "query");
        Objects.requireNonNull(config, "config");
        this.maxAuthorities = config.maxAuthoritiesPerDomainSnapshot();
        this.maxReferences = config.maxReferencesPerDomainSnapshot();
    }

    void addAuthority(GcAuthorityToken authority) {
        Objects.requireNonNull(authority, "authority");
        if (authorityCount < maxAuthorities) {
            authorities.add(authority);
        } else {
            limitExceeded = true;
        }
        authorityCount = boundedIncrement(authorityCount, maxAuthorities);
    }

    void addReference(GcReference reference) {
        Objects.requireNonNull(reference, "reference");
        if (referenceCount < maxReferences) {
            references.add(reference);
        } else {
            limitExceeded = true;
        }
        referenceCount = boundedIncrement(referenceCount, maxReferences);
    }

    boolean limitExceeded() {
        return limitExceeded;
    }

    void veto() {
        veto = true;
    }

    GcReferenceSnapshot finish() {
        authorities.sort(AUTHORITY_ORDER);
        references.sort(REFERENCE_ORDER);
        return GcReferenceSnapshot.create(
                domainId,
                protocolVersion,
                query.queryIdentitySha256(),
                !limitExceeded,
                limitExceeded || veto,
                authorityCount,
                referenceCount,
                List.copyOf(authorities),
                List.copyOf(references));
    }

    static GcReferenceSnapshot unsupportedOwnerless(
            String domainId, int protocolVersion, GcReferenceQuery query) {
        return GcReferenceSnapshot.create(
                domainId,
                protocolVersion,
                query.queryIdentitySha256(),
                false,
                true,
                0,
                0,
                List.of(),
                List.of());
    }

    private static long boundedIncrement(long current, int maximum) {
        return current >= maximum ? Math.addExact((long) maximum, 1) : Math.addExact(current, 1);
    }
}
