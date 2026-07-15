/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.Checksum;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Canonical complete-or-veto snapshot returned by one registered reference domain. */
public record GcReferenceSnapshot(
        String domainId,
        int protocolVersion,
        Checksum queryIdentitySha256,
        boolean complete,
        boolean veto,
        long authorityCount,
        long referenceCount,
        List<GcAuthorityToken> authorities,
        List<GcReference> references,
        Checksum snapshotSha256) {
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

    public GcReferenceSnapshot {
        domainId = requireDomainId(domainId);
        if (protocolVersion <= 0) {
            throw new IllegalArgumentException("protocolVersion must be positive");
        }
        queryIdentitySha256 = GcReferenceQuery.requireSha256(
                queryIdentitySha256, "queryIdentitySha256");
        if (authorityCount < 0 || referenceCount < 0) {
            throw new IllegalArgumentException("snapshot counts must be non-negative");
        }
        authorities = canonical(authorities, AUTHORITY_ORDER, "authorities");
        references = canonical(references, REFERENCE_ORDER, "references");
        if (authorityCount < authorities.size() || referenceCount < references.size()) {
            throw new IllegalArgumentException("snapshot counts cannot be smaller than retained values");
        }
        if (complete && (authorityCount != authorities.size() || referenceCount != references.size())) {
            throw new IllegalArgumentException("complete snapshot counts must equal complete value lists");
        }
        if (!complete && !veto) {
            throw new IllegalArgumentException("an incomplete snapshot must veto deletion");
        }
        snapshotSha256 = GcReferenceQuery.requireSha256(snapshotSha256, "snapshotSha256");
        Checksum expected = PhysicalValueDigests.snapshot(
                domainId,
                protocolVersion,
                queryIdentitySha256,
                complete,
                veto,
                authorityCount,
                referenceCount,
                authorities,
                references);
        if (!expected.equals(snapshotSha256)) {
            throw new IllegalArgumentException("snapshotSha256 does not match canonical snapshot fields");
        }
    }

    public static GcReferenceSnapshot create(
            String domainId,
            int protocolVersion,
            Checksum queryIdentitySha256,
            boolean complete,
            boolean veto,
            long authorityCount,
            long referenceCount,
            List<GcAuthorityToken> authorities,
            List<GcReference> references) {
        List<GcAuthorityToken> exactAuthorities = List.copyOf(authorities);
        List<GcReference> exactReferences = List.copyOf(references);
        return new GcReferenceSnapshot(
                domainId,
                protocolVersion,
                queryIdentitySha256,
                complete,
                veto,
                authorityCount,
                referenceCount,
                exactAuthorities,
                exactReferences,
                PhysicalValueDigests.snapshot(
                        domainId,
                        protocolVersion,
                        queryIdentitySha256,
                        complete,
                        veto,
                        authorityCount,
                        referenceCount,
                        exactAuthorities,
                        exactReferences));
    }

    private static String requireDomainId(String value) {
        Objects.requireNonNull(value, "domainId");
        if (value.length() > 128 || !value.matches("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")) {
            throw new IllegalArgumentException("domainId is not canonical");
        }
        return value;
    }

    private static <T> List<T> canonical(List<T> values, Comparator<T> order, String name) {
        List<T> exact = List.copyOf(Objects.requireNonNull(values, name));
        for (int index = 1; index < exact.size(); index++) {
            if (order.compare(exact.get(index - 1), exact.get(index)) >= 0) {
                throw new IllegalArgumentException(name + " must be strictly sorted and unique");
            }
        }
        return exact;
    }
}
