/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.core.physical.GcAuthorityToken;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Exact bounded protocol-reference proof for one stream-registration retirement attempt. */
public record StreamRetirementReferenceAuthoritySnapshot(
        LiveProjectionSubject subject,
        boolean complete,
        boolean referenceFree,
        long authorityCount,
        long liveReferenceCount,
        List<GcAuthorityToken> authorities) {
    private static final Comparator<GcAuthorityToken> AUTHORITY_ORDER = Comparator
            .comparing(GcAuthorityToken::authorityKey)
            .thenComparingLong(GcAuthorityToken::metadataVersion)
            .thenComparing(value -> value.identitySha256().value());

    public StreamRetirementReferenceAuthoritySnapshot {
        Objects.requireNonNull(subject, "subject");
        if (authorityCount < 0 || liveReferenceCount < 0) {
            throw new IllegalArgumentException("stream-retirement authority counts must be non-negative");
        }
        ArrayList<GcAuthorityToken> canonical =
                new ArrayList<>(Objects.requireNonNull(authorities, "authorities"));
        if (canonical.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("stream-retirement authorities cannot contain null");
        }
        canonical.sort(AUTHORITY_ORDER);
        HashSet<String> keys = new HashSet<>();
        for (GcAuthorityToken authority : canonical) {
            if (!keys.add(authority.authorityKey())) {
                throw new IllegalArgumentException(
                        "stream-retirement authorities contain duplicate keys");
            }
        }
        authorities = List.copyOf(canonical);
        if (authorityCount < authorities.size()) {
            throw new IllegalArgumentException(
                    "authorityCount cannot be smaller than retained authorities");
        }
        if (complete && authorityCount != authorities.size()) {
            throw new IllegalArgumentException(
                    "a complete stream-retirement snapshot must retain every authority");
        }
        if (referenceFree != (complete && liveReferenceCount == 0)) {
            throw new IllegalArgumentException(
                    "referenceFree requires a complete zero-reference snapshot");
        }
    }

    public static StreamRetirementReferenceAuthoritySnapshot complete(
            LiveProjectionSubject subject,
            long liveReferenceCount,
            List<GcAuthorityToken> authorities) {
        List<GcAuthorityToken> exact = List.copyOf(authorities);
        return new StreamRetirementReferenceAuthoritySnapshot(
                subject,
                true,
                liveReferenceCount == 0,
                exact.size(),
                liveReferenceCount,
                exact);
    }
}
