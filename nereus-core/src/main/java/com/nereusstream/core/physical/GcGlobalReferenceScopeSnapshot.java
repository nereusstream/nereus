/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.StreamId;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Canonical activation/backfill-gated stream enumeration for an ownerless reference scan. */
public record GcGlobalReferenceScopeSnapshot(
        boolean complete,
        long streamCount,
        long authorityCount,
        List<StreamId> streams,
        List<GcAuthorityToken> authorities) {
    private static final Comparator<GcAuthorityToken> AUTHORITY_ORDER = Comparator
            .comparing(GcAuthorityToken::authorityKey)
            .thenComparingLong(GcAuthorityToken::metadataVersion)
            .thenComparing(value -> value.identitySha256().value());

    public GcGlobalReferenceScopeSnapshot {
        if (streamCount < 0 || authorityCount < 0) {
            throw new IllegalArgumentException("global scope counts must be non-negative");
        }
        streams = List.copyOf(Objects.requireNonNull(streams, "streams"));
        authorities = List.copyOf(Objects.requireNonNull(authorities, "authorities"));
        if (streamCount < streams.size() || authorityCount < authorities.size()) {
            throw new IllegalArgumentException(
                    "global scope counts cannot be smaller than retained values");
        }
        for (int index = 1; index < streams.size(); index++) {
            if (streams.get(index - 1).value().compareTo(streams.get(index).value()) >= 0) {
                throw new IllegalArgumentException(
                        "global scope streams must be strictly sorted and unique");
            }
        }
        for (int index = 1; index < authorities.size(); index++) {
            if (AUTHORITY_ORDER.compare(authorities.get(index - 1), authorities.get(index)) >= 0) {
                throw new IllegalArgumentException(
                        "global scope authorities must be strictly sorted and unique");
            }
        }
        if (complete
                && (streamCount != streams.size()
                        || authorityCount != authorities.size())) {
            throw new IllegalArgumentException(
                    "complete global scope counts must equal retained values");
        }
    }

    public static GcGlobalReferenceScopeSnapshot incomplete() {
        return new GcGlobalReferenceScopeSnapshot(
                false, 0, 0, List.of(), List.of());
    }

    public void contributeTo(GcReferenceSnapshotBuilder builder) {
        Objects.requireNonNull(builder, "builder");
        authorities.forEach(builder::addAuthority);
        if (!complete) {
            builder.markIncomplete();
        }
    }
}
