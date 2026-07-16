/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Exact protocol metadata observed while classifying one registered stream.
 *
 * <p>A non-live snapshot still carries the binding/topic presence or absence
 * authorities that made the classification. Re-capturing this value therefore
 * detects a concurrent recreation instead of treating a stale registration as
 * durable liveness truth.
 */
public record GenerationProjectionAuthoritySnapshot(
        LiveProjectionSubject subject,
        boolean live,
        Optional<ManagedLedgerProjectionIdentity> managedLedgerIdentity,
        List<GcAuthorityToken> authorities) {
    private static final Comparator<GcAuthorityToken> AUTHORITY_ORDER = Comparator
            .comparing(GcAuthorityToken::authorityKey)
            .thenComparingLong(GcAuthorityToken::metadataVersion)
            .thenComparing(value -> value.identitySha256().value());

    public GenerationProjectionAuthoritySnapshot {
        Objects.requireNonNull(subject, "subject");
        managedLedgerIdentity = Objects.requireNonNull(
                managedLedgerIdentity, "managedLedgerIdentity");
        ArrayList<GcAuthorityToken> canonical =
                new ArrayList<>(Objects.requireNonNull(authorities, "authorities"));
        if (canonical.isEmpty() || canonical.size() > 8 || canonical.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(
                    "projection authority snapshot must contain between one and eight authorities");
        }
        canonical.sort(AUTHORITY_ORDER);
        HashSet<String> keys = new HashSet<>();
        for (GcAuthorityToken authority : canonical) {
            if (!keys.add(authority.authorityKey())) {
                throw new IllegalArgumentException(
                        "projection authority snapshot contains duplicate authority keys");
            }
        }
        authorities = List.copyOf(canonical);
        if (live != managedLedgerIdentity.isPresent()) {
            throw new IllegalArgumentException(
                    "only a live projection snapshot may expose its managed-ledger identity");
        }
        managedLedgerIdentity.ifPresent(identity -> {
            if (!identity.streamId().equals(subject.streamId().value())) {
                throw new IllegalArgumentException(
                        "projection identity belongs to another stream");
            }
        });
    }
}
