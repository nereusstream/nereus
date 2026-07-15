/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable result of one ordered local reference-domain registry pass. */
public final class GcReferenceCollection {
    private final GcReferenceQuery query;
    private final List<GcReferenceSnapshot> snapshots;
    private final GcReferenceCollectionStatus status;
    private final Optional<String> blockingDomainId;

    GcReferenceCollection(
            GcReferenceQuery query,
            List<GcReferenceSnapshot> snapshots,
            GcReferenceCollectionStatus status,
            Optional<String> blockingDomainId) {
        this.query = Objects.requireNonNull(query, "query");
        this.snapshots = List.copyOf(Objects.requireNonNull(snapshots, "snapshots"));
        this.status = Objects.requireNonNull(status, "status");
        this.blockingDomainId = Objects.requireNonNull(blockingDomainId, "blockingDomainId");
        if (this.snapshots.isEmpty()) {
            throw new IllegalArgumentException("a reference collection must include at least one domain result");
        }
        for (int index = 0; index < this.snapshots.size(); index++) {
            GcReferenceSnapshot snapshot = this.snapshots.get(index);
            if (!snapshot.queryIdentitySha256().equals(query.queryIdentitySha256())) {
                throw new IllegalArgumentException("reference collection contains a different query identity");
            }
            if (index > 0 && GcPlanValidation.DOMAIN_ORDER.compare(
                    this.snapshots.get(index - 1), snapshot) >= 0) {
                throw new IllegalArgumentException("reference collection snapshots are not canonical");
            }
        }
        if ((status == GcReferenceCollectionStatus.CLEAR) == blockingDomainId.isPresent()) {
            throw new IllegalArgumentException("reference collection blocking domain does not match status");
        }
        blockingDomainId.ifPresent(domain -> {
            if (this.snapshots.stream().noneMatch(snapshot -> snapshot.domainId().equals(domain))) {
                throw new IllegalArgumentException("blocking domain has no captured snapshot");
            }
        });
        if (status == GcReferenceCollectionStatus.CLEAR
                && this.snapshots.stream().anyMatch(snapshot -> !snapshot.complete() || snapshot.veto())) {
            throw new IllegalArgumentException("a CLEAR collection contains a vetoing or incomplete snapshot");
        }
    }

    public GcReferenceQuery query() {
        return query;
    }

    public List<GcReferenceSnapshot> snapshots() {
        return snapshots;
    }

    public GcReferenceCollectionStatus status() {
        return status;
    }

    public Optional<String> blockingDomainId() {
        return blockingDomainId;
    }

    public boolean clear() {
        return status == GcReferenceCollectionStatus.CLEAR;
    }

    public Optional<GcReferenceSnapshot> snapshot(String domainId) {
        Objects.requireNonNull(domainId, "domainId");
        return snapshots.stream().filter(value -> value.domainId().equals(domainId)).findFirst();
    }
}
