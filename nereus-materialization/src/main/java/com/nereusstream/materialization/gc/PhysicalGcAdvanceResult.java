/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Outcome of drain revalidation and the optional MARKED-to-DELETING intent CAS. */
public record PhysicalGcAdvanceResult(
        PhysicalGcAdvanceStatus status,
        Optional<VersionedPhysicalObjectRoot> root,
        OptionalLong retryAtMillis) {
    public PhysicalGcAdvanceResult {
        Objects.requireNonNull(status, "status");
        root = Objects.requireNonNull(root, "root");
        retryAtMillis = Objects.requireNonNull(retryAtMillis, "retryAtMillis");
        boolean waiting = status == PhysicalGcAdvanceStatus.WAITING_FOR_GRACE
                || status == PhysicalGcAdvanceStatus.WAITING_FOR_READERS;
        if (waiting != retryAtMillis.isPresent()) {
            throw new IllegalArgumentException("retry timestamp does not match waiting status");
        }
        boolean carriesRoot = status == PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED
                || status == PhysicalGcAdvanceStatus.DELETE_INTENT;
        if (carriesRoot != root.isPresent()) {
            throw new IllegalArgumentException("advance result root does not match terminal mutation status");
        }
    }

    public static PhysicalGcAdvanceResult simple(PhysicalGcAdvanceStatus status) {
        return new PhysicalGcAdvanceResult(status, Optional.empty(), OptionalLong.empty());
    }

    public static PhysicalGcAdvanceResult waiting(
            PhysicalGcAdvanceStatus status, long retryAtMillis) {
        return new PhysicalGcAdvanceResult(
                status, Optional.empty(), OptionalLong.of(retryAtMillis));
    }

    public static PhysicalGcAdvanceResult withRoot(
            PhysicalGcAdvanceStatus status, VersionedPhysicalObjectRoot root) {
        return new PhysicalGcAdvanceResult(
                status, Optional.of(root), OptionalLong.empty());
    }
}
