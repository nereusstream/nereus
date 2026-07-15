/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Non-exceptional outcome of one attempt to establish an ACTIVE-to-MARKED fence. */
public record PhysicalGcMarkResult(
        PhysicalGcMarkStatus status,
        Optional<GcPlan> plan,
        OptionalLong retryAtMillis,
        Optional<GcReferenceCollectionStatus> domainStatus) {
    public PhysicalGcMarkResult {
        Objects.requireNonNull(status, "status");
        plan = Objects.requireNonNull(plan, "plan");
        retryAtMillis = Objects.requireNonNull(retryAtMillis, "retryAtMillis");
        domainStatus = Objects.requireNonNull(domainStatus, "domainStatus");
        if ((status == PhysicalGcMarkStatus.MARKED) != plan.isPresent()) {
            throw new IllegalArgumentException("only MARKED results carry a GC plan");
        }
        if ((status == PhysicalGcMarkStatus.NOT_YET_ELIGIBLE) != retryAtMillis.isPresent()) {
            throw new IllegalArgumentException("only not-yet-eligible results carry a retry timestamp");
        }
        if ((status == PhysicalGcMarkStatus.DOMAIN_BLOCKED) != domainStatus.isPresent()) {
            throw new IllegalArgumentException("only domain-blocked results carry a domain status");
        }
    }

    public static PhysicalGcMarkResult simple(PhysicalGcMarkStatus status) {
        return new PhysicalGcMarkResult(
                status, Optional.empty(), OptionalLong.empty(), Optional.empty());
    }

    public static PhysicalGcMarkResult notYetEligible(long retryAtMillis) {
        return new PhysicalGcMarkResult(
                PhysicalGcMarkStatus.NOT_YET_ELIGIBLE,
                Optional.empty(),
                OptionalLong.of(retryAtMillis),
                Optional.empty());
    }

    public static PhysicalGcMarkResult domainBlocked(GcReferenceCollectionStatus status) {
        if (status == GcReferenceCollectionStatus.CLEAR) {
            throw new IllegalArgumentException("CLEAR is not a blocking domain result");
        }
        return new PhysicalGcMarkResult(
                PhysicalGcMarkStatus.DOMAIN_BLOCKED,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(status));
    }

    public static PhysicalGcMarkResult marked(GcPlan plan) {
        return new PhysicalGcMarkResult(
                PhysicalGcMarkStatus.MARKED,
                Optional.of(plan),
                OptionalLong.empty(),
                Optional.empty());
    }
}
