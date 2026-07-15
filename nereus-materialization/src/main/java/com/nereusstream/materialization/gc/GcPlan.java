/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.util.List;
import java.util.Objects;

/** Canonical process-local plan reconstructed from authoritative facts after a root mark. */
public record GcPlan(
        String gcAttemptId,
        GcCandidate candidate,
        List<GcReferenceSnapshot> domainSnapshots,
        List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
        List<GcPlannedMetadataRemoval> plannedMetadataRemovals,
        Checksum referenceSetSha256,
        long markedRootMetadataVersion,
        long markedRootLifecycleEpoch,
        long deleteNotBeforeMillis) {
    public GcPlan {
        gcAttemptId = GcPlanValidation.requireBase32Id(gcAttemptId, "gcAttemptId");
        Objects.requireNonNull(candidate, "candidate");
        domainSnapshots = GcPlanValidation.canonical(
                domainSnapshots,
                GcPlanValidation.DOMAIN_ORDER,
                GcPlanValidation.MAX_REFERENCE_DOMAINS,
                "domainSnapshots");
        plannedProtectionRemovals = GcPlanValidation.canonicalAllowEmpty(
                plannedProtectionRemovals,
                GcPlanValidation.PROTECTION_ORDER,
                PhysicalGcConfig.MAX_DOMAIN_VALUES,
                "plannedProtectionRemovals");
        plannedMetadataRemovals = GcPlanValidation.canonicalAllowEmpty(
                plannedMetadataRemovals,
                GcPlanValidation.METADATA_ORDER,
                PhysicalGcConfig.MAX_DOMAIN_VALUES,
                "plannedMetadataRemovals");
        referenceSetSha256 = GcReferenceQuery.requireSha256(
                referenceSetSha256, "referenceSetSha256");
        validateSnapshots(candidate, domainSnapshots);
        validateProtectionObjects(candidate, plannedProtectionRemovals);
        Checksum expected = GcPlanValidation.referenceSetSha256(
                candidate.referenceQuery(),
                domainSnapshots,
                plannedProtectionRemovals,
                plannedMetadataRemovals);
        if (!expected.equals(referenceSetSha256)) {
            throw new IllegalArgumentException("referenceSetSha256 does not match canonical plan facts");
        }
        if (!matchesMarkedRoot(candidate, markedRootMetadataVersion, markedRootLifecycleEpoch)) {
            throw new IllegalArgumentException("marked root version/epoch do not match the candidate root source");
        }
        if (deleteNotBeforeMillis < candidate.notBeforeMillis()) {
            throw new IllegalArgumentException("deleteNotBeforeMillis precedes candidate eligibility");
        }
    }

    public static Checksum computeReferenceSetSha256(
            PhysicalGcConfig config,
            GcCandidate candidate,
            List<GcReferenceSnapshot> domainSnapshots,
            List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
            List<GcPlannedMetadataRemoval> plannedMetadataRemovals) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(candidate, "candidate");
        GcReferenceQuery query = candidate.referenceQuery();
        List<GcReferenceSnapshot> snapshots = validateForConfig(config, query, domainSnapshots);
        List<GcPlannedProtectionRemoval> protections = GcPlanValidation.canonicalAllowEmpty(
                plannedProtectionRemovals,
                GcPlanValidation.PROTECTION_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "plannedProtectionRemovals");
        validateProtectionObjects(candidate, protections);
        List<GcPlannedMetadataRemoval> removals = GcPlanValidation.canonicalAllowEmpty(
                plannedMetadataRemovals,
                GcPlanValidation.METADATA_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "plannedMetadataRemovals");
        return GcPlanValidation.referenceSetSha256(query, snapshots, protections, removals);
    }

    public static GcPlan fromMarkedRoot(
            PhysicalGcConfig config,
            String gcAttemptId,
            GcCandidate candidate,
            List<GcReferenceSnapshot> domainSnapshots,
            List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
            List<GcPlannedMetadataRemoval> plannedMetadataRemovals,
            VersionedPhysicalObjectRoot markedRoot) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(markedRoot, "markedRoot");
        List<GcReferenceSnapshot> snapshots = validateForConfig(
                config, candidate.referenceQuery(), domainSnapshots);
        List<GcPlannedProtectionRemoval> protections = GcPlanValidation.canonicalAllowEmpty(
                plannedProtectionRemovals,
                GcPlanValidation.PROTECTION_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "plannedProtectionRemovals");
        List<GcPlannedMetadataRemoval> removals = GcPlanValidation.canonicalAllowEmpty(
                plannedMetadataRemovals,
                GcPlanValidation.METADATA_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "plannedMetadataRemovals");
        validateProtectionObjects(candidate, protections);
        Checksum digest = GcPlanValidation.referenceSetSha256(
                candidate.referenceQuery(), snapshots, protections, removals);
        if (markedRoot.value().lifecycle() != PhysicalObjectLifecycle.MARKED
                || !PhysicalObjectIdentityMatches.exact(candidate, markedRoot)
                || !markedRoot.value().gcAttemptId().equals(gcAttemptId)
                || !markedRoot.value().referenceSetSha256().equals(digest.value())) {
            throw new IllegalArgumentException("marked root does not carry the exact candidate plan identity");
        }
        return new GcPlan(
                gcAttemptId,
                candidate,
                snapshots,
                protections,
                removals,
                digest,
                markedRoot.metadataVersion(),
                markedRoot.value().lifecycleEpoch(),
                markedRoot.value().deleteNotBeforeMillis());
    }

    private static List<GcReferenceSnapshot> validateForConfig(
            PhysicalGcConfig config,
            GcReferenceQuery query,
            List<GcReferenceSnapshot> snapshots) {
        List<GcReferenceSnapshot> exact = GcPlanValidation.canonical(
                snapshots,
                GcPlanValidation.DOMAIN_ORDER,
                GcPlanValidation.MAX_REFERENCE_DOMAINS,
                "domainSnapshots");
        for (GcReferenceSnapshot snapshot : exact) {
            if (snapshot.authorityCount() > config.maxAuthoritiesPerDomainSnapshot()
                    || snapshot.referenceCount() > config.maxReferencesPerDomainSnapshot()) {
                throw new IllegalArgumentException("reference-domain snapshot exceeds configured bounds");
            }
        }
        validateSnapshots(query, exact);
        return exact;
    }

    private static void validateSnapshots(
            GcCandidate candidate, List<GcReferenceSnapshot> snapshots) {
        validateSnapshots(candidate.referenceQuery(), snapshots);
    }

    private static void validateSnapshots(
            GcReferenceQuery query, List<GcReferenceSnapshot> snapshots) {
        for (GcReferenceSnapshot snapshot : snapshots) {
            if (!snapshot.queryIdentitySha256().equals(query.queryIdentitySha256())
                    || !snapshot.complete()
                    || snapshot.veto()) {
                throw new IllegalArgumentException(
                        "every plan domain snapshot must be complete, non-vetoing, and query-bound");
            }
        }
    }

    private static void validateProtectionObjects(
            GcCandidate candidate, List<GcPlannedProtectionRemoval> protections) {
        java.util.HashSet<com.nereusstream.metadata.oxia.ObjectProtectionIdentity> identities =
                new java.util.HashSet<>();
        for (GcPlannedProtectionRemoval protection : protections) {
            if (!protection.identity().object().equals(candidate.object().objectKeyHash())
                    || protection.protection().value().rootLifecycleEpoch()
                            != candidate.activeRootLifecycleEpoch()) {
                throw new IllegalArgumentException(
                        "planned protection does not belong to the candidate ACTIVE root");
            }
            if (!identities.add(protection.identity())) {
                throw new IllegalArgumentException("planned protection identities must be unique");
            }
        }
    }

    private static boolean matchesMarkedRoot(
            GcCandidate candidate,
            long markedRootMetadataVersion,
            long markedRootLifecycleEpoch) {
        return switch (candidate.rootState()) {
            case ACTIVE_DISCOVERY -> markedRootMetadataVersion > candidate.rootMetadataVersion()
                    && markedRootLifecycleEpoch == Math.addExact(candidate.rootLifecycleEpoch(), 1);
            case MARKED_RECOVERY -> markedRootMetadataVersion == candidate.rootMetadataVersion()
                    && markedRootLifecycleEpoch == candidate.rootLifecycleEpoch();
        };
    }

    private static final class PhysicalObjectIdentityMatches {
        private PhysicalObjectIdentityMatches() {
        }

        private static boolean exact(GcCandidate candidate, VersionedPhysicalObjectRoot root) {
            return candidate.object().equals(
                    com.nereusstream.core.physical.PhysicalObjectIdentity.from(root.value()));
        }
    }
}
