/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.util.List;
import java.util.Objects;

/** Canonical process-local plan reconstructed from authoritative facts after a root mark. */
public record GcPlan(
        String gcAttemptId,
        GcCandidate candidate,
        List<GcReferenceSnapshot> domainSnapshots,
        List<ObjectProtectionIdentity> plannedProtectionRemovals,
        List<String> plannedMetadataKeys,
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
        plannedMetadataKeys = GcPlanValidation.canonicalMetadataKeys(
                plannedMetadataKeys, PhysicalGcConfig.MAX_DOMAIN_VALUES);
        referenceSetSha256 = GcReferenceQuery.requireSha256(
                referenceSetSha256, "referenceSetSha256");
        validateSnapshots(candidate, domainSnapshots);
        validateProtectionObjects(candidate, plannedProtectionRemovals);
        Checksum expected = GcPlanValidation.referenceSetSha256(
                candidate.referenceQuery(),
                domainSnapshots,
                plannedProtectionRemovals,
                plannedMetadataKeys);
        if (!expected.equals(referenceSetSha256)) {
            throw new IllegalArgumentException("referenceSetSha256 does not match canonical plan facts");
        }
        if (markedRootMetadataVersion <= candidate.rootMetadataVersion()
                || markedRootLifecycleEpoch != Math.addExact(candidate.rootLifecycleEpoch(), 1)) {
            throw new IllegalArgumentException("marked root version/epoch do not follow the candidate ACTIVE root");
        }
        if (deleteNotBeforeMillis < candidate.notBeforeMillis()) {
            throw new IllegalArgumentException("deleteNotBeforeMillis precedes candidate eligibility");
        }
    }

    public static Checksum computeReferenceSetSha256(
            PhysicalGcConfig config,
            GcReferenceQuery query,
            List<GcReferenceSnapshot> domainSnapshots,
            List<ObjectProtectionIdentity> plannedProtectionRemovals,
            List<String> plannedMetadataKeys) {
        Objects.requireNonNull(config, "config");
        List<GcReferenceSnapshot> snapshots = validateForConfig(config, query, domainSnapshots);
        List<ObjectProtectionIdentity> protections = GcPlanValidation.canonicalAllowEmpty(
                plannedProtectionRemovals,
                GcPlanValidation.PROTECTION_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "plannedProtectionRemovals");
        validateProtectionObjects(query, protections);
        List<String> keys = GcPlanValidation.canonicalMetadataKeys(
                plannedMetadataKeys, config.maxReferencesPerDomainSnapshot());
        return GcPlanValidation.referenceSetSha256(query, snapshots, protections, keys);
    }

    public static GcPlan fromMarkedRoot(
            PhysicalGcConfig config,
            String gcAttemptId,
            GcCandidate candidate,
            List<GcReferenceSnapshot> domainSnapshots,
            List<ObjectProtectionIdentity> plannedProtectionRemovals,
            List<String> plannedMetadataKeys,
            VersionedPhysicalObjectRoot markedRoot) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(markedRoot, "markedRoot");
        List<GcReferenceSnapshot> snapshots = validateForConfig(
                config, candidate.referenceQuery(), domainSnapshots);
        List<ObjectProtectionIdentity> protections = GcPlanValidation.canonicalAllowEmpty(
                plannedProtectionRemovals,
                GcPlanValidation.PROTECTION_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "plannedProtectionRemovals");
        List<String> keys = GcPlanValidation.canonicalMetadataKeys(
                plannedMetadataKeys, config.maxReferencesPerDomainSnapshot());
        Checksum digest = GcPlanValidation.referenceSetSha256(
                candidate.referenceQuery(), snapshots, protections, keys);
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
                keys,
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
            GcCandidate candidate, List<ObjectProtectionIdentity> protections) {
        validateProtectionObjects(candidate.referenceQuery(), protections);
    }

    private static void validateProtectionObjects(
            GcReferenceQuery query, List<ObjectProtectionIdentity> protections) {
        if (protections.stream().anyMatch(value -> !value.object().equals(query.object().objectKeyHash()))) {
            throw new IllegalArgumentException("planned protection does not belong to the candidate object");
        }
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
