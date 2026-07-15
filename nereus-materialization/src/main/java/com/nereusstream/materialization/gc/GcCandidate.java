/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.util.Objects;

/** Exact process-local discovery evidence from one ACTIVE root or one MARKED recovery root. */
public record GcCandidate(
        String candidateId,
        PhysicalObjectIdentity object,
        GcReferenceQuery referenceQuery,
        Checksum discoveryEvidenceSha256,
        GcCandidateRootState rootState,
        long rootMetadataVersion,
        long rootLifecycleEpoch,
        long discoveredAtMillis,
        long notBeforeMillis) {
    public GcCandidate {
        candidateId = GcPlanValidation.requireBase32Id(candidateId, "candidateId");
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(referenceQuery, "referenceQuery");
        discoveryEvidenceSha256 = GcReferenceQuery.requireSha256(
                discoveryEvidenceSha256, "discoveryEvidenceSha256");
        if (!referenceQuery.object().equals(object)
                || !referenceQuery.candidateEvidenceSha256().equals(discoveryEvidenceSha256)) {
            throw new IllegalArgumentException("candidate object/query/discovery evidence do not match");
        }
        Objects.requireNonNull(rootState, "rootState");
        if (referenceQuery.affectedStreams().size() > PhysicalGcConfig.MAX_STREAMS_PER_CANDIDATE) {
            throw new IllegalArgumentException("candidate affected-stream set exceeds the protocol bound");
        }
        if (rootMetadataVersion < 0 || rootLifecycleEpoch <= 0) {
            throw new IllegalArgumentException("candidate root version/epoch are invalid");
        }
        if (rootState == GcCandidateRootState.MARKED_RECOVERY && rootLifecycleEpoch < 2) {
            throw new IllegalArgumentException("a MARKED recovery root must follow a positive ACTIVE epoch");
        }
        if (discoveredAtMillis < 0 || notBeforeMillis < 0) {
            throw new IllegalArgumentException("candidate timestamps are invalid");
        }
    }

    public static GcCandidate fromActiveRoot(
            PhysicalGcConfig config,
            String candidateId,
            VersionedPhysicalObjectRoot root,
            GcReferenceQuery referenceQuery,
            Checksum discoveryEvidenceSha256,
            long discoveredAtMillis,
            long notBeforeMillis) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(root, "root");
        if (root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE) {
            throw new IllegalArgumentException("a GC candidate requires an ACTIVE root");
        }
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
        if (!object.equals(referenceQuery.object())) {
            throw new IllegalArgumentException("reference query does not identify the ACTIVE root");
        }
        if (referenceQuery.affectedStreams().size() > config.maxStreamsPerCandidate()) {
            throw new IllegalArgumentException("candidate affected-stream set exceeds configured bounds");
        }
        if (discoveredAtMillis < root.value().createdAtMillis()
                || notBeforeMillis < root.value().orphanNotBeforeMillis()) {
            throw new IllegalArgumentException("candidate predates the root's durable eligibility facts");
        }
        return new GcCandidate(
                candidateId,
                object,
                referenceQuery,
                discoveryEvidenceSha256,
                GcCandidateRootState.ACTIVE_DISCOVERY,
                root.metadataVersion(),
                root.value().lifecycleEpoch(),
                discoveredAtMillis,
                notBeforeMillis);
    }

    public static GcCandidate fromMarkedRoot(
            PhysicalGcConfig config,
            String candidateId,
            VersionedPhysicalObjectRoot root,
            GcReferenceQuery referenceQuery,
            Checksum discoveryEvidenceSha256,
            long discoveredAtMillis) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(root, "root");
        if (root.value().lifecycle() != PhysicalObjectLifecycle.MARKED) {
            throw new IllegalArgumentException("GC recovery requires a MARKED root");
        }
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
        if (!object.equals(referenceQuery.object())) {
            throw new IllegalArgumentException("reference query does not identify the MARKED root");
        }
        if (referenceQuery.affectedStreams().size() > config.maxStreamsPerCandidate()) {
            throw new IllegalArgumentException("candidate affected-stream set exceeds configured bounds");
        }
        if (discoveredAtMillis < root.value().createdAtMillis()) {
            throw new IllegalArgumentException("candidate predates the root's durable facts");
        }
        return new GcCandidate(
                candidateId,
                object,
                referenceQuery,
                discoveryEvidenceSha256,
                GcCandidateRootState.MARKED_RECOVERY,
                root.metadataVersion(),
                root.value().lifecycleEpoch(),
                discoveredAtMillis,
                root.value().deleteNotBeforeMillis());
    }

    /** Epoch under which removable protections had to complete their ACTIVE-root handshake. */
    public long activeRootLifecycleEpoch() {
        return switch (rootState) {
            case ACTIVE_DISCOVERY -> rootLifecycleEpoch;
            case MARKED_RECOVERY -> Math.subtractExact(rootLifecycleEpoch, 1);
        };
    }
}
