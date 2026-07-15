/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.util.Objects;

/** Exact process-local discovery evidence for one ACTIVE physical root. */
public record GcCandidate(
        String candidateId,
        PhysicalObjectIdentity object,
        GcReferenceQuery referenceQuery,
        Checksum discoveryEvidenceSha256,
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
        if (referenceQuery.affectedStreams().size() > PhysicalGcConfig.MAX_STREAMS_PER_CANDIDATE) {
            throw new IllegalArgumentException("candidate affected-stream set exceeds the protocol bound");
        }
        if (rootMetadataVersion < 0 || rootLifecycleEpoch <= 0) {
            throw new IllegalArgumentException("candidate root version/epoch are invalid");
        }
        if (discoveredAtMillis < 0 || notBeforeMillis < discoveredAtMillis) {
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
                root.metadataVersion(),
                root.value().lifecycleEpoch(),
                discoveredAtMillis,
                notBeforeMillis);
    }
}
