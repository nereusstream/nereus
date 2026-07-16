/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.util.List;
import java.util.Objects;

/** Durable cluster authority for generation publication/deletion and the exact GC domain set. */
public record GenerationProtocolActivationRecord(
        int schemaVersion,
        int protocolVersion,
        GenerationProtocolActivationLifecycle lifecycle,
        boolean publicationEnabled,
        boolean physicalDeleteEnabled,
        boolean cursorSnapshotDeleteEnabled,
        long brokerCapabilityReadinessEpoch,
        List<ReferenceDomainVersionRecord> requiredReferenceDomains,
        GenerationBackfillProofRecord streamRegistrationBackfill,
        GenerationBackfillProofRecord physicalRootBackfill,
        GenerationBackfillProofRecord cursorSnapshotBackfill,
        String objectStoreCapabilitySha256,
        String activatingBrokerRunId,
        long preparedAtMillis,
        long activatedAtMillis,
        long updatedAtMillis,
        long metadataVersion) {
    public static final int PROTOCOL_VERSION = 1;
    public static final int MAX_REFERENCE_DOMAINS = 32;

    public GenerationProtocolActivationRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("protocolVersion must be 1");
        }
        Objects.requireNonNull(lifecycle, "lifecycle");
        requiredReferenceDomains = F4RecordValidation.immutableBoundedList(
                requiredReferenceDomains,
                MAX_REFERENCE_DOMAINS,
                "requiredReferenceDomains");
        if (requiredReferenceDomains.isEmpty()) {
            throw new IllegalArgumentException(
                    "requiredReferenceDomains must be non-empty");
        }
        for (int index = 1; index < requiredReferenceDomains.size(); index++) {
            if (requiredReferenceDomains.get(index - 1)
                            .compareTo(requiredReferenceDomains.get(index))
                    >= 0) {
                throw new IllegalArgumentException(
                        "requiredReferenceDomains must be strictly sorted and unique");
            }
        }
        Objects.requireNonNull(streamRegistrationBackfill, "streamRegistrationBackfill");
        Objects.requireNonNull(physicalRootBackfill, "physicalRootBackfill");
        Objects.requireNonNull(cursorSnapshotBackfill, "cursorSnapshotBackfill");
        F4RecordValidation.requireNonNegative(
                brokerCapabilityReadinessEpoch,
                "brokerCapabilityReadinessEpoch");
        requireBackfillEpoch(streamRegistrationBackfill, brokerCapabilityReadinessEpoch);
        requireBackfillEpoch(physicalRootBackfill, brokerCapabilityReadinessEpoch);
        requireBackfillEpoch(cursorSnapshotBackfill, brokerCapabilityReadinessEpoch);
        objectStoreCapabilitySha256 = F4RecordValidation.requireOptionalSha256(
                objectStoreCapabilitySha256, "objectStoreCapabilitySha256");
        activatingBrokerRunId = F4RecordValidation.requireBase32Id(
                activatingBrokerRunId, "activatingBrokerRunId");
        F4RecordValidation.requireNonNegative(preparedAtMillis, "preparedAtMillis");
        F4RecordValidation.requireNonNegative(activatedAtMillis, "activatedAtMillis");
        F4RecordValidation.requireNonNegative(updatedAtMillis, "updatedAtMillis");
        F4RecordValidation.requireMetadataVersion(metadataVersion);
        if (updatedAtMillis < preparedAtMillis) {
            throw new IllegalArgumentException(
                    "updatedAtMillis cannot precede preparedAtMillis");
        }
        if (lifecycle == GenerationProtocolActivationLifecycle.PREPARED) {
            if (publicationEnabled
                    || physicalDeleteEnabled
                    || cursorSnapshotDeleteEnabled
                    || activatedAtMillis != 0) {
                throw new IllegalArgumentException(
                        "PREPARED activation cannot enable capabilities or carry activatedAtMillis");
            }
        } else if (!publicationEnabled
                || activatedAtMillis < preparedAtMillis
                || activatedAtMillis > updatedAtMillis) {
            throw new IllegalArgumentException(
                    "ACTIVE activation requires publication and a valid activation time");
        }
        if (physicalDeleteEnabled != cursorSnapshotDeleteEnabled) {
            throw new IllegalArgumentException(
                    "V1 physical and cursor-snapshot deletion must activate together");
        }
        if (physicalDeleteEnabled
                && (!streamRegistrationBackfill.complete()
                        || !physicalRootBackfill.complete()
                        || !cursorSnapshotBackfill.complete()
                        || objectStoreCapabilitySha256.isEmpty())) {
            throw new IllegalArgumentException(
                    "physical deletion requires all backfills and object-store capability");
        }
    }

    public GenerationProtocolActivationRecord withMetadataVersion(long version) {
        return new GenerationProtocolActivationRecord(
                schemaVersion,
                protocolVersion,
                lifecycle,
                publicationEnabled,
                physicalDeleteEnabled,
                cursorSnapshotDeleteEnabled,
                brokerCapabilityReadinessEpoch,
                requiredReferenceDomains,
                streamRegistrationBackfill,
                physicalRootBackfill,
                cursorSnapshotBackfill,
                objectStoreCapabilitySha256,
                activatingBrokerRunId,
                preparedAtMillis,
                activatedAtMillis,
                updatedAtMillis,
                version);
    }

    private static void requireBackfillEpoch(
            GenerationBackfillProofRecord proof,
            long brokerCapabilityReadinessEpoch) {
        if (proof.brokerReadinessEpoch() > brokerCapabilityReadinessEpoch) {
            throw new IllegalArgumentException(
                    "backfill proof cannot be newer than the broker readiness epoch");
        }
        if (proof.complete()
                && proof.brokerReadinessEpoch()
                        != brokerCapabilityReadinessEpoch) {
            throw new IllegalArgumentException(
                    "complete backfill proof belongs to another broker readiness epoch");
        }
    }
}
