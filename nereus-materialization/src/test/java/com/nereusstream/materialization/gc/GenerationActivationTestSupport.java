/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import java.util.List;

final class GenerationActivationTestSupport {
    private GenerationActivationTestSupport() {
    }

    static List<GcReferenceDomainVersion> installedDomains() {
        return F4MetadataTestValues.referenceDomains().stream()
                .map(value -> new GcReferenceDomainVersion(
                        value.domainId(), value.protocolVersion()))
                .toList();
    }

    static GenerationProtocolActivationRecord publication(
            GenerationProtocolActivationRecord current) {
        return copy(
                current,
                true,
                false,
                7,
                GenerationBackfillProofRecord.incomplete(7),
                GenerationBackfillProofRecord.incomplete(7),
                GenerationBackfillProofRecord.incomplete(7),
                "",
                1_100);
    }

    static GenerationProtocolActivationRecord deletion(
            GenerationProtocolActivationRecord current,
            long readinessEpoch,
            String objectCapabilitySha256,
            long updatedAtMillis) {
        return copy(
                current,
                true,
                true,
                readinessEpoch,
                complete(
                        F4MetadataTestValues.ATTEMPT,
                        readinessEpoch,
                        F4MetadataTestValues.HASH_A,
                        updatedAtMillis - 3),
                complete(
                        F4MetadataTestValues.CLAIM,
                        readinessEpoch,
                        F4MetadataTestValues.HASH_B,
                        updatedAtMillis - 2),
                complete(
                        F4MetadataTestValues.PUBLICATION,
                        readinessEpoch,
                        F4MetadataTestValues.HASH_C,
                        updatedAtMillis - 1),
                objectCapabilitySha256,
                updatedAtMillis);
    }

    private static GenerationBackfillProofRecord complete(
            String runId,
            long readinessEpoch,
            String coverageSha256,
            long completedAtMillis) {
        return new GenerationBackfillProofRecord(
                runId,
                readinessEpoch,
                coverageSha256,
                true,
                completedAtMillis);
    }

    private static GenerationProtocolActivationRecord copy(
            GenerationProtocolActivationRecord current,
            boolean publicationEnabled,
            boolean deletionEnabled,
            long readinessEpoch,
            GenerationBackfillProofRecord registration,
            GenerationBackfillProofRecord roots,
            GenerationBackfillProofRecord cursors,
            String objectCapabilitySha256,
            long updatedAtMillis) {
        boolean alreadyActive =
                current.lifecycle() == GenerationProtocolActivationLifecycle.ACTIVE;
        return new GenerationProtocolActivationRecord(
                current.schemaVersion(),
                current.protocolVersion(),
                GenerationProtocolActivationLifecycle.ACTIVE,
                publicationEnabled,
                deletionEnabled,
                deletionEnabled,
                readinessEpoch,
                current.requiredReferenceDomains(),
                registration,
                roots,
                cursors,
                objectCapabilitySha256,
                current.activatingBrokerRunId(),
                current.preparedAtMillis(),
                alreadyActive ? current.activatedAtMillis() : 1_050,
                updatedAtMillis,
                0);
    }
}
