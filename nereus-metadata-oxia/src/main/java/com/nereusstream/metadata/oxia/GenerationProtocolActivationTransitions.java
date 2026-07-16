/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;

/** Closed monotonic transition guard for the cluster generation activation record. */
final class GenerationProtocolActivationTransitions {
    private GenerationProtocolActivationTransitions() {
    }

    static void requireValidReplacement(
            GenerationProtocolActivationRecord current,
            GenerationProtocolActivationRecord replacement) {
        if (current.schemaVersion() != replacement.schemaVersion()
                || current.protocolVersion() != replacement.protocolVersion()
                || !current.activatingBrokerRunId().equals(
                        replacement.activatingBrokerRunId())
                || current.preparedAtMillis() != replacement.preparedAtMillis()) {
            throw invariant(
                    "generation activation CAS changed immutable protocol identity");
        }
        if (replacement.updatedAtMillis() < current.updatedAtMillis()
                || replacement.brokerCapabilityReadinessEpoch()
                        < current.brokerCapabilityReadinessEpoch()) {
            throw invariant(
                    "generation activation CAS moved time or readiness epoch backward");
        }
        if (current.lifecycle() == GenerationProtocolActivationLifecycle.ACTIVE
                && replacement.lifecycle()
                        != GenerationProtocolActivationLifecycle.ACTIVE) {
            throw invariant("ACTIVE generation protocol cannot return to PREPARED");
        }
        if (current.publicationEnabled() && !replacement.publicationEnabled()
                || current.physicalDeleteEnabled()
                        && !replacement.physicalDeleteEnabled()
                || current.cursorSnapshotDeleteEnabled()
                        && !replacement.cursorSnapshotDeleteEnabled()) {
            throw invariant("generation activation capability bits are monotonic");
        }
        if (current.lifecycle() == GenerationProtocolActivationLifecycle.ACTIVE
                && current.activatedAtMillis()
                        != replacement.activatedAtMillis()) {
            throw invariant("generation activation CAS changed activatedAtMillis");
        }
        boolean mutablePreparedDomainSet = current.lifecycle()
                        == GenerationProtocolActivationLifecycle.PREPARED
                && replacement.lifecycle()
                        == GenerationProtocolActivationLifecycle.PREPARED
                && !current.publicationEnabled()
                && !replacement.publicationEnabled();
        if (!mutablePreparedDomainSet
                && !current.requiredReferenceDomains().equals(
                        replacement.requiredReferenceDomains())) {
            throw invariant(
                    "active generation domain set requires a separate capability transition");
        }
        long currentEpoch = current.brokerCapabilityReadinessEpoch();
        long replacementEpoch = replacement.brokerCapabilityReadinessEpoch();
        requireBackfillTransition(
                current.streamRegistrationBackfill(),
                replacement.streamRegistrationBackfill(),
                currentEpoch,
                replacementEpoch,
                "stream-registration");
        requireBackfillTransition(
                current.physicalRootBackfill(),
                replacement.physicalRootBackfill(),
                currentEpoch,
                replacementEpoch,
                "physical-root");
        requireBackfillTransition(
                current.cursorSnapshotBackfill(),
                replacement.cursorSnapshotBackfill(),
                currentEpoch,
                replacementEpoch,
                "cursor-snapshot");
        if (!current.objectStoreCapabilitySha256().isEmpty()
                && !current.objectStoreCapabilitySha256().equals(
                        replacement.objectStoreCapabilitySha256())
                && replacementEpoch == currentEpoch) {
            throw invariant(
                    "object-store capability changed without a newer readiness epoch");
        }
    }

    private static void requireBackfillTransition(
            GenerationBackfillProofRecord current,
            GenerationBackfillProofRecord replacement,
            long currentEpoch,
            long replacementEpoch,
            String name) {
        if (replacementEpoch == currentEpoch) {
            if (current.complete() && !current.equals(replacement)) {
                throw invariant(name + " backfill changed after completion in one epoch");
            }
            if (!current.complete()
                    && replacement.complete()
                    && replacement.brokerReadinessEpoch() != replacementEpoch) {
                throw invariant(name + " backfill completed for another readiness epoch");
            }
            if (!current.complete()
                    && !replacement.complete()
                    && replacement.brokerReadinessEpoch()
                            < current.brokerReadinessEpoch()) {
                throw invariant(name + " backfill attempt epoch moved backward");
            }
            return;
        }
        if (replacement.complete()
                && replacement.brokerReadinessEpoch() != replacementEpoch) {
            throw invariant(name + " backfill does not match the newer readiness epoch");
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
