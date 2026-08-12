/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import java.util.List;
import java.util.Objects;

/** Complete immutable logical NVR1 Registry value. */
public record PulsarVirtualLedgerRegistryV1(
        DeploymentId deploymentId,
        ReservationDomainId reservationDomainId,
        BookKeeperInstanceIdV1 instanceId,
        Sha256Digest ledgerIdCompatibilityNamespaceId,
        long registryEpoch,
        RegistryEvidenceReferenceV1 admissionEvidence,
        List<RegistryWriterRowV1> writers,
        List<VirtualLedgerSliceAssignmentV1> assignments) {
    public PulsarVirtualLedgerRegistryV1(
            DeploymentId deploymentId,
            ReservationDomainId reservationDomainId,
            BookKeeperInstanceIdV1 instanceId,
            Sha256Digest ledgerIdCompatibilityNamespaceId,
            long registryEpoch,
            RegistryEvidenceReferenceV1 admissionEvidence,
            List<RegistryWriterRowV1> writers,
            List<VirtualLedgerSliceAssignmentV1> assignments) {
        this.deploymentId = Objects.requireNonNull(deploymentId, "deploymentId");
        this.reservationDomainId = Objects.requireNonNull(reservationDomainId, "reservationDomainId");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
        this.ledgerIdCompatibilityNamespaceId =
                Objects.requireNonNull(ledgerIdCompatibilityNamespaceId, "ledgerIdCompatibilityNamespaceId");
        this.registryEpoch = registryEpoch;
        this.admissionEvidence = Objects.requireNonNull(admissionEvidence, "admissionEvidence");
        this.writers = List.copyOf(Objects.requireNonNull(writers, "writers"));
        this.assignments = List.copyOf(Objects.requireNonNull(assignments, "assignments"));
        PulsarVirtualLedgerRegistryValidatorV1.validate(this);
    }

    public PulsarVirtualLedgerRegistryV1 successor(
            RegistryEvidenceReferenceV1 evidence,
            List<RegistryWriterRowV1> successorWriters,
            List<VirtualLedgerSliceAssignmentV1> successorAssignments) {
        final long successorEpoch;
        try {
            successorEpoch = Math.addExact(registryEpoch, 1);
        } catch (ArithmeticException error) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_EPOCH_INVALID, "Registry epoch overflows", error);
        }
        PulsarVirtualLedgerRegistryV1 successor = new PulsarVirtualLedgerRegistryV1(
                deploymentId,
                reservationDomainId,
                instanceId,
                ledgerIdCompatibilityNamespaceId,
                successorEpoch,
                evidence,
                successorWriters,
                successorAssignments);
        PulsarVirtualLedgerRegistryTransitionValidatorV1.validate(this, successor);
        return successor;
    }

    public VirtualLedgerSliceViewV1 sliceView(PulsarCellId cellId) {
        Objects.requireNonNull(cellId, "cellId");
        VirtualLedgerSliceAssignmentV1 assignment = assignments.stream()
                .filter(candidate -> candidate.pulsarCellId().equals(cellId))
                .findFirst()
                .orElseThrow(() -> new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID,
                        "Registry has no assignment for the requested Pulsar Cell"));
        return new VirtualLedgerSliceViewV1(ledgerIdCompatibilityNamespaceId, registryEpoch, assignment);
    }
}
