/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Closed semantic and capacity validator for one NVR1 value. */
public final class PulsarVirtualLedgerRegistryValidatorV1 {
    public static final int MAX_REGISTRY_BYTES = 65_536;
    public static final int MAX_CANONICAL_REGISTRY_BYTES = 51_016;
    public static final int MAX_WRITER_COUNT = 14;
    public static final int MAX_ASSIGNMENTS_EVER = 256;
    public static final int FIXED_HEADER_BYTES = 184;

    private PulsarVirtualLedgerRegistryValidatorV1() {}

    public static void validate(PulsarVirtualLedgerRegistryV1 registry) {
        if (registry.registryEpoch() <= 0) {
            reject(RegistryRejectionCodeV1.REGISTRY_EPOCH_INVALID, "Registry epoch must be positive");
        }
        if (!registry.ledgerIdCompatibilityNamespaceId()
                .equals(LedgerIdCompatibilityNamespaceV1.derive(registry.instanceId()))) {
            reject(RegistryRejectionCodeV1.REGISTRY_IDENTITY_INVALID, "NLI1 namespace identity differs");
        }
        validateWriters(registry.writers(), registry.admissionEvidence());
        validateAssignments(registry);
        int encodedBytes;
        try {
            encodedBytes = Math.addExact(
                    FIXED_HEADER_BYTES,
                    Math.addExact(
                            Math.multiplyExact(registry.writers().size(), RegistryWriterRowV1.BYTES),
                            Math.multiplyExact(registry.assignments().size(), VirtualLedgerSliceAssignmentV1.BYTES)));
        } catch (ArithmeticException error) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_CANONICAL_BYTES_EXCEEDED,
                    "Registry canonical-byte arithmetic overflow",
                    error);
        }
        if (encodedBytes > MAX_CANONICAL_REGISTRY_BYTES || encodedBytes > MAX_REGISTRY_BYTES) {
            reject(
                    RegistryRejectionCodeV1.REGISTRY_CANONICAL_BYTES_EXCEEDED,
                    "Registry canonical bytes exceed the v1 bound: " + encodedBytes);
        }
    }

    private static void validateWriters(
            List<RegistryWriterRowV1> writers, RegistryEvidenceReferenceV1 admissionEvidence) {
        if (writers.size() > MAX_WRITER_COUNT) {
            reject(RegistryRejectionCodeV1.REGISTRY_WRITER_COUNT_EXCEEDED, "Registry writer count exceeds 14");
        }
        if (!writers.equals(
                writers.stream().sorted(RegistryWriterRowV1.CANONICAL_ORDER).toList())) {
            reject(RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL, "Registry writers are not canonically sorted");
        }
        Set<String> identities = new HashSet<>();
        Set<String> principals = new HashSet<>();
        Set<RegistryWriterKindV1> kinds = new HashSet<>();
        for (RegistryWriterRowV1 writer : writers) {
            if (!writer.evidence().equals(admissionEvidence)) {
                reject(
                        RegistryRejectionCodeV1.REGISTRY_UNAUTHORIZED_WRITER,
                        "writer evidence does not bind the Registry admission evidence");
            }
            String identity = writer.writerKind().code()
                    + ":"
                    + writer.principalGeneration()
                    + ":"
                    + writer.principalDigest().toHex();
            if (!identities.add(identity)) {
                reject(RegistryRejectionCodeV1.REGISTRY_UNAUTHORIZED_WRITER, "duplicate writer identity");
            }
            if (!principals.add(writer.principalDigest().toHex())) {
                reject(RegistryRejectionCodeV1.REGISTRY_UNAUTHORIZED_WRITER, "principal is reused by multiple rows");
            }
            kinds.add(writer.writerKind());
        }
        if (!kinds.containsAll(Arrays.asList(RegistryWriterKindV1.values()))) {
            reject(
                    RegistryRejectionCodeV1.REGISTRY_OMITTED_AUTHORIZED_WRITER,
                    "an admitted Registry must commit both closed writer kinds");
        }
    }

    private static void validateAssignments(PulsarVirtualLedgerRegistryV1 registry) {
        List<VirtualLedgerSliceAssignmentV1> assignments = registry.assignments();
        if (assignments.size() > MAX_ASSIGNMENTS_EVER) {
            reject(RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_COUNT_EXCEEDED, "Registry assignment count exceeds 256");
        }
        Set<Object> cells = new HashSet<>();
        long previousEnd = -1;
        for (VirtualLedgerSliceAssignmentV1 assignment : assignments) {
            if (!assignment.deploymentId().equals(registry.deploymentId())
                    || !assignment.reservationDomainId().equals(registry.reservationDomainId())
                    || !assignment
                            .ledgerIdCompatibilityNamespaceId()
                            .equals(registry.ledgerIdCompatibilityNamespaceId())) {
                reject(RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID, "assignment owner or namespace differs");
            }
            if (!cells.add(assignment.pulsarCellId())) {
                reject(RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID, "one Cell cannot own a second slice");
            }
            if (previousEnd >= 0 && assignment.startInclusive() <= previousEnd) {
                reject(
                        RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID,
                        "assignment ranges overlap or are unsorted");
            }
            previousEnd = assignment.endInclusive();
        }
    }

    static void reject(RegistryRejectionCodeV1 code, String message) {
        throw new RegistryValidationException(code, message);
    }
}
