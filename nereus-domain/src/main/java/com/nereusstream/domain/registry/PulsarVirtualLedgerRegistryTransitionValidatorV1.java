/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

import java.util.HashMap;
import java.util.Map;

/** Exact create/successor transition rules for NVR1. */
public final class PulsarVirtualLedgerRegistryTransitionValidatorV1 {
    private PulsarVirtualLedgerRegistryTransitionValidatorV1() {}

    public static void validateInitial(PulsarVirtualLedgerRegistryV1 candidate) {
        PulsarVirtualLedgerRegistryValidatorV1.validate(candidate);
        if (candidate.registryEpoch() != 1) {
            reject(RegistryRejectionCodeV1.REGISTRY_EPOCH_INVALID, "initial Registry epoch must be one");
        }
    }

    public static void validate(PulsarVirtualLedgerRegistryV1 predecessor, PulsarVirtualLedgerRegistryV1 candidate) {
        PulsarVirtualLedgerRegistryValidatorV1.validate(predecessor);
        PulsarVirtualLedgerRegistryValidatorV1.validate(candidate);
        if (!predecessor.deploymentId().equals(candidate.deploymentId())
                || !predecessor.reservationDomainId().equals(candidate.reservationDomainId())
                || !predecessor.instanceId().equals(candidate.instanceId())
                || !predecessor
                        .ledgerIdCompatibilityNamespaceId()
                        .equals(candidate.ledgerIdCompatibilityNamespaceId())) {
            reject(RegistryRejectionCodeV1.REGISTRY_IDENTITY_INVALID, "Registry identity cannot change");
        }
        final long expectedEpoch;
        try {
            expectedEpoch = Math.addExact(predecessor.registryEpoch(), 1);
        } catch (ArithmeticException error) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_EPOCH_INVALID, "Registry epoch overflows", error);
        }
        if (candidate.registryEpoch() != expectedEpoch) {
            reject(RegistryRejectionCodeV1.REGISTRY_EPOCH_INVALID, "Registry epoch must advance by exactly one");
        }

        Map<Object, VirtualLedgerSliceAssignmentV1> successorById = new HashMap<>();
        for (VirtualLedgerSliceAssignmentV1 value : candidate.assignments()) {
            successorById.put(value.sliceAssignmentId(), value);
        }
        for (VirtualLedgerSliceAssignmentV1 prior : predecessor.assignments()) {
            VirtualLedgerSliceAssignmentV1 successor = successorById.remove(prior.sliceAssignmentId());
            if (successor == null) {
                reject(RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID, "assignment rows are permanent");
            }
            if (!sameImmutableAssignment(prior, successor)) {
                reject(
                        RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID,
                        "assignment identity, owner, or bounds cannot change");
            }
            if (!prior.lifecycle().canAdvanceTo(successor.lifecycle())) {
                reject(
                        RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID,
                        "slice lifecycle may only advance ACTIVE to RETIRING to RETIRED");
            }
        }
        for (VirtualLedgerSliceAssignmentV1 added : successorById.values()) {
            if (added.lifecycle() != VirtualLedgerSliceLifecycleV1.ACTIVE) {
                reject(RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID, "new assignment must be ACTIVE");
            }
        }
    }

    private static boolean sameImmutableAssignment(
            VirtualLedgerSliceAssignmentV1 first, VirtualLedgerSliceAssignmentV1 second) {
        return first.deploymentId().equals(second.deploymentId())
                && first.reservationDomainId().equals(second.reservationDomainId())
                && first.pulsarCellId().equals(second.pulsarCellId())
                && first.ledgerIdCompatibilityNamespaceId().equals(second.ledgerIdCompatibilityNamespaceId())
                && first.sliceAssignmentId().equals(second.sliceAssignmentId())
                && first.startInclusive() == second.startInclusive()
                && first.endInclusive() == second.endInclusive();
    }

    private static void reject(RegistryRejectionCodeV1 code, String message) {
        throw new RegistryValidationException(code, message);
    }
}
