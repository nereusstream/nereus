/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Small namespace-bound projection consumed by the later allocator. */
public record VirtualLedgerSliceViewV1(
        Sha256Digest ledgerIdCompatibilityNamespaceId, long registryEpoch, VirtualLedgerSliceAssignmentV1 assignment) {
    public VirtualLedgerSliceViewV1 {
        Objects.requireNonNull(ledgerIdCompatibilityNamespaceId, "ledgerIdCompatibilityNamespaceId");
        Objects.requireNonNull(assignment, "assignment");
        if (registryEpoch <= 0
                || !ledgerIdCompatibilityNamespaceId.equals(assignment.ledgerIdCompatibilityNamespaceId())) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID,
                    "derived slice view Registry epoch or namespace differs");
        }
    }

    public boolean allocationAllowed() {
        return assignment.lifecycle() == VirtualLedgerSliceLifecycleV1.ACTIVE;
    }
}
