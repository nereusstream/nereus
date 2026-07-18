/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import java.util.Objects;

/** Redacted durable facts returned after final readiness and activation revalidation. */
public record ManagedLedgerPhysicalDeletionActivationResult(
        String runId,
        long brokerReadinessEpoch,
        String physicalRootCoverageSha256,
        String cursorSnapshotCoverageSha256,
        String objectStoreCapabilitySha256,
        long activationMetadataVersion,
        Status status) {
    public enum Status {
        ACTIVATED,
        ALREADY_ACTIVE
    }

    public ManagedLedgerPhysicalDeletionActivationResult {
        new ManagedLedgerPhysicalDeletionActivationRequest(
                runId, 1, java.time.Duration.ofMillis(1));
        if (brokerReadinessEpoch < 0) {
            throw new IllegalArgumentException(
                    "brokerReadinessEpoch must be non-negative");
        }
        physicalRootCoverageSha256 = requireSha256(
                physicalRootCoverageSha256,
                "physicalRootCoverageSha256");
        cursorSnapshotCoverageSha256 = requireSha256(
                cursorSnapshotCoverageSha256,
                "cursorSnapshotCoverageSha256");
        objectStoreCapabilitySha256 = requireSha256(
                objectStoreCapabilitySha256,
                "objectStoreCapabilitySha256");
        if (activationMetadataVersion < 0) {
            throw new IllegalArgumentException(
                    "activationMetadataVersion must be non-negative");
        }
        Objects.requireNonNull(status, "status");
    }

    private static String requireSha256(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.length() != 64) {
            throw new IllegalArgumentException(
                    field + " must be lowercase SHA-256");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9')
                    || (character >= 'a' && character <= 'f'))) {
                throw new IllegalArgumentException(
                        field + " must be lowercase SHA-256");
            }
        }
        return value;
    }
}
