/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.CursorIds;
import com.nereusstream.metadata.oxia.CursorNames;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Crash-recoverable protection, floor reconciliation, and logical-trim coordination. */
public interface CursorRetentionCoordinator extends AutoCloseable {
    CompletableFuture<CursorRetentionView> claimAndRecover(CursorOwnerSession owner);

    CompletableFuture<ProtectionLease> beginProtection(
            CursorOwnerSession owner, ProtectionRequest request);

    CompletableFuture<CursorRetentionView> completeProtection(ProtectionLease lease);

    CompletableFuture<CursorRetentionView> reconcileFloor(CursorOwnerSession owner);

    CompletableFuture<CursorRetentionView> requestTrim(
            CursorOwnerSession owner, long candidateOffset, String reason);

    @Override
    void close();

    record ProtectionRequest(
            CursorRetentionView.PendingProtection.Kind kind,
            String cursorName,
            String cursorNameHash,
            long expectedCursorGeneration,
            long targetCursorGeneration,
            long targetMarkDeleteOffset,
            Optional<BatchAckState> targetPartialBatch,
            Map<String, Long> initialPositionProperties,
            Map<String, String> initialCursorProperties) {
        public ProtectionRequest {
            Objects.requireNonNull(kind, "kind");
            cursorName = CursorNames.requireCursorName(cursorName);
            Objects.requireNonNull(cursorNameHash, "cursorNameHash");
            if (!CursorNames.cursorNameHash(cursorName).equals(cursorNameHash)) {
                throw new IllegalArgumentException("cursor protection name/hash mismatch");
            }
            if (targetMarkDeleteOffset < 0) {
                throw new IllegalArgumentException("targetMarkDeleteOffset must be non-negative");
            }
            targetPartialBatch = Objects.requireNonNull(targetPartialBatch, "targetPartialBatch");
            initialPositionProperties = immutableMap(
                    initialPositionProperties, "initialPositionProperties");
            initialCursorProperties = immutableMap(
                    initialCursorProperties, "initialCursorProperties");
            switch (kind) {
                case CREATE -> {
                    if (expectedCursorGeneration != 0
                            || targetCursorGeneration != 1
                            || targetPartialBatch.isPresent()) {
                        throw new IllegalArgumentException("CREATE protection has invalid generation or partial state");
                    }
                }
                case RECREATE -> {
                    if (expectedCursorGeneration < 1
                            || targetCursorGeneration
                                    != Math.addExact(expectedCursorGeneration, 1)
                            || targetPartialBatch.isPresent()) {
                        throw new IllegalArgumentException(
                                "RECREATE protection has invalid generation or partial state");
                    }
                }
                case BACKWARD_RESET -> {
                    if (expectedCursorGeneration < 1
                            || targetCursorGeneration != expectedCursorGeneration
                            || !initialPositionProperties.isEmpty()
                            || !initialCursorProperties.isEmpty()) {
                        throw new IllegalArgumentException(
                                "BACKWARD_RESET protection has invalid generation or properties");
                    }
                }
            }
        }

        private static <K, V> Map<K, V> immutableMap(Map<K, V> values, String fieldName) {
            Objects.requireNonNull(values, fieldName);
            LinkedHashMap<K, V> copy = new LinkedHashMap<>();
            values.forEach((key, value) -> copy.put(
                    Objects.requireNonNull(key, fieldName + " contains null key"),
                    Objects.requireNonNull(value, fieldName + " contains null value")));
            return Collections.unmodifiableMap(copy);
        }
    }

    record ProtectionLease(
            CursorOwnerSession owner,
            String attemptId,
            long retentionMetadataVersion) {
        public ProtectionLease {
            Objects.requireNonNull(owner, "owner");
            attemptId = CursorIds.requireRandomId(attemptId, "attemptId");
            if (retentionMetadataVersion < 0) {
                throw new IllegalArgumentException("retentionMetadataVersion must be non-negative");
            }
        }
    }
}
