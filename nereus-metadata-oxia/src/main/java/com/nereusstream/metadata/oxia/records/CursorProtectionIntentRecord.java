/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.CursorIds;
import com.nereusstream.metadata.oxia.CursorNames;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Complete immutable input needed to recover one create, recreate, or backward reset. */
public record CursorProtectionIntentRecord(
        String attemptId,
        CursorProtectionKind kind,
        String cursorName,
        String cursorNameHash,
        long expectedCursorGeneration,
        long targetCursorGeneration,
        long targetMarkDeleteOffset,
        Optional<CursorPartialBatchAckRecord> targetPartialBatch,
        Map<String, Long> initialPositionProperties,
        Map<String, String> initialCursorProperties,
        long createdAtMillis) {
    public CursorProtectionIntentRecord {
        attemptId = CursorIds.requireRandomId(attemptId, "attemptId");
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
        targetPartialBatch.ifPresent(partial -> {
            if (partial.entryOffset() != targetMarkDeleteOffset) {
                throw new IllegalArgumentException("target partial offset must equal targetMarkDeleteOffset");
            }
        });
        initialPositionProperties = CursorRecordValidation.canonicalLongMap(
                initialPositionProperties, "initialPositionProperties");
        initialCursorProperties = CursorRecordValidation.canonicalStringMap(
                initialCursorProperties, "initialCursorProperties");
        if (createdAtMillis < 0) {
            throw new IllegalArgumentException("intent createdAtMillis must be non-negative");
        }
        switch (kind) {
            case CREATE -> {
                if (expectedCursorGeneration != 0 || targetCursorGeneration != 1
                        || targetPartialBatch.isPresent()) {
                    throw new IllegalArgumentException("CREATE protection has invalid generation or partial state");
                }
            }
            case RECREATE -> {
                if (expectedCursorGeneration < 1
                        || targetCursorGeneration != Math.addExact(expectedCursorGeneration, 1)
                        || targetPartialBatch.isPresent()) {
                    throw new IllegalArgumentException("RECREATE protection has invalid generation or partial state");
                }
            }
            case BACKWARD_RESET -> {
                if (expectedCursorGeneration < 1
                        || targetCursorGeneration != expectedCursorGeneration
                        || !initialPositionProperties.isEmpty()
                        || !initialCursorProperties.isEmpty()) {
                    throw new IllegalArgumentException("BACKWARD_RESET protection has invalid generation or properties");
                }
            }
        }
    }
}
