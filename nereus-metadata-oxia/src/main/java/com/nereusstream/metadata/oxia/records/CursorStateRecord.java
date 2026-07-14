/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.CursorIds;
import com.nereusstream.metadata.oxia.CursorNames;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Single-key authority for one historical cursor-name key and its current generation. */
public record CursorStateRecord(
        long metadataVersion,
        ManagedLedgerProjectionIdentity projection,
        String ownerSessionId,
        String cursorName,
        String cursorNameHash,
        long cursorGeneration,
        CursorRecordLifecycle lifecycle,
        long mutationSequence,
        long ackStateEpoch,
        String lastProtectionAttemptId,
        long markDeleteOffset,
        Optional<CursorSnapshotReferenceRecord> snapshotReference,
        List<CursorAckRangeRecord> inlineWholeAckDeltas,
        List<CursorPartialBatchAckRecord> inlinePartialAckOverrides,
        Map<String, Long> positionProperties,
        Map<String, String> cursorProperties,
        long createdAtMillis,
        long updatedAtMillis,
        OptionalLong deletedAtMillis) {
    public CursorStateRecord {
        if (metadataVersion != 0) {
            throw new IllegalArgumentException("encoded cursor metadataVersion must be zero");
        }
        Objects.requireNonNull(projection, "projection");
        ownerSessionId = CursorIds.requireRandomId(ownerSessionId, "ownerSessionId");
        cursorName = CursorNames.requireCursorName(cursorName);
        Objects.requireNonNull(cursorNameHash, "cursorNameHash");
        if (!CursorNames.cursorNameHash(cursorName).equals(cursorNameHash)) {
            throw new IllegalArgumentException("cursor name/hash mismatch");
        }
        if (cursorGeneration < 1 || mutationSequence < 1 || ackStateEpoch < 1) {
            throw new IllegalArgumentException("cursor generation, mutation sequence, and ack epoch must be positive");
        }
        Objects.requireNonNull(lifecycle, "lifecycle");
        lastProtectionAttemptId = CursorIds.requireRandomId(
                lastProtectionAttemptId, "lastProtectionAttemptId");
        if (markDeleteOffset < 0) {
            throw new IllegalArgumentException("markDeleteOffset must be non-negative");
        }
        snapshotReference = Objects.requireNonNull(snapshotReference, "snapshotReference");
        snapshotReference.ifPresent(reference -> {
            if (reference.cursorGeneration() != cursorGeneration
                    || reference.sourceMutationSequence() > mutationSequence
                    || reference.baseMarkDeleteOffset() > markDeleteOffset) {
                throw new IllegalArgumentException("cursor snapshot reference does not match its parent root");
            }
        });
        inlineWholeAckDeltas = CursorRecordValidation.canonicalRanges(
                inlineWholeAckDeltas, markDeleteOffset);
        inlinePartialAckOverrides = CursorRecordValidation.canonicalPartials(
                inlinePartialAckOverrides, markDeleteOffset, inlineWholeAckDeltas);
        positionProperties = CursorRecordValidation.canonicalLongMap(
                positionProperties, "positionProperties");
        cursorProperties = CursorRecordValidation.canonicalStringMap(
                cursorProperties, "cursorProperties");
        if (createdAtMillis < 0 || updatedAtMillis < createdAtMillis) {
            throw new IllegalArgumentException("cursor timestamps are invalid");
        }
        deletedAtMillis = Objects.requireNonNull(deletedAtMillis, "deletedAtMillis");
        if (lifecycle == CursorRecordLifecycle.ACTIVE) {
            if (deletedAtMillis.isPresent()) {
                throw new IllegalArgumentException("an ACTIVE cursor cannot carry deletedAtMillis");
            }
        } else {
            if (snapshotReference.isPresent()
                    || !inlineWholeAckDeltas.isEmpty()
                    || !inlinePartialAckOverrides.isEmpty()
                    || !positionProperties.isEmpty()
                    || !cursorProperties.isEmpty()) {
                throw new IllegalArgumentException("a DELETED cursor tombstone cannot retain mutable cursor fields");
            }
            if (deletedAtMillis.isEmpty() || deletedAtMillis.getAsLong() < updatedAtMillis) {
                throw new IllegalArgumentException("a DELETED cursor requires a valid deletedAtMillis");
            }
        }
    }
}
