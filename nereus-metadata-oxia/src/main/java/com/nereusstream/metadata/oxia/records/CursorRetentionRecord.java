/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.CursorIds;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Single-key authority for conservative per-stream cursor retention and pending barriers. */
public record CursorRetentionRecord(
        long metadataVersion,
        ManagedLedgerProjectionIdentity projection,
        String ownerSessionId,
        CursorRetentionLifecycle lifecycle,
        long mutationSequence,
        long protectedFloorOffset,
        long lastCompletedTrimOffset,
        Optional<CursorProtectionIntentRecord> pendingProtectionIntent,
        Optional<String> pendingTrimAttemptId,
        OptionalLong pendingTrimOffset,
        Optional<String> pendingTrimReason,
        long updatedAtMillis) {
    public CursorRetentionRecord {
        if (metadataVersion != 0) {
            throw new IllegalArgumentException("encoded retention metadataVersion must be zero");
        }
        Objects.requireNonNull(projection, "projection");
        ownerSessionId = CursorIds.requireRandomId(ownerSessionId, "ownerSessionId");
        Objects.requireNonNull(lifecycle, "lifecycle");
        if (mutationSequence < 1) {
            throw new IllegalArgumentException("retention mutationSequence must be positive");
        }
        if (lastCompletedTrimOffset < 0 || protectedFloorOffset < lastCompletedTrimOffset) {
            throw new IllegalArgumentException("retention floor/trim offsets are invalid");
        }
        pendingProtectionIntent = Objects.requireNonNull(
                pendingProtectionIntent, "pendingProtectionIntent");
        pendingTrimAttemptId = Objects.requireNonNull(pendingTrimAttemptId, "pendingTrimAttemptId");
        pendingTrimOffset = Objects.requireNonNull(pendingTrimOffset, "pendingTrimOffset");
        pendingTrimReason = Objects.requireNonNull(pendingTrimReason, "pendingTrimReason");
        if (updatedAtMillis < 0) {
            throw new IllegalArgumentException("retention updatedAtMillis must be non-negative");
        }
        switch (lifecycle) {
            case ACTIVE -> requireNoPending(
                    pendingProtectionIntent, pendingTrimAttemptId, pendingTrimOffset, pendingTrimReason);
            case PROTECTION_PENDING -> {
                if (pendingProtectionIntent.isEmpty()
                        || pendingTrimAttemptId.isPresent()
                        || pendingTrimOffset.isPresent()
                        || pendingTrimReason.isPresent()) {
                    throw new IllegalArgumentException("PROTECTION_PENDING retention has invalid pending fields");
                }
                long target = pendingProtectionIntent.orElseThrow().targetMarkDeleteOffset();
                if (lastCompletedTrimOffset > target || protectedFloorOffset > target) {
                    throw new IllegalArgumentException("protection target is below the retained floor");
                }
            }
            case TRIM_PENDING -> {
                if (pendingProtectionIntent.isPresent()
                        || pendingTrimAttemptId.isEmpty()
                        || pendingTrimOffset.isEmpty()
                        || pendingTrimReason.isEmpty()) {
                    throw new IllegalArgumentException("TRIM_PENDING retention has invalid pending fields");
                }
                String attemptId = CursorIds.requireRandomId(
                        pendingTrimAttemptId.orElseThrow(), "pendingTrimAttemptId");
                long trimOffset = pendingTrimOffset.getAsLong();
                if (trimOffset < lastCompletedTrimOffset || trimOffset != protectedFloorOffset) {
                    throw new IllegalArgumentException("pending trim offset must freeze the protected floor");
                }
                String reason = CursorRecordValidation.requireString(
                        pendingTrimReason.orElseThrow(),
                        "pendingTrimReason",
                        CursorRecordValidation.TRIM_REASON_MAX_BYTES,
                        false);
                String prefix = "nereus-cursor-retention/" + attemptId + ":";
                if (!reason.startsWith(prefix)
                        || reason.getBytes(StandardCharsets.UTF_8).length
                                > CursorRecordValidation.TRIM_REASON_MAX_BYTES) {
                    throw new IllegalArgumentException("pending trim reason does not match its attempt ID");
                }
            }
        }
    }

    private static void requireNoPending(
            Optional<CursorProtectionIntentRecord> protection,
            Optional<String> trimAttempt,
            OptionalLong trimOffset,
            Optional<String> trimReason) {
        if (protection.isPresent() || trimAttempt.isPresent() || trimOffset.isPresent() || trimReason.isPresent()) {
            throw new IllegalArgumentException("ACTIVE retention cannot carry pending fields");
        }
    }
}
