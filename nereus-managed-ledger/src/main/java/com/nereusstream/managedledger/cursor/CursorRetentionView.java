/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.CursorIds;
import java.util.Objects;
import java.util.Optional;

/** Immutable owner-scoped view of the per-stream cursor retention root. */
public record CursorRetentionView(
        CursorLedgerIdentity ledger,
        String ownerSessionId,
        Lifecycle lifecycle,
        long mutationSequence,
        long metadataVersion,
        long protectedFloorOffset,
        long lastCompletedTrimOffset,
        Optional<PendingProtection> pendingProtection,
        Optional<PendingTrim> pendingTrim) {
    public CursorRetentionView {
        Objects.requireNonNull(ledger, "ledger");
        ownerSessionId = CursorIds.requireRandomId(ownerSessionId, "ownerSessionId");
        Objects.requireNonNull(lifecycle, "lifecycle");
        if (mutationSequence < 1 || metadataVersion < 0
                || lastCompletedTrimOffset < 0
                || protectedFloorOffset < lastCompletedTrimOffset) {
            throw new IllegalArgumentException("retention sequence, version, or floor is invalid");
        }
        pendingProtection = Objects.requireNonNull(pendingProtection, "pendingProtection");
        pendingTrim = Objects.requireNonNull(pendingTrim, "pendingTrim");
        if ((lifecycle == Lifecycle.PROTECTION_PENDING) != pendingProtection.isPresent()
                || (lifecycle == Lifecycle.TRIM_PENDING) != pendingTrim.isPresent()
                || (lifecycle == Lifecycle.ACTIVE && (pendingProtection.isPresent() || pendingTrim.isPresent()))) {
            throw new IllegalArgumentException("retention lifecycle does not match its pending view");
        }
    }

    public enum Lifecycle {
        ACTIVE,
        PROTECTION_PENDING,
        TRIM_PENDING
    }

    public record PendingProtection(
            String attemptId,
            Kind kind,
            String cursorNameHash,
            long targetCursorGeneration,
            long targetMarkDeleteOffset) {
        public PendingProtection {
            attemptId = CursorIds.requireRandomId(attemptId, "attemptId");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(cursorNameHash, "cursorNameHash");
            if (targetCursorGeneration < 1 || targetMarkDeleteOffset < 0) {
                throw new IllegalArgumentException("pending protection generation or offset is invalid");
            }
        }

        public enum Kind {
            CREATE,
            RECREATE,
            BACKWARD_RESET
        }
    }

    public record PendingTrim(String attemptId, long targetTrimOffset, String composedReason) {
        public PendingTrim {
            attemptId = CursorIds.requireRandomId(attemptId, "attemptId");
            if (targetTrimOffset < 0) {
                throw new IllegalArgumentException("targetTrimOffset must be non-negative");
            }
            Objects.requireNonNull(composedReason, "composedReason");
            if (!composedReason.startsWith("nereus-cursor-retention/" + attemptId + ":")) {
                throw new IllegalArgumentException("composedReason does not match attemptId");
            }
        }
    }
}
