/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import java.util.Objects;

/** Exact current cursor-root authority captured before an immutable snapshot upload. */
public record CursorSnapshotWriteAuthority(
        VersionedCursorState currentRoot,
        String ownerSessionId,
        long targetMutationSequence) {
    public CursorSnapshotWriteAuthority {
        Objects.requireNonNull(currentRoot, "currentRoot");
        Objects.requireNonNull(ownerSessionId, "ownerSessionId");
        if (currentRoot.value().lifecycle() != CursorRecordLifecycle.ACTIVE
                || !currentRoot.value().ownerSessionId().equals(ownerSessionId)) {
            throw new IllegalArgumentException(
                    "snapshot write authority requires the exact ACTIVE owner session");
        }
        if (targetMutationSequence <= currentRoot.value().mutationSequence()) {
            throw new IllegalArgumentException(
                    "snapshot target mutation sequence must advance the current root");
        }
    }

    public void requireMatches(CursorSnapshotWriteRequest request) {
        Objects.requireNonNull(request, "request");
        if (!currentRoot.value().projection().equals(
                        request.identity().ledger().projection())
                || !currentRoot.value().cursorName().equals(
                        request.identity().cursorName())
                || !currentRoot.value().cursorNameHash().equals(
                        request.identity().cursorNameHash())
                || currentRoot.value().cursorGeneration()
                        != request.identity().cursorGeneration()
                || targetMutationSequence != request.sourceMutationSequence()) {
            throw new IllegalArgumentException(
                    "snapshot request does not match its captured cursor authority");
        }
    }
}
