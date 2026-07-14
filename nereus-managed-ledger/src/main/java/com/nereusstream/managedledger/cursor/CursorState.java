/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.CursorIds;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Coherent hydrated cursor root plus effective snapshot-backed acknowledgement state. */
public record CursorState(
        CursorIdentity identity,
        String ownerSessionId,
        CursorLifecycle lifecycle,
        long mutationSequence,
        long ackStateEpoch,
        String lastProtectionAttemptId,
        CursorAckState acknowledgements,
        Map<String, Long> positionProperties,
        Map<String, String> cursorProperties,
        Optional<CursorSnapshotReference> snapshotReference,
        long createdAtMillis,
        long updatedAtMillis,
        long metadataVersion) {
    public CursorState {
        Objects.requireNonNull(identity, "identity");
        ownerSessionId = CursorIds.requireRandomId(ownerSessionId, "ownerSessionId");
        Objects.requireNonNull(lifecycle, "lifecycle");
        if (mutationSequence < 1 || ackStateEpoch < 1) {
            throw new IllegalArgumentException("cursor sequence and ack epoch must be positive");
        }
        lastProtectionAttemptId = CursorIds.requireRandomId(
                lastProtectionAttemptId, "lastProtectionAttemptId");
        Objects.requireNonNull(acknowledgements, "acknowledgements");
        positionProperties = immutableMap(positionProperties, "positionProperties");
        cursorProperties = immutableMap(cursorProperties, "cursorProperties");
        snapshotReference = Objects.requireNonNull(snapshotReference, "snapshotReference");
        snapshotReference.ifPresent(reference -> {
            if (reference.cursorGeneration() != identity.cursorGeneration()
                    || reference.sourceMutationSequence() > mutationSequence
                    || reference.baseMarkDeleteOffset() > acknowledgements.markDeleteOffset()) {
                throw new IllegalArgumentException("snapshot reference does not match the effective cursor state");
            }
        });
        if (createdAtMillis < 0 || updatedAtMillis < createdAtMillis || metadataVersion < 0) {
            throw new IllegalArgumentException("cursor timestamps or metadataVersion are invalid");
        }
    }

    private static <K, V> Map<K, V> immutableMap(Map<K, V> value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        LinkedHashMap<K, V> copy = new LinkedHashMap<>();
        value.forEach((key, item) -> copy.put(
                Objects.requireNonNull(key, fieldName + " contains null key"),
                Objects.requireNonNull(item, fieldName + " contains null value")));
        return Collections.unmodifiableMap(copy);
    }
}
