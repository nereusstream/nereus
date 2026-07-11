/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import java.util.Objects;
public record StreamStateTransitionRequest(StreamId streamId, StreamState expectedState,
        StreamState targetState, long expectedMetadataVersion) {
    public StreamStateTransitionRequest { Objects.requireNonNull(streamId); Objects.requireNonNull(expectedState);
        Objects.requireNonNull(targetState); if (expectedMetadataVersion < 0) throw new IllegalArgumentException("negative metadata version");
        boolean legal = expectedState == StreamState.ACTIVE && targetState == StreamState.SEALED
                || (expectedState == StreamState.ACTIVE || expectedState == StreamState.SEALED) && targetState == StreamState.DELETING
                || expectedState == StreamState.DELETING && targetState == StreamState.DELETED;
        if (!legal) throw new IllegalArgumentException("illegal stream state transition"); }
}
