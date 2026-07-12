/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.snapshot;

import com.nereusstream.api.AppendResult;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import java.util.Objects;

/** Monotonic merger of complete remote metadata and the exact local append result overlay. */
public final class StreamSnapshotTracker {
    private StreamMetadata remoteBase;
    private StreamSnapshotView effective;

    public StreamSnapshotTracker(StreamMetadata initial, long observedCommitVersion) {
        this.remoteBase = Objects.requireNonNull(initial, "initial");
        this.effective = new StreamSnapshotView(initial, requireNonNegative(observedCommitVersion), false);
    }

    public synchronized StreamSnapshotView current() {
        return effective;
    }

    public synchronized StreamSnapshotView updateFromMetadata(StreamMetadata candidate) {
        Objects.requireNonNull(candidate, "candidate");
        requireIdentity(remoteBase, candidate);
        if (candidate.metadataVersion() < remoteBase.metadataVersion()) {
            return effective;
        }
        if (candidate.metadataVersion() == remoteBase.metadataVersion()) {
            if (!candidate.equals(remoteBase)) {
                throw invariant("equal backend metadata version carries different remote stream content");
            }
        } else {
            requireRemoteMonotonic(remoteBase, candidate);
            remoteBase = candidate;
        }

        StreamMetadata current = effective.metadata();
        if (candidate.committedEndOffset() >= current.committedEndOffset()) {
            if (candidate.cumulativeSize() < current.cumulativeSize()) {
                throw invariant("remote cumulative size regressed while catching the local append overlay");
            }
            effective = new StreamSnapshotView(candidate, effective.observedCommitVersion(), false);
        } else {
            StreamMetadata merged = new StreamMetadata(
                    candidate.streamId(),
                    candidate.streamName(),
                    candidate.state(),
                    candidate.profile(),
                    candidate.attributes(),
                    candidate.createdAtMillis(),
                    candidate.metadataVersion(),
                    current.committedEndOffset(),
                    current.cumulativeSize(),
                    candidate.trimOffset());
            effective = new StreamSnapshotView(merged, effective.observedCommitVersion(), true);
        }
        return effective;
    }

    public synchronized StreamSnapshotView advanceFromAppend(AppendResult result) {
        Objects.requireNonNull(result, "result");
        StreamMetadata current = effective.metadata();
        if (!result.streamId().equals(current.streamId())
                || result.entryCount() != 1
                || result.recordCount() != 1
                || result.range().recordCount() != 1
                || result.payloadFormat() != com.nereusstream.api.PayloadFormat.OPAQUE_RECORD_BATCH
                || !result.schemaRefs().isEmpty()
                || result.projectionRef().isPresent()
                || result.committedEndOffset() != result.range().endOffset()) {
            throw invariant("append result does not describe one entry for the tracked stream");
        }
        boolean fullyOlder = result.committedEndOffset() <= current.committedEndOffset()
                && result.cumulativeSize() <= current.cumulativeSize()
                && result.commitVersion() <= effective.observedCommitVersion();
        if (fullyOlder) {
            return effective;
        }
        if (result.committedEndOffset() < current.committedEndOffset()
                || result.cumulativeSize() < current.cumulativeSize()
                || result.commitVersion() < effective.observedCommitVersion()) {
            throw invariant("append result regresses the effective stream snapshot");
        }
        if (current.state() != StreamState.ACTIVE) {
            throw invariant("append result cannot advance a non-active stream snapshot");
        }
        StreamMetadata advanced = new StreamMetadata(
                current.streamId(),
                current.streamName(),
                current.state(),
                current.profile(),
                current.attributes(),
                current.createdAtMillis(),
                current.metadataVersion(),
                result.committedEndOffset(),
                result.cumulativeSize(),
                current.trimOffset());
        effective = new StreamSnapshotView(advanced, result.commitVersion(), true);
        return effective;
    }

    private static void requireIdentity(StreamMetadata expected, StreamMetadata candidate) {
        if (!candidate.streamId().equals(expected.streamId())
                || !candidate.streamName().equals(expected.streamName())
                || candidate.profile() != expected.profile()
                || !candidate.attributes().equals(expected.attributes())
                || candidate.createdAtMillis() != expected.createdAtMillis()) {
            throw invariant("stream metadata identity/profile/immutable attributes changed");
        }
    }

    private static void requireRemoteMonotonic(StreamMetadata previous, StreamMetadata candidate) {
        if (candidate.committedEndOffset() < previous.committedEndOffset()
                || candidate.cumulativeSize() < previous.cumulativeSize()
                || candidate.trimOffset() < previous.trimOffset()
                || (candidate.committedEndOffset() == previous.committedEndOffset()
                        && candidate.cumulativeSize() != previous.cumulativeSize())
                || stateRank(candidate.state()) < stateRank(previous.state())) {
            throw invariant("newer backend metadata regresses stream truth");
        }
    }

    private static int stateRank(StreamState state) {
        return switch (state) {
            case CREATING -> 0;
            case ACTIVE -> 1;
            case SEALED -> 2;
            case DELETING -> 3;
            case DELETED -> 4;
        };
    }

    private static long requireNonNegative(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("observedCommitVersion must be non-negative");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
