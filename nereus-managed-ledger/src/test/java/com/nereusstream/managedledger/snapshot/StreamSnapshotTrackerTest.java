/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendResult;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StreamSnapshotTrackerTest {
    private static final StreamId STREAM_ID = new StreamId("s-stream");
    private static final StreamName STREAM_NAME = new StreamName("stream-name");

    @Test
    void localAppendOverlayAdvancesExactEndAndSizeUntilRemoteCatchesUp() {
        StreamMetadata initial = metadata(1, 0, 0, 0, StreamState.ACTIVE);
        StreamSnapshotTracker tracker = new StreamSnapshotTracker(initial, 0);

        StreamSnapshotView appended = tracker.advanceFromAppend(result(0, 1, 7, 1));
        StreamSnapshotView stale = tracker.updateFromMetadata(initial);
        StreamSnapshotView newerButBehind = tracker.updateFromMetadata(
                metadata(2, 0, 0, 0, StreamState.ACTIVE));
        StreamSnapshotView caughtUp = tracker.updateFromMetadata(
                metadata(3, 1, 7, 0, StreamState.ACTIVE));

        assertThat(appended.metadata().committedEndOffset()).isEqualTo(1);
        assertThat(appended.metadata().cumulativeSize()).isEqualTo(7);
        assertThat(stale).isEqualTo(appended);
        assertThat(newerButBehind.localAppendOverlay()).isTrue();
        assertThat(newerButBehind.metadata().metadataVersion()).isEqualTo(2);
        assertThat(newerButBehind.metadata().committedEndOffset()).isEqualTo(1);
        assertThat(caughtUp.localAppendOverlay()).isFalse();
        assertThat(caughtUp.observedCommitVersion()).isEqualTo(1);
    }

    @Test
    void olderRecoveredAppendIsANoopButMixedRegressionFails() {
        StreamSnapshotTracker tracker = new StreamSnapshotTracker(
                metadata(3, 2, 14, 0, StreamState.ACTIVE), 2);

        assertThat(tracker.advanceFromAppend(result(0, 1, 7, 1)))
                .isEqualTo(tracker.current());
        assertThatThrownBy(() -> tracker.advanceFromAppend(result(2, 3, 13, 3)))
                .isInstanceOf(NereusException.class)
                .hasMessageContaining("regresses");
    }

    @Test
    void equalVersionDriftAndNewerRemoteRegressionFailClosed() {
        StreamSnapshotTracker tracker = new StreamSnapshotTracker(
                metadata(3, 2, 14, 1, StreamState.ACTIVE), 2);

        assertThatThrownBy(() -> tracker.updateFromMetadata(
                        metadata(3, 2, 14, 2, StreamState.ACTIVE)))
                .isInstanceOf(NereusException.class)
                .hasMessageContaining("equal backend");
        assertThatThrownBy(() -> tracker.updateFromMetadata(
                        metadata(4, 1, 7, 1, StreamState.ACTIVE)))
                .isInstanceOf(NereusException.class)
                .hasMessageContaining("regresses");
        assertThatThrownBy(() -> tracker.updateFromMetadata(new StreamMetadata(
                        new StreamId("s-other"), STREAM_NAME, StreamState.ACTIVE,
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of(), 1, 4, 2, 14, 1)))
                .isInstanceOf(NereusException.class)
                .hasMessageContaining("identity");
    }

    @Test
    void lifecycleCanAdvanceWhileTheLocalTailOverlayRemainsExact() {
        StreamSnapshotTracker tracker = new StreamSnapshotTracker(
                metadata(1, 0, 0, 0, StreamState.ACTIVE), 0);
        tracker.advanceFromAppend(result(0, 1, 7, 1));

        StreamSnapshotView sealed = tracker.updateFromMetadata(
                metadata(2, 0, 0, 0, StreamState.SEALED));

        assertThat(sealed.metadata().state()).isEqualTo(StreamState.SEALED);
        assertThat(sealed.metadata().committedEndOffset()).isEqualTo(1);
        assertThat(sealed.localAppendOverlay()).isTrue();
    }

    private static StreamMetadata metadata(
            long metadataVersion,
            long end,
            long size,
            long trim,
            StreamState state) {
        return new StreamMetadata(
                STREAM_ID,
                STREAM_NAME,
                state,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                Map.of(),
                1,
                metadataVersion,
                end,
                size,
                trim);
    }

    private static AppendResult result(long start, long end, long cumulativeSize, long commitVersion) {
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(), Optional.empty(), Optional.empty(),
                0, 1, new Checksum(ChecksumType.CRC32C, "11111111"));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                new ObjectId("object"),
                new ObjectKey("key"),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "slice",
                0,
                1,
                new Checksum(ChecksumType.CRC32C, "22222222"),
                index);
        return new AppendResult(
                STREAM_ID,
                new OffsetRange(start, end),
                end,
                cumulativeSize,
                0,
                target,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                7,
                List.of(),
                Optional.empty(),
                commitVersion);
    }
}
