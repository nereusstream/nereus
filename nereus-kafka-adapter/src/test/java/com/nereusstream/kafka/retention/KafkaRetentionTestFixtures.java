/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.retention;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.RollReason;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

final class KafkaRetentionTestFixtures {
    static final StreamId STREAM_ID = new StreamId("kafka-stream-1");
    static final KafkaPartitionIdentity IDENTITY = identity();

    private KafkaRetentionTestFixtures() {}

    static KafkaTrimBarrier.Snapshot snapshot(
            VersionedKafkaPartitionBinding binding,
            StableStreamHeadSnapshot head,
            KafkaRetentionPlanner.Snapshot retention) {
        return new KafkaTrimBarrier.Snapshot(IDENTITY, binding, head, retention);
    }

    static KafkaRetentionPlanner.Snapshot retentionSnapshot(
            long retentionBytes, long retentionMs, long highWatermark, long nowMillis) {
        int flags = LogConfigHistoryEntry.CLEANUP_DELETE_FLAG | LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG;
        return retentionSnapshot(0, retentionBytes, retentionMs, highWatermark, nowMillis, flags);
    }

    static KafkaRetentionPlanner.Snapshot retentionSnapshot(
            long logStartOffset,
            long retentionBytes,
            long retentionMs,
            long highWatermark,
            long nowMillis,
            int cleanupPolicyFlags) {
        LogConfigHistoryEntry config = LogConfigHistoryEntry.create(
                7,
                0,
                1_024,
                60_000,
                0,
                1_024,
                64,
                retentionBytes,
                retentionMs,
                0,
                86_400_000,
                0,
                Long.MAX_VALUE,
                0.5,
                cleanupPolicyFlags);
        KafkaVirtualSegmentState state = new KafkaVirtualSegmentState(
                logStartOffset,
                40,
                List.of(
                        segment(config, 0, 10, 0, 100, 200, 1_000, 0, 100, SegmentState.CLOSED),
                        segment(config, 10, 20, 1, 200, 300, 2_000, 100, 200, SegmentState.CLOSED),
                        segment(config, 20, 30, 2, 300, 400, 10_000, 200, 300, SegmentState.CLOSED),
                        segment(config, 30, 40, 3, 400, 0, 11_000, 300, 400, SegmentState.ACTIVE)),
                List.of(config));
        return new KafkaRetentionPlanner.Snapshot(
                state, KafkaRetentionPlanner.Policy.from(config), logStartOffset, highWatermark, nowMillis);
    }

    static StableStreamHeadSnapshot head(long trimOffset) {
        return head(trimOffset, 1);
    }

    static StableStreamHeadSnapshot head(long trimOffset, long leaseVersion) {
        AppendAuthority authority = new AppendAuthority(
                "kafka-partition-leader-v1", IDENTITY.durableId().canonicalIdentity(), 3, "broker-1", 4);
        AcquiredAppendSession acquired = new AcquiredAppendSession(
                new AppendSession(STREAM_ID, "writer-1", 1, "token-1", leaseVersion, 10_000), Optional.of(authority));
        return new StableStreamHeadSnapshot(
                STREAM_ID,
                StreamState.ACTIVE,
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                trimOffset,
                40,
                400,
                1,
                "commit-1",
                Optional.of(acquired),
                sha256('c'),
                1);
    }

    static VersionedKafkaPartitionBinding binding(KafkaCheckpointReferenceRecord... checkpoints) {
        return binding(0, checkpoints);
    }

    static VersionedKafkaPartitionBinding binding(
            long observedLogStartOffset, KafkaCheckpointReferenceRecord... checkpoints) {
        return binding(observedLogStartOffset, 40, checkpoints);
    }

    static VersionedKafkaPartitionBinding binding(
            long observedLogStartOffset, long observedStableEndOffset, KafkaCheckpointReferenceRecord... checkpoints) {
        KafkaPartitionPendingOperationRecord operation = new KafkaPartitionPendingOperationRecord(
                KafkaPartitionOperationType.CREATE.wireId(), "create-test", "broker-1", 1, 20_000, 7, 10_000, "");
        var creating = KafkaPartitionMetadataTransitions.creating(
                IDENTITY.durableId(),
                IDENTITY.observedTopicName(),
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT.name(),
                7,
                10_000,
                operation);
        var active = KafkaPartitionMetadataTransitions.activate(creating, "kafka-stream", STREAM_ID.value(), 7, 10_001);
        var root = KafkaPartitionMetadataTransitions.observe(
                active,
                IDENTITY.observedTopicName(),
                7,
                1,
                3,
                4,
                observedLogStartOffset,
                observedStableEndOffset,
                10_002);
        ArrayList<KafkaCheckpointReferenceRecord> ascending = new ArrayList<>(Arrays.asList(checkpoints));
        ascending.sort(java.util.Comparator.comparingLong(KafkaCheckpointReferenceRecord::checkpointOffset));
        long now = 10_003;
        for (KafkaCheckpointReferenceRecord checkpoint : ascending) {
            root = KafkaPartitionMetadataTransitions.prependCheckpoint(
                    root, checkpoint, observedLogStartOffset, observedStableEndOffset, now++);
        }
        return new VersionedKafkaPartitionBinding(
                "/test/kafka-binding", root, 0, sha256(checkpoints.length == 0 ? 'a' : 'b'));
    }

    static KafkaCheckpointReferenceRecord checkpoint(long checkpointOffset) {
        byte[] objectSha = new byte[32];
        byte[] headSha = new byte[32];
        Arrays.fill(objectSha, (byte) checkpointOffset);
        Arrays.fill(headSha, (byte) 0x6b);
        return new KafkaCheckpointReferenceRecord(
                1,
                "checkpoint-" + checkpointOffset,
                "kafka/checkpoint-" + checkpointOffset,
                1_024,
                objectSha,
                checkpointOffset,
                0,
                1,
                headSha,
                "test-build",
                10_000 + checkpointOffset);
    }

    static KafkaTrimBarrier.VerifiedCheckpoint verified(KafkaCheckpointReferenceRecord checkpoint) {
        return new KafkaTrimBarrier.VerifiedCheckpoint(checkpoint, checkpoint.objectSha256());
    }

    private static VirtualSegment segment(
            LogConfigHistoryEntry config,
            long baseOffset,
            long endOffset,
            long rollSequence,
            long createdAt,
            long closedAt,
            long largestTimestamp,
            long firstCumulativeBytes,
            long lastCumulativeBytes,
            SegmentState state) {
        return new VirtualSegment(
                baseOffset,
                endOffset,
                rollSequence,
                createdAt,
                closedAt,
                0,
                largestTimestamp,
                endOffset - 1,
                lastCumulativeBytes - firstCumulativeBytes,
                firstCumulativeBytes,
                lastCumulativeBytes,
                config.configDigest(),
                rollSequence == 0 ? RollReason.INITIAL : RollReason.SIZE,
                state);
    }

    private static KafkaPartitionIdentity identity() {
        ByteBuffer bytes =
                ByteBuffer.allocate(16).putLong(0x1234_5678_9abc_def0L).putLong(99);
        return new KafkaPartitionIdentity(
                "kraft", Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array()), 3, "orders");
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }
}
