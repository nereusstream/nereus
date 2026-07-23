/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.kafka.metadata.KafkaPartitionBinding;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

final class KafkaPartitionStorageTestSupport {
    private KafkaPartitionStorageTestSupport() {}

    static KafkaPartitionIdentity identity() {
        ByteBuffer bytes = ByteBuffer.allocate(16).putLong(0x1234_5678_9abc_def0L).putLong(1);
        return new KafkaPartitionIdentity(
                "kraft",
                Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array()),
                3,
                "orders");
    }

    static KafkaPartitionBinding binding(StorageProfile profile) {
        KafkaPartitionIdentity identity = identity();
        KafkaPartitionPendingOperationRecord operation = new KafkaPartitionPendingOperationRecord(
                KafkaPartitionOperationType.CREATE.wireId(),
                "create-test",
                "broker-test",
                1,
                20_000,
                1,
                10_000,
                "");
        var creating = KafkaPartitionMetadataTransitions.creating(
                identity.durableId(), identity.observedTopicName(), profile.name(), 1, 10_000, operation);
        StreamName streamName = new StreamName("kafka-partition-test-stream");
        StreamId streamId = new StreamId("kafka-partition-test-stream-id");
        var active = KafkaPartitionMetadataTransitions.activate(
                creating, streamName.value(), streamId.value(), 1, 10_001);
        return new KafkaPartitionBinding(
                identity,
                streamName,
                streamId,
                new VersionedKafkaPartitionBinding(
                        "/test/kafka-binding",
                        active,
                        0,
                        new Checksum(ChecksumType.SHA256, "a".repeat(64))));
    }

    static KafkaPartitionOpenPlan openPlan(KafkaLeaderAuthority authority) {
        StorageProfile profile = StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT;
        return new KafkaPartitionOpenPlan(
                authority,
                binding(profile),
                KafkaStorageProfilePolicy.forProfile(profile),
                java.time.Duration.ofSeconds(5));
    }

    static byte[] batch(long baseOffset, CompressionType type, long timestamp, String... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int index = 0; index < values.length; index++) {
            records[index] = new SimpleRecord(timestamp + index, values[index].getBytes());
        }
        ByteBuffer buffer = MemoryRecords.withRecords(
                baseOffset, Compression.of(type).build(), records).buffer().duplicate();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    static byte[] concat(byte[]... values) {
        int size = 0;
        for (byte[] value : values) size = Math.addExact(size, value.length);
        ByteBuffer result = ByteBuffer.allocate(size);
        for (byte[] value : values) result.put(value);
        return result.array();
    }

    static ReadBatch readBatch(OffsetRange range, byte[] payload, int ordinal) {
        Checksum checksum = new Checksum(ChecksumType.CRC32C, "00000000");
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                1,
                checksum);
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                new ObjectId("kafka-partition-object-" + ordinal),
                new ObjectKey("f9/kafka-partition-object-" + ordinal),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "KAFKA_RECORD_BATCH_V1",
                "slice-" + ordinal,
                0,
                payload.length,
                checksum,
                index);
        return new ReadBatch(
                range,
                PayloadFormat.KAFKA_RECORD_BATCH,
                payload,
                List.of(),
                Optional.empty(),
                new ReadSourceRef(range, 0, 1, target, ReadTargetIdentities.sha256(target)));
    }
}
