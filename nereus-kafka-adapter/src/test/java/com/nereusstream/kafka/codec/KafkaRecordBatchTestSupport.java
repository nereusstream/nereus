/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.codec;

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
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32C;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

final class KafkaRecordBatchTestSupport {
    private KafkaRecordBatchTestSupport() {}

    static byte[] batch(long baseOffset, CompressionType compressionType, long timestamp, String... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int index = 0; index < values.length; index++) {
            records[index] = new SimpleRecord(timestamp + index, values[index].getBytes());
        }
        return bytes(MemoryRecords.withRecords(
                baseOffset,
                Compression.of(compressionType).build(),
                records));
    }

    static byte[] bytes(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    static byte[] concat(byte[]... batches) {
        int size = 0;
        for (byte[] batch : batches) {
            size = Math.addExact(size, batch.length);
        }
        ByteBuffer result = ByteBuffer.allocate(size);
        for (byte[] batch : batches) {
            result.put(batch);
        }
        return result.array();
    }

    static void recomputeCrc(byte[] encodedBatch) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(encodedBatch, 21, encodedBatch.length - 21);
        ByteBuffer.wrap(encodedBatch).putInt(17, (int) crc32c.getValue());
    }

    static ReadBatch readBatch(OffsetRange range, byte[] payload, String suffix) {
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
                new ObjectId("kafka-batch-" + suffix),
                new ObjectKey("f9/kafka-batch-" + suffix),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "KAFKA_RECORD_BATCH_V1",
                "slice-" + suffix,
                0,
                payload.length,
                checksum,
                index);
        ReadSourceRef source = new ReadSourceRef(
                range,
                0,
                1,
                target,
                ReadTargetIdentities.sha256(target));
        return new ReadBatch(
                range,
                PayloadFormat.KAFKA_RECORD_BATCH,
                payload,
                List.of(),
                Optional.empty(),
                source);
    }
}
