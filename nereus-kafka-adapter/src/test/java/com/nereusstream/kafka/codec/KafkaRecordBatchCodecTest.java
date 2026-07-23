/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

class KafkaRecordBatchCodecTest {
    private final KafkaRecordBatchCodec codec = new KafkaRecordBatchCodec();

    @Test
    void decodesStockKafkaBatchesWithoutChangingTheCallerBuffer() {
        byte[] first = KafkaRecordBatchTestSupport.batch(10, CompressionType.NONE, 1_000, "a", "b");
        byte[] second = KafkaRecordBatchTestSupport.batch(12, CompressionType.GZIP, 2_000, "c");
        byte[] records = KafkaRecordBatchTestSupport.concat(first, second);
        ByteBuffer input = ByteBuffer.allocate(records.length + 9);
        input.position(4).put(records).putInt(0x1234_5678).flip().position(4).limit(4 + records.length);
        int originalPosition = input.position();
        int originalLimit = input.limit();

        List<KafkaRecordBatch> decoded = codec.decodeAll(input);

        assertThat(input.position()).isEqualTo(originalPosition);
        assertThat(input.limit()).isEqualTo(originalLimit);
        assertThat(decoded).hasSize(2);
        assertThat(decoded.get(0).encodedBytes()).isEqualTo(first);
        assertThat(decoded.get(0).baseOffset()).isEqualTo(10);
        assertThat(decoded.get(0).lastOffset()).isEqualTo(11);
        assertThat(decoded.get(0).recordCount()).isEqualTo(2);
        assertThat(decoded.get(1).encodedBytes()).isEqualTo(second);
        assertThat(decoded.get(1).baseOffset()).isEqualTo(12);
        assertThat(decoded.get(1).compressionTypeId()).isEqualTo(CompressionType.GZIP.id);
        decoded.forEach(batch -> assertThat(batch.checksum()).isPositive());
    }

    @Test
    void preservesProducerFactsFromAStockIdempotentBatch() {
        MemoryRecords records = MemoryRecords.withIdempotentRecords(
                44,
                Compression.of(CompressionType.NONE).build(),
                987,
                (short) 3,
                11,
                8,
                new SimpleRecord(5_000, "key".getBytes(), "value".getBytes()));

        KafkaRecordBatch batch = codec.decode(KafkaRecordBatchTestSupport.bytes(records));

        assertThat(batch.baseOffset()).isEqualTo(44);
        assertThat(batch.lastOffset()).isEqualTo(44);
        assertThat(batch.producerId()).isEqualTo(987);
        assertThat(batch.producerEpoch()).isEqualTo((short) 3);
        assertThat(batch.baseSequence()).isEqualTo(11);
        assertThat(batch.partitionLeaderEpoch()).isEqualTo(8);
    }

    @Test
    void failsClosedForCrcLengthMagicCompressionAndProducerCorruption() {
        byte[] valid = KafkaRecordBatchTestSupport.batch(1, CompressionType.NONE, 1_000, "v");

        byte[] badCrc = valid.clone();
        badCrc[badCrc.length - 1] ^= 1;
        assertThatThrownBy(() -> codec.decode(badCrc)).hasMessageContaining("CRC mismatch");

        assertThatThrownBy(() -> codec.decode(ByteBuffer.wrap(valid, 0, valid.length - 1)))
                .hasMessageContaining("declares")
                .hasMessageContaining("remain");

        byte[] badMagic = valid.clone();
        badMagic[16] = 1;
        assertThatThrownBy(() -> codec.decode(badMagic)).hasMessageContaining("magic 1");

        byte[] badCompression = valid.clone();
        badCompression[22] = 7;
        KafkaRecordBatchTestSupport.recomputeCrc(badCompression);
        assertThatThrownBy(() -> codec.decode(badCompression)).hasMessageContaining("compression type id 7");

        byte[] badProducer = valid.clone();
        ByteBuffer.wrap(badProducer).putShort(51, (short) 2);
        KafkaRecordBatchTestSupport.recomputeCrc(badProducer);
        assertThatThrownBy(() -> codec.decode(badProducer)).hasMessageContaining("producer epoch and sequence -1");
    }

    @Test
    void returnsDefensiveOwnedBytes() {
        byte[] bytes = KafkaRecordBatchTestSupport.batch(0, CompressionType.NONE, 1_000, "v");
        KafkaRecordBatch batch = codec.decode(bytes);
        bytes[0] = 99;
        byte[] returned = batch.encodedBytes();
        returned[0] = 88;

        assertThat(batch.baseOffset()).isZero();
        assertThat(batch.encodedBytes()[0]).isZero();
        assertThat(batch.encodedBuffer().isReadOnly()).isTrue();
        assertThat(batch.encodedBuffer().position()).isZero();
        assertThat(batch.encodedBuffer().remaining()).isEqualTo(batch.sizeInBytes());
    }
}
