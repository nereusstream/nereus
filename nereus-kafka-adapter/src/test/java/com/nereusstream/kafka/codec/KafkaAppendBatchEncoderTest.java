/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendResult;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StreamId;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

class KafkaAppendBatchEncoderTest {
    private final KafkaAppendBatchEncoder encoder = new KafkaAppendBatchEncoder(new KafkaRecordBatchCodec());

    @Test
    void emitsOneExactNereusEntryPerDenseKafkaBatch() {
        byte[] first = KafkaRecordBatchTestSupport.batch(10, CompressionType.NONE, 1_000, "a", "b");
        byte[] second = KafkaRecordBatchTestSupport.batch(12, CompressionType.GZIP, 2_000, "c", "d", "e");
        ByteBuffer input = ByteBuffer.wrap(KafkaRecordBatchTestSupport.concat(first, second));
        int originalPosition = input.position();

        EncodedKafkaAppend encoded = encoder.encode(input, 10);

        assertThat(input.position()).isEqualTo(originalPosition);
        assertThat(encoded.range()).isEqualTo(new OffsetRange(10, 15));
        assertThat(encoded.encodedBytes()).isEqualTo(first.length + second.length);
        assertThat(encoded.appendBatch().payloadFormat()).isEqualTo(PayloadFormat.KAFKA_RECORD_BATCH);
        assertThat(encoded.appendBatch().recordCount()).isEqualTo(5);
        assertThat(encoded.appendBatch().entryCount()).isEqualTo(2);
        assertThat(encoded.appendBatch().minEventTimeMillis()).isEqualTo(1_001);
        assertThat(encoded.appendBatch().maxEventTimeMillis()).isEqualTo(2_002);
        assertThat(encoded.appendBatch().entries().get(0).payload()).isEqualTo(first);
        assertThat(encoded.appendBatch().entries().get(1).payload()).isEqualTo(second);
        assertThat(encoded.appendBatch().checksum()).isPresent();
    }

    @Test
    void rejectsWrongStartGapsEmptyAndM4ProducerStateBatches() {
        byte[] first = KafkaRecordBatchTestSupport.batch(10, CompressionType.NONE, 1_000, "a");
        byte[] gap = KafkaRecordBatchTestSupport.batch(12, CompressionType.NONE, 2_000, "b");

        assertThatThrownBy(() -> encoder.encode(ByteBuffer.wrap(first), 9))
                .hasMessageContaining("expected base 9");
        assertThatThrownBy(() -> encoder.encode(
                        ByteBuffer.wrap(KafkaRecordBatchTestSupport.concat(first, gap)), 10))
                .hasMessageContaining("not dense");
        assertThatThrownBy(() -> encoder.encode(ByteBuffer.allocate(0), 0))
                .hasMessageContaining("cannot be empty");

        MemoryRecords idempotent = MemoryRecords.withIdempotentRecords(
                10,
                Compression.of(CompressionType.NONE).build(),
                7,
                (short) 1,
                0,
                2,
                new SimpleRecord(1_000, "v".getBytes()));
        assertThatThrownBy(() -> encoder.encode(
                        ByteBuffer.wrap(KafkaRecordBatchTestSupport.bytes(idempotent)), 10))
                .isInstanceOfSatisfying(NereusException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ErrorCode.UNSUPPORTED_FORMAT))
                .hasMessageContaining("F9-M3")
                .hasMessageContaining("non-idempotent");
    }

    @Test
    void validatesEveryStableAppendResultFact() {
        byte[] batch = KafkaRecordBatchTestSupport.batch(10, CompressionType.NONE, 1_000, "a", "b");
        EncodedKafkaAppend encoded = encoder.encode(ByteBuffer.wrap(batch), 10);
        StreamId streamId = new StreamId("kafka-stream");
        AppendResult valid = result(streamId, encoded, encoded.encodedBytes());

        assertThat(KafkaAppendResultValidator.validate(streamId, encoded, valid)).isSameAs(valid);

        AppendResult wrongBytes = result(streamId, encoded, encoded.encodedBytes() + 1);
        assertThatThrownBy(() -> KafkaAppendResultValidator.validate(streamId, encoded, wrongBytes))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exactly match");
    }

    private static AppendResult result(StreamId streamId, EncodedKafkaAppend encoded, long logicalBytes) {
        return new AppendResult(
                streamId,
                encoded.range(),
                encoded.range().endOffset(),
                logicalBytes,
                0,
                KafkaRecordBatchTestSupport.readBatch(
                        encoded.range(),
                        encoded.recordBatches().get(0).encodedBytes(),
                        "append-target").source().target(),
                PayloadFormat.KAFKA_RECORD_BATCH,
                encoded.appendBatch().recordCount(),
                encoded.appendBatch().entryCount(),
                logicalBytes,
                List.of(),
                Optional.empty(),
                1);
    }
}
