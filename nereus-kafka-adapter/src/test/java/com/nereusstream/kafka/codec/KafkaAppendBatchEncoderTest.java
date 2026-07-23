/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendResult;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StreamId;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
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
    void rejectsWrongStartGapsAndEmptyAppends() {
        byte[] first = KafkaRecordBatchTestSupport.batch(10, CompressionType.NONE, 1_000, "a");
        byte[] gap = KafkaRecordBatchTestSupport.batch(12, CompressionType.NONE, 2_000, "b");

        assertThatThrownBy(() -> encoder.encode(ByteBuffer.wrap(first), 9))
                .hasMessageContaining("expected base 9");
        assertThatThrownBy(() -> encoder.encode(
                        ByteBuffer.wrap(KafkaRecordBatchTestSupport.concat(first, gap)), 10))
                .hasMessageContaining("not dense");
        assertThatThrownBy(() -> encoder.encode(ByteBuffer.allocate(0), 0))
                .hasMessageContaining("cannot be empty");

    }

    @Test
    void preservesIdempotentTransactionalAndControlBatchesExactly() {
        MemoryRecords idempotent = MemoryRecords.withIdempotentRecords(
                10,
                Compression.of(CompressionType.NONE).build(),
                7,
                (short) 1,
                0,
                2,
                new SimpleRecord(1_000, "v".getBytes()));
        byte[] idempotentBytes = KafkaRecordBatchTestSupport.bytes(idempotent);
        EncodedKafkaAppend encodedIdempotent =
                encoder.encode(ByteBuffer.wrap(idempotentBytes), 10);

        assertThat(encodedIdempotent.range())
                .isEqualTo(new OffsetRange(10, 11));
        assertThat(encodedIdempotent.recordBatches().get(0).producerId())
                .isEqualTo(7);
        assertThat(encodedIdempotent.appendBatch().entries().get(0).payload())
                .isEqualTo(idempotentBytes);

        MemoryRecords transactional = MemoryRecords.withTransactionalRecords(
                11,
                Compression.of(CompressionType.GZIP).build(),
                8,
                (short) 2,
                5,
                3,
                new SimpleRecord(2_000, "txn".getBytes()));
        MemoryRecords marker = MemoryRecords.withEndTransactionMarker(
                12,
                2_001,
                3,
                8,
                (short) 2,
                new EndTransactionMarker(ControlRecordType.ABORT, 4));
        byte[] transactionalBytes =
                KafkaRecordBatchTestSupport.bytes(transactional);
        byte[] markerBytes = KafkaRecordBatchTestSupport.bytes(marker);
        EncodedKafkaAppend encodedTransaction = encoder.encode(
                ByteBuffer.wrap(KafkaRecordBatchTestSupport.concat(
                        transactionalBytes, markerBytes)),
                11);

        assertThat(encodedTransaction.range())
                .isEqualTo(new OffsetRange(11, 13));
        assertThat(encodedTransaction.recordBatches().get(0).transactional())
                .isTrue();
        assertThat(encodedTransaction.recordBatches().get(1).controlBatch())
                .isTrue();
        assertThat(encodedTransaction.recordBatches().get(1).baseSequence())
                .isEqualTo(RecordBatch.NO_SEQUENCE);
        assertThat(encodedTransaction.appendBatch().entries().get(0).payload())
                .isEqualTo(transactionalBytes);
        assertThat(encodedTransaction.appendBatch().entries().get(1).payload())
                .isEqualTo(markerBytes);
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
