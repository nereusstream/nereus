/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.junit.jupiter.api.Test;

class KafkaFetchAssemblerTest {
    private final KafkaFetchAssembler assembler = new KafkaFetchAssembler(new KafkaRecordBatchCodec());

    @Test
    void returnsOwnedByteExactMemoryRecordsAndContainingBatchFacts() {
        byte[] first = KafkaRecordBatchTestSupport.batch(10, CompressionType.GZIP, 1_000, "a", "b");
        byte[] second = KafkaRecordBatchTestSupport.batch(12, CompressionType.NONE, 2_000, "c");
        SemanticReadResult read = committedRead(
                11,
                List.of(
                        KafkaRecordBatchTestSupport.readBatch(new OffsetRange(10, 12), first, "first"),
                        KafkaRecordBatchTestSupport.readBatch(new OffsetRange(12, 13), second, "second")),
                13);

        KafkaFetchAssembly result = assembler.assemble(
                read,
                first.length + second.length,
                true,
                10,
                0,
                List.of());

        assertThat(result.encodedRecords()).isEqualTo(KafkaRecordBatchTestSupport.concat(first, second));
        assertThat(result.actualFirstBatchBaseOffset()).hasValue(10);
        assertThat(result.nextLogicalOffset()).isEqualTo(13);
        assertThat(result.sourceCoverageEndOffset()).isEqualTo(13);
        assertThat(result.firstEntryOverflow()).isTrue();
        assertThat(result.recordsBuffer().isReadOnly()).isTrue();
        assertThat(result.recordsBuffer().position()).isZero();
        MemoryRecords kafkaRecords = MemoryRecords.readableRecords(result.recordsBuffer());
        List<RecordBatchFacts> facts = new ArrayList<>();
        kafkaRecords.batches().forEach(batch -> facts.add(new RecordBatchFacts(
                batch.baseOffset(), batch.lastOffset(), batch.isValid())));
        assertThat(facts).containsExactly(
                new RecordBatchFacts(10, 11, true),
                new RecordBatchFacts(12, 12, true));

        byte[] returned = result.encodedRecords();
        returned[0] = 99;
        assertThat(result.encodedRecords()[0]).isEqualTo(first[0]);
    }

    @Test
    void preservesSparseCompactedCoverageAndAbortedTransactionFacts() {
        byte[] first = KafkaRecordBatchTestSupport.batch(0, CompressionType.NONE, 1_000, "a", "b");
        byte[] second = KafkaRecordBatchTestSupport.batch(5, CompressionType.NONE, 2_000, "f");
        List<ReadBatch> batches = List.of(
                KafkaRecordBatchTestSupport.readBatch(new OffsetRange(0, 2), first, "compact-first"),
                KafkaRecordBatchTestSupport.readBatch(new OffsetRange(5, 6), second, "compact-second"));
        ReadResult raw = new ReadResult(new StreamId("kafka-stream"), 0, 6, batches, false);
        SemanticReadResult compacted = new SemanticReadResult(ReadView.TOPIC_COMPACTED, raw, 8);
        KafkaAbortedTransaction aborted = new KafkaAbortedTransaction(9, 1);

        KafkaFetchAssembly result = assembler.assemble(
                compacted,
                first.length + second.length,
                false,
                0,
                123,
                List.of(aborted));

        assertThat(result.nextLogicalOffset()).isEqualTo(6);
        assertThat(result.sourceCoverageEndOffset()).isEqualTo(8);
        assertThat(result.relativeLogicalBytePosition()).isEqualTo(123);
        assertThat(result.abortedTransactions()).containsExactly(aborted);
    }

    @Test
    void rejectsHardLimitRangeFormatCrcAndOrderingViolations() {
        byte[] first = KafkaRecordBatchTestSupport.batch(10, CompressionType.NONE, 1_000, "a");
        ReadBatch valid = KafkaRecordBatchTestSupport.readBatch(new OffsetRange(10, 11), first, "valid");
        SemanticReadResult read = committedRead(10, List.of(valid), 11);

        assertThatThrownBy(() -> assembler.assemble(read, first.length - 1, false, 10, 0, List.of()))
                .hasMessageContaining("hard response limit");

        ReadBatch wrongRange = KafkaRecordBatchTestSupport.readBatch(new OffsetRange(10, 12), first, "wrong-range");
        assertThatThrownBy(() -> assembler.assemble(
                        committedRead(10, List.of(wrongRange), 12), first.length, false, 10, 0, List.of()))
                .hasMessageContaining("offsets do not match");

        ReadBatch wrongFormat = new ReadBatch(
                valid.range(),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                valid.payload(),
                List.of(),
                Optional.empty(),
                valid.source());
        assertThatThrownBy(() -> assembler.assemble(
                        committedRead(10, List.of(wrongFormat), 11), first.length, false, 10, 0, List.of()))
                .hasMessageContaining("non-Kafka payload");

        byte[] corrupt = first.clone();
        corrupt[corrupt.length - 1] ^= 1;
        ReadBatch badCrc = KafkaRecordBatchTestSupport.readBatch(new OffsetRange(10, 11), corrupt, "crc");
        assertThatThrownBy(() -> assembler.assemble(
                        committedRead(10, List.of(badCrc), 11), first.length, false, 10, 0, List.of()))
                .hasMessageContaining("CRC mismatch");

        assertThatThrownBy(() -> assembler.assemble(read, first.length, false, 11, 0, List.of()))
                .hasMessageContaining("virtual segment base");
    }

    @Test
    void returnsAnEmptyOwnedResultWithoutInventingOffsets() {
        ReadResult raw = new ReadResult(new StreamId("kafka-stream"), 7, 7, List.of(), true);
        KafkaFetchAssembly result = assembler.assemble(
                new SemanticReadResult(ReadView.COMMITTED, raw, 7),
                1,
                false,
                7,
                0,
                List.of());

        assertThat(result.encodedRecords()).isEmpty();
        assertThat(result.actualFirstBatchBaseOffset()).isEmpty();
        assertThat(result.nextLogicalOffset()).isEqualTo(7);
    }

    private static SemanticReadResult committedRead(long requestedOffset, List<ReadBatch> batches, long nextOffset) {
        ReadResult raw = new ReadResult(new StreamId("kafka-stream"), requestedOffset, nextOffset, batches, false);
        return new SemanticReadResult(ReadView.COMMITTED, raw, nextOffset);
    }

    private record RecordBatchFacts(long baseOffset, long lastOffset, boolean valid) {}
}
