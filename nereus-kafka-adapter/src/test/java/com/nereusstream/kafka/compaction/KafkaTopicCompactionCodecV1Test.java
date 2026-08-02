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

package com.nereusstream.kafka.compaction;

import static com.nereusstream.kafka.codec.KafkaRecordBatchTestSupport.bytes;
import static com.nereusstream.kafka.codec.KafkaRecordBatchTestSupport.readBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.materialization.CompactionRewriteContext;
import com.nereusstream.materialization.DecodedCompactionRecord;
import com.nereusstream.materialization.DecodedCompactionRecord.ControlKind;
import com.nereusstream.materialization.DecodedCompactionRecord.KeyKind;
import com.nereusstream.materialization.RewrittenCompactionRecord;
import com.nereusstream.materialization.RewrittenCompactionRecord.Disposition;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

class KafkaTopicCompactionCodecV1Test {
    private final KafkaTopicCompactionCodecV1 codec = new KafkaTopicCompactionCodecV1();

    @Test
    void decodesEmptyKeyNullKeyAndTombstoneAsDistinctLogicalRecords() {
        assertThat(codec.codecId()).isEqualTo("kafka-topic-compaction-codec-v1");
        assertThat(codec.codecVersion()).isEqualTo(1);
        assertThat(codec.messageFormatSha256().value())
                .isEqualTo("2940e62ac155a477052b955c1b30a2e7e77862bb7383d240b792ca064f472104");
        MemoryRecords records = MemoryRecords.withRecords(
                10,
                Compression.gzip().build(),
                new SimpleRecord(1_000, new byte[0], utf8("a")),
                new SimpleRecord(1_001, null, utf8("b")),
                new SimpleRecord(1_002, utf8("k"), null, new Header[] {new RecordHeader("h", utf8("v"))}));

        List<DecodedCompactionRecord> decoded = decode(records, 10, 13, "key-kinds");

        assertThat(decoded).hasSize(3);
        assertThat(decoded).extracting(DecodedCompactionRecord::absoluteOffset).containsExactly(10L, 11L, 12L);
        assertThat(decoded)
                .extracting(DecodedCompactionRecord::keyKind)
                .containsExactly(KeyKind.KEYED, KeyKind.UNKEYED, KeyKind.KEYED);
        assertThat(decoded).extracting(DecodedCompactionRecord::tombstone).containsExactly(false, false, true);
        assertThat(decoded)
                .extracting(DecodedCompactionRecord::sourceRecordIndex)
                .containsExactly(0, 1, 2);
        assertThat(decoded)
                .extracting(DecodedCompactionRecord::sourceBatchSha256)
                .allMatch(decoded.get(0).sourceBatchSha256()::equals);
        assertThat(byteArray(decoded.get(0).taggedCompactionKey())).containsExactly((byte) 0x01);
        assertThat(byteArray(decoded.get(1).taggedCompactionKey())).startsWith((byte) 0x02);
    }

    @Test
    void normalizesDerivedSequencesForNonIdempotentMultiRecordBatches() {
        MemoryRecords source = MemoryRecords.withRecords(
                20,
                Compression.gzip().build(),
                new SimpleRecord(1_500, utf8("a"), utf8("one")),
                new SimpleRecord(1_501, utf8("b"), utf8("two")));

        List<DecodedCompactionRecord> decoded = decode(source, 20, 22, "non-idempotent-sequence");

        assertThat(decoded).extracting(DecodedCompactionRecord::producerId).containsOnly(-1L);
        assertThat(decoded).extracting(DecodedCompactionRecord::producerEpoch).containsOnly((short) -1);
        assertThat(decoded).extracting(DecodedCompactionRecord::sequence).containsOnly(-1);

        RewrittenCompactionRecord rewritten = codec.rewrite(
                decoded.get(1),
                new CompactionRewriteContext(
                        RecordBatch.MAGIC_VALUE_V2, codec.messageFormatSha256(), false, OptionalLong.empty()));
        RecordBatch rewrittenBatch = MemoryRecords.readableRecords(rewritten.exactPayload())
                .batches()
                .iterator()
                .next();
        assertThat(rewrittenBatch.hasProducerId()).isFalse();
        assertThat(rewrittenBatch.iterator().next().sequence()).isEqualTo(RecordBatch.NO_SEQUENCE);
    }

    @Test
    void rewritesOneKeyedSurvivorAtItsAbsoluteOffsetWithExactHeadersAndCompression() {
        MemoryRecords source = MemoryRecords.withRecords(
                10,
                Compression.gzip().build(),
                new SimpleRecord(1_000, utf8("k0"), utf8("v0")),
                new SimpleRecord(1_001, utf8("k1"), utf8("v1"), new Header[] {new RecordHeader("trace", utf8("one"))}));
        DecodedCompactionRecord survivor =
                decode(source, 10, 12, "rewrite-value").get(1);

        RewrittenCompactionRecord rewritten = codec.rewrite(
                survivor,
                new CompactionRewriteContext(
                        RecordBatch.MAGIC_VALUE_V2, codec.messageFormatSha256(), false, OptionalLong.empty()));

        assertThat(rewritten.absoluteOffset()).isEqualTo(11);
        assertThat(rewritten.disposition()).isEqualTo(Disposition.RETAIN_VALUE);
        MemoryRecords output = MemoryRecords.readableRecords(rewritten.exactPayload());
        RecordBatch batch = output.batches().iterator().next();
        Record record = batch.iterator().next();
        assertThat(batch.baseOffset()).isEqualTo(11);
        assertThat(batch.lastOffset()).isEqualTo(11);
        assertThat(batch.compressionType()).isEqualTo(CompressionType.GZIP);
        assertThat(byteArray(record.key())).containsExactly(utf8("k1"));
        assertThat(byteArray(record.value())).containsExactly(utf8("v1"));
        assertThat(record.headers()).containsExactly(new RecordHeader("trace", utf8("one")));
    }

    @Test
    void preservesTransactionalProducerAndSelectedRecordSequence() {
        MemoryRecords source = MemoryRecords.withTransactionalRecords(
                30,
                Compression.gzip().build(),
                99,
                (short) 4,
                500,
                7,
                new SimpleRecord(2_000, utf8("a"), utf8("one")),
                new SimpleRecord(2_001, utf8("b"), utf8("two")));
        DecodedCompactionRecord survivor = decode(source, 30, 32, "rewrite-txn").get(1);

        RewrittenCompactionRecord rewritten = codec.rewrite(
                survivor,
                new CompactionRewriteContext(
                        RecordBatch.MAGIC_VALUE_V2, codec.messageFormatSha256(), false, OptionalLong.empty()));

        RecordBatch batch = MemoryRecords.readableRecords(rewritten.exactPayload())
                .batches()
                .iterator()
                .next();
        Record record = batch.iterator().next();
        assertThat(batch.isTransactional()).isTrue();
        assertThat(batch.producerId()).isEqualTo(99);
        assertThat(batch.producerEpoch()).isEqualTo((short) 4);
        assertThat(record.sequence()).isEqualTo(501);
        assertThat(record.offset()).isEqualTo(31);
    }

    @Test
    void decodesAndRewritesTransactionControlMarkersWithoutChangingTheirMeaning() {
        EndTransactionMarker marker = new EndTransactionMarker(ControlRecordType.ABORT, 8);
        MemoryRecords source = MemoryRecords.withEndTransactionMarker(40, 3_000, 9, 101, (short) 5, marker);
        DecodedCompactionRecord survivor =
                decode(source, 40, 41, "rewrite-control").get(0);

        RewrittenCompactionRecord rewritten = codec.rewrite(
                survivor,
                new CompactionRewriteContext(
                        RecordBatch.MAGIC_VALUE_V2, codec.messageFormatSha256(), false, OptionalLong.of(6_000)));

        assertThat(survivor.keyKind()).isEqualTo(KeyKind.CONTROL);
        assertThat(survivor.controlKind()).isEqualTo(ControlKind.ABORT);
        assertThat(survivor.coordinatorEpoch()).isEqualTo(8);
        assertThat(rewritten.disposition()).isEqualTo(Disposition.RETAIN_CONTROL);
        RecordBatch batch = MemoryRecords.readableRecords(rewritten.exactPayload())
                .batches()
                .iterator()
                .next();
        Record record = batch.iterator().next();
        assertThat(batch.isControlBatch()).isTrue();
        assertThat(batch.deleteHorizonMs()).isEqualTo(OptionalLong.of(6_000));
        assertThat(EndTransactionMarker.deserialize(record)).isEqualTo(marker);
    }

    @Test
    void introducesTheFrozenDeleteHorizonWhenRewritingAFirstPassTombstone() {
        MemoryRecords source =
                MemoryRecords.withRecords(45, Compression.NONE, new SimpleRecord(3_500, utf8("deleted"), null));
        DecodedCompactionRecord survivor =
                decode(source, 45, 46, "rewrite-tombstone").get(0);

        RewrittenCompactionRecord rewritten = codec.rewrite(
                survivor,
                new CompactionRewriteContext(
                        RecordBatch.MAGIC_VALUE_V2, codec.messageFormatSha256(), false, OptionalLong.of(9_000)));

        RecordBatch batch = MemoryRecords.readableRecords(rewritten.exactPayload())
                .batches()
                .iterator()
                .next();
        assertThat(rewritten.disposition()).isEqualTo(Disposition.RETAIN_TOMBSTONE);
        assertThat(batch.deleteHorizonMs()).isEqualTo(OptionalLong.of(9_000));
        assertThat(batch.iterator().next().hasValue()).isFalse();
    }

    @Test
    void rejectsRangeDigestAndMessageFormatDriftBeforeRewrite() {
        MemoryRecords source =
                MemoryRecords.withRecords(50, Compression.NONE, new SimpleRecord(4_000, utf8("k"), utf8("v")));
        assertThatThrownBy(() -> decode(source, 50, 52, "wrong-range"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact source range");

        DecodedCompactionRecord exact = decode(source, 50, 51, "rewrite-drift").get(0);
        byte[] token = byteArray(exact.rewriteToken());
        token[token.length - 1] ^= 1;
        DecodedCompactionRecord changed = new DecodedCompactionRecord(
                exact.absoluteOffset(),
                exact.keyKind(),
                exact.controlKind(),
                exact.coordinatorEpoch(),
                exact.taggedCompactionKey(),
                exact.tombstone(),
                exact.eventTimeMillis(),
                exact.deleteHorizonMillis(),
                exact.sourceBatchBaseOffset(),
                exact.sourceRecordIndex(),
                exact.sourceBatchSha256(),
                exact.transactional(),
                exact.producerId(),
                exact.producerEpoch(),
                exact.sequence(),
                ByteBuffer.wrap(token));
        assertThatThrownBy(() -> codec.rewrite(
                        changed,
                        new CompactionRewriteContext(
                                RecordBatch.MAGIC_VALUE_V2, codec.messageFormatSha256(), false, OptionalLong.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");

        Checksum wrongFormat = new Checksum(ChecksumType.SHA256, "0".repeat(64));
        assertThatThrownBy(() -> codec.rewrite(
                        exact,
                        new CompactionRewriteContext(
                                RecordBatch.MAGIC_VALUE_V2, wrongFormat, false, OptionalLong.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("context");
    }

    private List<DecodedCompactionRecord> decode(MemoryRecords records, long start, long end, String suffix) {
        byte[] payload = bytes(records);
        ArrayList<DecodedCompactionRecord> decoded = new ArrayList<>();
        codec.decode(readBatch(new OffsetRange(start, end), payload, suffix), decoded::add);
        return List.copyOf(decoded);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] byteArray(ByteBuffer buffer) {
        ByteBuffer exact = buffer.asReadOnlyBuffer();
        byte[] bytes = new byte[exact.remaining()];
        exact.get(bytes);
        return bytes;
    }
}
