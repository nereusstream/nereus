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

package com.nereusstream.kafka.bookkeeper.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.jupiter.api.Test;

class KafkaRecordBatchBudgetV2Test {
    @Test
    void compressedRecordBytesCannotBypassDecodedByteBudget() throws IOException {
        var value = new byte[32768];
        Arrays.fill(value, (byte) 7);
        var body = KafkaSemanticCompactorV1Test.records(0, 1, Compression.gzip().build(), new SimpleRecord(1, value));
        assertThat(body.length()).isLessThan(1024);
        assertThatThrownBy(() -> KafkaRecordBatchCodecV1.parseBounded(body, 1, 1024))
                .hasMessage("Kafka decoded record budget exhausted");
        var read = KafkaRecordBatchCodecV1.parseBounded(body, 1, 65536);
        assertThat(read.batch()).isEqualTo(KafkaRecordBatchCodecV1.parse(body));
        assertThat(read.decodedBytes()).isGreaterThan(value.length);

        // A valid batch CRC encloses only a huge length varint, with no record body to allocate.
        var declaredLength = ByteBuffer.allocate(5);
        ByteUtils.writeVarint(Integer.MAX_VALUE, declaredLength);
        var oversized = compressedFraming(declaredLength.array(), 1);
        assertThat(oversized.length()).isLessThan(128);
        assertThatThrownBy(() -> KafkaRecordBatchCodecV1.parseBounded(oversized, 1, 1024))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Kafka decoded record budget exhausted");
        var truncated = compressedFraming(new byte[] {20}, 1);
        assertThatThrownBy(() -> KafkaRecordBatchCodecV1.parseBounded(truncated, 1, 1024))
                .hasMessage("Kafka record body is truncated");
    }

    @Test
    void exactRecordCountBoundRejectsNextRecordBeforeMaterialization() throws IOException {
        var body = KafkaSemanticCompactorV1Test.records(
                0, 1, Compression.NONE, new SimpleRecord(1, new byte[1]), new SimpleRecord(2, new byte[1]));
        assertThatThrownBy(() -> KafkaRecordBatchCodecV1.parseBounded(body, 1, 1000))
                .hasMessage("Kafka decoded record budget exhausted");
        assertThat(KafkaRecordBatchCodecV1.parseBounded(body, 2, 1000).batch().records())
                .hasSize(2);
        var undeclared = compressedFraming(new byte[] {0}, 0);
        assertThatThrownBy(() -> KafkaRecordBatchCodecV1.parseBounded(undeclared, 0, 0))
                .hasMessage("Kafka RecordBatch has trailing decoded record bytes");
    }

    @Test
    void genuineEmptyBatchNeedsNoDecodedRecordBudget() {
        var empty = KafkaSemanticCompactorV1Test.emptyBatch(0, 1);
        var read = KafkaRecordBatchCodecV1.parseBounded(empty, 0, 0);
        assertThat(read.batch().records()).isEmpty();
        assertThat(read.decodedBytes()).isZero();
    }

    private static CanonicalBytes compressedFraming(byte[] decoded, int count) throws IOException {
        var compressed = new ByteArrayOutputStream();
        try (var gzip = new GZIPOutputStream(compressed)) {
            gzip.write(decoded);
        }
        var encoded = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + compressed.size());
        encoded.position(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        encoded.put(compressed.toByteArray());
        encoded.position(0);
        DefaultRecordBatch.writeHeader(
                encoded,
                0,
                0,
                encoded.capacity(),
                RecordBatch.MAGIC_VALUE_V2,
                CompressionType.GZIP,
                TimestampType.CREATE_TIME,
                1,
                1,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                false,
                false,
                1,
                count);
        encoded.position(0);
        MemoryRecords.readableRecords(encoded).batches().iterator().next().ensureValid();
        return CanonicalBytes.copyOf(encoded.array());
    }
}
