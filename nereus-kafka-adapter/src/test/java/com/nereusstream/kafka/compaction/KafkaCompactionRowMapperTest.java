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

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.materialization.RewrittenCompactionRecord;
import com.nereusstream.materialization.RewrittenCompactionRecord.Disposition;
import com.nereusstream.objectstore.compacted.KafkaCompactionDispositionV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import java.nio.ByteBuffer;
import java.util.OptionalLong;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

class KafkaCompactionRowMapperTest {
    private final KafkaCompactionRowMapper mapper = new KafkaCompactionRowMapper();

    @Test
    void mapsEveryRetainedDispositionWithoutChangingVerifiedBytesOrSourceIdentity() {
        assertMapped(10, Disposition.RETAIN_VALUE, KafkaCompactionDispositionV2.RETAIN_VALUE);
        assertMapped(11, Disposition.RETAIN_TOMBSTONE, KafkaCompactionDispositionV2.RETAIN_TOMBSTONE);
        assertMapped(12, Disposition.RETAIN_UNKEYED, KafkaCompactionDispositionV2.RETAIN_UNKEYED);
        assertMapped(13, Disposition.RETAIN_CONTROL, KafkaCompactionDispositionV2.RETAIN_CONTROL);
    }

    private void assertMapped(long offset, Disposition disposition, KafkaCompactionDispositionV2 expectedDisposition) {
        byte[] payload = new byte[] {2, (byte) offset, 7};
        ByteBuffer key =
                switch (disposition) {
                    case RETAIN_VALUE, RETAIN_TOMBSTONE ->
                        KafkaCompactionKeyEncodingV2.keyed(ByteBuffer.wrap(new byte[] {(byte) offset}));
                    case RETAIN_UNKEYED -> KafkaCompactionKeyEncodingV2.nullKey(offset);
                    case RETAIN_CONTROL -> KafkaCompactionKeyEncodingV2.control(offset);
                };
        RewrittenCompactionRecord rewritten = new RewrittenCompactionRecord(
                offset,
                disposition,
                key,
                ByteBuffer.wrap(payload),
                crc32c(payload),
                offset - 1,
                1,
                new Checksum(ChecksumType.SHA256, "a".repeat(64)),
                OptionalLong.of(1_000 + offset));

        KafkaTopicCompactedObjectRow row = mapper.toNtc2Row(rewritten);

        assertThat(row.streamOffsetStart()).isEqualTo(offset);
        assertThat(row.recordCount()).isEqualTo(1);
        assertThat(row.disposition()).isEqualTo(expectedDisposition);
        assertThat(row.compactionKey()).isEqualTo(key);
        assertThat(row.exactPayload()).isEqualTo(ByteBuffer.wrap(payload));
        assertThat(row.payloadCrc32c()).isEqualTo(rewritten.payloadCrc32c());
        assertThat(row.sourceBatchBaseOffset()).isEqualTo(offset - 1);
        assertThat(row.sourceRecordIndex()).isEqualTo(1);
        assertThat(row.sourceBatchSha256()).isEqualTo(rewritten.sourceBatchSha256());
        assertThat(row.eventTimeMillis()).isEqualTo(OptionalLong.of(1_000 + offset));
    }

    private static int crc32c(byte[] payload) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(payload, 0, payload.length);
        return (int) crc32c.getValue();
    }
}
