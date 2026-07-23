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

package com.nereusstream.kafka.codec;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;

/** Converts stock Kafka-validated raw batches to one offset-conditional Nereus append. */
public final class KafkaAppendBatchEncoder {
    private final KafkaRecordBatchCodec codec;

    public KafkaAppendBatchEncoder(KafkaRecordBatchCodec codec) {
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    public EncodedKafkaAppend encode(ByteBuffer validRecords, long expectedStartOffset) {
        if (expectedStartOffset < 0) {
            throw new IllegalArgumentException("expectedStartOffset must be non-negative");
        }
        List<KafkaRecordBatch> recordBatches = codec.decodeAll(validRecords);
        if (recordBatches.isEmpty()) {
            throw new IllegalArgumentException("Kafka append cannot be empty");
        }

        List<AppendEntry> entries = new ArrayList<>(recordBatches.size());
        CRC32C concatenatedChecksum = new CRC32C();
        long expectedBase = expectedStartOffset;
        long minEventTimeMillis = Long.MAX_VALUE;
        long maxEventTimeMillis = 0;
        int totalRecords = 0;
        int totalBytes = 0;
        for (KafkaRecordBatch batch : recordBatches) {
            if (batch.baseOffset() != expectedBase) {
                throw new IllegalArgumentException(
                        "Kafka batches are not dense: expected base " + expectedBase + " but found " + batch.baseOffset());
            }
            int recordCount = batch.logicalRecordCount();
            long eventTimeMillis = Math.max(0, batch.maxTimestamp());
            byte[] payload = batch.encodedBytes();
            entries.add(new AppendEntry(payload, recordCount, eventTimeMillis, Map.of()));
            concatenatedChecksum.update(payload, 0, payload.length);
            minEventTimeMillis = Math.min(minEventTimeMillis, eventTimeMillis);
            maxEventTimeMillis = Math.max(maxEventTimeMillis, eventTimeMillis);
            totalRecords = Math.addExact(totalRecords, recordCount);
            totalBytes = Math.addExact(totalBytes, payload.length);
            expectedBase = batch.nextOffset();
        }

        OffsetRange range = new OffsetRange(expectedStartOffset, expectedBase);
        AppendBatch appendBatch = new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                entries,
                totalRecords,
                entries.size(),
                minEventTimeMillis,
                maxEventTimeMillis,
                List.of(),
                Map.of(),
                Optional.of(new Checksum(
                        ChecksumType.CRC32C,
                        String.format(Locale.ROOT, "%08x", concatenatedChecksum.getValue()))));
        return new EncodedKafkaAppend(appendBatch, range, totalBytes, recordBatches);
    }
}
