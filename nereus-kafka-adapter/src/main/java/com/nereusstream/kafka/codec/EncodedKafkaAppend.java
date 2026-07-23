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
import com.nereusstream.api.OffsetRange;
import java.util.List;
import java.util.Objects;

/** Exact Nereus append value and validation facts derived from stock Kafka-validated records. */
public record EncodedKafkaAppend(
        AppendBatch appendBatch,
        OffsetRange range,
        int encodedBytes,
        List<KafkaRecordBatch> recordBatches) {
    public EncodedKafkaAppend {
        Objects.requireNonNull(appendBatch, "appendBatch");
        Objects.requireNonNull(range, "range");
        recordBatches = List.copyOf(Objects.requireNonNull(recordBatches, "recordBatches"));
        if (range.isEmpty() || encodedBytes <= 0 || recordBatches.isEmpty()) {
            throw new IllegalArgumentException("encoded Kafka append must be non-empty");
        }
        if (appendBatch.recordCount() != range.recordCount()) {
            throw new IllegalArgumentException("append record count must equal encoded offset span");
        }
        if (appendBatch.entryCount() != recordBatches.size()) {
            throw new IllegalArgumentException("append entry count must equal Kafka batch count");
        }
    }
}
