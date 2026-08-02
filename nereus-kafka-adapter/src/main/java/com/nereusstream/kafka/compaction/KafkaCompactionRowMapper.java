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

import com.nereusstream.materialization.RewrittenCompactionRecord;
import com.nereusstream.objectstore.compacted.KafkaCompactionDispositionV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import java.util.Objects;

/**
 * Lossless boundary from a verified Kafka survivor rewrite to one sparse NTC2 row.
 */
public final class KafkaCompactionRowMapper {

    public KafkaTopicCompactedObjectRow toNtc2Row(RewrittenCompactionRecord rewritten) {
        Objects.requireNonNull(rewritten, "rewritten");
        return new KafkaTopicCompactedObjectRow(
                rewritten.absoluteOffset(),
                1,
                disposition(rewritten),
                rewritten.taggedCompactionKey(),
                rewritten.exactPayload(),
                rewritten.payloadCrc32c(),
                rewritten.sourceBatchBaseOffset(),
                rewritten.sourceRecordIndex(),
                rewritten.sourceBatchSha256(),
                rewritten.eventTimeMillis());
    }

    private static KafkaCompactionDispositionV2 disposition(RewrittenCompactionRecord rewritten) {
        return switch (rewritten.disposition()) {
            case RETAIN_VALUE -> KafkaCompactionDispositionV2.RETAIN_VALUE;
            case RETAIN_TOMBSTONE -> KafkaCompactionDispositionV2.RETAIN_TOMBSTONE;
            case RETAIN_UNKEYED -> KafkaCompactionDispositionV2.RETAIN_UNKEYED;
            case RETAIN_CONTROL -> KafkaCompactionDispositionV2.RETAIN_CONTROL;
        };
    }
}
