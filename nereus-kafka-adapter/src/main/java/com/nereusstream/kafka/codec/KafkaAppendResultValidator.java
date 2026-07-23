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

import com.nereusstream.api.AppendResult;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Fail-closed validator for the stable Nereus result returned to the Kafka log path. */
public final class KafkaAppendResultValidator {
    private KafkaAppendResultValidator() {}

    public static AppendResult validate(
            StreamId expectedStreamId,
            EncodedKafkaAppend expectedAppend,
            AppendResult actual) {
        Objects.requireNonNull(expectedStreamId, "expectedStreamId");
        Objects.requireNonNull(expectedAppend, "expectedAppend");
        Objects.requireNonNull(actual, "actual");
        if (!actual.streamId().equals(expectedStreamId)
                || !actual.range().equals(expectedAppend.range())
                || actual.committedEndOffset() != expectedAppend.range().endOffset()
                || actual.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH
                || actual.recordCount() != expectedAppend.appendBatch().recordCount()
                || actual.entryCount() != expectedAppend.appendBatch().entryCount()
                || actual.logicalBytes() != expectedAppend.encodedBytes()
                || !actual.schemaRefs().isEmpty()
                || actual.projectionRef().isPresent()) {
            throw new IllegalStateException("Nereus append result does not exactly match the encoded Kafka append");
        }
        return actual;
    }
}
