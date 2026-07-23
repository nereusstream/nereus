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

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SemanticReadResult;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** Verifies Nereus read batches and concatenates exact Kafka batches into an owned Fetch buffer. */
public final class KafkaFetchAssembler {
    private final KafkaRecordBatchCodec codec;

    public KafkaFetchAssembler(KafkaRecordBatchCodec codec) {
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    public KafkaFetchAssembly assemble(
            SemanticReadResult readResult,
            int maxResponseBytes,
            boolean firstEntryOverflow,
            long virtualSegmentBaseOffset,
            long relativeLogicalBytePosition,
            List<KafkaAbortedTransaction> abortedTransactions) {
        Objects.requireNonNull(readResult, "readResult");
        Objects.requireNonNull(abortedTransactions, "abortedTransactions");
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be positive");
        }
        if (virtualSegmentBaseOffset < 0 || relativeLogicalBytePosition < 0) {
            throw new IllegalArgumentException("virtual Kafka position facts must be non-negative");
        }

        List<ReadBatch> readBatches = readResult.result().batches();
        if (readBatches.isEmpty()) {
            if (firstEntryOverflow) {
                throw new IllegalArgumentException("empty Fetch result cannot report first-entry overflow");
            }
            if (readResult.result().nextOffset() != readResult.result().requestedOffset()) {
                throw new IllegalArgumentException("empty Fetch result cannot advance the logical cursor");
            }
            return new KafkaFetchAssembly(
                    new byte[0],
                    OptionalLong.empty(),
                    readResult.result().nextOffset(),
                    readResult.sourceCoverageEndOffset(),
                    false,
                    virtualSegmentBaseOffset,
                    relativeLogicalBytePosition,
                    abortedTransactions);
        }

        KafkaRecordBatch[] decoded = new KafkaRecordBatch[readBatches.size()];
        int totalBytes = 0;
        long previousEnd = -1;
        for (int index = 0; index < readBatches.size(); index++) {
            ReadBatch readBatch = readBatches.get(index);
            if (readBatch.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH) {
                throw new IllegalArgumentException("Fetch source contains a non-Kafka payload");
            }
            KafkaRecordBatch kafkaBatch = codec.decode(readBatch.payload());
            OffsetRange encodedRange = new OffsetRange(kafkaBatch.baseOffset(), kafkaBatch.nextOffset());
            if (!encodedRange.equals(readBatch.range())) {
                throw new IllegalArgumentException("Kafka batch offsets do not match the Nereus ReadBatch range");
            }
            if (index == 0
                    && readResult.view() == ReadView.COMMITTED
                    && !encodedRange.contains(readResult.result().requestedOffset())) {
                throw new IllegalArgumentException("committed Fetch first batch must contain the requested offset");
            }
            if (previousEnd >= 0) {
                boolean invalidOrder = readResult.view() == ReadView.COMMITTED
                        ? encodedRange.startOffset() != previousEnd
                        : encodedRange.startOffset() < previousEnd;
                if (invalidOrder) {
                    throw new IllegalArgumentException("Kafka Fetch batches violate the selected read-view ordering");
                }
            }
            previousEnd = encodedRange.endOffset();
            decoded[index] = kafkaBatch;
            totalBytes = Math.addExact(totalBytes, kafkaBatch.sizeInBytes());
            if (totalBytes > maxResponseBytes) {
                throw new IllegalArgumentException("assembled Kafka Fetch bytes exceed the hard response limit");
            }
        }
        if (readResult.result().nextOffset() != previousEnd) {
            throw new IllegalArgumentException("Kafka Fetch cursor must equal the last returned batch end offset");
        }

        long actualFirstOffset = decoded[0].baseOffset();
        if (virtualSegmentBaseOffset > actualFirstOffset) {
            throw new IllegalArgumentException("virtual segment base cannot exceed the first returned batch offset");
        }
        ByteBuffer output = ByteBuffer.allocate(totalBytes);
        for (KafkaRecordBatch batch : decoded) {
            output.put(batch.encodedBuffer());
        }
        return new KafkaFetchAssembly(
                output.array(),
                OptionalLong.of(actualFirstOffset),
                readResult.result().nextOffset(),
                readResult.sourceCoverageEndOffset(),
                firstEntryOverflow,
                virtualSegmentBaseOffset,
                relativeLogicalBytePosition,
                abortedTransactions);
    }
}
