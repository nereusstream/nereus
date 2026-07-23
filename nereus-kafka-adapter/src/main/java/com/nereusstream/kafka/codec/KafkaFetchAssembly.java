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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** Owned Kafka Fetch bytes plus logical and virtual-position facts required by the fork. */
public final class KafkaFetchAssembly {
    private final byte[] encodedRecords;
    private final OptionalLong actualFirstBatchBaseOffset;
    private final long nextLogicalOffset;
    private final long sourceCoverageEndOffset;
    private final boolean firstEntryOverflow;
    private final long virtualSegmentBaseOffset;
    private final long relativeLogicalBytePosition;
    private final List<KafkaAbortedTransaction> abortedTransactions;

    KafkaFetchAssembly(
            byte[] encodedRecords,
            OptionalLong actualFirstBatchBaseOffset,
            long nextLogicalOffset,
            long sourceCoverageEndOffset,
            boolean firstEntryOverflow,
            long virtualSegmentBaseOffset,
            long relativeLogicalBytePosition,
            List<KafkaAbortedTransaction> abortedTransactions) {
        this.encodedRecords = Objects.requireNonNull(encodedRecords, "encodedRecords").clone();
        this.actualFirstBatchBaseOffset = Objects.requireNonNull(
                actualFirstBatchBaseOffset, "actualFirstBatchBaseOffset");
        this.nextLogicalOffset = nextLogicalOffset;
        this.sourceCoverageEndOffset = sourceCoverageEndOffset;
        this.firstEntryOverflow = firstEntryOverflow;
        this.virtualSegmentBaseOffset = virtualSegmentBaseOffset;
        this.relativeLogicalBytePosition = relativeLogicalBytePosition;
        this.abortedTransactions = List.copyOf(Objects.requireNonNull(abortedTransactions, "abortedTransactions"));
    }

    public byte[] encodedRecords() {
        return encodedRecords.clone();
    }

    /** Returns a new read-only Kafka MemoryRecords-compatible buffer on every call. */
    public ByteBuffer recordsBuffer() {
        return ByteBuffer.wrap(encodedRecords).asReadOnlyBuffer();
    }

    public int sizeInBytes() {
        return encodedRecords.length;
    }

    public OptionalLong actualFirstBatchBaseOffset() {
        return actualFirstBatchBaseOffset;
    }

    public long nextLogicalOffset() {
        return nextLogicalOffset;
    }

    public long sourceCoverageEndOffset() {
        return sourceCoverageEndOffset;
    }

    public boolean firstEntryOverflow() {
        return firstEntryOverflow;
    }

    public long virtualSegmentBaseOffset() {
        return virtualSegmentBaseOffset;
    }

    public long relativeLogicalBytePosition() {
        return relativeLogicalBytePosition;
    }

    public List<KafkaAbortedTransaction> abortedTransactions() {
        return abortedTransactions;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaFetchAssembly that
                && nextLogicalOffset == that.nextLogicalOffset
                && sourceCoverageEndOffset == that.sourceCoverageEndOffset
                && firstEntryOverflow == that.firstEntryOverflow
                && virtualSegmentBaseOffset == that.virtualSegmentBaseOffset
                && relativeLogicalBytePosition == that.relativeLogicalBytePosition
                && Arrays.equals(encodedRecords, that.encodedRecords)
                && actualFirstBatchBaseOffset.equals(that.actualFirstBatchBaseOffset)
                && abortedTransactions.equals(that.abortedTransactions);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                actualFirstBatchBaseOffset,
                nextLogicalOffset,
                sourceCoverageEndOffset,
                firstEntryOverflow,
                virtualSegmentBaseOffset,
                relativeLogicalBytePosition,
                abortedTransactions);
        return 31 * result + Arrays.hashCode(encodedRecords);
    }
}
