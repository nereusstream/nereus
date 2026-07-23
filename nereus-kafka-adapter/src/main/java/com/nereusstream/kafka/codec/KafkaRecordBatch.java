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
import java.util.Objects;

/** One validated, byte-exact Kafka magic-v2 record batch and its durable offset facts. */
public final class KafkaRecordBatch {
    private final byte[] encodedBytes;
    private final long baseOffset;
    private final long lastOffset;
    private final int partitionLeaderEpoch;
    private final long checksum;
    private final short attributes;
    private final long baseTimestamp;
    private final long maxTimestamp;
    private final long producerId;
    private final short producerEpoch;
    private final int baseSequence;
    private final int recordCount;

    KafkaRecordBatch(
            byte[] encodedBytes,
            long baseOffset,
            long lastOffset,
            int partitionLeaderEpoch,
            long checksum,
            short attributes,
            long baseTimestamp,
            long maxTimestamp,
            long producerId,
            short producerEpoch,
            int baseSequence,
            int recordCount) {
        this.encodedBytes = Objects.requireNonNull(encodedBytes, "encodedBytes").clone();
        this.baseOffset = baseOffset;
        this.lastOffset = lastOffset;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.checksum = checksum;
        this.attributes = attributes;
        this.baseTimestamp = baseTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.recordCount = recordCount;
    }

    public byte[] encodedBytes() {
        return encodedBytes.clone();
    }

    /** Returns an owned, read-only view whose position is zero and whose limit is the exact batch size. */
    public ByteBuffer encodedBuffer() {
        return ByteBuffer.wrap(encodedBytes).asReadOnlyBuffer();
    }

    public int sizeInBytes() {
        return encodedBytes.length;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public long nextOffset() {
        return Math.addExact(lastOffset, 1);
    }

    /** Logical offset span. This can exceed {@link #recordCount()} after Kafka compaction. */
    public int logicalRecordCount() {
        return Math.toIntExact(Math.addExact(Math.subtractExact(lastOffset, baseOffset), 1));
    }

    public int partitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    public long checksum() {
        return checksum;
    }

    public short attributes() {
        return attributes;
    }

    public int compressionTypeId() {
        return attributes & KafkaRecordBatchCodec.COMPRESSION_CODEC_MASK;
    }

    public boolean transactional() {
        return (attributes & KafkaRecordBatchCodec.TRANSACTIONAL_FLAG_MASK) != 0;
    }

    public boolean controlBatch() {
        return (attributes & KafkaRecordBatchCodec.CONTROL_FLAG_MASK) != 0;
    }

    public long baseTimestamp() {
        return baseTimestamp;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public int baseSequence() {
        return baseSequence;
    }

    /** Number of physical records declared by the Kafka batch header. */
    public int recordCount() {
        return recordCount;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaRecordBatch that
                && baseOffset == that.baseOffset
                && lastOffset == that.lastOffset
                && partitionLeaderEpoch == that.partitionLeaderEpoch
                && checksum == that.checksum
                && attributes == that.attributes
                && baseTimestamp == that.baseTimestamp
                && maxTimestamp == that.maxTimestamp
                && producerId == that.producerId
                && producerEpoch == that.producerEpoch
                && baseSequence == that.baseSequence
                && recordCount == that.recordCount
                && Arrays.equals(encodedBytes, that.encodedBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                baseOffset, lastOffset, partitionLeaderEpoch, checksum, attributes, baseTimestamp, maxTimestamp,
                producerId, producerEpoch, baseSequence, recordCount);
        return 31 * result + Arrays.hashCode(encodedBytes);
    }
}
