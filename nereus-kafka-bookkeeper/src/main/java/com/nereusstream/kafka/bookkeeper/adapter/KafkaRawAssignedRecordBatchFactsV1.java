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

package com.nereusstream.kafka.bookkeeper.adapter;

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Independent parser facts for one stored complete assigned magic-v2 RecordBatch. */
public final class KafkaRawAssignedRecordBatchFactsV1 implements KafkaNativeRecordBatchFactsV1 {
    private static final int LOG_OVERHEAD_BYTES = 12;
    private static final int PARTITION_LEADER_EPOCH_OFFSET = 12;
    private static final int MAGIC_OFFSET = 16;
    private static final int CRC_OFFSET = 17;
    private static final int CRC_DOMAIN_OFFSET = 21;
    private static final int LAST_OFFSET_DELTA_OFFSET = 23;
    private static final int RECORD_BATCH_OVERHEAD_BYTES = 61;

    private final CanonicalBytes raw;
    private final long baseOffset;
    private final int lastOffsetDelta;
    private final int partitionLeaderEpoch;
    private final byte magic;
    private final long storedCrc32c;
    private final long computedCrc32c;

    private KafkaRawAssignedRecordBatchFactsV1(
            CanonicalBytes raw,
            long baseOffset,
            int lastOffsetDelta,
            int partitionLeaderEpoch,
            byte magic,
            long storedCrc32c,
            long computedCrc32c) {
        this.raw = raw;
        this.baseOffset = baseOffset;
        this.lastOffsetDelta = lastOffsetDelta;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.magic = magic;
        this.storedCrc32c = storedCrc32c;
        this.computedCrc32c = computedCrc32c;
    }

    public static KafkaRawAssignedRecordBatchFactsV1 parse(CanonicalBytes raw) {
        Objects.requireNonNull(raw, "raw");
        byte[] bytes = raw.toByteArray();
        if (bytes.length < RECORD_BATCH_OVERHEAD_BYTES) {
            throw new IllegalArgumentException("stored assigned RecordBatch is shorter than the magic-v2 header");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int declaredBatchLength = buffer.getInt(Long.BYTES);
        if (declaredBatchLength < 0 || declaredBatchLength != bytes.length - LOG_OVERHEAD_BYTES) {
            throw new IllegalArgumentException("stored assigned RecordBatch length does not reach strict EOF");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, CRC_DOMAIN_OFFSET, bytes.length - CRC_DOMAIN_OFFSET);
        return new KafkaRawAssignedRecordBatchFactsV1(
                raw,
                buffer.getLong(0),
                buffer.getInt(LAST_OFFSET_DELTA_OFFSET),
                buffer.getInt(PARTITION_LEADER_EPOCH_OFFSET),
                buffer.get(MAGIC_OFFSET),
                Integer.toUnsignedLong(buffer.getInt(CRC_OFFSET)),
                crc.getValue());
    }

    @Override
    public byte[] rawAssignedRecordBatch() {
        return raw.toByteArray();
    }

    @Override
    public int batchCount() {
        return 1;
    }

    @Override
    public int completeBytes() {
        return raw.length();
    }

    @Override
    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public long lastOffset() {
        return Math.addExact(baseOffset, lastOffsetDelta);
    }

    @Override
    public int partitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    @Override
    public byte magic() {
        return magic;
    }

    @Override
    public long storedCrc32c() {
        return storedCrc32c;
    }

    @Override
    public long computedCrc32c() {
        return computedCrc32c;
    }
}
