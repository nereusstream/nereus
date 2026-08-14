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

/** One complete broker-assigned magic-v2 RecordBatch after native and independent K2 cross-checks. */
public final class KafkaNativeAssignedRecordBatchV1 {
    static final int LOG_OVERHEAD_BYTES = 12;
    static final int PARTITION_LEADER_EPOCH_OFFSET = 12;
    static final int MAGIC_OFFSET = 16;
    static final int CRC_OFFSET = 17;
    static final int CRC_DOMAIN_OFFSET = 21;
    static final int LAST_OFFSET_DELTA_OFFSET = 23;
    static final int RECORD_BATCH_OVERHEAD_BYTES = 61;
    static final int MAGIC_V2 = 2;
    private static final long MAX_UNSIGNED_INT = 0xffff_ffffL;
    private final CanonicalBytes rawAssignedRecordBatch;
    private final long baseOffset;
    private final int lastOffsetDelta;
    private final int partitionLeaderEpoch;
    private final long storedCrc32c;

    private KafkaNativeAssignedRecordBatchV1(
            CanonicalBytes rawAssignedRecordBatch,
            long baseOffset,
            int lastOffsetDelta,
            int partitionLeaderEpoch,
            long storedCrc32c) {
        this.rawAssignedRecordBatch = Objects.requireNonNull(rawAssignedRecordBatch, "rawAssignedRecordBatch");
        this.baseOffset = baseOffset;
        this.lastOffsetDelta = lastOffsetDelta;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.storedCrc32c = storedCrc32c;
        if (baseOffset < 0 || lastOffsetDelta < 0 || partitionLeaderEpoch < 0) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.INVALID_OFFSET_COVERAGE,
                    "assigned RecordBatch offsets and leader epoch must be non-negative");
        }
        if (storedCrc32c < 0 || storedCrc32c > MAX_UNSIGNED_INT) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.CRC_MISMATCH,
                    "assigned RecordBatch CRC32C is outside the unsigned-int domain");
        }
        try {
            Math.addExact(baseOffset, (long) lastOffsetDelta + 1L);
        } catch (ArithmeticException failure) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.INVALID_OFFSET_COVERAGE,
                    "assigned RecordBatch coverage overflows",
                    failure);
        }
    }

    public static KafkaNativeAssignedRecordBatchV1 validate(KafkaNativeRecordBatchFactsV1 nativeFacts) {
        Objects.requireNonNull(nativeFacts, "nativeFacts");
        byte[] raw = Objects.requireNonNull(nativeFacts.rawAssignedRecordBatch(), "rawAssignedRecordBatch")
                .clone();
        if (nativeFacts.batchCount() != 1) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.BATCH_COUNT_MISMATCH,
                    "K2 requires exactly one complete assigned RecordBatch");
        }
        if (raw.length <= MAGIC_OFFSET) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.TRUNCATED_BATCH,
                    "assigned RecordBatch is truncated before the Kafka magic field");
        }
        if (nativeFacts.completeBytes() != raw.length) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.NATIVE_FACT_MISMATCH,
                    "native complete-byte count differs from the assigned bytes");
        }

        byte headerMagic = raw[MAGIC_OFFSET];
        if (headerMagic != MAGIC_V2 || nativeFacts.magic() != MAGIC_V2) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.UNSUPPORTED_MAGIC,
                    "K2 accepts only assigned Kafka magic-v2 RecordBatch bytes");
        }
        if (headerMagic != nativeFacts.magic()) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.NATIVE_FACT_MISMATCH,
                    "native magic differs from the assigned header");
        }
        if (raw.length < RECORD_BATCH_OVERHEAD_BYTES) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.TRUNCATED_BATCH,
                    "assigned RecordBatch is shorter than the magic-v2 overhead");
        }

        ByteBuffer header = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        int declaredBodyBytes = header.getInt(Long.BYTES);
        long declaredTotalBytes = (long) declaredBodyBytes + LOG_OVERHEAD_BYTES;
        if (declaredBodyBytes < 0 || declaredTotalBytes != raw.length) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.DECLARED_LENGTH_MISMATCH,
                    "Kafka batch length does not cover exactly the supplied bytes");
        }

        long headerBaseOffset = header.getLong(0);
        int headerLastOffsetDelta = header.getInt(LAST_OFFSET_DELTA_OFFSET);
        long headerEndOffset;
        try {
            headerEndOffset = Math.addExact(headerBaseOffset, Math.addExact((long) headerLastOffsetDelta, 1L));
        } catch (ArithmeticException failure) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.INVALID_OFFSET_COVERAGE,
                    "assigned RecordBatch header coverage overflows",
                    failure);
        }
        if (headerBaseOffset < 0 || headerLastOffsetDelta < 0 || headerEndOffset <= headerBaseOffset) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.INVALID_OFFSET_COVERAGE,
                    "assigned RecordBatch header coverage is outside the K2 domain");
        }
        if (nativeFacts.baseOffset() != headerBaseOffset || nativeFacts.lastOffset() != headerEndOffset - 1L) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.NATIVE_FACT_MISMATCH,
                    "native offset facts differ from the assigned header");
        }

        int headerLeaderEpoch = header.getInt(PARTITION_LEADER_EPOCH_OFFSET);
        if (headerLeaderEpoch < 0 || nativeFacts.partitionLeaderEpoch() != headerLeaderEpoch) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.LEADER_EPOCH_MISMATCH,
                    "native leader epoch differs from the assigned header or is unassigned");
        }

        long headerStoredCrc = Integer.toUnsignedLong(header.getInt(CRC_OFFSET));
        long computedCrc = crc32c(raw);
        if (nativeFacts.storedCrc32c() != headerStoredCrc || nativeFacts.computedCrc32c() != computedCrc) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.NATIVE_FACT_MISMATCH,
                    "native CRC facts differ from the exact assigned bytes");
        }
        if (headerStoredCrc != computedCrc) {
            throw reject(KafkaAssignedRecordBatchRejectionV1.CRC_MISMATCH, "assigned RecordBatch CRC32C mismatch");
        }

        return new KafkaNativeAssignedRecordBatchV1(
                CanonicalBytes.copyOf(raw),
                headerBaseOffset,
                headerLastOffsetDelta,
                headerLeaderEpoch,
                headerStoredCrc);
    }

    public long endOffsetExclusive() {
        return Math.addExact(baseOffset, (long) lastOffsetDelta + 1L);
    }

    public CanonicalBytes rawAssignedRecordBatch() {
        return rawAssignedRecordBatch;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public int lastOffsetDelta() {
        return lastOffsetDelta;
    }

    public int partitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    public long storedCrc32c() {
        return storedCrc32c;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof KafkaNativeAssignedRecordBatchV1 that)) {
            return false;
        }
        return baseOffset == that.baseOffset
                && lastOffsetDelta == that.lastOffsetDelta
                && partitionLeaderEpoch == that.partitionLeaderEpoch
                && storedCrc32c == that.storedCrc32c
                && rawAssignedRecordBatch.equals(that.rawAssignedRecordBatch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawAssignedRecordBatch, baseOffset, lastOffsetDelta, partitionLeaderEpoch, storedCrc32c);
    }

    @Override
    public String toString() {
        return "KafkaNativeAssignedRecordBatchV1[rawAssignedRecordBatch="
                + rawAssignedRecordBatch
                + ", baseOffset="
                + baseOffset
                + ", lastOffsetDelta="
                + lastOffsetDelta
                + ", partitionLeaderEpoch="
                + partitionLeaderEpoch
                + ", storedCrc32c="
                + storedCrc32c
                + ']';
    }

    private static long crc32c(byte[] raw) {
        CRC32C crc = new CRC32C();
        crc.update(raw, CRC_DOMAIN_OFFSET, raw.length - CRC_DOMAIN_OFFSET);
        return crc.getValue();
    }

    private static KafkaAssignedRecordBatchException reject(
            KafkaAssignedRecordBatchRejectionV1 rejection, String message) {
        return new KafkaAssignedRecordBatchException(rejection, message);
    }

    private static KafkaAssignedRecordBatchException reject(
            KafkaAssignedRecordBatchRejectionV1 rejection, String message, Throwable cause) {
        return new KafkaAssignedRecordBatchException(rejection, message, cause);
    }
}
