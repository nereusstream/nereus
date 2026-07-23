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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Strict header, range, producer-fact and CRC validator for byte-exact Kafka magic-v2 batches. */
public final class KafkaRecordBatchCodec {
    public static final byte MAGIC_VALUE_V2 = 2;
    public static final int RECORD_BATCH_OVERHEAD = 61;

    static final int COMPRESSION_CODEC_MASK = 0x07;
    static final int TRANSACTIONAL_FLAG_MASK = 0x10;
    static final int CONTROL_FLAG_MASK = 0x20;

    private static final int LOG_OVERHEAD = 12;
    private static final int LENGTH_OFFSET = 8;
    private static final int PARTITION_LEADER_EPOCH_OFFSET = 12;
    private static final int MAGIC_OFFSET = 16;
    private static final int CRC_OFFSET = 17;
    private static final int ATTRIBUTES_OFFSET = 21;
    private static final int LAST_OFFSET_DELTA_OFFSET = 23;
    private static final int BASE_TIMESTAMP_OFFSET = 27;
    private static final int MAX_TIMESTAMP_OFFSET = 35;
    private static final int PRODUCER_ID_OFFSET = 43;
    private static final int PRODUCER_EPOCH_OFFSET = 51;
    private static final int BASE_SEQUENCE_OFFSET = 53;
    private static final int RECORDS_COUNT_OFFSET = 57;
    private static final int MIN_BATCH_LENGTH = RECORD_BATCH_OVERHEAD - LOG_OVERHEAD;
    private static final long NO_PRODUCER_ID = -1;
    private static final short NO_PRODUCER_EPOCH = -1;
    private static final int NO_SEQUENCE = -1;
    private static final int MAX_COMPRESSION_TYPE_ID = 4;

    /** Decodes one exact batch without changing the supplied buffer's position, limit or byte order. */
    public KafkaRecordBatch decode(ByteBuffer encodedBatch) {
        Objects.requireNonNull(encodedBatch, "encodedBatch");
        List<KafkaRecordBatch> batches = decodeAll(encodedBatch);
        if (batches.size() != 1) {
            throw invalid("expected exactly one Kafka record batch, found %d", batches.size());
        }
        return batches.get(0);
    }

    public KafkaRecordBatch decode(byte[] encodedBatch) {
        Objects.requireNonNull(encodedBatch, "encodedBatch");
        return decode(ByteBuffer.wrap(encodedBatch));
    }

    /** Decodes concatenated batches while preserving each batch as a separate owned byte array. */
    public List<KafkaRecordBatch> decodeAll(ByteBuffer encodedRecords) {
        Objects.requireNonNull(encodedRecords, "encodedRecords");
        ByteBuffer input = encodedRecords.duplicate().order(ByteOrder.BIG_ENDIAN);
        List<KafkaRecordBatch> batches = new ArrayList<>();
        while (input.hasRemaining()) {
            int batchStart = input.position();
            if (input.remaining() < LOG_OVERHEAD) {
                throw invalid("truncated Kafka batch header: %d bytes remain", input.remaining());
            }
            int batchLength = input.getInt(batchStart + LENGTH_OFFSET);
            if (batchLength < MIN_BATCH_LENGTH) {
                throw invalid("Kafka batch length %d is smaller than minimum %d", batchLength, MIN_BATCH_LENGTH);
            }
            long size = (long) LOG_OVERHEAD + batchLength;
            if (size > input.remaining()) {
                throw invalid("Kafka batch declares %d bytes but only %d remain", size, input.remaining());
            }
            int sizeInBytes = Math.toIntExact(size);
            byte[] exactBytes = new byte[sizeInBytes];
            ByteBuffer exactView = input.duplicate();
            exactView.limit(Math.addExact(batchStart, sizeInBytes));
            exactView.get(exactBytes);
            batches.add(decodeOwned(exactBytes));
            input.position(Math.addExact(batchStart, sizeInBytes));
        }
        return List.copyOf(batches);
    }

    private static KafkaRecordBatch decodeOwned(byte[] encoded) {
        ByteBuffer batch = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        byte magic = batch.get(MAGIC_OFFSET);
        if (magic != MAGIC_VALUE_V2) {
            throw invalid("unsupported Kafka record batch magic %d", magic);
        }

        long storedChecksum = Integer.toUnsignedLong(batch.getInt(CRC_OFFSET));
        CRC32C crc32c = new CRC32C();
        crc32c.update(encoded, ATTRIBUTES_OFFSET, encoded.length - ATTRIBUTES_OFFSET);
        if (storedChecksum != crc32c.getValue()) {
            throw invalid(
                    "Kafka record batch CRC mismatch: stored %08x, computed %08x",
                    storedChecksum,
                    crc32c.getValue());
        }

        long baseOffset = batch.getLong(0);
        int lastOffsetDelta = batch.getInt(LAST_OFFSET_DELTA_OFFSET);
        if (baseOffset < 0 || lastOffsetDelta < 0) {
            throw invalid("invalid Kafka offset range: base %d, delta %d", baseOffset, lastOffsetDelta);
        }
        long lastOffset;
        int logicalRecordCount;
        try {
            lastOffset = Math.addExact(baseOffset, lastOffsetDelta);
            logicalRecordCount = Math.toIntExact(Math.addExact((long) lastOffsetDelta, 1));
            Math.addExact(lastOffset, 1);
        } catch (ArithmeticException overflow) {
            throw invalid("Kafka batch offset range overflows");
        }

        short attributes = batch.getShort(ATTRIBUTES_OFFSET);
        int compressionType = attributes & COMPRESSION_CODEC_MASK;
        if (compressionType > MAX_COMPRESSION_TYPE_ID) {
            throw invalid("unknown Kafka compression type id %d", compressionType);
        }
        int recordCount = batch.getInt(RECORDS_COUNT_OFFSET);
        if (recordCount < 0 || recordCount > logicalRecordCount) {
            throw invalid(
                    "Kafka physical record count %d is outside logical span %d", recordCount, logicalRecordCount);
        }

        long baseTimestamp = batch.getLong(BASE_TIMESTAMP_OFFSET);
        long maxTimestamp = batch.getLong(MAX_TIMESTAMP_OFFSET);
        if (baseTimestamp < -1 || maxTimestamp < -1) {
            throw invalid("Kafka timestamps must be -1 or non-negative");
        }

        long producerId = batch.getLong(PRODUCER_ID_OFFSET);
        short producerEpoch = batch.getShort(PRODUCER_EPOCH_OFFSET);
        int baseSequence = batch.getInt(BASE_SEQUENCE_OFFSET);
        boolean transactional = (attributes & TRANSACTIONAL_FLAG_MASK) != 0;
        boolean control = (attributes & CONTROL_FLAG_MASK) != 0;
        validateProducerFacts(producerId, producerEpoch, baseSequence, transactional, control);

        return new KafkaRecordBatch(
                encoded,
                baseOffset,
                lastOffset,
                batch.getInt(PARTITION_LEADER_EPOCH_OFFSET),
                storedChecksum,
                attributes,
                baseTimestamp,
                maxTimestamp,
                producerId,
                producerEpoch,
                baseSequence,
                recordCount);
    }

    private static void validateProducerFacts(
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean transactional,
            boolean control) {
        if (producerId < NO_PRODUCER_ID) {
            throw invalid("Kafka producer id must be -1 or non-negative");
        }
        if (producerId == NO_PRODUCER_ID) {
            if (producerEpoch != NO_PRODUCER_EPOCH || baseSequence != NO_SEQUENCE) {
                throw invalid("Kafka batch without producer id must use producer epoch and sequence -1");
            }
            if (transactional || control) {
                throw invalid("transactional/control Kafka batch requires a producer id");
            }
        } else if (producerEpoch < 0
                || baseSequence < NO_SEQUENCE
                || (!control && baseSequence == NO_SEQUENCE)) {
            throw invalid("Kafka batch with producer id has invalid producer epoch or sequence");
        }
    }

    private static IllegalArgumentException invalid(String format, Object... arguments) {
        return new IllegalArgumentException(String.format(Locale.ROOT, format, arguments));
    }
}
