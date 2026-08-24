/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

/** SDK-neutral strict Kafka magic-v2 RecordBatch framing and native CRC verifier. */
public final class Nwg1StrictNativePayloadVerifierV1 implements Nwg1VerificationContextV1.NativePayloadVerifier {
    private static final int MINIMUM_BATCH_BYTES = 61;

    @Override
    public Nwg1VerificationContextV1.NativeCoverage validateKafka(
            byte[] assigned,
            long partitionId,
            long kafkaLeaderEpoch,
            long expectedCoverageStart,
            long expectedCoverageEnd) {
        if (partitionId < 0
                || partitionId > Integer.MAX_VALUE
                || kafkaLeaderEpoch < 0
                || kafkaLeaderEpoch > Integer.MAX_VALUE) {
            fail(Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION, "Kafka partition/leader epoch");
        }
        if (assigned == null || assigned.length < MINIMUM_BATCH_BYTES) {
            fail(Nwg1RejectionV1.NATIVE_FRAMING_INVALID, "short Kafka RecordBatch");
        }
        int cursor = 0;
        long nextOffset = expectedCoverageStart;
        while (cursor < assigned.length) {
            if (assigned.length - cursor < MINIMUM_BATCH_BYTES) {
                fail(Nwg1RejectionV1.NATIVE_FRAMING_INVALID, "truncated Kafka RecordBatch");
            }
            ByteBuffer batch =
                    ByteBuffer.wrap(assigned, cursor, assigned.length - cursor).order(ByteOrder.BIG_ENDIAN);
            long baseOffset = batch.getLong();
            long batchLength = Integer.toUnsignedLong(batch.getInt());
            long totalLength;
            try {
                totalLength = Math.addExact(12L, batchLength);
            } catch (ArithmeticException e) {
                fail(Nwg1RejectionV1.ARITHMETIC_OVERFLOW, "Kafka batch length");
                throw new AssertionError();
            }
            if (totalLength < MINIMUM_BATCH_BYTES || totalLength > assigned.length - cursor) {
                fail(Nwg1RejectionV1.NATIVE_FRAMING_INVALID, "Kafka batch length");
            }
            int leaderEpoch = batch.getInt();
            int magic = Byte.toUnsignedInt(batch.get());
            int storedCrc = batch.getInt();
            if (magic != 2) {
                fail(Nwg1RejectionV1.NATIVE_FRAMING_INVALID, "Kafka magic must be v2");
            }
            if (leaderEpoch != (int) kafkaLeaderEpoch) {
                fail(Nwg1RejectionV1.COVERAGE_MISMATCH, "Kafka leader epoch mismatch");
            }
            CRC32C crc = new CRC32C();
            crc.update(assigned, cursor + 21, Math.toIntExact(totalLength) - 21);
            if ((int) crc.getValue() != storedCrc) {
                fail(Nwg1RejectionV1.NATIVE_CHECKSUM_MISMATCH, "Kafka native CRC mismatch");
            }
            int lastOffsetDelta = ByteBuffer.wrap(assigned, cursor + 23, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            int recordsCount = ByteBuffer.wrap(assigned, cursor + 57, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            if (baseOffset < 0 || lastOffsetDelta < 0 || recordsCount < 0) {
                fail(Nwg1RejectionV1.NATIVE_FRAMING_INVALID, "Kafka native value domain");
            }
            long batchEnd;
            try {
                batchEnd = Math.addExact(baseOffset, Math.addExact(lastOffsetDelta, 1L));
            } catch (ArithmeticException e) {
                fail(Nwg1RejectionV1.ARITHMETIC_OVERFLOW, "Kafka coverage");
                throw new AssertionError();
            }
            if (baseOffset != nextOffset) {
                fail(
                        baseOffset < nextOffset ? Nwg1RejectionV1.RANGE_OVERLAP : Nwg1RejectionV1.RANGE_GAP,
                        "Kafka batch coverage discontinuity");
            }
            nextOffset = batchEnd;
            cursor = Math.addExact(cursor, Math.toIntExact(totalLength));
        }
        if (cursor != assigned.length || nextOffset != expectedCoverageEnd) {
            fail(Nwg1RejectionV1.COVERAGE_MISMATCH, "Kafka frame coverage");
        }
        return new Nwg1VerificationContextV1.NativeCoverage(expectedCoverageStart, expectedCoverageEnd);
    }

    private static void fail(Nwg1RejectionV1 rejection, String message) {
        throw new Nwg1ValidationException(
                rejection, Nwg1ValidationStageV1.NATIVE_FRAME, Nwg1IsolationScopeV1.APPEND_UNIT, message);
    }
}
