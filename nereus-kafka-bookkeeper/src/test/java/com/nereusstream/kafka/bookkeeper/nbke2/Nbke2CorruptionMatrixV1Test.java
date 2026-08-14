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

package com.nereusstream.kafka.bookkeeper.nbke2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

class Nbke2CorruptionMatrixV1Test {
    private static final int BODY_OFFSET = Nbke2ConstantsV1.FIXED_HEADER_BYTES + 146 + "orders".length();

    @Test
    void rejectsEveryCommonHeaderAndIntegrityCutAtItsExactLayer() {
        byte[] header = encode(0, Nbke2TestFrames.runHeader());

        assertRejected(mutate(header, 0, 0), 0, Nbke2RejectionV1.BAD_MAGIC);
        assertRejected(mutate(header, 5, 2), 0, Nbke2RejectionV1.UNKNOWN_MAJOR);
        assertRejected(mutate(header, 6, 1), 0, Nbke2RejectionV1.UNKNOWN_MINOR);
        assertRejected(mutate(header, 7, 99), 0, Nbke2RejectionV1.UNKNOWN_FRAME_TYPE);
        assertRejected(mutate(header, 8, 1), 0, Nbke2RejectionV1.UNKNOWN_FLAGS);
        assertRejected(mutate(header, 9, 1), 0, Nbke2RejectionV1.RESERVED_NON_ZERO);
        assertRejected(mutate(header, 11, 31), 0, Nbke2RejectionV1.HEADER_LENGTH_MISMATCH);
        assertRejected(mutate(header, 15, header.length - 1), 0, Nbke2RejectionV1.TOTAL_LENGTH_INVALID);
        assertRejected(header, 0, Nbke2RejectionV1.LEDGER_ID_MISMATCH, Nbke2TestFrames.LEDGER_ID + 1);
        assertRejected(header, 0, Nbke2RejectionV1.ENTRY_ID_MISMATCH, Nbke2TestFrames.LEDGER_ID, 1);

        byte[] crcCorruption = header.clone();
        crcCorruption[BODY_OFFSET] ^= 1;
        assertRejected(crcCorruption, 0, Nbke2RejectionV1.CRC32C_MISMATCH);

        assertThatThrownBy(() -> Nbke2CodecV1.decode(new byte[8], Nbke2TestFrames.LEDGER_ID, 0))
                .isInstanceOfSatisfying(Nbke2DecodingException.class, rejection -> assertThat(rejection.rejection())
                        .isEqualTo(Nbke2RejectionV1.TRUNCATED));
    }

    @Test
    void rejectsDataLengthCountOrdinalOverflowAndDescriptorCorruption() {
        byte[] data = encode(1, Nbke2TestFrames.data());

        byte[] oversizedLength = data.clone();
        putInt(oversizedLength, BODY_OFFSET + 12, Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES + 1);
        rewriteCrc(oversizedLength);
        assertRejected(oversizedLength, 1, Nbke2RejectionV1.LENGTH_LIMIT_EXCEEDED);

        byte[] zeroCount = data.clone();
        putInt(zeroCount, BODY_OFFSET + 20, 0);
        rewriteCrc(zeroCount);
        assertRejected(zeroCount, 1, Nbke2RejectionV1.FIELD_OUT_OF_DOMAIN);

        byte[] badOrdinal = data.clone();
        putInt(badOrdinal, BODY_OFFSET + 16, 1);
        rewriteCrc(badOrdinal);
        assertRejected(badOrdinal, 1, Nbke2RejectionV1.FIELD_OUT_OF_DOMAIN);

        byte[] offsetOverflow = data.clone();
        putLong(offsetOverflow, BODY_OFFSET, Long.MAX_VALUE);
        putInt(offsetOverflow, BODY_OFFSET + 8, 0);
        rewriteCrc(offsetOverflow);
        assertRejected(offsetOverflow, 1, Nbke2RejectionV1.ARITHMETIC_OVERFLOW);

        byte[] missingTerminalFlag = data.clone();
        missingTerminalFlag[8] = 0;
        rewriteCrc(missingTerminalFlag);
        assertRejected(missingTerminalFlag, 1, Nbke2RejectionV1.TOTAL_LENGTH_INVALID);

        byte[] trailing = appendTrailingByte(encode(0, Nbke2TestFrames.runHeader()));
        assertRejected(trailing, 0, Nbke2RejectionV1.TRAILING_BYTES);
    }

    @Test
    void rejectsControlShaCountLengthAndOrderingCorruption() {
        byte[] range = encode(3, Nbke2TestFrames.rangeIndexBlock());
        byte[] rangeSha = range.clone();
        rangeSha[BODY_OFFSET] ^= 1;
        rewriteCrc(rangeSha);
        assertRejected(rangeSha, 3, Nbke2RejectionV1.SHA256_MISMATCH);

        byte[] tooManyLocators = range.clone();
        putInt(tooManyLocators, BODY_OFFSET + 56, Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT + 1);
        rewriteCrc(tooManyLocators);
        assertRejected(tooManyLocators, 3, Nbke2RejectionV1.COUNT_LIMIT_EXCEEDED);

        byte[] unordered = range.clone();
        int secondLocator = BODY_OFFSET + 60 + Nbke2ConstantsV1.LOCATOR_BYTES;
        putLong(unordered, secondLocator, 4);
        rewriteControlShaAndCrc(unordered);
        assertRejected(unordered, 3, Nbke2RejectionV1.ORDERING_VIOLATION);

        byte[] checkpoint = encode(3, Nbke2TestFrames.protocolCheckpoint());
        putInt(checkpoint, BODY_OFFSET + 32, Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES + 1);
        rewriteCrc(checkpoint);
        assertRejected(checkpoint, 3, Nbke2RejectionV1.LENGTH_LIMIT_EXCEEDED);

        byte[] footer = encode(5, Nbke2TestFrames.runFooter());
        putInt(footer, BODY_OFFSET + 40, Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT + 1);
        rewriteCrc(footer);
        assertRejected(footer, 5, Nbke2RejectionV1.COUNT_LIMIT_EXCEEDED);
    }

    @Test
    void rejectsFramesAboveThePersistedTotalLengthCapBeforeAllocation() {
        byte[] oversized = new byte[Nbke2ConstantsV1.FORMAT_MAX_FRAME_BYTES + 1];

        assertThatThrownBy(() -> Nbke2CodecV1.decode(oversized, 0, 0))
                .isInstanceOfSatisfying(Nbke2DecodingException.class, rejection -> assertThat(rejection.rejection())
                        .isEqualTo(Nbke2RejectionV1.LENGTH_LIMIT_EXCEEDED));
    }

    private static byte[] encode(long entryId, Nbke2FrameV1 frame) {
        return Nbke2CodecV1.encode(Nbke2TestFrames.LEDGER_ID, entryId, frame);
    }

    private static byte[] mutate(byte[] source, int index, int unsignedValue) {
        byte[] copy = source.clone();
        copy[index] = (byte) unsignedValue;
        return copy;
    }

    private static byte[] appendTrailingByte(byte[] source) {
        byte[] result = Arrays.copyOf(source, source.length + 1);
        int oldCrcPosition = source.length - Nbke2ConstantsV1.CRC32C_BYTES;
        result[oldCrcPosition] = 0x55;
        putInt(result, 12, result.length);
        rewriteCrc(result);
        return result;
    }

    private static void rewriteControlShaAndCrc(byte[] frame) {
        int digestPosition = frame.length - Nbke2ConstantsV1.CRC32C_BYTES - Nbke2ConstantsV1.SHA256_BYTES;
        byte[] digest = com.nereusstream.domain.bytes.Sha256Digest.hash(
                        com.nereusstream.domain.bytes.CanonicalBytes.copyOf(Arrays.copyOf(frame, digestPosition)))
                .bytes()
                .toByteArray();
        System.arraycopy(digest, 0, frame, digestPosition, digest.length);
        rewriteCrc(frame);
    }

    private static void rewriteCrc(byte[] frame) {
        CRC32C crc = new CRC32C();
        crc.update(frame, 0, frame.length - Nbke2ConstantsV1.CRC32C_BYTES);
        putInt(frame, frame.length - Nbke2ConstantsV1.CRC32C_BYTES, (int) crc.getValue());
    }

    private static void putInt(byte[] target, int offset, int value) {
        ByteBuffer.wrap(target).order(ByteOrder.BIG_ENDIAN).putInt(offset, value);
    }

    private static void putLong(byte[] target, int offset, long value) {
        ByteBuffer.wrap(target).order(ByteOrder.BIG_ENDIAN).putLong(offset, value);
    }

    private static void assertRejected(byte[] bytes, long entryId, Nbke2RejectionV1 expected) {
        assertRejected(bytes, entryId, expected, Nbke2TestFrames.LEDGER_ID, entryId);
    }

    private static void assertRejected(byte[] bytes, long entryId, Nbke2RejectionV1 expected, long expectedLedgerId) {
        assertRejected(bytes, entryId, expected, expectedLedgerId, entryId);
    }

    private static void assertRejected(
            byte[] bytes, long entryId, Nbke2RejectionV1 expected, long expectedLedgerId, long expectedEntryId) {
        assertThatThrownBy(() -> Nbke2CodecV1.decode(bytes, expectedLedgerId, expectedEntryId))
                .isInstanceOfSatisfying(Nbke2DecodingException.class, rejection -> assertThat(rejection.rejection())
                        .isEqualTo(expected));
    }
}
