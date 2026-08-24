/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import com.github.luben.zstd.Zstd;

/** Bounded NWG1 Zstandard standard-frame support. Exact compressor output is not wire authority. */
public final class Nwg1ZstdV1 {
    private static final int STANDARD_MAGIC_LE = 0xfd2fb528;
    private static final int SKIPPABLE_MASK_LE = 0xfffffff0;
    private static final int SKIPPABLE_MAGIC_LE = 0x184d2a50;

    private Nwg1ZstdV1() {}

    public record EncodingResult(int codecKind, int codecVersion, byte[] preAeadBytes) {
        public EncodingResult {
            preAeadBytes = preAeadBytes.clone();
        }

        @Override
        public byte[] preAeadBytes() {
            return preAeadBytes.clone();
        }
    }

    /** Selects ZSTD only when it is strictly smaller; otherwise returns exact NONE bytes and codes. */
    public static EncodingResult encodeIfSmaller(byte[] decoded) {
        if (decoded.length == 0 || decoded.length > Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES) {
            throw new IllegalArgumentException("ZSTD decoded length");
        }
        byte[] candidate = Zstd.compress(decoded);
        validateStandardFrame(candidate, decoded.length);
        return candidate.length < decoded.length
                ? new EncodingResult(Nwg1ConstantsV1.CODEC_ZSTD_KIND, Nwg1ConstantsV1.CODEC_ZSTD_VERSION, candidate)
                : new EncodingResult(Nwg1ConstantsV1.CODEC_NONE_KIND, Nwg1ConstantsV1.CODEC_NONE_VERSION, decoded);
    }

    public static byte[] decompress(byte[] frame, int decodedLength) {
        validateStandardFrame(frame, decodedLength);
        byte[] decoded = Zstd.decompress(frame, decodedLength);
        if (Zstd.isError(decoded.length) || decoded.length != decodedLength) {
            fail("ZSTD decoded length mismatch");
        }
        return decoded;
    }

    /** Rejects dictionaries, skippable frames, missing/wrong content size, trailing or concatenated bytes. */
    public static void validateStandardFrame(byte[] frame, int expectedDecodedLength) {
        if (frame == null
                || frame.length < 6
                || expectedDecodedLength <= 0
                || expectedDecodedLength > Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES) {
            fail("invalid ZSTD bounds");
        }
        int magic = littleEndianInt(frame, 0);
        if ((magic & SKIPPABLE_MASK_LE) == SKIPPABLE_MAGIC_LE || magic != STANDARD_MAGIC_LE) {
            fail("not one standard ZSTD frame");
        }
        FrameFacts facts = parseFrame(frame);
        if (facts.frameBytes() != frame.length) {
            fail("ZSTD trailing or concatenated bytes");
        }
        if (facts.contentSize() != expectedDecodedLength) {
            fail("ZSTD content size absent or mismatched");
        }
        if (facts.windowSize() > Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES) {
            fail("ZSTD window cap exceeded");
        }
        if (Zstd.getDictIdFromFrame(frame) != 0) {
            fail("ZSTD dictionary is forbidden");
        }
        long libraryContentSize = Zstd.getFrameContentSize(frame);
        if (libraryContentSize != expectedDecodedLength) {
            fail("ZSTD library content-size mismatch");
        }
        byte[] decoded;
        try {
            decoded = Zstd.decompress(frame, expectedDecodedLength);
        } catch (RuntimeException e) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.CODEC_CONTRACT_VIOLATION,
                    Nwg1ValidationStageV1.FRAME_CODEC,
                    Nwg1IsolationScopeV1.APPEND_UNIT,
                    "invalid ZSTD standard frame",
                    e);
        }
        if (decoded.length != expectedDecodedLength) {
            fail("ZSTD decoded size mismatch");
        }
    }

    private static FrameFacts parseFrame(byte[] frame) {
        int cursor = 4;
        int descriptor = unsigned(frame, cursor++);
        int contentSizeFlag = descriptor >>> 6;
        boolean singleSegment = (descriptor & 0x20) != 0;
        if ((descriptor & 0x18) != 0) {
            fail("ZSTD reserved/unused descriptor bit");
        }
        boolean checksum = (descriptor & 0x04) != 0;
        int dictionaryFlag = descriptor & 0x03;
        if (dictionaryFlag != 0) {
            fail("ZSTD dictionary ID is forbidden");
        }

        long windowSize = -1;
        if (!singleSegment) {
            require(frame, cursor, 1);
            int windowDescriptor = unsigned(frame, cursor++);
            int exponent = windowDescriptor >>> 3;
            int mantissa = windowDescriptor & 7;
            int windowLog = 10 + exponent;
            if (windowLog >= 63) {
                fail("ZSTD window arithmetic overflow");
            }
            long windowBase = 1L << windowLog;
            windowSize = Math.addExact(windowBase, (windowBase >>> 3) * mantissa);
        }

        int contentSizeBytes =
                switch (contentSizeFlag) {
                    case 0 -> singleSegment ? 1 : 0;
                    case 1 -> 2;
                    case 2 -> 4;
                    case 3 -> 8;
                    default -> throw new AssertionError();
                };
        if (contentSizeBytes == 0) {
            fail("ZSTD content size must be present");
        }
        require(frame, cursor, contentSizeBytes);
        long contentSize = littleEndianUnsigned(frame, cursor, contentSizeBytes);
        cursor += contentSizeBytes;
        if (contentSizeBytes == 2) {
            contentSize = Math.addExact(contentSize, 256);
        }
        if (singleSegment) {
            windowSize = contentSize;
        }

        boolean last;
        do {
            require(frame, cursor, 3);
            int blockHeader =
                    unsigned(frame, cursor) | (unsigned(frame, cursor + 1) << 8) | (unsigned(frame, cursor + 2) << 16);
            cursor += 3;
            last = (blockHeader & 1) != 0;
            int blockType = (blockHeader >>> 1) & 3;
            int blockSize = blockHeader >>> 3;
            if (blockType == 3) {
                fail("ZSTD reserved block type");
            }
            int storedBytes = blockType == 1 ? 1 : blockSize;
            require(frame, cursor, storedBytes);
            cursor = Math.addExact(cursor, storedBytes);
        } while (!last);
        if (checksum) {
            require(frame, cursor, 4);
            cursor += 4;
        }
        return new FrameFacts(cursor, contentSize, windowSize);
    }

    private static long littleEndianUnsigned(byte[] value, int offset, int bytes) {
        if (bytes == 8 && (value[offset + 7] & 0x80) != 0) {
            fail("ZSTD content size outside signed-long domain");
        }
        long result = 0;
        for (int i = 0; i < bytes; i++) {
            result |= (long) unsigned(value, offset + i) << (8 * i);
        }
        return result;
    }

    private static void require(byte[] value, int offset, int bytes) {
        if (offset < 0 || bytes < 0 || offset > value.length - bytes) {
            fail("truncated ZSTD frame");
        }
    }

    private static int unsigned(byte[] value, int offset) {
        return value[offset] & 0xff;
    }

    private record FrameFacts(int frameBytes, long contentSize, long windowSize) {}

    private static int littleEndianInt(byte[] value, int offset) {
        return (value[offset] & 0xff)
                | ((value[offset + 1] & 0xff) << 8)
                | ((value[offset + 2] & 0xff) << 16)
                | ((value[offset + 3] & 0xff) << 24);
    }

    private static void fail(String message) {
        throw new Nwg1ValidationException(
                Nwg1RejectionV1.CODEC_CONTRACT_VIOLATION,
                Nwg1ValidationStageV1.FRAME_CODEC,
                Nwg1IsolationScopeV1.APPEND_UNIT,
                message);
    }
}
