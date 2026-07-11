/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.api.ApiLimits;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

final class CanonicalTargetBinary {
    private CanonicalTargetBinary() { }

    static final class Writer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        void magic(String magic) { bytes(magic.getBytes(StandardCharsets.US_ASCII)); }
        void string(String value) { lengthBytes(StrictUtf8.encode(value)); }
        void byteArray(byte[] value) { lengthBytes(value); }
        void longValue(long value) { bytes(ByteBuffer.allocate(Long.BYTES).putLong(value).array()); }
        void intValue(int value) { bytes(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()); }
        byte[] finish() {
            byte[] result = out.toByteArray();
            if (result.length > ApiLimits.MAX_READ_TARGET_ENCODED_BYTES) {
                throw new MetadataCodecException("canonical target payload is too large");
            }
            return result;
        }
        private void lengthBytes(byte[] value) { intValue(value.length); bytes(value); }
        private void bytes(byte[] value) { out.writeBytes(value); }
    }

    static final class Reader {
        private final ByteBuffer buffer;
        Reader(byte[] bytes) {
            if (bytes.length == 0 || bytes.length > ApiLimits.MAX_READ_TARGET_ENCODED_BYTES) {
                throw new MetadataCodecException("invalid canonical target payload length");
            }
            buffer = ByteBuffer.wrap(bytes);
        }
        void magic(String expected) {
            byte[] value = new byte[expected.length()];
            take(value);
            if (!expected.equals(new String(value, StandardCharsets.US_ASCII))) {
                throw new MetadataCodecException("invalid canonical target magic");
            }
        }
        String string(String name) { return StrictUtf8.decode(byteArray(), name); }
        byte[] byteArray() {
            int length = intValue();
            if (length < 0 || length > buffer.remaining()) throw new MetadataCodecException("invalid target field length");
            byte[] value = new byte[length]; take(value); return value;
        }
        long longValue() { require(Long.BYTES); return buffer.getLong(); }
        int intValue() { require(Integer.BYTES); return buffer.getInt(); }
        void finish() { if (buffer.hasRemaining()) throw new MetadataCodecException("canonical target has trailing bytes"); }
        private void take(byte[] value) { require(value.length); buffer.get(value); }
        private void require(int length) { if (length < 0 || buffer.remaining() < length) throw new MetadataCodecException("truncated canonical target"); }
    }
}
