/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Strict bounded primitive reader/writer for the two explicit F3 payload codecs. */
final class F3Binary {
    static final int MAX_PAYLOAD_BYTES = 64 * 1024;

    private F3Binary() {
    }

    static final class Writer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        int size() {
            return out.size();
        }

        byte[] toByteArray() {
            return out.toByteArray();
        }

        void writeByte(int value) {
            out.write(value & 0xff);
        }

        void writeUnsignedShort(int value) {
            if (value < 0 || value > 0xffff) {
                throw new MetadataCodecException("unsigned short is outside its range");
            }
            writeRaw(ByteBuffer.allocate(Short.BYTES).putShort((short) value).array());
        }

        void writeInt(int value) {
            writeRaw(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
        }

        void writeLong(long value) {
            writeRaw(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
        }

        void writeCount(int value) {
            if (value < 0) {
                throw new MetadataCodecException("negative F3 collection count");
            }
            writeInt(value);
        }

        void writeString(String value) {
            byte[] bytes = strictEncode(Objects.requireNonNull(value, "value"));
            writeCount(bytes.length);
            writeRaw(bytes);
        }

        void writeRaw(byte[] bytes) {
            out.writeBytes(Objects.requireNonNull(bytes, "bytes"));
            if (out.size() > MAX_PAYLOAD_BYTES) {
                throw new MetadataCodecException("F3 metadata payload exceeds the fixed value bound");
            }
        }
    }

    static final class Reader {
        private final ByteBuffer buffer;

        Reader(byte[] bytes) {
            Objects.requireNonNull(bytes, "bytes");
            if (bytes.length > MAX_PAYLOAD_BYTES) {
                throw new MetadataCodecException("F3 metadata payload exceeds the fixed value bound");
            }
            buffer = ByteBuffer.wrap(bytes);
        }

        int position() {
            return buffer.position();
        }

        int remaining() {
            return buffer.remaining();
        }

        int readUnsignedByte(String fieldName) {
            requireRemaining(1, fieldName);
            return Byte.toUnsignedInt(buffer.get());
        }

        int readUnsignedShort(String fieldName) {
            requireRemaining(Short.BYTES, fieldName);
            return Short.toUnsignedInt(buffer.getShort());
        }

        int readInt(String fieldName) {
            requireRemaining(Integer.BYTES, fieldName);
            return buffer.getInt();
        }

        long readLong(String fieldName) {
            requireRemaining(Long.BYTES, fieldName);
            return buffer.getLong();
        }

        int readCount(String fieldName, int minimumElementBytes) {
            int count = readInt(fieldName);
            if (count < 0) {
                throw new MetadataCodecException(fieldName + " is outside the supported unsigned range");
            }
            if (minimumElementBytes > 0 && count > buffer.remaining() / minimumElementBytes) {
                throw new MetadataCodecException(fieldName + " exceeds the remaining payload bound");
            }
            return count;
        }

        String readString(String fieldName) {
            int length = readCount(fieldName + " length", 1);
            requireRemaining(length, fieldName);
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return strictDecode(bytes, fieldName);
        }

        void requireConsumed() {
            if (buffer.hasRemaining()) {
                throw new MetadataCodecException("F3 metadata payload has trailing bytes");
            }
        }

        private void requireRemaining(int bytes, String fieldName) {
            if (bytes < 0 || buffer.remaining() < bytes) {
                throw new MetadataCodecException("truncated F3 metadata field: " + fieldName);
            }
        }
    }

    static void writeLongMap(Writer writer, Map<String, Long> values) {
        writer.writeCount(values.size());
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            writer.writeString(entry.getKey());
            writer.writeLong(entry.getValue());
        }
    }

    static Map<String, Long> readLongMap(Reader reader, String fieldName) {
        int count = reader.readCount(fieldName + " count", Integer.BYTES + Long.BYTES);
        LinkedHashMap<String, Long> values = new LinkedHashMap<>();
        String previous = null;
        for (int index = 0; index < count; index++) {
            String key = reader.readString(fieldName + " key");
            requireIncreasingKey(previous, key, fieldName);
            if (values.put(key, reader.readLong(fieldName + " value")) != null) {
                throw new MetadataCodecException(fieldName + " contains a duplicate key");
            }
            previous = key;
        }
        return Collections.unmodifiableMap(values);
    }

    static void writeStringMap(Writer writer, Map<String, String> values) {
        writer.writeCount(values.size());
        for (Map.Entry<String, String> entry : values.entrySet()) {
            writer.writeString(entry.getKey());
            writer.writeString(entry.getValue());
        }
    }

    static Map<String, String> readStringMap(Reader reader, String fieldName) {
        int count = reader.readCount(fieldName + " count", Integer.BYTES * 2);
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        String previous = null;
        for (int index = 0; index < count; index++) {
            String key = reader.readString(fieldName + " key");
            requireIncreasingKey(previous, key, fieldName);
            String value = reader.readString(fieldName + " value");
            if (values.put(key, value) != null) {
                throw new MetadataCodecException(fieldName + " contains a duplicate key");
            }
            previous = key;
        }
        return Collections.unmodifiableMap(values);
    }

    static List<Long> readLongs(Reader reader, String fieldName, int count) {
        if (count < 0 || count > reader.remaining() / Long.BYTES) {
            throw new MetadataCodecException(fieldName + " exceeds the remaining payload bound");
        }
        List<Long> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(reader.readLong(fieldName));
        }
        return List.copyOf(values);
    }

    static MetadataCodecException malformed(String recordType, Throwable cause) {
        if (cause instanceof MetadataCodecException codec) {
            return codec;
        }
        if (cause instanceof BufferUnderflowException || cause instanceof IllegalArgumentException
                || cause instanceof ArithmeticException || cause instanceof NullPointerException) {
            return new MetadataCodecException("invalid " + recordType + " payload", cause);
        }
        return new MetadataCodecException("failed to decode " + recordType + " payload", cause);
    }

    private static void requireIncreasingKey(String previous, String current, String fieldName) {
        if (previous != null && compareUtf8(previous, current) >= 0) {
            throw new MetadataCodecException(fieldName + " keys are not in canonical unsigned UTF-8 order");
        }
    }

    private static int compareUtf8(String left, String right) {
        byte[] leftBytes = strictEncode(left);
        byte[] rightBytes = strictEncode(right);
        int limit = Math.min(leftBytes.length, rightBytes.length);
        for (int index = 0; index < limit; index++) {
            int result = Integer.compare(leftBytes[index] & 0xff, rightBytes[index] & 0xff);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }

    private static byte[] strictEncode(String value) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException e) {
            throw new MetadataCodecException("invalid UTF-16 input for strict UTF-8 encoding", e);
        }
    }

    private static String strictDecode(byte[] bytes, String fieldName) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new MetadataCodecException("invalid UTF-8 in " + fieldName, e);
        }
    }
}
