/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.api.SchemaRef;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Strict, bounded primitive codec for Phase 4 metadata payloads. */
final class F4Binary {
    static final int MAX_PAYLOAD_BYTES = 64 * 1024;

    private F4Binary() {
    }

    static final class Writer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

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

        void writeString(String value) {
            byte[] bytes = strictEncode(Objects.requireNonNull(value, "value"));
            writeBytes(bytes);
        }

        void writeBytes(byte[] value) {
            Objects.requireNonNull(value, "value");
            writeInt(value.length);
            writeRaw(value);
        }

        void writeOptional(boolean present) {
            writeByte(present ? 1 : 0);
        }

        void writeRaw(byte[] value) {
            out.writeBytes(value);
            if (out.size() > MAX_PAYLOAD_BYTES) {
                throw new MetadataValueTooLargeException("F4 metadata payload exceeds 64 KiB");
            }
        }
    }

    static final class Reader {
        private final ByteBuffer buffer;

        Reader(byte[] value) {
            Objects.requireNonNull(value, "value");
            if (value.length > MAX_PAYLOAD_BYTES) {
                throw new MetadataValueTooLargeException("F4 metadata payload exceeds 64 KiB");
            }
            this.buffer = ByteBuffer.wrap(value);
        }

        int readUnsignedByte(String name) {
            requireRemaining(1, name);
            return Byte.toUnsignedInt(buffer.get());
        }

        int readUnsignedShort(String name) {
            requireRemaining(Short.BYTES, name);
            return Short.toUnsignedInt(buffer.getShort());
        }

        int readInt(String name) {
            requireRemaining(Integer.BYTES, name);
            return buffer.getInt();
        }

        long readLong(String name) {
            requireRemaining(Long.BYTES, name);
            return buffer.getLong();
        }

        int readCount(String name, int minimumElementBytes, int hardMaximum) {
            int count = readInt(name);
            if (count < 0 || count > hardMaximum
                    || (minimumElementBytes > 0 && count > buffer.remaining() / minimumElementBytes)) {
                throw new MetadataCodecException(name + " exceeds its supported bound");
            }
            return count;
        }

        String readString(String name) {
            return strictDecode(readBytes(name), name);
        }

        byte[] readBytes(String name) {
            int length = readCount(name + "Length", 1, MAX_PAYLOAD_BYTES);
            requireRemaining(length, name);
            byte[] value = new byte[length];
            buffer.get(value);
            return value;
        }

        boolean readOptional(String name) {
            return switch (readUnsignedByte(name)) {
                case 0 -> false;
                case 1 -> true;
                default -> throw new MetadataCodecException(name + " must be encoded as 0 or 1");
            };
        }

        void requireConsumed() {
            if (buffer.hasRemaining()) {
                throw new MetadataCodecException("F4 metadata payload has trailing bytes");
            }
        }

        private void requireRemaining(int count, String name) {
            if (count < 0 || buffer.remaining() < count) {
                throw new MetadataCodecException("truncated F4 metadata field: " + name);
            }
        }
    }

    static void writeSchemaRefs(Writer writer, List<SchemaRef> values) {
        writer.writeInt(values.size());
        for (SchemaRef value : values) {
            writer.writeString(value.namespace());
            writer.writeString(value.id());
            writer.writeLong(value.version());
        }
    }

    static List<SchemaRef> readSchemaRefs(Reader reader, String name) {
        int count = reader.readCount(name + "Count", Integer.BYTES * 2 + Long.BYTES, 4_096);
        List<SchemaRef> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(new SchemaRef(
                    reader.readString(name + "Namespace"),
                    reader.readString(name + "Id"),
                    reader.readLong(name + "Version")));
        }
        return List.copyOf(values);
    }

    static void writeReadTarget(Writer writer, ReadTargetRecord value) {
        Writer nested = new Writer();
        nested.writeString(value.targetType());
        nested.writeInt(value.targetVersion());
        nested.writeString(value.payloadEncoding());
        nested.writeBytes(value.payload());
        nested.writeString(value.identityChecksumType());
        nested.writeString(value.identityChecksumValue());
        writer.writeBytes(nested.toByteArray());
    }

    static ReadTargetRecord readReadTarget(Reader reader, String name) {
        Reader nested = new Reader(reader.readBytes(name));
        ReadTargetRecord value = new ReadTargetRecord(
                nested.readString("targetType"),
                nested.readInt("targetVersion"),
                nested.readString("payloadEncoding"),
                nested.readBytes("payload"),
                nested.readString("identityChecksumType"),
                nested.readString("identityChecksumValue"));
        nested.requireConsumed();
        ReadTargetCodecRegistry.phase15().decode(value);
        return value;
    }

    static MetadataCodecException malformed(String recordType, Throwable failure) {
        if (failure instanceof MetadataCodecException codec) {
            return codec;
        }
        if (failure instanceof IllegalArgumentException || failure instanceof ArithmeticException
                || failure instanceof NullPointerException || failure instanceof java.nio.BufferUnderflowException) {
            return new MetadataCodecException("invalid " + recordType + " payload", failure);
        }
        return new MetadataCodecException("failed to decode " + recordType + " payload", failure);
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
        } catch (CharacterCodingException failure) {
            throw new MetadataCodecException("invalid UTF-16 input", failure);
        }
    }

    private static String strictDecode(byte[] value, String name) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw new MetadataCodecException("invalid UTF-8 in " + name, failure);
        }
    }
}
