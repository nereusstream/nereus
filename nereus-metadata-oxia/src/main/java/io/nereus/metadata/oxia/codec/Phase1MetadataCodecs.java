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

package io.nereus.metadata.oxia.codec;

import io.nereus.metadata.oxia.records.AppendSessionRecord;
import io.nereus.metadata.oxia.records.AppendSessionSnapshotRecord;
import io.nereus.metadata.oxia.records.CommittedEndOffsetRecord;
import io.nereus.metadata.oxia.records.CommittedSliceRecord;
import io.nereus.metadata.oxia.records.EntryIndexReferenceRecord;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.metadata.oxia.records.ObjectReferenceRecord;
import io.nereus.metadata.oxia.records.OffsetIndexRecord;
import io.nereus.metadata.oxia.records.StreamCommitRecord;
import io.nereus.metadata.oxia.records.StreamHeadRecord;
import io.nereus.metadata.oxia.records.StreamMetadataRecord;
import io.nereus.metadata.oxia.records.StreamNameRecord;
import io.nereus.metadata.oxia.records.StreamSliceManifestRecord;
import io.nereus.metadata.oxia.records.TrimRecord;
import io.nereus.metadata.oxia.records.VisibleSliceReferenceRecord;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Shared Phase 1 metadata codecs used by fake and future real Oxia adapters. */
public final class Phase1MetadataCodecs {
    private static final int SCHEMA_VERSION = 1;
    private static final int MIN_READER_SCHEMA_VERSION = 1;

    private static final byte TYPE_STRING = 1;
    private static final byte TYPE_LONG = 2;
    private static final byte TYPE_INT = 3;
    private static final byte TYPE_BOOLEAN = 4;
    private static final byte TYPE_BYTES = 5;
    private static final byte TYPE_LIST = 6;
    private static final byte TYPE_MAP = 7;
    private static final byte TYPE_RECORD = 8;

    private static final MapMetadataCodecRegistry REGISTRY = new MapMetadataCodecRegistry(List.of(
            registered(AppendSessionRecord.class),
            registered(AppendSessionSnapshotRecord.class),
            registered(CommittedEndOffsetRecord.class),
            registered(CommittedSliceRecord.class),
            registered(EntryIndexReferenceRecord.class),
            registered(ObjectManifestRecord.class),
            registered(ObjectReferenceRecord.class),
            registered(OffsetIndexRecord.class),
            registered(StreamCommitRecord.class),
            registered(StreamHeadRecord.class),
            registered(StreamMetadataRecord.class),
            registered(StreamNameRecord.class),
            registered(StreamSliceManifestRecord.class),
            registered(TrimRecord.class),
            registered(VisibleSliceReferenceRecord.class)));

    private Phase1MetadataCodecs() {
    }

    public static MetadataCodecRegistry registry() {
        return REGISTRY;
    }

    public static <T> byte[] encodeEnvelope(T record, Class<T> recordClass) {
        MetadataRecordCodec<T> codec = REGISTRY.codecForClass(recordClass);
        return MetadataRecordEnvelope.encode(
                codec.recordType(),
                codec.schemaVersion(),
                codec.minReaderSchemaVersion(),
                MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1,
                codec.encode(record));
    }

    public static <T> T decodeEnvelope(byte[] bytes, Class<T> recordClass) {
        MetadataRecordEnvelope.DecodedEnvelope envelope = MetadataRecordEnvelope.decode(bytes);
        MetadataRecordCodec<T> codec = REGISTRY.codecForClass(recordClass);
        if (!codec.recordType().equals(envelope.recordType())) {
            throw new MetadataCodecException("metadata envelope record type mismatch: expected "
                    + codec.recordType() + " but found " + envelope.recordType());
        }
        if (!MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1.equals(envelope.payloadEncoding())) {
            throw new MetadataCodecException("unsupported metadata payload encoding: " + envelope.payloadEncoding());
        }
        if (envelope.minReaderSchemaVersion() > codec.schemaVersion()
                || envelope.schemaVersion() != codec.schemaVersion()) {
            throw new MetadataCodecException("unsupported metadata schema version for " + codec.recordType()
                    + ": writer=" + envelope.schemaVersion()
                    + ", minReader=" + envelope.minReaderSchemaVersion());
        }
        return codec.decode(envelope.payload());
    }

    public static String envelopeHex(Object record, Class<?> recordClass) {
        return HexFormat.of().formatHex(encodeEnvelopeUnchecked(record, recordClass));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static byte[] encodeEnvelopeUnchecked(Object record, Class<?> recordClass) {
        return encodeEnvelope(record, (Class) recordClass);
    }

    private static <T extends Record> MapMetadataCodecRegistry.RegisteredCodec<T> registered(Class<T> recordClass) {
        return new MapMetadataCodecRegistry.RegisteredCodec<>(recordClass, new RecordMetadataCodec<>(recordClass));
    }

    private static final class RecordMetadataCodec<T extends Record> implements MetadataRecordCodec<T> {
        private final Class<T> recordClass;

        private RecordMetadataCodec(Class<T> recordClass) {
            this.recordClass = Objects.requireNonNull(recordClass, "recordClass");
            if (!recordClass.isRecord()) {
                throw new IllegalArgumentException("metadata codec class must be a record: " + recordClass);
            }
        }

        @Override
        public String recordType() {
            return recordClass.getSimpleName();
        }

        @Override
        public int schemaVersion() {
            return SCHEMA_VERSION;
        }

        @Override
        public int minReaderSchemaVersion() {
            return MIN_READER_SCHEMA_VERSION;
        }

        @Override
        public byte[] encode(T record) {
            Objects.requireNonNull(record, "record");
            if (!recordClass.isInstance(record)) {
                throw new MetadataCodecException("record is not a " + recordClass.getName());
            }
            PayloadWriter writer = new PayloadWriter();
            writer.writeRecord(record, recordClass);
            return writer.toByteArray();
        }

        @Override
        public T decode(byte[] bytes) {
            PayloadReader reader = new PayloadReader(bytes);
            T record = reader.readRecord(recordClass);
            reader.requireConsumed();
            return record;
        }
    }

    private static final class PayloadWriter {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        byte[] toByteArray() {
            return out.toByteArray();
        }

        void writeValue(Object value, Type genericType, Class<?> rawType) {
            Objects.requireNonNull(value, "metadata record field cannot be null");
            if (rawType == String.class) {
                writeString((String) value);
            } else if (rawType == long.class || rawType == Long.class) {
                writeLong((Long) value);
            } else if (rawType == int.class || rawType == Integer.class) {
                writeInt((Integer) value);
            } else if (rawType == boolean.class || rawType == Boolean.class) {
                writeBoolean((Boolean) value);
            } else if (rawType == byte[].class) {
                writeBytes((byte[]) value);
            } else if (List.class.isAssignableFrom(rawType)) {
                writeList((List<?>) value, listElementClass(genericType));
            } else if (Map.class.isAssignableFrom(rawType)) {
                writeStringMap((Map<?, ?>) value);
            } else if (rawType.isRecord()) {
                writeRecord((Record) value, rawType.asSubclass(Record.class));
            } else {
                throw new MetadataCodecException("unsupported metadata field type: " + rawType.getName());
            }
        }

        void writeString(String value) {
            putByte(TYPE_STRING);
            putBytes(StrictUtf8.encode(value));
        }

        void writeLong(long value) {
            putByte(TYPE_LONG);
            putLong(value);
        }

        void writeInt(int value) {
            putByte(TYPE_INT);
            putInt(value);
        }

        void writeBoolean(boolean value) {
            putByte(TYPE_BOOLEAN);
            putByte(value ? 1 : 0);
        }

        void writeBytes(byte[] value) {
            putByte(TYPE_BYTES);
            putBytes(value.clone());
        }

        void writeList(List<?> values, Class<?> elementClass) {
            putByte(TYPE_LIST);
            putInt(values.size());
            for (Object value : values) {
                writeValue(value, elementClass, elementClass);
            }
        }

        void writeStringMap(Map<?, ?> map) {
            putByte(TYPE_MAP);
            List<Map.Entry<String, String>> entries = new ArrayList<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key) || !(entry.getValue() instanceof String value)) {
                    throw new MetadataCodecException("metadata map fields must be Map<String,String>");
                }
                entries.add(Map.entry(key, value));
            }
            entries.sort(Map.Entry.comparingByKey(Phase1MetadataCodecs::compareUtf8));
            putInt(entries.size());
            for (Map.Entry<String, String> entry : entries) {
                writeString(entry.getKey());
                writeString(entry.getValue());
            }
        }

        void writeRecord(Record record, Class<? extends Record> expectedClass) {
            if (!expectedClass.isInstance(record)) {
                throw new MetadataCodecException("nested record is not a " + expectedClass.getName());
            }
            putByte(TYPE_RECORD);
            writeString(expectedClass.getSimpleName());
            RecordComponent[] components = expectedClass.getRecordComponents();
            putInt(components.length);
            for (RecordComponent component : components) {
                writeString(component.getName());
                try {
                    Object value = component.getAccessor().invoke(record);
                    writeValue(value, component.getGenericType(), component.getType());
                } catch (ReflectiveOperationException e) {
                    throw new MetadataCodecException("failed to encode metadata field "
                            + expectedClass.getSimpleName() + "." + component.getName(), e);
                }
            }
        }

        private void putByte(int value) {
            out.write(value & 0xff);
        }

        private void putBytes(byte[] bytes) {
            putInt(bytes.length);
            out.writeBytes(bytes);
        }

        private void putInt(int value) {
            out.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
        }

        private void putLong(long value) {
            out.writeBytes(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
        }
    }

    private static final class PayloadReader {
        private final ByteBuffer buffer;

        PayloadReader(byte[] bytes) {
            buffer = ByteBuffer.wrap(Objects.requireNonNull(bytes, "bytes"));
        }

        void requireConsumed() {
            if (buffer.hasRemaining()) {
                throw new MetadataCodecException("metadata payload has trailing bytes");
            }
        }

        Object readValue(Type genericType, Class<?> rawType) {
            if (rawType == String.class) {
                return readString();
            } else if (rawType == long.class || rawType == Long.class) {
                return readLong();
            } else if (rawType == int.class || rawType == Integer.class) {
                return readInt();
            } else if (rawType == boolean.class || rawType == Boolean.class) {
                return readBoolean();
            } else if (rawType == byte[].class) {
                return readBytes();
            } else if (List.class.isAssignableFrom(rawType)) {
                return readList(listElementClass(genericType));
            } else if (Map.class.isAssignableFrom(rawType)) {
                return readStringMap();
            } else if (rawType.isRecord()) {
                return readRecord(rawType.asSubclass(Record.class));
            }
            throw new MetadataCodecException("unsupported metadata field type: " + rawType.getName());
        }

        String readString() {
            expectType(TYPE_STRING);
            return StrictUtf8.decode(readLengthPrefixedBytes(), "metadata payload string");
        }

        long readLong() {
            expectType(TYPE_LONG);
            requireRemaining(Long.BYTES);
            return buffer.getLong();
        }

        int readInt() {
            expectType(TYPE_INT);
            requireRemaining(Integer.BYTES);
            return buffer.getInt();
        }

        boolean readBoolean() {
            expectType(TYPE_BOOLEAN);
            requireRemaining(1);
            byte value = buffer.get();
            if (value != 0 && value != 1) {
                throw new MetadataCodecException("invalid boolean value in metadata payload");
            }
            return value == 1;
        }

        byte[] readBytes() {
            expectType(TYPE_BYTES);
            return readLengthPrefixedBytes();
        }

        List<?> readList(Class<?> elementClass) {
            expectType(TYPE_LIST);
            int size = readSize();
            List<Object> values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                values.add(readValue(elementClass, elementClass));
            }
            return List.copyOf(values);
        }

        Map<String, String> readStringMap() {
            expectType(TYPE_MAP);
            int size = readSize();
            Map<String, String> map = new LinkedHashMap<>();
            for (int i = 0; i < size; i++) {
                String key = readString();
                String previous = map.put(key, readString());
                if (previous != null) {
                    throw new MetadataCodecException("duplicate key in metadata map field: " + key);
                }
            }
            return map;
        }

        <T extends Record> T readRecord(Class<T> recordClass) {
            expectType(TYPE_RECORD);
            String recordType = readString();
            if (!recordClass.getSimpleName().equals(recordType)) {
                throw new MetadataCodecException("metadata nested record type mismatch: expected "
                        + recordClass.getSimpleName() + " but found " + recordType);
            }
            RecordComponent[] components = recordClass.getRecordComponents();
            int fieldCount = readSize();
            if (fieldCount != components.length) {
                throw new MetadataCodecException("metadata field count mismatch for " + recordType);
            }
            Object[] args = new Object[components.length];
            Class<?>[] parameterTypes = new Class<?>[components.length];
            for (int i = 0; i < components.length; i++) {
                RecordComponent component = components[i];
                String fieldName = readString();
                if (!component.getName().equals(fieldName)) {
                    throw new MetadataCodecException("metadata field order mismatch for " + recordType
                            + ": expected " + component.getName() + " but found " + fieldName);
                }
                args[i] = readValue(component.getGenericType(), component.getType());
                parameterTypes[i] = component.getType();
            }
            try {
                Constructor<T> constructor = recordClass.getDeclaredConstructor(parameterTypes);
                return constructor.newInstance(args);
            } catch (ReflectiveOperationException e) {
                throw new MetadataCodecException("failed to decode metadata record " + recordType, e);
            }
        }

        private void expectType(byte expected) {
            requireRemaining(1);
            byte actual = buffer.get();
            if (actual != expected) {
                throw new MetadataCodecException("metadata payload type mismatch: expected "
                        + expected + " but found " + actual);
            }
        }

        private byte[] readLengthPrefixedBytes() {
            int length = readSizeWithoutTag();
            requireRemaining(length);
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return bytes;
        }

        private int readSize() {
            int size = readSizeWithoutTag();
            if (size < 0) {
                throw new MetadataCodecException("negative metadata collection size");
            }
            return size;
        }

        private int readSizeWithoutTag() {
            requireRemaining(Integer.BYTES);
            int size = buffer.getInt();
            if (size < 0) {
                throw new MetadataCodecException("negative metadata payload length");
            }
            return size;
        }

        private void requireRemaining(int bytes) {
            if (buffer.remaining() < bytes) {
                throw new MetadataCodecException("truncated metadata payload");
            }
        }
    }

    private static Class<?> listElementClass(Type genericType) {
        if (!(genericType instanceof ParameterizedType parameterizedType)
                || parameterizedType.getActualTypeArguments().length != 1
                || !(parameterizedType.getActualTypeArguments()[0] instanceof Class<?> elementClass)) {
            throw new MetadataCodecException("metadata list fields must declare one concrete element type");
        }
        return elementClass;
    }

    private static int compareUtf8(String left, String right) {
        byte[] leftBytes = StrictUtf8.encode(left);
        byte[] rightBytes = StrictUtf8.encode(right);
        int limit = Math.min(leftBytes.length, rightBytes.length);
        for (int i = 0; i < limit; i++) {
            int leftByte = leftBytes[i] & 0xff;
            int rightByte = rightBytes[i] & 0xff;
            if (leftByte != rightByte) {
                return Integer.compare(leftByte, rightByte);
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }
}
