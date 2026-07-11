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

package com.nereusstream.metadata.oxia.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Versioned metadata value envelope shared by fake and future real Oxia adapters. */
public final class MetadataRecordEnvelope {
    public static final String MAGIC = "NRM1";
    public static final String PAYLOAD_ENCODING_BINARY_V1 = "binary-v1";

    private MetadataRecordEnvelope() {
    }

    public static byte[] encode(
            String recordType,
            int schemaVersion,
            int minReaderSchemaVersion,
            String payloadEncoding,
            byte[] payload) {
        byte[] magicBytes = MAGIC.getBytes(StandardCharsets.US_ASCII);
        byte[] recordTypeBytes = StrictUtf8.encode(requireNonBlank(recordType, "recordType"));
        byte[] payloadEncodingBytes = StrictUtf8.encode(requireNonBlank(payloadEncoding, "payloadEncoding"));
        Objects.requireNonNull(payload, "payload");
        if (schemaVersion < 0 || minReaderSchemaVersion < 0 || minReaderSchemaVersion > schemaVersion) {
            throw new IllegalArgumentException("invalid schema version fields");
        }
        if (recordTypeBytes.length > Short.MAX_VALUE || payloadEncodingBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("envelope string field is too large");
        }

        ByteBuffer buffer = ByteBuffer.allocate(
                magicBytes.length
                        + Short.BYTES + recordTypeBytes.length
                        + Integer.BYTES
                        + Integer.BYTES
                        + Short.BYTES + payloadEncodingBytes.length
                        + Integer.BYTES
                        + Integer.BYTES
                        + payload.length);
        buffer.put(magicBytes);
        putShortBytes(buffer, recordTypeBytes);
        buffer.putInt(schemaVersion);
        buffer.putInt(minReaderSchemaVersion);
        putShortBytes(buffer, payloadEncodingBytes);
        buffer.putInt(payload.length);
        buffer.putInt(crc32c(payload));
        buffer.put(payload);
        return buffer.array();
    }

    public static DecodedEnvelope decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            byte[] magic = new byte[MAGIC.length()];
            buffer.get(magic);
            if (!MAGIC.equals(new String(magic, StandardCharsets.US_ASCII))) {
                throw new MetadataCodecException("invalid metadata envelope magic");
            }
            String recordType = readShortString(buffer, "recordType");
            int schemaVersion = buffer.getInt();
            int minReaderSchemaVersion = buffer.getInt();
            String payloadEncoding = readShortString(buffer, "payloadEncoding");
            int payloadLength = buffer.getInt();
            int expectedChecksum = buffer.getInt();
            if (payloadLength < 0 || payloadLength > buffer.remaining()) {
                throw new MetadataCodecException("invalid metadata payload length");
            }
            byte[] payload = new byte[payloadLength];
            buffer.get(payload);
            if (buffer.hasRemaining()) {
                throw new MetadataCodecException("metadata envelope has trailing bytes");
            }
            int actualChecksum = crc32c(payload);
            if (actualChecksum != expectedChecksum) {
                throw new MetadataCodecException("metadata payload checksum mismatch");
            }
            return new DecodedEnvelope(
                    recordType,
                    schemaVersion,
                    minReaderSchemaVersion,
                    payloadEncoding,
                    payload);
        } catch (MetadataCodecException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new MetadataCodecException("truncated or malformed metadata envelope", e);
        }
    }

    private static void putShortBytes(ByteBuffer buffer, byte[] bytes) {
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private static String readShortString(ByteBuffer buffer, String fieldName) {
        int length = Short.toUnsignedInt(buffer.getShort());
        if (length > buffer.remaining()) {
            throw new MetadataCodecException("truncated metadata envelope " + fieldName);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return StrictUtf8.decode(bytes, "metadata envelope " + fieldName);
    }

    private static int crc32c(byte[] payload) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(payload, 0, payload.length);
        return (int) crc32c.getValue();
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }

    public record DecodedEnvelope(
            String recordType,
            int schemaVersion,
            int minReaderSchemaVersion,
            String payloadEncoding,
            byte[] payload) {
        public DecodedEnvelope {
            requireNonBlank(recordType, "recordType");
            requireNonBlank(payloadEncoding, "payloadEncoding");
            payload = Objects.requireNonNull(payload, "payload").clone();
            if (schemaVersion < 0 || minReaderSchemaVersion < 0 || minReaderSchemaVersion > schemaVersion) {
                throw new IllegalArgumentException("invalid schema version fields");
            }
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DecodedEnvelope that)) {
                return false;
            }
            return schemaVersion == that.schemaVersion
                    && minReaderSchemaVersion == that.minReaderSchemaVersion
                    && recordType.equals(that.recordType)
                    && payloadEncoding.equals(that.payloadEncoding)
                    && Arrays.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(recordType, schemaVersion, minReaderSchemaVersion, payloadEncoding);
            result = 31 * result + Arrays.hashCode(payload);
            return result;
        }
    }
}
