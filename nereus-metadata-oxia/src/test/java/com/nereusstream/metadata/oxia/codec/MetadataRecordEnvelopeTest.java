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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

class MetadataRecordEnvelopeTest {
    @Test
    void envelopeRoundTripsPayloadAndRejectsChecksumMismatch() {
        byte[] encoded = MetadataRecordEnvelope.encode(
                "StreamHeadRecord",
                1,
                1,
                MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1,
                new byte[] {1, 2, 3});

        MetadataRecordEnvelope.DecodedEnvelope decoded = MetadataRecordEnvelope.decode(encoded);

        assertThat(decoded.recordType()).isEqualTo("StreamHeadRecord");
        assertThat(decoded.schemaVersion()).isEqualTo(1);
        assertThat(decoded.payload()).containsExactly(1, 2, 3);

        encoded[encoded.length - 1] ^= 0x7f;
        assertThatThrownBy(() -> MetadataRecordEnvelope.decode(encoded))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("checksum");
    }

    @Test
    void envelopeRejectsTruncatedPayload() {
        byte[] encoded = MetadataRecordEnvelope.encode(
                "OffsetIndexRecord",
                1,
                1,
                MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1,
                new byte[] {1, 2, 3});
        byte[] truncated = java.util.Arrays.copyOf(encoded, encoded.length - 2);

        assertThatThrownBy(() -> MetadataRecordEnvelope.decode(truncated))
                .isInstanceOf(MetadataCodecException.class);
    }

    @Test
    void envelopeRejectsMalformedUtf8RecordType() {
        byte[] encoded = rawEnvelope(
                new byte[] {(byte) 0xc3, 0x28},
                1,
                1,
                MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1.getBytes(StandardCharsets.UTF_8),
                new byte[] {1, 2, 3});

        assertThatThrownBy(() -> MetadataRecordEnvelope.decode(encoded))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("invalid UTF-8");
    }

    @Test
    void registryRejectsUnknownAndDuplicateCodecs() {
        MetadataRecordCodec<String> codec = new StringCodec("TestRecord");
        MapMetadataCodecRegistry registry = new MapMetadataCodecRegistry(List.of(
                new MapMetadataCodecRegistry.RegisteredCodec<>(String.class, codec)));

        assertThat(registry.<String>codecForType("TestRecord")).isSameAs(codec);
        assertThat(registry.codecForClass(String.class)).isSameAs(codec);
        assertThatThrownBy(() -> registry.codecForType("MissingRecord"))
                .isInstanceOf(MetadataCodecException.class);
        assertThatThrownBy(() -> new MapMetadataCodecRegistry(List.of(
                new MapMetadataCodecRegistry.RegisteredCodec<>(String.class, codec),
                new MapMetadataCodecRegistry.RegisteredCodec<>(String.class, codec))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private record StringCodec(String recordType) implements MetadataRecordCodec<String> {
        @Override
        public int schemaVersion() {
            return 1;
        }

        @Override
        public int minReaderSchemaVersion() {
            return 1;
        }

        @Override
        public byte[] encode(String record) {
            return record.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] bytes) {
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    private static byte[] rawEnvelope(
            byte[] recordType,
            int schemaVersion,
            int minReaderSchemaVersion,
            byte[] payloadEncoding,
            byte[] payload) {
        byte[] magic = MetadataRecordEnvelope.MAGIC.getBytes(StandardCharsets.US_ASCII);
        ByteBuffer buffer = ByteBuffer.allocate(
                magic.length
                        + Short.BYTES + recordType.length
                        + Integer.BYTES
                        + Integer.BYTES
                        + Short.BYTES + payloadEncoding.length
                        + Integer.BYTES
                        + Integer.BYTES
                        + payload.length);
        buffer.put(magic);
        putShortBytes(buffer, recordType);
        buffer.putInt(schemaVersion);
        buffer.putInt(minReaderSchemaVersion);
        putShortBytes(buffer, payloadEncoding);
        buffer.putInt(payload.length);
        buffer.putInt(crc32c(payload));
        buffer.put(payload);
        return buffer.array();
    }

    private static void putShortBytes(ByteBuffer buffer, byte[] bytes) {
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private static int crc32c(byte[] payload) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(payload, 0, payload.length);
        return (int) crc32c.getValue();
    }
}
