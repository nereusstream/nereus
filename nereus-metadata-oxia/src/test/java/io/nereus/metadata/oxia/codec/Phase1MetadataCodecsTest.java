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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.nereus.metadata.oxia.records.StreamMetadataRecord;
import io.nereus.metadata.oxia.records.StreamNameRecord;
import io.nereus.metadata.oxia.records.TrimRecord;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class Phase1MetadataCodecsTest {
    @Test
    void roundTripsEveryPhase1RecordType() {
        for (MetadataCodecSamples.Sample<?> sample : MetadataCodecSamples.samples()) {
            assertRoundTrip(sample);
            assertThat(Phase1MetadataCodecs.registry()
                    .codecForType(sample.recordClass().getSimpleName())
                    .recordType())
                    .isEqualTo(sample.recordClass().getSimpleName());
        }
    }

    @Test
    void goldenEnvelopeHexIsStableForEveryPhase1RecordType() throws IOException {
        Properties golden = loadGoldenEnvelopeHex();
        assertThat(golden.stringPropertyNames())
                .containsExactlyInAnyOrderElementsOf(MetadataCodecSamples.samples().stream()
                        .map(sample -> sample.recordClass().getSimpleName())
                        .collect(Collectors.toSet()));

        for (MetadataCodecSamples.Sample<?> sample : MetadataCodecSamples.samples()) {
            assertThat(Phase1MetadataCodecs.envelopeHex(sample.record(), sample.recordClass()))
                    .as(sample.recordClass().getSimpleName())
                    .isEqualTo(golden.getProperty(sample.recordClass().getSimpleName()));
        }
    }

    @Test
    void mapEncodingIsDeterministicByUtf8KeyBytes() {
        Map<String, String> leftAttributes = new LinkedHashMap<>();
        leftAttributes.put("b", "2");
        leftAttributes.put("a", "1");
        Map<String, String> rightAttributes = new LinkedHashMap<>();
        rightAttributes.put("a", "1");
        rightAttributes.put("b", "2");

        StreamMetadataRecord left = new StreamMetadataRecord(
                "stream",
                "tenant/ns/topic",
                "stream-name-hash",
                "ACTIVE",
                "OBJECT_WAL",
                leftAttributes,
                1_000,
                1,
                7);
        StreamMetadataRecord right = new StreamMetadataRecord(
                "stream",
                "tenant/ns/topic",
                "stream-name-hash",
                "ACTIVE",
                "OBJECT_WAL",
                rightAttributes,
                1_000,
                1,
                7);

        assertThat(Phase1MetadataCodecs.envelopeHex(left, StreamMetadataRecord.class))
                .isEqualTo(Phase1MetadataCodecs.envelopeHex(right, StreamMetadataRecord.class));
    }

    @Test
    void rejectsWrongEnvelopeRecordType() {
        StreamNameRecord record = new StreamNameRecord("tenant/ns/topic", "stream", "hash", 1, 2);
        byte[] encoded = Phase1MetadataCodecs.encodeEnvelope(record, StreamNameRecord.class);

        assertThatThrownBy(() -> Phase1MetadataCodecs.decodeEnvelope(encoded, TrimRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("record type mismatch");
    }

    @Test
    void rejectsUnsupportedSchemaVersionAndEncoding() {
        StreamNameRecord record = new StreamNameRecord("tenant/ns/topic", "stream", "hash", 1, 2);
        MetadataRecordCodec<StreamNameRecord> codec =
                Phase1MetadataCodecs.registry().codecForClass(StreamNameRecord.class);
        byte[] payload = codec.encode(record);

        byte[] newerVersion = MetadataRecordEnvelope.encode(
                codec.recordType(),
                2,
                2,
                MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1,
                payload);
        byte[] unsupportedEncoding = MetadataRecordEnvelope.encode(
                codec.recordType(),
                codec.schemaVersion(),
                codec.minReaderSchemaVersion(),
                "json-v1",
                payload);

        assertThatThrownBy(() -> Phase1MetadataCodecs.decodeEnvelope(newerVersion, StreamNameRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("unsupported metadata schema version");
        assertThatThrownBy(() -> Phase1MetadataCodecs.decodeEnvelope(unsupportedEncoding, StreamNameRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("unsupported metadata payload encoding");
    }

    @Test
    void rejectsMalformedPayloadTypeTagAndUtf8() {
        StreamNameRecord record = new StreamNameRecord("tenant/ns/topic", "stream", "hash", 1, 2);
        byte[] valid = Phase1MetadataCodecs.encodeEnvelope(record, StreamNameRecord.class);
        MetadataRecordEnvelope.DecodedEnvelope decoded = MetadataRecordEnvelope.decode(valid);
        byte[] invalidTypePayload = decoded.payload();
        invalidTypePayload[0] = 99;
        byte[] invalidTypeEnvelope = MetadataRecordEnvelope.encode(
                decoded.recordType(),
                decoded.schemaVersion(),
                decoded.minReaderSchemaVersion(),
                decoded.payloadEncoding(),
                invalidTypePayload);
        byte[] invalidUtf8Envelope = MetadataRecordEnvelope.encode(
                decoded.recordType(),
                decoded.schemaVersion(),
                decoded.minReaderSchemaVersion(),
                decoded.payloadEncoding(),
                malformedStreamNameRecordPayload());

        assertThatThrownBy(() -> Phase1MetadataCodecs.decodeEnvelope(invalidTypeEnvelope, StreamNameRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("type mismatch");
        assertThatThrownBy(() -> Phase1MetadataCodecs.decodeEnvelope(invalidUtf8Envelope, StreamNameRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("invalid UTF-8");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> void assertRoundTrip(MetadataCodecSamples.Sample<T> sample) {
        byte[] encoded = Phase1MetadataCodecs.encodeEnvelope(sample.record(), sample.recordClass());
        Object decoded = Phase1MetadataCodecs.decodeEnvelope(encoded, (Class) sample.recordClass());

        assertThat(decoded).isEqualTo(sample.record());
    }

    private static byte[] malformedStreamNameRecordPayload() {
        PayloadBuilder builder = new PayloadBuilder();
        builder.putByte(8);
        builder.writeString("StreamNameRecord");
        builder.putInt(5);
        builder.writeString("streamName");
        builder.writeMalformedStringBytes(new byte[] {(byte) 0xc3, 0x28});
        builder.writeString("streamId");
        builder.writeString("stream");
        builder.writeString("streamNameHash");
        builder.writeString("hash");
        builder.writeString("createdAtMillis");
        builder.writeLong(1);
        builder.writeString("metadataVersion");
        builder.writeLong(2);
        return builder.toByteArray();
    }

    private static Properties loadGoldenEnvelopeHex() throws IOException {
        Properties properties = new Properties();
        try (InputStream input = Phase1MetadataCodecsTest.class.getResourceAsStream(
                "phase1-metadata-codec-golden.properties")) {
            assertThat(input).isNotNull();
            properties.load(input);
        }
        return properties;
    }

    private static final class PayloadBuilder {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        byte[] toByteArray() {
            return out.toByteArray();
        }

        void writeString(String value) {
            putByte(1);
            putBytes(StrictUtf8.encode(value));
        }

        void writeMalformedStringBytes(byte[] bytes) {
            putByte(1);
            putBytes(bytes);
        }

        void writeLong(long value) {
            putByte(2);
            out.writeBytes(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
        }

        void putByte(int value) {
            out.write(value & 0xff);
        }

        void putInt(int value) {
            out.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
        }

        private void putBytes(byte[] bytes) {
            putInt(bytes.length);
            out.writeBytes(bytes);
        }
    }
}
