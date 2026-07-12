/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.records.StreamNameRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class F2MetadataCodecsTest {
    @Test
    void roundTripsAllFourRecordsThroughTheThirdExplicitRegistryAndFactory() {
        for (F2MetadataCodecSamples.Sample<?> sample : F2MetadataCodecSamples.samples()) {
            assertRoundTrip(sample);
            assertThat(F2MetadataCodecs.registry().codecForClass(sample.recordClass()).recordType())
                    .isEqualTo(sample.recordClass().getSimpleName());
        }
    }

    @Test
    void goldenEnvelopeHexIsStableForAllFourRecords() throws IOException {
        Properties golden = loadGoldenEnvelopeHex();
        assertThat(golden.stringPropertyNames()).containsExactlyInAnyOrderElementsOf(
                F2MetadataCodecSamples.samples().stream()
                        .map(sample -> sample.recordClass().getSimpleName())
                        .collect(Collectors.toSet()));
        for (F2MetadataCodecSamples.Sample<?> sample : F2MetadataCodecSamples.samples()) {
            assertThat(F2MetadataCodecs.envelopeHex(sample.record(), sample.recordClass()))
                    .as(sample.recordClass().getSimpleName())
                    .isEqualTo(golden.getProperty(sample.recordClass().getSimpleName()));
        }
    }

    @Test
    void factoryAdditionDoesNotChangePhase1GoldenBytes() {
        StreamNameRecord phase1 = new StreamNameRecord("tenant/ns/topic", "stream", "hash", 1, 0);

        assertThat(MetadataRecordCodecFactory.encodeEnvelope(phase1, StreamNameRecord.class))
                .isEqualTo(Phase1MetadataCodecs.encodeEnvelope(phase1, StreamNameRecord.class));
        assertThat(MetadataRecordCodecFactory.decodeEnvelope(
                        Phase1MetadataCodecs.encodeEnvelope(phase1, StreamNameRecord.class),
                        StreamNameRecord.class))
                .isEqualTo(phase1);
        assertThat(ProjectionIdentity.class).isNotEqualTo(
                com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity.class);
    }

    @Test
    void rejectsNonZeroDurableMetadataVersionOnEncodeAndDecode() {
        TopicProjectionRecord nonzero = F2MetadataCodecSamples.topic(9);
        MetadataRecordCodec<TopicProjectionRecord> raw =
                Phase1MetadataCodecs.recordCodec(TopicProjectionRecord.class);
        byte[] forged = MetadataRecordEnvelope.encode(
                raw.recordType(), raw.schemaVersion(), raw.minReaderSchemaVersion(),
                MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1, raw.encode(nonzero));

        assertThatThrownBy(() -> F2MetadataCodecs.encodeEnvelope(nonzero, TopicProjectionRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("must be zero");
        assertThatThrownBy(() -> MetadataRecordCodecFactory.decodeEnvelope(forged, TopicProjectionRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("must be zero");
    }

    @Test
    void rejectsWrongTypeChecksumFailureAndTrailingBytes() {
        TopicProjectionRecord topic = F2MetadataCodecSamples.topic(0);
        byte[] valid = F2MetadataCodecs.encodeEnvelope(topic, TopicProjectionRecord.class);
        byte[] corrupt = valid.clone();
        corrupt[corrupt.length - 1] ^= 1;
        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);

        assertThatThrownBy(() -> MetadataRecordCodecFactory.decodeEnvelope(
                        valid,
                        com.nereusstream.metadata.oxia.records.LedgerIdAllocatorRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("type mismatch");
        assertThatThrownBy(() -> MetadataRecordCodecFactory.decodeEnvelope(corrupt, TopicProjectionRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("checksum");
        assertThatThrownBy(() -> MetadataRecordCodecFactory.decodeEnvelope(trailing, TopicProjectionRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("trailing");
    }

    @Test
    void rejectsUnknownDurableFacadeState() {
        TopicProjectionRecord topic = F2MetadataCodecSamples.topic(0);
        byte[] valid = F2MetadataCodecs.encodeEnvelope(topic, TopicProjectionRecord.class);
        MetadataRecordEnvelope.DecodedEnvelope envelope = MetadataRecordEnvelope.decode(valid);
        byte[] payload = envelope.payload();
        replaceAscii(payload, "OPEN", "NOPE");
        byte[] forged = MetadataRecordEnvelope.encode(
                envelope.recordType(), envelope.schemaVersion(), envelope.minReaderSchemaVersion(),
                envelope.payloadEncoding(), payload);

        assertThatThrownBy(() -> MetadataRecordCodecFactory.decodeEnvelope(forged, TopicProjectionRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("failed to decode");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> void assertRoundTrip(F2MetadataCodecSamples.Sample<T> sample) {
        byte[] encoded = MetadataRecordCodecFactory.encodeEnvelope(sample.record(), sample.recordClass());
        Object decoded = MetadataRecordCodecFactory.decodeEnvelope(encoded, (Class) sample.recordClass());
        assertThat(decoded).isEqualTo(sample.record());
        assertThat(MetadataRecordCodecFactory.recordType(encoded))
                .isEqualTo(sample.recordClass().getSimpleName());
    }

    private static Properties loadGoldenEnvelopeHex() throws IOException {
        Properties properties = new Properties();
        try (InputStream input = F2MetadataCodecsTest.class.getResourceAsStream(
                "f2-metadata-codec-golden.properties")) {
            assertThat(input).isNotNull();
            properties.load(input);
        }
        return properties;
    }

    private static void replaceAscii(byte[] bytes, String expected, String replacement) {
        byte[] needle = expected.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        byte[] value = replacement.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        for (int i = 0; i <= bytes.length - needle.length; i++) {
            if (Arrays.equals(Arrays.copyOfRange(bytes, i, i + needle.length), needle)) {
                System.arraycopy(value, 0, bytes, i, value.length);
                return;
            }
        }
        throw new AssertionError("expected ASCII value not found");
    }
}
