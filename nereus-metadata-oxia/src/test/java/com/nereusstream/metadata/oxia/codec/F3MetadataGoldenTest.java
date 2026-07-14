/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class F3MetadataGoldenTest {
    @Test
    void canonicalF3EnvelopesMatchFrozenGoldenBytes() throws IOException {
        Map<String, String> actual = new LinkedHashMap<>();
        actual.put("cursor.minimal", F3MetadataCodecs.envelopeHex(
                F3MetadataCodecSamples.minimalActive(), CursorStateRecord.class));
        actual.put("cursor.full", F3MetadataCodecs.envelopeHex(
                F3MetadataCodecSamples.fullActive(), CursorStateRecord.class));
        actual.put("cursor.deleted", F3MetadataCodecs.envelopeHex(
                F3MetadataCodecSamples.deleted(), CursorStateRecord.class));
        actual.put("retention.active", F3MetadataCodecs.envelopeHex(
                F3MetadataCodecSamples.activeRetention(), CursorRetentionRecord.class));
        actual.put("retention.create", F3MetadataCodecs.envelopeHex(
                F3MetadataCodecSamples.protectionRetention(F3MetadataCodecSamples.createIntent()),
                CursorRetentionRecord.class));
        actual.put("retention.recreate", F3MetadataCodecs.envelopeHex(
                F3MetadataCodecSamples.protectionRetention(F3MetadataCodecSamples.recreateIntent()),
                CursorRetentionRecord.class));
        actual.put("retention.reset", F3MetadataCodecs.envelopeHex(
                F3MetadataCodecSamples.protectionRetention(F3MetadataCodecSamples.backwardResetIntent()),
                CursorRetentionRecord.class));
        actual.put("retention.trim", F3MetadataCodecs.envelopeHex(
                F3MetadataCodecSamples.trimRetention(), CursorRetentionRecord.class));

        Properties expected = new Properties();
        try (var input = F3MetadataGoldenTest.class.getResourceAsStream("f3-metadata-codec-golden.properties")) {
            expected.load(new InputStreamReader(input, StandardCharsets.UTF_8));
        }
        assertThat(actual).allSatisfy((key, value) ->
                assertThat(value).as(key).isEqualTo(expected.getProperty(key)));
        assertThat(expected.stringPropertyNames()).containsExactlyInAnyOrderElementsOf(actual.keySet());
    }
}
