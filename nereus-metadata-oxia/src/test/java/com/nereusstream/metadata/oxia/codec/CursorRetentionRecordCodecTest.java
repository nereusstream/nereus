/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import java.util.List;
import org.junit.jupiter.api.Test;

class CursorRetentionRecordCodecTest {
    private final CursorRetentionRecordCodecV1 codec = new CursorRetentionRecordCodecV1();

    @Test
    void roundTripsAllLifecycleAndProtectionShapesByteForByte() {
        List<CursorRetentionRecord> samples = List.of(
                F3MetadataCodecSamples.activeRetention(),
                F3MetadataCodecSamples.protectionRetention(F3MetadataCodecSamples.createIntent()),
                F3MetadataCodecSamples.protectionRetention(F3MetadataCodecSamples.recreateIntent()),
                F3MetadataCodecSamples.protectionRetention(F3MetadataCodecSamples.backwardResetIntent()),
                F3MetadataCodecSamples.trimRetention());
        for (CursorRetentionRecord sample : samples) {
            byte[] encoded = codec.encode(sample);
            CursorRetentionRecord decoded = codec.decode(encoded);
            assertThat(decoded).isEqualTo(sample);
            assertThat(codec.encode(decoded)).containsExactly(encoded);
        }
    }

    @Test
    void rejectsTruncatedAndTrailingPayloads() {
        byte[] encoded = codec.encode(F3MetadataCodecSamples.trimRetention());
        assertThatThrownBy(() -> codec.decode(java.util.Arrays.copyOf(encoded, encoded.length - 1)))
                .isInstanceOf(MetadataCodecException.class);
        assertThatThrownBy(() -> codec.decode(java.util.Arrays.copyOf(encoded, encoded.length + 1)))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("trailing");
    }
}
