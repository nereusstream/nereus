/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import org.junit.jupiter.api.Test;

class F3MetadataCodecsCompatibilityTest {
    @Test
    void factoryDispatchesExactF3RecordTypesWithoutDecoderProbing() {
        CursorStateRecord cursor = F3MetadataCodecSamples.fullActive();
        byte[] cursorBytes = MetadataRecordCodecFactory.encodeEnvelope(cursor, CursorStateRecord.class);
        assertThat(MetadataRecordCodecFactory.recordType(cursorBytes)).isEqualTo("CursorStateRecord");
        assertThat(MetadataRecordCodecFactory.decodeEnvelope(cursorBytes, CursorStateRecord.class))
                .isEqualTo(cursor);

        CursorRetentionRecord retention = F3MetadataCodecSamples.trimRetention();
        byte[] retentionBytes = MetadataRecordCodecFactory.encodeEnvelope(retention, CursorRetentionRecord.class);
        assertThat(MetadataRecordCodecFactory.recordType(retentionBytes)).isEqualTo("CursorRetentionRecord");
        assertThat(MetadataRecordCodecFactory.decodeEnvelope(retentionBytes, CursorRetentionRecord.class))
                .isEqualTo(retention);
        assertThatThrownBy(() -> MetadataRecordCodecFactory.decodeEnvelope(
                cursorBytes, CursorRetentionRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("record type mismatch");
    }
}
