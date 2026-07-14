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

    @Test
    void everySingleByteEnvelopeMutationAndEveryTruncationFailsClosed() {
        assertEveryMutationFails(
                MetadataRecordCodecFactory.encodeEnvelope(
                        F3MetadataCodecSamples.fullActive(), CursorStateRecord.class),
                CursorStateRecord.class);
        assertEveryMutationFails(
                MetadataRecordCodecFactory.encodeEnvelope(
                        F3MetadataCodecSamples.trimRetention(), CursorRetentionRecord.class),
                CursorRetentionRecord.class);
    }

    private static <T> void assertEveryMutationFails(byte[] canonical, Class<T> recordClass) {
        for (int index = 0; index < canonical.length; index++) {
            byte[] mutated = canonical.clone();
            mutated[index] ^= 1;
            assertDecodeFails(mutated, recordClass,
                    "accepted mutated byte " + index + " of " + recordClass.getSimpleName());
        }
        for (int length = 0; length < canonical.length; length++) {
            byte[] truncated = java.util.Arrays.copyOf(canonical, length);
            assertDecodeFails(truncated, recordClass,
                    "accepted truncated length " + length + " of " + recordClass.getSimpleName());
        }
        byte[] trailing = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        assertThatThrownBy(() -> MetadataRecordCodecFactory.decodeEnvelope(trailing, recordClass))
                .isInstanceOf(MetadataCodecException.class);
    }

    private static <T> void assertDecodeFails(byte[] bytes, Class<T> recordClass, String message) {
        try {
            MetadataRecordCodecFactory.decodeEnvelope(bytes, recordClass);
        } catch (MetadataCodecException expected) {
            return;
        }
        throw new AssertionError(message);
    }
}
