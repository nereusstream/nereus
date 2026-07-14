/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class CursorStateRecordCodecTest {
    private final CursorStateRecordCodecV1 codec = new CursorStateRecordCodecV1();

    @Test
    void roundTripsCanonicalCursorShapesByteForByte() {
        for (CursorStateRecord sample : List.of(
                F3MetadataCodecSamples.minimalActive(),
                F3MetadataCodecSamples.fullActive(),
                F3MetadataCodecSamples.deleted())) {
            byte[] encoded = codec.encode(sample);
            CursorStateRecord decoded = codec.decode(encoded);
            assertThat(decoded).isEqualTo(sample);
            assertThat(codec.encode(decoded)).containsExactly(encoded);
        }
    }

    @Test
    void rejectsNonzeroEncodedMetadataVersion() {
        CursorStateRecord sample = F3MetadataCodecSamples.minimalActive();
        assertThatThrownBy(() -> new CursorStateRecord(
                1,
                sample.projection(),
                sample.ownerSessionId(),
                sample.cursorName(),
                sample.cursorNameHash(),
                sample.cursorGeneration(),
                sample.lifecycle(),
                sample.mutationSequence(),
                sample.ackStateEpoch(),
                sample.lastProtectionAttemptId(),
                sample.markDeleteOffset(),
                Optional.empty(),
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                sample.createdAtMillis(),
                sample.updatedAtMillis(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metadataVersion");
    }

    @Test
    void rejectsTruncationAndTrailingBytes() {
        byte[] encoded = codec.encode(F3MetadataCodecSamples.fullActive());
        assertThatThrownBy(() -> codec.decode(java.util.Arrays.copyOf(encoded, encoded.length - 1)))
                .isInstanceOf(MetadataCodecException.class);
        byte[] trailing = java.util.Arrays.copyOf(encoded, encoded.length + 1);
        assertThatThrownBy(() -> codec.decode(trailing))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("trailing");
    }

    @Test
    void preservesSignedPositionValuesAndRemainingWords() {
        CursorStateRecord decoded = codec.decode(codec.encode(F3MetadataCodecSamples.fullActive()));
        assertThat(decoded.positionProperties())
                .containsEntry("a", Long.MAX_VALUE)
                .containsEntry("z", Long.MIN_VALUE);
        assertThat(decoded.inlinePartialAckOverrides().get(0).remainingWords())
                .containsExactly(-2L, 1L);
    }
}
