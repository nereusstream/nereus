/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.ManagedLedgerCursorProtocol;
import com.nereusstream.metadata.oxia.ProjectionCreateRequest;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class F3ProjectionActivationGoldenTest {
    @Test
    void activatedProjectionHasNewGoldenAndLockedF2PropertyDecoderRejectsIt() throws IOException {
        TopicProjectionRecord base = F2MetadataCodecSamples.topic(0);
        TopicProjectionRecord activated = new TopicProjectionRecord(
                base.managedLedgerName(),
                base.managedLedgerNameHash(),
                base.storageClassBindingGeneration(),
                base.incarnation(),
                base.streamName(),
                base.streamId(),
                base.storageClass(),
                base.storageProfile(),
                base.virtualLedgerId(),
                base.positionMappingVersion(),
                base.payloadMapping(),
                base.facadeState(),
                ManagedLedgerCursorProtocol.activate(base.properties()),
                base.createdAtMillis(),
                base.stateVersion(),
                0);
        Properties golden = new Properties();
        try (var input = F3ProjectionActivationGoldenTest.class.getResourceAsStream(
                "f3-projection-activation-golden.properties")) {
            golden.load(new InputStreamReader(input, StandardCharsets.UTF_8));
        }
        String encoded = F2MetadataCodecs.envelopeHex(activated, TopicProjectionRecord.class);
        assertThat(encoded).isEqualTo(golden.getProperty("projection.activated"));
        assertThat(F2MetadataCodecs.decodeEnvelope(
                java.util.HexFormat.of().parseHex(encoded), TopicProjectionRecord.class))
                .isEqualTo(activated);
        assertThatThrownBy(() -> ProjectionCreateRequest.canonicalProperties(activated.properties()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
    }
}
