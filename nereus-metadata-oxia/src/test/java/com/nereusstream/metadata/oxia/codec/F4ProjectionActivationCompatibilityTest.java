/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.ManagedLedgerCursorProtocol;
import com.nereusstream.metadata.oxia.ManagedLedgerGenerationProtocol;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class F4ProjectionActivationCompatibilityTest {
    @Test
    void oldF3DecoderRejectsActivatedTopicBeforeReturningALedger() {
        TopicProjectionRecord base = F2MetadataCodecSamples.topic(0);
        Map<String, String> f4Properties = ManagedLedgerGenerationProtocol.activate(
                ManagedLedgerCursorProtocol.activate(base.properties()));
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
                f4Properties,
                base.createdAtMillis(),
                base.stateVersion(),
                0);
        byte[] durableEnvelope = F2MetadataCodecs.encodeEnvelope(
                activated, TopicProjectionRecord.class);
        AtomicInteger returnedLedgers = new AtomicInteger();

        assertThatThrownBy(() -> frozenF3DecodeThenOpen(
                        durableEnvelope, returnedLedgers::incrementAndGet))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ManagedLedgerGenerationProtocol.PROPERTY);
        assertThat(returnedLedgers).hasValue(0);
    }

    /**
     * Freezes the F3-only property decoder: cursor V1 was known, while every later
     * {@code nereus.*} key was a fail-closed minimum-reader fence.
     */
    private static void frozenF3DecodeThenOpen(
            byte[] durableEnvelope, Runnable returnLedger) {
        TopicProjectionRecord decoded = F2MetadataCodecs.decodeEnvelope(
                durableEnvelope, TopicProjectionRecord.class);
        decoded.properties().forEach((key, value) -> {
            if (key.equals(ManagedLedgerCursorProtocol.PROPERTY)) {
                if (!ManagedLedgerCursorProtocol.VERSION_1.equals(value)) {
                    throw new IllegalArgumentException(
                            "old F3 decoder rejects cursor protocol " + value);
                }
                return;
            }
            if (key.startsWith("nereus.")) {
                throw new IllegalArgumentException(
                        "old F3 decoder rejects reserved property " + key);
            }
        });
        returnLedger.run();
    }
}
