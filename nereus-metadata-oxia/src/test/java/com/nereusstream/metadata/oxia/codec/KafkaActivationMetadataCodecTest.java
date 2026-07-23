/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.KafkaActivationTestValues;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaPayloadMapping;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaActivationMetadataCodecTest {
    @Test
    void activationCapabilityAndReadinessRoundTripWithFrozenBytes() {
        KafkaStorageProtocolActivationRecord activation = KafkaActivationTestValues.activation(
                KafkaStorageActivationLifecycle.PREPARED, 0);
        KafkaBrokerCapabilityRecord capability = KafkaActivationTestValues.capability(1_100, 31_100);
        KafkaStorageReadinessRecord readiness = KafkaActivationTestValues.readiness(7, 101, 1_200);

        byte[] activationBytes = roundTrip(activation, KafkaStorageProtocolActivationRecord.class);
        byte[] capabilityBytes = roundTrip(capability, KafkaBrokerCapabilityRecord.class);
        byte[] readinessBytes = roundTrip(readiness, KafkaStorageReadinessRecord.class);

        assertThat(sha256(activationBytes))
                .isEqualTo("e59f6be4871be684ade4311f8ada9e036f41d9e34a2f937ffb01863eaad2d571");
        assertThat(sha256(capabilityBytes))
                .isEqualTo("7a67e8e2da0ed899b37cdc3fcf3cf40bdfee64d0d4801ed2994e332a7a9b896a");
        assertThat(sha256(readinessBytes))
                .isEqualTo("6ada42ad993900ea3448f0cca6ca35ab8e602e3912ec448b92e5930d2a67e024");
    }

    @Test
    void invalidLifecycleProfilesDigestAndHeartbeatFailClosed() {
        assertThatThrownBy(() -> KafkaActivationTestValues.activation(
                KafkaStorageActivationLifecycle.ACTIVE, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaStorageProtocolActivationRecord(
                1,
                KafkaStorageActivationLifecycle.PREPARED.wireId(),
                "kraft",
                1,
                1,
                2,
                1,
                KafkaPayloadMapping.KAFKA_RECORD_BATCH_V1.wireId(),
                1,
                2,
                2,
                1,
                1,
                List.of("OBJECT_WAL_SYNC_OBJECT", "BOOKKEEPER_WAL_ONLY"),
                "OBJECT_WAL_SYNC_OBJECT",
                KafkaActivationTestValues.bytes(1),
                KafkaActivationTestValues.bytes(2),
                1,
                100,
                3,
                1_000,
                0,
                0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sorted");
        assertThatThrownBy(() -> KafkaActivationTestValues.capability(31_100, 31_100))
                .isInstanceOf(IllegalArgumentException.class);

        KafkaStorageReadinessRecord valid = KafkaActivationTestValues.readiness(7, 101, 1_200);
        assertThatThrownBy(() -> new KafkaStorageReadinessRecord(
                valid.recordVersion(),
                valid.kafkaClusterId(),
                valid.readinessEpoch(),
                valid.kraftMetadataOffset(),
                valid.brokers(),
                KafkaActivationTestValues.bytes(99),
                valid.capabilitySha256(),
                valid.providerScopeSha256(),
                valid.createdAtMillis(),
                valid.expiresAtMillis(),
                0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical broker set");
    }

    private static <T> byte[] roundTrip(T value, Class<T> type) {
        byte[] first = KafkaMetadataCodecs.encodeEnvelope(value, type);
        byte[] second = KafkaMetadataCodecs.encodeEnvelope(value, type);
        assertThat(second).isEqualTo(first);
        assertThat(KafkaMetadataCodecs.decodeEnvelope(first, type)).isEqualTo(value);
        return first;
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
