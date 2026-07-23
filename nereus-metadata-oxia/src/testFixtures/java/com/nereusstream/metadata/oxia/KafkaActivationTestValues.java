/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaPayloadMapping;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.util.List;

/** Canonical F9 activation values shared by deterministic and real-Oxia contracts. */
public final class KafkaActivationTestValues {
    public static final String KAFKA_CLUSTER = "kraft";

    private KafkaActivationTestValues() { }

    public static KafkaStorageProtocolActivationRecord activation(
            KafkaStorageActivationLifecycle lifecycle,
            long activatedAtMillis) {
        return new KafkaStorageProtocolActivationRecord(
                1,
                lifecycle.wireId(),
                KAFKA_CLUSTER,
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
                profiles(),
                "OBJECT_WAL_SYNC_OBJECT",
                bytes(1),
                bytes(2),
                1,
                100,
                3,
                1_000,
                activatedAtMillis,
                0);
    }

    public static KafkaBrokerCapabilityRecord capability(
            long heartbeatAtMillis,
            long expiresAtMillis) {
        return new KafkaBrokerCapabilityRecord(
                1,
                KAFKA_CLUSTER,
                1,
                11,
                "runtime-1",
                "4.3.0",
                "0.1.0-f9-dev",
                "21",
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
                1,
                profiles(),
                bytes(3),
                bytes(4),
                bytes(5),
                1_000,
                heartbeatAtMillis,
                expiresAtMillis,
                0);
    }

    public static KafkaStorageReadinessRecord readiness(
            long readinessEpoch,
            long kraftMetadataOffset,
            long createdAtMillis) {
        List<KafkaBrokerIdentity> brokers = List.of(
                new KafkaBrokerIdentity(1, 11),
                new KafkaBrokerIdentity(2, 12));
        return new KafkaStorageReadinessRecord(
                1,
                KAFKA_CLUSTER,
                readinessEpoch,
                kraftMetadataOffset,
                brokers,
                KafkaStorageReadinessRecord.brokerSetSha256(brokers),
                bytes(1),
                bytes(5),
                createdAtMillis,
                createdAtMillis + 30_000,
                0);
    }

    public static List<String> profiles() {
        return List.of("BOOKKEEPER_WAL_ONLY", "OBJECT_WAL_SYNC_OBJECT");
    }

    public static byte[] bytes(int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
