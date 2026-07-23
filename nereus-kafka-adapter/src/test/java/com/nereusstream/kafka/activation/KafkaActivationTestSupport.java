/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import java.time.Duration;
import java.util.Set;

final class KafkaActivationTestSupport {
    static final String CLUSTER = "kraft-cluster";
    static final KafkaBrokerIdentity BROKER = new KafkaBrokerIdentity(1, 11);

    private KafkaActivationTestSupport() { }

    static KafkaBrokerCapabilitySpecification specification(int providerSeed) {
        return new KafkaBrokerCapabilitySpecification(
                CLUSTER,
                BROKER,
                "runtime-1",
                "4.3.0",
                "nereus-test",
                "21",
                Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT),
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                bytes(1),
                bytes(2),
                bytes(providerSeed),
                Duration.ofMillis(10),
                Duration.ofMillis(100));
    }

    static byte[] bytes(int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) value[index] = (byte) (seed + index);
        return value;
    }
}
