/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class KafkaPartitionKeyspaceTest {
    @Test
    void canonicalBindingAndRegistryKeysRoundTrip() {
        KafkaPartitionKeyspace keys = new KafkaPartitionKeyspace("nereus/cluster", "kraft-cluster");
        KafkaPartitionId id = new KafkaPartitionId("kraft-cluster", topicId(17), 3);
        KafkaBrokerIdentity broker = new KafkaBrokerIdentity(12, 34);

        assertThat(keys.parseBindingRootKey(keys.bindingRootKey(id))).isEqualTo(id);
        assertThat(keys.parseRegistryKey(keys.registryShard(id), keys.registryKey(id))).isEqualTo(id);
        assertThat(keys.parseCapabilityKey(
                keys.capabilityKey(broker.brokerId(), broker.brokerEpoch()))).isEqualTo(broker);
        assertThat(keys.bindingRootKey(id)).contains("/0000000003/root");
        assertThat(keys.identitySha256(id)).hasSize(64);
        assertThat(keys.activationPartitionKey().value()).isEqualTo(
                "kafka-activation-v1-178580dc89c7d27312ce0a99659a5ce6a8d0069a51e0b7c199f4ae8846e5a636");
    }

    @Test
    void parserRejectsWrongClusterDepthAndAlternateDecimal() {
        KafkaPartitionKeyspace keys = new KafkaPartitionKeyspace("nereus", "kraft-a");
        KafkaPartitionKeyspace other = new KafkaPartitionKeyspace("nereus", "kraft-b");
        KafkaPartitionId id = new KafkaPartitionId("kraft-a", topicId(4), 12);
        String canonical = keys.bindingRootKey(id);

        assertThatThrownBy(() -> other.parseBindingRootKey(canonical)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> keys.parseBindingRootKey(canonical + "/extra"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> keys.parseBindingRootKey(canonical.replace("0000000012", "12")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> keys.parseCapabilityKey(keys.capabilityKey(1, 2) + "/extra"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> keys.parseCapabilityKey(keys.capabilityKey(1, 2)
                .replace("0000000001", "1")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaPartitionId("kraft-a", "AAAAAAAAAAAAAAAAAAAAAA", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deterministicHashingReachesEveryRegistryShard() {
        KafkaPartitionKeyspace keys = new KafkaPartitionKeyspace("nereus", "kraft");
        Set<Integer> shards = new HashSet<>();
        for (int value = 1; value <= 20_000 && shards.size() < 64; value++) {
            shards.add(keys.registryShard(new KafkaPartitionId("kraft", topicId(value), value % 17)));
        }
        assertThat(shards).hasSize(64);
    }

    public static String topicId(int value) {
        byte[] bytes = new byte[16];
        bytes[12] = (byte) (value >>> 24);
        bytes[13] = (byte) (value >>> 16);
        bytes[14] = (byte) (value >>> 8);
        bytes[15] = (byte) value;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
