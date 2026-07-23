/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.keys.KeyComponentCodec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;

/** Strict durable key and partition routing for native Kafka metadata. */
public final class KafkaPartitionKeyspace {
    public static final int REGISTRY_SHARDS = 64;

    private final String nereusCluster;
    private final String kafkaClusterId;
    private final String prefix;

    public KafkaPartitionKeyspace(String nereusCluster, String kafkaClusterId) {
        this.nereusCluster = text(nereusCluster, "nereusCluster");
        this.kafkaClusterId = text(kafkaClusterId, "kafkaClusterId");
        this.prefix = new OxiaKeyspace(nereusCluster).prefix()
                + "/kafka/" + KeyComponentCodec.encodeComponent(kafkaClusterId);
    }

    public String nereusCluster() { return nereusCluster; }
    public String kafkaClusterId() { return kafkaClusterId; }
    public String prefix() { return prefix; }

    public String activationKey() { return prefix + "/activation"; }
    public String readinessKey() { return prefix + "/readiness"; }

    public String capabilityKey(int brokerId, long brokerEpoch) {
        if (brokerId < 0 || brokerEpoch < 0) {
            throw new IllegalArgumentException("broker identity must be non-negative");
        }
        return prefix + "/capabilities/" + partitionDigits(brokerId) + "/"
                + KeyComponentCodec.encodeNonNegativeLong(brokerEpoch);
    }

    public String bindingRootKey(KafkaPartitionId id) {
        KafkaPartitionId exact = requireIdentity(id);
        return prefix + "/partitions/" + KeyComponentCodec.encodeComponent(exact.topicId())
                + "/" + partitionDigits(exact.partitionId()) + "/root";
    }

    public PartitionKey bindingPartitionKey(KafkaPartitionId id) {
        return new PartitionKey("kafka-binding-v1-" + identitySha256(requireIdentity(id)));
    }

    public int registryShard(KafkaPartitionId id) {
        byte[] digest = identityDigest(requireIdentity(id));
        return Byte.toUnsignedInt(digest[0]) >>> 2;
    }

    public PartitionKey registryPartitionKey(int shard) {
        requireShard(shard);
        return new PartitionKey("kafka-registry-v1-" + String.format(Locale.ROOT, "%02d", shard));
    }

    public String registryShardPrefix(int shard) {
        requireShard(shard);
        return prefix + "/registry/" + String.format(Locale.ROOT, "%02d", shard);
    }

    public String registryKey(KafkaPartitionId id) {
        KafkaPartitionId exact = requireIdentity(id);
        return registryShardPrefix(registryShard(exact)) + "/"
                + KeyComponentCodec.encodeComponent(exact.topicId()) + "/"
                + partitionDigits(exact.partitionId());
    }

    public KafkaPartitionId parseBindingRootKey(String key) {
        String family = prefix + "/partitions/";
        String supplied = scoped(key, family, "binding root");
        String suffix = supplied.substring(family.length());
        String[] components = suffix.split("/", -1);
        if (components.length != 3 || !components[2].equals("root")) {
            throw new IllegalArgumentException("binding root key has an unknown depth");
        }
        KafkaPartitionId id = new KafkaPartitionId(
                kafkaClusterId,
                KeyComponentCodec.decodeComponent(components[0]),
                parsePartition(components[1]));
        if (!bindingRootKey(id).equals(supplied)) {
            throw new IllegalArgumentException("binding root key is not canonical");
        }
        return id;
    }

    public KafkaPartitionId parseRegistryKey(int expectedShard, String key) {
        requireShard(expectedShard);
        String family = registryShardPrefix(expectedShard) + "/";
        String supplied = scoped(key, family, "registry");
        String suffix = supplied.substring(family.length());
        String[] components = suffix.split("/", -1);
        if (components.length != 2) {
            throw new IllegalArgumentException("registry key has an unknown depth");
        }
        KafkaPartitionId id = new KafkaPartitionId(
                kafkaClusterId,
                KeyComponentCodec.decodeComponent(components[0]),
                parsePartition(components[1]));
        if (registryShard(id) != expectedShard || !registryKey(id).equals(supplied)) {
            throw new IllegalArgumentException("registry key is not canonical for its shard");
        }
        return id;
    }

    public String identitySha256(KafkaPartitionId id) {
        return HexFormat.of().formatHex(identityDigest(requireIdentity(id)));
    }

    public String bindingRootKeySha256(KafkaPartitionId id) {
        return HexFormat.of().formatHex(sha256().digest(
                bindingRootKey(id).getBytes(StandardCharsets.UTF_8)));
    }

    private KafkaPartitionId requireIdentity(KafkaPartitionId id) {
        KafkaPartitionId exact = Objects.requireNonNull(id, "id");
        if (!exact.kafkaClusterId().equals(kafkaClusterId)) {
            throw new IllegalArgumentException("Kafka partition identity belongs to another cluster");
        }
        return exact;
    }

    private byte[] identityDigest(KafkaPartitionId id) {
        MessageDigest digest = sha256();
        digest.update(id.kafkaClusterId().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
        digest.update(id.topicId().getBytes(StandardCharsets.US_ASCII));
        digest.update((byte) 0);
        digest.update(Integer.toString(id.partitionId()).getBytes(StandardCharsets.US_ASCII));
        return digest.digest();
    }

    private String scoped(String key, String family, String name) {
        String supplied = text(key, name + "Key");
        if (!supplied.startsWith(family)) {
            throw new IllegalArgumentException(name + " key belongs to another cluster or family");
        }
        return supplied;
    }

    private static int parsePartition(String encoded) {
        if (encoded.length() != 10) throw new IllegalArgumentException("partition has wrong width");
        for (int index = 0; index < encoded.length(); index++) {
            if (encoded.charAt(index) < '0' || encoded.charAt(index) > '9') {
                throw new IllegalArgumentException("partition is not decimal");
            }
        }
        try {
            int value = Integer.parseInt(encoded);
            if (!partitionDigits(value).equals(encoded)) {
                throw new IllegalArgumentException("partition is not canonical");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("partition exceeds int range", failure);
        }
    }

    private static String partitionDigits(int value) {
        if (value < 0) throw new IllegalArgumentException("partition must be non-negative");
        return String.format(Locale.ROOT, "%010d", value);
    }

    private static void requireShard(int shard) {
        if (shard < 0 || shard >= REGISTRY_SHARDS) {
            throw new IllegalArgumentException("registry shard must be in [0,63]");
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
