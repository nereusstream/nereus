/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;

/** Closed NKC1 constants, limits, and deterministic identity rules. */
public final class KafkaCheckpointFormatV1 {
    public static final int FORMAT_VERSION = 1;
    public static final int MIN_READER_VERSION = 1;
    public static final int HEADER_ALLOW_OPTIONAL_SECTIONS_FLAG = 1;
    public static final int SECTION_REQUIRED_FLAG = 1;
    public static final int MAX_HEADER_BYTES = 1 << 20;
    public static final int MAX_SECTION_COUNT = 16;
    public static final int MAX_SECTION_BYTES = 256 << 20;
    public static final long MAX_OBJECT_BYTES = 1L << 30;
    public static final int MAX_STRING_BYTES = 64 << 10;
    public static final int TRAILER_BYTES = Long.BYTES + 32 + Integer.BYTES;
    public static final int SECTION_HEADER_BYTES = Short.BYTES * 2 + Integer.BYTES
            + Long.BYTES + Integer.BYTES + 32;
    public static final String CONTENT_TYPE = "application/vnd.nereus.kafka-checkpoint-v1";
    public static final String OBJECT_SUFFIX = ".nkc";
    static final byte[] MAGIC = "NKC1".getBytes(StandardCharsets.US_ASCII);
    private static final String ZERO_TOPIC_ID = "AAAAAAAAAAAAAAAAAAAAAA";

    private KafkaCheckpointFormatV1() { }

    public static ObjectKeyPrefix prefix(
            String nereusCluster, String kafkaClusterId, String topicId, int partitionId) {
        if (partitionId < 0) throw new IllegalArgumentException("partitionId must be non-negative");
        return new ObjectKeyPrefix(KeyComponentCodec.encodeComponent(text(nereusCluster, "nereusCluster"))
                + "/kafka/checkpoints/v1/"
                + KeyComponentCodec.encodeComponent(text(kafkaClusterId, "kafkaClusterId")) + "/"
                + canonicalTopicId(topicId) + "/" + partition(partitionId) + "/");
    }

    public static String attemptId(KafkaCheckpointHeader header, Checksum contentPolicySha256) {
        Objects.requireNonNull(header, "header");
        requireSha256(contentPolicySha256, "contentPolicySha256");
        return DeterministicIds.stableHashComponent(
                header.kafkaClusterId() + "\0" + header.topicId() + "\0" + header.partitionId()
                        + "\0" + header.streamId().value() + "\0" + header.checkpointOffset()
                        + "\0" + header.sourceCommitVersion() + "\0" + contentPolicySha256.value());
    }

    public static ObjectKey objectKey(
            String nereusCluster, KafkaCheckpointHeader header, Checksum contentPolicySha256) {
        String attempt = attemptId(header, contentPolicySha256);
        return new ObjectKey(prefix(
                        nereusCluster, header.kafkaClusterId(), header.topicId(), header.partitionId()).value()
                + KeyComponentCodec.encodeNonNegativeLong(header.checkpointOffset()) + "/"
                + attempt + OBJECT_SUFFIX);
    }

    public static ObjectId objectId(ObjectKey key) {
        return new ObjectId("kc1-" + DeterministicIds.stableHashComponent(
                Objects.requireNonNull(key, "key").value()));
    }

    public static ObjectKeyHash objectKeyHash(ObjectKey key) {
        return ObjectKeyHash.from(Objects.requireNonNull(key, "key"));
    }

    public static String canonicalTopicId(String value) {
        String exact = text(value, "topicId");
        if (exact.length() != 22 || exact.equals(ZERO_TOPIC_ID)) {
            throw new IllegalArgumentException("topicId must be a non-zero canonical Kafka UUID");
        }
        try {
            byte[] decoded = Base64.getUrlDecoder().decode(exact + "==");
            if (decoded.length != 16
                    || !Base64.getUrlEncoder().withoutPadding().encodeToString(decoded).equals(exact)) {
                throw new IllegalArgumentException("topicId must be a canonical Kafka UUID");
            }
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("topicId must be a canonical Kafka UUID", failure);
        }
        return exact;
    }

    static byte[] topicIdBytes(String value) {
        return Base64.getUrlDecoder().decode(canonicalTopicId(value) + "==");
    }

    static String topicId(byte[] value) {
        if (value.length != 16) throw new IllegalArgumentException("topic UUID must contain 16 bytes");
        return canonicalTopicId(Base64.getUrlEncoder().withoutPadding().encodeToString(value));
    }

    static Checksum requireSha256(Checksum value, String name) {
        Objects.requireNonNull(value, name);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return value;
    }

    private static String partition(int partitionId) {
        return String.format(Locale.ROOT, "%010d", partitionId);
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > MAX_STRING_BYTES) {
            throw new IllegalArgumentException(name + " must be nonblank and bounded");
        }
        return value;
    }
}
