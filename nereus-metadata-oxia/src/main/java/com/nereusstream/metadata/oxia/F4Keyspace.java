/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;

/** Durable key and partition builder for the Phase 4 protocol. */
public final class F4Keyspace {
    public static final int MATERIALIZATION_REGISTRY_SHARDS = 64;
    public static final int PHYSICAL_OBJECT_SHARDS = 256;
    private static final String BASE32_ALPHABET = "abcdefghijklmnopqrstuvwxyz234567";

    private final OxiaKeyspace oxia;

    public F4Keyspace(String cluster) {
        this.oxia = new OxiaKeyspace(cluster);
    }

    public String cluster() {
        return oxia.cluster();
    }

    public int materializationRegistryShard(StreamId streamId) {
        byte[] hash = sha256(Objects.requireNonNull(streamId, "streamId").value());
        return Byte.toUnsignedInt(hash[hash.length - 1]) & (MATERIALIZATION_REGISTRY_SHARDS - 1);
    }

    public PartitionKey materializationRegistryPartitionKey(int shard) {
        requireShard(shard, MATERIALIZATION_REGISTRY_SHARDS, "materialization registry shard");
        return new PartitionKey("materialization-registry-v1-" + twoDigit(shard));
    }

    public String materializationRegistryKey(StreamId streamId) {
        int shard = materializationRegistryShard(streamId);
        return materializationRegistryPrefix(shard)
                + "/"
                + KeyComponentCodec.encodeComponent(streamId.value());
    }

    public String materializationRegistryPrefix(int shard) {
        requireShard(shard, MATERIALIZATION_REGISTRY_SHARDS, "materialization registry shard");
        return oxia.prefix() + "/materialization/v1/stream-registry/" + twoDigit(shard);
    }

    public String taskKey(StreamId streamId, String taskId) {
        return taskPrefix(streamId) + "/" + KeyComponentCodec.encodeComponent(requireText(taskId, "taskId"));
    }

    public String taskPrefix(StreamId streamId) {
        return streamPrefix(streamId) + "/materialization/v1/tasks";
    }

    public String checkpointKey(StreamId streamId, String policyId, long policyVersion) {
        return checkpointPrefix(streamId)
                + "/"
                + KeyComponentCodec.encodeComponent(requireText(policyId, "policyId"))
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(requirePositive(policyVersion, "policyVersion"));
    }

    public String checkpointPrefix(StreamId streamId) {
        return streamPrefix(streamId) + "/materialization/v1/checkpoints";
    }

    public String generationSequenceKey(StreamId streamId, ReadView view) {
        return streamPrefix(streamId)
                + "/materialization/v1/generation-sequences/"
                + twoDigit(Objects.requireNonNull(view, "view").wireId());
    }

    public String generationIndexKey(StreamId streamId, ReadView view, long offsetEnd, long generation) {
        return generationIndexPrefix(streamId, view)
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(offsetEnd)
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(generation);
    }

    public String generationIndexPrefix(StreamId streamId, ReadView view) {
        Objects.requireNonNull(view, "view");
        if (view == ReadView.COMMITTED) {
            return streamPrefix(streamId) + "/offset-index";
        }
        return streamPrefix(streamId) + "/views/v1/topic-compacted/offset-index";
    }

    /** Strict restart router for a journaled generation-index key. */
    public GenerationCandidateKeyIdentity parseGenerationIndexKey(String suppliedKey) {
        String key = requireText(suppliedKey, "generationIndexKey");
        String streamsPrefix = oxia.prefix() + "/streams/";
        if (!key.startsWith(streamsPrefix)) {
            throw new IllegalArgumentException("generation index key belongs to another cluster namespace");
        }
        String remainder = key.substring(streamsPrefix.length());
        int streamEnd = remainder.indexOf('/');
        if (streamEnd <= 0) {
            throw new IllegalArgumentException("generation index key is missing its stream component");
        }
        StreamId stream = new StreamId(KeyComponentCodec.decodeComponent(
                remainder.substring(0, streamEnd)));
        String suffix = remainder.substring(streamEnd + 1);
        String committedPrefix = "offset-index/";
        String topicCompactedPrefix = "views/v1/topic-compacted/offset-index/";
        ReadView view;
        String identity;
        if (suffix.startsWith(committedPrefix)) {
            view = ReadView.COMMITTED;
            identity = suffix.substring(committedPrefix.length());
        } else if (suffix.startsWith(topicCompactedPrefix)) {
            view = ReadView.TOPIC_COMPACTED;
            identity = suffix.substring(topicCompactedPrefix.length());
        } else {
            throw new IllegalArgumentException("generation index key has an unknown view namespace");
        }
        int separator = identity.indexOf('/');
        if (separator <= 0
                || separator == identity.length() - 1
                || identity.indexOf('/', separator + 1) >= 0) {
            throw new IllegalArgumentException("generation index key has an invalid identity depth");
        }
        long offsetEnd = KeyComponentCodec.decodeNonNegativeLong(
                identity.substring(0, separator));
        long generation = KeyComponentCodec.decodeNonNegativeLong(
                identity.substring(separator + 1));
        GenerationCandidateKeyIdentity decoded = new GenerationCandidateKeyIdentity(
                stream, view, offsetEnd, generation);
        if (!generationIndexKey(stream, view, offsetEnd, generation).equals(key)) {
            throw new IllegalArgumentException("generation index key is not canonical");
        }
        return decoded;
    }

    public String generationIndexScanFrom(StreamId streamId, ReadView view, long offsetEndInclusive) {
        return generationIndexPrefix(streamId, view)
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(offsetEndInclusive)
                + "/";
    }

    public String generationIndexScanToAfterEnd(StreamId streamId, ReadView view, long offsetEndInclusive) {
        if (offsetEndInclusive == Long.MAX_VALUE) {
            return generationIndexPrefix(streamId, view) + "/~/";
        }
        return generationIndexScanFrom(streamId, view, offsetEndInclusive + 1);
    }

    public String recoveryRootKey(StreamId streamId) {
        return streamPrefix(streamId) + "/recovery/v1/root";
    }

    public String retentionStatsKey(StreamId streamId, long offsetEnd, long commitVersion) {
        return retentionStatsPrefix(streamId)
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(offsetEnd)
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(commitVersion);
    }

    public String retentionStatsPrefix(StreamId streamId) {
        return streamPrefix(streamId) + "/retention/v1/range-stats";
    }

    public String retentionStatsScanFrom(StreamId streamId, long offsetEndInclusive) {
        return retentionStatsPrefix(streamId)
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(offsetEndInclusive)
                + "/";
    }

    public String retentionStatsScanToAfterEnd(StreamId streamId, long offsetEndInclusive) {
        if (offsetEndInclusive == Long.MAX_VALUE) {
            return retentionStatsPrefix(streamId) + "/~/";
        }
        return retentionStatsScanFrom(streamId, offsetEndInclusive + 1);
    }

    public String generationProtocolActivationKey() {
        return oxia.prefix() + "/capabilities/generation-v1/activation";
    }

    public PartitionKey generationProtocolActivationPartitionKey() {
        return new PartitionKey("generation-protocol-v1");
    }

    public int physicalObjectShard(ObjectKeyHash object) {
        String hash = Objects.requireNonNull(object, "object").value();
        int first = BASE32_ALPHABET.indexOf(hash.charAt(0));
        int second = BASE32_ALPHABET.indexOf(hash.charAt(1));
        if (first < 0 || second < 0) {
            throw new IllegalArgumentException("object hash is not canonical base32");
        }
        return (first << 3) | (second >>> 2);
    }

    public PartitionKey physicalObjectPartitionKey(ObjectKeyHash object) {
        return new PartitionKey("physical-object-v1-" + threeDigit(physicalObjectShard(object)));
    }

    public String physicalRootKey(ObjectKeyHash object) {
        return physicalRootShardPrefix(physicalObjectShard(object)) + "/" + object.value();
    }

    public String physicalRootShardPrefix(int shard) {
        requireShard(shard, PHYSICAL_OBJECT_SHARDS, "physical object shard");
        return physicalShardPrefix(shard) + "/roots";
    }

    public String readerLeaseKey(ObjectKeyHash object, String processRunId) {
        return readerPrefix(object)
                + "/"
                + KeyComponentCodec.encodeComponent(requireText(processRunId, "processRunId"));
    }

    public String protectionKey(
            ObjectKeyHash object,
            ObjectProtectionType type,
            String referenceId) {
        Objects.requireNonNull(type, "type");
        return protectionPrefix(object)
                + "/"
                + twoDigit(type.wireId())
                + "/"
                + KeyComponentCodec.encodeComponent(requireText(referenceId, "referenceId"));
    }

    public String protectionPrefix(ObjectKeyHash object) {
        return physicalObjectPrefix(object) + "/protections";
    }

    public String gcRetirementManifestKey(ObjectKeyHash object, String gcAttemptId) {
        return gcRetirementAttemptPrefix(object, gcAttemptId) + "/manifest";
    }

    public String gcRetirementProtectionKey(
            ObjectKeyHash object, String gcAttemptId, String protectionKey) {
        return gcRetirementProtectionPrefix(object, gcAttemptId)
                + "/"
                + sha256Hex("protection\0" + requireText(protectionKey, "protectionKey"));
    }

    public String gcRetirementProtectionPrefix(ObjectKeyHash object, String gcAttemptId) {
        return gcRetirementAttemptPrefix(object, gcAttemptId) + "/protections";
    }

    public String gcRetirementRemovalKey(
            ObjectKeyHash object, String gcAttemptId, String removalKey) {
        return gcRetirementRemovalPrefix(object, gcAttemptId)
                + "/"
                + sha256Hex("removal\0" + requireText(removalKey, "removalKey"));
    }

    public String gcRetirementRemovalPrefix(ObjectKeyHash object, String gcAttemptId) {
        return gcRetirementAttemptPrefix(object, gcAttemptId) + "/removals";
    }

    public String gcRetirementAttemptPrefix(ObjectKeyHash object, String gcAttemptId) {
        return physicalObjectPrefix(object)
                + "/gc-retirement/"
                + KeyComponentCodec.encodeComponent(requireBase32Id(gcAttemptId, "gcAttemptId"));
    }

    public String readerPrefix(ObjectKeyHash object) {
        return physicalObjectPrefix(object) + "/readers";
    }

    public PartitionKey streamPartitionKey(StreamId streamId) {
        return oxia.streamPartitionKey(streamId);
    }

    private String streamPrefix(StreamId streamId) {
        Objects.requireNonNull(streamId, "streamId");
        return oxia.prefix() + "/streams/" + KeyComponentCodec.encodeComponent(streamId.value());
    }

    private String physicalObjectPrefix(ObjectKeyHash object) {
        Objects.requireNonNull(object, "object");
        return physicalShardPrefix(physicalObjectShard(object)) + "/objects/" + object.value();
    }

    private String physicalShardPrefix(int shard) {
        return oxia.prefix() + "/physical-objects/v1/" + threeDigit(shard);
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    private static String sha256Hex(String value) {
        return HexFormat.of().formatHex(sha256(value));
    }

    private static String twoDigit(int value) {
        if (value < 0 || value > 99) {
            throw new IllegalArgumentException("value cannot be encoded in two digits");
        }
        return String.format(Locale.ROOT, "%02d", value);
    }

    private static String threeDigit(int value) {
        if (value < 0 || value > 999) {
            throw new IllegalArgumentException("value cannot be encoded in three digits");
        }
        return String.format(Locale.ROOT, "%03d", value);
    }

    private static void requireShard(int shard, int count, String name) {
        if (shard < 0 || shard >= count) {
            throw new IllegalArgumentException(name + " must be in [0, " + (count - 1) + "]");
        }
    }

    private static long requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    private static String requireBase32Id(String value, String name) {
        String exact = requireText(value, name);
        if (exact.length() < 26 || exact.length() > 128) {
            throw new IllegalArgumentException(name + " must encode at least 128 bits and be bounded");
        }
        for (int index = 0; index < exact.length(); index++) {
            char character = exact.charAt(index);
            if (!((character >= 'a' && character <= 'z')
                    || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException(
                        name + " must be lowercase base32 without padding");
            }
        }
        return exact;
    }
}
