/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Closed NRC1 constants, hard limits, and deterministic object identity rules. */
public final class RecoveryCheckpointFormatV1 {
    public static final int MAJOR_VERSION = 1;
    public static final int FLAGS = 0;
    public static final int COMMIT_DIRECTORY_STRIDE = 256;
    public static final int MAX_STRING_BYTES = 16 << 10;
    public static final int MAX_EMBEDDED_RECORD_BYTES = 64 << 10;
    public static final int MAX_ENTRY_COUNT = 1_000_000;
    public static final int MAX_PUBLICATION_COUNT = 1_000_000;
    public static final int MAX_PUBLICATION_REFS_PER_ENTRY = 8;
    public static final int MAX_PUBLICATION_SCAN_PAGE_SIZE = 1_000;
    public static final int MAX_DIRECTORY_BYTES = 32 << 20;
    public static final int MAX_HEADER_BYTES = 128 << 10;
    public static final int MAX_RECORD_BYTES = 128 << 10;
    public static final int FOOTER_BYTES = 72;
    public static final long MAX_OBJECT_BYTES = 1L << 30;
    public static final int HASH_READ_CHUNK_BYTES = 1 << 20;
    public static final String CONTENT_TYPE = "application/vnd.nereus.recovery-checkpoint-v1";

    static final byte[] HEADER_MAGIC = "NRC1".getBytes(StandardCharsets.US_ASCII);
    static final byte[] FOOTER_MAGIC = "NRF1".getBytes(StandardCharsets.US_ASCII);

    private RecoveryCheckpointFormatV1() {
    }

    public static ObjectKeyPrefix prefix(String cluster) {
        return new ObjectKeyPrefix(KeyComponentCodec.encodeComponent(
                        RecoveryCheckpointValidation.requireText(cluster, "cluster"))
                + "/recovery-checkpoints/v1/");
    }

    public static ObjectKey objectKey(RecoveryCheckpointWriteRequest request, Checksum contentSha256) {
        Objects.requireNonNull(request, "request");
        RecoveryCheckpointValidation.requireSha256(contentSha256, "contentSha256");
        return new ObjectKey(KeyComponentCodec.encodeComponent(request.cluster())
                + "/recovery-checkpoints/v1/"
                + KeyComponentCodec.encodeComponent(request.streamId().value())
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(request.checkpointSequence())
                + "-"
                + contentSha256.value()
                + "-"
                + request.checkpointAttemptId()
                + ".nrc");
    }

    public static ObjectId objectId(ObjectKey objectKey) {
        Objects.requireNonNull(objectKey, "objectKey");
        return new ObjectId("rc1-" + DeterministicIds.stableHashComponent(objectKey.value()));
    }

    public static ObjectKeyHash objectKeyHash(ObjectKey objectKey) {
        return ObjectKeyHash.from(Objects.requireNonNull(objectKey, "objectKey"));
    }

    /** Strict ownerless inverse used only by complete object inventory. */
    public static ParsedRecoveryCheckpointKey parseObjectKey(
            String cluster, ObjectKey objectKey) {
        Objects.requireNonNull(objectKey, "objectKey");
        String exactPrefix = prefix(cluster).value();
        if (!objectKey.value().startsWith(exactPrefix)) {
            throw new IllegalArgumentException("recovery checkpoint key is outside the cluster prefix");
        }
        String[] components = objectKey.value().substring(exactPrefix.length()).split("/", -1);
        if (components.length != 2 || !components[1].endsWith(".nrc")) {
            throw new IllegalArgumentException("recovery checkpoint key path is not canonical");
        }
        String streamValue = KeyComponentCodec.decodeComponent(components[0]);
        StreamId streamId = new StreamId(streamValue);
        String filename = components[1].substring(0, components[1].length() - ".nrc".length());
        String[] identity = filename.split("-", -1);
        if (identity.length != 3) {
            throw new IllegalArgumentException("recovery checkpoint filename is not canonical");
        }
        long sequence = KeyComponentCodec.decodeNonNegativeLong(identity[0]);
        if (sequence <= 0) {
            throw new IllegalArgumentException("recovery checkpoint sequence must be positive");
        }
        Checksum contentSha256 = new Checksum(ChecksumType.SHA256, identity[1]);
        RecoveryCheckpointValidation.requireSha256(contentSha256, "contentSha256");
        String attempt = RecoveryCheckpointValidation.requireBase32Id(
                identity[2], "checkpointAttemptId");
        String canonical = exactPrefix
                + KeyComponentCodec.encodeComponent(streamId.value())
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(sequence)
                + "-"
                + contentSha256.value()
                + "-"
                + attempt
                + ".nrc";
        if (!canonical.equals(objectKey.value())) {
            throw new IllegalArgumentException("recovery checkpoint key is not canonical");
        }
        return new ParsedRecoveryCheckpointKey(
                streamId,
                sequence,
                contentSha256,
                attempt,
                objectId(objectKey));
    }

    static Checksum sha256(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(newSha256().digest(bytes)));
    }

    static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    public record ParsedRecoveryCheckpointKey(
            StreamId streamId,
            long checkpointSequence,
            Checksum contentSha256,
            String checkpointAttemptId,
            ObjectId objectId) {
        public ParsedRecoveryCheckpointKey {
            Objects.requireNonNull(streamId, "streamId");
            if (checkpointSequence <= 0) {
                throw new IllegalArgumentException("checkpointSequence must be positive");
            }
            contentSha256 = RecoveryCheckpointValidation.requireSha256(
                    contentSha256, "contentSha256");
            checkpointAttemptId = RecoveryCheckpointValidation.requireBase32Id(
                    checkpointAttemptId, "checkpointAttemptId");
            Objects.requireNonNull(objectId, "objectId");
        }
    }
}
