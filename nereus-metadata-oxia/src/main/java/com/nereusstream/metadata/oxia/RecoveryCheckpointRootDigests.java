/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Canonical digest of the complete ordered recovery-checkpoint reference set. */
public final class RecoveryCheckpointRootDigests {
    private RecoveryCheckpointRootDigests() {
    }

    public static Checksum checkpointSetSha256(
            List<RecoveryCheckpointReferenceRecord> references) {
        List<RecoveryCheckpointReferenceRecord> values = List.copyOf(
                Objects.requireNonNull(references, "references"));
        Digest digest = new Digest();
        digest.text("nereus-recovery-checkpoint-set-v1");
        digest.integer(values.size());
        for (RecoveryCheckpointReferenceRecord value : values) {
            digest.longValue(value.checkpointSequence());
            digest.text(value.checkpointAttemptId());
            digest.longValue(value.coveredStartOffset());
            digest.longValue(value.coveredEndOffset());
            digest.longValue(value.firstCommitVersion());
            digest.longValue(value.lastCommitVersion());
            digest.longValue(value.cumulativeSizeAtStart());
            digest.longValue(value.cumulativeSizeAtEnd());
            digest.text(value.firstCommitId());
            digest.text(value.lastCommitId());
            digest.text(value.sourceHeadCommitId());
            digest.longValue(value.sourceHeadCommitVersion());
            digest.text(value.projectionIdentitySha256());
            digest.text(value.objectId());
            digest.text(value.objectKey());
            digest.text(value.objectKeyHash());
            digest.longValue(value.objectLength());
            digest.text(value.storageCrc32c());
            digest.text(value.contentSha256());
            digest.integer(value.commitEntryCount());
            digest.integer(value.publicationCount());
        }
        return digest.finish();
    }

    private static final class Digest {
        private final MessageDigest digest;

        private Digest() {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
        }

        private void text(String value) {
            byte[] bytes = Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8);
            integer(bytes.length);
            digest.update(bytes);
        }

        private void integer(int value) {
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
        }

        private void longValue(long value) {
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
        }

        private Checksum finish() {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(digest.digest()));
        }
    }
}
