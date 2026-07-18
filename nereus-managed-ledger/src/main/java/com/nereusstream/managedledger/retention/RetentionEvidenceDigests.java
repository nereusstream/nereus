/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.managedledger.cursor.CursorRetentionView;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

final class RetentionEvidenceDigests {
    private RetentionEvidenceDigests() {
    }

    static Checksum candidate(
            StreamId streamId,
            StreamMetadataSnapshot head,
            CursorRetentionView cursor,
            RetentionPolicySnapshot policy,
            long cursorCut,
            long timeCut,
            long sizeCut,
            long candidateTrimOffset,
            List<RetentionStatsToken> tokens,
            long plannedAtMillis) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            text(digest, "nereus-retention-candidate-evidence-v1");
            text(digest, streamId.value());
            text(digest, head.metadata().state());
            text(digest, head.metadata().profile());
            number(digest, head.metadata().policyVersion());
            number(digest, head.metadataVersion());
            number(digest, head.committedEnd().committedEndOffset());
            number(digest, head.committedEnd().cumulativeSize());
            number(digest, head.committedEnd().commitVersion());
            number(digest, head.trim().trimOffset());
            text(digest, cursor.ownerSessionId());
            text(digest, cursor.lifecycle().name());
            number(digest, cursor.mutationSequence());
            number(digest, cursor.metadataVersion());
            number(digest, cursor.protectedFloorOffset());
            number(digest, cursor.lastCompletedTrimOffset());
            number(digest, policy.policyVersion());
            number(digest, policy.retentionTimeMillis());
            number(digest, policy.retentionSizeBytes());
            number(digest, cursorCut);
            number(digest, timeCut);
            number(digest, sizeCut);
            number(digest, candidateTrimOffset);
            number(digest, plannedAtMillis);
            number(digest, tokens.size());
            for (RetentionStatsToken token : tokens) {
                text(digest, token.key());
                number(digest, token.metadataVersion());
                text(digest, token.durableValueSha256().value());
            }
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(digest.digest()));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static void text(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(bytes.length)
                .array());
        digest.update(bytes);
    }

    private static void number(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(value)
                .array());
    }
}
