/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.Objects;

/** Ephemeral, authority-bound logical-trim candidate. */
public record RetentionCandidate(
        StreamId streamId,
        long currentTrimOffset,
        long committedEndOffset,
        long cursorCut,
        long timeCut,
        long sizeCut,
        long candidateTrimOffset,
        long streamHeadMetadataVersion,
        long cursorRetentionMetadataVersion,
        long policyVersion,
        List<RetentionStatsToken> statsTokens,
        Checksum evidenceSha256,
        long plannedAtMillis) {
    public static final int MAX_STATS_TOKENS = 4_096;

    public RetentionCandidate {
        Objects.requireNonNull(streamId, "streamId");
        if (currentTrimOffset < 0
                || committedEndOffset < currentTrimOffset
                || cursorCut < currentTrimOffset
                || cursorCut > committedEndOffset
                || timeCut < currentTrimOffset
                || timeCut > cursorCut
                || sizeCut < currentTrimOffset
                || sizeCut > cursorCut
                || candidateTrimOffset <= currentTrimOffset
                || candidateTrimOffset > cursorCut
                || candidateTrimOffset != Math.min(
                        cursorCut, Math.max(timeCut, sizeCut))) {
            throw new IllegalArgumentException(
                    "retention candidate offsets or formula are invalid");
        }
        if (streamHeadMetadataVersion < 0
                || cursorRetentionMetadataVersion < 0
                || policyVersion < 0
                || plannedAtMillis < 0) {
            throw new IllegalArgumentException(
                    "retention candidate versions/time must be non-negative");
        }
        statsTokens = List.copyOf(
                Objects.requireNonNull(statsTokens, "statsTokens"));
        if (statsTokens.size() > MAX_STATS_TOKENS) {
            throw new IllegalArgumentException(
                    "retention candidate stats token count exceeds 4096");
        }
        String previous = null;
        for (RetentionStatsToken token : statsTokens) {
            Objects.requireNonNull(token, "statsTokens contains null");
            if (previous != null && previous.compareTo(token.key()) >= 0) {
                throw new IllegalArgumentException(
                        "retention candidate stats tokens must be strictly ordered and unique");
            }
            previous = token.key();
        }
        Objects.requireNonNull(evidenceSha256, "evidenceSha256");
        if (evidenceSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("evidenceSha256 must use SHA256");
        }
    }
}
