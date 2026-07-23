/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/** One exact durable stream-head observation, including its current append authority and canonical byte digest. */
public record StableStreamHeadSnapshot(
        StreamId streamId,
        StreamState state,
        StorageProfile storageProfile,
        long trimOffset,
        long committedEndOffset,
        long cumulativeSize,
        long commitVersion,
        String lastCommitId,
        Optional<AcquiredAppendSession> appendSession,
        Checksum durableHeadSha256,
        long metadataVersion) {
    public StableStreamHeadSnapshot {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(storageProfile, "storageProfile");
        Objects.requireNonNull(lastCommitId, "lastCommitId");
        appendSession = Objects.requireNonNull(appendSession, "appendSession");
        Objects.requireNonNull(durableHeadSha256, "durableHeadSha256");
        if (lastCommitId.getBytes(StandardCharsets.UTF_8).length > 64 * 1024) {
            throw new IllegalArgumentException("lastCommitId is too large");
        }
        if (durableHeadSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("durableHeadSha256 must use SHA256");
        }
        if (trimOffset < 0 || committedEndOffset < trimOffset || cumulativeSize < 0
                || commitVersion < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid stable stream-head numeric fields");
        }
        boolean emptyCommitChain = commitVersion == 0;
        if (emptyCommitChain != lastCommitId.isEmpty()
                || emptyCommitChain != (committedEndOffset == 0)
                || (emptyCommitChain && cumulativeSize != 0)) {
            throw new IllegalArgumentException("stable stream-head commit anchor is inconsistent");
        }
        appendSession.ifPresent(value -> {
            if (!value.session().streamId().equals(streamId)) {
                throw new IllegalArgumentException("append session belongs to another stream");
            }
        });
    }
}
