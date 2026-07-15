/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Newest-to-oldest exact live-tail page ending at either a continuation or the requested anchor. */
public record AppendRecoveryTailPage(
        AppendRecoveryAnchor anchor,
        AppendRecoveryHead observedHead,
        List<AppendRecoveryCommit> commitsNewestFirst,
        boolean anchorReached,
        Optional<AppendRecoveryTailCursor> continuation) {
    public AppendRecoveryTailPage {
        Objects.requireNonNull(anchor, "anchor");
        Objects.requireNonNull(observedHead, "observedHead");
        commitsNewestFirst = List.copyOf(Objects.requireNonNull(
                commitsNewestFirst, "commitsNewestFirst"));
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (!anchor.streamId().equals(observedHead.streamId())
                || anchorReached == continuation.isPresent()
                || (commitsNewestFirst.isEmpty() && continuation.isPresent())) {
            throw new IllegalArgumentException("append recovery tail page terminal state is inconsistent");
        }
        continuation.ifPresent(value -> {
            if (!value.anchor().equals(anchor) || !value.observedHead().equals(observedHead)) {
                throw new IllegalArgumentException("append recovery continuation changed page anchors");
            }
        });
    }
}
