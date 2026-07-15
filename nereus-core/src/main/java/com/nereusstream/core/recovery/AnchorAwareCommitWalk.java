/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailCursor;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Stable-root snapshot of a bounded live commit tail, ordered newest to oldest. */
public record AnchorAwareCommitWalk(
        Optional<VersionedRecoveryCheckpointRoot> recoveryRoot,
        AppendRecoveryAnchor anchor,
        AppendRecoveryHead observedHead,
        List<AppendRecoveryCommit> commitsNewestFirst,
        boolean anchorReached,
        Optional<AppendRecoveryTailCursor> continuation) {
    public AnchorAwareCommitWalk {
        recoveryRoot = Objects.requireNonNull(recoveryRoot, "recoveryRoot");
        Objects.requireNonNull(anchor, "anchor");
        Objects.requireNonNull(observedHead, "observedHead");
        commitsNewestFirst = List.copyOf(Objects.requireNonNull(
                commitsNewestFirst, "commitsNewestFirst"));
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (!anchor.streamId().equals(observedHead.streamId())
                || anchorReached == continuation.isPresent()) {
            throw new IllegalArgumentException("anchor-aware commit walk terminal state is inconsistent");
        }
        requireRootAnchor(recoveryRoot, anchor);
        requireCommitOrder(anchor, observedHead, commitsNewestFirst, anchorReached, continuation);
    }

    private static void requireRootAnchor(
            Optional<VersionedRecoveryCheckpointRoot> root,
            AppendRecoveryAnchor anchor) {
        if (root.isEmpty() || root.orElseThrow().value().checkpoints().isEmpty()) {
            if (!anchor.isGenesis()) {
                throw new IllegalArgumentException("empty recovery root must select the genesis anchor");
            }
            return;
        }
        var value = root.orElseThrow().value();
        if (!value.streamId().equals(anchor.streamId().value())
                || !value.lastCommitId().equals(anchor.lastCommitId())
                || value.coveredEndOffset() != anchor.offsetEnd()
                || value.cumulativeSizeAtEnd() != anchor.cumulativeSize()
                || value.lastCommitVersion() != anchor.commitVersion()) {
            throw new IllegalArgumentException("recovery root does not reproduce the walk anchor");
        }
    }

    private static void requireCommitOrder(
            AppendRecoveryAnchor anchor,
            AppendRecoveryHead head,
            List<AppendRecoveryCommit> commits,
            boolean anchorReached,
            Optional<AppendRecoveryTailCursor> continuation) {
        String expectedId = head.lastCommitId();
        long expectedEnd = head.offsetEnd();
        long expectedSize = head.cumulativeSize();
        long expectedVersion = head.commitVersion();
        for (AppendRecoveryCommit evidence : commits) {
            var commit = evidence.canonicalCommit();
            if (!commit.streamId().equals(head.streamId().value())
                    || !commit.commitId().equals(expectedId)
                    || commit.offsetEnd() != expectedEnd
                    || commit.cumulativeSize() != expectedSize
                    || commit.commitVersion() != expectedVersion) {
                throw new IllegalArgumentException("anchor-aware commits are not a canonical newest-first chain");
            }
            expectedId = commit.previousCommitId();
            expectedEnd = commit.offsetStart();
            expectedSize = Math.subtractExact(commit.cumulativeSize(), commit.logicalBytes());
            expectedVersion = Math.subtractExact(commit.commitVersion(), 1);
        }
        if (anchorReached) {
            if (!expectedId.equals(anchor.lastCommitId())
                    || expectedEnd != anchor.offsetEnd()
                    || expectedSize != anchor.cumulativeSize()
                    || expectedVersion != anchor.commitVersion()) {
                throw new IllegalArgumentException("anchor-aware commits do not close at the recovery anchor");
            }
        } else {
            AppendRecoveryTailCursor cursor = continuation.orElseThrow();
            if (!cursor.nextCommitId().equals(expectedId)
                    || cursor.nextOffsetEnd() != expectedEnd
                    || cursor.nextCumulativeSize() != expectedSize
                    || cursor.nextCommitVersion() != expectedVersion) {
                throw new IllegalArgumentException("anchor-aware continuation does not follow the returned chain");
            }
        }
    }
}
