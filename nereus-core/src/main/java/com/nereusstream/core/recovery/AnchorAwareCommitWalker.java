/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailCursor;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Root-double-read live-tail walker shared by checkpoint, replay, and repair paths. */
public final class AnchorAwareCommitWalker {
    private static final int MAX_ROOT_RESTARTS = 8;
    private static final int MAX_PAGE_SIZE = 1_000;

    private final String cluster;
    private final OxiaMetadataStore l0Store;
    private final GenerationMetadataStore generationStore;

    public AnchorAwareCommitWalker(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Store = Objects.requireNonNull(l0Store, "l0Store");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
    }

    public CompletableFuture<AnchorAwareCommitWalk> walk(
            StreamId streamId,
            int maxCommits,
            int pageSize) {
        Objects.requireNonNull(streamId, "streamId");
        if (maxCommits <= 0 || pageSize <= 0 || pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "maxCommits must be positive and pageSize must be in [1, 1000]");
        }
        return walkAttempt(streamId, maxCommits, pageSize, 0);
    }

    private CompletableFuture<AnchorAwareCommitWalk> walkAttempt(
            StreamId streamId,
            int maxCommits,
            int pageSize,
            int rootRestarts) {
        return generationStore.getRecoveryRoot(cluster, streamId).thenCompose(root -> {
            AppendRecoveryAnchor anchor = anchor(streamId, root);
            Accumulator accumulator = new Accumulator(root, anchor, maxCommits, pageSize);
            return readPage(streamId, accumulator, Optional.empty())
                    .thenCompose(ignored -> generationStore.getRecoveryRoot(cluster, streamId))
                    .thenCompose(reloaded -> {
                        if (sameRoot(root, reloaded)) {
                            return CompletableFuture.completedFuture(accumulator.result());
                        }
                        if (rootRestarts + 1 >= MAX_ROOT_RESTARTS) {
                            return CompletableFuture.failedFuture(new NereusException(
                                    ErrorCode.METADATA_CONDITION_FAILED,
                                    true,
                                    "recovery root changed throughout the anchor-aware walk"));
                        }
                        return walkAttempt(streamId, maxCommits, pageSize, rootRestarts + 1);
                    });
        });
    }

    private CompletableFuture<Void> readPage(
            StreamId streamId,
            Accumulator accumulator,
            Optional<AppendRecoveryTailCursor> continuation) {
        int remaining = accumulator.maxCommits - accumulator.commits.size();
        if (remaining <= 0) {
            accumulator.anchorReached = false;
            accumulator.continuation = continuation;
            return CompletableFuture.completedFuture(null);
        }
        int limit = Math.min(accumulator.pageSize, remaining);
        return l0Store.readAppendRecoveryTail(
                        cluster,
                        streamId,
                        accumulator.anchor,
                        continuation,
                        limit)
                .thenCompose(page -> {
                    accumulator.add(page);
                    if (page.anchorReached()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    Optional<AppendRecoveryTailCursor> next = page.continuation();
                    if (accumulator.commits.size() >= accumulator.maxCommits) {
                        accumulator.anchorReached = false;
                        accumulator.continuation = next;
                        return CompletableFuture.completedFuture(null);
                    }
                    return readPage(streamId, accumulator, next);
                });
    }

    private static AppendRecoveryAnchor anchor(
            StreamId streamId,
            Optional<VersionedRecoveryCheckpointRoot> root) {
        if (root.isEmpty() || root.orElseThrow().value().checkpoints().isEmpty()) {
            return AppendRecoveryAnchor.genesis(streamId);
        }
        var value = root.orElseThrow().value();
        if (!value.streamId().equals(streamId.value())) {
            throw invariant("recovery root belongs to another stream");
        }
        return new AppendRecoveryAnchor(
                streamId,
                value.lastCommitId(),
                value.coveredEndOffset(),
                value.cumulativeSizeAtEnd(),
                value.lastCommitVersion());
    }

    private static boolean sameRoot(
            Optional<VersionedRecoveryCheckpointRoot> left,
            Optional<VersionedRecoveryCheckpointRoot> right) {
        return left.equals(right);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static final class Accumulator {
        private final Optional<VersionedRecoveryCheckpointRoot> root;
        private final AppendRecoveryAnchor anchor;
        private final int maxCommits;
        private final int pageSize;
        private final List<AppendRecoveryCommit> commits = new ArrayList<>();
        private AppendRecoveryHead observedHead;
        private boolean anchorReached;
        private Optional<AppendRecoveryTailCursor> continuation = Optional.empty();

        private Accumulator(
                Optional<VersionedRecoveryCheckpointRoot> root,
                AppendRecoveryAnchor anchor,
                int maxCommits,
                int pageSize) {
            this.root = root;
            this.anchor = anchor;
            this.maxCommits = maxCommits;
            this.pageSize = pageSize;
        }

        private void add(AppendRecoveryTailPage page) {
            if (!page.anchor().equals(anchor)
                    || (observedHead != null && !observedHead.equals(page.observedHead()))) {
                throw invariant("append recovery pages changed their root/head anchors");
            }
            if (observedHead == null) {
                observedHead = page.observedHead();
            }
            commits.addAll(page.commitsNewestFirst());
            anchorReached = page.anchorReached();
            continuation = page.continuation();
        }

        private AnchorAwareCommitWalk result() {
            if (observedHead == null) {
                throw invariant("anchor-aware commit walk returned no head observation");
            }
            return new AnchorAwareCommitWalk(
                    root,
                    anchor,
                    observedHead,
                    commits,
                    anchorReached,
                    continuation);
        }
    }
}
