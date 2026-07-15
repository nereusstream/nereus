/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.AppendReplayCursor;
import com.nereusstream.metadata.oxia.AppendReplaySearchResult;
import com.nereusstream.metadata.oxia.AppendReplayStatus;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Legacy genesis-reachable adapter retained for runtimes without recovery checkpoints. */
public final class MetadataAppendRecoverySearcher implements AppendRecoverySearcher {
    private final String cluster;
    private final OxiaMetadataStore store;

    public MetadataAppendRecoverySearcher(String cluster, OxiaMetadataStore store) {
        this.cluster = requireText(cluster, "cluster");
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public CompletableFuture<AppendReplayResolution> search(
            CommitAppendRequest request,
            int maximumLiveCommits,
            int pageSize,
            Duration timeout) {
        Objects.requireNonNull(request, "request");
        requireBounds(maximumLiveCommits, pageSize, timeout);
        return searchPage(request, maximumLiveCommits, pageSize, Optional.empty(), 0);
    }

    private CompletableFuture<AppendReplayResolution> searchPage(
            CommitAppendRequest request,
            int maximumLiveCommits,
            int pageSize,
            Optional<AppendReplayCursor> continuation,
            int scanned) {
        int remaining = maximumLiveCommits - scanned;
        if (remaining <= 0) {
            return exhausted();
        }
        return store.searchAppendReplay(
                        cluster,
                        request,
                        continuation,
                        Math.min(pageSize, remaining))
                .thenCompose(result -> resolvePage(
                        request, maximumLiveCommits, pageSize, scanned, result));
    }

    private CompletableFuture<AppendReplayResolution> resolvePage(
            CommitAppendRequest request,
            int maximumLiveCommits,
            int pageSize,
            int scannedBefore,
            AppendReplaySearchResult result) {
        int scanned = Math.addExact(scannedBefore, result.scannedRecords());
        if (result.status() == AppendReplayStatus.FOUND) {
            return CompletableFuture.completedFuture(AppendReplayResolution.found(
                    result.committedAppend().orElseThrow(),
                    AppendReplayEvidenceSource.LIVE_COMMIT,
                    scanned));
        }
        if (result.status() == AppendReplayStatus.PROVEN_NOT_COMMITTED) {
            return CompletableFuture.completedFuture(
                    AppendReplayResolution.notCommitted(scanned));
        }
        if (result.scannedRecords() <= 0 || result.continuation().isEmpty()) {
            return CompletableFuture.failedFuture(invariant(
                    "live append replay returned a continuation without progress"));
        }
        if (scanned >= maximumLiveCommits) {
            return exhausted();
        }
        return searchPage(
                request,
                maximumLiveCommits,
                pageSize,
                result.continuation(),
                scanned);
    }

    private static <T> CompletableFuture<T> exhausted() {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                "append replay exhausted the live commit scan budget",
                AppendOutcome.MAY_HAVE_COMMITTED));
    }

    private static void requireBounds(
            int maximumLiveCommits,
            int pageSize,
            Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (maximumLiveCommits <= 0
                || pageSize <= 0
                || pageSize > maximumLiveCommits
                || timeout.isZero()
                || timeout.isNegative()) {
            throw new IllegalArgumentException("append replay bounds are invalid");
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                AppendOutcome.MAY_HAVE_COMMITTED);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
