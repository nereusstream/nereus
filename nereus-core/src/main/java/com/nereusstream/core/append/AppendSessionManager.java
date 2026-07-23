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

package com.nereusstream.core.append;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.AppendSessionRequest;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public final class AppendSessionManager {
    private final StreamStorageConfig config;
    private final OxiaMetadataStore metadataStore;
    private final Clock clock;
    private final ConcurrentHashMap<StreamId, AppendSession> cache = new ConcurrentHashMap<>();

    public AppendSessionManager(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<AppendSession> acquire(StreamId streamId, AppendSessionOptions options) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(options, "options");
        if (!config.writerId().equals(options.writerId())) {
            return NereusException.failedFuture(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "append session writerId must match StreamStorageConfig.writerId");
        }
        return metadataStore.acquireAppendSession(config.cluster(), streamId, options)
                .thenApply(this::cacheRecord);
    }

    public CompletableFuture<AcquiredAppendSession> acquire(
            StreamId streamId, AppendSessionRequest request) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(request, "request");
        if (!config.writerId().equals(request.options().writerId())) {
            return NereusException.failedFuture(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "append session writerId must match StreamStorageConfig.writerId");
        }
        return metadataStore.acquireAppendSession(config.cluster(), streamId, request)
                .thenApply(record -> new AcquiredAppendSession(cacheRecord(record), record.authority()));
    }

    CompletableFuture<AppendSession> ensureSession(
            StreamId streamId,
            Optional<AppendSession> supplied,
            boolean autoAcquire,
            AppendDeadline deadline) {
        AppendSession selected;
        try {
            Optional<AppendSession> validatedSupplied = selectSupplied(streamId, supplied);
            if (validatedSupplied.isPresent()
                    && validatedSupplied.get().expiresAtMillis() > clock.millis()) {
                selected = validatedSupplied.get();
            } else if (autoAcquire && config.autoAcquireAppendSession()) {
                selected = cache.get(streamId);
            } else {
                selected = null;
            }
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        if (selected != null && selected.expiresAtMillis() > clock.millis()) {
            return ensureCommitWindow(selected, deadline);
        }
        if (selected != null) {
            cache.remove(streamId, selected);
        }
        if (!autoAcquire || !config.autoAcquireAppendSession()) {
            return NereusException.failedAppendFuture(
                    ErrorCode.APPEND_SESSION_EXPIRED,
                    true,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "append requires a live session and auto-acquire is disabled");
        }
        AppendSessionOptions acquireOptions = new AppendSessionOptions(
                config.writerId(), config.appendSessionTtl(), true);
        return deadline.bound(
                        () -> metadataStore.acquireAppendSession(config.cluster(), streamId, acquireOptions),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "acquire append session")
                .thenApply(this::cacheRecord)
                .thenCompose(session -> ensureCommitWindow(session, deadline));
    }

    CompletableFuture<AppendSession> ensureCommitWindow(
            AppendSession session,
            AppendDeadline deadline) {
        long minimumMillis = durationMillis(config.appendSessionMinCommitRemaining());
        long remainingMillis = session.expiresAtMillis() - clock.millis();
        if (remainingMillis >= minimumMillis) {
            return CompletableFuture.completedFuture(session);
        }
        return deadline.bound(
                        () -> metadataStore.renewAppendSession(
                                config.cluster(),
                                session.streamId(),
                                session.writerId(),
                                session.epoch(),
                                session.fencingToken(),
                                config.appendSessionTtl()),
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "renew append session")
                .thenApply(this::cacheRecord);
    }

    void invalidate(StreamId streamId) {
        cache.remove(streamId);
    }

    private Optional<AppendSession> selectSupplied(
            StreamId streamId,
            Optional<AppendSession> supplied) {
        Objects.requireNonNull(supplied, "appendSession");
        if (supplied.isEmpty()) {
            return Optional.empty();
        }
        AppendSession session = supplied.get();
        if (!session.streamId().equals(streamId)) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "append session belongs to another stream",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        if (!session.writerId().equals(config.writerId())) {
            throw new NereusException(
                    ErrorCode.FENCED_APPEND,
                    true,
                    "append session belongs to another writer",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        return supplied;
    }

    private AppendSession cacheRecord(AppendSessionRecord record) {
        AppendSession session = new AppendSession(
                new StreamId(record.streamId()),
                record.writerId(),
                record.epoch(),
                record.fencingToken(),
                record.leaseVersion(),
                record.expiresAtMillis());
        AppendSession retained = cache.compute(session.streamId(), (ignored, existing) ->
                existing == null || session.leaseVersion() >= existing.leaseVersion() ? session : existing);
        while (cache.size() > config.maxCachedStreams()) {
            Optional<java.util.Map.Entry<StreamId, AppendSession>> earliest = cache.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(session.streamId()))
                    .min(java.util.Comparator.comparingLong(entry -> entry.getValue().expiresAtMillis()));
            if (earliest.isEmpty()) {
                break;
            }
            java.util.Map.Entry<StreamId, AppendSession> entry = earliest.orElseThrow();
            cache.remove(entry.getKey(), entry.getValue());
        }
        return retained;
    }

    private static long durationMillis(java.time.Duration duration) {
        try {
            return duration.toMillis();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
