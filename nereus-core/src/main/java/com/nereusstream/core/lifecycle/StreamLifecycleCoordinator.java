/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.core.lifecycle;

import com.nereusstream.api.DeleteOptions;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.SealOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.AppendCoordinator;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.StreamStateTransitionRequest;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Authoritative seal and two-step logical-delete state machine. */
public final class StreamLifecycleCoordinator implements AutoCloseable {
    private static final int MAX_CAS_RETRIES = 64;
    private final StreamStorageConfig config;
    private final OxiaMetadataStore metadata;
    private final AppendCoordinator mutationLanes;
    private final AtomicBoolean closed = new AtomicBoolean();

    public StreamLifecycleCoordinator(
            StreamStorageConfig config, OxiaMetadataStore metadata, AppendCoordinator mutationLanes) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.mutationLanes = Objects.requireNonNull(mutationLanes, "mutationLanes");
    }

    public CompletableFuture<StreamMetadata> seal(StreamId streamId, SealOptions options) {
        Objects.requireNonNull(streamId, "streamId"); Objects.requireNonNull(options, "options");
        return admitted(streamId, options.timeout(), () -> sealStep(streamId, 0));
    }

    public CompletableFuture<StreamMetadata> delete(StreamId streamId, DeleteOptions options) {
        Objects.requireNonNull(streamId, "streamId"); Objects.requireNonNull(options, "options");
        return admitted(streamId, options.timeout(), () -> deleteStep(streamId, 0));
    }

    private CompletableFuture<StreamMetadata> admitted(
            StreamId streamId, Duration timeout,
            java.util.function.Supplier<CompletableFuture<StreamMetadata>> operation) {
        if (closed.get()) return NereusException.failedFuture(ErrorCode.STORAGE_CLOSED, false, "storage is closed");
        CompletableFuture<StreamMetadata> source = mutationLanes.enqueueLifecycleMutation(streamId, operation);
        return source.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS).exceptionallyCompose(error -> {
            Throwable cause = unwrap(error);
            if (cause instanceof TimeoutException) return NereusException.failedFuture(
                    ErrorCode.TIMEOUT, true, "stream lifecycle operation timed out", cause);
            return CompletableFuture.failedFuture(cause);
        });
    }

    private CompletableFuture<StreamMetadata> sealStep(StreamId streamId, int attempt) {
        return metadata.getStreamSnapshot(config.cluster(), streamId).thenCompose(snapshot -> {
            StreamState state = state(snapshot);
            if (state == StreamState.SEALED) return CompletableFuture.completedFuture(toMetadata(snapshot));
            if (state != StreamState.ACTIVE) return NereusException.failedFuture(
                    ErrorCode.STREAM_NOT_ACTIVE, state == StreamState.CREATING,
                    "stream state does not allow seal");
            return transition(snapshot, StreamState.ACTIVE, StreamState.SEALED)
                    .thenApply(StreamLifecycleCoordinator::toMetadata)
                    .exceptionallyCompose(error -> retryTransition(error, attempt,
                            () -> sealStep(streamId, attempt + 1)));
        });
    }

    private CompletableFuture<StreamMetadata> deleteStep(StreamId streamId, int attempt) {
        return metadata.getStreamSnapshot(config.cluster(), streamId).thenCompose(snapshot -> {
            StreamState state = state(snapshot);
            if (state == StreamState.DELETED) return CompletableFuture.completedFuture(toMetadata(snapshot));
            if (state == StreamState.CREATING) return NereusException.failedFuture(
                    ErrorCode.STREAM_NOT_ACTIVE, true, "creating stream cannot be deleted yet");
            if (state == StreamState.ACTIVE || state == StreamState.SEALED) {
                return transition(snapshot, state, StreamState.DELETING)
                        .thenCompose(ignored -> deleteStep(streamId, 0))
                        .exceptionallyCompose(error -> retryTransition(error, attempt,
                                () -> deleteStep(streamId, attempt + 1)));
            }
            if (state == StreamState.DELETING) {
                return transition(snapshot, StreamState.DELETING, StreamState.DELETED)
                        .thenApply(StreamLifecycleCoordinator::toMetadata)
                        .exceptionallyCompose(error -> retryTransition(error, attempt,
                                () -> deleteStep(streamId, attempt + 1)));
            }
            return NereusException.failedFuture(ErrorCode.STREAM_NOT_ACTIVE, false, "invalid delete state");
        });
    }

    private CompletableFuture<StreamMetadataSnapshot> transition(
            StreamMetadataSnapshot snapshot, StreamState expected, StreamState target) {
        return metadata.transitionStreamState(config.cluster(), new StreamStateTransitionRequest(
                new StreamId(snapshot.metadata().streamId()), expected, target, snapshot.metadataVersion()));
    }

    private static CompletableFuture<StreamMetadata> retryTransition(
            Throwable error, int attempt,
            java.util.function.Supplier<CompletableFuture<StreamMetadata>> retry) {
        Throwable cause = unwrap(error);
        if (cause instanceof NereusException nereus
                && nereus.retriable() && attempt < MAX_CAS_RETRIES) {
            return retry.get();
        }
        return CompletableFuture.failedFuture(cause);
    }

    private static StreamState state(StreamMetadataSnapshot snapshot) {
        try { return StreamState.valueOf(snapshot.metadata().state()); }
        catch (IllegalArgumentException e) { throw new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, "unknown stream state", e); }
    }

    private static StreamMetadata toMetadata(StreamMetadataSnapshot snapshot) {
        return new StreamMetadata(new StreamId(snapshot.metadata().streamId()),
                new StreamName(snapshot.metadata().streamName()), state(snapshot),
                StorageProfile.valueOf(snapshot.metadata().profile()).canonical(), snapshot.metadata().attributes(),
                snapshot.metadata().createdAtMillis(), snapshot.metadataVersion(),
                snapshot.committedEnd().committedEndOffset(), snapshot.committedEnd().cumulativeSize(),
                snapshot.trim().trimOffset());
    }

    private static Throwable unwrap(Throwable error) {
        Throwable value = error;
        while (value instanceof CompletionException && value.getCause() != null) value = value.getCause();
        return value;
    }

    @Override public void close() { closed.set(true); }
}
