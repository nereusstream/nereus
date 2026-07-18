/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.StreamId;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.managedledger.NereusManagedLedgerOwnershipGuard;
import com.nereusstream.managedledger.cursor.CursorOwnerSession;
import com.nereusstream.managedledger.cursor.CursorRetentionCoordinator;
import com.nereusstream.managedledger.cursor.CursorRetentionView;
import com.nereusstream.managedledger.cursor.CursorStorage;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/** Process-owned production composition for per-ledger planners and the shared bounded execution lane. */
public final class NereusRetentionRuntime implements AutoCloseable {
    private final String cluster;
    private final OxiaMetadataStore l0;
    private final GenerationMetadataStore generations;
    private final CursorStorage cursors;
    private final CursorRetentionCoordinator cursorRetention;
    private final GenerationProtocolActivationGuard activationGuard;
    private final NereusRetentionConfig config;
    private final Clock clock;
    private final NereusRetentionExecutionLane lane;

    public NereusRetentionRuntime(
            String cluster,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            CursorStorage cursors,
            CursorRetentionCoordinator cursorRetention,
            GenerationProtocolActivationGuard activationGuard,
            NereusRetentionConfig config,
            Clock clock,
            ThreadFactory threadFactory) {
        this.cluster = requireText(cluster, "cluster");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.cursors = Objects.requireNonNull(cursors, "cursors");
        this.cursorRetention = Objects.requireNonNull(
                cursorRetention,
                "cursorRetention");
        this.activationGuard = Objects.requireNonNull(
                activationGuard,
                "activationGuard");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.lane = new NereusRetentionExecutionLane(
                config,
                Objects.requireNonNull(threadFactory, "threadFactory"));
    }

    public NereusManagedLedgerRetentionService createService(
            StreamId streamId,
            LiveProjectionSubject liveProjection,
            NereusManagedLedgerOwnershipGuard ownershipGuard,
            RetentionPolicySnapshotProvider policies,
            CursorOwnerSession owner,
            Consumer<CursorRetentionView> completedTrimObserver) {
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        RetentionPolicySnapshotProvider exactPolicies =
                Objects.requireNonNull(policies, "policies");
        DefaultRetentionCandidatePlanner planner =
                new DefaultRetentionCandidatePlanner(
                        cluster,
                        l0,
                        generations,
                        cursors,
                        Objects.requireNonNull(owner, "owner"),
                        exactPolicies,
                        config,
                        clock);
        return new NereusManagedLedgerRetentionService(
                exactStream,
                Objects.requireNonNull(liveProjection, "liveProjection"),
                Objects.requireNonNull(ownershipGuard, "ownershipGuard"),
                activationGuard,
                exactPolicies,
                planner,
                cursorRetention,
                owner,
                Objects.requireNonNull(
                        completedTrimObserver,
                        "completedTrimObserver"));
    }

    public CompletableFuture<Optional<RetentionCandidate>> trim(
            StreamId streamId,
            NereusManagedLedgerRetentionService service,
            String reason) {
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        NereusManagedLedgerRetentionService exactService =
                Objects.requireNonNull(service, "service");
        if (!exactService.streamId().equals(exactStream)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "retention service and execution stream must agree"));
        }
        return lane.submit(
                exactStream,
                () -> exactService.trim(reason));
    }

    public NereusRetentionConfig config() {
        return config;
    }

    @Override
    public void close() {
        lane.close();
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    field + " cannot be blank or contain NUL");
        }
        return value;
    }
}
