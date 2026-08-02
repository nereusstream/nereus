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

package com.nereusstream.kafka.retention;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointPublicationFactory;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointState;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointFailureQuarantine;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointPublicationCoordinator;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceValidator;
import com.nereusstream.kafka.compaction.KafkaCompactionPartitionPass;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.MandatoryCoverage;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryCoordinator;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Exact product composition of retention and DeleteRecords for one recovered Kafka leader.
 */
public final class DefaultKafkaPartitionMaintenance implements KafkaPartitionMaintenance {
    private final KafkaPartitionIdentity identity;
    private final int leaderEpoch;
    private final StreamId streamId;
    private final KafkaCheckpointSourceValidator sourceValidator;
    private final KafkaPartitionMetadataStore bindings;
    private final StreamStorage streams;
    private final CheckpointGateFactory checkpointGates;
    private final Duration trimTimeout;
    private final Clock clock;

    public DefaultKafkaPartitionMaintenance(
            KafkaPartitionIdentity identity,
            int leaderEpoch,
            StreamId streamId,
            KafkaCheckpointSourceValidator sourceValidator,
            KafkaPartitionMetadataStore bindings,
            StreamStorage streams,
            KafkaCheckpointRecoveryCoordinator recovery,
            KafkaCanonicalCheckpointPublicationFactory publicationFactory,
            KafkaCheckpointPublicationCoordinator publication,
            KafkaCheckpointFailureQuarantine checkpointFailureQuarantine,
            Duration verificationTimeout,
            Duration trimTimeout,
            Clock clock) {
        this.identity = Objects.requireNonNull(identity, "identity");
        if (leaderEpoch < 0) {
            throw new IllegalArgumentException("leaderEpoch must be non-negative");
        }
        this.leaderEpoch = leaderEpoch;
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        this.sourceValidator = Objects.requireNonNull(sourceValidator, "sourceValidator");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.streams = Objects.requireNonNull(streams, "streams");
        KafkaCheckpointRecoveryCoordinator exactRecovery = Objects.requireNonNull(recovery, "recovery");
        KafkaCanonicalCheckpointPublicationFactory exactPublicationFactory =
                Objects.requireNonNull(publicationFactory, "publicationFactory");
        KafkaCheckpointPublicationCoordinator exactPublication = Objects.requireNonNull(publication, "publication");
        KafkaCheckpointFailureQuarantine exactFailureQuarantine =
                Objects.requireNonNull(checkpointFailureQuarantine, "checkpointFailureQuarantine");
        Duration exactVerificationTimeout = positive(verificationTimeout, "verificationTimeout");
        this.checkpointGates = hooks -> {
            KafkaRetentionCheckpointServices services = new KafkaRetentionCheckpointServices(
                    exactRecovery,
                    exactPublicationFactory,
                    exactPublication,
                    this.bindings,
                    snapshot -> this.sourceValidator.loadCurrent().thenCompose(source -> capture(hooks, source)
                            .thenApply(captured -> new KafkaRetentionCheckpointServices.Capture(
                                    source, captured.canonicalState(), this.leaderEpoch, this.sourceValidator))),
                    exactVerificationTimeout);
            return new KafkaRetentionCheckpointGate(services, services, exactFailureQuarantine);
        };
        this.trimTimeout = positive(trimTimeout, "trimTimeout");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    DefaultKafkaPartitionMaintenance(
            KafkaPartitionIdentity identity,
            int leaderEpoch,
            StreamId streamId,
            KafkaCheckpointSourceValidator sourceValidator,
            KafkaPartitionMetadataStore bindings,
            StreamStorage streams,
            CheckpointGateFactory checkpointGates,
            Duration trimTimeout,
            Clock clock) {
        this.identity = Objects.requireNonNull(identity, "identity");
        if (leaderEpoch < 0) {
            throw new IllegalArgumentException("leaderEpoch must be non-negative");
        }
        this.leaderEpoch = leaderEpoch;
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        this.sourceValidator = Objects.requireNonNull(sourceValidator, "sourceValidator");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.streams = Objects.requireNonNull(streams, "streams");
        this.checkpointGates = Objects.requireNonNull(checkpointGates, "checkpointGates");
        this.trimTimeout = positive(trimTimeout, "trimTimeout");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<KafkaRetentionCoordinator.RunResult> runRetention(Hooks hooks) {
        Operation operation = operation(hooks);
        return new KafkaRetentionCoordinator(operation.snapshots(), operation.planner(), operation.barrier()).runOnce();
    }

    @Override
    public CompletableFuture<KafkaDeleteRecordsCoordinator.Result> deleteRecords(
            Hooks hooks, long normalizedRequestedOffset) {
        Operation operation = operation(hooks);
        return operation.snapshots().loadCurrent().thenCompose(snapshot -> new KafkaDeleteRecordsCoordinator(
                        operation.barrier())
                .deleteTo(snapshot, normalizedRequestedOffset));
    }

    @Override
    public CompletableFuture<KafkaCompactionPartitionPass.Capture> captureCompaction(CompactionHooks hooks) {
        CompactionHooks exactHooks = Objects.requireNonNull(hooks, "hooks");
        return sourceValidator.loadCurrent().thenCompose(source -> streams.getStableHeadSnapshot(streamId)
                .thenCompose(head -> bindings.get(identity.durableId())
                        .thenCompose(current -> captureCompactionState(exactHooks, source)
                                .thenCompose(captured -> finishCompactionCapture(
                                        exactHooks,
                                        source,
                                        head,
                                        current.orElseThrow(
                                                () -> invariant("Kafka" + " compaction" + " binding" + " disappeared")),
                                        captured)))));
    }

    private Operation operation(Hooks hooks) {
        Hooks exactHooks = Objects.requireNonNull(hooks, "hooks");
        KafkaRetentionPlanner planner = new KafkaRetentionPlanner();
        KafkaTrimBarrier.SnapshotLoader snapshots = () -> loadSnapshot(exactHooks);
        KafkaTrimBarrier.CheckpointGate checkpointGate = Objects.requireNonNull(
                checkpointGates.create(exactHooks), "Kafka maintenance checkpoint-gate factory returned null");
        KafkaRetentionDurableTrimListener listener =
                new KafkaRetentionDurableTrimListener(bindings, exactHooks::advanceLogStart, clock);
        KafkaTrimBarrier barrier =
                new KafkaTrimBarrier(planner, snapshots, checkpointGate, streams, trimTimeout, listener);
        return new Operation(planner, snapshots, barrier);
    }

    private CompletableFuture<KafkaCompactionPartitionPass.Capture> finishCompactionCapture(
            CompactionHooks hooks,
            KafkaCheckpointSourceState source,
            StableStreamHeadSnapshot head,
            VersionedKafkaPartitionBinding binding,
            CompactionState captured) {
        requireSameSource(source, head);
        requireCompactionBinding(source, binding, captured);
        KafkaCompactionPlanner.Snapshot plannerSnapshot = compactionSnapshot(binding, captured);
        KafkaCompactionPlanner.Candidate candidate = new KafkaCompactionPlanner().select(plannerSnapshot);
        CompletableFuture<KafkaCompactionPartitionPass.PassOneInputs> passOne;
        try {
            passOne = Objects.requireNonNull(
                    hooks.capturePassOne(source, candidate, captured),
                    "Kafka compaction pass-one capture hook returned a null future");
        } catch (Throwable failure) {
            passOne = CompletableFuture.failedFuture(failure);
        }
        return passOne.thenApply(inputs -> new KafkaCompactionPartitionPass.Capture(
                binding,
                plannerSnapshot,
                captured.outputPolicy(),
                Objects.requireNonNull(inputs, "Kafka compaction pass-one capture"),
                captured.writeSettings(),
                compactionAuthorityGuard(source, binding)));
    }

    private CompletableFuture<CompactionState> captureCompactionState(
            CompactionHooks hooks, KafkaCheckpointSourceState source) {
        CompletableFuture<CompactionState> captured;
        try {
            captured = Objects.requireNonNull(
                    hooks.capture(source), "Kafka compaction state capture hook returned a null future");
        } catch (Throwable failure) {
            captured = CompletableFuture.failedFuture(failure);
        }
        return captured.thenApply(value -> {
            CompactionState exact = Objects.requireNonNull(value, "Kafka compaction state capture");
            if (exact.canonicalState().stableEndOffset() != source.endOffset()
                    || exact.canonicalState().logStartOffset() != source.trimOffset()) {
                throw invariant("Kafka compaction hook captured another stable source");
            }
            return exact;
        });
    }

    private KafkaCompactionPlanner.Snapshot compactionSnapshot(
            VersionedKafkaPartitionBinding binding, CompactionState captured) {
        var history = captured.canonicalState().virtualSegmentState().configHistory();
        if (history.isEmpty()) {
            throw invariant("Kafka compaction capture has no effective log config");
        }
        return new KafkaCompactionPlanner.Snapshot(
                captured.canonicalState().virtualSegmentState(),
                KafkaCompactionPlanner.Policy.from(history.get(history.size() - 1)),
                mandatoryCoverage(binding.value().compactionCoverage()),
                captured.lastStableOffset(),
                captured.highWatermark(),
                clock.millis());
    }

    private com.nereusstream.materialization.MaterializationTaskMutationGuard compactionAuthorityGuard(
            KafkaCheckpointSourceState capturedSource, VersionedKafkaPartitionBinding capturedBinding) {
        return () -> sourceValidator.loadCurrent().thenCompose(currentSource -> bindings.get(identity.durableId())
                .thenApply(currentBinding -> {
                    requireCurrentCompactionAuthority(
                            capturedSource,
                            capturedBinding,
                            currentSource,
                            currentBinding.orElseThrow(() -> invariant("Kafka"
                                    + " compaction"
                                    + " binding"
                                    + " disappeared"
                                    + " during"
                                    + " authority"
                                    + " revalidation")));
                    return null;
                }));
    }

    private void requireCompactionBinding(
            KafkaCheckpointSourceState source, VersionedKafkaPartitionBinding binding, CompactionState captured) {
        KafkaPartitionBindingRecord root = binding.value();
        KafkaCanonicalCheckpointState canonical = captured.canonicalState();
        if (!root.identity().equals(identity.durableId())
                || root.lifecycle() != KafkaPartitionLifecycle.ACTIVE
                || !root.streamId().equals(streamId.value())
                || root.observedLeaderEpoch() != leaderEpoch
                || root.observedLogStartOffset() != source.trimOffset()
                || root.observedStableEndOffset() > source.endOffset()
                || canonical.checkpointOffset() != source.endOffset()
                || canonical.stableEndOffset() != source.endOffset()
                || canonical.logStartOffset() != source.trimOffset()
                || source.appendInFlight()
                || source.stateMapEndOffset() != source.endOffset()) {
            throw invariant("Kafka compaction capture does not match the ACTIVE binding and stable source");
        }
    }

    private void requireCurrentCompactionAuthority(
            KafkaCheckpointSourceState capturedSource,
            VersionedKafkaPartitionBinding capturedBinding,
            KafkaCheckpointSourceState currentSource,
            VersionedKafkaPartitionBinding currentBinding) {
        KafkaPartitionBindingRecord captured = capturedBinding.value();
        KafkaPartitionBindingRecord current = currentBinding.value();
        if (!current.identity().equals(captured.identity())
                || current.lifecycle() != KafkaPartitionLifecycle.ACTIVE
                || current.incarnation() != captured.incarnation()
                || current.bindingEpoch() < captured.bindingEpoch()
                || !current.streamId().equals(captured.streamId())
                || current.payloadMappingId() != captured.payloadMappingId()
                || !current.storageProfile().equals(captured.storageProfile())
                || current.observedLeaderId() != captured.observedLeaderId()
                || current.observedLeaderEpoch() != captured.observedLeaderEpoch()
                || current.observedBrokerEpoch() != captured.observedBrokerEpoch()
                || current.observedLogStartOffset() != captured.observedLogStartOffset()
                || current.observedStableEndOffset() < captured.observedStableEndOffset()
                || !capturedSource.sameSession(currentSource)
                || currentSource.trimOffset() != capturedSource.trimOffset()
                || currentSource.endOffset() < capturedSource.endOffset()
                || currentSource.commitVersion() < capturedSource.commitVersion()
                || current.observedLogStartOffset() > currentSource.trimOffset()
                || current.observedStableEndOffset() > currentSource.endOffset()
                || currentSource.appendInFlight()
                || currentSource.stateMapEndOffset() != currentSource.endOffset()) {
            throw invariant("Kafka compaction partition authority changed after capture"
                    + authorityChangeDetails(capturedSource, captured, currentSource, current));
        }
    }

    private static String authorityChangeDetails(
            KafkaCheckpointSourceState capturedSource,
            KafkaPartitionBindingRecord captured,
            KafkaCheckpointSourceState currentSource,
            KafkaPartitionBindingRecord current) {
        return " [bindingIdentityEqual="
                + current.identity().equals(captured.identity())
                + ", capturedLifecycle="
                + captured.lifecycle()
                + ", currentLifecycle="
                + current.lifecycle()
                + ", capturedIncarnation="
                + captured.incarnation()
                + ", currentIncarnation="
                + current.incarnation()
                + ", capturedBindingEpoch="
                + captured.bindingEpoch()
                + ", currentBindingEpoch="
                + current.bindingEpoch()
                + ", bindingStreamEqual="
                + current.streamId().equals(captured.streamId())
                + ", capturedLeader="
                + captured.observedLeaderId()
                + "/"
                + captured.observedLeaderEpoch()
                + "/"
                + captured.observedBrokerEpoch()
                + ", currentLeader="
                + current.observedLeaderId()
                + "/"
                + current.observedLeaderEpoch()
                + "/"
                + current.observedBrokerEpoch()
                + ", capturedObservedOffsets="
                + captured.observedLogStartOffset()
                + ".."
                + captured.observedStableEndOffset()
                + ", currentObservedOffsets="
                + current.observedLogStartOffset()
                + ".."
                + current.observedStableEndOffset()
                + ", sameSession="
                + capturedSource.sameSession(currentSource)
                + ", capturedLease="
                + capturedSource.leaseVersion()
                + ", currentLease="
                + currentSource.leaseVersion()
                + ", capturedSource="
                + capturedSource.trimOffset()
                + ".."
                + capturedSource.endOffset()
                + "/"
                + capturedSource.commitVersion()
                + "/"
                + capturedSource.stateMapEndOffset()
                + ", currentSource="
                + currentSource.trimOffset()
                + ".."
                + currentSource.endOffset()
                + "/"
                + currentSource.commitVersion()
                + "/"
                + currentSource.stateMapEndOffset()
                + ", currentAppendInFlight="
                + currentSource.appendInFlight()
                + "]";
    }

    private static Optional<MandatoryCoverage> mandatoryCoverage(KafkaCompactionCoverageRecord coverage) {
        if (coverage.coverageVersion() == 0) {
            return Optional.empty();
        }
        return Optional.of(new MandatoryCoverage(
                coverage.startOffset(),
                coverage.endOffset(),
                coverage.activationEpoch(),
                sha256(coverage.generationSetSha256()),
                sha256(coverage.policySha256())));
    }

    private static Checksum sha256(byte[] value) {
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(value));
    }

    private CompletableFuture<KafkaTrimBarrier.Snapshot> loadSnapshot(Hooks hooks) {
        return sourceValidator.loadCurrent().thenCompose(source -> streams.getStableHeadSnapshot(streamId)
                .thenCompose(head -> bindings.get(identity.durableId()).thenCompose(binding -> capture(hooks, source)
                        .thenApply(captured -> snapshot(
                                source,
                                head,
                                binding.orElseThrow(
                                        () -> invariant("Kafka" + " maintenance" + " binding" + " disappeared")),
                                captured)))));
    }

    private KafkaTrimBarrier.Snapshot snapshot(
            KafkaCheckpointSourceState source,
            StableStreamHeadSnapshot head,
            VersionedKafkaPartitionBinding binding,
            Capture captured) {
        requireSameSource(source, head);
        KafkaCanonicalCheckpointState canonical = captured.canonicalState();
        if (canonical.logStartOffset() != source.trimOffset()
                || canonical.stableEndOffset() != source.endOffset()
                || canonical.checkpointOffset() != source.endOffset()) {
            throw invariant("Kafka maintenance capture does not match the stable source");
        }
        var history = canonical.virtualSegmentState().configHistory();
        if (history.isEmpty()) {
            throw invariant("Kafka maintenance capture has no effective log config");
        }
        KafkaRetentionPlanner.Snapshot retention = new KafkaRetentionPlanner.Snapshot(
                canonical.virtualSegmentState(),
                KafkaRetentionPlanner.Policy.from(history.get(history.size() - 1)),
                captured.lastStableOffset(),
                captured.highWatermark(),
                clock.millis());
        return new KafkaTrimBarrier.Snapshot(identity, binding, head, retention);
    }

    private CompletableFuture<Capture> capture(Hooks hooks, KafkaCheckpointSourceState source) {
        CompletableFuture<Capture> captured;
        try {
            captured = Objects.requireNonNull(
                    hooks.capture(source), "Kafka maintenance capture hook returned a null future");
        } catch (Throwable failure) {
            captured = CompletableFuture.failedFuture(failure);
        }
        return captured.thenApply(value -> {
            Capture exact = Objects.requireNonNull(value, "Kafka maintenance capture");
            if (exact.canonicalState().stableEndOffset() != source.endOffset()
                    || exact.canonicalState().logStartOffset() != source.trimOffset()) {
                throw invariant("Kafka maintenance hook captured another stable source");
            }
            return exact;
        });
    }

    private void requireSameSource(KafkaCheckpointSourceState source, StableStreamHeadSnapshot head) {
        if (!head.streamId().equals(streamId)
                || head.trimOffset() != source.trimOffset()
                || head.committedEndOffset() != source.endOffset()
                || head.commitVersion() != source.commitVersion()
                || !head.lastCommitId().equals(source.lastCommitId())
                || !head.durableHeadSha256().equals(source.headSha256())) {
            throw invariant("Kafka maintenance stream head changed during capture");
        }
        AcquiredAppendSession acquired =
                head.appendSession().orElseThrow(() -> invariant("Kafka maintenance stream lost append authority"));
        AppendSession session = acquired.session();
        if (!acquired.authority().equals(java.util.Optional.of(source.authority()))
                || !session.writerId().equals(source.writerId())
                || session.epoch() != source.sessionEpoch()
                || !session.fencingToken().equals(source.fencingToken())
                || session.leaseVersion() != source.leaseVersion()
                || source.authority().authorityEpoch() != leaderEpoch) {
            throw invariant("Kafka maintenance append authority changed during capture");
        }
    }

    private static Duration positive(Duration value, String field) {
        Duration exact = Objects.requireNonNull(value, field);
        if (exact.isZero() || exact.isNegative() || exact.toMillis() <= 0) {
            throw new IllegalArgumentException(field + " must be positive and millisecond-representable");
        }
        return exact;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private record Operation(
            KafkaRetentionPlanner planner, KafkaTrimBarrier.SnapshotLoader snapshots, KafkaTrimBarrier barrier) {
        private Operation {
            Objects.requireNonNull(planner, "planner");
            Objects.requireNonNull(snapshots, "snapshots");
            Objects.requireNonNull(barrier, "barrier");
        }
    }

    @FunctionalInterface
    interface CheckpointGateFactory {
        KafkaTrimBarrier.CheckpointGate create(Hooks hooks);
    }
}
