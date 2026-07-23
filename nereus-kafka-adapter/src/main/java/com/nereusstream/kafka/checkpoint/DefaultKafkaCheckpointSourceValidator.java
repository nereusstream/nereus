/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.checkpoint;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact stream-head loader and immutable commit-chain validator for one authority-bound Kafka leader open. */
public final class DefaultKafkaCheckpointSourceValidator implements KafkaCheckpointSourceValidator {
    private final StreamStorage streams;
    private final StreamId streamId;
    private final AcquiredAppendSession acquiredSession;
    private final Optional<StorageProfile> expectedProfile;

    public DefaultKafkaCheckpointSourceValidator(
            StreamStorage streams,
            StreamId streamId,
            AcquiredAppendSession acquiredSession) {
        this(streams, streamId, acquiredSession, Optional.empty());
    }

    public DefaultKafkaCheckpointSourceValidator(
            StreamStorage streams,
            StreamId streamId,
            AcquiredAppendSession acquiredSession,
            StorageProfile expectedProfile) {
        this(streams, streamId, acquiredSession, Optional.of(
                Objects.requireNonNull(expectedProfile, "expectedProfile")));
    }

    private DefaultKafkaCheckpointSourceValidator(
            StreamStorage streams,
            StreamId streamId,
            AcquiredAppendSession acquiredSession,
            Optional<StorageProfile> expectedProfile) {
        this.streams = Objects.requireNonNull(streams, "streams");
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        this.acquiredSession = Objects.requireNonNull(acquiredSession, "acquiredSession");
        this.expectedProfile = Objects.requireNonNull(expectedProfile, "expectedProfile");
        if (!acquiredSession.session().streamId().equals(streamId)
                || acquiredSession.authority().isEmpty()) {
            throw new IllegalArgumentException("Kafka source validation requires an authoritative session for its stream");
        }
    }

    @Override
    public CompletableFuture<KafkaCheckpointSourceState> loadCurrent() {
        return streams.getStableHeadSnapshot(streamId).thenApply(this::sourceState);
    }

    @Override
    public CompletableFuture<Boolean> isSourceCommitReachable(
            KafkaCheckpointHeader captured,
            KafkaCheckpointSourceState current) {
        Objects.requireNonNull(captured, "captured");
        Objects.requireNonNull(current, "current");
        if (!captured.streamId().equals(streamId)) {
            return CompletableFuture.failedFuture(invariant(
                    "Kafka checkpoint source belongs to another stream"));
        }
        return streams.getStableHeadSnapshot(streamId).thenCompose(snapshot -> {
            KafkaCheckpointSourceState exact = sourceState(snapshot);
            if (!sameObservation(current, exact)) {
                return CompletableFuture.failedFuture(fenced(
                        "Kafka stream head or authority changed before commit reachability validation"));
            }
            return streams.isCommitReachable(
                    snapshot.commitAnchor(),
                    captured.sourceLastCommitId(),
                    captured.sourceCommitVersion());
        });
    }

    private KafkaCheckpointSourceState sourceState(StableStreamHeadSnapshot snapshot) {
        if (!snapshot.streamId().equals(streamId)
                || snapshot.state() != StreamState.ACTIVE
                || expectedProfile.filter(profile -> profile != snapshot.storageProfile()).isPresent()) {
            throw fenced("Kafka stream is not the expected ACTIVE stream");
        }
        AcquiredAppendSession current = snapshot.appendSession()
                .orElseThrow(() -> fenced("Kafka stream has no active authoritative session"));
        AppendSession expectedSession = acquiredSession.session();
        AppendSession actualSession = current.session();
        if (!current.authority().equals(acquiredSession.authority())
                || !actualSession.writerId().equals(expectedSession.writerId())
                || actualSession.epoch() != expectedSession.epoch()
                || !actualSession.fencingToken().equals(expectedSession.fencingToken())
                || actualSession.leaseVersion() < expectedSession.leaseVersion()) {
            throw fenced("Kafka append authority/session no longer matches the acquired leader session");
        }
        return new KafkaCheckpointSourceState(
                current.authority().orElseThrow(),
                actualSession.writerId(),
                actualSession.epoch(),
                actualSession.fencingToken(),
                actualSession.leaseVersion(),
                snapshot.trimOffset(),
                snapshot.committedEndOffset(),
                snapshot.commitVersion(),
                snapshot.lastCommitId(),
                snapshot.durableHeadSha256(),
                false,
                snapshot.committedEndOffset());
    }

    private static boolean sameObservation(
            KafkaCheckpointSourceState expected,
            KafkaCheckpointSourceState actual) {
        return expected.authority().equals(actual.authority())
                && expected.writerId().equals(actual.writerId())
                && expected.sessionEpoch() == actual.sessionEpoch()
                && expected.fencingToken().equals(actual.fencingToken())
                && expected.leaseVersion() == actual.leaseVersion()
                && expected.trimOffset() == actual.trimOffset()
                && expected.endOffset() == actual.endOffset()
                && expected.commitVersion() == actual.commitVersion()
                && expected.lastCommitId().equals(actual.lastCommitId())
                && expected.headSha256().equals(actual.headSha256())
                && !actual.appendInFlight()
                && actual.stateMapEndOffset() == actual.endOffset();
    }

    private static NereusException fenced(String message) {
        return new NereusException(ErrorCode.FENCED_APPEND, false, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
