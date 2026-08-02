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

import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointPublicationFactory;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointState;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointPublicationCoordinator;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointPublicationRequest;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceValidator;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryCoordinator;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryRequest;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointObject;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Concrete retention adapters for existing-checkpoint recovery verification and canonical
 * publication.
 */
public final class KafkaRetentionCheckpointServices
        implements KafkaRetentionCheckpointGate.ExistingCheckpointVerifier,
                KafkaRetentionCheckpointGate.CheckpointPublisher {
    private final KafkaCheckpointRecoveryCoordinator recovery;
    private final KafkaCanonicalCheckpointPublicationFactory publicationFactory;
    private final KafkaCheckpointPublicationCoordinator publication;
    private final KafkaPartitionMetadataStore bindings;
    private final CaptureProvider captures;
    private final Duration verificationTimeout;

    public KafkaRetentionCheckpointServices(
            KafkaCheckpointRecoveryCoordinator recovery,
            KafkaCanonicalCheckpointPublicationFactory publicationFactory,
            KafkaCheckpointPublicationCoordinator publication,
            KafkaPartitionMetadataStore bindings,
            CaptureProvider captures,
            Duration verificationTimeout) {
        this.recovery = Objects.requireNonNull(recovery, "recovery");
        this.publicationFactory = Objects.requireNonNull(publicationFactory, "publicationFactory");
        this.publication = Objects.requireNonNull(publication, "publication");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.captures = Objects.requireNonNull(captures, "captures");
        this.verificationTimeout = positive(verificationTimeout);
    }

    @Override
    public CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> verify(
            KafkaTrimBarrier.Snapshot snapshot, KafkaCheckpointReferenceRecord reference) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(reference, "reference");
        return capture(snapshot)
                .thenCompose(capture -> recovery.recoverReference(
                        new KafkaCheckpointRecoveryRequest(
                                snapshot.identity(),
                                snapshot.binding(),
                                capture.source(),
                                capture.sourceValidator(),
                                verificationTimeout),
                        reference))
                .thenApply(recovered -> {
                    if (!recovered.reference().equals(reference)) {
                        throw invariant("recovered Kafka retention checkpoint differs from requested root");
                    }
                    return new KafkaTrimBarrier.VerifiedCheckpoint(reference, reference.objectSha256());
                });
    }

    @Override
    public CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> publish(KafkaTrimBarrier.Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        return capture(snapshot)
                .thenApply(capture -> publicationFactory.create(
                        snapshot.identity(),
                        snapshot.binding(),
                        capture.source(),
                        capture.canonicalState(),
                        capture.leaderEpoch(),
                        capture.sourceValidator()))
                .thenCompose(request -> publication.publish(request).thenCompose(object -> bindings.get(
                                snapshot.identity().durableId())
                        .thenApply(current -> verifiedPublished(
                                snapshot,
                                request,
                                object,
                                current.orElseThrow(() ->
                                        invariant("Kafka binding disappeared after checkpoint" + " publication"))))));
    }

    private CompletableFuture<Capture> capture(KafkaTrimBarrier.Snapshot snapshot) {
        CompletableFuture<Capture> loaded;
        try {
            loaded = Objects.requireNonNull(
                    captures.capture(snapshot), "Kafka retention checkpoint capture provider returned a null future");
        } catch (Throwable failure) {
            loaded = CompletableFuture.failedFuture(failure);
        }
        return loaded.thenApply(capture -> validateCapture(snapshot, capture));
    }

    private static Capture validateCapture(KafkaTrimBarrier.Snapshot snapshot, Capture capture) {
        Objects.requireNonNull(capture, "capture");
        KafkaCheckpointSourceState source = capture.source();
        KafkaCanonicalCheckpointState state = capture.canonicalState();
        var acquired = snapshot.sourceHead().appendSession().orElseThrow();
        AppendSession session = acquired.session();
        if (capture.leaderEpoch() != snapshot.binding().value().observedLeaderEpoch()
                || !source.authority().equals(acquired.authority().orElseThrow())
                || !source.writerId().equals(session.writerId())
                || source.sessionEpoch() != session.epoch()
                || !source.fencingToken().equals(session.fencingToken())
                || source.leaseVersion() != session.leaseVersion()
                || source.trimOffset() != snapshot.sourceHead().trimOffset()
                || source.endOffset() != snapshot.sourceHead().committedEndOffset()
                || source.commitVersion() != snapshot.sourceHead().commitVersion()
                || !source.lastCommitId().equals(snapshot.sourceHead().lastCommitId())
                || !source.headSha256().equals(snapshot.sourceHead().durableHeadSha256())
                || source.appendInFlight()
                || source.stateMapEndOffset() != source.endOffset()
                || state.logStartOffset() != source.trimOffset()
                || state.stableEndOffset() != source.endOffset()
                || !state.virtualSegmentState().equals(snapshot.retention().virtualSegments())) {
            throw invariant("Kafka retention checkpoint capture does not match the frozen partition view");
        }
        return capture;
    }

    private static KafkaTrimBarrier.VerifiedCheckpoint verifiedPublished(
            KafkaTrimBarrier.Snapshot snapshot,
            KafkaCheckpointPublicationRequest request,
            KafkaCheckpointObject object,
            VersionedKafkaPartitionBinding current) {
        KafkaCheckpointReferenceRecord reference = current.value().checkpointReferences().stream()
                .filter(candidate ->
                        candidate.objectId().equals(object.objectId().value()))
                .findFirst()
                .orElseThrow(() -> invariant("published Kafka retention checkpoint is absent from binding root"));
        if (!current.value().identity().equals(snapshot.identity().durableId())
                || !object.header().equals(request.objectRequest().header())
                || !reference.objectKey().equals(object.objectKey().value())
                || reference.objectLength() != object.objectLength()
                || !Arrays.equals(
                        reference.objectSha256(),
                        HexFormat.of().parseHex(object.objectSha256().value()))
                || reference.checkpointOffset() != object.header().checkpointOffset()
                || reference.logStartOffsetAtCheckpoint() != object.header().logStartOffset()
                || reference.sourceCommitVersion() != object.header().sourceCommitVersion()
                || !Arrays.equals(
                        reference.sourceHeadSha256(),
                        HexFormat.of()
                                .parseHex(object.header().sourceHeadSha256().value()))) {
            throw invariant("published Kafka retention checkpoint conflicts with its authoritative root");
        }
        return new KafkaTrimBarrier.VerifiedCheckpoint(
                reference, HexFormat.of().parseHex(object.objectSha256().value()));
    }

    private static Duration positive(Duration value) {
        Objects.requireNonNull(value, "verificationTimeout");
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException("Kafka retention checkpoint verification timeout must be positive");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    @FunctionalInterface
    public interface CaptureProvider {
        CompletableFuture<Capture> capture(KafkaTrimBarrier.Snapshot snapshot);
    }

    public record Capture(
            KafkaCheckpointSourceState source,
            KafkaCanonicalCheckpointState canonicalState,
            int leaderEpoch,
            KafkaCheckpointSourceValidator sourceValidator) {
        public Capture {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(canonicalState, "canonicalState");
            Objects.requireNonNull(sourceValidator, "sourceValidator");
            if (leaderEpoch < 0) {
                throw new IllegalArgumentException("Kafka retention checkpoint leader epoch must be non-negative");
            }
        }
    }
}
