/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointObject;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointReader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointVerifier;
import java.time.Clock;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Newest-first exact-key checkpoint recovery with durable reader pins and bounded fallback. */
public final class KafkaCheckpointRecoveryCoordinator {
    @FunctionalInterface
    public interface FailureObserver {
        void unusable(KafkaCheckpointReferenceRecord reference, Throwable failure);
    }

    private final String nereusCluster;
    private final KafkaPartitionMetadataStore bindings;
    private final PhysicalObjectMetadataStore physicalStore;
    private final ObjectReadPinManager readPins;
    private final KafkaCheckpointReader reader;
    private final KafkaCheckpointVerifier verifier;
    private final Clock clock;
    private final FailureObserver observer;

    public KafkaCheckpointRecoveryCoordinator(
            String nereusCluster,
            KafkaPartitionMetadataStore bindings,
            PhysicalObjectMetadataStore physicalStore,
            ObjectReadPinManager readPins,
            KafkaCheckpointReader reader,
            KafkaCheckpointVerifier verifier,
            Clock clock,
            FailureObserver observer) {
        this.nereusCluster = requireText(nereusCluster, "nereusCluster");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.physicalStore = Objects.requireNonNull(physicalStore, "physicalStore");
        this.readPins = Objects.requireNonNull(readPins, "readPins");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.verifier = Objects.requireNonNull(verifier, "verifier");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.observer = Objects.requireNonNull(observer, "observer");
    }

    public CompletableFuture<KafkaCheckpointRecoveryResult> recover(
            KafkaCheckpointRecoveryRequest request) {
        Objects.requireNonNull(request, "request");
        validateRequest(request);
        long deadline;
        try {
            deadline = Math.addExact(clock.millis(), request.timeout().toMillis());
        } catch (ArithmeticException failure) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "Kafka checkpoint recovery deadline overflows", failure));
        }
        List<KafkaCheckpointReferenceRecord> references = request.binding().value().checkpointReferences();
        return tryReference(request, references, 0, deadline).thenCompose(result -> {
            if (result.isPresent()) {
                return CompletableFuture.completedFuture(
                        new KafkaCheckpointRecoveryResult(result));
            }
            if (request.currentSource().trimOffset() == 0) {
                return CompletableFuture.completedFuture(KafkaCheckpointRecoveryResult.fullReplay());
            }
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "trimmed Kafka partition has no usable recovery checkpoint"));
        });
    }

    private CompletableFuture<Optional<KafkaRecoveredCheckpoint>> tryReference(
            KafkaCheckpointRecoveryRequest request,
            List<KafkaCheckpointReferenceRecord> references,
            int index,
            long deadline) {
        if (index == references.size()) return CompletableFuture.completedFuture(Optional.empty());
        KafkaCheckpointReferenceRecord reference = references.get(index);
        return recoverOne(request, reference, deadline)
                .thenApply(Optional::of)
                .exceptionallyCompose(failure -> {
                    Throwable exact = unwrap(failure);
                    if (!canFallback(exact)) {
                        return CompletableFuture.failedFuture(exact);
                    }
                    observer.unusable(reference, exact);
                    return tryReference(request, references, index + 1, deadline);
                });
    }

    private CompletableFuture<KafkaRecoveredCheckpoint> recoverOne(
            KafkaCheckpointRecoveryRequest request,
            KafkaCheckpointReferenceRecord reference,
            long deadline) {
        return loadPhysical(reference).thenCompose(physical -> readPins.acquire(
                        physical,
                        deadline,
                        () -> requireReferenced(request.identity().durableId(), reference))
                .thenCompose(lease -> readPinned(request, reference, physical, lease, deadline)));
    }

    private CompletableFuture<KafkaRecoveredCheckpoint> readPinned(
            KafkaCheckpointRecoveryRequest request,
            KafkaCheckpointReferenceRecord reference,
            PhysicalObjectIdentity physical,
            ObjectReadLease lease,
            long deadline) {
        long remaining = deadline - clock.millis();
        if (remaining <= 0) {
            return releaseAfter(lease, CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.TIMEOUT, true, "Kafka checkpoint recovery deadline expired")));
        }
        CompletableFuture<KafkaRecoveredCheckpoint> operation = reader.openAndVerify(
                        physical.objectKey(), physical.objectLength(), physical.storageChecksum(),
                        physical.contentSha256().orElseThrow(), java.time.Duration.ofMillis(remaining))
                .thenCompose(object -> validateObject(request, reference, object))
                .thenCompose(object -> requireReferenced(request.identity().durableId(), reference)
                        .thenApply(ignored -> new KafkaRecoveredCheckpoint(
                                reference, object.header(), object.sections())));
        return releaseAfter(lease, operation);
    }

    private CompletableFuture<KafkaCheckpointObject> validateObject(
            KafkaCheckpointRecoveryRequest request,
            KafkaCheckpointReferenceRecord reference,
            KafkaCheckpointObject object) {
        if (!object.objectId().value().equals(reference.objectId())
                || !object.objectKey().value().equals(reference.objectKey())
                || object.objectLength() != reference.objectLength()
                || !Arrays.equals(
                        HexFormat.of().parseHex(object.objectSha256().value()), reference.objectSha256())
                || object.header().checkpointOffset() != reference.checkpointOffset()
                || object.header().logStartOffset() != reference.logStartOffsetAtCheckpoint()
                || object.header().sourceCommitVersion() != reference.sourceCommitVersion()
                || !Arrays.equals(
                        HexFormat.of().parseHex(object.header().sourceHeadSha256().value()),
                        reference.sourceHeadSha256())) {
            return CompletableFuture.failedFuture(invariant(
                    "NKC1 object conflicts with its authoritative binding reference"));
        }
        verifier.verifyRecoveryWindow(
                object,
                request.identity().kafkaClusterId(),
                request.identity().topicId(),
                request.identity().partition(),
                request.binding().value().incarnation(),
                request.binding().value().streamId(),
                request.binding().value().payloadMappingId(),
                request.currentSource().trimOffset(),
                request.currentSource().endOffset());
        KafkaCheckpointHeader header = object.header();
        KafkaCheckpointSourceState current = request.currentSource();
        if (current.commitVersion() == header.sourceCommitVersion()) {
            if (!current.lastCommitId().equals(header.sourceLastCommitId())
                    || !current.headSha256().equals(header.sourceHeadSha256())) {
                return CompletableFuture.failedFuture(invariant(
                        "NKC1 source anchor conflicts at the same commit version"));
            }
            return CompletableFuture.completedFuture(object);
        }
        if (current.commitVersion() < header.sourceCommitVersion()) {
            return CompletableFuture.failedFuture(invariant(
                    "NKC1 source commit is ahead of the current stream head"));
        }
        return request.sourceValidator().isSourceCommitReachable(header, current).thenCompose(reachable ->
                reachable
                        ? CompletableFuture.completedFuture(object)
                        : CompletableFuture.failedFuture(invariant(
                                "NKC1 source commit is not reachable from the current stream head")));
    }

    private CompletableFuture<PhysicalObjectIdentity> loadPhysical(
            KafkaCheckpointReferenceRecord reference) {
        ObjectKey key = new ObjectKey(reference.objectKey());
        ObjectKeyHash hash = ObjectKeyHash.from(key);
        return physicalStore.getRoot(nereusCluster, hash).thenApply(optional -> {
            var root = optional.orElseThrow(() -> new NereusException(
                    ErrorCode.OBJECT_NOT_FOUND, true,
                    "Kafka checkpoint physical root is absent"));
            PhysicalObjectIdentity physical = PhysicalObjectIdentity.from(root.value());
            Checksum expectedSha = new Checksum(
                    ChecksumType.SHA256, HexFormat.of().formatHex(reference.objectSha256()));
            if (root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                    || !physical.objectKey().equals(key)
                    || physical.objectId().filter(id -> id.equals(new ObjectId(reference.objectId()))).isEmpty()
                    || physical.kind() != PhysicalObjectKind.KAFKA_PARTITION_CHECKPOINT
                    || physical.objectLength() != reference.objectLength()
                    || physical.contentSha256().filter(expectedSha::equals).isEmpty()) {
                throw invariant("Kafka checkpoint physical root is not exact and ACTIVE");
            }
            return physical;
        });
    }

    private CompletableFuture<Void> requireReferenced(
            com.nereusstream.metadata.oxia.KafkaPartitionId identity,
            KafkaCheckpointReferenceRecord expected) {
        return bindings.get(identity).thenAccept(optional -> {
            VersionedKafkaPartitionBinding current = optional.orElseThrow(() -> invariant(
                    "Kafka binding disappeared while revalidating a checkpoint read"));
            KafkaCheckpointReferenceRecord actual = current.value().checkpointReferences().stream()
                    .filter(reference -> reference.objectId().equals(expected.objectId()))
                    .findFirst()
                    .orElseThrow(() -> invariant("Kafka checkpoint is no longer referenced"));
            if (!actual.equals(expected)) {
                throw invariant("Kafka checkpoint reference changed while pinned");
            }
        });
    }

    private static <T> CompletableFuture<T> releaseAfter(
            ObjectReadLease lease, CompletableFuture<T> operation) {
        return operation.handle((value, failure) -> lease.release().handle((ignored, releaseFailure) -> {
            if (failure == null && releaseFailure == null) return value;
            Throwable exact = failure == null ? unwrap(releaseFailure) : unwrap(failure);
            if (releaseFailure != null && failure != null) exact.addSuppressed(unwrap(releaseFailure));
            throw new CompletionException(exact);
        })).thenCompose(value -> value);
    }

    private static void validateRequest(KafkaCheckpointRecoveryRequest request) {
        VersionedKafkaPartitionBinding binding = request.binding();
        if (!binding.value().identity().equals(request.identity().durableId())
                || binding.value().lifecycle() != KafkaPartitionLifecycle.ACTIVE) {
            throw invariant("Kafka checkpoint recovery requires the exact ACTIVE binding");
        }
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) current = current.getCause();
        return current;
    }

    private static boolean canFallback(Throwable failure) {
        if (!(failure instanceof NereusException nereus)) return false;
        return switch (nereus.code()) {
            case OBJECT_NOT_FOUND,
                    OBJECT_CHECKSUM_MISMATCH,
                    UNSUPPORTED_FORMAT,
                    METADATA_INVARIANT_VIOLATION -> true;
            default -> false;
        };
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
