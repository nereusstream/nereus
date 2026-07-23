/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.checkpoint;

import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatV1;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointObject;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointUploadIdentity;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointVerifier;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointWriter;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

/** Cross-shard-safe NKC1 publication: pending protection -> PUT/verify -> binding CAS -> permanent protection. */
public final class KafkaCheckpointPublicationCoordinator {
    private static final int MAX_CAS_RETRIES = 32;
    private static final String AUTHORITY_TYPE = "kafka-partition-leader-v1";

    private final KafkaPartitionMetadataStore bindings;
    private final KafkaCheckpointWriter writer;
    private final KafkaCheckpointVerifier verifier;
    private final ObjectProtectionManager protections;
    private final Clock clock;

    public KafkaCheckpointPublicationCoordinator(
            KafkaPartitionMetadataStore bindings,
            KafkaCheckpointWriter writer,
            KafkaCheckpointVerifier verifier,
            ObjectProtectionManager protections,
            Clock clock) {
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.verifier = Objects.requireNonNull(verifier, "verifier");
        this.protections = Objects.requireNonNull(protections, "protections");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<KafkaCheckpointObject> publish(
            KafkaCheckpointPublicationRequest request) {
        Objects.requireNonNull(request, "request");
        validateCapture(request);
        AtomicReference<ObjectProtection> pending = new AtomicReference<>();
        AtomicReference<PhysicalObjectIdentity> physical = new AtomicReference<>();
        CompletableFuture<KafkaCheckpointObject> pipeline = writer.write(
                        request.objectRequest(), upload -> acquirePending(request, upload, pending, physical))
                .thenCompose(object -> {
                    PhysicalObjectIdentity exactPhysical = requirePhysical(object, physical.get());
                    verifier.verifyRecoveryWindow(
                            object,
                            request.identity().kafkaClusterId(),
                            request.identity().topicId(),
                            request.identity().partition(),
                            request.capturedBinding().value().incarnation(),
                            request.capturedBinding().value().streamId(),
                            request.capturedBinding().value().payloadMappingId(),
                            request.capturedSource().trimOffset(),
                            request.capturedSource().endOffset());
                    KafkaCheckpointReferenceRecord reference = reference(
                            object, request.writerBuild(), clock.millis());
                    return publishReference(request, reference, 0)
                            .thenCompose(published -> acquirePermanent(
                                    published, reference, exactPhysical))
                            .thenCompose(permanent -> releasePublishedPending(
                                    request.identity(), pending.get(), reference, permanent))
                            .thenApply(ignored -> object);
                });
        return pipeline.exceptionallyCompose(failure -> cleanupFailedPending(
                        request, pending.get(), physical.get())
                .handle((ignored, cleanupFailure) -> {
                    Throwable exact = unwrap(failure);
                    if (cleanupFailure != null) exact.addSuppressed(unwrap(cleanupFailure));
                    throw new CompletionException(exact);
                }));
    }

    private CompletableFuture<Void> acquirePending(
            KafkaCheckpointPublicationRequest request,
            KafkaCheckpointUploadIdentity upload,
            AtomicReference<ObjectProtection> pending,
            AtomicReference<PhysicalObjectIdentity> physical) {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                upload.objectKey(), Optional.of(upload.objectId()),
                PhysicalObjectKind.KAFKA_PARTITION_CHECKPOINT, upload.objectLength(),
                upload.storageCrc32c(), Optional.of(upload.objectSha256()), Optional.empty());
        physical.set(object);
        ObjectProtectionOwner owner = owner(request.capturedBinding());
        long expiry;
        try {
            expiry = Math.addExact(clock.millis(), request.pendingProtectionTtl().toMillis());
        } catch (ArithmeticException failure) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "Kafka checkpoint pending protection expiry overflows", failure));
        }
        ObjectProtectionRequest protectionRequest = new ObjectProtectionRequest(
                object,
                ObjectProtectionType.KAFKA_CHECKPOINT_PENDING,
                KafkaCheckpointFormatV1.attemptId(
                        request.objectRequest().header(), request.objectRequest().contentPolicySha256()),
                owner,
                expiry);
        return protections.acquireOrTransfer(
                        protectionRequest,
                        actual -> requireExactRoot(request.capturedBinding(), owner, actual))
                .thenAccept(pending::set);
    }

    private CompletableFuture<VersionedKafkaPartitionBinding> publishReference(
            KafkaCheckpointPublicationRequest request,
            KafkaCheckpointReferenceRecord reference,
            int attempt) {
        if (attempt >= MAX_CAS_RETRIES) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED, true,
                    "Kafka checkpoint root CAS retry budget exhausted"));
        }
        return bindings.get(request.identity().durableId()).thenCompose(optional -> {
            VersionedKafkaPartitionBinding current = optional.orElseThrow(() -> invariant(
                    "Kafka binding disappeared during checkpoint publication"));
            validateBinding(request, current);
            Optional<KafkaCheckpointReferenceRecord> existing = current.value().checkpointReferences().stream()
                    .filter(value -> value.objectId().equals(reference.objectId()))
                    .findFirst();
            if (existing.isPresent()) {
                requireSameReference(existing.orElseThrow(), reference);
                return CompletableFuture.completedFuture(current);
            }
            return revalidateSource(request).thenCompose(source -> {
                var update = KafkaPartitionMetadataTransitions.prependCheckpoint(
                        current.value(), reference, source.trimOffset(), source.endOffset(), clock.millis());
                return bindings.compareAndSet(current, update)
                        .exceptionallyCompose(failure -> conditionFailure(failure)
                                ? publishReference(request, reference, attempt + 1)
                                : CompletableFuture.failedFuture(unwrap(failure)));
            });
        });
    }

    private CompletableFuture<KafkaCheckpointSourceState> revalidateSource(
            KafkaCheckpointPublicationRequest request) {
        KafkaCheckpointHeader header = request.objectRequest().header();
        return request.sourceValidator().loadCurrent().thenCompose(current -> {
            KafkaCheckpointSourceState captured = request.capturedSource();
            if (!captured.sameSession(current)
                    || current.trimOffset() > header.checkpointOffset()
                    || current.endOffset() < header.checkpointOffset()
                    || current.commitVersion() < header.sourceCommitVersion()) {
                return CompletableFuture.failedFuture(invariant(
                        "Kafka checkpoint source authority or committed window changed unsafely"));
            }
            if (current.commitVersion() == header.sourceCommitVersion()) {
                if (!current.lastCommitId().equals(header.sourceLastCommitId())
                        || !current.headSha256().equals(header.sourceHeadSha256())) {
                    return CompletableFuture.failedFuture(invariant(
                            "Kafka checkpoint source head changed at the same commit version"));
                }
                return CompletableFuture.completedFuture(current);
            }
            return request.sourceValidator().isSourceCommitReachable(header, current).thenCompose(reachable ->
                    reachable
                            ? CompletableFuture.completedFuture(current)
                            : CompletableFuture.failedFuture(invariant(
                                    "Kafka checkpoint source commit is no longer reachable")));
        });
    }

    private CompletableFuture<ObjectProtection> acquirePermanent(
            VersionedKafkaPartitionBinding published,
            KafkaCheckpointReferenceRecord reference,
            PhysicalObjectIdentity physical) {
        ObjectProtectionOwner owner = owner(published);
        ObjectProtectionRequest request = new ObjectProtectionRequest(
                physical,
                ObjectProtectionType.KAFKA_CHECKPOINT_ROOT,
                reference.objectId(),
                owner,
                0);
        return protections.acquireOrTransfer(
                request, actual -> requireExactRoot(published, owner, actual));
    }

    private CompletableFuture<Void> releasePublishedPending(
            com.nereusstream.kafka.partition.KafkaPartitionIdentity identity,
            ObjectProtection pending,
            KafkaCheckpointReferenceRecord reference,
            ObjectProtection permanent) {
        if (pending == null) return CompletableFuture.failedFuture(invariant(
                "Kafka checkpoint upload completed without pending protection"));
        return protections.revalidate(
                        permanent,
                        ignored -> requireReferenced(identity, reference))
                .thenCompose(ignored -> protections.release(
                        pending,
                        actual -> requireReferenced(identity, reference)));
    }

    private CompletableFuture<Void> cleanupFailedPending(
            KafkaCheckpointPublicationRequest request,
            ObjectProtection pending,
            PhysicalObjectIdentity physical) {
        if (pending == null || physical == null) return CompletableFuture.completedFuture(null);
        return bindings.get(request.identity().durableId()).thenCompose(optional -> {
            boolean referenced = optional.stream().flatMap(value -> value.value().checkpointReferences().stream())
                    .anyMatch(reference -> reference.objectId().equals(
                            physical.objectId().orElseThrow().value()));
            if (referenced) return CompletableFuture.completedFuture(null);
            return protections.release(pending, ignored -> bindings.get(request.identity().durableId())
                    .thenAccept(latest -> {
                        boolean nowReferenced = latest.stream()
                                .flatMap(value -> value.value().checkpointReferences().stream())
                                .anyMatch(reference -> reference.objectId().equals(
                                        physical.objectId().orElseThrow().value()));
                        if (nowReferenced) throw new KafkaMetadataConditionFailedException(
                                "Kafka checkpoint became referenced during pending cleanup");
                    }));
        });
    }

    private CompletableFuture<Void> requireReferenced(
            com.nereusstream.kafka.partition.KafkaPartitionIdentity identity,
            KafkaCheckpointReferenceRecord expected) {
        return bindings.get(identity.durableId()).thenAccept(optional -> {
            VersionedKafkaPartitionBinding current = optional.orElseThrow(() -> invariant(
                    "Kafka checkpoint binding disappeared while revalidating protection"));
            KafkaCheckpointReferenceRecord actual = current.value().checkpointReferences().stream()
                    .filter(reference -> reference.objectId().equals(expected.objectId()))
                    .findFirst()
                    .orElseThrow(() -> new KafkaMetadataConditionFailedException(
                            "Kafka checkpoint root reference changed"));
            requireSameReference(actual, expected);
        });
    }

    private CompletableFuture<Void> requireExactRoot(
            VersionedKafkaPartitionBinding expected,
            ObjectProtectionOwner expectedOwner,
            ObjectProtectionOwner actualOwner) {
        if (!actualOwner.equals(expectedOwner)) {
            return CompletableFuture.failedFuture(invariant("Kafka checkpoint protection owner changed"));
        }
        return bindings.get(expected.value().identity()).thenAccept(current -> {
            if (!current.equals(Optional.of(expected))) {
                throw new KafkaMetadataConditionFailedException(
                        "Kafka checkpoint protection owner root changed");
            }
        });
    }

    private static void validateCapture(KafkaCheckpointPublicationRequest request) {
        VersionedKafkaPartitionBinding binding = request.capturedBinding();
        validateBinding(request, binding);
        KafkaCheckpointHeader header = request.objectRequest().header();
        KafkaCheckpointSourceState source = request.capturedSource();
        AppendAuthority authority = source.authority();
        String authorityId = request.identity().durableId().canonicalIdentity();
        if (!authority.authorityType().equals(AUTHORITY_TYPE)
                || !authority.authorityId().equals(authorityId)
                || authority.authorityEpoch() != header.leaderEpoch()
                || source.appendInFlight()
                || source.stateMapEndOffset() != source.endOffset()
                || header.checkpointOffset() != source.endOffset()
                || header.logStartOffset() != source.trimOffset()
                || header.stableEndOffset() != source.endOffset()
                || header.sourceCommitVersion() != source.commitVersion()
                || !header.sourceLastCommitId().equals(source.lastCommitId())
                || !header.sourceHeadSha256().equals(source.headSha256())) {
            throw invariant("Kafka checkpoint capture is not an exact quiescent partition snapshot");
        }
    }

    private static void validateBinding(
            KafkaCheckpointPublicationRequest request,
            VersionedKafkaPartitionBinding binding) {
        KafkaCheckpointHeader header = request.objectRequest().header();
        var value = binding.value();
        if (!value.identity().equals(request.identity().durableId())
                || value.lifecycle() != KafkaPartitionLifecycle.ACTIVE
                || !value.kafkaClusterId().equals(header.kafkaClusterId())
                || !value.topicId().equals(header.topicId())
                || value.partitionId() != header.partitionId()
                || value.incarnation() != header.incarnation()
                || !value.streamId().equals(header.streamId().value())
                || value.payloadMappingId() != header.payloadMappingId()) {
            throw invariant("Kafka checkpoint binding identity is not exact and ACTIVE");
        }
    }

    private static KafkaCheckpointReferenceRecord reference(
            KafkaCheckpointObject object, String writerBuild, long now) {
        KafkaCheckpointHeader header = object.header();
        return new KafkaCheckpointReferenceRecord(
                1,
                object.objectId().value(),
                object.objectKey().value(),
                object.objectLength(),
                HexFormat.of().parseHex(object.objectSha256().value()),
                header.checkpointOffset(),
                header.logStartOffset(),
                header.sourceCommitVersion(),
                HexFormat.of().parseHex(header.sourceHeadSha256().value()),
                writerBuild,
                now);
    }

    private static PhysicalObjectIdentity requirePhysical(
            KafkaCheckpointObject object, PhysicalObjectIdentity physical) {
        if (physical == null
                || !physical.objectKey().equals(object.objectKey())
                || physical.objectId().filter(object.objectId()::equals).isEmpty()
                || physical.objectLength() != object.objectLength()
                || !physical.storageChecksum().equals(object.storageCrc32c())
                || physical.contentSha256().filter(object.objectSha256()::equals).isEmpty()) {
            throw invariant("verified NKC1 differs from its pre-upload physical identity");
        }
        return physical;
    }

    private static void requireSameReference(
            KafkaCheckpointReferenceRecord existing,
            KafkaCheckpointReferenceRecord requested) {
        if (!existing.objectKey().equals(requested.objectKey())
                || existing.objectLength() != requested.objectLength()
                || !java.util.Arrays.equals(existing.objectSha256(), requested.objectSha256())
                || existing.checkpointOffset() != requested.checkpointOffset()
                || existing.logStartOffsetAtCheckpoint() != requested.logStartOffsetAtCheckpoint()
                || existing.sourceCommitVersion() != requested.sourceCommitVersion()
                || !java.util.Arrays.equals(existing.sourceHeadSha256(), requested.sourceHeadSha256())) {
            throw invariant("Kafka checkpoint object ID conflicts with an existing root reference");
        }
    }

    private static ObjectProtectionOwner owner(VersionedKafkaPartitionBinding binding) {
        return new ObjectProtectionOwner(
                binding.key(), binding.metadataVersion(), binding.durableValueSha256());
    }

    private static boolean conditionFailure(Throwable failure) {
        return unwrap(failure) instanceof KafkaMetadataConditionFailedException;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) current = current.getCause();
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
