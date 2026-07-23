/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.metadata;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.metadata.oxia.AppendAuthoritySessionTransitions;
import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.KafkaPartitionKeyspace;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Restart-safe deterministic binding creation and deletion; no cross-key atomicity is assumed. */
public final class KafkaPartitionLifecycleCoordinator {
    private static final int MAX_RECONCILE_RETRIES = 64;
    public static final String PROTOCOL_OWNER_ATTRIBUTE = "nereus.protocol.owner";
    public static final String KAFKA_CLUSTER_ATTRIBUTE = "nereus.kafka.cluster.id";
    public static final String TOPIC_ID_ATTRIBUTE = "nereus.kafka.topic.id";
    public static final String PARTITION_ATTRIBUTE = "nereus.kafka.partition";
    public static final String PAYLOAD_MAPPING_ATTRIBUTE = "nereus.kafka.payload.mapping";

    private final KafkaPartitionMetadataStore bindings;
    private final StreamStorage streams;
    private final KafkaPartitionKeyspace keys;
    private final Clock clock;

    public KafkaPartitionLifecycleCoordinator(
            KafkaPartitionMetadataStore bindings,
            StreamStorage streams,
            KafkaPartitionKeyspace keys,
            Clock clock) {
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.streams = Objects.requireNonNull(streams, "streams");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<KafkaPartitionBinding> ensureBinding(KafkaBindingRequest request) {
        Objects.requireNonNull(request, "request");
        return ensureBinding(request, 0);
    }

    private CompletableFuture<KafkaPartitionBinding> ensureBinding(
            KafkaBindingRequest request, int attempt) {
        if (attempt >= MAX_RECONCILE_RETRIES) {
            return NereusException.failedFuture(
                    ErrorCode.METADATA_CONDITION_FAILED, true,
                    "Kafka binding reconciliation retry budget exhausted");
        }
        KafkaPartitionId id = request.identity().durableId();
        return bindings.get(id).thenCompose(optional -> optional
                .map(root -> reconcileExisting(request, root, attempt))
                .orElseGet(() -> createRoot(request, attempt)));
    }

    private CompletableFuture<KafkaPartitionBinding> createRoot(
            KafkaBindingRequest request, int attempt) {
        long now = clock.millis();
        KafkaPartitionId id = request.identity().durableId();
        String attemptId = KafkaPartitionMetadataTransitions.deterministicCreateAttemptId(
                id, request.metadataOffset());
        KafkaPartitionPendingOperationRecord operation = operation(
                KafkaPartitionOperationType.CREATE, attemptId, request, now);
        KafkaPartitionBindingRecord creating = KafkaPartitionMetadataTransitions.creating(
                id, request.identity().observedTopicName(), request.storageProfile().name(),
                request.metadataOffset(), now, operation);
        return bindings.putCreatingIfAbsent(creating)
                .thenCompose(root -> reconcileExisting(request, root, attempt))
                .exceptionallyCompose(failure -> retryCondition(request, attempt, failure));
    }

    private CompletableFuture<KafkaPartitionBinding> reconcileExisting(
            KafkaBindingRequest request,
            VersionedKafkaPartitionBinding root,
            int attempt) {
        KafkaPartitionBindingRecord value = root.value();
        if (!value.identity().equals(request.identity().durableId())) {
            return invariant("Kafka binding lookup returned another identity");
        }
        if (!value.storageProfile().equals(request.storageProfile().name())) {
            return corruptAndFail(root, "BINDING_PROFILE_MISMATCH", "Kafka binding storage profile changed");
        }
        return switch (value.lifecycle()) {
            case ACTIVE -> publishHint(request.identity(), root).thenApply(ignored -> toBinding(request.identity(), root));
            case CREATING -> createOrVerifyStream(request, root, attempt);
            case DELETING, DELETED -> invariant("Kafka partition binding is deleted or deleting");
            case CORRUPT -> invariant("Kafka partition binding is CORRUPT");
        };
    }

    private CompletableFuture<KafkaPartitionBinding> createOrVerifyStream(
            KafkaBindingRequest request,
            VersionedKafkaPartitionBinding root,
            int attempt) {
        long now = clock.millis();
        String expectedAttempt = KafkaPartitionMetadataTransitions.deterministicCreateAttemptId(
                root.value().identity(), root.value().createdMetadataOffset());
        KafkaPartitionPendingOperationRecord current = root.value().pendingOperation();
        CompletableFuture<VersionedKafkaPartitionBinding> claimed;
        if (!current.isEmpty() && current.attemptId().equals(expectedAttempt)
                && current.ownerId().equals(request.operationOwnerId())
                && current.ownerEpoch() == request.operationOwnerEpoch()
                && current.leaseExpiresAtMillis() > now) {
            claimed = CompletableFuture.completedFuture(root);
        } else {
            claimed = bindings.claimOperation(
                    root,
                    operation(KafkaPartitionOperationType.CREATE, expectedAttempt, request, now),
                    now);
        }
        return claimed.thenCompose(owner -> {
            StreamName streamName = new StreamName(KafkaPartitionMetadataTransitions.deterministicStreamName(
                    owner.value().identity(), owner.value().incarnation()));
            Map<String, String> attributes = streamAttributes(owner.value().identity());
            return streams.createOrGetStream(
                            streamName, new StreamCreateOptions(request.storageProfile(), attributes))
                    .thenCompose(metadata -> activate(request, owner, streamName, attributes, metadata, attempt));
        }).exceptionallyCompose(failure -> retryCondition(request, attempt, failure));
    }

    private CompletableFuture<KafkaPartitionBinding> activate(
            KafkaBindingRequest request,
            VersionedKafkaPartitionBinding owner,
            StreamName streamName,
            Map<String, String> expectedAttributes,
            StreamMetadata stream,
            int attempt) {
        if (!stream.streamName().equals(streamName)
                || stream.state() != StreamState.ACTIVE
                || stream.profile().canonical() != request.storageProfile()
                || !stream.attributes().equals(expectedAttributes)) {
            return corruptAndFail(owner, "STREAM_BINDING_MISMATCH",
                    "deterministic Kafka stream facts do not match binding");
        }
        KafkaPartitionBindingRecord active = KafkaPartitionMetadataTransitions.activate(
                owner.value(), streamName.value(), stream.streamId().value(),
                Math.max(owner.value().lastAppliedMetadataOffset(), request.metadataOffset()), clock.millis());
        return bindings.compareAndSet(owner, active)
                .thenCompose(winner -> publishHint(request.identity(), winner)
                        .thenApply(ignored -> toBinding(request.identity(), winner)))
                .exceptionallyCompose(failure -> retryCondition(request, attempt, failure));
    }

    public CompletableFuture<Void> delete(
            KafkaPartitionIdentity identity,
            long metadataOffset,
            String ownerId,
            long ownerEpoch,
            java.time.Duration operationTtl,
            java.time.Duration streamTimeout) {
        KafkaBindingRequest request = new KafkaBindingRequest(
                identity, com.nereusstream.api.StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                metadataOffset, ownerId, ownerEpoch, operationTtl);
        return bindings.get(identity.durableId()).thenCompose(optional -> {
            VersionedKafkaPartitionBinding root = optional.orElseThrow(() ->
                    new NereusException(ErrorCode.STREAM_NOT_FOUND, false, "Kafka binding is absent"));
            return delete(request, root, streamTimeout, 0);
        });
    }

    private CompletableFuture<Void> delete(
            KafkaBindingRequest request,
            VersionedKafkaPartitionBinding root,
            java.time.Duration streamTimeout,
            int attempt) {
        if (attempt >= MAX_RECONCILE_RETRIES) {
            return NereusException.failedFuture(
                    ErrorCode.METADATA_CONDITION_FAILED, true, "Kafka delete retry budget exhausted");
        }
        if (root.value().lifecycle() == KafkaPartitionLifecycle.DELETED) {
            return CompletableFuture.completedFuture(null);
        }
        if (root.value().lifecycle() == KafkaPartitionLifecycle.CORRUPT
                || root.value().lifecycle() == KafkaPartitionLifecycle.CREATING) {
            return invariant("Kafka binding is not deletable in its current lifecycle");
        }
        CompletableFuture<VersionedKafkaPartitionBinding> deleting;
        if (root.value().lifecycle() == KafkaPartitionLifecycle.ACTIVE) {
            long now = clock.millis();
            String attemptId = KafkaPartitionMetadataTransitions.deterministicDeleteAttemptId(
                    root.value().identity(), request.metadataOffset());
            KafkaPartitionBindingRecord update = KafkaPartitionMetadataTransitions.beginDelete(
                    root.value(), operation(KafkaPartitionOperationType.DELETE, attemptId, request, now), now);
            deleting = bindings.compareAndSet(root, update);
        } else {
            deleting = CompletableFuture.completedFuture(root);
        }
        return deleting.thenCompose(owner -> {
            StreamId streamId = new StreamId(owner.value().streamId());
            return streams.getStreamMetadata(streamId).thenCompose(metadata -> {
                CompletableFuture<StreamMetadata> terminal = switch (metadata.state()) {
                    case ACTIVE -> streams.seal(streamId, new com.nereusstream.api.SealOptions(
                                    streamTimeout, "Kafka topic ID deletion"))
                            .thenCompose(ignored -> streams.delete(streamId,
                                    new com.nereusstream.api.DeleteOptions(
                                            streamTimeout, "Kafka topic ID deletion")));
                    case SEALED, DELETING -> streams.delete(streamId,
                            new com.nereusstream.api.DeleteOptions(streamTimeout, "Kafka topic ID deletion"));
                    case DELETED -> CompletableFuture.completedFuture(metadata);
                    case CREATING -> CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                            "bound stream remained CREATING during Kafka deletion"));
                };
                return terminal;
            }).thenCompose(deleted -> {
                if (deleted.state() != StreamState.DELETED) {
                    return invariant("Kafka stream deletion did not reach DELETED");
                }
                KafkaPartitionBindingRecord terminal = KafkaPartitionMetadataTransitions.markDeleted(
                        owner.value(), owner.value().pendingOperation().attemptId(), clock.millis());
                return bindings.compareAndSet(owner, terminal).thenApply(ignored -> (Void) null);
            });
        }).exceptionallyCompose(failure -> {
            if (!conditionFailure(failure)) return CompletableFuture.failedFuture(unwrap(failure));
            return bindings.get(request.identity().durableId()).thenCompose(latest ->
                    delete(request, latest.orElseThrow(), streamTimeout, attempt + 1));
        });
    }

    private CompletableFuture<Void> publishHint(
            KafkaPartitionIdentity identity,
            VersionedKafkaPartitionBinding root) {
        return bindings.putRegistryHint(new KafkaPartitionRegistryRecord(
                1, identity.kafkaClusterId(), identity.topicId(), identity.partition(),
                keys.bindingRootKey(identity.durableId()),
                HexFormat.of().parseHex(root.durableValueSha256().value()),
                root.value().lifecycleId(), root.value().bindingEpoch(), clock.millis(), 0));
    }

    private CompletableFuture<KafkaPartitionBinding> corruptAndFail(
            VersionedKafkaPartitionBinding root, String code, String message) {
        KafkaPartitionBindingRecord corrupt = KafkaPartitionMetadataTransitions.markCorrupt(
                root.value(), code, clock.millis());
        return bindings.compareAndSet(root, corrupt)
                .handle((ignored, failure) -> {
                    throw new CompletionException(new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION, false, message,
                            failure == null ? null : unwrap(failure)));
                });
    }

    private CompletableFuture<KafkaPartitionBinding> retryCondition(
            KafkaBindingRequest request, int attempt, Throwable failure) {
        Throwable cause = unwrap(failure);
        return conditionFailure(cause)
                ? ensureBinding(request, attempt + 1)
                : CompletableFuture.failedFuture(cause);
    }

    private static KafkaPartitionPendingOperationRecord operation(
            KafkaPartitionOperationType type,
            String attemptId,
            KafkaBindingRequest request,
            long now) {
        long ttl;
        try {
            ttl = request.operationTtl().toMillis();
            if (ttl <= 0) throw new ArithmeticException("sub-millisecond operation TTL");
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("operation TTL is outside millisecond range", failure);
        }
        return new KafkaPartitionPendingOperationRecord(
                type.wireId(), attemptId, request.operationOwnerId(), request.operationOwnerEpoch(),
                Math.addExact(now, ttl), request.metadataOffset(), now, "");
    }

    public static Map<String, String> streamAttributes(KafkaPartitionId id) {
        return Map.of(
                PROTOCOL_OWNER_ATTRIBUTE, "kafka-native",
                KAFKA_CLUSTER_ATTRIBUTE, id.kafkaClusterId(),
                TOPIC_ID_ATTRIBUTE, id.topicId(),
                PARTITION_ATTRIBUTE, Integer.toString(id.partitionId()),
                PAYLOAD_MAPPING_ATTRIBUTE, "KAFKA_RECORD_BATCH_V1",
                AppendAuthoritySessionTransitions.AUTHORITY_MODE_ATTRIBUTE,
                AppendAuthoritySessionTransitions.EXTERNAL_MONOTONIC_TERM_V1);
    }

    private static KafkaPartitionBinding toBinding(
            KafkaPartitionIdentity identity, VersionedKafkaPartitionBinding root) {
        return new KafkaPartitionBinding(
                identity, new StreamName(root.value().streamName()),
                new StreamId(root.value().streamId()), root);
    }

    private static boolean conditionFailure(Throwable failure) {
        return unwrap(failure) instanceof KafkaMetadataConditionFailedException;
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) current = current.getCause();
        return current;
    }

    private static <T> CompletableFuture<T> invariant(String message) {
        return NereusException.failedFuture(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
