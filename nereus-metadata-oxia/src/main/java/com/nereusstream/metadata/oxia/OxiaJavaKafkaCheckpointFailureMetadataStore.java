/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.codec.KafkaMetadataCodecs;
import com.nereusstream.metadata.oxia.codec.MetadataCodecException;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Production immutable NKC1 quarantine store over a borrowed shared Oxia runtime.
 */
public final class OxiaJavaKafkaCheckpointFailureMetadataStore implements KafkaCheckpointFailureMetadataStore {
    private final PartitionedOxiaClient client;
    private final KafkaPartitionKeyspace keys;
    private final AtomicBoolean closed = new AtomicBoolean();

    public static OxiaJavaKafkaCheckpointFailureMetadataStore usingSharedRuntime(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime,
            String nereusCluster,
            String kafkaClusterId) {
        Objects.requireNonNull(runtime, "runtime")
                .requireCompatible(Objects.requireNonNull(configuration, "configuration"));
        return new OxiaJavaKafkaCheckpointFailureMetadataStore(
                runtime.client(), new KafkaPartitionKeyspace(nereusCluster, kafkaClusterId));
    }

    OxiaJavaKafkaCheckpointFailureMetadataStore(PartitionedOxiaClient client, KafkaPartitionKeyspace keys) {
        this.client = Objects.requireNonNull(client, "client");
        this.keys = Objects.requireNonNull(keys, "keys");
    }

    @Override
    public CompletableFuture<Optional<VersionedKafkaCheckpointFailure>> get(
            KafkaPartitionId identity, long partitionIncarnation, String objectId) {
        ensureOpen();
        KafkaPartitionId exactIdentity = Objects.requireNonNull(identity, "identity");
        String key = keys.checkpointFailureKey(exactIdentity, partitionIncarnation, objectId);
        return client.get(key, keys.bindingPartitionKey(exactIdentity))
                .thenApply(optional ->
                        optional.map(stored -> failure(stored, exactIdentity, partitionIncarnation, objectId)));
    }

    @Override
    public CompletableFuture<VersionedKafkaCheckpointFailure> putIfAbsent(KafkaCheckpointFailureRecord value) {
        ensureOpen();
        KafkaCheckpointFailureRecord exact = Objects.requireNonNull(value, "value");
        if (exact.metadataVersion() != 0) {
            throw new IllegalArgumentException("checkpoint failure metadataVersion must be zero");
        }
        KafkaPartitionId identity = exact.identity();
        if (!identity.kafkaClusterId().equals(keys.kafkaClusterId())) {
            throw new IllegalArgumentException("checkpoint failure belongs to another Kafka cluster");
        }
        String key = keys.checkpointFailureKey(identity, exact.partitionIncarnation(), exact.objectId());
        byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(exact, KafkaCheckpointFailureRecord.class);
        CompletableFuture<VersionedKafkaCheckpointFailure> write = client.putIfAbsent(
                        key, bytes, keys.bindingPartitionKey(identity))
                .thenApply(result -> versioned(key, exact, result.version(), bytes));
        return write.exceptionallyCompose(failure -> reconcileCreate(exact, unwrap(failure)));
    }

    private CompletableFuture<VersionedKafkaCheckpointFailure> reconcileCreate(
            KafkaCheckpointFailureRecord requested, Throwable writeFailure) {
        return get(requested.identity(), requested.partitionIncarnation(), requested.objectId())
                .handle((optional, readFailure) -> {
                    if (readFailure != null) {
                        Throwable exactRead = unwrap(readFailure);
                        writeFailure.addSuppressed(exactRead);
                        throw new CompletionException(
                                metadataFailure("failed to reconcile Kafka checkpoint quarantine", writeFailure));
                    }
                    if (optional.isEmpty()) {
                        throw new CompletionException(
                                metadataFailure("failed to create Kafka checkpoint quarantine", writeFailure));
                    }
                    VersionedKafkaCheckpointFailure existing = optional.orElseThrow();
                    requireSameReference(requested, existing.value());
                    return existing;
                });
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private VersionedKafkaCheckpointFailure failure(
            PartitionedOxiaClient.VersionedValue stored,
            KafkaPartitionId expectedIdentity,
            long expectedIncarnation,
            String expectedObjectId) {
        try {
            KafkaCheckpointFailureRecord value = KafkaMetadataCodecs.decodeEnvelope(
                            stored.value(), KafkaCheckpointFailureRecord.class)
                    .withMetadataVersion(stored.version());
            String expectedKey = keys.checkpointFailureKey(expectedIdentity, expectedIncarnation, expectedObjectId);
            if (!stored.key().equals(expectedKey)
                    || !value.identity().equals(expectedIdentity)
                    || value.partitionIncarnation() != expectedIncarnation
                    || !value.objectId().equals(expectedObjectId)) {
                throw invariant("Kafka checkpoint failure key/value identity mismatch", null);
            }
            return versioned(stored.key(), value, stored.version(), stored.value());
        } catch (MetadataCodecException | IllegalArgumentException failure) {
            throw invariant("invalid authoritative Kafka checkpoint failure metadata", failure);
        }
    }

    private static VersionedKafkaCheckpointFailure versioned(
            String key, KafkaCheckpointFailureRecord value, long version, byte[] durableBytes) {
        return new VersionedKafkaCheckpointFailure(
                key, value.withMetadataVersion(version), version, sha256(durableBytes));
    }

    private static void requireSameReference(
            KafkaCheckpointFailureRecord requested, KafkaCheckpointFailureRecord existing) {
        if (!requested.identity().equals(existing.identity())
                || requested.partitionIncarnation() != existing.partitionIncarnation()
                || !requested.objectId().equals(existing.objectId())
                || !Arrays.equals(requested.referenceSha256(), existing.referenceSha256())) {
            throw invariant("existing Kafka checkpoint quarantine conflicts with exact reference", null);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "Kafka checkpoint failure metadata store is closed");
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException metadataFailure(String message, Throwable failure) {
        if (failure instanceof NereusException nereus) {
            return nereus;
        }
        return new NereusException(ErrorCode.METADATA_UNAVAILABLE, true, message, failure);
    }

    private static NereusException invariant(String message, Throwable failure) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, failure);
    }

    private static Checksum sha256(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of()
                            .formatHex(MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
