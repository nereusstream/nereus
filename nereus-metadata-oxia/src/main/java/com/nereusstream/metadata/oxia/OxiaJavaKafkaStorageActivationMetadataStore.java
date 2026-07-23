/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.codec.KafkaMetadataCodecs;
import com.nereusstream.metadata.oxia.codec.MetadataCodecException;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/** Production single-partition Oxia CAS store for the F9 activation control plane. */
public final class OxiaJavaKafkaStorageActivationMetadataStore
        implements KafkaStorageActivationMetadataStore {
    private final PartitionedOxiaClient client;
    private final KafkaPartitionKeyspace keys;
    private final AtomicBoolean closed = new AtomicBoolean();

    public static OxiaJavaKafkaStorageActivationMetadataStore usingSharedRuntime(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime,
            String nereusCluster,
            String kafkaClusterId) {
        Objects.requireNonNull(runtime, "runtime").requireCompatible(
                Objects.requireNonNull(configuration, "configuration"));
        return new OxiaJavaKafkaStorageActivationMetadataStore(
                runtime.client(), new KafkaPartitionKeyspace(nereusCluster, kafkaClusterId));
    }

    OxiaJavaKafkaStorageActivationMetadataStore(
            PartitionedOxiaClient client,
            KafkaPartitionKeyspace keys) {
        this.client = Objects.requireNonNull(client, "client");
        this.keys = Objects.requireNonNull(keys, "keys");
    }

    @Override
    public CompletableFuture<Optional<VersionedKafkaStorageProtocolActivation>> getActivation() {
        ensureOpen();
        return client.get(keys.activationKey(), keys.activationPartitionKey())
                .thenApply(optional -> optional.map(this::activation));
    }

    @Override
    public CompletableFuture<VersionedKafkaStorageProtocolActivation> createActivation(
            KafkaStorageProtocolActivationRecord value) {
        ensureOpen();
        KafkaStorageProtocolActivationRecord exact = requireActivation(value);
        byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(
                exact, KafkaStorageProtocolActivationRecord.class);
        CompletableFuture<VersionedKafkaStorageProtocolActivation> write = client.putIfAbsent(
                        keys.activationKey(), bytes, keys.activationPartitionKey())
                .thenApply(result -> activation(exact, result.version(), bytes));
        return recoverCreate(write, this::getActivation, exact, current ->
                current.value().withMetadataVersion(0));
    }

    @Override
    public CompletableFuture<VersionedKafkaStorageProtocolActivation> compareAndSetActivation(
            VersionedKafkaStorageProtocolActivation expected,
            KafkaStorageProtocolActivationRecord replacement) {
        ensureOpen();
        VersionedKafkaStorageProtocolActivation current = requireActivationExpected(expected);
        KafkaStorageProtocolActivationRecord exact = requireActivation(replacement);
        KafkaStorageActivationTransitions.requireActivationReplacement(current.value(), exact);
        byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(
                exact, KafkaStorageProtocolActivationRecord.class);
        CompletableFuture<VersionedKafkaStorageProtocolActivation> write = client.putIfVersion(
                        keys.activationKey(),
                        bytes,
                        current.metadataVersion(),
                        keys.activationPartitionKey())
                .thenApply(result -> activation(exact, result.version(), bytes));
        return recoverCas(write, this::getActivation, exact, current.metadataVersion(), loaded ->
                loaded.value().withMetadataVersion(0));
    }

    @Override
    public CompletableFuture<Optional<VersionedKafkaBrokerCapability>> getCapability(
            KafkaBrokerIdentity identity) {
        ensureOpen();
        KafkaBrokerIdentity exact = Objects.requireNonNull(identity, "identity");
        return client.get(
                        keys.capabilityKey(exact.brokerId(), exact.brokerEpoch()),
                        keys.activationPartitionKey())
                .thenApply(optional -> optional.map(stored -> capability(stored, exact)));
    }

    @Override
    public CompletableFuture<VersionedKafkaBrokerCapability> createCapability(
            KafkaBrokerCapabilityRecord value) {
        ensureOpen();
        KafkaBrokerCapabilityRecord exact = requireCapability(value);
        String key = keys.capabilityKey(exact.brokerId(), exact.brokerEpoch());
        byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(exact, KafkaBrokerCapabilityRecord.class);
        CompletableFuture<VersionedKafkaBrokerCapability> write = client.putIfAbsent(
                        key, bytes, keys.activationPartitionKey())
                .thenApply(result -> capability(key, exact, result.version(), bytes));
        return recoverCreate(write, () -> getCapability(exact.identity()), exact, current ->
                current.value().withMetadataVersion(0));
    }

    @Override
    public CompletableFuture<VersionedKafkaBrokerCapability> heartbeatCapability(
            VersionedKafkaBrokerCapability expected,
            KafkaBrokerCapabilityRecord replacement) {
        ensureOpen();
        VersionedKafkaBrokerCapability current = requireCapabilityExpected(expected);
        KafkaBrokerCapabilityRecord exact = requireCapability(replacement);
        KafkaStorageActivationTransitions.requireCapabilityHeartbeat(current.value(), exact);
        String key = keys.capabilityKey(exact.brokerId(), exact.brokerEpoch());
        byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(exact, KafkaBrokerCapabilityRecord.class);
        CompletableFuture<VersionedKafkaBrokerCapability> write = client.putIfVersion(
                        key, bytes, current.metadataVersion(), keys.activationPartitionKey())
                .thenApply(result -> capability(key, exact, result.version(), bytes));
        return recoverCas(
                write,
                () -> getCapability(exact.identity()),
                exact,
                current.metadataVersion(),
                loaded -> loaded.value().withMetadataVersion(0));
    }

    @Override
    public CompletableFuture<Optional<VersionedKafkaStorageReadiness>> getReadiness() {
        ensureOpen();
        return client.get(keys.readinessKey(), keys.activationPartitionKey())
                .thenApply(optional -> optional.map(this::readiness));
    }

    @Override
    public CompletableFuture<VersionedKafkaStorageReadiness> createReadiness(
            KafkaStorageReadinessRecord value) {
        ensureOpen();
        KafkaStorageReadinessRecord exact = requireReadiness(value);
        byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(exact, KafkaStorageReadinessRecord.class);
        CompletableFuture<VersionedKafkaStorageReadiness> write = client.putIfAbsent(
                        keys.readinessKey(), bytes, keys.activationPartitionKey())
                .thenApply(result -> readiness(exact, result.version(), bytes));
        return recoverCreate(write, this::getReadiness, exact, current ->
                current.value().withMetadataVersion(0));
    }

    @Override
    public CompletableFuture<VersionedKafkaStorageReadiness> compareAndSetReadiness(
            VersionedKafkaStorageReadiness expected,
            KafkaStorageReadinessRecord replacement) {
        ensureOpen();
        VersionedKafkaStorageReadiness current = requireReadinessExpected(expected);
        KafkaStorageReadinessRecord exact = requireReadiness(replacement);
        KafkaStorageActivationTransitions.requireReadinessReplacement(current.value(), exact);
        byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(exact, KafkaStorageReadinessRecord.class);
        CompletableFuture<VersionedKafkaStorageReadiness> write = client.putIfVersion(
                        keys.readinessKey(),
                        bytes,
                        current.metadataVersion(),
                        keys.activationPartitionKey())
                .thenApply(result -> readiness(exact, result.version(), bytes));
        return recoverCas(write, this::getReadiness, exact, current.metadataVersion(), loaded ->
                loaded.value().withMetadataVersion(0));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private VersionedKafkaStorageProtocolActivation activation(
            PartitionedOxiaClient.VersionedValue stored) {
        try {
            KafkaStorageProtocolActivationRecord value = KafkaMetadataCodecs.decodeEnvelope(
                    stored.value(), KafkaStorageProtocolActivationRecord.class);
            if (!stored.key().equals(keys.activationKey())
                    || !value.kafkaClusterId().equals(keys.kafkaClusterId())) {
                throw invariant("Kafka activation key/value identity mismatch", null);
            }
            return activation(value, stored.version(), stored.value());
        } catch (MetadataCodecException | IllegalArgumentException failure) {
            throw invariant("invalid authoritative Kafka activation metadata", failure);
        }
    }

    private VersionedKafkaStorageProtocolActivation activation(
            KafkaStorageProtocolActivationRecord value,
            long version,
            byte[] durableBytes) {
        return new VersionedKafkaStorageProtocolActivation(
                keys.activationKey(),
                value.withMetadataVersion(version),
                version,
                sha256(durableBytes));
    }

    private VersionedKafkaBrokerCapability capability(
            PartitionedOxiaClient.VersionedValue stored,
            KafkaBrokerIdentity expected) {
        try {
            KafkaBrokerCapabilityRecord value = KafkaMetadataCodecs.decodeEnvelope(
                    stored.value(), KafkaBrokerCapabilityRecord.class);
            if (!stored.key().equals(keys.capabilityKey(expected.brokerId(), expected.brokerEpoch()))
                    || !value.kafkaClusterId().equals(keys.kafkaClusterId())
                    || !value.identity().equals(expected)) {
                throw invariant("Kafka capability key/value identity mismatch", null);
            }
            return capability(stored.key(), value, stored.version(), stored.value());
        } catch (MetadataCodecException | IllegalArgumentException failure) {
            throw invariant("invalid authoritative Kafka broker capability metadata", failure);
        }
    }

    private VersionedKafkaBrokerCapability capability(
            String key,
            KafkaBrokerCapabilityRecord value,
            long version,
            byte[] durableBytes) {
        return new VersionedKafkaBrokerCapability(
                key, value.withMetadataVersion(version), version, sha256(durableBytes));
    }

    private VersionedKafkaStorageReadiness readiness(
            PartitionedOxiaClient.VersionedValue stored) {
        try {
            KafkaStorageReadinessRecord value = KafkaMetadataCodecs.decodeEnvelope(
                    stored.value(), KafkaStorageReadinessRecord.class);
            if (!stored.key().equals(keys.readinessKey())
                    || !value.kafkaClusterId().equals(keys.kafkaClusterId())) {
                throw invariant("Kafka readiness key/value identity mismatch", null);
            }
            return readiness(value, stored.version(), stored.value());
        } catch (MetadataCodecException | IllegalArgumentException failure) {
            throw invariant("invalid authoritative Kafka readiness metadata", failure);
        }
    }

    private VersionedKafkaStorageReadiness readiness(
            KafkaStorageReadinessRecord value,
            long version,
            byte[] durableBytes) {
        return new VersionedKafkaStorageReadiness(
                keys.readinessKey(),
                value.withMetadataVersion(version),
                version,
                sha256(durableBytes));
    }

    private KafkaStorageProtocolActivationRecord requireActivation(
            KafkaStorageProtocolActivationRecord value) {
        KafkaStorageProtocolActivationRecord exact = Objects.requireNonNull(value, "value");
        requireWritableMetadataVersion(exact.metadataVersion());
        requireCluster(exact.kafkaClusterId());
        return exact;
    }

    private KafkaBrokerCapabilityRecord requireCapability(KafkaBrokerCapabilityRecord value) {
        KafkaBrokerCapabilityRecord exact = Objects.requireNonNull(value, "value");
        requireWritableMetadataVersion(exact.metadataVersion());
        requireCluster(exact.kafkaClusterId());
        return exact;
    }

    private KafkaStorageReadinessRecord requireReadiness(KafkaStorageReadinessRecord value) {
        KafkaStorageReadinessRecord exact = Objects.requireNonNull(value, "value");
        requireWritableMetadataVersion(exact.metadataVersion());
        requireCluster(exact.kafkaClusterId());
        return exact;
    }

    private VersionedKafkaStorageProtocolActivation requireActivationExpected(
            VersionedKafkaStorageProtocolActivation expected) {
        VersionedKafkaStorageProtocolActivation exact = Objects.requireNonNull(expected, "expected");
        requireCluster(exact.value().kafkaClusterId());
        if (!exact.key().equals(keys.activationKey())) {
            throw new IllegalArgumentException("activation expected key belongs to another scope");
        }
        return exact;
    }

    private VersionedKafkaBrokerCapability requireCapabilityExpected(
            VersionedKafkaBrokerCapability expected) {
        VersionedKafkaBrokerCapability exact = Objects.requireNonNull(expected, "expected");
        requireCluster(exact.value().kafkaClusterId());
        if (!exact.key().equals(keys.capabilityKey(
                exact.value().brokerId(), exact.value().brokerEpoch()))) {
            throw new IllegalArgumentException("capability expected key belongs to another identity");
        }
        return exact;
    }

    private VersionedKafkaStorageReadiness requireReadinessExpected(
            VersionedKafkaStorageReadiness expected) {
        VersionedKafkaStorageReadiness exact = Objects.requireNonNull(expected, "expected");
        requireCluster(exact.value().kafkaClusterId());
        if (!exact.key().equals(keys.readinessKey())) {
            throw new IllegalArgumentException("readiness expected key belongs to another scope");
        }
        return exact;
    }

    private void requireCluster(String kafkaClusterId) {
        if (!keys.kafkaClusterId().equals(kafkaClusterId)) {
            throw new IllegalArgumentException("Kafka activation record belongs to another cluster");
        }
    }

    private static void requireWritableMetadataVersion(long metadataVersion) {
        if (metadataVersion != 0) {
            throw new IllegalArgumentException("encoded Kafka activation metadataVersion must be zero");
        }
    }

    private <T, V> CompletableFuture<V> recoverCreate(
            CompletableFuture<V> write,
            Supplier<CompletableFuture<Optional<V>>> reload,
            T desired,
            Function<V, T> unversioned) {
        return write.handle((result, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(result);
            }
            Throwable original = F4MetadataStoreSupport.unwrap(failure);
            return reload.get().thenCompose(optional -> {
                if (optional.isPresent() && unversioned.apply(optional.orElseThrow()).equals(desired)) {
                    return CompletableFuture.completedFuture(optional.orElseThrow());
                }
                if (F4MetadataStoreSupport.isConditionFailure(original)) {
                    return failed(new KafkaMetadataConditionFailedException(
                            "Kafka activation metadata create lost its exact condition", original));
                }
                return failed(metadataFailure("failed to create Kafka activation metadata", original));
            });
        }).thenCompose(Function.identity());
    }

    private <T, V> CompletableFuture<V> recoverCas(
            CompletableFuture<V> write,
            Supplier<CompletableFuture<Optional<V>>> reload,
            T desired,
            long expectedVersion,
            Function<V, T> unversioned) {
        return write.handle((result, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(result);
            }
            Throwable original = F4MetadataStoreSupport.unwrap(failure);
            return reload.get().thenCompose(optional -> {
                if (optional.isPresent()) {
                    V loaded = optional.orElseThrow();
                    if (version(loaded) > expectedVersion && unversioned.apply(loaded).equals(desired)) {
                        return CompletableFuture.completedFuture(loaded);
                    }
                }
                if (F4MetadataStoreSupport.isConditionFailure(original)) {
                    return failed(new KafkaMetadataConditionFailedException(
                            "Kafka activation metadata CAS lost its exact version condition", original));
                }
                return failed(metadataFailure("failed to CAS Kafka activation metadata", original));
            });
        }).thenCompose(Function.identity());
    }

    private static long version(Object value) {
        if (value instanceof VersionedKafkaStorageProtocolActivation activation) {
            return activation.metadataVersion();
        }
        if (value instanceof VersionedKafkaBrokerCapability capability) {
            return capability.metadataVersion();
        }
        if (value instanceof VersionedKafkaStorageReadiness readiness) {
            return readiness.metadataVersion();
        }
        throw new IllegalArgumentException("unknown Kafka activation wrapper type");
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "Kafka activation metadata store is closed");
        }
    }

    private static RuntimeException metadataFailure(String message, Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            return runtime;
        }
        return new NereusException(ErrorCode.METADATA_UNAVAILABLE, true, message, failure);
    }

    private static NereusException invariant(String message, Throwable failure) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, failure);
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static <T> CompletableFuture<T> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }
}
