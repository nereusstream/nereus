/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.codec.KafkaMetadataCodecs;
import com.nereusstream.metadata.oxia.codec.MetadataCodecException;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Production native-Kafka binding store over a borrowed shared Oxia runtime. */
public final class OxiaJavaKafkaPartitionMetadataStore implements KafkaPartitionMetadataStore {
    private static final int MAX_HINT_CAS_RETRIES = 64;

    private final KafkaPartitionKeyspace keys;
    private final PartitionedOxiaClient client;
    private final AtomicBoolean closed = new AtomicBoolean();

    public static OxiaJavaKafkaPartitionMetadataStore usingSharedRuntime(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime,
            String nereusCluster,
            String kafkaClusterId) {
        Objects.requireNonNull(runtime, "runtime").requireCompatible(
                Objects.requireNonNull(configuration, "configuration"));
        return new OxiaJavaKafkaPartitionMetadataStore(
                runtime.client(), new KafkaPartitionKeyspace(nereusCluster, kafkaClusterId));
    }

    OxiaJavaKafkaPartitionMetadataStore(
            PartitionedOxiaClient client, KafkaPartitionKeyspace keys) {
        this.client = Objects.requireNonNull(client, "client");
        this.keys = Objects.requireNonNull(keys, "keys");
    }

    @Override
    public CompletableFuture<Optional<VersionedKafkaPartitionBinding>> get(KafkaPartitionId id) {
        ensureOpen();
        String key = keys.bindingRootKey(id);
        return client.get(key, keys.bindingPartitionKey(id))
                .thenApply(optional -> optional.map(value -> binding(value, id)));
    }

    @Override
    public CompletableFuture<VersionedKafkaPartitionBinding> putCreatingIfAbsent(
            KafkaPartitionBindingRecord value) {
        ensureOpen();
        KafkaPartitionBindingRecord exact = requireWritableBinding(value);
        if (exact.lifecycle() != KafkaPartitionLifecycle.CREATING) {
            throw new IllegalArgumentException("putCreatingIfAbsent requires a CREATING binding");
        }
        KafkaPartitionId id = exact.identity();
        String key = keys.bindingRootKey(id);
        byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(exact, KafkaPartitionBindingRecord.class);
        return client.putIfAbsent(key, bytes, keys.bindingPartitionKey(id))
                .thenApply(result -> versionedBinding(key, exact, result.version(), bytes))
                .exceptionallyCompose(failure -> {
                    if (!F4MetadataStoreSupport.isConditionFailure(failure)) {
                        return failed(metadataFailure("failed to create Kafka partition binding", failure));
                    }
                    return get(id).thenApply(optional -> {
                        VersionedKafkaPartitionBinding existing = optional.orElseThrow(() ->
                                new KafkaMetadataConditionFailedException(
                                        "Kafka binding create lost condition but root is absent"));
                        requireSameCreation(exact, existing.value());
                        return existing;
                    });
                });
    }

    @Override
    public CompletableFuture<VersionedKafkaPartitionBinding> compareAndSet(
            VersionedKafkaPartitionBinding expected,
            KafkaPartitionBindingRecord update) {
        ensureOpen();
        Objects.requireNonNull(expected, "expected");
        KafkaPartitionBindingRecord exact = requireWritableBinding(update);
        KafkaPartitionMetadataTransitions.validate(expected.value(), exact);
        KafkaPartitionId id = expected.value().identity();
        String key = keys.bindingRootKey(id);
        if (!expected.key().equals(key) || !exact.identity().equals(id)) {
            throw new IllegalArgumentException("Kafka binding CAS key/value identity mismatch");
        }
        byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(exact, KafkaPartitionBindingRecord.class);
        return client.putIfVersion(key, bytes, expected.metadataVersion(), keys.bindingPartitionKey(id))
                .thenApply(result -> versionedBinding(key, exact, result.version(), bytes))
                .exceptionallyCompose(failure -> {
                    if (F4MetadataStoreSupport.isConditionFailure(failure)) {
                        return failed(new KafkaMetadataConditionFailedException(
                                "Kafka binding CAS lost its exact version condition",
                                F4MetadataStoreSupport.unwrap(failure)));
                    }
                    return failed(metadataFailure("failed to CAS Kafka partition binding", failure));
                });
    }

    @Override
    public CompletableFuture<Void> putRegistryHint(KafkaPartitionRegistryRecord value) {
        ensureOpen();
        KafkaPartitionRegistryRecord exact = requireWritableRegistry(value);
        return putRegistryHint(exact, 0);
    }

    private CompletableFuture<Void> putRegistryHint(
            KafkaPartitionRegistryRecord update, int attempt) {
        if (attempt >= MAX_HINT_CAS_RETRIES) {
            return failed(new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED, true,
                    "Kafka registry hint CAS retry budget exhausted"));
        }
        KafkaPartitionId id = update.identity();
        String key = keys.registryKey(id);
        PartitionKey partition = keys.registryPartitionKey(keys.registryShard(id));
        return client.get(key, partition).thenCompose(optional -> {
            if (optional.isPresent()) {
                VersionedKafkaPartitionRegistry existing = registry(optional.orElseThrow(), keys.registryShard(id));
                if (existing.value().bindingEpoch() > update.bindingEpoch()) {
                    return CompletableFuture.completedFuture(null);
                }
                byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(update, KafkaPartitionRegistryRecord.class);
                return client.putIfVersion(key, bytes, existing.metadataVersion(), partition)
                        .thenApply(ignored -> (Void) null);
            }
            byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(update, KafkaPartitionRegistryRecord.class);
            return client.putIfAbsent(key, bytes, partition).thenApply(ignored -> (Void) null);
        }).exceptionallyCompose(failure -> {
            if (F4MetadataStoreSupport.isConditionFailure(failure)) {
                return putRegistryHint(update, attempt + 1);
            }
            return failed(metadataFailure("failed to update Kafka registry hint", failure));
        });
    }

    @Override
    public CompletableFuture<KafkaPartitionScanPage> scanRegistry(
            int shard, Optional<String> continuation, int limit) {
        ensureOpen();
        if (limit <= 0 || limit > 1_024) {
            throw new IllegalArgumentException("registry scan limit must be in [1,1024]");
        }
        Objects.requireNonNull(continuation, "continuation").ifPresent(key -> keys.parseRegistryKey(shard, key));
        String prefix = keys.registryShardPrefix(shard);
        String from = continuation.map(key -> key + '\0')
                .orElse(F4MetadataStoreSupport.fixedDepthStart(prefix, 2));
        String to = F4MetadataStoreSupport.fixedDepthEnd(prefix, 2);
        return client.rangeScan(from, to, limit + 1, keys.registryPartitionKey(shard))
                .thenApply(stored -> {
                    boolean more = stored.size() > limit;
                    List<PartitionedOxiaClient.VersionedValue> selected =
                            more ? stored.subList(0, limit) : stored;
                    ArrayList<VersionedKafkaPartitionRegistry> values = new ArrayList<>(selected.size());
                    for (PartitionedOxiaClient.VersionedValue item : selected) {
                        values.add(registry(item, shard));
                    }
                    Optional<String> next = more
                            ? Optional.of(values.get(values.size() - 1).key())
                            : Optional.empty();
                    return new KafkaPartitionScanPage(values, next);
                });
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private VersionedKafkaPartitionBinding binding(
            PartitionedOxiaClient.VersionedValue stored, KafkaPartitionId expected) {
        try {
            KafkaPartitionBindingRecord value = KafkaMetadataCodecs.decodeEnvelope(
                    stored.value(), KafkaPartitionBindingRecord.class).withMetadataVersion(stored.version());
            if (!value.identity().equals(expected) || !stored.key().equals(keys.bindingRootKey(expected))) {
                throw invariant("Kafka binding key/value identity mismatch", null);
            }
            return new VersionedKafkaPartitionBinding(
                    stored.key(), value, stored.version(), sha256(stored.value()));
        } catch (MetadataCodecException | IllegalArgumentException failure) {
            throw invariant("invalid authoritative Kafka binding metadata", failure);
        }
    }

    private VersionedKafkaPartitionRegistry registry(
            PartitionedOxiaClient.VersionedValue stored, int shard) {
        try {
            KafkaPartitionRegistryRecord value = KafkaMetadataCodecs.decodeEnvelope(
                    stored.value(), KafkaPartitionRegistryRecord.class).withMetadataVersion(stored.version());
            KafkaPartitionId id = keys.parseRegistryKey(shard, stored.key());
            if (!id.equals(value.identity()) || !value.bindingRootKey().equals(keys.bindingRootKey(id))) {
                throw invariant("Kafka registry key/value identity mismatch", null);
            }
            return new VersionedKafkaPartitionRegistry(
                    stored.key(), value, stored.version(), sha256(stored.value()));
        } catch (MetadataCodecException | IllegalArgumentException failure) {
            throw invariant("invalid Kafka registry metadata", failure);
        }
    }

    private static KafkaPartitionBindingRecord requireWritableBinding(KafkaPartitionBindingRecord value) {
        KafkaPartitionBindingRecord exact = Objects.requireNonNull(value, "value");
        if (exact.metadataVersion() != 0) {
            throw new IllegalArgumentException("encoded Kafka binding metadataVersion must be zero");
        }
        return exact;
    }

    private static KafkaPartitionRegistryRecord requireWritableRegistry(KafkaPartitionRegistryRecord value) {
        KafkaPartitionRegistryRecord exact = Objects.requireNonNull(value, "value");
        if (exact.metadataVersion() != 0) {
            throw new IllegalArgumentException("encoded Kafka registry metadataVersion must be zero");
        }
        return exact;
    }

    private static void requireSameCreation(
            KafkaPartitionBindingRecord requested, KafkaPartitionBindingRecord existing) {
        if (!requested.identity().equals(existing.identity())
                || requested.incarnation() != existing.incarnation()
                || requested.payloadMappingId() != existing.payloadMappingId()
                || !requested.storageProfile().equals(existing.storageProfile())
                || requested.createdMetadataOffset() != existing.createdMetadataOffset()) {
            throw invariant("existing Kafka binding conflicts with deterministic creation", null);
        }
    }

    private VersionedKafkaPartitionBinding versionedBinding(
            String key, KafkaPartitionBindingRecord value, long version, byte[] bytes) {
        return new VersionedKafkaPartitionBinding(
                key, value.withMetadataVersion(version), version, sha256(bytes));
    }

    private void ensureOpen() {
        if (closed.get()) throw new NereusException(
                ErrorCode.STORAGE_CLOSED, false, "Kafka partition metadata store is closed");
    }

    private static RuntimeException metadataFailure(String message, Throwable failure) {
        Throwable cause = F4MetadataStoreSupport.unwrap(failure);
        if (cause instanceof RuntimeException runtime) return runtime;
        return new NereusException(ErrorCode.METADATA_UNAVAILABLE, true, message, cause);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
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
