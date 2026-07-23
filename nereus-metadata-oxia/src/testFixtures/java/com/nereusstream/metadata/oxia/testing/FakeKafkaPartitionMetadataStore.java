/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.testing;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.KafkaPartitionKeyspace;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.KafkaPartitionScanPage;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionRegistry;
import com.nereusstream.metadata.oxia.codec.KafkaMetadataCodecs;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/** Deterministic codec-backed fake for F9 binding state-machine tests. */
public final class FakeKafkaPartitionMetadataStore implements KafkaPartitionMetadataStore {
    private final KafkaPartitionKeyspace keys;
    private final Map<String, Stored> bindings = new TreeMap<>();
    private final Map<String, Stored> registry = new TreeMap<>();
    private long nextVersion = 1;
    private boolean closed;

    public FakeKafkaPartitionMetadataStore(String nereusCluster, String kafkaClusterId) {
        this.keys = new KafkaPartitionKeyspace(nereusCluster, kafkaClusterId);
    }

    public KafkaPartitionKeyspace keyspace() { return keys; }

    @Override
    public synchronized CompletableFuture<Optional<VersionedKafkaPartitionBinding>> get(KafkaPartitionId id) {
        return complete(() -> Optional.ofNullable(bindings.get(keys.bindingRootKey(id)))
                .map(stored -> binding(stored, id)));
    }

    @Override
    public synchronized CompletableFuture<VersionedKafkaPartitionBinding> putCreatingIfAbsent(
            KafkaPartitionBindingRecord value) {
        return complete(() -> {
            requireWritable(value.metadataVersion(), "binding");
            if (value.lifecycle() != KafkaPartitionLifecycle.CREATING) {
                throw new IllegalArgumentException("putCreatingIfAbsent requires CREATING");
            }
            String key = keys.bindingRootKey(value.identity());
            Stored existing = bindings.get(key);
            if (existing != null) {
                VersionedKafkaPartitionBinding decoded = binding(existing, value.identity());
                if (!decoded.value().storageProfile().equals(value.storageProfile())
                        || decoded.value().createdMetadataOffset() != value.createdMetadataOffset()) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                            "deterministic Kafka binding creation conflicts with existing root");
                }
                return decoded;
            }
            byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(value, KafkaPartitionBindingRecord.class);
            Stored stored = new Stored(key, bytes, nextVersion++);
            bindings.put(key, stored);
            return binding(stored, value.identity());
        });
    }

    @Override
    public synchronized CompletableFuture<VersionedKafkaPartitionBinding> compareAndSet(
            VersionedKafkaPartitionBinding expected, KafkaPartitionBindingRecord update) {
        return complete(() -> {
            requireWritable(update.metadataVersion(), "binding");
            KafkaPartitionMetadataTransitions.validate(expected.value(), update);
            String key = keys.bindingRootKey(expected.value().identity());
            Stored current = bindings.get(key);
            if (current == null || current.version != expected.metadataVersion()) {
                throw new KafkaMetadataConditionFailedException("fake Kafka binding CAS version changed");
            }
            byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(update, KafkaPartitionBindingRecord.class);
            Stored stored = new Stored(key, bytes, nextVersion++);
            bindings.put(key, stored);
            return binding(stored, update.identity());
        });
    }

    @Override
    public synchronized CompletableFuture<Void> putRegistryHint(KafkaPartitionRegistryRecord value) {
        return complete(() -> {
            requireWritable(value.metadataVersion(), "registry");
            String key = keys.registryKey(value.identity());
            Stored current = registry.get(key);
            if (current != null) {
                KafkaPartitionRegistryRecord existing = KafkaMetadataCodecs.decodeEnvelope(
                        current.bytes, KafkaPartitionRegistryRecord.class);
                if (existing.bindingEpoch() > value.bindingEpoch()) return null;
            }
            byte[] bytes = KafkaMetadataCodecs.encodeEnvelope(value, KafkaPartitionRegistryRecord.class);
            registry.put(key, new Stored(key, bytes, nextVersion++));
            return null;
        });
    }

    @Override
    public synchronized CompletableFuture<KafkaPartitionScanPage> scanRegistry(
            int shard, Optional<String> continuation, int limit) {
        return complete(() -> {
            if (limit <= 0 || limit > 1_024) {
                throw new IllegalArgumentException("registry scan limit must be in [1,1024]");
            }
            Objects.requireNonNull(continuation, "continuation")
                    .ifPresent(key -> keys.parseRegistryKey(shard, key));
            String prefix = keys.registryShardPrefix(shard) + "/";
            String after = continuation.orElse("");
            ArrayList<VersionedKafkaPartitionRegistry> values = new ArrayList<>();
            boolean more = false;
            for (Stored stored : registry.values()) {
                if (!stored.key.startsWith(prefix) || (!after.isEmpty() && stored.key.compareTo(after) <= 0)) continue;
                if (values.size() == limit) {
                    more = true;
                    break;
                }
                values.add(registry(stored, shard));
            }
            return new KafkaPartitionScanPage(
                    values,
                    more ? Optional.of(values.get(values.size() - 1).key()) : Optional.empty());
        });
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    public synchronized List<byte[]> durableBindingBytes() {
        return bindings.values().stream().map(value -> value.bytes.clone()).toList();
    }

    private VersionedKafkaPartitionBinding binding(Stored stored, KafkaPartitionId id) {
        KafkaPartitionBindingRecord value = KafkaMetadataCodecs.decodeEnvelope(
                stored.bytes, KafkaPartitionBindingRecord.class).withMetadataVersion(stored.version);
        if (!value.identity().equals(id) || !stored.key.equals(keys.bindingRootKey(id))) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "fake Kafka binding key/value identity mismatch");
        }
        return new VersionedKafkaPartitionBinding(stored.key, value, stored.version, sha256(stored.bytes));
    }

    private VersionedKafkaPartitionRegistry registry(Stored stored, int shard) {
        KafkaPartitionRegistryRecord value = KafkaMetadataCodecs.decodeEnvelope(
                stored.bytes, KafkaPartitionRegistryRecord.class).withMetadataVersion(stored.version);
        KafkaPartitionId id = keys.parseRegistryKey(shard, stored.key);
        if (!id.equals(value.identity())) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "fake Kafka registry key/value identity mismatch");
        }
        return new VersionedKafkaPartitionRegistry(stored.key, value, stored.version, sha256(stored.bytes));
    }

    private <T> CompletableFuture<T> complete(java.util.concurrent.Callable<T> operation) {
        if (closed) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "fake Kafka metadata store is closed"));
        }
        try {
            return CompletableFuture.completedFuture(operation.call());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static void requireWritable(long version, String type) {
        if (version != 0) throw new IllegalArgumentException(
                "encoded Kafka " + type + " metadataVersion must be zero");
    }

    private static Checksum sha256(byte[] value) {
        try {
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private record Stored(String key, byte[] bytes, long version) {
        private Stored {
            bytes = bytes.clone();
        }
    }
}
