/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaBrokerCapability;
import com.nereusstream.metadata.oxia.VersionedKafkaStorageProtocolActivation;
import com.nereusstream.metadata.oxia.VersionedKafkaStorageReadiness;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

final class InMemoryKafkaStorageActivationStore implements KafkaStorageActivationMetadataStore {
    private static final Checksum DIGEST = new Checksum(ChecksumType.SHA256, "0".repeat(64));
    private final Map<KafkaBrokerIdentity, VersionedKafkaBrokerCapability> capabilities = new HashMap<>();
    private final AtomicInteger heartbeatCount = new AtomicInteger();
    private VersionedKafkaStorageProtocolActivation activation;
    private VersionedKafkaStorageReadiness readiness;
    private long nextVersion = 1;
    private boolean failHeartbeats;
    private boolean failNextActivationCas;

    @Override
    public synchronized CompletableFuture<Optional<VersionedKafkaStorageProtocolActivation>>
            getActivation() {
        return CompletableFuture.completedFuture(Optional.ofNullable(activation));
    }

    @Override
    public synchronized CompletableFuture<VersionedKafkaStorageProtocolActivation> createActivation(
            KafkaStorageProtocolActivationRecord value) {
        if (activation != null) return condition("activation exists");
        activation = activation(value);
        return CompletableFuture.completedFuture(activation);
    }

    @Override
    public synchronized CompletableFuture<VersionedKafkaStorageProtocolActivation>
            compareAndSetActivation(
                    VersionedKafkaStorageProtocolActivation expected,
                    KafkaStorageProtocolActivationRecord replacement) {
        if (failNextActivationCas) {
            failNextActivationCas = false;
            return CompletableFuture.failedFuture(new IllegalStateException("activation CAS interrupted"));
        }
        if (activation == null || activation.metadataVersion() != expected.metadataVersion()) {
            return condition("stale activation");
        }
        activation = activation(replacement);
        return CompletableFuture.completedFuture(activation);
    }

    @Override
    public synchronized CompletableFuture<Optional<VersionedKafkaBrokerCapability>> getCapability(
            KafkaBrokerIdentity identity) {
        return CompletableFuture.completedFuture(Optional.ofNullable(capabilities.get(identity)));
    }

    @Override
    public synchronized CompletableFuture<VersionedKafkaBrokerCapability> createCapability(
            KafkaBrokerCapabilityRecord value) {
        if (capabilities.containsKey(value.identity())) return condition("capability exists");
        VersionedKafkaBrokerCapability created = capability(value);
        capabilities.put(value.identity(), created);
        return CompletableFuture.completedFuture(created);
    }

    @Override
    public synchronized CompletableFuture<VersionedKafkaBrokerCapability> heartbeatCapability(
            VersionedKafkaBrokerCapability expected,
            KafkaBrokerCapabilityRecord replacement) {
        if (failHeartbeats) return CompletableFuture.failedFuture(new IllegalStateException("heartbeat failed"));
        VersionedKafkaBrokerCapability current = capabilities.get(expected.value().identity());
        if (current == null || current.metadataVersion() != expected.metadataVersion()) {
            return condition("stale capability");
        }
        VersionedKafkaBrokerCapability updated = capability(replacement);
        capabilities.put(replacement.identity(), updated);
        heartbeatCount.incrementAndGet();
        return CompletableFuture.completedFuture(updated);
    }

    @Override
    public synchronized CompletableFuture<Optional<VersionedKafkaStorageReadiness>> getReadiness() {
        return CompletableFuture.completedFuture(Optional.ofNullable(readiness));
    }

    @Override
    public synchronized CompletableFuture<VersionedKafkaStorageReadiness> createReadiness(
            KafkaStorageReadinessRecord value) {
        if (readiness != null) return condition("readiness exists");
        readiness = readiness(value);
        return CompletableFuture.completedFuture(readiness);
    }

    @Override
    public synchronized CompletableFuture<VersionedKafkaStorageReadiness> compareAndSetReadiness(
            VersionedKafkaStorageReadiness expected,
            KafkaStorageReadinessRecord replacement) {
        if (readiness == null || readiness.metadataVersion() != expected.metadataVersion()) {
            return condition("stale readiness");
        }
        readiness = readiness(replacement);
        return CompletableFuture.completedFuture(readiness);
    }

    int heartbeatCount() { return heartbeatCount.get(); }

    synchronized void failHeartbeats() { failHeartbeats = true; }

    synchronized void failNextActivationCas() { failNextActivationCas = true; }

    @Override public void close() { }

    private VersionedKafkaStorageProtocolActivation activation(
            KafkaStorageProtocolActivationRecord value) {
        long version = nextVersion++;
        return new VersionedKafkaStorageProtocolActivation(
                "activation", value.withMetadataVersion(version), version, DIGEST);
    }

    private VersionedKafkaBrokerCapability capability(KafkaBrokerCapabilityRecord value) {
        long version = nextVersion++;
        return new VersionedKafkaBrokerCapability(
                "capability", value.withMetadataVersion(version), version, DIGEST);
    }

    private VersionedKafkaStorageReadiness readiness(KafkaStorageReadinessRecord value) {
        long version = nextVersion++;
        return new VersionedKafkaStorageReadiness(
                "readiness", value.withMetadataVersion(version), version, DIGEST);
    }

    private static <T> CompletableFuture<T> condition(String message) {
        return CompletableFuture.failedFuture(new KafkaMetadataConditionFailedException(message));
    }
}
