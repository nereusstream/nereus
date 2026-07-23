/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaBrokerCapability;
import com.nereusstream.metadata.oxia.VersionedKafkaStorageProtocolActivation;
import com.nereusstream.metadata.oxia.VersionedKafkaStorageReadiness;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaPayloadMapping;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/** Controller-side empty-cluster PREPARED to ACTIVE protocol with failover-safe CAS recovery. */
public final class KafkaStorageFirstActivationCoordinator {
    private final KafkaStorageActivationMetadataStore store;
    private final KafkaStorageClusterSnapshotProvider clusterSnapshots;
    private final KafkaStorageActivationPolicy policy;
    private final Clock clock;

    public KafkaStorageFirstActivationCoordinator(
            KafkaStorageActivationMetadataStore store,
            KafkaStorageClusterSnapshotProvider clusterSnapshots,
            KafkaStorageActivationPolicy policy,
            Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.clusterSnapshots = Objects.requireNonNull(clusterSnapshots, "clusterSnapshots");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletionStage<VersionedKafkaStorageProtocolActivation> activate() {
        return store.getActivation().thenCompose(existing -> {
            if (existing.isPresent()
                    && existing.orElseThrow().value().lifecycle()
                            == KafkaStorageActivationLifecycle.ACTIVE) {
                return currentSnapshot().thenApply(snapshot -> {
                    requireActive(existing.orElseThrow(), snapshot);
                    return existing.orElseThrow();
                });
            }
            return currentSnapshot().thenCompose(snapshot -> beginOrResume(existing, snapshot));
        });
    }

    private CompletionStage<VersionedKafkaStorageProtocolActivation> beginOrResume(
            Optional<VersionedKafkaStorageProtocolActivation> existing,
            KafkaStorageClusterSnapshot firstSnapshot) {
        requireFirstActivationSnapshot(firstSnapshot);
        return loadCapabilities(firstSnapshot).thenCompose(proof -> {
            if (existing.isPresent()) {
                VersionedKafkaStorageProtocolActivation prepared = existing.orElseThrow();
                requirePrepared(prepared, firstSnapshot, proof);
                return store.getReadiness().thenCompose(readiness -> {
                    VersionedKafkaStorageReadiness exact = required(
                            readiness, "PREPARED activation has no readiness proof");
                    requireReadiness(exact, firstSnapshot, proof);
                    requirePreparedReadiness(prepared.value(), exact.value());
                    return revalidateAndActivate(prepared, exact, firstSnapshot);
                });
            }
            return upsertReadiness(firstSnapshot, proof).thenCompose(readiness ->
                    createPrepared(firstSnapshot, proof, readiness).thenCompose(prepared ->
                            revalidateAndActivate(prepared, readiness, firstSnapshot)));
        });
    }

    private CompletionStage<VersionedKafkaStorageProtocolActivation> revalidateAndActivate(
            VersionedKafkaStorageProtocolActivation prepared,
            VersionedKafkaStorageReadiness readiness,
            KafkaStorageClusterSnapshot firstSnapshot) {
        return currentSnapshot().thenCompose(secondSnapshot -> {
            requireFirstActivationSnapshot(secondSnapshot);
            require(secondSnapshot.metadataOffset() >= firstSnapshot.metadataOffset(),
                    "KRaft metadata offset regressed during activation", true);
            require(secondSnapshot.brokers().equals(firstSnapshot.brokers()),
                    "KRaft broker set changed during activation", true);
            return loadCapabilities(secondSnapshot).thenCompose(proof -> {
                requireReadiness(readiness, secondSnapshot, proof);
                requirePreparedOrActive(prepared, secondSnapshot, proof);
                requirePreparedReadiness(prepared.value(), readiness.value());
                return writeActive(prepared);
            });
        });
    }

    private CompletableFuture<VersionedKafkaStorageReadiness> upsertReadiness(
            KafkaStorageClusterSnapshot snapshot,
            CapabilityProof proof) {
        return store.getReadiness().thenCompose(existing -> {
            long now = clock.millis();
            if (existing.isPresent()) {
                VersionedKafkaStorageReadiness current = existing.orElseThrow();
                if (readinessMatches(current.value(), snapshot, proof, now)) {
                    return CompletableFuture.completedFuture(current);
                }
                require(snapshot.metadataOffset() >= current.value().kraftMetadataOffset(),
                        "KRaft image precedes existing readiness", true);
                KafkaStorageReadinessRecord replacement = readiness(
                        snapshot,
                        proof,
                        addExact(current.value().readinessEpoch(), 1),
                        Math.max(now, addExact(current.value().createdAtMillis(), 1)));
                return recoverReadinessWrite(
                        store.compareAndSetReadiness(current, replacement), snapshot, proof);
            }
            return recoverReadinessWrite(
                    store.createReadiness(readiness(snapshot, proof, 1, now)), snapshot, proof);
        });
    }

    private CompletableFuture<VersionedKafkaStorageReadiness> recoverReadinessWrite(
            CompletableFuture<VersionedKafkaStorageReadiness> attempt,
            KafkaStorageClusterSnapshot snapshot,
            CapabilityProof proof) {
        return attempt.exceptionallyCompose(failure -> {
            Throwable exact = unwrap(failure);
            if (!(exact instanceof KafkaMetadataConditionFailedException)) {
                return CompletableFuture.failedFuture(exact);
            }
            return store.getReadiness().thenApply(current -> {
                VersionedKafkaStorageReadiness recovered = required(
                        current, "readiness CAS lost and no winner exists");
                requireReadiness(recovered, snapshot, proof);
                return recovered;
            });
        });
    }

    private CompletableFuture<VersionedKafkaStorageProtocolActivation> createPrepared(
            KafkaStorageClusterSnapshot snapshot,
            CapabilityProof proof,
            VersionedKafkaStorageReadiness readiness) {
        KafkaStorageProtocolActivationRecord value = new KafkaStorageProtocolActivationRecord(
                KafkaStorageProtocolActivationRecord.RECORD_VERSION,
                KafkaStorageActivationLifecycle.PREPARED.wireId(),
                policy.kafkaClusterId(),
                KafkaStorageProtocolActivationRecord.PROTOCOL_VERSION,
                KafkaStorageProtocolActivationRecord.API_VERSION,
                KafkaStorageProtocolActivationRecord.STREAM_HEAD_SESSION_VERSION,
                KafkaStorageProtocolActivationRecord.BINDING_VERSION,
                KafkaPayloadMapping.KAFKA_RECORD_BATCH_V1.wireId(),
                KafkaStorageProtocolActivationRecord.OBJECT_WAL_ENTRY_INDEX_VERSION,
                KafkaStorageProtocolActivationRecord.NCP_VERSION,
                KafkaStorageProtocolActivationRecord.NTC_VERSION,
                KafkaStorageProtocolActivationRecord.CHECKPOINT_VERSION,
                KafkaStorageProtocolActivationRecord.COMPACTION_STRATEGY_VERSION,
                policy.allowedStorageProfiles(),
                policy.defaultStorageProfile(),
                proof.capabilitySha256(),
                readiness.value().brokerSetSha256(),
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                snapshot.metadataOffset(),
                readiness.value().readinessEpoch(),
                readiness.value().createdAtMillis(),
                0,
                0);
        return store.createActivation(value).exceptionallyCompose(failure -> {
            Throwable exact = unwrap(failure);
            if (!(exact instanceof KafkaMetadataConditionFailedException)) {
                return CompletableFuture.failedFuture(exact);
            }
            return store.getActivation().thenApply(current -> {
                VersionedKafkaStorageProtocolActivation winner = required(
                        current, "activation create lost and no winner exists");
                requirePreparedOrActive(winner, snapshot, proof);
                requirePreparedReadiness(winner.value(), readiness.value());
                return winner;
            });
        });
    }

    private CompletableFuture<VersionedKafkaStorageProtocolActivation> writeActive(
            VersionedKafkaStorageProtocolActivation prepared) {
        KafkaStorageProtocolActivationRecord current = prepared.value();
        if (current.lifecycle() == KafkaStorageActivationLifecycle.ACTIVE) {
            return CompletableFuture.completedFuture(prepared);
        }
        long activatedAt = Math.max(clock.millis(), current.preparedAtMillis());
        KafkaStorageProtocolActivationRecord active = new KafkaStorageProtocolActivationRecord(
                current.recordVersion(),
                KafkaStorageActivationLifecycle.ACTIVE.wireId(),
                current.kafkaClusterId(),
                current.protocolVersion(),
                current.apiVersion(),
                current.streamHeadSessionVersion(),
                current.bindingVersion(),
                current.payloadMappingId(),
                current.objectWalEntryIndexVersion(),
                current.ncpVersion(),
                current.ntcVersion(),
                current.checkpointVersion(),
                current.compactionStrategyVersion(),
                current.allowedStorageProfiles(),
                current.defaultStorageProfile(),
                current.requiredCapabilitySha256(),
                current.requiredBrokerSetSha256(),
                current.kafkaFeatureLevel(),
                current.preparedAtMetadataOffset(),
                current.activationEpoch(),
                current.preparedAtMillis(),
                activatedAt,
                0);
        return store.compareAndSetActivation(prepared, active).exceptionallyCompose(failure -> {
            Throwable exact = unwrap(failure);
            if (!(exact instanceof KafkaMetadataConditionFailedException)) {
                return CompletableFuture.failedFuture(exact);
            }
            return store.getActivation().thenApply(currentValue -> {
                VersionedKafkaStorageProtocolActivation winner = required(
                        currentValue, "activation CAS lost and no winner exists");
                require(winner.value().lifecycle() == KafkaStorageActivationLifecycle.ACTIVE,
                        "activation CAS lost to a non-ACTIVE value", false);
                requireSamePreparedFacts(prepared.value(), winner.value());
                return winner;
            });
        });
    }

    private CompletionStage<CapabilityProof> loadCapabilities(KafkaStorageClusterSnapshot snapshot) {
        List<CompletableFuture<Optional<VersionedKafkaBrokerCapability>>> reads =
                new ArrayList<>(snapshot.brokers().size());
        for (KafkaBrokerIdentity broker : snapshot.brokers()) reads.add(store.getCapability(broker));
        return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).thenApply(ignored -> {
            List<VersionedKafkaBrokerCapability> capabilities = new ArrayList<>(reads.size());
            byte[] capabilitySha256 = null;
            byte[] providerScopeSha256 = null;
            long now = clock.millis();
            for (int index = 0; index < reads.size(); index++) {
                KafkaBrokerIdentity identity = snapshot.brokers().get(index);
                VersionedKafkaBrokerCapability capability = required(
                        reads.get(index).join(),
                        "capability is absent for broker " + identity.brokerId()
                                + " epoch " + identity.brokerEpoch());
                KafkaBrokerCapabilityRecord value = capability.value();
                require(value.kafkaClusterId().equals(snapshot.kafkaClusterId()),
                        "capability Kafka cluster does not match KRaft", false);
                require(value.identity().equals(identity),
                        "capability identity does not match KRaft", false);
                require(value.expiresAtMillis() > now,
                        "capability is expired for broker " + identity.brokerId(), true);
                require(value.supportedStorageProfiles().equals(policy.allowedStorageProfiles()),
                        "broker storage profiles differ from activation policy", false);
                byte[] digest = KafkaStorageCapabilityDigests.compatibilitySha256(value);
                if (capabilitySha256 == null) capabilitySha256 = digest;
                if (providerScopeSha256 == null) providerScopeSha256 = value.providerScopeSha256();
                require(Arrays.equals(capabilitySha256, digest),
                        "broker capability compatibility facts differ", false);
                require(Arrays.equals(providerScopeSha256, value.providerScopeSha256()),
                        "broker provider scopes differ", false);
                capabilities.add(capability);
            }
            return new CapabilityProof(capabilities, capabilitySha256, providerScopeSha256);
        });
    }

    private KafkaStorageReadinessRecord readiness(
            KafkaStorageClusterSnapshot snapshot,
            CapabilityProof proof,
            long epoch,
            long createdAtMillis) {
        return new KafkaStorageReadinessRecord(
                KafkaStorageReadinessRecord.RECORD_VERSION,
                policy.kafkaClusterId(),
                epoch,
                snapshot.metadataOffset(),
                snapshot.brokers(),
                KafkaStorageReadinessRecord.brokerSetSha256(snapshot.brokers()),
                proof.capabilitySha256(),
                proof.providerScopeSha256(),
                createdAtMillis,
                addExact(createdAtMillis, policy.readinessTtl().toMillis()),
                0);
    }

    private void requireFirstActivationSnapshot(KafkaStorageClusterSnapshot snapshot) {
        requireSnapshotIdentity(snapshot);
        require(snapshot.emptyForFirstActivation(),
                "first activation requires zero topics, authoritative local logs and bindings", false);
    }

    private void requireActive(
            VersionedKafkaStorageProtocolActivation activation,
            KafkaStorageClusterSnapshot snapshot) {
        requireSnapshotIdentity(snapshot);
        require(activation.value().lifecycle() == KafkaStorageActivationLifecycle.ACTIVE,
                "stored activation is not ACTIVE", false);
        requireActivationPolicy(activation.value(), snapshot);
    }

    private void requirePrepared(
            VersionedKafkaStorageProtocolActivation activation,
            KafkaStorageClusterSnapshot snapshot,
            CapabilityProof proof) {
        require(activation.value().lifecycle() == KafkaStorageActivationLifecycle.PREPARED,
                "first activation encountered a non-PREPARED value", false);
        requirePreparedOrActive(activation, snapshot, proof);
    }

    private void requirePreparedOrActive(
            VersionedKafkaStorageProtocolActivation activation,
            KafkaStorageClusterSnapshot snapshot,
            CapabilityProof proof) {
        require(activation.value().lifecycle() == KafkaStorageActivationLifecycle.PREPARED
                        || activation.value().lifecycle() == KafkaStorageActivationLifecycle.ACTIVE,
                "first activation encountered an unknown lifecycle", false);
        requireActivationPolicy(activation.value(), snapshot);
        require(Arrays.equals(activation.value().requiredCapabilitySha256(), proof.capabilitySha256()),
                "PREPARED capability digest differs from current brokers", false);
        require(Arrays.equals(activation.value().requiredBrokerSetSha256(),
                        KafkaStorageReadinessRecord.brokerSetSha256(snapshot.brokers())),
                "PREPARED broker-set digest differs from current KRaft", true);
    }

    private void requireActivationPolicy(
            KafkaStorageProtocolActivationRecord activation,
            KafkaStorageClusterSnapshot snapshot) {
        require(activation.kafkaClusterId().equals(policy.kafkaClusterId()),
                "activation Kafka cluster differs from policy", false);
        require(activation.kafkaFeatureLevel() == snapshot.kafkaFeatureLevel(),
                "activation feature level differs from KRaft", false);
        require(activation.preparedAtMetadataOffset() <= snapshot.metadataOffset(),
                "KRaft image precedes activation preparation", true);
        require(activation.allowedStorageProfiles().equals(policy.allowedStorageProfiles()),
                "activation profiles differ from policy", false);
        require(activation.defaultStorageProfile().equals(policy.defaultStorageProfile()),
                "activation default profile differs from policy", false);
    }

    private void requireReadiness(
            VersionedKafkaStorageReadiness readiness,
            KafkaStorageClusterSnapshot snapshot,
            CapabilityProof proof) {
        KafkaStorageReadinessRecord value = readiness.value();
        require(value.kafkaClusterId().equals(policy.kafkaClusterId()),
                "readiness Kafka cluster differs from activation policy", false);
        require(value.kraftMetadataOffset() <= snapshot.metadataOffset(),
                "KRaft image precedes readiness", true);
        require(value.brokers().equals(snapshot.brokers()),
                "readiness broker set differs from current KRaft", true);
        require(Arrays.equals(value.capabilitySha256(), proof.capabilitySha256()),
                "readiness capability digest differs from current brokers", false);
        require(Arrays.equals(value.providerScopeSha256(), proof.providerScopeSha256()),
                "readiness provider scope differs from current brokers", false);
        require(value.expiresAtMillis() > clock.millis(), "readiness is expired", true);
    }

    private boolean readinessMatches(
            KafkaStorageReadinessRecord readiness,
            KafkaStorageClusterSnapshot snapshot,
            CapabilityProof proof,
            long now) {
        return readiness.kafkaClusterId().equals(policy.kafkaClusterId())
                && readiness.kraftMetadataOffset() <= snapshot.metadataOffset()
                && readiness.brokers().equals(snapshot.brokers())
                && Arrays.equals(readiness.brokerSetSha256(),
                        KafkaStorageReadinessRecord.brokerSetSha256(snapshot.brokers()))
                && Arrays.equals(readiness.capabilitySha256(), proof.capabilitySha256())
                && Arrays.equals(readiness.providerScopeSha256(), proof.providerScopeSha256())
                && readiness.expiresAtMillis() > now;
    }

    private static void requirePreparedReadiness(
            KafkaStorageProtocolActivationRecord activation,
            KafkaStorageReadinessRecord readiness) {
        require(activation.activationEpoch() == readiness.readinessEpoch(),
                "PREPARED activation epoch differs from readiness", false);
        require(activation.preparedAtMetadataOffset() == readiness.kraftMetadataOffset(),
                "PREPARED metadata offset differs from readiness", false);
        require(Arrays.equals(activation.requiredCapabilitySha256(), readiness.capabilitySha256()),
                "PREPARED capability digest differs from readiness", false);
        require(Arrays.equals(activation.requiredBrokerSetSha256(), readiness.brokerSetSha256()),
                "PREPARED broker-set digest differs from readiness", false);
    }

    private static void requireSamePreparedFacts(
            KafkaStorageProtocolActivationRecord prepared,
            KafkaStorageProtocolActivationRecord active) {
        require(prepared.withMetadataVersion(0).equals(new KafkaStorageProtocolActivationRecord(
                        active.recordVersion(),
                        KafkaStorageActivationLifecycle.PREPARED.wireId(),
                        active.kafkaClusterId(),
                        active.protocolVersion(),
                        active.apiVersion(),
                        active.streamHeadSessionVersion(),
                        active.bindingVersion(),
                        active.payloadMappingId(),
                        active.objectWalEntryIndexVersion(),
                        active.ncpVersion(),
                        active.ntcVersion(),
                        active.checkpointVersion(),
                        active.compactionStrategyVersion(),
                        active.allowedStorageProfiles(),
                        active.defaultStorageProfile(),
                        active.requiredCapabilitySha256(),
                        active.requiredBrokerSetSha256(),
                        active.kafkaFeatureLevel(),
                        active.preparedAtMetadataOffset(),
                        active.activationEpoch(),
                        active.preparedAtMillis(),
                        0,
                        0)),
                "ACTIVE winner changed PREPARED immutable facts", false);
    }

    private void requireSnapshotIdentity(KafkaStorageClusterSnapshot snapshot) {
        require(snapshot.kafkaClusterId().equals(policy.kafkaClusterId()),
                "KRaft Kafka cluster differs from activation policy", false);
        require(snapshot.kafkaFeatureLevel()
                        == KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                "KRaft nereus.storage.version is not the exact supported level", false);
    }

    private CompletableFuture<KafkaStorageClusterSnapshot> currentSnapshot() {
        return Objects.requireNonNull(
                        clusterSnapshots.currentSnapshot(), "KRaft snapshot future")
                .thenApply(snapshot -> Objects.requireNonNull(snapshot, "KRaft snapshot"))
                .toCompletableFuture();
    }

    private static <T> T required(Optional<T> value, String message) {
        return value.orElseThrow(() -> failure(message, true));
    }

    private static void require(boolean condition, String message, boolean retriable) {
        if (!condition) throw failure(message, retriable);
    }

    private static NereusException failure(String message, boolean retriable) {
        return new NereusException(
                retriable ? ErrorCode.METADATA_UNAVAILABLE : ErrorCode.METADATA_INVARIANT_VIOLATION,
                retriable,
                message);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("activation timestamp or epoch overflow", failure);
        }
    }

    private record CapabilityProof(
            List<VersionedKafkaBrokerCapability> capabilities,
            byte[] capabilitySha256,
            byte[] providerScopeSha256) {
        private CapabilityProof {
            capabilities = List.copyOf(capabilities);
            capabilitySha256 = capabilitySha256.clone();
            providerScopeSha256 = providerScopeSha256.clone();
        }

        @Override public byte[] capabilitySha256() { return capabilitySha256.clone(); }
        @Override public byte[] providerScopeSha256() { return providerScopeSha256.clone(); }
    }
}
