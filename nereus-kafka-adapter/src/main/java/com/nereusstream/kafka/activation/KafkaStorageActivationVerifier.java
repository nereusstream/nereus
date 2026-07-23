/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaBrokerCapability;
import com.nereusstream.metadata.oxia.VersionedKafkaStorageProtocolActivation;
import com.nereusstream.metadata.oxia.VersionedKafkaStorageReadiness;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
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
import java.util.concurrent.CompletionStage;

/** Fail-closed broker admission against ACTIVE, readiness, capabilities and current KRaft facts. */
public final class KafkaStorageActivationVerifier {
    private final KafkaStorageActivationMetadataStore store;
    private final KafkaBrokerCapabilitySpecification localSpecification;
    private final KafkaStorageClusterSnapshotProvider clusterSnapshots;
    private final Clock clock;

    public KafkaStorageActivationVerifier(
            KafkaStorageActivationMetadataStore store,
            KafkaBrokerCapabilitySpecification localSpecification,
            KafkaStorageClusterSnapshotProvider clusterSnapshots,
            Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.localSpecification = Objects.requireNonNull(localSpecification, "localSpecification");
        this.clusterSnapshots = Objects.requireNonNull(clusterSnapshots, "clusterSnapshots");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletionStage<VerifiedKafkaStorageActivation> verifyCurrent() {
        CompletionStage<KafkaStorageClusterSnapshot> supplied = Objects.requireNonNull(
                clusterSnapshots.currentSnapshot(), "KRaft snapshot future");
        return supplied.thenCompose(this::loadAuthorities);
    }

    private CompletionStage<VerifiedKafkaStorageActivation> loadAuthorities(
            KafkaStorageClusterSnapshot snapshot) {
        KafkaStorageClusterSnapshot exact = Objects.requireNonNull(snapshot, "KRaft snapshot");
        requireSnapshot(exact);
        CompletableFuture<Optional<VersionedKafkaStorageProtocolActivation>> activation =
                store.getActivation();
        CompletableFuture<Optional<VersionedKafkaStorageReadiness>> readiness = store.getReadiness();
        List<CompletableFuture<Optional<VersionedKafkaBrokerCapability>>> capabilities =
                new ArrayList<>(exact.brokers().size());
        for (KafkaBrokerIdentity broker : exact.brokers()) {
            capabilities.add(store.getCapability(broker));
        }
        CompletableFuture<?>[] all = new CompletableFuture<?>[capabilities.size() + 2];
        all[0] = activation;
        all[1] = readiness;
        for (int index = 0; index < capabilities.size(); index++) {
            all[index + 2] = capabilities.get(index);
        }
        return CompletableFuture.allOf(all).thenApply(ignored -> verifyLoaded(
                exact, required(activation.join(), "ACTIVE activation is absent"),
                required(readiness.join(), "readiness proof is absent"), capabilities));
    }

    private VerifiedKafkaStorageActivation verifyLoaded(
            KafkaStorageClusterSnapshot snapshot,
            VersionedKafkaStorageProtocolActivation activation,
            VersionedKafkaStorageReadiness readiness,
            List<CompletableFuture<Optional<VersionedKafkaBrokerCapability>>> capabilityReads) {
        long now = clock.millis();
        KafkaStorageProtocolActivationRecord active = activation.value();
        KafkaStorageReadinessRecord ready = readiness.value();
        require(active.lifecycle() == KafkaStorageActivationLifecycle.ACTIVE,
                "Kafka storage activation is not ACTIVE", true);
        require(active.kafkaClusterId().equals(snapshot.kafkaClusterId()),
                "activation Kafka cluster does not match KRaft", false);
        require(active.kafkaFeatureLevel() == snapshot.kafkaFeatureLevel(),
                "activation feature level does not match KRaft", false);
        require(active.preparedAtMetadataOffset() <= snapshot.metadataOffset(),
                "KRaft image precedes the activation preparation offset", true);
        require(active.allowedStorageProfiles().equals(localSpecification.supportedStorageProfiles()),
                "local storage profiles do not match ACTIVE", false);
        require(active.defaultStorageProfile().equals(localSpecification.defaultStorageProfile()),
                "local default storage profile does not match ACTIVE", false);

        require(ready.kafkaClusterId().equals(snapshot.kafkaClusterId()),
                "readiness Kafka cluster does not match KRaft", false);
        require(ready.expiresAtMillis() > now, "readiness proof is expired", true);
        require(ready.kraftMetadataOffset() <= snapshot.metadataOffset(),
                "KRaft image precedes the readiness source offset", true);
        require(ready.brokers().equals(snapshot.brokers()),
                "readiness broker set does not match KRaft", true);
        require(ready.readinessEpoch() >= active.activationEpoch(),
                "readiness epoch predates ACTIVE", false);
        if (ready.readinessEpoch() == active.activationEpoch()) {
            require(Arrays.equals(active.requiredBrokerSetSha256(), ready.brokerSetSha256()),
                    "activation-epoch readiness changed its broker set", false);
        }
        require(Arrays.equals(active.requiredCapabilitySha256(), ready.capabilitySha256()),
                "ACTIVE and readiness capability digests differ", false);
        require(Arrays.equals(ready.providerScopeSha256(), localSpecification.providerScopeSha256()),
                "readiness provider scope does not match local configuration", false);

        List<VersionedKafkaBrokerCapability> capabilities = new ArrayList<>(capabilityReads.size());
        byte[] requiredCapability = null;
        for (int index = 0; index < capabilityReads.size(); index++) {
            KafkaBrokerIdentity expectedIdentity = snapshot.brokers().get(index);
            VersionedKafkaBrokerCapability capability = required(
                    capabilityReads.get(index).join(),
                    "capability is absent for broker " + expectedIdentity.brokerId()
                            + " epoch " + expectedIdentity.brokerEpoch());
            KafkaBrokerCapabilityRecord value = capability.value();
            require(value.kafkaClusterId().equals(snapshot.kafkaClusterId()),
                    "capability Kafka cluster does not match KRaft", false);
            require(value.identity().equals(expectedIdentity),
                    "capability identity does not match KRaft", false);
            require(value.expiresAtMillis() > now,
                    "capability is expired for broker " + expectedIdentity.brokerId(), true);
            require(Arrays.equals(value.providerScopeSha256(), ready.providerScopeSha256()),
                    "broker capability provider scope differs from readiness", false);
            byte[] digest = KafkaStorageCapabilityDigests.compatibilitySha256(value);
            if (requiredCapability == null) requiredCapability = digest;
            require(Arrays.equals(requiredCapability, digest),
                    "broker capability compatibility facts differ", false);
            capabilities.add(capability);
        }
        require(requiredCapability != null, "KRaft broker set is empty", false);
        require(Arrays.equals(requiredCapability, ready.capabilitySha256()),
                "capability aggregate does not match readiness", false);
        require(Arrays.equals(requiredCapability, active.requiredCapabilitySha256()),
                "capability aggregate does not match ACTIVE", false);
        VersionedKafkaBrokerCapability local = capabilities.get(
                snapshot.brokers().indexOf(localSpecification.identity()));
        require(localSpecification.matchesImmutableFacts(local.value()),
                "published local capability differs from this broker process", false);
        return new VerifiedKafkaStorageActivation(snapshot, activation, readiness, capabilities);
    }

    private void requireSnapshot(KafkaStorageClusterSnapshot snapshot) {
        require(snapshot.kafkaClusterId().equals(localSpecification.kafkaClusterId()),
                "KRaft Kafka cluster does not match local configuration", false);
        require(snapshot.kafkaFeatureLevel()
                        == KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                "KRaft nereus.storage.version is not the exact supported level", false);
        require(snapshot.brokers().contains(localSpecification.identity()),
                "local broker registration epoch is absent from KRaft", true);
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
}
