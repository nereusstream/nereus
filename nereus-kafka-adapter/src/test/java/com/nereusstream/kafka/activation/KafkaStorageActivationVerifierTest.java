/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaPayloadMapping;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaStorageActivationVerifierTest {
    private static final long NOW = 10_000;
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC);

    @Test
    void admitsOnlyAnExactLiveAuthorityBundle() {
        KafkaBrokerCapabilitySpecification specification = KafkaActivationTestSupport.specification(3);
        InMemoryKafkaStorageActivationStore store = activeStore(specification, NOW + 30_000);
        KafkaStorageActivationVerifier verifier = verifier(store, specification, snapshot());

        VerifiedKafkaStorageActivation verified = verifier.verifyCurrent().toCompletableFuture().join();

        assertThat(verified.clusterSnapshot()).isEqualTo(snapshot());
        assertThat(verified.activation().value().lifecycle())
                .isEqualTo(KafkaStorageActivationLifecycle.ACTIVE);
        assertThat(verified.capabilities()).hasSize(1);
        assertThat(verified.capabilities().get(0).value().identity())
                .isEqualTo(KafkaActivationTestSupport.BROKER);
    }

    @Test
    void rejectsExpiredReadinessAsRetriableBeforePartitionIo() {
        KafkaBrokerCapabilitySpecification specification = KafkaActivationTestSupport.specification(3);
        InMemoryKafkaStorageActivationStore store = activeStore(specification, NOW);

        assertFailure(
                verifier(store, specification, snapshot()).verifyCurrent(),
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                "readiness proof is expired");
    }

    @Test
    void rejectsWrongProviderScopeAsAnInvariantViolation() {
        KafkaBrokerCapabilitySpecification published = KafkaActivationTestSupport.specification(3);
        KafkaBrokerCapabilitySpecification local = KafkaActivationTestSupport.specification(9);
        InMemoryKafkaStorageActivationStore store = activeStore(published, NOW + 30_000);

        assertFailure(
                verifier(store, local, snapshot()).verifyCurrent(),
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                "readiness provider scope does not match local configuration");
    }

    @Test
    void rejectsBrokerEpochDriftAsRetriable() {
        KafkaBrokerCapabilitySpecification specification = KafkaActivationTestSupport.specification(3);
        InMemoryKafkaStorageActivationStore store = activeStore(specification, NOW + 30_000);
        KafkaStorageClusterSnapshot restarted = new KafkaStorageClusterSnapshot(
                KafkaActivationTestSupport.CLUSTER,
                102,
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                List.of(new KafkaBrokerIdentity(1, 12)),
                false,
                false,
                false);

        assertFailure(
                verifier(store, specification, restarted).verifyCurrent(),
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                "local broker registration epoch is absent from KRaft");
    }

    @Test
    void freezesTheCanonicalCompatibilityDigest() {
        byte[] digest = KafkaStorageCapabilityDigests.compatibilitySha256(
                KafkaActivationTestSupport.specification(3).initialRecord(NOW));

        assertThat(java.util.HexFormat.of().formatHex(digest))
                .isEqualTo("62ab41304ac552add271f137c17543635950162d58c8b17d887e9418ce08b8d3");
    }

    private static InMemoryKafkaStorageActivationStore activeStore(
            KafkaBrokerCapabilitySpecification specification,
            long readinessExpiry) {
        InMemoryKafkaStorageActivationStore store = new InMemoryKafkaStorageActivationStore();
        KafkaBrokerCapabilityRecord capability = specification.initialRecord(NOW - 50);
        byte[] capabilitySha256 = KafkaStorageCapabilityDigests.compatibilitySha256(capability);
        List<KafkaBrokerIdentity> brokers = List.of(KafkaActivationTestSupport.BROKER);
        store.createCapability(capability).join();
        store.createReadiness(new KafkaStorageReadinessRecord(
                KafkaStorageReadinessRecord.RECORD_VERSION,
                KafkaActivationTestSupport.CLUSTER,
                7,
                101,
                brokers,
                KafkaStorageReadinessRecord.brokerSetSha256(brokers),
                capabilitySha256,
                specification.providerScopeSha256(),
                NOW - 500,
                readinessExpiry,
                0)).join();
        store.createActivation(new KafkaStorageProtocolActivationRecord(
                KafkaStorageProtocolActivationRecord.RECORD_VERSION,
                KafkaStorageActivationLifecycle.ACTIVE.wireId(),
                KafkaActivationTestSupport.CLUSTER,
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
                specification.supportedStorageProfiles(),
                specification.defaultStorageProfile(),
                capabilitySha256,
                KafkaStorageReadinessRecord.brokerSetSha256(brokers),
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                100,
                3,
                NOW - 1_000,
                NOW - 500,
                0)).join();
        return store;
    }

    private static KafkaStorageActivationVerifier verifier(
            InMemoryKafkaStorageActivationStore store,
            KafkaBrokerCapabilitySpecification specification,
            KafkaStorageClusterSnapshot snapshot) {
        return new KafkaStorageActivationVerifier(
                store,
                specification,
                () -> CompletableFuture.completedFuture(snapshot),
                CLOCK);
    }

    private static KafkaStorageClusterSnapshot snapshot() {
        return new KafkaStorageClusterSnapshot(
                KafkaActivationTestSupport.CLUSTER,
                102,
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                List.of(KafkaActivationTestSupport.BROKER),
                false,
                false,
                false);
    }

    private static void assertFailure(
            java.util.concurrent.CompletionStage<?> operation,
            ErrorCode code,
            boolean retriable,
            String message) {
        assertThatThrownBy(() -> operation.toCompletableFuture().join()).satisfies(failure -> {
            Throwable exact = unwrap(failure);
            assertThat(exact).isInstanceOf(NereusException.class);
            NereusException nereus = (NereusException) exact;
            assertThat(nereus.code()).isEqualTo(code);
            assertThat(nereus.retriable()).isEqualTo(retriable);
            assertThat(nereus).hasMessage(message);
        });
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
