/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.LiveStreamSubject;
import com.nereusstream.kafka.metadata.KafkaMaterializationStreamRegistration;
import com.nereusstream.materialization.DirectMaterializationStreamAuthority;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

class KafkaGenerationProtocolActivationGuardTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);
    private static final StreamId STREAM = new StreamId("kafka-generation-stream");

    @Test
    void admitsAndRevalidatesTheExactActiveDirectStreamAuthority() {
        GenerationMetadataStore generations = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        try {
            StorageProfile profile = StorageProfile.OBJECT_WAL_SYNC_OBJECT;
            new KafkaMaterializationStreamRegistration(
                            KafkaActivationTestSupport.CLUSTER, generations, CLOCK)
                    .ensure(STREAM, profile)
                    .join();
            KafkaBrokerCapabilitySpecification specification =
                    KafkaActivationTestSupport.specification(3);
            InMemoryKafkaStorageActivationStore activations =
                    KafkaStorageActivationVerifierTest.activeStore(specification, 40_000);
            KafkaGenerationProtocolActivationGuard guard =
                    new KafkaGenerationProtocolActivationGuard(
                            KafkaActivationTestSupport.CLUSTER,
                            generations,
                            KafkaStorageActivationVerifierTest.verifier(
                                    activations,
                                    specification,
                                    KafkaStorageActivationVerifierTest.snapshot()),
                            CLOCK);
            LiveStreamSubject subject =
                    new LiveStreamSubject(
                            STREAM,
                            DirectMaterializationStreamAuthority.identitySha256(STREAM, profile));

            var proof =
                    guard.requireReady(GenerationOperation.GENERATION_PUBLISH, subject, false)
                            .join();

            assertThat(proof.subject()).isEqualTo(subject);
            assertThat(proof.operation()).isEqualTo(GenerationOperation.GENERATION_PUBLISH);
            assertThat(proof.publicationEnabled()).isTrue();
            assertThat(proof.deletionEnabled()).isFalse();
            guard.revalidate(proof).join();
        } finally {
            generations.close();
        }
    }

    @Test
    void rejectsAStreamSubjectWithAnotherAuthorityDigest() {
        GenerationMetadataStore generations = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        try {
            new KafkaMaterializationStreamRegistration(
                            KafkaActivationTestSupport.CLUSTER, generations, CLOCK)
                    .ensure(STREAM, StorageProfile.OBJECT_WAL_SYNC_OBJECT)
                    .join();
            KafkaBrokerCapabilitySpecification specification =
                    KafkaActivationTestSupport.specification(3);
            KafkaGenerationProtocolActivationGuard guard =
                    new KafkaGenerationProtocolActivationGuard(
                            KafkaActivationTestSupport.CLUSTER,
                            generations,
                            KafkaStorageActivationVerifierTest.verifier(
                                    KafkaStorageActivationVerifierTest.activeStore(
                                            specification, 40_000),
                                    specification,
                                    KafkaStorageActivationVerifierTest.snapshot()),
                            CLOCK);

            assertThatThrownBy(
                            () ->
                                    guard.requireReady(
                                                    GenerationOperation.TOPIC_COMPACTED_PUBLISH,
                                                    new LiveStreamSubject(
                                                            STREAM,
                                                            new Checksum(
                                                                    ChecksumType.SHA256,
                                                                    "0".repeat(64))),
                                                    false)
                                            .join())
                    .hasRootCauseMessage(
                            "Kafka direct-stream registration no longer matches publication"
                                + " authority");
        } finally {
            generations.close();
        }
    }

    @Test
    void admitsOnlyWalOnlyTopicCompactionFromL0WhenRegistrationIsAbsent() {
        GenerationMetadataStore generations = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        AtomicReference<StreamMetadataSnapshot> snapshot =
                new AtomicReference<>(snapshot(StorageProfile.BOOKKEEPER_WAL_ONLY, 7));
        OxiaMetadataStore l0 = l0Store(snapshot);
        try {
            KafkaBrokerCapabilitySpecification specification =
                    KafkaActivationTestSupport.specification(3);
            KafkaGenerationProtocolActivationGuard guard =
                    new KafkaGenerationProtocolActivationGuard(
                            KafkaActivationTestSupport.CLUSTER,
                            generations,
                            l0,
                            KafkaStorageActivationVerifierTest.verifier(
                                    KafkaStorageActivationVerifierTest.activeStore(
                                            specification, 40_000),
                                    specification,
                                    KafkaStorageActivationVerifierTest.snapshot()),
                            CLOCK);
            LiveStreamSubject subject =
                    new LiveStreamSubject(
                            STREAM,
                            DirectMaterializationStreamAuthority.identitySha256(
                                    STREAM, StorageProfile.BOOKKEEPER_WAL_ONLY));

            var proof =
                    guard.requireReady(
                                    GenerationOperation.TOPIC_COMPACTED_PUBLISH,
                                    subject,
                                    false)
                            .join();

            assertThat(proof.subjectValidationVersion()).isEqualTo(7);
            guard.revalidate(proof).join();
            assertThatThrownBy(
                            () ->
                                    guard.requireReady(
                                                    GenerationOperation.GENERATION_PUBLISH,
                                                    subject,
                                                    false)
                                            .join())
                    .hasRootCauseMessage("Kafka direct-stream registration is absent");

            snapshot.set(snapshot(StorageProfile.OBJECT_WAL_SYNC_OBJECT, 7));
            assertThatThrownBy(() -> guard.revalidate(proof).join())
                    .hasRootCauseMessage(
                            "Kafka WAL-only compaction stream no longer matches publication"
                                + " authority");
        } finally {
            l0.close();
            generations.close();
        }
    }

    private static StreamMetadataSnapshot snapshot(
            StorageProfile profile, long policyVersion) {
        long metadataVersion = 11;
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "kafka-generation-stream-name",
                        "stream-name-hash",
                        StreamState.ACTIVE.name(),
                        profile.name(),
                        Map.of(),
                        1,
                        policyVersion,
                        metadataVersion),
                new CommittedEndOffsetRecord(
                        STREAM.value(), 2, 100, 2, metadataVersion),
                new TrimRecord(STREAM.value(), 0, "", 1, metadataVersion));
    }

    private static OxiaMetadataStore l0Store(
            AtomicReference<StreamMetadataSnapshot> snapshot) {
        return (OxiaMetadataStore)
                Proxy.newProxyInstance(
                        OxiaMetadataStore.class.getClassLoader(),
                        new Class<?>[] {OxiaMetadataStore.class},
                        (proxy, method, args) ->
                                switch (method.getName()) {
                                    case "getStreamSnapshot" ->
                                            CompletableFuture.completedFuture(snapshot.get());
                                    case "close" -> null;
                                    case "toString" -> "wal-only-compaction-l0";
                                    case "hashCode" -> System.identityHashCode(proxy);
                                    case "equals" -> proxy == args[0];
                                    default ->
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                });
    }
}
