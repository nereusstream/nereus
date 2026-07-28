/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.LiveStreamSubject;
import com.nereusstream.kafka.metadata.KafkaMaterializationStreamRegistration;
import com.nereusstream.materialization.DirectMaterializationStreamAuthority;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

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
}
