/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RegisteredStreamGcGlobalReferenceScopeTest {
    private static final Clock CLOCK =
            Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    @Test
    void absentActivationReturnsIncompleteWithoutCreatingClusterAuthority() {
        try (GenerationProtocolActivationStore activations = activationStore();
                GenerationMetadataStore generations =
                        GenerationMetadataStoreTestFactory.inMemory(CLOCK)) {
            var scope = scope(
                    activations,
                    generations,
                    GenerationActivationTestSupport.installedDomains(),
                    new GcReferenceDomainConfig(1, 100, 100));

            var snapshot = scope.snapshot().join();

            assertThat(snapshot.complete()).isFalse();
            assertThat(snapshot.streams()).isEmpty();
            assertThat(snapshot.authorities()).singleElement()
                    .satisfies(authority -> {
                        assertThat(authority.metadataVersion()).isZero();
                        assertThat(authority.authorityKey())
                                .endsWith("/capabilities/generation-v1/activation");
                    });
            assertThat(activations.get(F4MetadataTestValues.CLUSTER).join()).isEmpty();
        }
    }

    @Test
    void deletionReadyActivationPromotesEveryRegistrationShardToGlobalAuthority() {
        try (GenerationProtocolActivationStore activations = activationStore();
                GenerationMetadataStore generations =
                        GenerationMetadataStoreTestFactory.inMemory(CLOCK)) {
            List<StreamId> streams = List.of(
                    new StreamId("stream/global-z"),
                    new StreamId("stream/global-a"),
                    new StreamId("stream/global-m"));
            for (int index = 0; index < streams.size(); index++) {
                generations.createOrVerifyStreamRegistration(
                        F4MetadataTestValues.CLUSTER,
                        F4MetadataTestValues.registration(
                                streams.get(index).value(), index + 1)).join();
            }
            activateDeletion(activations);
            var scope = scope(
                    activations,
                    generations,
                    GenerationActivationTestSupport.installedDomains(),
                    new GcReferenceDomainConfig(1, 100, 100));

            var snapshot = scope.snapshot().join();

            assertThat(snapshot.complete()).isTrue();
            assertThat(snapshot.streams()).extracting(StreamId::value)
                    .containsExactly(
                            "stream/global-a",
                            "stream/global-m",
                            "stream/global-z");
            assertThat(snapshot.streamCount()).isEqualTo(3);
            assertThat(snapshot.authorityCount()).isEqualTo(4);
            assertThat(snapshot.authorities())
                    .extracting(value -> value.authorityKey())
                    .anyMatch(key -> key.endsWith("/capabilities/generation-v1/activation"))
                    .anyMatch(key -> key.contains("/stream-registry/"));
            assertThat(scope.snapshot().join()).isEqualTo(snapshot);
        }
    }

    @Test
    void unknownInstalledDomainOrAuthorityLimitKeepsGlobalScopeIncomplete() {
        try (GenerationProtocolActivationStore activations = activationStore();
                GenerationMetadataStore generations =
                        GenerationMetadataStoreTestFactory.inMemory(CLOCK)) {
            generations.createOrVerifyStreamRegistration(
                    F4MetadataTestValues.CLUSTER,
                    F4MetadataTestValues.registration("stream/global-one", 1)).join();
            generations.createOrVerifyStreamRegistration(
                    F4MetadataTestValues.CLUSTER,
                    F4MetadataTestValues.registration("stream/global-two", 2)).join();
            activateDeletion(activations);

            List<GcReferenceDomainVersion> missingSentinel =
                    GenerationActivationTestSupport.installedDomains().stream()
                            .filter(version -> !version.domainId().equals(
                                    FutureCatalogSentinelDomain.DOMAIN_ID))
                            .toList();
            var mismatch = scope(
                    activations,
                    generations,
                    missingSentinel,
                    new GcReferenceDomainConfig(1, 100, 100))
                    .snapshot().join();
            assertThat(mismatch.complete()).isFalse();
            assertThat(mismatch.streams()).isEmpty();
            assertThat(mismatch.authorities()).hasSize(1);

            var limited = scope(
                    activations,
                    generations,
                    GenerationActivationTestSupport.installedDomains(),
                    new GcReferenceDomainConfig(1, 2, 100))
                    .snapshot().join();
            assertThat(limited.complete()).isFalse();
            assertThat(limited.streamCount()).isEqualTo(2);
            assertThat(limited.authorityCount()).isEqualTo(3);
            assertThat(limited.streams()).hasSize(1);
            assertThat(limited.authorities()).hasSize(2);
        }
    }

    @Test
    void activationChangeAcrossGlobalEnumerationCannotProduceClearScope() {
        try (GenerationProtocolActivationStore durable = activationStore();
                GenerationMetadataStore generations =
                        GenerationMetadataStoreTestFactory.inMemory(CLOCK)) {
            var deletion = activateDeletion(durable);
            AtomicInteger reads = new AtomicInteger();
            GenerationProtocolActivationStore drifting =
                    new GenerationProtocolActivationStore() {
                        @Override
                        public CompletableFuture<Optional<VersionedGenerationProtocolActivation>>
                                get(String cluster) {
                            if (reads.incrementAndGet() == 2) {
                                return durable.compareAndSet(
                                                cluster,
                                                GenerationActivationTestSupport.deletion(
                                                        deletion.value(),
                                                        9,
                                                        F4MetadataTestValues.HASH_A,
                                                        1_300),
                                                deletion.metadataVersion())
                                        .thenCompose(ignored -> durable.get(cluster));
                            }
                            return durable.get(cluster);
                        }

                        @Override
                        public CompletableFuture<VersionedGenerationProtocolActivation>
                                getOrCreate(String cluster) {
                            return durable.getOrCreate(cluster);
                        }

                        @Override
                        public CompletableFuture<VersionedGenerationProtocolActivation>
                                compareAndSet(
                                        String cluster,
                                        GenerationProtocolActivationRecord replacement,
                                        long expectedVersion) {
                            return durable.compareAndSet(
                                    cluster, replacement, expectedVersion);
                        }

                        @Override
                        public void close() {
                            // The durable delegate is owned by the surrounding try-with-resources.
                        }
                    };
            var scope = scope(
                    drifting,
                    generations,
                    GenerationActivationTestSupport.installedDomains(),
                    new GcReferenceDomainConfig(1, 100, 100));

            var snapshot = scope.snapshot().join();

            assertThat(snapshot.complete()).isFalse();
            assertThat(snapshot.authorities()).hasSize(1);
            assertThat(reads).hasValue(2);
        }
    }

    private static RegisteredStreamGcGlobalReferenceScope scope(
            GenerationProtocolActivationStore activations,
            GenerationMetadataStore generations,
            List<GcReferenceDomainVersion> installed,
            GcReferenceDomainConfig config) {
        return new RegisteredStreamGcGlobalReferenceScope(
                F4MetadataTestValues.CLUSTER,
                activations,
                generations,
                installed,
                config);
    }

    private static GenerationProtocolActivationStore activationStore() {
        return GenerationProtocolActivationStoreTestFactory.inMemory(
                CLOCK,
                F4MetadataTestValues.PROCESS,
                F4MetadataTestValues.referenceDomains());
    }

    private static VersionedGenerationProtocolActivation activateDeletion(
            GenerationProtocolActivationStore activations) {
        var prepared = activations.getOrCreate(F4MetadataTestValues.CLUSTER).join();
        var publication = activations.compareAndSet(
                F4MetadataTestValues.CLUSTER,
                GenerationActivationTestSupport.publication(prepared.value()),
                prepared.metadataVersion()).join();
        return activations.compareAndSet(
                F4MetadataTestValues.CLUSTER,
                GenerationActivationTestSupport.deletion(
                        publication.value(),
                        8,
                        F4MetadataTestValues.HASH_D,
                        1_200),
                publication.metadataVersion()).join();
    }
}
