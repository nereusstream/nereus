/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStoreTestFactory;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class FutureCatalogSentinelTest {
    private static final Clock CLOCK =
            Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
    private static final GcReferenceDomainConfig CONFIG =
            new GcReferenceDomainConfig(1, 100, 100);

    @Test
    void absentActivationIsIncompleteAndReadOnly() {
        try (GenerationProtocolActivationStore activations = activationStore()) {
            var sentinel = sentinel(
                    activations, GenerationActivationTestSupport.installedDomains());

            var snapshot = sentinel.snapshot(query()).join();

            assertThat(snapshot.complete()).isFalse();
            assertThat(snapshot.veto()).isTrue();
            assertThat(snapshot.authorities()).singleElement()
                    .satisfies(authority -> assertThat(authority.metadataVersion()).isZero());
            assertThat(activations.get(F4MetadataTestValues.CLUSTER).join()).isEmpty();
        }
    }

    @Test
    void publicationOnlyStageAndUnknownFutureDomainVetoDeletion() {
        try (GenerationProtocolActivationStore activations = activationStore()) {
            var prepared = activations.getOrCreate(F4MetadataTestValues.CLUSTER).join();
            activations.compareAndSet(
                    F4MetadataTestValues.CLUSTER,
                    GenerationActivationTestSupport.publication(prepared.value()),
                    prepared.metadataVersion()).join();

            var publicationOnly = sentinel(
                            activations,
                            GenerationActivationTestSupport.installedDomains())
                    .snapshot(query()).join();
            assertThat(publicationOnly.complete()).isTrue();
            assertThat(publicationOnly.veto()).isTrue();
        }

        try (GenerationProtocolActivationStore activations = activationStore()) {
            activateDeletion(activations);
            List<GcReferenceDomainVersion> missingSentinel =
                    GenerationActivationTestSupport.installedDomains().stream()
                            .filter(version -> !version.domainId().equals(
                                    FutureCatalogSentinelDomain.DOMAIN_ID))
                            .toList();
            var mismatch = sentinel(activations, missingSentinel)
                    .snapshot(query()).join();
            assertThat(mismatch.complete()).isTrue();
            assertThat(mismatch.veto()).isTrue();
        }
    }

    @Test
    void exactDeletionReadyDomainSetClearsAndEveryActivationChangeInvalidatesSnapshot() {
        try (GenerationProtocolActivationStore activations = activationStore()) {
            var deletion = activateDeletion(activations);
            var sentinel = sentinel(
                    activations, GenerationActivationTestSupport.installedDomains());
            var clear = sentinel.snapshot(query()).join();

            assertThat(clear.complete()).isTrue();
            assertThat(clear.veto()).isFalse();
            assertThat(sentinel.stillMatches(query(), clear).join()).isTrue();

            activations.compareAndSet(
                    F4MetadataTestValues.CLUSTER,
                    GenerationActivationTestSupport.deletion(
                            deletion.value(),
                            9,
                            F4MetadataTestValues.HASH_A,
                            1_300),
                    deletion.metadataVersion()).join();
            assertThat(sentinel.stillMatches(query(), clear).join()).isFalse();
        }
    }

    private static FutureCatalogSentinelDomain sentinel(
            GenerationProtocolActivationStore activations,
            List<GcReferenceDomainVersion> installed) {
        return new FutureCatalogSentinelDomain(
                F4MetadataTestValues.CLUSTER,
                activations,
                CONFIG,
                installed);
    }

    private static GenerationProtocolActivationStore activationStore() {
        return GenerationProtocolActivationStoreTestFactory.inMemory(
                CLOCK,
                F4MetadataTestValues.PROCESS,
                F4MetadataTestValues.referenceDomains());
    }

    private static com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation
            activateDeletion(GenerationProtocolActivationStore activations) {
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

    private static GcReferenceQuery query() {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                new ObjectKey("objects/future-sentinel"),
                Optional.empty(),
                PhysicalObjectKind.OBJECT_WAL,
                8,
                new Checksum(ChecksumType.CRC32C, "00000000"),
                Optional.empty(),
                Optional.empty());
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object,
                List.of(new StreamId("stream/future-sentinel")),
                new Checksum(ChecksumType.SHA256, "e".repeat(64)));
    }
}
