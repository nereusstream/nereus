/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationProjectionRefV1;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.ManagedLedgerStreamProjection;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.StreamRegistrationScanPage;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedTopicProjection;
import com.nereusstream.metadata.oxia.VersionedVirtualLedgerProjection;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.metadata.oxia.records.VirtualLedgerProjectionRecord;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

final class BookKeeperStreamCoverageProofProducerTest {
    private static final String CLUSTER = "cluster-a";
    private static final String NAME = "tenant/ns/persistent/bookkeeper-coverage";
    private static final String NAMESPACE = "77".repeat(32);

    @Test
    void verifiesEveryRegistryShardAndExactL0F2Authority() {
        Fixture fixture = new Fixture(StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);
        BookKeeperStreamCoverageProofProducer producer = fixture.producer();

        BookKeeperStreamCoverageProof first = producer
                .produce(readiness(), Duration.ofSeconds(10))
                .join();
        BookKeeperStreamCoverageProof second = producer
                .produce(readiness(), Duration.ofSeconds(10))
                .join();

        assertThat(first.shardsScanned()).isEqualTo(64);
        assertThat(first.registrationsScanned()).isOne();
        assertThat(first.bookKeeperStreamsVerified()).isOne();
        assertThat(second.coverageSha256()).isEqualTo(first.coverageSha256());
    }

    @Test
    void rejectsARegisteredBookKeeperStreamWhoseL0ProfileDrifts() {
        Fixture fixture = new Fixture(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT);
        fixture.snapshotProfile = StorageProfile.OBJECT_WAL_ASYNC_OBJECT;

        assertThatThrownBy(() -> fixture.producer()
                        .produce(readiness(), Duration.ofSeconds(10))
                        .join())
                .hasRootCauseMessage(
                        "BookKeeper registration and L0 stream authority disagree");
    }

    private static BookKeeperBrokerReadiness readiness() {
        return new BookKeeperBrokerReadiness(
                11,
                new Checksum(ChecksumType.SHA256, "88".repeat(32)),
                2);
    }

    private static final class Fixture {
        private final BookKeeperWalConfiguration configuration =
                configuration();
        private final StorageProfile profile;
        private final TopicProjectionRecord topic;
        private final StreamId streamId;
        private final VersionedMaterializationStreamRegistration registration;
        private StorageProfile snapshotProfile;

        private Fixture(StorageProfile profile) {
            this.profile = profile;
            this.snapshotProfile = profile;
            this.topic = topic(profile);
            this.streamId = new StreamId(topic.streamId());
            ManagedLedgerGenerationProjectionRefV1 reference =
                    new ManagedLedgerGenerationProjectionRefV1(
                            NAME, topic.projectionIdentity());
            MaterializationStreamRegistrationRecord value =
                    new MaterializationStreamRegistrationRecord(
                            1,
                            streamId.value(),
                            ProjectionIdentity.encode(Optional.of(
                                    reference.toProjectionRef())),
                            reference.projectionIdentitySha256().value(),
                            profile.name(),
                            1,
                            7,
                            1,
                            0);
            F4Keyspace keys = new F4Keyspace(CLUSTER);
            this.registration = new VersionedMaterializationStreamRegistration(
                    keys.materializationRegistryKey(streamId),
                    value.withMetadataVersion(3),
                    3,
                    sha('a'));
        }

        private BookKeeperStreamCoverageProofProducer producer() {
            return new BookKeeperStreamCoverageProofProducer(
                    CLUSTER,
                    configuration,
                    NAMESPACE,
                    generationStore(),
                    l0Store(),
                    projectionStore());
        }

        private GenerationMetadataStore generationStore() {
            int populatedShard = new F4Keyspace(CLUSTER)
                    .materializationRegistryShard(streamId);
            return proxy(
                    GenerationMetadataStore.class,
                    (method, arguments) -> {
                        if (method.equals("scanStreamRegistrations")) {
                            int shard = (int) arguments[1];
                            return CompletableFuture.completedFuture(
                                    new StreamRegistrationScanPage(
                                            shard == populatedShard
                                                    ? java.util.List.of(registration)
                                                    : java.util.List.of(),
                                            Optional.empty()));
                        }
                        if (method.equals("close")) {
                            return null;
                        }
                        throw new UnsupportedOperationException(method);
                    });
        }

        private OxiaMetadataStore l0Store() {
            return proxy(
                    OxiaMetadataStore.class,
                    (method, arguments) -> {
                        if (method.equals("getStreamSnapshot")) {
                            return CompletableFuture.completedFuture(snapshot());
                        }
                        if (method.equals("close")) {
                            return null;
                        }
                        throw new UnsupportedOperationException(method);
                    });
        }

        private ManagedLedgerProjectionMetadataStore projectionStore() {
            return proxy(
                    ManagedLedgerProjectionMetadataStore.class,
                    (method, arguments) -> {
                        if (method.equals("getProjectionByStream")) {
                            return CompletableFuture.completedFuture(projection());
                        }
                        if (method.equals("close")) {
                            return null;
                        }
                        throw new UnsupportedOperationException(method);
                    });
        }

        private StreamMetadataSnapshot snapshot() {
            return new StreamMetadataSnapshot(
                    new StreamMetadataRecord(
                            streamId.value(),
                            topic.streamName(),
                            "stream-name-hash",
                            StreamState.ACTIVE.name(),
                            snapshotProfile.name(),
                            Map.of(
                                    ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                                    ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1),
                            topic.createdAtMillis(),
                            1,
                            5),
                    new CommittedEndOffsetRecord(
                            streamId.value(), 9, 90, 7, 5),
                    new TrimRecord(streamId.value(), 2, "", 1, 5));
        }

        private ManagedLedgerStreamProjection projection() {
            VirtualLedgerProjectionRecord bindingValue =
                    new VirtualLedgerProjectionRecord(
                            NAME,
                            ManagedLedgerProjectionNames.managedLedgerNameHash(NAME),
                            topic.projectionIdentity(),
                            0,
                            ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                            0);
            return new ManagedLedgerStreamProjection(
                    streamId,
                    Optional.of(new VersionedVirtualLedgerProjection(
                            "/binding", bindingValue, 2, sha('b'))),
                    Optional.of(new VersionedTopicProjection(
                            "/topic", topic, 4, sha('c'))));
        }
    }

    private static TopicProjectionRecord topic(StorageProfile profile) {
        long incarnation = 1;
        return new TopicProjectionRecord(
                NAME,
                ManagedLedgerProjectionNames.managedLedgerNameHash(NAME),
                7,
                incarnation,
                ManagedLedgerProjectionNames.streamName(NAME, incarnation).value(),
                ManagedLedgerProjectionNames.streamId(NAME, incarnation).value(),
                ManagedLedgerProjectionNames.STORAGE_CLASS,
                profile.name(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 1,
                ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1,
                ManagedLedgerFacadeState.OPEN.name(),
                Map.of(),
                100,
                0,
                0);
    }

    private static BookKeeperWalConfiguration configuration() {
        return new BookKeeperWalConfiguration(
                "primary",
                "11".repeat(32),
                12,
                0x801,
                "reservation-1",
                3,
                3,
                2,
                BookKeeperDigestType.CRC32C,
                new BookKeeperSecretRef("secret://bookkeeper/password", "v7"),
                100_000,
                256L * 1024 * 1024,
                1_000,
                8,
                64,
                32,
                Duration.ofHours(1),
                1,
                8,
                64L * 1024 * 1024,
                Duration.ofSeconds(30),
                Duration.ofSeconds(20),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofMinutes(2),
                Duration.ofSeconds(30),
                Duration.ofMinutes(1),
                256);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (ignored, method, arguments) -> invocation.invoke(
                        method.getName(), arguments == null ? new Object[0] : arguments));
    }

    private static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256,
                String.valueOf(value).repeat(64));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(String method, Object[] arguments) throws Throwable;
    }
}
