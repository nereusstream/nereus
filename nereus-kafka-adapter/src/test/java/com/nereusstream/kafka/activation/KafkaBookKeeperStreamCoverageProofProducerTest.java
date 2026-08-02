/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.activation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperStreamCoverageProof;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.kafka.metadata.KafkaBindingRequest;
import com.nereusstream.kafka.metadata.KafkaMaterializationStreamRegistration;
import com.nereusstream.kafka.metadata.KafkaPartitionBinding;
import com.nereusstream.kafka.metadata.KafkaPartitionLifecycleCoordinator;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.testing.TestStreamStorage;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.metadata.oxia.testing.FakeKafkaPartitionMetadataStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperStreamCoverageProofProducerTest {
    private static final String CLUSTER = "nereus";
    private static final String KAFKA_CLUSTER = "kraft";
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);
    private static final String NAMESPACE_SHA256 = "22".repeat(32);

    @Test
    void scansEveryBindingAndMaterializationShardAndProducesDeterministicProof() {
        try (Context context = new Context()) {
            context.create(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT, true);

            BookKeeperStreamCoverageProof first = context.producer(UnaryOperator.identity())
                    .produce(readiness(), Duration.ofSeconds(30))
                    .join();
            BookKeeperStreamCoverageProof second = context.producer(UnaryOperator.identity())
                    .produce(readiness(), Duration.ofSeconds(30))
                    .join();

            assertThat(context.bindingShards)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, 64).boxed().toList());
            assertThat(context.materializationShards)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, 64).boxed().toList());
            assertThat(first).isEqualTo(second);
            assertThat(first.shardsScanned()).isEqualTo(64);
            assertThat(first.registrationsScanned()).isOne();
            assertThat(first.bookKeeperStreamsVerified()).isOne();
        }
    }

    @Test
    void acceptsWalOnlyBindingWithoutObjectMaterializationRegistration() {
        try (Context context = new Context()) {
            KafkaPartitionBinding binding = context.create(StorageProfile.BOOKKEEPER_WAL_ONLY, true);

            BookKeeperStreamCoverageProof proof = context.producer(UnaryOperator.identity())
                    .produce(readiness(), Duration.ofSeconds(30))
                    .join();

            assertThat(context.generations
                            .getStreamRegistration(CLUSTER, binding.streamId())
                            .join())
                    .isEmpty();
            assertThat(proof.registrationsScanned()).isOne();
            assertThat(proof.bookKeeperStreamsVerified()).isOne();
        }
    }

    @Test
    void rejectsObjectBackedBindingWithoutDirectMaterializationRegistration() {
        try (Context context = new Context()) {
            context.create(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT, false);

            assertInvariant(context.producer(UnaryOperator.identity()).produce(readiness(), Duration.ofSeconds(30)));
        }
    }

    @Test
    void rejectsBindingWhoseProfileDriftsFromL0Authority() {
        try (Context context = new Context()) {
            context.create(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT, true);

            assertInvariant(
                    context.producer(snapshot -> withProfile(snapshot, StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT))
                            .produce(readiness(), Duration.ofSeconds(30)));
        }
    }

    private static void assertInvariant(CompletableFuture<?> future) {
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(NereusException.class)
                .extracting(failure -> ((NereusException) failure).code())
                .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
    }

    private static BookKeeperBrokerReadiness readiness() {
        return new BookKeeperBrokerReadiness(9, new Checksum(ChecksumType.SHA256, "88".repeat(32)), 2);
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
                1);
    }

    private static StreamMetadataSnapshot snapshot(StreamMetadata metadata) {
        long commitVersion = metadata.committedEndOffset() == 0 ? 0 : 1;
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        metadata.streamId().value(),
                        metadata.streamName().value(),
                        DeterministicIds.streamNameHash(metadata.streamName()),
                        metadata.state().name(),
                        metadata.profile().canonical().name(),
                        metadata.attributes(),
                        metadata.createdAtMillis(),
                        0,
                        metadata.metadataVersion()),
                new CommittedEndOffsetRecord(
                        metadata.streamId().value(),
                        metadata.committedEndOffset(),
                        metadata.cumulativeSize(),
                        commitVersion,
                        metadata.metadataVersion()),
                new TrimRecord(
                        metadata.streamId().value(),
                        metadata.trimOffset(),
                        "",
                        CLOCK.millis(),
                        metadata.metadataVersion()));
    }

    private static StreamMetadataSnapshot withProfile(StreamMetadataSnapshot snapshot, StorageProfile profile) {
        StreamMetadataRecord current = snapshot.metadata();
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        current.streamId(),
                        current.streamName(),
                        current.streamNameHash(),
                        current.state(),
                        profile.name(),
                        current.attributes(),
                        current.createdAtMillis(),
                        current.policyVersion(),
                        current.metadataVersion()),
                snapshot.committedEnd(),
                snapshot.trim());
    }

    @SuppressWarnings("unchecked")
    private static <T> T observingProxy(Class<T> contract, T delegate, BiConsumer<String, Object[]> observer) {
        return (T) Proxy.newProxyInstance(
                contract.getClassLoader(), new Class<?>[] {contract}, (proxy, method, arguments) -> {
                    Object[] exactArguments = arguments == null ? new Object[0] : arguments;
                    observer.accept(method.getName(), exactArguments);
                    try {
                        return method.invoke(delegate, exactArguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static KafkaPartitionIdentity identity(long value) {
        ByteBuffer bytes =
                ByteBuffer.allocate(16).putLong(0x1234_5678_9abc_def0L).putLong(value);
        return new KafkaPartitionIdentity(
                KAFKA_CLUSTER, Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array()), 0, "orders");
    }

    private static final class Context implements AutoCloseable {
        private final FakeKafkaPartitionMetadataStore bindings =
                new FakeKafkaPartitionMetadataStore(CLUSTER, KAFKA_CLUSTER);
        private final GenerationMetadataStore generations = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        private final TestStreamStorage streams = new TestStreamStorage();
        private final Set<Integer> bindingShards = new HashSet<>();
        private final Set<Integer> materializationShards = new HashSet<>();
        private long nextIdentity = 1;

        private KafkaPartitionBinding create(StorageProfile profile, boolean installMaterialization) {
            KafkaPartitionLifecycleCoordinator coordinator = installMaterialization
                    ? new KafkaPartitionLifecycleCoordinator(
                            bindings,
                            streams,
                            bindings.keyspace(),
                            CLOCK,
                            new KafkaMaterializationStreamRegistration(CLUSTER, generations, CLOCK))
                    : new KafkaPartitionLifecycleCoordinator(bindings, streams, bindings.keyspace(), CLOCK);
            return coordinator
                    .ensureBinding(new KafkaBindingRequest(
                            identity(nextIdentity++), profile, 20, "broker-run", 1, Duration.ofSeconds(30)))
                    .join();
        }

        private KafkaBookKeeperStreamCoverageProofProducer producer(UnaryOperator<StreamMetadataSnapshot> mutation) {
            KafkaPartitionMetadataStore observedBindings =
                    observingProxy(KafkaPartitionMetadataStore.class, bindings, (method, arguments) -> {
                        if (method.equals("scanRegistry")) {
                            bindingShards.add((Integer) arguments[0]);
                        }
                    });
            GenerationMetadataStore observedGenerations =
                    observingProxy(GenerationMetadataStore.class, generations, (method, arguments) -> {
                        if (method.equals("scanStreamRegistrations")) {
                            materializationShards.add((Integer) arguments[1]);
                        }
                    });
            OxiaMetadataStore l0 = (OxiaMetadataStore) Proxy.newProxyInstance(
                    OxiaMetadataStore.class.getClassLoader(),
                    new Class<?>[] {OxiaMetadataStore.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("getStreamSnapshot")) {
                            return streams.getStreamMetadata((StreamId) arguments[1])
                                    .thenApply(KafkaBookKeeperStreamCoverageProofProducerTest::snapshot)
                                    .thenApply(mutation);
                        }
                        if (method.getName().equals("close")) {
                            return null;
                        }
                        throw new UnsupportedOperationException(method.getName());
                    });
            return new KafkaBookKeeperStreamCoverageProofProducer(
                    CLUSTER,
                    KAFKA_CLUSTER,
                    configuration(),
                    NAMESPACE_SHA256,
                    observedGenerations,
                    l0,
                    observedBindings);
        }

        @Override
        public void close() {
            generations.close();
            bindings.close();
        }
    }
}
