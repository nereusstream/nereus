/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ManagedLedgerMaterializationRegistrationCoordinatorTest {
    private static final String CLUSTER = "cluster/a";
    private static final String NAME =
            "tenant/ns/persistent/generation-registration";
    private static final Clock CLOCK =
            Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    @Test
    void createsExactNpr1RegistrationAndAcceptsMutableProjectionCas() {
        TopicProjectionRecord initial = topic(1, 4, Map.of("owner", "a"));
        TopicProjectionRecord propertyUpdate =
                topic(1, 5, Map.of("owner", "b"));
        Fixture fixture = new Fixture(stream(7, StreamState.ACTIVE));
        fixture.projections.add(Optional.of(initial));
        fixture.projections.add(Optional.of(propertyUpdate));

        fixture.coordinator()
                .ensureRegistered(NAME, initial.projectionIdentity())
                .join();

        MaterializationStreamRegistrationRecord value =
                fixture.stored.get().value();
        ManagedLedgerGenerationProjectionRefV1 reference =
                new ManagedLedgerGenerationProjectionRefV1(
                        NAME, initial.projectionIdentity());
        assertThat(value.streamId()).isEqualTo(initial.streamId());
        assertThat(ProjectionIdentity.decode(value.projectionRef()))
                .contains(reference.toProjectionRef());
        assertThat(value.projectionIdentitySha256())
                .isEqualTo(
                        reference.projectionIdentitySha256().value());
        assertThat(value.storageProfile())
                .isEqualTo(
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name());
        assertThat(value.registeredAtMillis()).isEqualTo(1_000);
        assertThat(value.lastHintCommitVersion()).isEqualTo(7);
        assertThat(value.updatedAtMillis()).isEqualTo(1_000);
        assertThat(fixture.mutationCalls).hasValue(1);
    }

    @Test
    void refreshesHintAndRecoversLostCasResponseFromExactReload() {
        TopicProjectionRecord topic = topic(1, 4, Map.of());
        Fixture fixture = new Fixture(stream(9, StreamState.ACTIVE));
        fixture.projections.add(Optional.of(topic));
        fixture.loseCasResponse = true;
        ManagedLedgerGenerationProjectionRefV1 reference =
                new ManagedLedgerGenerationProjectionRefV1(
                        NAME, topic.projectionIdentity());
        MaterializationStreamRegistrationRecord existing =
                new MaterializationStreamRegistrationRecord(
                        1,
                        topic.streamId(),
                        ProjectionIdentity.encode(Optional.of(
                                reference.toProjectionRef())),
                        reference.projectionIdentitySha256().value(),
                        topic.storageProfile(),
                        50,
                        1,
                        50,
                        0);
        fixture.stored.set(versioned(existing, 3));

        fixture.coordinator()
                .ensureRegistered(NAME, topic.projectionIdentity())
                .join();

        assertThat(fixture.stored.get().value().registeredAtMillis())
                .isEqualTo(50);
        assertThat(fixture.stored.get().value().lastHintCommitVersion())
                .isEqualTo(9);
        assertThat(fixture.stored.get().metadataVersion()).isEqualTo(4);
        assertThat(fixture.mutationCalls).hasValue(2);
    }

    @Test
    void finalProjectionRecreationFailsAfterRegistrationWrite() {
        TopicProjectionRecord first = topic(1, 4, Map.of());
        TopicProjectionRecord recreated = topic(2, 5, Map.of());
        Fixture fixture = new Fixture(stream(0, StreamState.ACTIVE));
        fixture.projections.add(Optional.of(first));
        fixture.projections.add(Optional.of(recreated));

        assertThatThrownBy(() -> fixture.coordinator()
                        .ensureRegistered(
                                NAME, first.projectionIdentity())
                        .join())
                .satisfies(error -> assertThat(rootCause(error))
                        .isInstanceOf(NereusException.class)
                        .hasMessageContaining(
                                "projection identity changed"));
        assertThat(fixture.stored.get()).isNotNull();
    }

    @Test
    void deletingProjectionFailsBeforeRegistrationMutation() {
        TopicProjectionRecord deleting =
                topic(
                        1,
                        4,
                        Map.of(),
                        ManagedLedgerFacadeState.DELETING);
        Fixture fixture = new Fixture(
                stream(1, StreamState.DELETING));
        fixture.projections.add(Optional.of(deleting));

        assertThatThrownBy(() -> fixture.coordinator()
                        .ensureRegistered(
                                NAME, deleting.projectionIdentity())
                        .join())
                .satisfies(error -> assertThat(rootCause(error))
                        .isInstanceOf(NereusException.class)
                        .hasMessageContaining("not live"));
        assertThat(fixture.mutationCalls).hasValue(0);
        assertThat(fixture.stored.get()).isNull();
    }

    private static TopicProjectionRecord topic(
            long incarnation,
            long metadataVersion,
            Map<String, String> properties) {
        return topic(
                incarnation,
                metadataVersion,
                properties,
                ManagedLedgerFacadeState.OPEN);
    }

    private static TopicProjectionRecord topic(
            long incarnation,
            long metadataVersion,
            Map<String, String> properties,
            ManagedLedgerFacadeState state) {
        return new TopicProjectionRecord(
                NAME,
                ManagedLedgerProjectionNames.managedLedgerNameHash(
                        NAME),
                incarnation + 6,
                incarnation,
                ManagedLedgerProjectionNames.streamName(
                                NAME, incarnation)
                        .value(),
                ManagedLedgerProjectionNames.streamId(
                                NAME, incarnation)
                        .value(),
                ManagedLedgerProjectionNames.STORAGE_CLASS,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID
                        + incarnation,
                ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1,
                state.name(),
                properties,
                100,
                0,
                metadataVersion);
    }

    private static StreamMetadataSnapshot stream(
            long commitVersion, StreamState state) {
        String streamId =
                ManagedLedgerProjectionNames.streamId(NAME, 1).value();
        long endOffset = commitVersion == 0 ? 0 : commitVersion;
        long cumulative = commitVersion == 0 ? 0 : commitVersion * 10;
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        streamId,
                        ManagedLedgerProjectionNames.streamName(
                                        NAME, 1)
                                .value(),
                        "stream-name-hash",
                        state.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(
                                ManagedLedgerProjectionNames
                                        .PAYLOAD_MAPPING_ATTRIBUTE,
                                ManagedLedgerProjectionNames
                                        .PAYLOAD_MAPPING_V1),
                        100,
                        0,
                        11),
                new CommittedEndOffsetRecord(
                        streamId,
                        endOffset,
                        cumulative,
                        commitVersion,
                        11),
                new TrimRecord(streamId, 0, "", 100, 11));
    }

    private static VersionedMaterializationStreamRegistration versioned(
            MaterializationStreamRegistrationRecord value,
            long version) {
        return new VersionedMaterializationStreamRegistration(
                "/registration",
                value.withMetadataVersion(version),
                version,
                sha('a'));
    }

    private static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256,
                String.valueOf(value).repeat(64));
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class Fixture {
        private final ArrayDeque<Optional<TopicProjectionRecord>>
                projections = new ArrayDeque<>();
        private final StreamMetadataSnapshot stream;
        private final AtomicReference<
                        VersionedMaterializationStreamRegistration>
                stored = new AtomicReference<>();
        private final AtomicInteger mutationCalls =
                new AtomicInteger();
        private boolean loseCasResponse;

        private Fixture(StreamMetadataSnapshot stream) {
            this.stream = stream;
        }

        private DefaultManagedLedgerMaterializationRegistrationCoordinator
                coordinator() {
            return new DefaultManagedLedgerMaterializationRegistrationCoordinator(
                    CLUSTER,
                    projectionStore(),
                    l0Store(),
                    generationStore(),
                    CLOCK);
        }

        private ManagedLedgerProjectionMetadataStore projectionStore() {
            return proxy(
                    ManagedLedgerProjectionMetadataStore.class,
                    (method, arguments) -> {
                        if (method.equals("getProjection")) {
                            Optional<TopicProjectionRecord> value =
                                    projections.size() > 1
                                            ? projections.removeFirst()
                                            : projections.getFirst();
                            return CompletableFuture.completedFuture(
                                    value);
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
                            StreamId expected =
                                    ManagedLedgerProjectionNames
                                            .streamId(NAME, 1);
                            if (!CLUSTER.equals(arguments[0])
                                    || !expected.equals(arguments[1])) {
                                return CompletableFuture.failedFuture(
                                        new AssertionError(
                                                "unexpected L0 lookup"));
                            }
                            return CompletableFuture
                                    .completedFuture(stream);
                        }
                        if (method.equals("close")) {
                            return null;
                        }
                        throw new UnsupportedOperationException(method);
                    });
        }

        private GenerationMetadataStore generationStore() {
            return proxy(
                    GenerationMetadataStore.class,
                    (method, arguments) -> {
                        if (method.equals(
                                "createOrVerifyStreamRegistration")) {
                            mutationCalls.incrementAndGet();
                            VersionedMaterializationStreamRegistration
                                    current = stored.get();
                            if (current == null) {
                                current = versioned(
                                        (MaterializationStreamRegistrationRecord)
                                                arguments[1],
                                        1);
                                stored.set(current);
                            }
                            return CompletableFuture.completedFuture(
                                    current);
                        }
                        if (method.equals(
                                "compareAndSetStreamRegistration")) {
                            mutationCalls.incrementAndGet();
                            VersionedMaterializationStreamRegistration
                                    current = stored.get();
                            long expectedVersion =
                                    (long) arguments[2];
                            if (current == null
                                    || current.metadataVersion()
                                            != expectedVersion) {
                                return CompletableFuture.failedFuture(
                                        new F4MetadataConditionFailedException(
                                                "version mismatch"));
                            }
                            VersionedMaterializationStreamRegistration
                                    replacement = versioned(
                                            (MaterializationStreamRegistrationRecord)
                                                    arguments[1],
                                            expectedVersion + 1);
                            stored.set(replacement);
                            if (loseCasResponse) {
                                return CompletableFuture.failedFuture(
                                        new F4MetadataConditionFailedException(
                                                "simulated lost CAS response"));
                            }
                            return CompletableFuture.completedFuture(
                                    replacement);
                        }
                        if (method.equals("getStreamRegistration")) {
                            return CompletableFuture.completedFuture(
                                    Optional.ofNullable(stored.get()));
                        }
                        if (method.equals("close")) {
                            return null;
                        }
                        throw new UnsupportedOperationException(method);
                    });
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(
            Class<T> type, ProxyHandler handler) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (instance, method, arguments) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" ->
                                    "registration-test-"
                                            + type.getSimpleName();
                            case "hashCode" ->
                                    System.identityHashCode(instance);
                            case "equals" ->
                                    instance == arguments[0];
                            default ->
                                    throw new UnsupportedOperationException(
                                            method.getName());
                        };
                    }
                    return handler.invoke(
                            method.getName(),
                            arguments == null
                                    ? new Object[0]
                                    : arguments);
                });
    }

    @FunctionalInterface
    private interface ProxyHandler {
        Object invoke(String method, Object[] arguments)
                throws Throwable;
    }
}
