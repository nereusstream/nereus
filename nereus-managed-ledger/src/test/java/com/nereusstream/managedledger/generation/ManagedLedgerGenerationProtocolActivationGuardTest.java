/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.capability.DomainValidatedDeletionSubject;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationCapabilityReadiness;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStoreTestFactory;
import com.nereusstream.metadata.oxia.ManagedLedgerGenerationProtocol;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionCreateRequest;
import com.nereusstream.metadata.oxia.ProjectionPublishGuard;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class ManagedLedgerGenerationProtocolActivationGuardTest {
    private static final String CLUSTER = F4MetadataTestValues.CLUSTER;
    private static final String NAME =
            "tenant/ns/persistent/generation-activation";
    private static final long EPOCH = 7;
    private static final String RUN_ID =
            "abcdefghijklmnopqrstuvwxyz";
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
    private static final ProjectionPublishGuard ALLOW =
            () -> CompletableFuture.completedFuture(null);

    @Test
    void firstActivationRequiresExactClusterRegistrationAndMarksTheTopic() {
        try (Fixture fixture = new Fixture()) {
            fixture.installActivation(false, true);
            ManagedLedgerGenerationProtocolActivationGuard guard =
                    fixture.guard(true);

            GenerationActivationProof proof = guard.requireReady(
                            GenerationOperation.RECOVERY_CHECKPOINT,
                            fixture.subject,
                            true)
                    .join();

            TopicProjectionRecord activated =
                    fixture.currentProjection();
            assertThat(ManagedLedgerGenerationProtocol.isActivated(
                            activated))
                    .isTrue();
            assertThat(proof.subjectValidationVersion())
                    .isEqualTo(activated.metadataVersion());
            assertThat(proof.clusterActivationMetadataVersion())
                    .isEqualTo(fixture.currentActivation()
                            .metadataVersion());
            assertThat(proof.brokerCapabilityReadinessEpoch())
                    .isEqualTo(EPOCH);
            assertThat(proof.referenceDomainSetSha256().value())
                    .isEqualTo(
                            "5b29cf6df71cce198d01299f5bd740f0f123c601e12f04d8251d336a6a2a8c4d");
            assertThat(proof.publicationEnabled()).isTrue();
            assertThat(proof.deletionEnabled()).isFalse();
            guard.revalidate(proof).join();
        }
    }

    @Test
    void markerWriteResponseLossConvergesByExactProjectionReload() {
        try (Fixture fixture = new Fixture()) {
            fixture.installActivation(false, true);
            fixture.projections.failNext(
                    FakeManagedLedgerProjectionMetadataStore
                            .FailurePoint.AFTER_TOPIC_WRITE);

            GenerationActivationProof proof = fixture.guard(true)
                    .requireReady(
                            GenerationOperation.RECOVERY_CHECKPOINT,
                            fixture.subject,
                            true)
                    .join();

            assertThat(ManagedLedgerGenerationProtocol.isActivated(
                            fixture.currentProjection()))
                    .isTrue();
            fixture.guard(false).revalidate(proof).join();
        }
    }

    @Test
    void disabledFirstActivationLeavesMarkerAbsentButCannotDisableAnExistingMarker() {
        try (Fixture fixture = new Fixture()) {
            fixture.installActivation(false, true);

            assertConditionFailure(() -> fixture.guard(false)
                    .requireReady(
                            GenerationOperation.RECOVERY_CHECKPOINT,
                            fixture.subject,
                            true)
                    .join());
            assertThat(ManagedLedgerGenerationProtocol.isActivated(
                            fixture.currentProjection()))
                    .isFalse();

            TopicProjectionRecord current =
                    fixture.currentProjection();
            fixture.projections.activateGenerationProtocol(
                            CLUSTER,
                            NAME,
                            current.projectionIdentity(),
                            current.metadataVersion())
                    .join();
            GenerationActivationProof existing =
                    fixture.guard(false)
                            .requireReady(
                                    GenerationOperation
                                            .GENERATION_PUBLISH,
                                    fixture.subject,
                                    false)
                            .join();
            assertThat(existing.publicationEnabled()).isTrue();
        }
    }

    @Test
    void incompleteCoverageOrMissingRegistrationCannotCreateAMarker() {
        try (Fixture fixture = new Fixture()) {
            fixture.installActivation(false, false);
            assertConditionFailure(() -> fixture.guard(true)
                    .requireReady(
                            GenerationOperation.RECOVERY_CHECKPOINT,
                            fixture.subject,
                            true)
                    .join());
            assertThat(ManagedLedgerGenerationProtocol.isActivated(
                            fixture.currentProjection()))
                    .isFalse();
        }

        try (Fixture fixture = new Fixture()) {
            fixture.installActivation(false, true);
            GenerationMetadataStore missing =
                    proxy(
                            GenerationMetadataStore.class,
                            (method, arguments) -> {
                                if (method.equals(
                                        "getStreamRegistration")) {
                                    return CompletableFuture
                                            .completedFuture(
                                                    Optional.empty());
                                }
                                if (method.equals("close")) {
                                    return null;
                                }
                                throw new UnsupportedOperationException(
                                        method);
                            });
            assertConditionFailure(() -> fixture.guard(
                            true, missing)
                    .requireReady(
                            GenerationOperation.RECOVERY_CHECKPOINT,
                            fixture.subject,
                            true)
                    .join());
            assertThat(ManagedLedgerGenerationProtocol.isActivated(
                            fixture.currentProjection()))
                    .isFalse();
        }
    }

    @Test
    void projectionOrBrokerReadinessDriftInvalidatesTheEphemeralProof() {
        try (Fixture fixture = new Fixture()) {
            fixture.installActivation(false, true);
            ManagedLedgerGenerationProtocolActivationGuard guard =
                    fixture.guard(true);
            GenerationActivationProof proof = guard.requireReady(
                            GenerationOperation.RECOVERY_CHECKPOINT,
                            fixture.subject,
                            true)
                    .join();

            TopicProjectionRecord current =
                    fixture.currentProjection();
            fixture.projections.updateProperties(
                            CLUSTER,
                            NAME,
                            current.projectionIdentity(),
                            current.metadataVersion(),
                            Map.of("owner", "changed"))
                    .join();
            assertConditionFailure(() -> guard.revalidate(proof)
                    .join());

            fixture.readiness.value = null;
            assertConditionFailure(() -> guard.revalidate(proof)
                    .join());
        }
    }

    @Test
    void physicalDeleteRequiresDeleteBitsAndTheExactProjectionDomainSnapshot() {
        try (Fixture fixture = new Fixture()) {
            GcReferenceQuery query = query(
                    fixture.subject.streamId());
            GcReferenceSnapshot exact =
                    snapshot(query, false);
            fixture.projectionDomain.snapshot = exact;
            DomainValidatedDeletionSubject subject =
                    new DomainValidatedDeletionSubject(
                            query, exact.snapshotSha256());

            fixture.installActivation(false, true);
            assertConditionFailure(() -> fixture.guard(false)
                    .requireReady(
                            GenerationOperation.PHYSICAL_DELETE,
                            subject,
                            false)
                    .join());

            fixture.replaceActivationWithNewEpoch(true);
            fixture.readiness.value = readiness(8);
            ManagedLedgerGenerationProtocolActivationGuard guard =
                    fixture.guard(false);
            GenerationActivationProof proof = guard.requireReady(
                            GenerationOperation.PHYSICAL_DELETE,
                            subject,
                            false)
                    .join();
            assertThat(proof.subjectValidationVersion()).isZero();
            assertThat(proof.deletionEnabled()).isTrue();
            guard.revalidate(proof).join();

            fixture.projectionDomain.snapshot =
                    snapshot(query, true);
            assertConditionFailure(() -> guard.revalidate(proof)
                    .join());
        }
    }

    @Test
    void physicalDeleteRejectsAuthorityForAnotherConfiguredObjectStoreScope() {
        try (Fixture fixture = new Fixture()) {
            GcReferenceQuery query = query(fixture.subject.streamId());
            GcReferenceSnapshot exact = snapshot(query, false);
            fixture.projectionDomain.snapshot = exact;
            DomainValidatedDeletionSubject subject =
                    new DomainValidatedDeletionSubject(
                            query, exact.snapshotSha256());

            fixture.installActivation(true, true);

            assertConditionFailure(() -> fixture
                    .guard(false, sha("88").value())
                    .requireReady(
                            GenerationOperation.PHYSICAL_DELETE,
                            subject,
                            false)
                    .join());
        }
    }

    private static GcReferenceQuery query(StreamId streamId) {
        PhysicalObjectIdentity object =
                PhysicalObjectIdentity.create(
                        new ObjectKey(
                                "clusters/test/objects/activation"),
                        Optional.empty(),
                        PhysicalObjectKind.COMMITTED_COMPACTED,
                        1,
                        new Checksum(
                                ChecksumType.CRC32C,
                                "00000000"),
                        Optional.of(sha("55")),
                        Optional.of("etag"));
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object,
                List.of(streamId),
                sha("66"));
    }

    private static GcReferenceSnapshot snapshot(
            GcReferenceQuery query, boolean veto) {
        return GcReferenceSnapshot.create(
                "projection-generation-v1",
                1,
                query.queryIdentitySha256(),
                true,
                veto,
                0,
                0,
                List.of(),
                List.of());
    }

    private static GenerationCapabilityReadiness readiness(
            long epoch) {
        return new GenerationCapabilityReadiness(
                epoch, sha(epoch == EPOCH ? "11" : "22"), 2);
    }

    private static Checksum sha(String pair) {
        return new Checksum(
                ChecksumType.SHA256, pair.repeat(32));
    }

    private static void assertConditionFailure(
            Runnable operation) {
        assertThatThrownBy(operation::run)
                .satisfies(error -> assertThat(rootCause(error))
                        .isInstanceOf(NereusException.class)
                        .extracting(
                                value -> ((NereusException) value)
                                        .code())
                        .isEqualTo(
                                com.nereusstream.api.ErrorCode
                                        .METADATA_CONDITION_FAILED));
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(
            Class<T> type, Invocation invocation) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (instance, method, arguments) -> invocation.invoke(
                        method.getName(),
                        arguments == null
                                ? new Object[0]
                                : arguments));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(String method, Object[] arguments)
                throws Throwable;
    }

    private static final class Fixture
            implements AutoCloseable {
        private final FakeManagedLedgerProjectionMetadataStore
                projections =
                        new FakeManagedLedgerProjectionMetadataStore();
        private final GenerationMetadataStore generations =
                GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        private final GenerationProtocolActivationStore activations =
                GenerationProtocolActivationStoreTestFactory.inMemory(
                        CLOCK,
                        F4MetadataTestValues.PROCESS,
                        F4MetadataTestValues.referenceDomains());
        private final MutableReadiness readiness =
                new MutableReadiness(readiness(EPOCH));
        private final MutableProjectionDomain projectionDomain =
                new MutableProjectionDomain();
        private final StreamMetadataSnapshot stream;
        private final OxiaMetadataStore l0;
        private final LiveProjectionSubject subject;

        private Fixture() {
            TopicProjectionRecord projection =
                    projections.createFirstProjection(
                                    CLUSTER,
                                    request(),
                                    ALLOW)
                            .join();
            stream = stream(projection);
            l0 = proxy(
                    OxiaMetadataStore.class,
                    (method, arguments) -> {
                        if (method.equals(
                                "getStreamSnapshot")) {
                            return CompletableFuture
                                    .completedFuture(stream);
                        }
                        if (method.equals("close")) {
                            return null;
                        }
                        throw new UnsupportedOperationException(
                                method);
                    });
            new DefaultManagedLedgerMaterializationRegistrationCoordinator(
                            CLUSTER,
                            projections,
                            l0,
                            generations,
                            CLOCK)
                    .ensureRegistered(
                            NAME,
                            projection.projectionIdentity())
                    .join();
            ManagedLedgerGenerationProjectionRefV1 reference =
                    new ManagedLedgerGenerationProjectionRefV1(
                            NAME,
                            projection.projectionIdentity());
            subject = new LiveProjectionSubject(
                    new StreamId(projection.streamId()),
                    reference.toProjectionRef(),
                    reference.projectionIdentitySha256());
        }

        private ManagedLedgerGenerationProtocolActivationGuard
                guard(boolean enabled) {
            return guard(enabled, generations);
        }

        private ManagedLedgerGenerationProtocolActivationGuard
                guard(boolean enabled, String capabilitySha256) {
            return guard(enabled, generations, capabilitySha256);
        }

        private ManagedLedgerGenerationProtocolActivationGuard
                guard(
                        boolean enabled,
                        GenerationMetadataStore generationStore) {
            return guard(enabled, generationStore, sha("77").value());
        }

        private ManagedLedgerGenerationProtocolActivationGuard
                guard(
                        boolean enabled,
                        GenerationMetadataStore generationStore,
                        String capabilitySha256) {
            return new ManagedLedgerGenerationProtocolActivationGuard(
                    CLUSTER,
                    enabled,
                    readiness,
                    activations,
                    F4MetadataTestValues.referenceDomains(),
                    capabilitySha256,
                    projections,
                    l0,
                    generationStore,
                    projectionDomain,
                    CLOCK);
        }

        private void installActivation(
                boolean deletion,
                boolean streamComplete) {
            VersionedGenerationProtocolActivation prepared =
                    activations.getOrCreate(CLUSTER).join();
            activations.compareAndSet(
                            CLUSTER,
                            activation(
                                    prepared.value(),
                                    EPOCH,
                                    deletion,
                                    streamComplete),
                            prepared.metadataVersion())
                    .join();
        }

        private void replaceActivationWithNewEpoch(
                boolean deletion) {
            VersionedGenerationProtocolActivation current =
                    currentActivation();
            activations.compareAndSet(
                            CLUSTER,
                            activation(
                                    current.value(),
                                    8,
                                    deletion,
                                    true),
                            current.metadataVersion())
                    .join();
        }

        private VersionedGenerationProtocolActivation
                currentActivation() {
            return activations.get(CLUSTER)
                    .join()
                    .orElseThrow();
        }

        private TopicProjectionRecord currentProjection() {
            return projections.getProjection(CLUSTER, NAME)
                    .join()
                    .orElseThrow();
        }

        @Override
        public void close() {
            activations.close();
            generations.close();
            projections.close();
        }
    }

    private static GenerationProtocolActivationRecord activation(
            GenerationProtocolActivationRecord current,
            long epoch,
            boolean deletion,
            boolean streamComplete) {
        GenerationBackfillProofRecord streamProof =
                streamComplete
                        ? complete(
                                RUN_ID,
                                epoch,
                                sha("33").value(),
                                1_100)
                        : GenerationBackfillProofRecord.incomplete(
                                epoch);
        GenerationBackfillProofRecord physical =
                deletion
                        ? complete(
                                "bcdefghijklmnopqrstuvwxyza",
                                epoch,
                                sha("44").value(),
                                1_101)
                        : GenerationBackfillProofRecord.incomplete(
                                epoch);
        GenerationBackfillProofRecord cursor =
                deletion
                        ? complete(
                                "cdefghijklmnopqrstuvwxyzab",
                                epoch,
                                sha("55").value(),
                                1_102)
                        : GenerationBackfillProofRecord.incomplete(
                                epoch);
        long activatedAt =
                current.lifecycle()
                                == GenerationProtocolActivationLifecycle
                                        .ACTIVE
                        ? current.activatedAtMillis()
                        : 1_050;
        return new GenerationProtocolActivationRecord(
                current.schemaVersion(),
                current.protocolVersion(),
                GenerationProtocolActivationLifecycle.ACTIVE,
                true,
                deletion,
                deletion,
                epoch,
                current.requiredReferenceDomains(),
                streamProof,
                physical,
                cursor,
                deletion ? sha("77").value() : "",
                current.activatingBrokerRunId(),
                current.preparedAtMillis(),
                activatedAt,
                Math.max(current.updatedAtMillis(), 1_103),
                0);
    }

    private static GenerationBackfillProofRecord complete(
            String runId,
            long epoch,
            String coverage,
            long completedAt) {
        return new GenerationBackfillProofRecord(
                runId, epoch, coverage, true, completedAt);
    }

    private static ProjectionCreateRequest request() {
        return new ProjectionCreateRequest(
                NAME,
                7,
                1,
                new StreamMetadata(
                        ManagedLedgerProjectionNames.streamId(
                                NAME, 1),
                        ManagedLedgerProjectionNames.streamName(
                                NAME, 1),
                        StreamState.ACTIVE,
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        Map.of(
                                ManagedLedgerProjectionNames
                                        .PAYLOAD_MAPPING_ATTRIBUTE,
                                ManagedLedgerProjectionNames
                                        .PAYLOAD_MAPPING_V1),
                        100,
                        0,
                        0,
                        0,
                        0),
                Map.of());
    }

    private static StreamMetadataSnapshot stream(
            TopicProjectionRecord projection) {
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        projection.streamId(),
                        projection.streamName(),
                        "stream-name-hash",
                        StreamState.ACTIVE.name(),
                        projection.storageProfile(),
                        Map.of(
                                ManagedLedgerProjectionNames
                                        .PAYLOAD_MAPPING_ATTRIBUTE,
                                ManagedLedgerProjectionNames
                                        .PAYLOAD_MAPPING_V1),
                        projection.createdAtMillis(),
                        0,
                        11),
                new CommittedEndOffsetRecord(
                        projection.streamId(),
                        1,
                        10,
                        1,
                        11),
                new TrimRecord(
                        projection.streamId(),
                        0,
                        "",
                        100,
                        11));
    }

    private static final class MutableReadiness
            implements GenerationCapabilityReadinessProvider {
        private GenerationCapabilityReadiness value;

        private MutableReadiness(
                GenerationCapabilityReadiness value) {
            this.value = value;
        }

        @Override
        public CompletableFuture<GenerationCapabilityReadiness>
                requireGenerationCapabilityReadiness() {
            return value == null
                    ? CompletableFuture.failedFuture(
                            new IllegalStateException(
                                    "readiness unavailable"))
                    : CompletableFuture.completedFuture(value);
        }

        @Override
        public Optional<GenerationCapabilityReadiness>
                currentGenerationCapabilityReadiness() {
            return Optional.ofNullable(value);
        }
    }

    private static final class MutableProjectionDomain
            implements GcReferenceDomain {
        private GcReferenceSnapshot snapshot;

        @Override
        public String domainId() {
            return "projection-generation-v1";
        }

        @Override
        public int protocolVersion() {
            return 1;
        }

        @Override
        public CompletableFuture<GcReferenceSnapshot> snapshot(
                GcReferenceQuery query) {
            if (snapshot == null
                    || !snapshot.queryIdentitySha256()
                            .equals(query.queryIdentitySha256())) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException(
                                "projection snapshot unavailable"));
            }
            return CompletableFuture.completedFuture(snapshot);
        }

        @Override
        public CompletableFuture<Boolean> stillMatches(
                GcReferenceQuery query,
                GcReferenceSnapshot expected) {
            return CompletableFuture.completedFuture(
                    snapshot != null
                            && snapshot.equals(expected)
                            && snapshot.queryIdentitySha256()
                                    .equals(query
                                            .queryIdentitySha256()));
        }
    }
}
