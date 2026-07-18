/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.capability.GenerationProjectionAuthorityReader;
import com.nereusstream.core.capability.GenerationProjectionAuthoritySnapshot;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.FakeCursorMetadataStore;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStoreTestFactory;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.GenerationZeroIndexEncoding;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.metadata.oxia.retirement.GenericCommittedAppendIdentity;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroCommit;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class PhysicalRootBackfillCoordinatorTest {
    private static final String CLUSTER = "cluster/a";
    private static final long READINESS_EPOCH = 7;
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(3_000), ZoneOffset.UTC);
    private static final String RUN_ID = "b".repeat(26);

    @Test
    void emptyStableRegistryPublishesBothProofsAndRecoversLostCasResponse() {
        try (GenerationMetadataStore generations =
                        GenerationMetadataStoreTestFactory.inMemory(CLOCK);
                GenerationProtocolActivationStore durableActivation =
                        activationStore();
                CursorMetadataStore cursors =
                        new FakeCursorMetadataStore()) {
            prepareRegistrationBackfill(durableActivation);
            GenerationProtocolActivationStore lossy =
                    loseFirstSuccessfulCasResponse(durableActivation);
            FakePhysicalObjectMetadataStore physical =
                    new FakePhysicalObjectMetadataStore();
            try (DefaultObjectProtectionManager protections =
                            protectionManager(physical);
                    SourceRetirementMetadataStore sources =
                            unsupported(SourceRetirementMetadataStore.class);
                    OxiaMetadataStore l0 =
                            unsupported(OxiaMetadataStore.class);
                    ObjectStore objects =
                            unsupported(ObjectStore.class)) {
                var coordinator =
                        new DefaultPhysicalRootBackfillCoordinator(
                                CLUSTER,
                                l0,
                                generations,
                                sources,
                                cursors,
                                lossy,
                                physical,
                                protections,
                                objects,
                                subject -> CompletableFuture.failedFuture(
                                        new AssertionError(
                                                "empty registry cannot read a projection")),
                                8,
                                Duration.ofSeconds(1),
                                CLOCK);

                PhysicalRootBackfillReport report = coordinator.run(
                                new PhysicalRootBackfillRequest(
                                        RUN_ID,
                                        READINESS_EPOCH,
                                        4,
                                        Duration.ofSeconds(5)))
                        .join();

                assertThat(report.failureCount()).isZero();
                assertThat(report.streamsScanned()).isZero();
                assertThat(report.dataObjectsScanned()).isZero();
                assertThat(report.cursorObjectsScanned()).isZero();
                VersionedGenerationProtocolActivation activation =
                        durableActivation.get(CLUSTER).join().orElseThrow();
                assertThat(activation.value().physicalRootBackfill().complete())
                        .isTrue();
                assertThat(activation.value().cursorSnapshotBackfill().complete())
                        .isTrue();
                assertThat(activation.value().physicalRootBackfill().runId())
                        .isEqualTo(RUN_ID);
                assertThat(activation.value().physicalRootBackfill().coverageSha256())
                        .isEqualTo(report.dataCoverageSha256().value());
                assertThat(activation.value().cursorSnapshotBackfill().coverageSha256())
                        .isEqualTo(report.cursorCoverageSha256().value());
            }
        }
    }

    @Test
    void reachableCommitAndVisibleIndexCreateExactRootAndTwoOwnerProtections() {
        StreamFixture fixture = streamFixture();
        try (GenerationMetadataStore durableGenerations =
                        GenerationMetadataStoreTestFactory.inMemory(CLOCK);
                GenerationProtocolActivationStore activations =
                        activationStore();
                CursorMetadataStore cursors =
                        new FakeCursorMetadataStore()) {
            prepareRegistrationBackfill(activations);
            VersionedMaterializationStreamRegistration registration =
                    durableGenerations.createOrVerifyStreamRegistration(
                                    CLUSTER,
                                    fixture.registration())
                            .join();
            GenerationMetadataStore generations = generationStore(
                    durableGenerations,
                    fixture.zeroIndex());
            FakePhysicalObjectMetadataStore physical =
                    new FakePhysicalObjectMetadataStore();
            try (DefaultObjectProtectionManager protections =
                            protectionManager(physical);
                    SourceRetirementMetadataStore sources =
                            sourceStore(fixture.commitSource());
                    OxiaMetadataStore l0 = l0Store(fixture);
                    ObjectStore objects = headOnlyStore(fixture.head())) {
                var coordinator =
                        new DefaultPhysicalRootBackfillCoordinator(
                                CLUSTER,
                                l0,
                                generations,
                                sources,
                                cursors,
                                activations,
                                physical,
                                protections,
                                objects,
                                projectionReader(
                                        fixture.projectionIdentity()),
                                2,
                                Duration.ofSeconds(1),
                                CLOCK);

                PhysicalRootBackfillReport report = coordinator.run(
                                new PhysicalRootBackfillRequest(
                                        RUN_ID,
                                        READINESS_EPOCH,
                                        2,
                                        Duration.ofSeconds(5)))
                        .join();

                assertThat(report.failureCount()).isZero();
                assertThat(report.streamsScanned()).isOne();
                assertThat(report.dataObjectsScanned()).isEqualTo(2);
                assertThat(report.cursorObjectsScanned()).isZero();
                assertThat(report.rootsCreatedOrVerified()).isEqualTo(2);
                assertThat(report.protectionsCreatedOrVerified()).isEqualTo(2);
                assertThat(registration.value().streamId())
                        .isEqualTo(fixture.streamId().value());

                ObjectKeyHash objectHash =
                        ObjectKeyHash.from(fixture.objectKey());
                var root = physical.getRoot(CLUSTER, objectHash)
                        .join()
                        .orElseThrow();
                assertThat(root.value().objectKey())
                        .isEqualTo(fixture.objectKey().value());
                assertThat(physical.scanProtections(
                                        CLUSTER,
                                        objectHash,
                                        Optional.empty(),
                                        100)
                                .join()
                                .values())
                        .hasSize(2)
                        .extracting(value -> value.value().protectionTypeId())
                        .containsExactlyInAnyOrder(
                                com.nereusstream.metadata.oxia.records.ObjectProtectionType
                                        .REACHABLE_APPEND
                                        .wireId(),
                                com.nereusstream.metadata.oxia.records.ObjectProtectionType
                                        .VISIBLE_GENERATION
                                        .wireId());
            }
        }
    }

    @Test
    void activeCursorSnapshotCreatesExactEtagRootAndPermanentOwnerProtection() {
        CursorFixture fixture = cursorFixture();
        try (GenerationMetadataStore generations =
                        GenerationMetadataStoreTestFactory.inMemory(CLOCK);
                GenerationProtocolActivationStore activations =
                        activationStore();
                FakeCursorMetadataStore cursors =
                        new FakeCursorMetadataStore()) {
            prepareRegistrationBackfill(activations);
            generations.createOrVerifyStreamRegistration(
                            CLUSTER, fixture.registration())
                    .join();
            cursors.createRetention(
                            CLUSTER, fixture.retention())
                    .join();
            cursors.createCursor(CLUSTER, fixture.cursor())
                    .join();
            FakePhysicalObjectMetadataStore physical =
                    new FakePhysicalObjectMetadataStore();
            try (DefaultObjectProtectionManager protections =
                            protectionManager(physical);
                    SourceRetirementMetadataStore sources =
                            unsupported(SourceRetirementMetadataStore.class);
                    OxiaMetadataStore l0 = cursorL0Store(fixture);
                    ObjectStore objects =
                            headOnlyStore(fixture.head())) {
                var coordinator =
                        new DefaultPhysicalRootBackfillCoordinator(
                                CLUSTER,
                                l0,
                                generations,
                                sources,
                                cursors,
                                activations,
                                physical,
                                protections,
                                objects,
                                projectionReader(
                                        fixture.projectionIdentity()),
                                2,
                                Duration.ofSeconds(1),
                                CLOCK);

                PhysicalRootBackfillReport report = coordinator.run(
                                new PhysicalRootBackfillRequest(
                                        RUN_ID,
                                        READINESS_EPOCH,
                                        2,
                                        Duration.ofSeconds(5)))
                        .join();

                assertThat(report.failureCount()).isZero();
                assertThat(report.streamsScanned()).isOne();
                assertThat(report.dataObjectsScanned()).isZero();
                assertThat(report.cursorObjectsScanned()).isOne();
                ObjectKeyHash hash =
                        ObjectKeyHash.from(fixture.objectKey());
                var root = physical.getRoot(CLUSTER, hash)
                        .join()
                        .orElseThrow();
                assertThat(root.value().etag())
                        .isEqualTo("cursor-etag");
                assertThat(physical.scanProtections(
                                        CLUSTER,
                                        hash,
                                        Optional.empty(),
                                        100)
                                .join()
                                .values())
                        .singleElement()
                        .satisfies(value -> {
                            assertThat(
                                            value.value()
                                                    .protectionTypeId())
                                    .isEqualTo(
                                            com.nereusstream.metadata.oxia.records.ObjectProtectionType
                                                    .CURSOR_SNAPSHOT_ROOT
                                                    .wireId());
                            assertThat(value.value().ownerKey())
                                    .contains("/cursors/v1/");
                        });
            }
        }
    }

    @Test
    void incompleteRegistrationCoverageFailsClosedWithoutScanningOrPublishing() {
        try (GenerationMetadataStore generations =
                        GenerationMetadataStoreTestFactory.inMemory(CLOCK);
                GenerationProtocolActivationStore activations =
                        activationStore();
                CursorMetadataStore cursors =
                        new FakeCursorMetadataStore()) {
            activations.getOrCreate(CLUSTER).join();
            FakePhysicalObjectMetadataStore physical =
                    new FakePhysicalObjectMetadataStore();
            try (DefaultObjectProtectionManager protections =
                            protectionManager(physical);
                    SourceRetirementMetadataStore sources =
                            unsupported(SourceRetirementMetadataStore.class);
                    OxiaMetadataStore l0 =
                            unsupported(OxiaMetadataStore.class);
                    ObjectStore objects =
                            unsupported(ObjectStore.class)) {
                var coordinator =
                        new DefaultPhysicalRootBackfillCoordinator(
                                CLUSTER,
                                l0,
                                generations,
                                sources,
                                cursors,
                                activations,
                                physical,
                                protections,
                                objects,
                                subject -> CompletableFuture.failedFuture(
                                        new AssertionError()),
                                8,
                                Duration.ofSeconds(1),
                                CLOCK);

                PhysicalRootBackfillReport report = coordinator.run(
                                new PhysicalRootBackfillRequest(
                                        RUN_ID,
                                        0,
                                        1,
                                        Duration.ofSeconds(5)))
                        .join();

                assertThat(report.failureCount()).isOne();
                assertThat(report.boundedFailures())
                        .extracting(PhysicalRootBackfillFailure::errorCode)
                        .containsExactly("REGISTRATION_BACKFILL_INCOMPLETE");
                assertThat(activations.get(CLUSTER)
                                .join()
                                .orElseThrow()
                                .value()
                                .physicalRootBackfill()
                                .complete())
                        .isFalse();
            }
        }
    }

    @Test
    void deletionActiveRolloverScansWithoutPublishingPartialProofs() {
        try (GenerationMetadataStore generations =
                        GenerationMetadataStoreTestFactory.inMemory(CLOCK);
                GenerationProtocolActivationStore activations =
                        activationStore();
                CursorMetadataStore cursors =
                        new FakeCursorMetadataStore()) {
            VersionedGenerationProtocolActivation expected =
                    prepareDeletionAuthority(activations);
            FakePhysicalObjectMetadataStore physical =
                    new FakePhysicalObjectMetadataStore();
            try (DefaultObjectProtectionManager protections =
                            protectionManager(physical);
                    SourceRetirementMetadataStore sources =
                            unsupported(SourceRetirementMetadataStore.class);
                    OxiaMetadataStore l0 =
                            unsupported(OxiaMetadataStore.class);
                    ObjectStore objects =
                            unsupported(ObjectStore.class)) {
                var coordinator =
                        new DefaultPhysicalRootBackfillCoordinator(
                                CLUSTER,
                                l0,
                                generations,
                                sources,
                                cursors,
                                activations,
                                physical,
                                protections,
                                objects,
                                subject -> CompletableFuture.failedFuture(
                                        new AssertionError(
                                                "empty registry cannot read a projection")),
                                8,
                                Duration.ofSeconds(1),
                                CLOCK);

                PhysicalRootBackfillReport report = coordinator.runRollover(
                                new PhysicalRootBackfillRequest(
                                        RUN_ID,
                                        READINESS_EPOCH + 1,
                                        4,
                                        Duration.ofSeconds(5)),
                                expected)
                        .join();

                assertThat(report.failureCount()).isZero();
                assertThat(report.brokerReadinessEpoch())
                        .isEqualTo(READINESS_EPOCH + 1);
                assertThat(activations.get(CLUSTER).join())
                        .contains(expected);
                assertThat(expected.value().physicalRootBackfill()
                                .brokerReadinessEpoch())
                        .isEqualTo(READINESS_EPOCH);
                assertThat(expected.value().cursorSnapshotBackfill()
                                .brokerReadinessEpoch())
                        .isEqualTo(READINESS_EPOCH);
            }
        }
    }

    private static GenerationProtocolActivationStore activationStore() {
        return GenerationProtocolActivationStoreTestFactory.inMemory(
                CLOCK,
                F4MetadataTestValues.ATTEMPT,
                F4MetadataTestValues.referenceDomains());
    }

    private static void prepareRegistrationBackfill(
            GenerationProtocolActivationStore store) {
        VersionedGenerationProtocolActivation current =
                store.getOrCreate(CLUSTER).join();
        GenerationProtocolActivationRecord value = current.value();
        GenerationProtocolActivationRecord replacement =
                new GenerationProtocolActivationRecord(
                        value.schemaVersion(),
                        value.protocolVersion(),
                        value.lifecycle(),
                        false,
                        false,
                        false,
                        READINESS_EPOCH,
                        value.requiredReferenceDomains(),
                        new GenerationBackfillProofRecord(
                                F4MetadataTestValues.ATTEMPT,
                                READINESS_EPOCH,
                                F4MetadataTestValues.HASH_A,
                                true,
                                3_100),
                        GenerationBackfillProofRecord.incomplete(
                                READINESS_EPOCH),
                        GenerationBackfillProofRecord.incomplete(
                                READINESS_EPOCH),
                        "",
                        value.activatingBrokerRunId(),
                        value.preparedAtMillis(),
                        0,
                        3_200,
                        0);
        store.compareAndSet(
                        CLUSTER,
                        replacement,
                        current.metadataVersion())
                .join();
    }

    private static VersionedGenerationProtocolActivation
            prepareDeletionAuthority(
                    GenerationProtocolActivationStore store) {
        VersionedGenerationProtocolActivation current =
                store.getOrCreate(CLUSTER).join();
        GenerationProtocolActivationRecord value = current.value();
        GenerationProtocolActivationRecord replacement =
                new GenerationProtocolActivationRecord(
                        value.schemaVersion(),
                        value.protocolVersion(),
                        GenerationProtocolActivationLifecycle.ACTIVE,
                        true,
                        true,
                        true,
                        READINESS_EPOCH,
                        value.requiredReferenceDomains(),
                        completeProof(F4MetadataTestValues.HASH_A, 3_101),
                        completeProof(F4MetadataTestValues.HASH_B, 3_102),
                        completeProof(F4MetadataTestValues.HASH_C, 3_103),
                        F4MetadataTestValues.HASH_D,
                        value.activatingBrokerRunId(),
                        value.preparedAtMillis(),
                        3_100,
                        3_200,
                        0);
        return store.compareAndSet(
                        CLUSTER,
                        replacement,
                        current.metadataVersion())
                .join();
    }

    private static GenerationBackfillProofRecord completeProof(
            String coverage,
            long completedAtMillis) {
        return new GenerationBackfillProofRecord(
                RUN_ID,
                READINESS_EPOCH,
                coverage,
                true,
                completedAtMillis);
    }

    private static DefaultObjectProtectionManager protectionManager(
            FakePhysicalObjectMetadataStore physical) {
        return new DefaultObjectProtectionManager(
                CLUSTER,
                physical,
                Duration.ofMinutes(5),
                Duration.ofSeconds(1),
                Duration.ofMinutes(10),
                CLOCK);
    }

    private static GenerationProtocolActivationStore loseFirstSuccessfulCasResponse(
            GenerationProtocolActivationStore delegate) {
        AtomicBoolean first = new AtomicBoolean(true);
        return new GenerationProtocolActivationStore() {
            @Override
            public CompletableFuture<Optional<VersionedGenerationProtocolActivation>> get(
                    String cluster) {
                return delegate.get(cluster);
            }

            @Override
            public CompletableFuture<VersionedGenerationProtocolActivation> getOrCreate(
                    String cluster) {
                return delegate.getOrCreate(cluster);
            }

            @Override
            public CompletableFuture<VersionedGenerationProtocolActivation> compareAndSet(
                    String cluster,
                    GenerationProtocolActivationRecord replacement,
                    long expectedVersion) {
                return delegate.compareAndSet(
                                cluster, replacement, expectedVersion)
                        .thenCompose(updated -> first.compareAndSet(true, false)
                                ? CompletableFuture.failedFuture(
                                        new RuntimeException(
                                                "lost activation CAS response"))
                                : CompletableFuture.completedFuture(updated));
            }

            @Override
            public void close() {
                // Borrowed delegate.
            }
        };
    }

    private static GenerationMetadataStore generationStore(
            GenerationMetadataStore delegate,
            VersionedGenerationZeroIndex zero) {
        return proxy(
                GenerationMetadataStore.class,
                (method, arguments) -> switch (method.getName()) {
                    case "scanIndex" -> CompletableFuture.completedFuture(
                            new GenerationScanPage(
                                    List.of(zero), Optional.empty()));
                    case "getCandidateByKey" ->
                            CompletableFuture.completedFuture(
                                    Optional.<VersionedGenerationCandidate>of(
                                            zero));
                    case "getRecoveryRoot" ->
                            CompletableFuture.completedFuture(
                                    Optional.empty());
                    case "close" -> null;
                    default -> invoke(
                            delegate, method, arguments);
                });
    }

    private static OxiaMetadataStore l0Store(StreamFixture fixture) {
        return proxy(
                OxiaMetadataStore.class,
                (method, arguments) -> switch (method.getName()) {
                    case "getStreamSnapshot" ->
                            CompletableFuture.completedFuture(
                                    fixture.streamSnapshot());
                    case "readAppendRecoveryTail" ->
                            CompletableFuture.completedFuture(
                                    fixture.tail());
                    case "getObjectManifest" ->
                            CompletableFuture.completedFuture(
                                    Optional.of(fixture.manifest()));
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(
                            method.getName());
                });
    }

    private static OxiaMetadataStore cursorL0Store(
            CursorFixture fixture) {
        return proxy(
                OxiaMetadataStore.class,
                (method, arguments) -> switch (method.getName()) {
                    case "getStreamSnapshot" ->
                            CompletableFuture.completedFuture(
                                    fixture.streamSnapshot());
                    case "readAppendRecoveryTail" ->
                            CompletableFuture.completedFuture(
                                    fixture.tail());
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(
                            method.getName());
                });
    }

    private static SourceRetirementMetadataStore sourceStore(
            VersionedGenerationZeroCommit commit) {
        return proxy(
                SourceRetirementMetadataStore.class,
                (method, arguments) -> switch (method.getName()) {
                    case "getCommitNodeByKey" ->
                            CompletableFuture.completedFuture(
                                    Optional.of(commit));
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(
                            method.getName());
                });
    }

    private static GenerationProjectionAuthorityReader projectionReader(
            ManagedLedgerProjectionIdentity identity) {
        return subject -> CompletableFuture.completedFuture(
                new GenerationProjectionAuthoritySnapshot(
                        subject,
                        true,
                        Optional.of(identity),
                        List.of(
                                new GcAuthorityToken(
                                        "/projection/binding",
                                        11,
                                        sha('1')),
                                new GcAuthorityToken(
                                        "/projection/topic",
                                        12,
                                        sha('2')))));
    }

    private static ObjectStore headOnlyStore(
            HeadObjectResult expected) {
        return new ObjectStore() {
            @Override
            public CompletableFuture<PutObjectResult> putObject(
                    ObjectKey key,
                    ReplayableObjectUpload source,
                    PutObjectOptions options) {
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<RangeReadResult> readRange(
                    ObjectKey key,
                    long offset,
                    long length,
                    RangeReadOptions options) {
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<HeadObjectResult> headObject(
                    ObjectKey key,
                    HeadObjectOptions options) {
                if (!key.equals(expected.key())) {
                    return CompletableFuture.failedFuture(
                            new IllegalArgumentException(
                                    "unexpected object key"));
                }
                return CompletableFuture.completedFuture(expected);
            }

            @Override
            public CompletableFuture<DeleteObjectResult> deleteObject(
                    ObjectKey key,
                    DeleteObjectOptions options) {
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException());
            }

            @Override
            public void close() {
            }
        };
    }

    private static StreamFixture streamFixture() {
        String managedLedger =
                "tenant/ns/persistent/backfill";
        StreamId streamId =
                ManagedLedgerProjectionNames.streamId(
                        managedLedger, 1);
        ManagedLedgerProjectionIdentity projectionIdentity =
                new ManagedLedgerProjectionIdentity(
                        7,
                        1,
                        streamId.value(),
                        ManagedLedgerProjectionNames
                                        .MIN_VIRTUAL_LEDGER_ID
                                + 1);
        ProjectionRef projection =
                new ProjectionRef(
                        ProjectionType.VIRTUAL_LEDGER,
                        "backfill-projection");
        ObjectId objectId = new ObjectId("object-wal-backfill");
        ObjectKey objectKey =
                new ObjectKey("objects/backfill/wal-1");
        Checksum sliceChecksum =
                new Checksum(ChecksumType.CRC32C, "33333333");
        Checksum entryChecksum =
                new Checksum(ChecksumType.CRC32C, "44444444");
        Checksum storageChecksum =
                new Checksum(ChecksumType.CRC32C, "55555555");
        EntryIndexRef entryIndex = new EntryIndexRef(
                EntryIndexLocation.INLINE,
                Optional.empty(),
                Optional.empty(),
                Optional.of(new byte[] {1}),
                0,
                0,
                entryChecksum);
        ObjectSliceReadTarget target =
                new ObjectSliceReadTarget(
                        1,
                        objectId,
                        objectKey,
                        ObjectType.MULTI_STREAM_WAL_OBJECT,
                        "NWA1",
                        "PULSAR_ENTRY_V1",
                        "slice-1",
                        0,
                        8,
                        sliceChecksum,
                        entryIndex);
        CommitAppendRequest request =
                new CommitAppendRequest(
                        streamId,
                        "writer",
                        "writer-run",
                        1,
                        "fencing-token",
                        0,
                        target,
                        PayloadFormat.PULSAR_ENTRY_BATCH,
                        1,
                        1,
                        8,
                        List.of(),
                        100,
                        100,
                        Optional.of(projection));
        String commitId = request.commitId();
        StreamCommitTargetRecord commit =
                new StreamCommitTargetRecord(
                        streamId.value(),
                        commitId,
                        "",
                        0,
                        1,
                        0,
                        8,
                        1,
                        "writer",
                        "writer-run",
                        1,
                        request.fencingTokenHash(),
                        ReadTargetCodecRegistry.phase15()
                                .encode(target),
                        PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                        1,
                        1,
                        8,
                        List.of(),
                        request.projectionIdentity(),
                        100,
                        100,
                        150,
                        0);
        byte[] commitBytes =
                MetadataRecordCodecFactory.encodeEnvelope(
                        commit,
                        StreamCommitTargetRecord.class);
        Checksum commitSha = sha256(commitBytes);
        String commitKey =
                new OxiaKeyspace(CLUSTER)
                        .streamCommitKey(streamId, commitId);
        AppendRecoveryCommit recoveryCommit =
                new AppendRecoveryCommit(
                        commitKey,
                        AppendRecoveryCommitEncoding
                                .GENERIC_STREAM_COMMIT_TARGET_V1,
                        commit,
                        9,
                        commitSha,
                        ByteBuffer.wrap(commitBytes),
                        commitSha);
        AppendRecoveryHead recoveryHead =
                new AppendRecoveryHead(
                        streamId,
                        commitId,
                        1,
                        8,
                        1,
                        5);
        AppendRecoveryTailPage tail =
                new AppendRecoveryTailPage(
                        AppendRecoveryAnchor.genesis(streamId),
                        recoveryHead,
                        List.of(recoveryCommit),
                        true,
                        Optional.empty());
        StreamMetadataSnapshot snapshot =
                new StreamMetadataSnapshot(
                        new StreamMetadataRecord(
                                streamId.value(),
                                ManagedLedgerProjectionNames
                                        .streamName(
                                                managedLedger,
                                                1)
                                        .value(),
                                "stream-name-hash",
                                StreamState.ACTIVE.name(),
                                StorageProfile
                                        .OBJECT_WAL_SYNC_OBJECT
                                        .name(),
                                Map.of(
                                        ManagedLedgerProjectionNames
                                                .PAYLOAD_MAPPING_ATTRIBUTE,
                                        ManagedLedgerProjectionNames
                                                .PAYLOAD_MAPPING_V1),
                                100,
                                1,
                                5),
                        new CommittedEndOffsetRecord(
                                streamId.value(),
                                1,
                                8,
                                1,
                                5),
                        new TrimRecord(
                                streamId.value(),
                                0,
                                "",
                                100,
                                5));
        MaterializationStreamRegistrationRecord registration =
                new MaterializationStreamRegistrationRecord(
                        1,
                        streamId.value(),
                        request.projectionIdentity(),
                        sha('a').value(),
                        StorageProfile
                                .OBJECT_WAL_SYNC_OBJECT
                                .name(),
                        200,
                        1,
                        200,
                        0);
        OffsetIndexEntry indexValue =
                new OffsetIndexEntry(
                        streamId,
                        new OffsetRange(0, 1),
                        0,
                        8,
                        target,
                        PayloadFormat.PULSAR_ENTRY_BATCH,
                        1,
                        1,
                        8,
                        List.of(),
                        Optional.of(projection),
                        1,
                        false,
                        10);
        Checksum indexSha = sha('6');
        VersionedGenerationZeroIndex zeroIndex =
                new VersionedGenerationZeroIndex(
                        new OxiaKeyspace(CLUSTER)
                                .offsetIndexKey(streamId, 1, 0),
                        GenerationZeroIndexEncoding
                                .GENERIC_OFFSET_INDEX_TARGET_RECORD,
                        indexValue,
                        10,
                        indexSha);
        StreamSliceManifestRecord slice =
                new StreamSliceManifestRecord(
                        0,
                        streamId.value(),
                        "slice-1",
                        1,
                        0,
                        8,
                        1,
                        1,
                        8,
                        List.of(),
                        EntryIndexReferenceRecord.fromApi(
                                entryIndex),
                        sliceChecksum.type().name(),
                        sliceChecksum.value(),
                        PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                        "VISIBLE");
        ObjectManifestRecord manifest =
                new ObjectManifestRecord(
                        objectId.value(),
                        objectKey.value(),
                        ObjectType.MULTI_STREAM_WAL_OBJECT.name(),
                        "VISIBLE",
                        1,
                        0,
                        "test",
                        "writer",
                        "writer-run",
                        1,
                        100,
                        110,
                        8,
                        ChecksumType.SHA256.name(),
                        "7".repeat(64),
                        storageChecksum.type().name(),
                        storageChecksum.value(),
                        List.of(slice),
                        10_000,
                        4);
        VersionedGenerationZeroCommit source =
                new VersionedGenerationZeroCommit(
                        commitKey,
                        streamId,
                        commitId,
                        AppendRecoveryCommitEncoding
                                .GENERIC_STREAM_COMMIT_TARGET_V1,
                        new GenericCommittedAppendIdentity(
                                commitId),
                        commit,
                        0,
                        1,
                        1,
                        commitSha,
                        9,
                        commitSha);
        HeadObjectResult head =
                new HeadObjectResult(
                        objectKey,
                        8,
                        storageChecksum,
                        Optional.of("etag-1"),
                        Map.of());
        return new StreamFixture(
                streamId,
                projectionIdentity,
                registration,
                snapshot,
                tail,
                zeroIndex,
                manifest,
                source,
                objectKey,
                head);
    }

    private static CursorFixture cursorFixture() {
        String managedLedger =
                "tenant/ns/persistent/backfill-cursor";
        StreamId streamId =
                ManagedLedgerProjectionNames.streamId(
                        managedLedger, 1);
        ManagedLedgerProjectionIdentity projectionIdentity =
                new ManagedLedgerProjectionIdentity(
                        7,
                        1,
                        streamId.value(),
                        ManagedLedgerProjectionNames
                                        .MIN_VIRTUAL_LEDGER_ID
                                + 3);
        ProjectionRef projection =
                new ProjectionRef(
                        ProjectionType.VIRTUAL_LEDGER,
                        "backfill-cursor-projection");
        MaterializationStreamRegistrationRecord registration =
                new MaterializationStreamRegistrationRecord(
                        1,
                        streamId.value(),
                        encodeProjectionRef(projection),
                        sha('a').value(),
                        StorageProfile
                                .OBJECT_WAL_SYNC_OBJECT
                                .name(),
                        200,
                        0,
                        200,
                        0);
        StreamMetadataSnapshot snapshot =
                new StreamMetadataSnapshot(
                        new StreamMetadataRecord(
                                streamId.value(),
                                ManagedLedgerProjectionNames
                                        .streamName(
                                                managedLedger,
                                                1)
                                        .value(),
                                "cursor-stream-name-hash",
                                StreamState.ACTIVE.name(),
                                StorageProfile
                                        .OBJECT_WAL_SYNC_OBJECT
                                        .name(),
                                Map.of(
                                        ManagedLedgerProjectionNames
                                                .PAYLOAD_MAPPING_ATTRIBUTE,
                                        ManagedLedgerProjectionNames
                                                .PAYLOAD_MAPPING_V1),
                                100,
                                1,
                                5),
                        new CommittedEndOffsetRecord(
                                streamId.value(),
                                0,
                                0,
                                0,
                                5),
                        new TrimRecord(
                                streamId.value(),
                                0,
                                "",
                                100,
                                5));
        AppendRecoveryHead recoveryHead =
                new AppendRecoveryHead(
                        streamId, "", 0, 0, 0, 5);
        AppendRecoveryTailPage tail =
                new AppendRecoveryTailPage(
                        AppendRecoveryAnchor.genesis(streamId),
                        recoveryHead,
                        List.of(),
                        true,
                        Optional.empty());
        String owner = "0".repeat(32);
        String attempt = "1".repeat(32);
        String snapshotId = "2".repeat(32);
        String cursorName = "subscription-a";
        long generation = 1;
        ObjectKey objectKey = new ObjectKey(
                KeyComponentCodec.encodeComponent(CLUSTER)
                        + "/cursor-snapshots/v1/"
                        + KeyComponentCodec.encodeComponent(
                                streamId.value())
                        + "/"
                        + CursorNames.cursorNameHash(cursorName)
                        + "/"
                        + KeyComponentCodec.encodeNonNegativeLong(
                                generation)
                        + "/"
                        + snapshotId
                        + ".ncs");
        CursorSnapshotReferenceRecord reference =
                new CursorSnapshotReferenceRecord(
                        objectKey.value(),
                        snapshotId,
                        generation,
                        1,
                        0,
                        8,
                        ChecksumType.CRC32C.name(),
                        "66666666",
                        123,
                        1,
                        150);
        CursorRetentionRecord retention =
                new CursorRetentionRecord(
                        0,
                        projectionIdentity,
                        owner,
                        CursorRetentionLifecycle.ACTIVE,
                        1,
                        0,
                        0,
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty(),
                        Optional.empty(),
                        160);
        CursorStateRecord cursor =
                new CursorStateRecord(
                        0,
                        projectionIdentity,
                        owner,
                        cursorName,
                        CursorNames.cursorNameHash(
                                cursorName),
                        generation,
                        CursorRecordLifecycle.ACTIVE,
                        1,
                        1,
                        attempt,
                        0,
                        Optional.of(reference),
                        List.of(),
                        List.of(),
                        Map.of(),
                        Map.of(),
                        140,
                        160,
                        OptionalLong.empty());
        HeadObjectResult head =
                new HeadObjectResult(
                        objectKey,
                        8,
                        new Checksum(
                                ChecksumType.CRC32C,
                                "66666666"),
                        Optional.of("cursor-etag"),
                        Map.of(
                                "nereus-format",
                                "NCS1",
                                "nereus-object-type",
                                "CURSOR_SNAPSHOT_OBJECT",
                                "nereus-snapshot-id",
                                snapshotId));
        return new CursorFixture(
                projectionIdentity,
                registration,
                snapshot,
                tail,
                retention,
                cursor,
                objectKey,
                head);
    }

    private record StreamFixture(
            StreamId streamId,
            ManagedLedgerProjectionIdentity projectionIdentity,
            MaterializationStreamRegistrationRecord registration,
            StreamMetadataSnapshot streamSnapshot,
            AppendRecoveryTailPage tail,
            VersionedGenerationZeroIndex zeroIndex,
            ObjectManifestRecord manifest,
            VersionedGenerationZeroCommit commitSource,
            ObjectKey objectKey,
            HeadObjectResult head) {
    }

    private record CursorFixture(
            ManagedLedgerProjectionIdentity projectionIdentity,
            MaterializationStreamRegistrationRecord registration,
            StreamMetadataSnapshot streamSnapshot,
            AppendRecoveryTailPage tail,
            CursorRetentionRecord retention,
            CursorStateRecord cursor,
            ObjectKey objectKey,
            HeadObjectResult head) {
    }

    private static String encodeProjectionRef(
            ProjectionRef projection) {
        StringBuilder value = new StringBuilder();
        addProjectionPart(value, "projectionRef");
        addProjectionPart(value, "present");
        addProjectionPart(value, projection.type().name());
        addProjectionPart(value, projection.value());
        return value.toString();
    }

    private static void addProjectionPart(
            StringBuilder builder, String value) {
        builder.append(
                        value.getBytes(StandardCharsets.UTF_8)
                                .length)
                .append(':')
                .append(value);
    }

    private static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256,
                Character.toString(value).repeat(64));
    }

    private static Checksum sha256(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value)));
        } catch (NoSuchAlgorithmException failure) {
            throw new AssertionError(failure);
        }
    }

    private static Object invoke(
            Object delegate,
            java.lang.reflect.Method method,
            Object[] arguments) throws Throwable {
        try {
            return method.invoke(delegate, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T unsupported(Class<T> type) {
        return proxy(
                type,
                (method, arguments) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    throw new UnsupportedOperationException(
                            method.getName());
                });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(
            Class<T> type, Invocation invocation) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (instance, method, arguments) -> {
                    if (method.getName().equals("toString")) {
                        return "physical-root-backfill-test-"
                                + type.getSimpleName();
                    }
                    return invocation.invoke(
                            method,
                            arguments == null
                                    ? new Object[0]
                                    : arguments);
                });
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(
                java.lang.reflect.Method method,
                Object[] arguments) throws Throwable;
    }
}
