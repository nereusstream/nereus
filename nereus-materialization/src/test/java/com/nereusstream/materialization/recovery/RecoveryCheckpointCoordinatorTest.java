/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProjectionAuthoritySnapshot;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.StreamRetirementReferenceAuthoritySnapshot;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.core.recovery.AnchorAwareCommitWalker;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.materialization.gc.StreamRegistrationRetirementCoordinator;
import com.nereusstream.materialization.gc.StreamRegistrationRetirementStatus;
import com.nereusstream.metadata.oxia.AllocatedGeneration;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.RecoveryCheckpointRootDigests;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.checkpoint.DefaultRecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteRequest;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteResult;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointCoordinatorTest {
    private static final String BASE32 = "abcdefghijklmnopqrstuvwxyz234567";
    private static final String CLUSTER = "checkpoint-coordinator-cluster";
    private static final StreamId STREAM = new StreamId("checkpoint-coordinator-stream");
    private static final ProjectionRef PROJECTION = new ProjectionRef(
            ProjectionType.VIRTUAL_LEDGER, "persistent://tenant/ns/checkpoint-topic");
    private static final String PROJECTION_IDENTITY =
            projectionIdentity(PROJECTION);
    private static final Checksum PROJECTION_SHA = sha("projection");
    private static final Checksum REFERENCE_DOMAINS_SHA = sha("reference-domains");
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(Duration.ofDays(1).toMillis()), ZoneOffset.UTC);
    private static final Duration PENDING_DURATION = Duration.ofMinutes(5);

    @TempDir
    Path temporaryDirectory;

    @Test
    void publishesAndConvergesLostRecoveryRootCasResponse() throws Exception {
        try (Fixture fixture = new Fixture(temporaryDirectory.resolve("lost-cas"))) {
            RecoveryCheckpointCoordinator coordinator = fixture.coordinator(
                    loseFirstRootCasResponse(fixture.generations), fixture.physical);

            RecoveryCheckpointRunResult result = coordinator.checkpoint(STREAM).join();

            assertThat(result.status()).isEqualTo(RecoveryCheckpointBuildStatus.READY);
            VersionedRecoveryCheckpointRoot root = result.publication().orElseThrow().root();
            assertThat(root.value().checkpointSequence()).isEqualTo(1);
            assertThat(root.value().coveredEndOffset()).isEqualTo(2);
            fixture.assertPermanentProtections(root);
        }
    }

    @Test
    void mergesThirtyTwoReferencesWithPinnedSourcesAndConvergesLostRootCasResponse()
            throws Exception {
        try (Fixture fixture = new Fixture(
                temporaryDirectory.resolve("merge-lost-cas"))) {
            VersionedRecoveryCheckpointRoot seeded =
                    fixture.seedThirtyTwoCheckpointReferences();
            AtomicBoolean observedPinnedSources = new AtomicBoolean();
            GenerationMetadataStore generationStore =
                    observePinnedMergeAndLoseRootCasResponse(
                            fixture.generations,
                            fixture.physical,
                            seeded.value().checkpoints(),
                            observedPinnedSources);
            RecoveryCheckpointCoordinator coordinator = fixture.coordinator(
                    generationStore, fixture.physical);

            RecoveryCheckpointRunResult result = coordinator.checkpoint(STREAM).join();

            assertThat(result.status()).isEqualTo(
                    RecoveryCheckpointBuildStatus.READY);
            VersionedRecoveryCheckpointRoot merged =
                    result.publication().orElseThrow().root();
            assertThat(observedPinnedSources).isTrue();
            assertThat(merged.value().checkpointSequence()).isEqualTo(33);
            assertThat(merged.value().coveredStartOffset()).isZero();
            assertThat(merged.value().coveredEndOffset()).isEqualTo(32);
            assertThat(merged.value().firstCommitVersion()).isEqualTo(1);
            assertThat(merged.value().lastCommitVersion()).isEqualTo(32);
            assertThat(merged.value().checkpoints()).hasSize(1);
            assertThat(merged.value().checkpoints().get(0).commitEntryCount())
                    .isEqualTo(32);
            assertThat(merged.value().checkpoints().get(0).publicationCount())
                    .isEqualTo(32);
            assertThat(merged.value().checkpoints().get(0).objectKey())
                    .isNotIn(seeded.value().checkpoints().stream()
                            .map(RecoveryCheckpointReferenceRecord::objectKey)
                            .toList());
            fixture.assertMergedPermanentProtections(merged);
            fixture.assertNoReaderLeases(seeded.value().checkpoints());
            assertThat(fixture.stagingFiles.reservedBytes()).isZero();
        }
    }

    @Test
    void selectionDriftDuringMergePinLeavesOldRootAuthoritativeAndReleasesLease()
            throws Exception {
        try (Fixture fixture = new Fixture(
                temporaryDirectory.resolve("merge-selection-drift"))) {
            VersionedRecoveryCheckpointRoot seeded =
                    fixture.seedThirtyTwoCheckpointReferences();
            ObjectReadPinManager drifting = failFirstPinSelectionRevalidation(
                    fixture.newReadPins(fixture.physical));
            RecoveryCheckpointCoordinator coordinator = fixture.coordinator(
                    fixture.generations, fixture.physical, drifting);

            assertThatThrownBy(() -> coordinator.checkpoint(STREAM).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseInstanceOf(
                            F4MetadataConditionFailedException.class);

            assertThat(fixture.generations.getRecoveryRoot(CLUSTER, STREAM).join())
                    .contains(seeded);
            fixture.assertNoReaderLeases(seeded.value().checkpoints());
            assertThat(fixture.stagingFiles.reservedBytes()).isZero();
        }
    }

    @Test
    void cancellationWhileMergePinReturnsReleasesLateLeaseWithoutStaging()
            throws Exception {
        try (Fixture fixture = new Fixture(
                temporaryDirectory.resolve("merge-cancel"))) {
            VersionedRecoveryCheckpointRoot seeded =
                    fixture.seedThirtyTwoCheckpointReferences();
            VersionedMaterializationStreamRegistration registration =
                    fixture.generations.getStreamRegistration(CLUSTER, STREAM)
                            .join()
                            .orElseThrow();
            DelayedReadPins readPins = new DelayedReadPins();
            RecoveryCheckpointMerger merger = new RecoveryCheckpointMerger(
                    CLUSTER,
                    fixture.generations,
                    fixture.objectStore,
                    fixture.codec,
                    readPins,
                    CLOCK);
            try (MaterializationDeadline deadline =
                    new MaterializationDeadline(
                            fixture.config.operationTimeout(),
                            fixture.scheduler)) {
                CompletableFuture<RecoveryCheckpointMerger.PreparedMerge> prepared =
                        merger.prepare(
                                seeded,
                                registration,
                                "z".repeat(26),
                                deadline);
                readPins.acquisitionStarted.join();

                assertThat(prepared.cancel(true)).isTrue();
                readPins.completeAcquisition();
                readPins.released.join();

                assertThat(prepared).isCancelled();
                assertThat(readPins.lease.isReleased()).isTrue();
                assertThat(fixture.stagingFiles.reservedBytes()).isZero();
            }
        }
    }

    @Test
    void restartRepairsPermanentProtectionsAfterRootCas() throws Exception {
        try (Fixture fixture = new Fixture(temporaryDirectory.resolve("restart"))) {
            PhysicalObjectMetadataStore failingPhysical =
                    failFirstCheckpointObjectProtection(fixture.physical);
            RecoveryCheckpointCoordinator first = fixture.coordinator(
                    fixture.generations, failingPhysical);

            assertThatThrownBy(() -> first.checkpoint(STREAM).join())
                    .isInstanceOf(CompletionException.class);

            VersionedRecoveryCheckpointRoot published = fixture.generations
                    .getRecoveryRoot(CLUSTER, STREAM)
                    .join()
                    .orElseThrow();
            assertThat(published.value().checkpointSequence()).isEqualTo(1);
            fixture.assertPendingWithoutPermanent(published);

            RecoveryCheckpointCoordinator restarted = fixture.coordinator(
                    fixture.generations, fixture.physical);
            RecoveryCheckpointRunResult resumed = restarted.checkpoint(STREAM).join();

            assertThat(resumed.status()).isEqualTo(
                    RecoveryCheckpointBuildStatus.NO_LIVE_TAIL);
            fixture.assertPermanentProtections(published);
        }
    }

    @Test
    void registrationRetirementDrainsNonEmptyCheckpointRootAcrossDeleteResponseLoss()
            throws Exception {
        try (Fixture fixture = new Fixture(temporaryDirectory.resolve("registration-retirement"))) {
            VersionedRecoveryCheckpointRoot root = fixture.coordinator(
                            fixture.generations, fixture.physical)
                    .checkpoint(STREAM)
                    .join()
                    .publication()
                    .orElseThrow()
                    .root();
            fixture.assertPermanentProtections(root);
            retireIndex(fixture.generations, fixture.targetIndex);

            AtomicBoolean loseIndexDelete = new AtomicBoolean(true);
            AtomicBoolean loseRootDelete = new AtomicBoolean(true);
            AtomicBoolean loseRegistrationDelete = new AtomicBoolean(true);
            AtomicBoolean loseProtectionDelete = new AtomicBoolean(true);
            GenerationMetadataStore generations = loseRetirementDeleteResponses(
                    fixture.generations,
                    loseIndexDelete,
                    loseRootDelete,
                    loseRegistrationDelete);
            PhysicalObjectMetadataStore physical = loseRetirementProtectionDeleteResponse(
                    fixture.physical, loseProtectionDelete);
            StreamRegistrationRetirementCoordinator coordinator =
                    new StreamRegistrationRetirementCoordinator(
                            CLUSTER,
                            retirementL0(fixture.l0Store),
                            generations,
                            physical,
                            subject -> CompletableFuture.completedFuture(
                                    new GenerationProjectionAuthoritySnapshot(
                                            subject,
                                            false,
                                            Optional.empty(),
                                            List.of(new GcAuthorityToken(
                                                    "/projection/checkpoint-retirement",
                                                    1,
                                                    sha("checkpoint-retirement-projection"))))),
                            subject -> CompletableFuture.completedFuture(
                                    StreamRetirementReferenceAuthoritySnapshot.complete(
                                            subject, 0, List.of())),
                            fixture.codec,
                            physicalGcConfig(),
                            Duration.ofMillis(500),
                            CLOCK,
                            fixture.scheduler);

            var result = coordinator.retire(STREAM).join();

            assertThat(result.status())
                    .isEqualTo(StreamRegistrationRetirementStatus.RETIRED);
            assertThat(result.protectionsRetired()).isEqualTo(2);
            assertThat(result.indexesRetired()).isOne();
            assertThat(result.recoveryRootRetired()).isTrue();
            assertThat(result.sequencesRetired()).isOne();
            assertThat(result.registrationRetired()).isTrue();
            assertThat(loseIndexDelete).isFalse();
            assertThat(loseRootDelete).isFalse();
            assertThat(loseRegistrationDelete).isFalse();
            assertThat(loseProtectionDelete).isFalse();
            assertThat(fixture.generations.getRecoveryRoot(CLUSTER, STREAM).join())
                    .isEmpty();
            assertThat(fixture.generations.getStreamRegistration(CLUSTER, STREAM).join())
                    .isEmpty();

            var checkpointReference = root.value().checkpoints().get(0);
            assertThat(fixture.physical.getRoot(
                            CLUSTER,
                            new com.nereusstream.api.ObjectKeyHash(
                                    checkpointReference.objectKeyHash()))
                    .join())
                    .isPresent();
            assertThat(fixture.physical.getRoot(
                            CLUSTER,
                            com.nereusstream.api.ObjectKeyHash.from(fixture.target.objectKey()))
                    .join())
                    .isPresent();
        }
    }

    private static final class DelayedReadPins
            implements ObjectReadPinManager {
        private final CompletableFuture<Void> acquisitionStarted =
                new CompletableFuture<>();
        private final CompletableFuture<Void> released =
                new CompletableFuture<>();
        private final CompletableFuture<ObjectReadLease> acquisition =
                new CompletableFuture<>();
        private PhysicalObjectIdentity object;
        private long maximumReadDeadlineMillis;
        private LateLease lease;

        @Override
        public synchronized CompletableFuture<ObjectReadLease> acquire(
                PhysicalObjectIdentity object,
                long maximumReadDeadlineMillis,
                SelectionRevalidator selectionRevalidator) {
            if (this.object != null) {
                return CompletableFuture.failedFuture(
                        new AssertionError("unexpected second delayed pin acquisition"));
            }
            this.object = object;
            this.maximumReadDeadlineMillis = maximumReadDeadlineMillis;
            acquisitionStarted.complete(null);
            return acquisition;
        }

        private synchronized void completeAcquisition() {
            lease = new LateLease(
                    object, maximumReadDeadlineMillis, released);
            acquisition.complete(lease);
        }

        @Override
        public void close() {
        }
    }

    private static final class LateLease implements ObjectReadLease {
        private final PhysicalObjectIdentity object;
        private final long maximumReadDeadlineMillis;
        private final CompletableFuture<Void> released;
        private final AtomicBoolean releaseStarted = new AtomicBoolean();

        private LateLease(
                PhysicalObjectIdentity object,
                long maximumReadDeadlineMillis,
                CompletableFuture<Void> released) {
            this.object = object;
            this.maximumReadDeadlineMillis = maximumReadDeadlineMillis;
            this.released = released;
        }

        @Override
        public PhysicalObjectIdentity object() {
            return object;
        }

        @Override
        public String leaseId() {
            return "l".repeat(26);
        }

        @Override
        public long maximumReadDeadlineMillis() {
            return maximumReadDeadlineMillis;
        }

        @Override
        public CompletableFuture<Void> release() {
            if (releaseStarted.compareAndSet(false, true)) {
                released.complete(null);
            }
            return released;
        }

        @Override
        public boolean isReleased() {
            return releaseStarted.get();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final GenerationMetadataStore generations =
                GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        private final FakePhysicalObjectMetadataStore physical =
                new FakePhysicalObjectMetadataStore();
        private final ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(2);
        private final Path stagingDirectory;
        private final MaterializationConfig config;
        private final ObjectStore objectStore;
        private final StagingFileManager stagingFiles;
        private final RecoveryCheckpointCodecV1 codec;
        private final OxiaMetadataStore l0Store;
        private final VersionedGenerationIndex targetIndex;
        private final ObjectSliceReadTarget target;
        private final java.util.ArrayList<ObjectProtectionManager> protectionManagers =
                new java.util.ArrayList<>();
        private final java.util.ArrayList<ObjectReadPinManager> readPinManagers =
                new java.util.ArrayList<>();

        private Fixture(Path directory) throws Exception {
            Files.createDirectories(directory);
            stagingDirectory = Files.createDirectory(directory.resolve("staging"));
            Files.setPosixFilePermissions(
                    stagingDirectory,
                    PosixFilePermissions.fromString("rwx------"));
            config = MaterializationConfig.defaults(stagingDirectory);
            objectStore = new LocalFileObjectStore(directory.resolve("objects"));
            stagingFiles = new StagingFileManager(
                    stagingDirectory,
                    config.maxStagingBytes(),
                    config.uploadChunkBytes(),
                    Duration.ofHours(1),
                    Runnable::run);
            codec = new DefaultRecoveryCheckpointCodecV1(
                    objectStore,
                    stagingFiles,
                    Runnable::run,
                    new MetadataRecoveryCheckpointVerifier());
            target = target();
            createTargetRoot();
            targetIndex = createTargetIndex();
            createRegistration();
            AppendRecoveryHead head = new AppendRecoveryHead(
                    STREAM, "commit-2", 2, 20, 2, 8);
            List<AppendRecoveryCommit> commits = List.of(
                    commit("commit-2", "commit-1", 1, 2, 20, 2),
                    commit("commit-1", "", 0, 1, 10, 1));
            StreamMetadataSnapshot snapshot = snapshot();
            l0Store = l0Store(head, commits, snapshot);
        }

        private RecoveryCheckpointCoordinator coordinator(
                GenerationMetadataStore generationStore,
                PhysicalObjectMetadataStore physicalStore) {
            return coordinator(
                    generationStore,
                    physicalStore,
                    newReadPins(physicalStore));
        }

        private RecoveryCheckpointCoordinator coordinator(
                GenerationMetadataStore generationStore,
                PhysicalObjectMetadataStore physicalStore,
                ObjectReadPinManager readPins) {
            ObjectProtectionManager objectProtections = new DefaultObjectProtectionManager(
                    CLUSTER,
                    physicalStore,
                    PENDING_DURATION,
                    config.maximumClockSkew(),
                    Duration.ofMinutes(10),
                    CLOCK);
            protectionManagers.add(objectProtections);
            RecoveryCheckpointProtectionManager checkpointProtections =
                    new RecoveryCheckpointProtectionManager(
                            CLUSTER,
                            generationStore,
                            physicalStore,
                            objectProtections);
            RecoveryCheckpointBuilder builder = new RecoveryCheckpointBuilder(
                    CLUSTER,
                    l0Store,
                    generationStore,
                    new AnchorAwareCommitWalker(CLUSTER, l0Store, generationStore),
                    config,
                    CLOCK);
            return new RecoveryCheckpointCoordinator(
                    CLUSTER,
                    generationStore,
                    physicalStore,
                    objectStore,
                    codec,
                    builder,
                    checkpointProtections,
                    readPins,
                    activationGuard(),
                    () -> "a".repeat(26),
                    config,
                    PENDING_DURATION,
                    scheduler,
                    CLOCK);
        }

        private ObjectReadPinManager newReadPins(
                PhysicalObjectMetadataStore physicalStore) {
            ObjectReadPinManager readPins = new DefaultObjectReadPinManager(
                    CLUSTER,
                    "m".repeat(26),
                    physicalStore,
                    Duration.ofMinutes(2),
                    config.maximumClockSkew(),
                    Duration.ofMinutes(10),
                    CLOCK);
            readPinManagers.add(readPins);
            return readPins;
        }

        private void createTargetRoot() {
            PhysicalObjectIdentity identity = PhysicalObjectIdentity.create(
                    target.objectKey(),
                    Optional.of(target.objectId()),
                    PhysicalObjectKind.COMMITTED_COMPACTED,
                    target.objectLength(),
                    target.sliceChecksum(),
                    Optional.of(sha("target-content")),
                    Optional.of("target-etag"));
            physical.createRoot(CLUSTER, activeRoot(identity)).join();
        }

        private VersionedGenerationIndex createTargetIndex() {
            PublicationId publication = new PublicationId("p".repeat(26));
            AllocatedGeneration allocated = generations.allocateGeneration(
                    CLUSTER, STREAM, ReadView.COMMITTED, publication).join();
            GenerationIndexRecord prepared = generation(
                    allocated.generation().value(),
                    publication,
                    GenerationLifecycle.PREPARED,
                    0);
            VersionedGenerationIndex created = generations.createPrepared(
                    CLUSTER, prepared).join();
            return generations.compareAndSetIndex(
                    CLUSTER,
                    generation(
                            allocated.generation().value(),
                            publication,
                            GenerationLifecycle.COMMITTED,
                            2),
                    created.metadataVersion()).join();
        }

        private void createRegistration() {
            generations.createOrVerifyStreamRegistration(
                    CLUSTER,
                    new MaterializationStreamRegistrationRecord(
                            1,
                            STREAM.value(),
                            PROJECTION_IDENTITY,
                            PROJECTION_SHA.value(),
                            StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                            1,
                            2,
                            2,
                            0)).join();
        }

        private VersionedRecoveryCheckpointRoot
                seedThirtyTwoCheckpointReferences() {
            VersionedRecoveryCheckpointRoot current = generations
                    .getOrCreateRecoveryRoot(CLUSTER, STREAM)
                    .join();
            List<RecoveryCheckpointReferenceRecord> references =
                    new java.util.ArrayList<>();
            GenerationIndexRecordCodecV1 generationCodec =
                    new GenerationIndexRecordCodecV1();
            for (int index = 0;
                    index < RecoveryCheckpointMerger.MERGE_REFERENCE_COUNT;
                    index++) {
                long version = index + 1L;
                long start = index;
                long end = start + 1;
                long cumulativeStart = index * 10L;
                long cumulativeEnd = cumulativeStart + 10;
                String commitId = "merge-commit-" + version;
                String previousCommitId = index == 0
                        ? ""
                        : "merge-commit-" + index;
                VersionedGenerationIndex generation =
                        createMergeTargetIndex(
                                index,
                                start,
                                end,
                                cumulativeStart,
                                cumulativeEnd,
                                version);
                GenerationIndexRecord canonicalGeneration =
                        generation.value().withMetadataVersion(0);
                byte[] generationBytes = generationCodec.encode(
                        canonicalGeneration);
                RecoveryCheckpointPublication publication =
                        new RecoveryCheckpointPublication(
                                canonicalGeneration.generation(),
                                new PublicationId(
                                        canonicalGeneration.publicationId()),
                                new OffsetRange(start, end),
                                ByteBuffer.wrap(generationBytes),
                                sha(generationBytes));
                AppendRecoveryCommit evidence = commit(
                        commitId,
                        previousCommitId,
                        start,
                        end,
                        cumulativeEnd,
                        version);
                RecoveryCheckpointEntry entry =
                        new RecoveryCheckpointEntry(
                                version,
                                new OffsetRange(start, end),
                                cumulativeEnd,
                                commitId,
                                previousCommitId,
                                evidence.canonicalCommitRecord(),
                                evidence.canonicalCommitRecordSha256(),
                                List.of(0));
                RecoveryCheckpointWriteRequest request =
                        new RecoveryCheckpointWriteRequest(
                                CLUSTER,
                                STREAM,
                                version,
                                base32Id('a', index),
                                new OffsetRange(start, end),
                                version,
                                version,
                                cumulativeStart,
                                cumulativeEnd,
                                commitId,
                                commitId,
                                "merge-commit-32",
                                32,
                                PROJECTION_SHA,
                                1,
                                1);
                try (RecoveryCheckpointWriteResult written = codec.write(
                                request,
                                new RecoveryCheckpointListPublisher<>(
                                        List.of(publication)),
                                new RecoveryCheckpointListPublisher<>(
                                        List.of(entry)))
                        .join()) {
                    uploadCheckpoint(request, written);
                    references.add(reference(request, written));
                }
                RecoveryCheckpointRootRecord replacement = root(
                        version, references);
                current = generations.compareAndSetRecoveryRoot(
                                CLUSTER,
                                replacement,
                                current.metadataVersion())
                        .join();
            }
            return current;
        }

        private VersionedGenerationIndex createMergeTargetIndex(
                int index,
                long start,
                long end,
                long cumulativeStart,
                long cumulativeEnd,
                long commitVersion) {
            PublicationId publication = new PublicationId(
                    base32Id('p', index));
            AllocatedGeneration allocated = generations.allocateGeneration(
                    CLUSTER, STREAM, ReadView.COMMITTED, publication).join();
            GenerationIndexRecord prepared = mergeGeneration(
                    allocated.generation().value(),
                    publication,
                    GenerationLifecycle.PREPARED,
                    0,
                    start,
                    end,
                    cumulativeStart,
                    cumulativeEnd,
                    commitVersion);
            VersionedGenerationIndex created = generations.createPrepared(
                    CLUSTER, prepared).join();
            return generations.compareAndSetIndex(
                    CLUSTER,
                    mergeGeneration(
                            allocated.generation().value(),
                            publication,
                            GenerationLifecycle.COMMITTED,
                            commitVersion,
                            start,
                            end,
                            cumulativeStart,
                            cumulativeEnd,
                            commitVersion),
                    created.metadataVersion()).join();
        }

        private GenerationIndexRecord mergeGeneration(
                long generation,
                PublicationId publication,
                GenerationLifecycle lifecycle,
                long committedAt,
                long start,
                long end,
                long cumulativeStart,
                long cumulativeEnd,
                long commitVersion) {
            var encoded = ReadTargetCodecRegistry.phase15().encode(target);
            int records = Math.toIntExact(
                    Math.subtractExact(end, start));
            return new GenerationIndexRecord(
                    1,
                    STREAM.value(),
                    ReadView.COMMITTED.wireId(),
                    start,
                    end,
                    generation,
                    publication.value(),
                    "checkpoint-merge-task-" + commitVersion,
                    lifecycle,
                    sha("merge-source-" + commitVersion).value(),
                    sha("policy").value(),
                    encoded,
                    encoded.identityChecksumValue(),
                    sha("policy").value(),
                    PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                    records,
                    records,
                    records,
                    Math.subtractExact(cumulativeEnd, cumulativeStart),
                    cumulativeStart,
                    cumulativeEnd,
                    commitVersion,
                    commitVersion,
                    List.of(),
                    PROJECTION_IDENTITY,
                    1,
                    committedAt,
                    "",
                    Math.max(1, committedAt),
                    0);
        }

        private void uploadCheckpoint(
                RecoveryCheckpointWriteRequest request,
                RecoveryCheckpointWriteResult written) {
            objectStore.putObject(
                            written.objectKey(),
                            written.stagingFile(),
                            new PutObjectOptions(
                                    RecoveryCheckpointFormatV1.CONTENT_TYPE,
                                    written.storageCrc32c(),
                                    true,
                                    Map.of(
                                            "nereus-format", "NRC1",
                                            "nereus-content-sha256",
                                                    written.contentSha256().value(),
                                            "nereus-stream-id",
                                                    request.streamId().value(),
                                            "nereus-checkpoint-sequence",
                                                    Long.toString(
                                                            request.checkpointSequence()),
                                            "nereus-checkpoint-attempt-id",
                                                    request.checkpointAttemptId()),
                                    config.operationTimeout()))
                    .join();
        }

        private RecoveryCheckpointReferenceRecord reference(
                RecoveryCheckpointWriteRequest request,
                RecoveryCheckpointWriteResult written) {
            return new RecoveryCheckpointReferenceRecord(
                    request.checkpointSequence(),
                    request.checkpointAttemptId(),
                    request.coverage().startOffset(),
                    request.coverage().endOffset(),
                    request.firstCommitVersion(),
                    request.lastCommitVersion(),
                    request.cumulativeSizeAtStart(),
                    request.cumulativeSizeAtEnd(),
                    request.firstCommitId(),
                    request.lastCommitId(),
                    request.sourceHeadCommitId(),
                    request.sourceHeadCommitVersion(),
                    request.projectionIdentitySha256().value(),
                    written.objectId().value(),
                    written.objectKey().value(),
                    written.objectKeyHash().value(),
                    written.objectLength(),
                    written.storageCrc32c().value(),
                    written.contentSha256().value(),
                    request.expectedEntryCount(),
                    request.expectedPublicationCount());
        }

        private RecoveryCheckpointRootRecord root(
                long checkpointSequence,
                List<RecoveryCheckpointReferenceRecord> references) {
            List<RecoveryCheckpointReferenceRecord> exact =
                    List.copyOf(references);
            RecoveryCheckpointReferenceRecord first = exact.get(0);
            RecoveryCheckpointReferenceRecord last =
                    exact.get(exact.size() - 1);
            return new RecoveryCheckpointRootRecord(
                    1,
                    STREAM.value(),
                    checkpointSequence,
                    first.coveredStartOffset(),
                    last.coveredEndOffset(),
                    first.firstCommitVersion(),
                    last.lastCommitVersion(),
                    first.cumulativeSizeAtStart(),
                    last.cumulativeSizeAtEnd(),
                    first.firstCommitId(),
                    last.lastCommitId(),
                    exact,
                    RecoveryCheckpointRootDigests.checkpointSetSha256(
                                    exact)
                            .value(),
                    last.sourceHeadCommitId(),
                    last.sourceHeadCommitVersion(),
                    CLOCK.millis(),
                    0);
        }

        private GenerationIndexRecord generation(
                long generation,
                PublicationId publication,
                GenerationLifecycle lifecycle,
                long committedAt) {
            var encoded = ReadTargetCodecRegistry.phase15().encode(target);
            return new GenerationIndexRecord(
                    1,
                    STREAM.value(),
                    ReadView.COMMITTED.wireId(),
                    0,
                    2,
                    generation,
                    publication.value(),
                    "checkpoint-target-task",
                    lifecycle,
                    sha("source-set").value(),
                    sha("policy").value(),
                    encoded,
                    encoded.identityChecksumValue(),
                    sha("policy").value(),
                    PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                    2,
                    2,
                    2,
                    20,
                    0,
                    20,
                    1,
                    2,
                    List.of(),
                    PROJECTION_IDENTITY,
                    1,
                    committedAt,
                    "",
                    Math.max(1, committedAt),
                    0);
        }

        private void assertPendingWithoutPermanent(
                VersionedRecoveryCheckpointRoot root) {
            var reference = root.value().checkpoints().get(0);
            ObjectProtectionScanPage checkpoint = physical.scanProtections(
                    CLUSTER,
                    new com.nereusstream.api.ObjectKeyHash(reference.objectKeyHash()),
                    Optional.empty(),
                    100).join();
            assertThat(checkpoint.values())
                    .extracting(value -> ObjectProtectionType.fromWireId(
                            value.value().protectionTypeId()))
                    .contains(ObjectProtectionType.RECOVERY_CHECKPOINT_PENDING)
                    .doesNotContain(ObjectProtectionType.RECOVERY_CHECKPOINT_OBJECT);
        }

        private void assertPermanentProtections(
                VersionedRecoveryCheckpointRoot root) {
            var reference = root.value().checkpoints().get(0);
            ObjectProtectionScanPage checkpoint = physical.scanProtections(
                    CLUSTER,
                    new com.nereusstream.api.ObjectKeyHash(reference.objectKeyHash()),
                    Optional.empty(),
                    100).join();
            assertThat(checkpoint.values())
                    .extracting(value -> ObjectProtectionType.fromWireId(
                            value.value().protectionTypeId()))
                    .contains(ObjectProtectionType.RECOVERY_CHECKPOINT_OBJECT)
                    .doesNotContain(ObjectProtectionType.RECOVERY_CHECKPOINT_PENDING);
            assertThat(checkpoint.values())
                    .filteredOn(value -> ObjectProtectionType.fromWireId(
                                    value.value().protectionTypeId())
                            == ObjectProtectionType.RECOVERY_CHECKPOINT_OBJECT)
                    .allSatisfy(value -> {
                        assertThat(value.value().ownerKey()).isEqualTo(root.key());
                        assertThat(value.value().ownerMetadataVersion())
                                .isEqualTo(root.metadataVersion());
                    });

            ObjectProtectionScanPage targets = physical.scanProtections(
                    CLUSTER,
                    com.nereusstream.api.ObjectKeyHash.from(target.objectKey()),
                    Optional.empty(),
                    100).join();
            assertThat(targets.values())
                    .filteredOn(value -> ObjectProtectionType.fromWireId(
                                    value.value().protectionTypeId())
                            == ObjectProtectionType.RECOVERY_CHECKPOINT_TARGET)
                    .singleElement()
                    .satisfies(value -> {
                        assertThat(value.value().ownerKey()).isEqualTo(root.key());
                        assertThat(value.value().ownerMetadataVersion())
                                .isEqualTo(root.metadataVersion());
                    });
            assertThat(targetIndex.value().lifecycle())
                    .isEqualTo(GenerationLifecycle.COMMITTED);
        }

        private void assertMergedPermanentProtections(
                VersionedRecoveryCheckpointRoot root) {
            RecoveryCheckpointReferenceRecord reference =
                    root.value().checkpoints().get(0);
            ObjectProtectionScanPage checkpoint = physical.scanProtections(
                    CLUSTER,
                    new com.nereusstream.api.ObjectKeyHash(
                            reference.objectKeyHash()),
                    Optional.empty(),
                    100).join();
            assertThat(checkpoint.values())
                    .filteredOn(value -> ObjectProtectionType.fromWireId(
                                    value.value().protectionTypeId())
                            == ObjectProtectionType.RECOVERY_CHECKPOINT_OBJECT)
                    .singleElement()
                    .satisfies(value -> {
                        assertThat(value.value().ownerKey())
                                .isEqualTo(root.key());
                        assertThat(value.value().ownerMetadataVersion())
                                .isEqualTo(root.metadataVersion());
                    });
            assertThat(checkpoint.values())
                    .extracting(value -> ObjectProtectionType.fromWireId(
                            value.value().protectionTypeId()))
                    .doesNotContain(
                            ObjectProtectionType.RECOVERY_CHECKPOINT_PENDING);

            ObjectProtectionScanPage targets = physical.scanProtections(
                    CLUSTER,
                    com.nereusstream.api.ObjectKeyHash.from(target.objectKey()),
                    Optional.empty(),
                    1_000).join();
            assertThat(targets.values())
                    .filteredOn(value -> ObjectProtectionType.fromWireId(
                                    value.value().protectionTypeId())
                                    == ObjectProtectionType.RECOVERY_CHECKPOINT_TARGET
                            && value.value().ownerKey().equals(root.key())
                            && value.value().ownerMetadataVersion()
                                    == root.metadataVersion())
                    .hasSize(RecoveryCheckpointMerger.MERGE_REFERENCE_COUNT);
        }

        private void assertNoReaderLeases(
                List<RecoveryCheckpointReferenceRecord> references) {
            for (RecoveryCheckpointReferenceRecord reference : references) {
                assertThat(physical.scanReaderLeases(
                                CLUSTER,
                                new com.nereusstream.api.ObjectKeyHash(
                                        reference.objectKeyHash()),
                                Optional.empty(),
                                100)
                        .join()
                        .values())
                        .isEmpty();
            }
        }

        @Override
        public void close() throws Exception {
            for (ObjectReadPinManager manager : readPinManagers) {
                manager.close();
            }
            for (ObjectProtectionManager manager : protectionManagers) {
                manager.close();
            }
            stagingFiles.close();
            objectStore.close();
            generations.close();
            physical.close();
            scheduler.shutdownNow();
        }
    }

    private static OxiaMetadataStore l0Store(
            AppendRecoveryHead head,
            List<AppendRecoveryCommit> commits,
            StreamMetadataSnapshot snapshot) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "readAppendRecoveryTail" -> {
                        AppendRecoveryAnchor anchor = (AppendRecoveryAnchor) arguments[2];
                        if (anchor.isGenesis()) {
                            yield CompletableFuture.completedFuture(new AppendRecoveryTailPage(
                                    anchor,
                                    head,
                                    commits,
                                    true,
                                    Optional.empty()));
                        }
                        if (!anchor.lastCommitId().equals(head.lastCommitId())
                                || anchor.offsetEnd() != head.offsetEnd()
                                || anchor.cumulativeSize() != head.cumulativeSize()
                                || anchor.commitVersion() != head.commitVersion()) {
                            throw new AssertionError("unexpected checkpoint anchor: " + anchor);
                        }
                        yield CompletableFuture.completedFuture(new AppendRecoveryTailPage(
                                anchor,
                                head,
                                List.of(),
                                true,
                                Optional.empty()));
                    }
                    case "getStreamSnapshot" -> CompletableFuture.completedFuture(snapshot);
                    case "close" -> null;
                    case "toString" -> "RecoveryCheckpointCoordinatorL0";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static StreamMetadataSnapshot snapshot() {
        long version = 8;
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/checkpoint-topic",
                        "stream-name-hash",
                        StreamState.ACTIVE.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        version),
                new CommittedEndOffsetRecord(STREAM.value(), 2, 20, 2, version),
                new TrimRecord(STREAM.value(), 0, "", 1, version));
    }

    private static AppendRecoveryCommit commit(
            String id,
            String previous,
            long start,
            long end,
            long cumulative,
            long version) {
        StreamCommitTargetRecord value = new StreamCommitTargetRecord(
                STREAM.value(),
                id,
                previous,
                start,
                end,
                0,
                cumulative,
                version,
                "writer",
                "writer-run",
                1,
                "fencing-hash",
                ReadTargetCodecRegistry.phase15().encode(walTarget()),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                1,
                1,
                10,
                List.of(),
                PROJECTION_IDENTITY,
                1,
                1,
                1,
                0);
        byte[] bytes = MetadataRecordCodecFactory.encodeEnvelope(
                value, StreamCommitTargetRecord.class);
        Checksum digest = sha(bytes);
        return new AppendRecoveryCommit(
                "/commit/" + id,
                AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1,
                value,
                version,
                digest,
                ByteBuffer.wrap(bytes),
                digest);
    }

    private static ObjectSliceReadTarget target() {
        ObjectId objectId = new ObjectId("checkpoint-target-object");
        ObjectKey objectKey = new ObjectKey("objects/checkpoint/target-object");
        return new ObjectSliceReadTarget(
                1,
                objectId,
                objectKey,
                ObjectType.STREAM_COMPACTED_OBJECT,
                "NEREUS_COMPACTED_PARQUET_V1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "checkpoint-target-slice",
                0,
                128,
                crc("01020304"),
                new EntryIndexRef(
                        EntryIndexLocation.OBJECT_FOOTER,
                        Optional.of(objectId),
                        Optional.of(objectKey),
                        Optional.empty(),
                        64,
                        16,
                        crc("05060708")));
    }

    private static ObjectSliceReadTarget walTarget() {
        ObjectId objectId = new ObjectId("checkpoint-wal-object");
        ObjectKey objectKey = new ObjectKey("objects/checkpoint/wal-object");
        return new ObjectSliceReadTarget(
                1,
                objectId,
                objectKey,
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "checkpoint-wal-slice",
                0,
                128,
                crc("11111111"),
                new EntryIndexRef(
                        EntryIndexLocation.OBJECT_FOOTER,
                        Optional.of(objectId),
                        Optional.of(objectKey),
                        Optional.empty(),
                        64,
                        16,
                        crc("22222222")));
    }

    private static PhysicalObjectRootRecord activeRoot(
            PhysicalObjectIdentity identity) {
        long now = CLOCK.millis();
        return new PhysicalObjectRootRecord(
                1,
                identity.objectKeyHash().value(),
                identity.objectKey().value(),
                identity.objectId().orElseThrow().value(),
                identity.kind().wireId(),
                identity.objectLength(),
                identity.storageChecksum().type().name(),
                identity.storageChecksum().value(),
                identity.contentSha256().orElseThrow().value(),
                identity.etag().orElseThrow(),
                PhysicalObjectLifecycle.ACTIVE,
                1,
                now,
                now + Duration.ofMinutes(10).toMillis(),
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                0);
    }

    private static GenerationProtocolActivationGuard activationGuard() {
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<GenerationActivationProof> requireReady(
                    GenerationOperation operation,
                    com.nereusstream.core.capability.GenerationActivationSubject subject,
                    boolean activateLiveProjectionIfAbsent) {
                return CompletableFuture.completedFuture(GenerationActivationProof.create(
                        operation,
                        subject,
                        1,
                        1,
                        1,
                        REFERENCE_DOMAINS_SHA,
                        true,
                        false,
                        CLOCK.millis()));
            }

            @Override
            public CompletableFuture<Void> revalidate(
                    GenerationActivationProof proof) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static void retireIndex(
            GenerationMetadataStore store,
            VersionedGenerationIndex current) {
        VersionedGenerationIndex draining = store.compareAndSetIndex(
                        CLUSTER,
                        indexState(
                                current.value(),
                                GenerationLifecycle.DRAINING,
                                "registration-retirement-test-drain",
                                3),
                        current.metadataVersion())
                .join();
        store.compareAndSetIndex(
                        CLUSTER,
                        indexState(
                                draining.value(),
                                GenerationLifecycle.RETIRED,
                                "registration-retirement-test-retired",
                                4),
                        draining.metadataVersion())
                .join();
    }

    private static GenerationIndexRecord indexState(
            GenerationIndexRecord current,
            GenerationLifecycle lifecycle,
            String reason,
            long changedAtMillis) {
        return new GenerationIndexRecord(
                current.schemaVersion(),
                current.streamId(),
                current.readViewId(),
                current.offsetStart(),
                current.offsetEnd(),
                current.generation(),
                current.publicationId(),
                current.taskId(),
                lifecycle,
                current.sourceSetSha256(),
                current.policySha256(),
                current.readTarget(),
                current.targetIdentitySha256(),
                current.materializationPolicySha256(),
                current.payloadFormat(),
                current.sourceRecordCount(),
                current.outputRecordCount(),
                current.entryCount(),
                current.logicalBytes(),
                current.cumulativeSizeAtStart(),
                current.cumulativeSizeAtEnd(),
                current.firstCommitVersion(),
                current.lastCommitVersion(),
                current.schemaRefs(),
                current.projectionRef(),
                current.createdAtMillis(),
                current.committedAtMillis(),
                reason,
                changedAtMillis,
                0);
    }

    private static OxiaMetadataStore retirementL0(OxiaMetadataStore delegate) {
        StreamMetadataSnapshot deleted = deletedSnapshot();
        return proxy(
                OxiaMetadataStore.class,
                delegate,
                (method, arguments, result) -> method.getName().equals("getStreamSnapshot")
                        ? CompletableFuture.completedFuture(deleted)
                        : result);
    }

    private static StreamMetadataSnapshot deletedSnapshot() {
        long version = 9;
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/checkpoint-topic",
                        "stream-name-hash",
                        StreamState.DELETED.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        version),
                new CommittedEndOffsetRecord(STREAM.value(), 2, 20, 2, version),
                new TrimRecord(STREAM.value(), 0, "", 1, version));
    }

    private static GenerationMetadataStore loseRetirementDeleteResponses(
            GenerationMetadataStore delegate,
            AtomicBoolean loseIndexDelete,
            AtomicBoolean loseRootDelete,
            AtomicBoolean loseRegistrationDelete) {
        return proxy(
                GenerationMetadataStore.class,
                delegate,
                (method, arguments, result) -> {
                    AtomicBoolean loss = switch (method.getName()) {
                        case "deleteIndex" -> loseIndexDelete;
                        case "deleteRecoveryRoot" -> loseRootDelete;
                        case "deleteStreamRegistration" -> loseRegistrationDelete;
                        default -> null;
                    };
                    if (loss != null && loss.compareAndSet(true, false)) {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<Void> deleted = (CompletableFuture<Void>) result;
                        return deleted.thenCompose(ignored -> CompletableFuture.failedFuture(
                                new IllegalStateException(
                                        "lost " + method.getName() + " response")));
                    }
                    return result;
                });
    }

    private static PhysicalObjectMetadataStore loseRetirementProtectionDeleteResponse(
            PhysicalObjectMetadataStore delegate,
            AtomicBoolean loseResponse) {
        return proxy(
                PhysicalObjectMetadataStore.class,
                delegate,
                (method, arguments, result) -> {
                    if (method.getName().equals("deleteProtection")
                            && loseResponse.compareAndSet(true, false)) {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<Void> deleted = (CompletableFuture<Void>) result;
                        return deleted.thenCompose(ignored -> CompletableFuture.failedFuture(
                                new IllegalStateException(
                                        "lost deleteProtection response")));
                    }
                    return result;
                });
    }

    private static PhysicalGcConfig physicalGcConfig() {
        return new PhysicalGcConfig(
                true,
                false,
                1,
                1,
                1,
                10,
                100,
                100,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ofMillis(5),
                Duration.ofSeconds(11),
                Duration.ofSeconds(30),
                Duration.ofHours(1),
                Duration.ofHours(2),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2));
    }

    private static GenerationMetadataStore
            observePinnedMergeAndLoseRootCasResponse(
                    GenerationMetadataStore delegate,
                    PhysicalObjectMetadataStore physical,
                    List<RecoveryCheckpointReferenceRecord> sources,
                    AtomicBoolean observedPinnedSources) {
        AtomicBoolean lose = new AtomicBoolean(true);
        return proxy(
                GenerationMetadataStore.class,
                delegate,
                (method, arguments, result) -> {
                    if (!method.getName().equals(
                                    "compareAndSetRecoveryRoot")
                            || !(arguments[1]
                                    instanceof RecoveryCheckpointRootRecord
                                            replacement)
                            || replacement.checkpoints().size() != 1) {
                        return result;
                    }
                    assertThat(sources).hasSize(
                            RecoveryCheckpointMerger.MERGE_REFERENCE_COUNT);
                    for (RecoveryCheckpointReferenceRecord source : sources) {
                        assertThat(physical.scanReaderLeases(
                                        CLUSTER,
                                        new com.nereusstream.api.ObjectKeyHash(
                                                source.objectKeyHash()),
                                        Optional.empty(),
                                        100)
                                .join()
                                .values())
                                .as("source lease at merge root CAS: %s", source.objectKey())
                                .isNotEmpty();
                    }
                    observedPinnedSources.set(true);
                    if (lose.compareAndSet(true, false)) {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<VersionedRecoveryCheckpointRoot> exact =
                                (CompletableFuture<VersionedRecoveryCheckpointRoot>) result;
                        return exact.thenCompose(ignored ->
                                CompletableFuture.failedFuture(
                                        new F4MetadataConditionFailedException(
                                                "lost merged recovery-root CAS response")));
                    }
                    return result;
                });
    }

    private static ObjectReadPinManager
            failFirstPinSelectionRevalidation(
                    ObjectReadPinManager delegate) {
        AtomicBoolean fail = new AtomicBoolean(true);
        return new ObjectReadPinManager() {
            @Override
            public CompletableFuture<ObjectReadLease> acquire(
                    PhysicalObjectIdentity object,
                    long maximumReadDeadlineMillis,
                    SelectionRevalidator selectionRevalidator) {
                SelectionRevalidator exact = fail.compareAndSet(true, false)
                        ? () -> selectionRevalidator.revalidate()
                                .thenCompose(ignored ->
                                        CompletableFuture.failedFuture(
                                                new F4MetadataConditionFailedException(
                                                        "selection changed after merge lease write")))
                        : selectionRevalidator;
                return delegate.acquire(
                        object, maximumReadDeadlineMillis, exact);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }

    private static GenerationMetadataStore loseFirstRootCasResponse(
            GenerationMetadataStore delegate) {
        AtomicBoolean lose = new AtomicBoolean(true);
        return proxy(
                GenerationMetadataStore.class,
                delegate,
                (method, arguments, result) -> {
                    if (method.getName().equals("compareAndSetRecoveryRoot")
                            && lose.compareAndSet(true, false)) {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<VersionedRecoveryCheckpointRoot> exact =
                                (CompletableFuture<VersionedRecoveryCheckpointRoot>) result;
                        return exact.thenCompose(ignored -> CompletableFuture.failedFuture(
                                new F4MetadataConditionFailedException(
                                        "lost recovery-root CAS response")));
                    }
                    return result;
                });
    }

    private static PhysicalObjectMetadataStore failFirstCheckpointObjectProtection(
            PhysicalObjectMetadataStore delegate) {
        AtomicBoolean fail = new AtomicBoolean(true);
        return (PhysicalObjectMetadataStore) Proxy.newProxyInstance(
                PhysicalObjectMetadataStore.class.getClassLoader(),
                new Class<?>[] {PhysicalObjectMetadataStore.class},
                (proxy, method, arguments) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(delegate, arguments);
                    }
                    if (method.getName().equals("createProtection")
                            && arguments[1] instanceof ObjectProtectionRecord record
                            && ObjectProtectionType.fromWireId(record.protectionTypeId())
                                    == ObjectProtectionType.RECOVERY_CHECKPOINT_OBJECT
                            && fail.compareAndSet(true, false)) {
                        return CompletableFuture.failedFuture(
                                new F4MetadataConditionFailedException(
                                        "simulated process death after root CAS"));
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static <T> T proxy(
            Class<T> type,
            T delegate,
            ResultInterceptor interceptor) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (proxy, method, arguments) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(delegate, arguments);
                    }
                    Object result;
                    try {
                        result = method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    return interceptor.intercept(method, arguments, result);
                }));
    }

    private static Checksum crc(String value) {
        return new Checksum(ChecksumType.CRC32C, value);
    }

    private static String base32Id(char prefix, int index) {
        return Character.toString(prefix).repeat(24)
                + BASE32.charAt(index / BASE32.length())
                + BASE32.charAt(index % BASE32.length());
    }

    private static String projectionIdentity(ProjectionRef value) {
        return encoded("projectionRef")
                + encoded("present")
                + encoded(value.type().name())
                + encoded(value.value());
    }

    private static String encoded(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length + ":" + value;
    }

    private static Checksum sha(String value) {
        return sha(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Checksum sha(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    @FunctionalInterface
    private interface ResultInterceptor {
        Object intercept(Method method, Object[] arguments, Object result);
    }
}
