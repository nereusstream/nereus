/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.append.GenerationZeroProtectionIdentities;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationActivationSubject;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.retirement.GenerationZeroMarkerIdentity;
import com.nereusstream.metadata.oxia.retirement.GenericCommittedAppendIdentity;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroCommit;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroMarker;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AbandonedAppendIntentGcTest {
    private static final String CLUSTER = "cluster/abandoned";
    private static final StreamId STREAM = new StreamId("stream-abandoned");
    private static final ObjectId OBJECT_ID = new ObjectId("wal-abandoned");
    private static final ObjectKey OBJECT_KEY = new ObjectKey("objects/wal-abandoned");
    private static final String COMMIT_ID = "commit-abandoned";
    private static final Checksum SHA = sha('a');

    @TempDir
    Path temporary;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void restartRetiresAbandonedIntentOnlyAfterGraceAndAllGlobalDomainsClear() {
        MutableClock clock = new MutableClock(1_000);
        FakePhysicalObjectMetadataStore physical = new FakePhysicalObjectMetadataStore();
        MutableSourceStore sources = new MutableSourceStore();
        List<MutableDomain> domains = domains();
        MutableDomain appendRecovery = domains.stream()
                .filter(value -> value.domainId().equals(AppendRecoveryReferenceDomain.DOMAIN_ID))
                .findFirst()
                .orElseThrow();
        appendRecovery.veto = true;
        try (LocalFileObjectStore objects = new LocalFileObjectStore(
                temporary.resolve("restart"))) {
            Fixture fixture = fixture(physical, sources, objects);
            Assembly firstProcess = assembly(
                    physical, sources, objects, domains, clock);
            clock.setMillis(6_000);

            OwnerlessObjectGcExecutor.ExecutionResult vetoed =
                    firstProcess.executor().executeActive(fixture.root()).join();

            assertThat(vetoed.mark().orElseThrow().status())
                    .isEqualTo(PhysicalGcMarkStatus.DOMAIN_BLOCKED);
            assertThat(sources.get(COMMIT_ID)).contains(fixture.commit());
            assertThat(protections(physical, fixture.root())).hasSize(1);
            appendRecovery.veto = false;

            OwnerlessObjectGcExecutor.ExecutionResult marked = firstProcess
                    .executor()
                    .executeActive(fixture.root())
                    .join();

            assertThat(marked.mark().orElseThrow().status())
                    .isEqualTo(PhysicalGcMarkStatus.MARKED);
            GcPlan plan = marked.mark().orElseThrow().plan().orElseThrow();
            assertThat(plan.plannedProtectionRemovals()).hasSize(1);
            assertThat(plan.plannedMetadataRemovals())
                    .singleElement()
                    .satisfies(removal -> {
                        assertThat(removal.removalType())
                                .isEqualTo(GenerationZeroCommitRetirementHandler.REMOVAL_TYPE);
                        assertThat(removal.key()).isEqualTo(fixture.commit().key());
                    });
            assertThat(marked.advance().orElseThrow().status())
                    .isEqualTo(PhysicalGcAdvanceStatus.WAITING_FOR_GRACE);
            VersionedPhysicalObjectRoot durableMarked = physical.getRoot(
                            CLUSTER,
                            new ObjectKeyHash(
                                    fixture.root().value().objectKeyHash()))
                    .join()
                    .orElseThrow();
            assertThat(durableMarked.value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.MARKED);

            // No process-local candidate, future or journal object is reused after this point.
            sources.loseNextDeleteResponse.set(true);
            clock.setMillis(16_000);
            Assembly restarted = assembly(
                    physical, sources, objects, domains, clock);
            OwnerlessObjectGcExecutor.ExecutionResult deleted = restarted
                    .executor()
                    .recoverMarked(durableMarked)
                    .join();

            assertThat(deleted.advance().orElseThrow().status())
                    .isEqualTo(PhysicalGcAdvanceStatus.DELETE_INTENT);
            assertThat(deleted.deletion().orElseThrow().status())
                    .isEqualTo(PhysicalGcDeletionStatus.DELETED);
            assertThat(deleted.deletion().orElseThrow().metadataRetired())
                    .isZero();
            assertThat(deleted.deletion().orElseThrow().metadataAlreadyAbsent())
                    .isOne();
            assertThat(sources.get(COMMIT_ID)).isEmpty();
            assertThat(protections(physical, fixture.root())).isEmpty();
            assertThatThrownBy(() -> objects.headObject(
                                    OBJECT_KEY,
                                    new HeadObjectOptions(Duration.ofSeconds(1)))
                            .join())
                    .hasRootCauseInstanceOf(NereusException.class);
            assertThat(domains)
                    .allSatisfy(domain -> assertThat(domain.snapshotCalls)
                            .hasPositiveValue());
        }
    }

    @Test
    void completeDriftUnmarksThenStaleProtectionIsReboundBeforeAnotherMark() {
        MutableClock clock = new MutableClock(6_000);
        FakePhysicalObjectMetadataStore physical = new FakePhysicalObjectMetadataStore();
        MutableSourceStore sources = new MutableSourceStore();
        List<MutableDomain> domains = domains();
        MutableDomain appendRecovery = domains.stream()
                .filter(value -> value.domainId().equals(AppendRecoveryReferenceDomain.DOMAIN_ID))
                .findFirst()
                .orElseThrow();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(
                temporary.resolve("epoch-rebind"))) {
            Fixture fixture = fixture(physical, sources, objects);
            Assembly assembly = assembly(
                    physical, sources, objects, domains, clock);
            OwnerlessObjectGcExecutor.ExecutionResult first = assembly
                    .executor()
                    .executeActive(fixture.root())
                    .join();
            assertThat(first.mark().orElseThrow().status())
                    .isEqualTo(PhysicalGcMarkStatus.MARKED);
            VersionedPhysicalObjectRoot marked = physical.getRoot(
                            CLUSTER,
                            new ObjectKeyHash(
                                    fixture.root().value().objectKeyHash()))
                    .join()
                    .orElseThrow();
            appendRecovery.veto = true;
            clock.setMillis(16_000);

            OwnerlessObjectGcExecutor.ExecutionResult drifted = assembly
                    .executor()
                    .recoverMarked(marked)
                    .join();

            assertThat(drifted.advance().orElseThrow().status())
                    .isEqualTo(PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED);
            VersionedPhysicalObjectRoot activeAgain = physical.getRoot(
                            CLUSTER, new ObjectKeyHash(fixture.root().value().objectKeyHash()))
                    .join()
                    .orElseThrow();
            assertThat(activeAgain.value().lifecycleEpoch()).isEqualTo(3);
            assertThat(protections(physical, activeAgain))
                    .singleElement()
                    .satisfies(value -> assertThat(value.value().rootLifecycleEpoch())
                            .isEqualTo(1));
            appendRecovery.veto = false;

            OwnerlessObjectGcExecutor.ExecutionResult rebound = assembly
                    .executor()
                    .executeActive(activeAgain)
                    .join();

            assertThat(rebound.mark()).isEmpty();
            assertThat(protections(physical, activeAgain))
                    .singleElement()
                    .satisfies(value -> assertThat(value.value().rootLifecycleEpoch())
                            .isEqualTo(3));
            VersionedPhysicalObjectRoot stillActive = physical.getRoot(
                            CLUSTER, new ObjectKeyHash(activeAgain.value().objectKeyHash()))
                    .join()
                    .orElseThrow();
            assertThat(stillActive.value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.ACTIVE);

            OwnerlessObjectGcExecutor.ExecutionResult retried = assembly
                    .executor()
                    .executeActive(stillActive)
                    .join();

            assertThat(retried.mark().orElseThrow().status())
                    .isEqualTo(PhysicalGcMarkStatus.MARKED);
        }
    }

    @Test
    void ownerAppearanceAfterAnAbsentOwnerPlanChangesTheExactMetadataReload() {
        MutableClock clock = new MutableClock(6_000);
        FakePhysicalObjectMetadataStore physical = new FakePhysicalObjectMetadataStore();
        MutableSourceStore sources = new MutableSourceStore();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(
                temporary.resolve("owner-appearance"))) {
            Fixture fixture = fixture(physical, sources, objects);
            sources.remove(COMMIT_ID);
            PhysicalGcConfig config = config();
            ObjectProtectionManager manager = protectionManager(
                    physical, config, clock);
            AbandonedAppendIntentPlanBuilder builder =
                    new AbandonedAppendIntentPlanBuilder(
                            CLUSTER,
                            physical,
                            sources,
                            manager,
                            config,
                            scheduler);

            AbandonedAppendIntentPlanBuilder.Inspection inspection =
                    builder.inspectActive(fixture.root()).join();
            assertThat(inspection.metadataRemovals()).isEmpty();
            PhysicalObjectIdentity object = PhysicalObjectIdentity.from(
                    fixture.root().value());
            GcReferenceQuery query = GcReferenceQuery.create(
                    GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                    object,
                    List.of(),
                    object.identitySha256());
            GcCandidate candidate = GcCandidate.fromActiveRoot(
                    config,
                    "c".repeat(52),
                    fixture.root(),
                    query,
                    object.identitySha256(),
                    clock.millis(),
                    inspection.notBeforeMillis());

            sources.put(fixture.commit());
            assertThat(builder.reload(candidate, inspection.metadataRemovals()).join())
                    .singleElement()
                    .satisfies(removal -> assertThat(removal.key())
                            .isEqualTo(fixture.commit().key()));
        }
    }

    private Assembly assembly(
            FakePhysicalObjectMetadataStore physical,
            MutableSourceStore sources,
            LocalFileObjectStore objects,
            List<MutableDomain> domainList,
            Clock clock) {
        PhysicalGcConfig config = config();
        ObjectProtectionManager protections = protectionManager(
                physical, config, clock);
        AbandonedAppendIntentPlanBuilder builder =
                new AbandonedAppendIntentPlanBuilder(
                        CLUSTER,
                        physical,
                        sources,
                        protections,
                        config,
                        scheduler);
        GcReferenceDomainRegistry domains = new GcReferenceDomainRegistry(
                config,
                scheduler,
                new ArrayList<>(domainList));
        DefaultGcRetirementJournal journal = new DefaultGcRetirementJournal(
                CLUSTER, physical, config);
        PhysicalObjectGarbageCollector collector =
                new PhysicalObjectGarbageCollector(
                        CLUSTER,
                        config,
                        physical,
                        domains,
                        activationGuard(),
                        builder,
                        journal,
                        new SecureGcIdGenerator(),
                        clock,
                        scheduler);
        SourceRetirementCoordinator retirement = new SourceRetirementCoordinator(
                CLUSTER,
                config,
                physical,
                journal,
                new GcMetadataRetirementRegistry(List.of(
                        new GenerationZeroCommitRetirementHandler(
                                CLUSTER, sources))),
                objects,
                clock,
                scheduler);
        return new Assembly(
                builder,
                new OwnerlessObjectGcExecutor(
                        CLUSTER,
                        config,
                        builder,
                        domains,
                        collector,
                        retirement,
                        new SecureGcIdGenerator(),
                        clock));
    }

    private static ObjectProtectionManager protectionManager(
            FakePhysicalObjectMetadataStore physical,
            PhysicalGcConfig config,
            Clock clock) {
        return new DefaultObjectProtectionManager(
                CLUSTER,
                physical,
                config.pendingProtectionDuration(),
                config.maximumClockSkew(),
                config.orphanGrace(),
                clock);
    }

    private static Fixture fixture(
            FakePhysicalObjectMetadataStore physical,
            MutableSourceStore sources,
            LocalFileObjectStore objects) {
        byte[] bytes = "abandoned-object-wal".getBytes(StandardCharsets.UTF_8);
        PutObjectResult uploaded = objects.putObject(
                        OBJECT_KEY,
                        ByteBuffer.wrap(bytes),
                        new PutObjectOptions(
                                "application/octet-stream",
                                Crc32cChecksums.checksum(bytes),
                                true,
                                Map.of(),
                                Duration.ofSeconds(1)))
                .join();
        VersionedPhysicalObjectRoot root = physical.createRoot(
                        CLUSTER,
                        new PhysicalObjectRootRecord(
                                1,
                                ObjectKeyHash.from(uploaded.key()).value(),
                                uploaded.key().value(),
                                OBJECT_ID.value(),
                                PhysicalObjectKind.OBJECT_WAL.wireId(),
                                uploaded.objectLength(),
                                uploaded.checksum().type().name(),
                                uploaded.checksum().value(),
                                "",
                                uploaded.etag(),
                                PhysicalObjectLifecycle.ACTIVE,
                                1,
                                100,
                                100,
                                "",
                                "",
                                0,
                                0,
                                0,
                                0,
                                0,
                                "",
                                "",
                                0))
                .join();
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                OBJECT_ID,
                OBJECT_KEY,
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "NWA1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "slice-abandoned",
                0,
                uploaded.objectLength(),
                uploaded.checksum(),
                new EntryIndexRef(
                        EntryIndexLocation.INLINE,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new byte[] {1}),
                        0,
                        0,
                        new Checksum(ChecksumType.CRC32C, "01020304")));
        StreamCommitTargetRecord canonical = new StreamCommitTargetRecord(
                STREAM.value(),
                COMMIT_ID,
                "",
                0,
                1,
                0,
                uploaded.objectLength(),
                1,
                "writer",
                "writer-run",
                1,
                "fencing-token",
                ReadTargetCodecRegistry.phase15().encode(target),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                1,
                1,
                uploaded.objectLength(),
                List.of(),
                "projection",
                0,
                0,
                1_000,
                0);
        VersionedGenerationZeroCommit commit = new VersionedGenerationZeroCommit(
                new OxiaKeyspace(CLUSTER).streamCommitKey(STREAM, COMMIT_ID),
                STREAM,
                COMMIT_ID,
                AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1,
                new GenericCommittedAppendIdentity(COMMIT_ID),
                canonical,
                0,
                1,
                1,
                sha256(MetadataRecordCodecFactory.encodeEnvelope(
                        canonical, StreamCommitTargetRecord.class)),
                7,
                sha('c'));
        sources.put(commit);
        physical.createProtection(
                        CLUSTER,
                        new ObjectProtectionRecord(
                                1,
                                root.value().objectKeyHash(),
                                ObjectProtectionType.REACHABLE_APPEND.wireId(),
                                GenerationZeroProtectionIdentities.reachableAppendReferenceId(
                                        STREAM,
                                        COMMIT_ID,
                                        new ObjectKeyHash(root.value().objectKeyHash())),
                                commit.key(),
                                commit.metadataVersion(),
                                commit.durableValueSha256().value(),
                                root.value().lifecycleEpoch(),
                                1_000,
                                0,
                                0))
                .join();
        return new Fixture(root, commit);
    }

    private static List<com.nereusstream.metadata.oxia.VersionedObjectProtection> protections(
            FakePhysicalObjectMetadataStore physical,
            VersionedPhysicalObjectRoot root) {
        ArrayList<com.nereusstream.metadata.oxia.VersionedObjectProtection> result =
                new ArrayList<>();
        Optional<com.nereusstream.metadata.oxia.F4ScanToken> continuation =
                Optional.empty();
        do {
            ObjectProtectionScanPage page = physical.scanProtections(
                            CLUSTER,
                            new ObjectKeyHash(root.value().objectKeyHash()),
                            continuation,
                            10)
                    .join();
            result.addAll(page.values());
            continuation = page.continuation();
        } while (continuation.isPresent());
        return result;
    }

    private static List<MutableDomain> domains() {
        return List.of(
                new MutableDomain(AppendRecoveryReferenceDomain.DOMAIN_ID),
                new MutableDomain("cursor-snapshot-v1"),
                new MutableDomain(FutureCatalogSentinelDomain.DOMAIN_ID),
                new MutableDomain(GenerationReferenceDomain.DOMAIN_ID),
                new MutableDomain(MaterializationReferenceDomain.DOMAIN_ID),
                new MutableDomain(PhysicalObjectGarbageCollector.PROJECTION_REFERENCE_DOMAIN));
    }

    private static GenerationProtocolActivationGuard activationGuard() {
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<GenerationActivationProof> requireReady(
                    GenerationOperation operation,
                    GenerationActivationSubject subject,
                    boolean activateLiveProjectionIfAbsent) {
                return CompletableFuture.completedFuture(
                        GenerationActivationProof.create(
                                operation,
                                subject,
                                0,
                                1,
                                1,
                                SHA,
                                false,
                                true,
                                1_000));
            }

            @Override
            public CompletableFuture<Void> revalidate(
                    GenerationActivationProof proof) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static PhysicalGcConfig config() {
        return new PhysicalGcConfig(
                true,
                false,
                10,
                10,
                1,
                4_096,
                100,
                100,
                Duration.ofMinutes(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ZERO,
                Duration.ofSeconds(10),
                Duration.ofSeconds(1),
                Duration.ofSeconds(5),
                Duration.ofDays(7),
                Duration.ofSeconds(1),
                Duration.ofSeconds(2));
    }

    private static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256,
                String.valueOf(value).repeat(64));
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                            .digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private record Fixture(
            VersionedPhysicalObjectRoot root,
            VersionedGenerationZeroCommit commit) { }

    private record Assembly(
            AbandonedAppendIntentPlanBuilder builder,
            OwnerlessObjectGcExecutor executor) { }

    private static final class MutableDomain implements GcReferenceDomain {
        private final String domainId;
        private final AtomicInteger snapshotCalls = new AtomicInteger();
        private volatile boolean veto;

        private MutableDomain(String domainId) {
            this.domainId = domainId;
        }

        @Override
        public String domainId() {
            return domainId;
        }

        @Override
        public int protocolVersion() {
            return 1;
        }

        @Override
        public CompletableFuture<GcReferenceSnapshot> snapshot(
                GcReferenceQuery query) {
            snapshotCalls.incrementAndGet();
            return CompletableFuture.completedFuture(GcReferenceSnapshot.create(
                    domainId,
                    1,
                    query.queryIdentitySha256(),
                    true,
                    veto,
                    1,
                    0,
                    List.of(new GcAuthorityToken(
                            "/authority/" + domainId,
                            1,
                            SHA)),
                    List.of()));
        }

        @Override
        public CompletableFuture<Boolean> stillMatches(
                GcReferenceQuery query,
                GcReferenceSnapshot snapshot) {
            return snapshot(query).thenApply(snapshot::equals);
        }
    }

    private static final class MutableSourceStore
            implements SourceRetirementMetadataStore {
        private final Map<String, VersionedGenerationZeroCommit> commits =
                new HashMap<>();
        private final AtomicBoolean loseNextDeleteResponse = new AtomicBoolean();

        private synchronized void put(VersionedGenerationZeroCommit commit) {
            commits.put(commit.key(), commit);
        }

        private synchronized void remove(String commitId) {
            commits.values().removeIf(value -> value.commitId().equals(commitId));
        }

        private synchronized Optional<VersionedGenerationZeroCommit> get(
                String commitId) {
            return commits.values().stream()
                    .filter(value -> value.commitId().equals(commitId))
                    .findFirst();
        }

        @Override
        public synchronized CompletableFuture<Optional<VersionedGenerationZeroCommit>>
                getCommitNodeByKey(String cluster, String exactKey) {
            return CompletableFuture.completedFuture(
                    Optional.ofNullable(commits.get(exactKey)));
        }

        @Override
        public CompletableFuture<Optional<VersionedGenerationZeroMarker>>
                getCommittedMarkerByKey(String cluster, String exactKey) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Optional<VersionedGenerationZeroMarker>> getCommittedMarker(
                String cluster,
                StreamId streamId,
                GenerationZeroMarkerIdentity marker) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<Void> deleteGenerationZeroIndex(
                String cluster,
                StreamId streamId,
                long offsetEnd,
                long expectedVersion,
                Checksum expectedDurableValueSha256) {
            return unsupported();
        }

        @Override
        public CompletableFuture<Void> deleteCommittedMarker(
                String cluster,
                StreamId streamId,
                GenerationZeroMarkerIdentity marker,
                long expectedVersion,
                Checksum expectedDurableValueSha256) {
            return unsupported();
        }

        @Override
        public CompletableFuture<Void> deleteCommittedMarkerByKey(
                String cluster,
                String exactKey,
                long expectedVersion,
                Checksum expectedDurableValueSha256) {
            return unsupported();
        }

        @Override
        public CompletableFuture<Void> deleteCommitNode(
                String cluster,
                StreamId streamId,
                String commitId,
                long expectedVersion,
                Checksum expectedDurableValueSha256) {
            return deleteCommitNodeByKey(
                    cluster,
                    new OxiaKeyspace(cluster).streamCommitKey(streamId, commitId),
                    expectedVersion,
                    expectedDurableValueSha256);
        }

        @Override
        public synchronized CompletableFuture<Void> deleteCommitNodeByKey(
                String cluster,
                String exactKey,
                long expectedVersion,
                Checksum expectedDurableValueSha256) {
            VersionedGenerationZeroCommit current = commits.get(exactKey);
            if (current == null
                    || current.metadataVersion() != expectedVersion
                    || !current.durableValueSha256().equals(
                            expectedDurableValueSha256)) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("commit delete identity changed"));
            }
            commits.remove(exactKey);
            return loseNextDeleteResponse.compareAndSet(true, false)
                    ? CompletableFuture.failedFuture(
                            new IllegalStateException("lost commit delete response"))
                    : CompletableFuture.completedFuture(null);
        }

        private static CompletableFuture<Void> unsupported() {
            return CompletableFuture.failedFuture(
                    new UnsupportedOperationException("unused source operation"));
        }

        @Override
        public void close() { }
    }

    private static final class MutableClock extends Clock {
        private volatile long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void setMillis(long value) {
            millis = value;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
