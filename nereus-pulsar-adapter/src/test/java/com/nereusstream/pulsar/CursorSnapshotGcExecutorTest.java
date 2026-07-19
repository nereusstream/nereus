/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.managedledger.cursor.CursorIdentity;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorSnapshotKeys;
import com.nereusstream.managedledger.retention.CursorSnapshotGcScanner;
import com.nereusstream.managedledger.retention.CursorSnapshotReferenceDomain;
import com.nereusstream.materialization.gc.GcCandidate;
import com.nereusstream.materialization.gc.DefaultGcRetirementJournal;
import com.nereusstream.materialization.gc.GcMetadataRetirementRegistry;
import com.nereusstream.materialization.gc.GcPlannedProtectionRemoval;
import com.nereusstream.materialization.gc.GcReferenceDomainRegistry;
import com.nereusstream.materialization.gc.PhysicalGcAdvanceStatus;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.materialization.gc.PhysicalGcDeletionStatus;
import com.nereusstream.materialization.gc.PhysicalGcMarkStatus;
import com.nereusstream.materialization.gc.PhysicalObjectGarbageCollector;
import com.nereusstream.materialization.gc.SourceRetirementCoordinator;
import com.nereusstream.metadata.oxia.CursorKeyspace;
import com.nereusstream.metadata.oxia.CursorMetadataDigests;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.FakeCursorMetadataStore;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorProtectionIntentRecord;
import com.nereusstream.metadata.oxia.records.CursorProtectionKind;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CursorSnapshotGcExecutorTest {
    private static final String CLUSTER = "cluster/a";
    private static final String TOPIC = "persistent://tenant/ns/cursor-executor";
    private static final String OWNER_1 = "00112233445566778899aabbccddeeff";
    private static final String OWNER_2 = "ffeeddccbbaa99887766554433221100";
    private static final String SNAPSHOT = "11111111111111111111111111111111";
    private static final String PROTECTION_ATTEMPT = "33333333333333333333333333333333";
    private static final Checksum AUTHORITY_SHA = new Checksum(
            ChecksumType.SHA256, "a".repeat(64));

    @TempDir
    Path temporaryDirectory;

    @Test
    void markedRestartReconstructsThePlanAndCompletesExactDeletion() {
        try (Context context = new Context(temporaryDirectory.resolve("restart-delete"))) {
            VersionedCursorState cursor = context.createCursor();
            Snapshot snapshot = context.createSnapshot();
            context.createProtection(cursor, snapshot);

            CursorSnapshotGcExecutor.ScanExecutionReport first = context.executor
                    .scan(context.ledger)
                    .join();

            assertThat(first.executions()).singleElement().satisfies(execution -> {
                assertThat(execution.mark()).isPresent();
                assertThat(execution.advance().orElseThrow().status())
                        .isEqualTo(PhysicalGcAdvanceStatus.WAITING_FOR_GRACE);
                assertThat(execution.deletion()).isEmpty();
            });
            VersionedPhysicalObjectRoot marked = context.currentRoot(snapshot);
            assertThat(marked.value().lifecycle()).isEqualTo(PhysicalObjectLifecycle.MARKED);

            context.clock.advance(Duration.ofSeconds(12));
            CursorSnapshotGcExecutor.CandidateExecutionResult recovered =
                    context.executor.recoverMarked(context.ledger, marked).join();

            assertThat(recovered.mark()).isEmpty();
            assertThat(recovered.advance().orElseThrow().status())
                    .isEqualTo(PhysicalGcAdvanceStatus.DELETE_INTENT);
            assertThat(recovered.deletion().orElseThrow().status())
                    .isEqualTo(PhysicalGcDeletionStatus.DELETED);
            assertThat(context.currentRoot(snapshot).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.DELETED);
            assertThat(context.listSnapshots()).isEmpty();
            assertThat(context.physicalStore.scanProtections(
                            CLUSTER,
                            ObjectKeyHash.from(snapshot.key()),
                            Optional.empty(),
                            10)
                    .join().values())
                    .isEmpty();
        }
    }

    @Test
    void changedRestartEvidenceUnmarksWithoutDeletingBytes() {
        try (Context context = new Context(temporaryDirectory.resolve("restart-drift"))) {
            VersionedCursorState cursor = context.createCursor();
            Snapshot snapshot = context.createSnapshot();
            context.createProtection(cursor, snapshot);
            context.executor.scan(context.ledger).join();
            VersionedPhysicalObjectRoot marked = context.currentRoot(snapshot);

            context.changeCursorOwner(cursor);
            context.clock.advance(Duration.ofSeconds(12));
            CursorSnapshotGcExecutor.CandidateExecutionResult result =
                    context.executor.recoverMarked(context.ledger, marked).join();

            assertThat(result.advance().orElseThrow().status())
                    .isEqualTo(PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED);
            assertThat(result.deletion()).isEmpty();
            assertThat(context.currentRoot(snapshot).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
            assertThat(context.listSnapshots()).singleElement()
                    .extracting(value -> value.key())
                    .isEqualTo(snapshot.key());
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = CursorRetentionLifecycle.class,
            names = {"PROTECTION_PENDING", "TRIM_PENDING"})
    void pendingCursorLifecycleAtInventoryVetoesDeletion(
            CursorRetentionLifecycle lifecycle) {
        try (Context context = new Context(temporaryDirectory.resolve(
                "pending-inventory-" + lifecycle.name().toLowerCase()))) {
            VersionedCursorState cursor = context.createCursor();
            Snapshot snapshot = context.createSnapshot();
            context.createProtection(cursor, snapshot);
            context.setRetentionPending(lifecycle);

            CursorSnapshotGcExecutor.ScanExecutionReport result =
                    context.executor.scan(context.ledger).join();

            assertThat(result.scan().eligibleCandidates()).isZero();
            assertThat(result.executions()).isEmpty();
            assertThat(context.currentRoot(snapshot).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
            assertThat(context.listSnapshots()).hasSize(1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = CursorRetentionLifecycle.class,
            names = {"PROTECTION_PENDING", "TRIM_PENDING"})
    void pendingCursorLifecycleBetweenInventoryAndMarkBlocksMark(
            CursorRetentionLifecycle lifecycle) {
        try (Context context = new Context(temporaryDirectory.resolve(
                "pending-mark-" + lifecycle.name().toLowerCase()))) {
            VersionedCursorState cursor = context.createCursor();
            Snapshot snapshot = context.createSnapshot();
            context.createProtection(cursor, snapshot);
            ArrayList<CursorSnapshotGcScanner.Candidate> candidates = new ArrayList<>();
            context.scanner.scan(
                            context.ledger,
                            candidate -> {
                                candidates.add(candidate);
                                return CompletableFuture.completedFuture(null);
                            })
                    .join();
            CursorSnapshotGcScanner.Candidate discovered = candidates.getFirst();

            context.setRetentionPending(lifecycle);
            GcCandidate candidate = GcCandidate.fromActiveRoot(
                    context.gcConfig,
                    "d".repeat(52),
                    discovered.sourceRoot(),
                    discovered.referenceQuery(),
                    discovered.discoveryEvidenceSha256(),
                    discovered.discoveredAtMillis(),
                    discovered.notBeforeMillis());
            var mark = context.collector.mark(
                            candidate,
                            discovered.plannedProtectionRemovals().stream()
                                    .map(GcPlannedProtectionRemoval::new)
                                    .toList(),
                            List.of())
                    .join();

            assertThat(mark.status()).isEqualTo(PhysicalGcMarkStatus.DOMAIN_BLOCKED);
            assertThat(context.currentRoot(snapshot).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
            assertThat(context.listSnapshots()).hasSize(1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = CursorRetentionLifecycle.class,
            names = {"PROTECTION_PENDING", "TRIM_PENDING"})
    void pendingCursorLifecycleDuringDrainUnmarksWithoutDeletingBytes(
            CursorRetentionLifecycle lifecycle) {
        try (Context context = new Context(temporaryDirectory.resolve(
                "pending-drain-" + lifecycle.name().toLowerCase()))) {
            VersionedCursorState cursor = context.createCursor();
            Snapshot snapshot = context.createSnapshot();
            context.createProtection(cursor, snapshot);
            context.executor.scan(context.ledger).join();
            VersionedPhysicalObjectRoot marked = context.currentRoot(snapshot);
            assertThat(marked.value().lifecycle()).isEqualTo(PhysicalObjectLifecycle.MARKED);

            context.setRetentionPending(lifecycle);
            context.clock.advance(Duration.ofSeconds(12));
            CursorSnapshotGcExecutor.CandidateExecutionResult result =
                    context.executor.recoverMarked(context.ledger, marked).join();

            assertThat(result.advance().orElseThrow().status())
                    .isEqualTo(PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED);
            assertThat(result.deletion()).isEmpty();
            assertThat(context.currentRoot(snapshot).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
            assertThat(context.listSnapshots()).hasSize(1);
        }
    }

    private static final class Context implements AutoCloseable {
        private final FakeCursorMetadataStore cursorStore = new FakeCursorMetadataStore();
        private final FakePhysicalObjectMetadataStore physicalStore =
                new FakePhysicalObjectMetadataStore();
        private final LocalFileObjectStore objectStore;
        private final ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        private final MutableClock clock;
        private final ManagedLedgerProjectionIdentity projection;
        private final CursorLedgerIdentity ledger;
        private final CursorSnapshotGcScanner scanner;
        private final CursorSnapshotGcExecutor executor;
        private final PhysicalGcConfig gcConfig;
        private final PhysicalObjectGarbageCollector collector;

        private Context(Path root) {
            objectStore = new LocalFileObjectStore(root);
            clock = new MutableClock(
                    System.currentTimeMillis() + Duration.ofHours(27).toMillis());
            StreamId streamId = ManagedLedgerProjectionNames.streamId(TOPIC, 1);
            projection = new ManagedLedgerProjectionIdentity(
                    1,
                    1,
                    streamId.value(),
                    ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 1);
            ledger = new CursorLedgerIdentity(
                    TOPIC,
                    ManagedLedgerProjectionNames.managedLedgerNameHash(TOPIC),
                    projection);
            cursorStore.createRetention(CLUSTER, new CursorRetentionRecord(
                    0,
                    projection,
                    OWNER_1,
                    CursorRetentionLifecycle.ACTIVE,
                    1,
                    0,
                    0,
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    Optional.empty(),
                    0)).join();
            gcConfig = enabledConfig();
            scanner = new CursorSnapshotGcScanner(
                    CLUSTER,
                    cursorStore,
                    physicalStore,
                    objectStore,
                    new CursorSnapshotGcScanner.Configuration(
                            1,
                            1,
                            1,
                            10,
                            10,
                            10,
                            gcConfig.orphanGrace(),
                            gcConfig.maximumClockSkew(),
                            gcConfig.operationTimeout()),
                    clock,
                    scheduler);
            GcReferenceDomainRegistry domains = new GcReferenceDomainRegistry(
                    gcConfig,
                    scheduler,
                    List.of(
                            new CursorSnapshotReferenceDomain(
                                    CLUSTER,
                                    cursorStore,
                                    gcConfig.referenceDomainConfig()),
                            new EmptyDomain("generation-v1"),
                            new EmptyDomain("projection-generation-v1")));
            DefaultGcRetirementJournal journal = new DefaultGcRetirementJournal(
                    CLUSTER, physicalStore, gcConfig);
            collector = new PhysicalObjectGarbageCollector(
                    CLUSTER,
                    gcConfig,
                    physicalStore,
                    domains,
                    activationGuard(),
                    (candidate, expected) -> expected.isEmpty()
                            ? CompletableFuture.completedFuture(List.of())
                            : CompletableFuture.failedFuture(new AssertionError(
                                    "cursor candidate must not carry metadata removals")),
                    journal,
                    () -> "b".repeat(52),
                    clock,
                    scheduler);
            SourceRetirementCoordinator retirement = new SourceRetirementCoordinator(
                    CLUSTER,
                    gcConfig,
                    physicalStore,
                    journal,
                    new GcMetadataRetirementRegistry(List.of()),
                    objectStore,
                    clock,
                    scheduler);
            executor = new CursorSnapshotGcExecutor(
                    gcConfig,
                    scanner,
                    domains,
                    collector,
                    retirement,
                    () -> "c".repeat(52));
        }

        private void setRetentionPending(CursorRetentionLifecycle lifecycle) {
            var current = cursorStore.getRetention(
                            CLUSTER, new StreamId(projection.streamId()))
                    .join()
                    .orElseThrow();
            CursorRetentionRecord value = current.value();
            CursorRetentionRecord pending = switch (lifecycle) {
                case PROTECTION_PENDING -> new CursorRetentionRecord(
                        0,
                        projection,
                        OWNER_1,
                        lifecycle,
                        value.mutationSequence() + 1,
                        0,
                        0,
                        Optional.of(new CursorProtectionIntentRecord(
                                PROTECTION_ATTEMPT,
                                CursorProtectionKind.CREATE,
                                "pending-subscription",
                                CursorNames.cursorNameHash("pending-subscription"),
                                0,
                                1,
                                0,
                                Optional.empty(),
                                Map.of(),
                                Map.of(),
                                clock.millis())),
                        Optional.empty(),
                        OptionalLong.empty(),
                        Optional.empty(),
                        clock.millis());
                case TRIM_PENDING -> new CursorRetentionRecord(
                        0,
                        projection,
                        OWNER_1,
                        lifecycle,
                        value.mutationSequence() + 1,
                        1,
                        0,
                        Optional.empty(),
                        Optional.of(PROTECTION_ATTEMPT),
                        OptionalLong.of(1),
                        Optional.of("nereus-cursor-retention/"
                                + PROTECTION_ATTEMPT + ":gc-race"),
                        clock.millis());
                case ACTIVE -> throw new IllegalArgumentException(
                        "pending lifecycle test requires a pending value");
            };
            cursorStore.compareAndSetRetention(
                            CLUSTER, pending, current.metadataVersion())
                    .join();
        }

        private VersionedCursorState createCursor() {
            return cursorStore.createCursor(CLUSTER, new CursorStateRecord(
                    0,
                    projection,
                    OWNER_1,
                    "subscription-a",
                    CursorNames.cursorNameHash("subscription-a"),
                    1,
                    CursorRecordLifecycle.ACTIVE,
                    1,
                    1,
                    PROTECTION_ATTEMPT,
                    0,
                    Optional.empty(),
                    List.of(),
                    List.of(),
                    Map.of(),
                    Map.of(),
                    0,
                    0,
                    OptionalLong.empty())).join();
        }

        private Snapshot createSnapshot() {
            CursorIdentity identity = new CursorIdentity(
                    ledger,
                    "subscription-a",
                    CursorNames.cursorNameHash("subscription-a"),
                    1);
            ObjectKey key = CursorSnapshotKeys.objectKey(CLUSTER, identity, SNAPSHOT);
            byte[] payload = "cursor-snapshot-executor".getBytes(StandardCharsets.UTF_8);
            Checksum checksum = Crc32cChecksums.checksum(payload);
            PutObjectResult put = objectStore.putObject(
                            key,
                            ByteBuffer.wrap(payload),
                            new PutObjectOptions(
                                    "application/octet-stream",
                                    checksum,
                                    true,
                                    Map.of(),
                                    Duration.ofSeconds(2)))
                    .join();
            PhysicalObjectRootRecord root = new PhysicalObjectRootRecord(
                    1,
                    ObjectKeyHash.from(key).value(),
                    key.value(),
                    "",
                    PhysicalObjectKind.CURSOR_SNAPSHOT.wireId(),
                    payload.length,
                    checksum.type().name(),
                    checksum.value(),
                    "",
                    put.etag(),
                    PhysicalObjectLifecycle.ACTIVE,
                    1,
                    clock.millis() - Duration.ofHours(26).toMillis(),
                    clock.millis() - 1,
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
            return new Snapshot(key, physicalStore.createRoot(CLUSTER, root).join());
        }

        private void createProtection(
                VersionedCursorState cursor,
                Snapshot snapshot) {
            physicalStore.createProtection(CLUSTER, new ObjectProtectionRecord(
                    1,
                    snapshot.root().value().objectKeyHash(),
                    ObjectProtectionType.CURSOR_SNAPSHOT_ROOT.wireId(),
                    SNAPSHOT,
                    new CursorKeyspace(CLUSTER).cursorStateKey(
                            new StreamId(projection.streamId()),
                            cursor.value().cursorName()),
                    cursor.metadataVersion(),
                    CursorMetadataDigests.durableValueSha256(cursor.value()).value(),
                    snapshot.root().value().lifecycleEpoch(),
                    clock.millis() - 2_000,
                    0,
                    0)).join();
        }

        private VersionedCursorState changeCursorOwner(
                VersionedCursorState current) {
            CursorStateRecord value = current.value();
            return cursorStore.compareAndSetCursor(CLUSTER, new CursorStateRecord(
                    0,
                    value.projection(),
                    OWNER_2,
                    value.cursorName(),
                    value.cursorNameHash(),
                    value.cursorGeneration(),
                    value.lifecycle(),
                    value.mutationSequence() + 1,
                    value.ackStateEpoch(),
                    value.lastProtectionAttemptId(),
                    value.markDeleteOffset(),
                    value.snapshotReference(),
                    value.inlineWholeAckDeltas(),
                    value.inlinePartialAckOverrides(),
                    value.positionProperties(),
                    value.cursorProperties(),
                    value.createdAtMillis(),
                    value.updatedAtMillis() + 1,
                    value.deletedAtMillis()), current.metadataVersion()).join();
        }

        private VersionedPhysicalObjectRoot currentRoot(Snapshot snapshot) {
            return physicalStore.getRoot(
                            CLUSTER, ObjectKeyHash.from(snapshot.key()))
                    .join().orElseThrow();
        }

        private List<com.nereusstream.objectstore.ListedObject> listSnapshots() {
            return objectStore.listObjects(
                            new ObjectKeyPrefix(CursorSnapshotKeys.streamPrefix(CLUSTER, ledger)),
                            Optional.empty(),
                            new ListObjectsOptions(10, Duration.ofSeconds(2)))
                    .join().objects();
        }

        @Override
        public void close() {
            scanner.close();
            cursorStore.close();
            physicalStore.close();
            objectStore.close();
            scheduler.shutdownNow();
        }
    }

    private static PhysicalGcConfig enabledConfig() {
        return new PhysicalGcConfig(
                true,
                false,
                1,
                1,
                1,
                10,
                10,
                10,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ofMillis(5),
                Duration.ofSeconds(11),
                Duration.ofHours(1),
                Duration.ofHours(25),
                Duration.ofDays(7),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2));
    }

    private static GenerationProtocolActivationGuard activationGuard() {
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<GenerationActivationProof> requireReady(
                    com.nereusstream.core.capability.GenerationOperation operation,
                    com.nereusstream.core.capability.GenerationActivationSubject subject,
                    boolean activateLiveProjectionIfAbsent) {
                return CompletableFuture.completedFuture(GenerationActivationProof.create(
                        operation,
                        subject,
                        0,
                        1,
                        1,
                        new Checksum(ChecksumType.SHA256, "d".repeat(64)),
                        false,
                        true,
                        1_000));
            }

            @Override
            public CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private record Snapshot(
            ObjectKey key,
            VersionedPhysicalObjectRoot root) {
    }

    private record EmptyDomain(String domainId) implements GcReferenceDomain {
        @Override
        public int protocolVersion() {
            return 1;
        }

        @Override
        public CompletableFuture<GcReferenceSnapshot> snapshot(
                GcReferenceQuery query) {
            return CompletableFuture.completedFuture(GcReferenceSnapshot.create(
                    domainId,
                    1,
                    query.queryIdentitySha256(),
                    true,
                    false,
                    1,
                    0,
                    List.of(new GcAuthorityToken(
                            "/authority/" + domainId,
                            1,
                            AUTHORITY_SHA)),
                    List.of()));
        }

        @Override
        public CompletableFuture<Boolean> stillMatches(
                GcReferenceQuery query,
                GcReferenceSnapshot snapshot) {
            return snapshot(query).thenApply(snapshot::equals);
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong millis;

        private MutableClock(long millis) {
            this.millis = new AtomicLong(millis);
        }

        private void advance(Duration duration) {
            millis.addAndGet(duration.toMillis());
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
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public long millis() {
            return millis.get();
        }
    }
}
