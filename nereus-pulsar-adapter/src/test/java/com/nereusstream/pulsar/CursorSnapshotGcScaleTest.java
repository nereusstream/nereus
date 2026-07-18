/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
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
import com.nereusstream.materialization.MaterializationSchedulers;
import com.nereusstream.materialization.gc.DefaultGcRetirementJournal;
import com.nereusstream.materialization.gc.GcMetadataRetirementRegistry;
import com.nereusstream.materialization.gc.GcReferenceDomainRegistry;
import com.nereusstream.materialization.gc.PhysicalGcAdvanceStatus;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.materialization.gc.PhysicalGcDeletionStatus;
import com.nereusstream.materialization.gc.PhysicalObjectGarbageCollector;
import com.nereusstream.materialization.gc.SourceRetirementCoordinator;
import com.nereusstream.metadata.oxia.CursorKeyspace;
import com.nereusstream.metadata.oxia.CursorMetadataDigests;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.CursorScanPage;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.FakeCursorMetadataStore;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.WatchRegistration;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ListObjectsResult;
import com.nereusstream.objectstore.ListedObject;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class CursorSnapshotGcScaleTest {
    private static final String CLUSTER = "cluster/a";
    private static final String TOPIC = "persistent://tenant/ns/cursor-gc-scale";
    private static final String OWNER = "00112233445566778899aabbccddeeff";
    private static final String PROTECTION_ATTEMPT = "33333333333333333333333333333333";
    private static final int ROOT_COUNT = 10_000;
    private static final int PAGE_SIZE = 256;
    private static final int FULL_PAGE_COUNT = 40;
    private static final int LIVE_REFERENCE_COUNT = 9_997;
    private static final Checksum STORAGE_CHECKSUM = Crc32cChecksums.checksum(new byte[] {1});
    private static final Checksum AUTHORITY_SHA = new Checksum(
            ChecksumType.SHA256, "a".repeat(64));

    @Test
    void visitsTenThousandCandidatesSequentiallyWithoutStackGrowthOrDeadlineRetention() {
        try (Context context = new Context()) {
            for (int index = 0; index < ROOT_COUNT; index++) {
                context.createSnapshot(cursorName(index), randomId(index + 1));
            }
            AtomicInteger activeVisitors = new AtomicInteger();
            AtomicInteger maximumVisitors = new AtomicInteger();
            AtomicInteger visited = new AtomicInteger();

            CursorSnapshotGcScanner.ScanResult result = context.scanner.scan(
                            context.ledger,
                            candidate -> {
                                int active = activeVisitors.incrementAndGet();
                                maximumVisitors.accumulateAndGet(active, Math::max);
                                visited.incrementAndGet();
                                activeVisitors.decrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            })
                    .join();

            assertThat(result.listedObjects()).isEqualTo(ROOT_COUNT);
            assertThat(result.liveReferences()).isZero();
            assertThat(result.unreferencedObjects()).isEqualTo(ROOT_COUNT);
            assertThat(result.eligibleCandidates()).isEqualTo(ROOT_COUNT);
            assertThat(result.visitedCandidates()).isEqualTo(ROOT_COUNT);
            assertThat(visited).hasValue(ROOT_COUNT);
            assertThat(maximumVisitors).hasValue(1);
            assertThat(context.cursorStore.scanCalls()).isEqualTo(1);
            assertThat(context.objectStore.listCalls()).isEqualTo(FULL_PAGE_COUNT);
            assertThat(context.objectStore.continuationCalls()).isEqualTo(FULL_PAGE_COUNT - 1);
            assertThat(context.objectStore.deleteCalls()).isZero();
            assertThat(context.scheduler.getQueue()).isEmpty();
        }
    }

    @Test
    void classifiesAndDeletesLiveOldCasLostAndDeletedCursorSnapshotsAtTenThousandRoots() {
        try (Context context = new Context()) {
            MixedFixture fixture = context.populateMixedInventory();

            CursorSnapshotGcExecutor.ScanExecutionReport report = context.executor
                    .scan(context.ledger)
                    .join();

            assertThat(report.scan().listedObjects()).isEqualTo(ROOT_COUNT);
            assertThat(report.scan().liveReferences()).isEqualTo(LIVE_REFERENCE_COUNT);
            assertThat(report.scan().unreferencedObjects()).isEqualTo(3);
            assertThat(report.scan().eligibleCandidates()).isEqualTo(3);
            assertThat(report.scan().visitedCandidates()).isEqualTo(3);
            assertThat(report.executions())
                    .extracting(CursorSnapshotGcExecutor.CandidateExecutionResult::object)
                    .containsExactlyInAnyOrderElementsOf(fixture.candidates().stream()
                            .map(snapshot -> ObjectKeyHash.from(snapshot.key()))
                            .toList());
            assertThat(report.executions()).allSatisfy(execution -> {
                assertThat(execution.mark()).isPresent();
                assertThat(execution.advance().orElseThrow().status())
                        .isEqualTo(PhysicalGcAdvanceStatus.WAITING_FOR_GRACE);
                assertThat(execution.deletion()).isEmpty();
            });
            assertThat(context.cursorStore.scanCalls()).isEqualTo(FULL_PAGE_COUNT);
            assertThat(context.cursorStore.continuationCalls()).isEqualTo(FULL_PAGE_COUNT - 1);
            assertThat(context.objectStore.listCalls()).isEqualTo(FULL_PAGE_COUNT);
            assertThat(context.objectStore.continuationCalls()).isEqualTo(FULL_PAGE_COUNT - 1);
            assertThat(context.scheduler.getQueue()).isEmpty();

            context.clock.advance(Duration.ofSeconds(32));
            for (Snapshot candidate : fixture.candidates()) {
                VersionedPhysicalObjectRoot marked = context.currentRoot(candidate);
                assertThat(marked.value().lifecycle()).isEqualTo(PhysicalObjectLifecycle.MARKED);

                CursorSnapshotGcExecutor.CandidateExecutionResult recovered = context.executor
                        .recoverMarked(context.ledger, marked)
                        .join();

                assertThat(recovered.advance().orElseThrow().status())
                        .isEqualTo(PhysicalGcAdvanceStatus.DELETE_INTENT);
                assertThat(recovered.deletion().orElseThrow().status())
                        .isEqualTo(PhysicalGcDeletionStatus.DELETED);
                assertThat(context.currentRoot(candidate).value().lifecycle())
                        .isEqualTo(PhysicalObjectLifecycle.DELETED);
            }

            assertThat(context.objectStore.size()).isEqualTo(LIVE_REFERENCE_COUNT);
            assertThat(context.objectStore.deleteCalls()).isEqualTo(3);
            assertThat(context.objectStore.deletedKeys())
                    .containsExactlyInAnyOrderElementsOf(fixture.candidates().stream()
                            .map(Snapshot::key)
                            .toList());
            assertThat(context.objectStore.contains(fixture.current())).isTrue();
            assertThat(context.currentRoot(fixture.current()).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
            assertThat(context.physicalStore.scanProtections(
                            CLUSTER,
                            ObjectKeyHash.from(fixture.current().key()),
                            Optional.empty(),
                            PAGE_SIZE)
                    .join().values())
                    .hasSize(1);
            assertThat(context.cursorStore.scanCalls()).isGreaterThan(FULL_PAGE_COUNT);
            assertThat(context.objectStore.listCalls()).isGreaterThan(FULL_PAGE_COUNT);
            assertThat(context.scheduler.getQueue()).isEmpty();
        }
    }

    private static final class Context implements AutoCloseable {
        private final CountingCursorMetadataStore cursorStore =
                new CountingCursorMetadataStore(new FakeCursorMetadataStore());
        private final FakePhysicalObjectMetadataStore physicalStore =
                new FakePhysicalObjectMetadataStore();
        private final ScaleObjectStore objectStore = new ScaleObjectStore();
        private final ScheduledThreadPoolExecutor scheduler =
                MaterializationSchedulers.newSingleThreadScheduler(runnable -> {
                    Thread thread = new Thread(runnable, "cursor-gc-scale-deadline");
                    thread.setDaemon(true);
                    return thread;
                });
        private final MutableClock clock = new MutableClock(
                Instant.parse("2035-01-02T00:00:00Z").toEpochMilli());
        private final ManagedLedgerProjectionIdentity projection;
        private final CursorLedgerIdentity ledger;
        private final CursorSnapshotGcScanner scanner;
        private final CursorSnapshotGcExecutor executor;

        private Context() {
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
                    OWNER,
                    CursorRetentionLifecycle.ACTIVE,
                    1,
                    0,
                    0,
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    Optional.empty(),
                    0)).join();
            PhysicalGcConfig gcConfig = enabledConfig();
            scanner = new CursorSnapshotGcScanner(
                    CLUSTER,
                    cursorStore,
                    physicalStore,
                    objectStore,
                    new CursorSnapshotGcScanner.Configuration(
                            PAGE_SIZE,
                            PAGE_SIZE,
                            PAGE_SIZE,
                            ROOT_COUNT,
                            ROOT_COUNT,
                            ROOT_COUNT,
                            gcConfig.orphanGrace(),
                            gcConfig.maximumClockSkew(),
                            gcConfig.operationTimeout()),
                    clock,
                    scheduler);
            GcReferenceDomainRegistry domains = new GcReferenceDomainRegistry(
                    gcConfig,
                    scheduler,
                    List.of(
                            new EmptyDomain("generation-v1"),
                            new EmptyDomain("projection-generation-v1")));
            DefaultGcRetirementJournal journal = new DefaultGcRetirementJournal(
                    CLUSTER, physicalStore, gcConfig);
            PhysicalObjectGarbageCollector collector = new PhysicalObjectGarbageCollector(
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
            AtomicInteger candidateId = new AtomicInteger();
            executor = new CursorSnapshotGcExecutor(
                    gcConfig,
                    scanner,
                    domains,
                    collector,
                    retirement,
                    () -> Character.toString('c' + candidateId.getAndIncrement()).repeat(52));
        }

        private MixedFixture populateMixedInventory() {
            Snapshot old = createSnapshot(cursorName(0), randomId(20_001));
            VersionedCursorState oldOwner = createActiveCursor(
                    cursorName(0), Optional.of(old.reference(1)), 1);
            createProtection(oldOwner, old, ObjectProtectionType.CURSOR_SNAPSHOT_ROOT, 0);
            Snapshot current = createSnapshot(cursorName(0), randomId(20_002));
            VersionedCursorState currentOwner = replaceSnapshot(oldOwner, current);
            createProtection(currentOwner, current, ObjectProtectionType.CURSOR_SNAPSHOT_ROOT, 0);

            Snapshot casLost = createSnapshot(cursorName(1), randomId(20_003));
            VersionedCursorState casOwner = createActiveCursor(cursorName(1), Optional.empty(), 1);
            createProtection(
                    casOwner,
                    casLost,
                    ObjectProtectionType.CURSOR_SNAPSHOT_PENDING,
                    clock.millis() - Duration.ofSeconds(10).toMillis());

            Snapshot deleted = createSnapshot(cursorName(2), randomId(20_004));
            VersionedCursorState deletedOwner = createActiveCursor(
                    cursorName(2), Optional.of(deleted.reference(1)), 1);
            createProtection(deletedOwner, deleted, ObjectProtectionType.CURSOR_SNAPSHOT_ROOT, 0);
            deleteCursor(deletedOwner);

            createActiveCursor(cursorName(3), Optional.empty(), 1);
            for (int index = 4; index < ROOT_COUNT; index++) {
                Snapshot live = createSnapshot(cursorName(index), randomId(index + 1));
                VersionedCursorState owner = createActiveCursor(
                        cursorName(index), Optional.of(live.reference(1)), 1);
                createProtection(owner, live, ObjectProtectionType.CURSOR_SNAPSHOT_ROOT, 0);
            }

            assertThat(objectStore.size()).isEqualTo(ROOT_COUNT);
            return new MixedFixture(current, List.of(old, casLost, deleted));
        }

        private Snapshot createSnapshot(String cursorName, String snapshotId) {
            CursorIdentity identity = new CursorIdentity(
                    ledger,
                    cursorName,
                    CursorNames.cursorNameHash(cursorName),
                    1);
            ObjectKey key = CursorSnapshotKeys.objectKey(CLUSTER, identity, snapshotId);
            String etag = "etag-" + snapshotId;
            Instant lastModified = clock.instant().minus(Duration.ofHours(26));
            objectStore.add(key, etag, lastModified);
            PhysicalObjectRootRecord root = new PhysicalObjectRootRecord(
                    1,
                    ObjectKeyHash.from(key).value(),
                    key.value(),
                    "",
                    PhysicalObjectKind.CURSOR_SNAPSHOT.wireId(),
                    1,
                    STORAGE_CHECKSUM.type().name(),
                    STORAGE_CHECKSUM.value(),
                    "",
                    etag,
                    PhysicalObjectLifecycle.ACTIVE,
                    1,
                    lastModified.toEpochMilli(),
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
            return new Snapshot(
                    key,
                    snapshotId,
                    lastModified.toEpochMilli(),
                    physicalStore.createRoot(CLUSTER, root).join());
        }

        private VersionedCursorState createActiveCursor(
                String cursorName,
                Optional<CursorSnapshotReferenceRecord> reference,
                long mutationSequence) {
            return cursorStore.createCursor(CLUSTER, cursorRecord(
                    cursorName,
                    CursorRecordLifecycle.ACTIVE,
                    mutationSequence,
                    reference,
                    OptionalLong.empty())).join();
        }

        private VersionedCursorState replaceSnapshot(
                VersionedCursorState current,
                Snapshot replacement) {
            long nextSequence = current.value().mutationSequence() + 1;
            return cursorStore.compareAndSetCursor(
                    CLUSTER,
                    cursorRecord(
                            current.value().cursorName(),
                            CursorRecordLifecycle.ACTIVE,
                            nextSequence,
                            Optional.of(replacement.reference(nextSequence)),
                            OptionalLong.empty()),
                    current.metadataVersion()).join();
        }

        private VersionedCursorState deleteCursor(VersionedCursorState current) {
            long nextSequence = current.value().mutationSequence() + 1;
            return cursorStore.compareAndSetCursor(
                    CLUSTER,
                    cursorRecord(
                            current.value().cursorName(),
                            CursorRecordLifecycle.DELETED,
                            nextSequence,
                            Optional.empty(),
                            OptionalLong.of(clock.millis() - 1)),
                    current.metadataVersion()).join();
        }

        private CursorStateRecord cursorRecord(
                String cursorName,
                CursorRecordLifecycle lifecycle,
                long mutationSequence,
                Optional<CursorSnapshotReferenceRecord> reference,
                OptionalLong deletedAtMillis) {
            long createdAt = clock.millis() - Duration.ofDays(2).toMillis();
            long updatedAt = createdAt + mutationSequence;
            return new CursorStateRecord(
                    0,
                    projection,
                    OWNER,
                    cursorName,
                    CursorNames.cursorNameHash(cursorName),
                    1,
                    lifecycle,
                    mutationSequence,
                    1,
                    PROTECTION_ATTEMPT,
                    0,
                    reference,
                    List.of(),
                    List.of(),
                    Map.of(),
                    Map.of(),
                    createdAt,
                    updatedAt,
                    deletedAtMillis);
        }

        private void createProtection(
                VersionedCursorState owner,
                Snapshot snapshot,
                ObjectProtectionType type,
                long expiresAtMillis) {
            long createdAt = type == ObjectProtectionType.CURSOR_SNAPSHOT_PENDING
                    ? expiresAtMillis - 1_000
                    : snapshot.createdAtMillis();
            physicalStore.createProtection(CLUSTER, new ObjectProtectionRecord(
                    1,
                    snapshot.root().value().objectKeyHash(),
                    type.wireId(),
                    snapshot.snapshotId(),
                    new CursorKeyspace(CLUSTER).cursorStateKey(
                            new StreamId(projection.streamId()),
                            owner.value().cursorName()),
                    owner.metadataVersion(),
                    CursorMetadataDigests.durableValueSha256(owner.value()).value(),
                    snapshot.root().value().lifecycleEpoch(),
                    createdAt,
                    expiresAtMillis,
                    0)).join();
        }

        private VersionedPhysicalObjectRoot currentRoot(Snapshot snapshot) {
            return physicalStore.getRoot(CLUSTER, ObjectKeyHash.from(snapshot.key()))
                    .join().orElseThrow();
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
                PAGE_SIZE,
                PAGE_SIZE,
                1,
                10,
                10,
                10,
                Duration.ofSeconds(1),
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Duration.ofSeconds(1),
                Duration.ofSeconds(31),
                Duration.ofHours(1),
                Duration.ofHours(25),
                Duration.ofDays(7),
                Duration.ofSeconds(20),
                Duration.ofSeconds(20));
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
            String snapshotId,
            long createdAtMillis,
            VersionedPhysicalObjectRoot root) {
        private CursorSnapshotReferenceRecord reference(long sourceMutationSequence) {
            return new CursorSnapshotReferenceRecord(
                    key.value(),
                    snapshotId,
                    1,
                    sourceMutationSequence,
                    0,
                    1,
                    STORAGE_CHECKSUM.type().name(),
                    STORAGE_CHECKSUM.value(),
                    0,
                    1,
                    createdAtMillis);
        }
    }

    private record MixedFixture(Snapshot current, List<Snapshot> candidates) {
        private MixedFixture {
            candidates = List.copyOf(candidates);
        }
    }

    private record EmptyDomain(String domainId) implements GcReferenceDomain {
        @Override
        public int protocolVersion() {
            return 1;
        }

        @Override
        public CompletableFuture<GcReferenceSnapshot> snapshot(GcReferenceQuery query) {
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

    private static final class CountingCursorMetadataStore implements CursorMetadataStore {
        private final CursorMetadataStore delegate;
        private final AtomicInteger scanCalls = new AtomicInteger();
        private final AtomicInteger continuationCalls = new AtomicInteger();

        private CountingCursorMetadataStore(CursorMetadataStore delegate) {
            this.delegate = delegate;
        }

        private int scanCalls() {
            return scanCalls.get();
        }

        private int continuationCalls() {
            return continuationCalls.get();
        }

        @Override
        public CompletableFuture<Optional<VersionedCursorState>> getCursor(
                String cluster, StreamId streamId, String cursorName) {
            return delegate.getCursor(cluster, streamId, cursorName);
        }

        @Override
        public CompletableFuture<VersionedCursorState> createCursor(
                String cluster, CursorStateRecord value) {
            return delegate.createCursor(cluster, value);
        }

        @Override
        public CompletableFuture<VersionedCursorState> compareAndSetCursor(
                String cluster,
                CursorStateRecord value,
                long expectedMetadataVersion) {
            return delegate.compareAndSetCursor(cluster, value, expectedMetadataVersion);
        }

        @Override
        public CompletableFuture<CursorScanPage> scanCursors(
                String cluster,
                StreamId streamId,
                Optional<CursorScanToken> continuation,
                int pageSize) {
            scanCalls.incrementAndGet();
            if (continuation.isPresent()) {
                continuationCalls.incrementAndGet();
            }
            return delegate.scanCursors(cluster, streamId, continuation, pageSize);
        }

        @Override
        public CompletableFuture<Optional<VersionedCursorRetention>> getRetention(
                String cluster, StreamId streamId) {
            return delegate.getRetention(cluster, streamId);
        }

        @Override
        public CompletableFuture<VersionedCursorRetention> createRetention(
                String cluster, CursorRetentionRecord value) {
            return delegate.createRetention(cluster, value);
        }

        @Override
        public CompletableFuture<VersionedCursorRetention> compareAndSetRetention(
                String cluster,
                CursorRetentionRecord value,
                long expectedMetadataVersion) {
            return delegate.compareAndSetRetention(cluster, value, expectedMetadataVersion);
        }

        @Override
        public WatchRegistration watchStreamCursors(
                String cluster, StreamId streamId, Runnable invalidation) {
            return delegate.watchStreamCursors(cluster, streamId, invalidation);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class ScaleObjectStore implements ObjectStore {
        private final NavigableMap<String, StoredObject> objects = new TreeMap<>();
        private final Set<ObjectKey> deletedKeys = new HashSet<>();
        private final AtomicInteger listCalls = new AtomicInteger();
        private final AtomicInteger continuationCalls = new AtomicInteger();
        private final AtomicInteger deleteCalls = new AtomicInteger();
        private boolean closed;

        private synchronized void add(
                ObjectKey key,
                String etag,
                Instant lastModified) {
            ensureOpen();
            StoredObject prior = objects.putIfAbsent(
                    key.value(), new StoredObject(key, etag, lastModified));
            if (prior != null) {
                throw new AssertionError("duplicate scale object: " + key.value());
            }
        }

        private synchronized int size() {
            return objects.size();
        }

        private synchronized boolean contains(Snapshot snapshot) {
            return objects.containsKey(snapshot.key().value());
        }

        private int listCalls() {
            return listCalls.get();
        }

        private int continuationCalls() {
            return continuationCalls.get();
        }

        private int deleteCalls() {
            return deleteCalls.get();
        }

        private synchronized Set<ObjectKey> deletedKeys() {
            return Set.copyOf(deletedKeys);
        }

        @Override
        public synchronized CompletableFuture<ListObjectsResult> listObjects(
                ObjectKeyPrefix prefix,
                Optional<String> continuationToken,
                ListObjectsOptions options) {
            ensureOpen();
            listCalls.incrementAndGet();
            if (continuationToken.isPresent()) {
                continuationCalls.incrementAndGet();
            }
            NavigableMap<String, StoredObject> remaining = continuationToken
                    .map(token -> objects.tailMap(token, false))
                    .orElse(objects);
            ArrayList<ListedObject> page = new ArrayList<>(options.maxKeys());
            boolean hasMore = false;
            for (StoredObject stored : remaining.values()) {
                if (!stored.key().value().startsWith(prefix.value())) {
                    if (!page.isEmpty()) {
                        break;
                    }
                    continue;
                }
                if (page.size() == options.maxKeys()) {
                    hasMore = true;
                    break;
                }
                page.add(stored.listed());
            }
            Optional<String> next = hasMore
                    ? Optional.of(page.get(page.size() - 1).key().value())
                    : Optional.empty();
            return CompletableFuture.completedFuture(
                    new ListObjectsResult(prefix, page, next));
        }

        @Override
        public synchronized CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key,
                HeadObjectOptions options) {
            ensureOpen();
            StoredObject stored = objects.get(key.value());
            if (stored == null) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_NOT_FOUND, true, "scale object not found"));
            }
            return CompletableFuture.completedFuture(stored.head());
        }

        @Override
        public synchronized CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key,
                DeleteObjectOptions options) {
            ensureOpen();
            deleteCalls.incrementAndGet();
            StoredObject stored = objects.get(key.value());
            if (stored == null) {
                return CompletableFuture.completedFuture(new DeleteObjectResult(
                        key, DeleteObjectResult.Status.ALREADY_ABSENT));
            }
            if (options.expectedLength() != 1
                    || !options.expectedStorageChecksum().equals(STORAGE_CHECKSUM)
                    || !options.expectedEtag().equals(Optional.of(stored.etag()))) {
                return CompletableFuture.failedFuture(new AssertionError(
                        "cursor GC delete did not carry the exact scale-object identity"));
            }
            objects.remove(key.value());
            deletedKeys.add(key);
            return CompletableFuture.completedFuture(new DeleteObjectResult(
                    key, DeleteObjectResult.Status.DELETED));
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key,
                long offset,
                long length,
                RangeReadOptions options) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public synchronized void close() {
            closed = true;
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("scale object store is closed");
            }
        }
    }

    private record StoredObject(ObjectKey key, String etag, Instant lastModified) {
        private ListedObject listed() {
            return new ListedObject(
                    key, 1, Optional.of(etag), Optional.of(lastModified));
        }

        private HeadObjectResult head() {
            return new HeadObjectResult(
                    key, 1, STORAGE_CHECKSUM, Optional.of(etag), Map.of());
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

    private static String cursorName(int index) {
        return "subscription-" + String.format("%05d", index);
    }

    private static String randomId(int value) {
        return String.format("%032x", value);
    }
}
