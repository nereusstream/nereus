/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.managedledger.cursor.CursorIdentity;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorSnapshotKeys;
import com.nereusstream.metadata.oxia.CursorKeyspace;
import com.nereusstream.metadata.oxia.CursorMetadataDigests;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.FakeCursorMetadataStore;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.HeadObjectOptions;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CursorSnapshotGcScannerTest {
    private static final String CLUSTER = "cluster/a";
    private static final String TOPIC = "persistent://tenant/ns/cursor-gc";
    private static final String OWNER_1 = "00112233445566778899aabbccddeeff";
    private static final String OWNER_2 = "ffeeddccbbaa99887766554433221100";
    private static final String SNAPSHOT_1 = "11111111111111111111111111111111";
    private static final String SNAPSHOT_2 = "22222222222222222222222222222222";
    private static final String PROTECTION_ATTEMPT = "33333333333333333333333333333333";
    private static final Duration ORPHAN_GRACE = Duration.ofHours(25);

    @TempDir
    Path temporaryDirectory;

    @Test
    void emitsExactCandidateAndMarkedRevalidationRejectsCursorOwnerDrift() {
        try (Context context = new Context(temporaryDirectory.resolve("candidate"))) {
            VersionedCursorState cursor = context.createCursor("subscription-a");
            Snapshot snapshot = context.createSnapshot("subscription-a", SNAPSHOT_1);
            context.createProtection(
                    cursor,
                    snapshot,
                    ObjectProtectionType.CURSOR_SNAPSHOT_ROOT,
                    0);
            CursorSnapshotGcScanner scanner = context.scanner(context.configuration(10));
            ArrayList<CursorSnapshotGcScanner.Candidate> visited = new ArrayList<>();

            CursorSnapshotGcScanner.ScanResult result = scanner.scan(
                            context.ledger,
                            candidate -> {
                                visited.add(candidate);
                                return java.util.concurrent.CompletableFuture.completedFuture(null);
                            })
                    .join();

            assertThat(result.listedObjects()).isEqualTo(1);
            assertThat(result.unreferencedObjects()).isEqualTo(1);
            assertThat(result.eligibleCandidates()).isEqualTo(1);
            assertThat(result.visitedCandidates()).isEqualTo(1);
            assertThat(result.protectionBlocked()).isZero();
            assertThat(visited).hasSize(1);
            CursorSnapshotGcScanner.Candidate candidate = visited.get(0);
            assertThat(candidate.plannedProtectionRemovals()).hasSize(1);
            assertThat(candidate.referenceQuery().kind())
                    .isEqualTo(com.nereusstream.core.physical.GcReferenceQueryKind.CURSOR_SNAPSHOT_CANDIDATE);
            assertThat(context.objectStore.headObject(
                            snapshot.key(), new HeadObjectOptions(Duration.ofSeconds(1))).join().key())
                    .isEqualTo(snapshot.key());

            context.mark(snapshot.root());
            assertThat(scanner.revalidate(candidate).join()).isTrue();

            context.changeCursorOwner(cursor);
            assertThat(scanner.revalidate(candidate).join()).isFalse();

            scanner.close();
            assertThatThrownBy(() -> scanner.scan(
                            context.ledger,
                            ignored -> java.util.concurrent.CompletableFuture.completedFuture(null))
                    .join())
                    .satisfies(failure -> assertThat(unwrap(failure))
                            .isInstanceOfSatisfying(NereusException.class, error ->
                                    assertThat(error.code()).isEqualTo(ErrorCode.STORAGE_CLOSED)));
            assertThat(context.physicalStore.getRoot(
                            CLUSTER, candidate.object().objectKeyHash()).join())
                    .isPresent();
        }
    }

    @Test
    void pendingProtectionBlocksUntilExpiryPlusClockSkewIsStrictlyPast() {
        try (Context context = new Context(temporaryDirectory.resolve("pending"))) {
            VersionedCursorState cursor = context.createCursor("subscription-a");
            Snapshot snapshot = context.createSnapshot("subscription-a", SNAPSHOT_1);
            long expiresAt = context.clock.millis() + 1_000;
            context.createProtection(
                    cursor,
                    snapshot,
                    ObjectProtectionType.CURSOR_SNAPSHOT_PENDING,
                    expiresAt);
            CursorSnapshotGcScanner scanner = context.scanner(context.configuration(10));

            CursorSnapshotGcScanner.ScanResult blocked = scanner.scan(
                            context.ledger,
                            ignored -> java.util.concurrent.CompletableFuture.completedFuture(null))
                    .join();

            assertThat(blocked.eligibleCandidates()).isZero();
            assertThat(blocked.protectionBlocked()).isEqualTo(1);

            context.clock.setMillis(expiresAt + Duration.ofSeconds(5).toMillis() + 1);
            ArrayList<CursorSnapshotGcScanner.Candidate> visited = new ArrayList<>();
            CursorSnapshotGcScanner.ScanResult eligible = scanner.scan(
                            context.ledger,
                            candidate -> {
                                visited.add(candidate);
                                return java.util.concurrent.CompletableFuture.completedFuture(null);
                            })
                    .join();

            assertThat(eligible.eligibleCandidates()).isEqualTo(1);
            assertThat(visited).singleElement().satisfies(candidate ->
                    assertThat(candidate.plannedProtectionRemovals()).hasSize(1));
        }
    }

    @Test
    void inventoryLimitFailsClosedInsteadOfReturningATruncatedCandidateSet() {
        try (Context context = new Context(temporaryDirectory.resolve("limit"))) {
            context.createSnapshot("subscription-a", SNAPSHOT_1);
            context.createSnapshot("subscription-a", SNAPSHOT_2);
            CursorSnapshotGcScanner scanner = context.scanner(context.configuration(1));

            assertThatThrownBy(() -> scanner.scan(
                            context.ledger,
                            ignored -> java.util.concurrent.CompletableFuture.completedFuture(null))
                    .join())
                    .satisfies(failure -> assertThat(unwrap(failure))
                            .isInstanceOfSatisfying(NereusException.class, error ->
                                    assertThat(error.code()).isEqualTo(ErrorCode.METADATA_LIMIT_EXCEEDED)));
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

        private Context(Path root) {
            objectStore = new LocalFileObjectStore(root);
            long now = System.currentTimeMillis() + Duration.ofHours(27).toMillis();
            clock = new MutableClock(now);
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
        }

        private VersionedCursorState createCursor(String cursorName) {
            CursorStateRecord value = new CursorStateRecord(
                    0,
                    projection,
                    OWNER_1,
                    cursorName,
                    CursorNames.cursorNameHash(cursorName),
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
                    OptionalLong.empty());
            return cursorStore.createCursor(CLUSTER, value).join();
        }

        private Snapshot createSnapshot(String cursorName, String snapshotId) {
            CursorIdentity identity = new CursorIdentity(
                    ledger,
                    cursorName,
                    CursorNames.cursorNameHash(cursorName),
                    1);
            ObjectKey key = CursorSnapshotKeys.objectKey(
                    CLUSTER, identity, snapshotId);
            byte[] payload = ("snapshot-" + snapshotId).getBytes(StandardCharsets.UTF_8);
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
            long createdAt = clock.millis() - Duration.ofHours(26).toMillis();
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
                    createdAt,
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
                    physicalStore.createRoot(CLUSTER, root).join());
        }

        private void createProtection(
                VersionedCursorState owner,
                Snapshot snapshot,
                ObjectProtectionType type,
                long expiresAtMillis) {
            long createdAt = type == ObjectProtectionType.CURSOR_SNAPSHOT_PENDING
                    ? expiresAtMillis - 2_000
                    : clock.millis() - 2_000;
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

        private VersionedPhysicalObjectRoot mark(VersionedPhysicalObjectRoot active) {
            var value = active.value();
            return physicalStore.compareAndSetRoot(CLUSTER, new PhysicalObjectRootRecord(
                    value.schemaVersion(),
                    value.objectKeyHash(),
                    value.objectKey(),
                    value.objectId(),
                    value.objectKindId(),
                    value.objectLength(),
                    value.storageChecksumType(),
                    value.storageChecksumValue(),
                    value.contentSha256(),
                    value.etag(),
                    PhysicalObjectLifecycle.MARKED,
                    value.lifecycleEpoch() + 1,
                    value.createdAtMillis(),
                    value.orphanNotBeforeMillis(),
                    "b".repeat(52),
                    "a".repeat(64),
                    clock.millis(),
                    clock.millis(),
                    0,
                    0,
                    0,
                    "",
                    "",
                    0), active.metadataVersion()).join();
        }

        private VersionedCursorState changeCursorOwner(VersionedCursorState current) {
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

        private CursorSnapshotGcScanner.Configuration configuration(
                int maxSnapshotObjects) {
            return new CursorSnapshotGcScanner.Configuration(
                    1,
                    1,
                    1,
                    10,
                    maxSnapshotObjects,
                    10,
                    ORPHAN_GRACE,
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(5));
        }

        private CursorSnapshotGcScanner scanner(
                CursorSnapshotGcScanner.Configuration configuration) {
            return new CursorSnapshotGcScanner(
                    CLUSTER,
                    cursorStore,
                    physicalStore,
                    objectStore,
                    configuration,
                    clock,
                    scheduler);
        }

        @Override
        public void close() {
            cursorStore.close();
            physicalStore.close();
            objectStore.close();
            scheduler.shutdownNow();
        }
    }

    private record Snapshot(
            ObjectKey key,
            String snapshotId,
            VersionedPhysicalObjectRoot root) {
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong millis;

        private MutableClock(long millis) {
            this.millis = new AtomicLong(millis);
        }

        private void setMillis(long value) {
            millis.set(value);
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

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
