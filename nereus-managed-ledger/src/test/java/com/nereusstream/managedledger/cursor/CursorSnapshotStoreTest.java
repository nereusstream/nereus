/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.FakeCursorMetadataStore;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectAlreadyExistsException;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectAttemptGuard;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class CursorSnapshotStoreTest {
    private static final String SECOND_SNAPSHOT = "00110011001100110011001100110011";
    private static final String OWNER = "0123456789abcdef0123456789abcdef";
    private static final String PROTECTION_ATTEMPT = "abcdef0123456789abcdef0123456789";
    private static final String PROCESS_RUN_ID = "r".repeat(26);
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(105), ZoneOffset.UTC);
    private static final Duration PENDING_PROTECTION_DURATION = Duration.ofMinutes(5);

    @Test
    void publishesPendingThenPermanentProtectionAndReadsOnlyUnderDurableLease() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        try (Harness harness = new Harness(
                objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
            CursorSnapshotPublication publication = harness.prepare();
            ObjectProtectionIdentity pending = protection(
                    publication, ObjectProtectionType.CURSOR_SNAPSHOT_PENDING);
            ObjectProtectionIdentity permanent = protection(
                    publication, ObjectProtectionType.CURSOR_SNAPSHOT_ROOT);

            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER, pending)).isPresent();
            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER, permanent)).isEmpty();

            CursorSnapshotReference reference = harness.publish(publication);
            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER, pending)).isEmpty();
            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER, permanent)).isPresent();

            objectStore.events.clear();
            CursorAckState recovered = harness.store.read(
                    reference, CursorTestSamples.identity()).join();

            assertThat(recovered).isEqualTo(CursorTestSamples.complexState());
            assertThat(reference.objectKey().value()).isEqualTo(
                    KeyComponentCodec.encodeComponent(CursorTestSamples.CLUSTER)
                            + "/cursor-snapshots/v1/"
                            + KeyComponentCodec.encodeComponent(
                                    CursorTestSamples.identity()
                                            .ledger()
                                            .projection()
                                            .streamId())
                            + "/"
                            + CursorTestSamples.identity().cursorNameHash()
                            + "/0000000000000000001/"
                            + CursorTestSamples.SNAPSHOT
                            + ".ncs");
            assertThat(objectStore.lastPutOptions.ifAbsent()).isTrue();
            assertThat(objectStore.lastPutOptions.contentType())
                    .isEqualTo("application/vnd.nereus.cursor-snapshot-v1");
            assertThat(objectStore.lastPutOptions.metadata())
                    .containsExactlyInAnyOrderEntriesOf(Map.of(
                            "nereus-format", "NCS1",
                            "nereus-object-type", "CURSOR_SNAPSHOT_OBJECT",
                            "nereus-snapshot-id", CursorTestSamples.SNAPSHOT));
            assertThat(objectStore.events).containsExactly(
                    "head",
                    "pin-acquire",
                    "pin-acquired",
                    "head",
                    "read",
                    "pin-release",
                    "pin-released");
            assertThat(harness.physicalStore.readerLease(
                    CursorTestSamples.CLUSTER,
                    publication.object().objectKeyHash(),
                    PROCESS_RUN_ID)).isEmpty();
            assertThat(objectStore.putCalls).isEqualTo(1);
            assertThat(objectStore.headCalls).isEqualTo(3);
            assertThat(objectStore.readCalls).isEqualTo(1);

            harness.store.close();
            assertThat(objectStore.closed).isFalse();
        }
    }

    @Test
    void retriesIfAbsentCollisionWithFreshRandomIdAndNeverOverwritesBytes() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        try (Harness harness = new Harness(
                objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
            CursorSnapshotPublication original = harness.prepare();
            DefaultCursorSnapshotStore retrying = harness.newStore(new ArrayDeque<>(
                    List.of(CursorTestSamples.SNAPSHOT, SECOND_SNAPSHOT)));

            CursorSnapshotPublication retried = retrying.prepareWrite(
                    CursorTestSamples.request(), harness.authority()).join();

            assertThat(retried.reference().snapshotId()).isEqualTo(SECOND_SNAPSHOT);
            assertThat(retried.reference().objectKey())
                    .isNotEqualTo(original.reference().objectKey());
            assertThat(objectStore.objects).hasSize(2);
            assertThat(objectStore.putCalls).isEqualTo(3);
        }
    }

    @Test
    void preparedSnapshotWithoutCursorCasKeepsOnlyBoundedPendingProtection() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        try (Harness harness = new Harness(
                objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
            CursorSnapshotPublication publication = harness.prepare();

            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER,
                    protection(publication, ObjectProtectionType.CURSOR_SNAPSHOT_PENDING)))
                    .isPresent();
            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER,
                    protection(publication, ObjectProtectionType.CURSOR_SNAPSHOT_ROOT)))
                    .isEmpty();
            assertThat(harness.cursorStore.getCursor(
                            CursorTestSamples.CLUSTER,
                            new com.nereusstream.api.StreamId(CursorTestSamples.identity()
                                    .ledger()
                                    .projection()
                                    .streamId()),
                            CursorTestSamples.CURSOR)
                    .join()
                    .orElseThrow())
                    .isEqualTo(harness.currentRoot);
        }
    }

    @Test
    void readReconcilesPermanentProtectionAfterCursorCasResponseLoss() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        try (Harness harness = new Harness(
                objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
            CursorSnapshotPublication publication = harness.prepare();
            CursorSnapshotReference reference =
                    harness.publishRootWithoutCompleting(publication);
            ObjectProtectionIdentity pending = protection(
                    publication, ObjectProtectionType.CURSOR_SNAPSHOT_PENDING);
            ObjectProtectionIdentity permanent = protection(
                    publication, ObjectProtectionType.CURSOR_SNAPSHOT_ROOT);

            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER, pending)).isPresent();
            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER, permanent)).isEmpty();

            assertThat(harness.store.read(
                    reference, CursorTestSamples.identity()).join())
                    .isEqualTo(CursorTestSamples.complexState());

            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER, pending)).isPresent();
            assertThat(harness.physicalStore.protection(
                    CursorTestSamples.CLUSTER, permanent)).isPresent();
        }
    }

    @Test
    void guardedPutRejectsAChangedCursorAuthorityBeforeUploadingBytes() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        try (Harness harness = new Harness(
                objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
            CursorSnapshotWriteAuthority stale = harness.authority();
            harness.advanceWithoutSnapshot();

            assertThatThrownBy(() -> harness.store.prepareWrite(
                            CursorTestSamples.request(), stale).join())
                    .satisfies(error -> assertThat(rootCause(error))
                            .isInstanceOfSatisfying(NereusException.class, failure ->
                                    assertThat(failure.code())
                                            .isEqualTo(ErrorCode.METADATA_CONDITION_FAILED)));
            assertThat(objectStore.putCalls).isZero();
            assertThat(harness.physicalStore.getRoot(
                            CursorTestSamples.CLUSTER,
                            com.nereusstream.api.ObjectKeyHash.from(
                                    CursorSnapshotKeys.objectKey(
                                            CursorTestSamples.CLUSTER,
                                            CursorTestSamples.identity(),
                                            CursorTestSamples.SNAPSHOT)))
                    .join())
                    .isEmpty();
        }
    }

    @Test
    void stableWrongHeadMetadataFailsWithoutReadingObjectBytes() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        try (Harness harness = new Harness(
                objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
            CursorSnapshotReference reference = harness.writeAndPublish();
            objectStore.headMutation = HeadMutation.WRONG_METADATA;

            assertThatThrownBy(() -> harness.store.read(
                            reference, CursorTestSamples.identity()).join())
                    .satisfies(error -> assertThat(rootCause(error))
                            .isInstanceOfSatisfying(NereusException.class, failure ->
                                    assertThat(failure.code())
                                            .isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH)));
            assertThat(objectStore.readCalls).isZero();
        }
    }

    @Test
    void everyHeadIdentityMismatchFailsBeforeReadingObjectBytes() {
        for (HeadMutation mutation : List.of(
                HeadMutation.WRONG_KEY,
                HeadMutation.WRONG_LENGTH,
                HeadMutation.WRONG_CHECKSUM)) {
            RecordingObjectStore objectStore = new RecordingObjectStore();
            try (Harness harness = new Harness(
                    objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
                CursorSnapshotReference reference = harness.writeAndPublish();
                objectStore.headMutation = mutation;

                assertThatThrownBy(() -> harness.store.read(
                                reference, CursorTestSamples.identity()).join())
                        .satisfies(error -> assertThat(rootCause(error))
                                .isInstanceOfSatisfying(NereusException.class, failure ->
                                        assertThat(failure.code())
                                                .isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH)));
                assertThat(objectStore.readCalls).isZero();
            }
        }
    }

    @Test
    void everyRangeResultMismatchFailsBeforeSnapshotDecodeAndStillReleasesLease() {
        for (ReadMutation mutation : ReadMutation.values()) {
            if (mutation == ReadMutation.NONE) {
                continue;
            }
            RecordingObjectStore objectStore = new RecordingObjectStore();
            try (Harness harness = new Harness(
                    objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
                CursorSnapshotPublication publication = harness.prepare();
                CursorSnapshotReference reference = harness.publish(publication);
                objectStore.readMutation = mutation;

                assertThatThrownBy(() -> harness.store.read(
                                reference, CursorTestSamples.identity()).join())
                        .satisfies(error -> assertThat(rootCause(error))
                                .isInstanceOfSatisfying(NereusException.class, failure ->
                                        assertThat(failure.code())
                                                .isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH)));
                assertThat(objectStore.readCalls).isEqualTo(1);
                assertThat(harness.physicalStore.readerLease(
                        CursorTestSamples.CLUSTER,
                        publication.object().objectKeyHash(),
                        PROCESS_RUN_ID)).isEmpty();
            }
        }
    }

    @Test
    void nonCollisionPutResultMismatchIsNotRetriedAsFreshSnapshotId() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        objectStore.returnWrongPutLength = true;
        try (Harness harness = new Harness(objectStore, new ArrayDeque<>(List.of(
                CursorTestSamples.SNAPSHOT, SECOND_SNAPSHOT)))) {
            assertThatThrownBy(() -> harness.prepare())
                    .satisfies(error -> assertThat(rootCause(error))
                            .isInstanceOfSatisfying(NereusException.class, failure ->
                                    assertThat(failure.code())
                                            .isEqualTo(ErrorCode.OBJECT_UPLOAD_FAILED)));
            assertThat(objectStore.putCalls).isEqualTo(1);
        }
    }

    @Test
    void mismatchedLogicalObjectKeyFailsBeforeIssuingObjectStoreIo() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        try (Harness harness = new Harness(
                objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
            CursorSnapshotReference reference = harness.writeAndPublish();
            int headsAfterWrite = objectStore.headCalls;
            CursorSnapshotReference wrongKey = new CursorSnapshotReference(
                    new ObjectKey("wrong/key"),
                    reference.snapshotId(),
                    reference.cursorGeneration(),
                    reference.sourceMutationSequence(),
                    reference.baseMarkDeleteOffset(),
                    reference.objectLength(),
                    reference.storageChecksum(),
                    reference.formatCrc32c(),
                    reference.formatVersion(),
                    reference.createdAtMillis());

            assertThatThrownBy(() -> harness.store.read(
                            wrongKey, CursorTestSamples.identity()).join())
                    .satisfies(error -> assertThat(rootCause(error))
                            .isInstanceOf(
                                    CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class));
            assertThat(objectStore.headCalls).isEqualTo(headsAfterWrite);
            assertThat(objectStore.readCalls).isZero();
        }
    }

    @Test
    void closeIsIdempotentAndRejectsNewOperationsWithoutClosingBorrowedResources() {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        try (Harness harness = new Harness(
                objectStore, new ArrayDeque<>(List.of(CursorTestSamples.SNAPSHOT)))) {
            harness.store.close();
            harness.store.close();

            assertThatThrownBy(() -> harness.store.prepareWrite(
                            CursorTestSamples.request(), harness.authority()).join())
                    .satisfies(error -> assertThat(rootCause(error))
                            .isInstanceOfSatisfying(NereusException.class, failure ->
                                    assertThat(failure.code())
                                            .isEqualTo(ErrorCode.STORAGE_CLOSED)));
            assertThat(objectStore.closed).isFalse();
            assertThat(harness.cursorStore.getCursor(
                            CursorTestSamples.CLUSTER,
                            new com.nereusstream.api.StreamId(CursorTestSamples.identity()
                                    .ledger()
                                    .projection()
                                    .streamId()),
                            CursorTestSamples.CURSOR)
                    .join())
                    .isPresent();
        }
    }

    private static ObjectProtectionIdentity protection(
            CursorSnapshotPublication publication,
            ObjectProtectionType type) {
        return new ObjectProtectionIdentity(
                publication.object().objectKeyHash(),
                type,
                publication.reference().snapshotId());
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class Harness implements AutoCloseable {
        private final RecordingObjectStore objectStore;
        private final FakeCursorMetadataStore cursorStore = new FakeCursorMetadataStore();
        private final FakePhysicalObjectMetadataStore physicalStore =
                new FakePhysicalObjectMetadataStore();
        private final ObjectProtectionManager protections =
                new DefaultObjectProtectionManager(
                        CursorTestSamples.CLUSTER,
                        physicalStore,
                        PENDING_PROTECTION_DURATION,
                        Duration.ofSeconds(1),
                        Duration.ofHours(1),
                        CLOCK);
        private final RecordingReadPinManager readPins;
        private final CursorStorageConfig config = CursorStorageConfig.defaults();
        private final CursorStatePersistencePlanner planner =
                new CursorStatePersistencePlanner(CursorTestSamples.CLUSTER, config);
        private final List<DefaultCursorSnapshotStore> stores = new ArrayList<>();
        private VersionedCursorState currentRoot;
        private DefaultCursorSnapshotStore store;

        private Harness(
                RecordingObjectStore objectStore,
                ArrayDeque<String> snapshotIds) {
            this.objectStore = objectStore;
            this.readPins = new RecordingReadPinManager(
                    new DefaultObjectReadPinManager(
                            CursorTestSamples.CLUSTER,
                            PROCESS_RUN_ID,
                            physicalStore,
                            Duration.ofMinutes(2),
                            Duration.ofSeconds(1),
                            Duration.ofHours(1),
                            CLOCK),
                    objectStore.events);
            CursorState initial = state(
                    7,
                    7,
                    CursorAckState.empty(10),
                    104,
                    0);
            currentRoot = cursorStore.createCursor(
                    CursorTestSamples.CLUSTER,
                    planner.recordWithoutSnapshot(initial)).join();
            store = newStore(snapshotIds);
        }

        private DefaultCursorSnapshotStore newStore(ArrayDeque<String> snapshotIds) {
            DefaultCursorSnapshotStore created = new DefaultCursorSnapshotStore(
                    CursorTestSamples.CLUSTER,
                    objectStore,
                    cursorStore,
                    physicalStore,
                    protections,
                    readPins,
                    config,
                    Duration.ofSeconds(10),
                    PENDING_PROTECTION_DURATION,
                    CLOCK,
                    snapshotIds::removeFirst,
                    System::nanoTime);
            stores.add(created);
            return created;
        }

        private CursorSnapshotWriteAuthority authority() {
            return new CursorSnapshotWriteAuthority(currentRoot, OWNER, 8);
        }

        private CursorSnapshotPublication prepare() {
            return store.prepareWrite(
                    CursorTestSamples.request(), authority()).join();
        }

        private CursorSnapshotReference publish(
                CursorSnapshotPublication publication) {
            CursorSnapshotReference reference =
                    publishRootWithoutCompleting(publication);
            store.completeWrite(publication, currentRoot).join();
            return reference;
        }

        private CursorSnapshotReference publishRootWithoutCompleting(
                CursorSnapshotPublication publication) {
            CursorState candidate = state(
                    8,
                    8,
                    CursorTestSamples.complexState(),
                    CursorTestSamples.request().createdAtMillis(),
                    currentRoot.metadataVersion());
            currentRoot = cursorStore.compareAndSetCursor(
                    CursorTestSamples.CLUSTER,
                    planner.afterSnapshot(candidate, publication.reference()),
                    currentRoot.metadataVersion()).join();
            return publication.reference();
        }

        private CursorSnapshotReference writeAndPublish() {
            return publish(prepare());
        }

        private void advanceWithoutSnapshot() {
            CursorState candidate = state(
                    8,
                    8,
                    CursorTestSamples.complexState(),
                    CursorTestSamples.request().createdAtMillis(),
                    currentRoot.metadataVersion());
            currentRoot = cursorStore.compareAndSetCursor(
                    CursorTestSamples.CLUSTER,
                    planner.recordWithoutSnapshot(candidate),
                    currentRoot.metadataVersion()).join();
        }

        private static CursorState state(
                long mutationSequence,
                long ackStateEpoch,
                CursorAckState acknowledgements,
                long updatedAtMillis,
                long metadataVersion) {
            return new CursorState(
                    CursorTestSamples.identity(),
                    OWNER,
                    CursorLifecycle.ACTIVE,
                    mutationSequence,
                    ackStateEpoch,
                    PROTECTION_ATTEMPT,
                    acknowledgements,
                    Map.of(),
                    Map.of(),
                    Optional.empty(),
                    100,
                    updatedAtMillis,
                    metadataVersion);
        }

        @Override
        public void close() {
            stores.forEach(DefaultCursorSnapshotStore::close);
            readPins.close();
            protections.close();
            cursorStore.close();
            physicalStore.close();
        }
    }

    private static final class RecordingReadPinManager implements ObjectReadPinManager {
        private final ObjectReadPinManager delegate;
        private final List<String> events;

        private RecordingReadPinManager(
                ObjectReadPinManager delegate,
                List<String> events) {
            this.delegate = delegate;
            this.events = events;
        }

        @Override
        public CompletableFuture<ObjectReadLease> acquire(
                PhysicalObjectIdentity object,
                long maximumReadDeadlineMillis,
                SelectionRevalidator selectionRevalidator) {
            events.add("pin-acquire");
            return delegate.acquire(
                            object,
                            maximumReadDeadlineMillis,
                            selectionRevalidator)
                    .thenApply(lease -> {
                        events.add("pin-acquired");
                        return new ObjectReadLease() {
                            @Override
                            public PhysicalObjectIdentity object() {
                                return lease.object();
                            }

                            @Override
                            public String leaseId() {
                                return lease.leaseId();
                            }

                            @Override
                            public long maximumReadDeadlineMillis() {
                                return lease.maximumReadDeadlineMillis();
                            }

                            @Override
                            public CompletableFuture<Void> release() {
                                events.add("pin-release");
                                return lease.release().thenRun(
                                        () -> events.add("pin-released"));
                            }

                            @Override
                            public boolean isReleased() {
                                return lease.isReleased();
                            }
                        };
                    });
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class RecordingObjectStore implements ObjectStore {
        private final Map<ObjectKey, StoredObject> objects = new HashMap<>();
        private final List<String> events = new ArrayList<>();
        private PutObjectOptions lastPutOptions;
        private int putCalls;
        private int headCalls;
        private int readCalls;
        private boolean returnWrongPutLength;
        private HeadMutation headMutation = HeadMutation.NONE;
        private ReadMutation readMutation = ReadMutation.NONE;
        private boolean closed;

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options,
                PutObjectAttemptGuard attemptGuard) {
            return attemptGuard.authorize(key, 1)
                    .thenCompose(ignored -> collect(source))
                    .thenCompose(bytes -> putObject(
                            key, ByteBuffer.wrap(bytes), options));
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key, ByteBuffer payload, PutObjectOptions options) {
            events.add("put");
            putCalls++;
            lastPutOptions = options;
            byte[] bytes = bytes(payload);
            Checksum checksum = Crc32cChecksums.checksum(bytes);
            if (!checksum.equals(options.expectedChecksum())) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                        false,
                        "checksum mismatch"));
            }
            if (options.ifAbsent() && objects.containsKey(key)) {
                return CompletableFuture.failedFuture(
                        new ObjectAlreadyExistsException("object already exists"));
            }
            objects.put(
                    key,
                    new StoredObject(
                            bytes,
                            checksum,
                            Optional.of(checksum.value()),
                            options.metadata()));
            return CompletableFuture.completedFuture(
                    new PutObjectResult(
                            key,
                            returnWrongPutLength ? bytes.length + 1L : bytes.length,
                            checksum,
                            checksum.value()));
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key, long offset, long length, RangeReadOptions options) {
            events.add("read");
            readCalls++;
            StoredObject stored = require(key);
            byte[] range = Arrays.copyOfRange(
                    stored.bytes(),
                    Math.toIntExact(offset),
                    Math.toIntExact(offset + length));
            Checksum checksum = Crc32cChecksums.checksum(range);
            if (readMutation == ReadMutation.NONE
                    && options.expectedChecksum().isPresent()
                    && !options.expectedChecksum().orElseThrow().equals(checksum)) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                        false,
                        "range checksum mismatch"));
            }
            ObjectKey resultKey = readMutation == ReadMutation.WRONG_KEY
                    ? new ObjectKey("wrong/range-key")
                    : key;
            long resultOffset = readMutation == ReadMutation.WRONG_OFFSET
                    ? offset + 1
                    : offset;
            long resultLength = switch (readMutation) {
                case SHORT_LENGTH -> length - 1;
                case LONG_LENGTH -> length + 1;
                default -> length;
            };
            byte[] resultBytes = switch (readMutation) {
                case SHORT_LENGTH -> Arrays.copyOf(range, range.length - 1);
                case LONG_LENGTH -> Arrays.copyOf(range, range.length + 1);
                default -> range;
            };
            Optional<Checksum> resultChecksum = switch (readMutation) {
                case MISSING_CHECKSUM -> Optional.empty();
                case WRONG_CHECKSUM -> Optional.of(differentChecksum(checksum));
                default -> Optional.of(checksum);
            };
            return CompletableFuture.completedFuture(new RangeReadResult(
                    resultKey,
                    resultOffset,
                    resultLength,
                    ByteBuffer.wrap(resultBytes),
                    resultChecksum));
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key, HeadObjectOptions options) {
            events.add("head");
            headCalls++;
            StoredObject stored = require(key);
            Map<String, String> metadata = headMutation == HeadMutation.WRONG_METADATA
                    ? Map.of("nereus-format", "wrong")
                    : stored.metadata();
            ObjectKey resultKey = headMutation == HeadMutation.WRONG_KEY
                    ? new ObjectKey("wrong/head-key")
                    : key;
            long resultLength = headMutation == HeadMutation.WRONG_LENGTH
                    ? stored.bytes().length + 1L
                    : stored.bytes().length;
            Checksum resultChecksum = headMutation == HeadMutation.WRONG_CHECKSUM
                    ? differentChecksum(stored.checksum())
                    : stored.checksum();
            return CompletableFuture.completedFuture(new HeadObjectResult(
                    resultKey,
                    resultLength,
                    resultChecksum,
                    stored.etag(),
                    metadata));
        }

        @Override
        public void close() {
            closed = true;
        }

        private StoredObject require(ObjectKey key) {
            StoredObject stored = objects.get(key);
            if (stored == null) {
                throw new NereusException(
                        ErrorCode.OBJECT_NOT_FOUND,
                        true,
                        "missing object");
            }
            return stored;
        }

        private static CompletableFuture<byte[]> collect(
                ReplayableObjectUpload source) {
            CompletableFuture<byte[]> result = new CompletableFuture<>();
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            source.openPublisher().subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    output.writeBytes(bytes(item));
                }

                @Override
                public void onError(Throwable throwable) {
                    result.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    result.complete(output.toByteArray());
                }
            });
            return result;
        }

        private static byte[] bytes(ByteBuffer input) {
            ByteBuffer copy = input.asReadOnlyBuffer();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }

        private static Checksum differentChecksum(Checksum checksum) {
            return new Checksum(
                    ChecksumType.CRC32C,
                    checksum.value().equals("00000000")
                            ? "ffffffff"
                            : "00000000");
        }
    }

    private enum HeadMutation {
        NONE,
        WRONG_KEY,
        WRONG_LENGTH,
        WRONG_CHECKSUM,
        WRONG_METADATA
    }

    private enum ReadMutation {
        NONE,
        WRONG_KEY,
        WRONG_OFFSET,
        SHORT_LENGTH,
        LONG_LENGTH,
        MISSING_CHECKSUM,
        WRONG_CHECKSUM
    }

    private record StoredObject(
            byte[] bytes,
            Checksum checksum,
            Optional<String> etag,
            Map<String, String> metadata) {
        private StoredObject {
            bytes = bytes.clone();
            etag = Objects.requireNonNull(etag, "etag");
            metadata = Map.copyOf(metadata);
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }
}
